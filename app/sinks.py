"""Sinks that apply a MergePlan to a table per Salesforce object.

`LocalDeltaSink` writes real Delta Lake tables on disk with delta-rs (no Spark
or JVM), so the same ACID MERGE semantics are testable offline.
`DatabricksSqlSink` runs the equivalent `MERGE INTO` on a Databricks SQL
warehouse with Unity Catalog table names.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol

import pyarrow as pa

from app.merge import KEY, MergePlan

logger = logging.getLogger(__name__)

ARROW_TYPES = {"string": pa.string(), "double": pa.float64(), "boolean": pa.bool_(), "long": pa.int64()}
META_TYPES = {"_sf_commit_ts": "long", "_sf_change_type": "string", "_ingested_at": "string"}


def infer_type(values: list[Any]) -> str:
    sample = next((v for v in values if v is not None), None)
    if isinstance(sample, bool):
        return "boolean"
    if isinstance(sample, int | float):
        return "double"
    return "string"


def coerce(value: Any, kind: str) -> Any:
    if value is None:
        return None
    if kind == "double":
        return float(value)
    if kind == "boolean":
        return value if isinstance(value, bool) else str(value).lower() == "true"
    if kind == "long":
        return int(value)
    return value if isinstance(value, str) else str(value)


class Sink(Protocol):
    name: str

    def apply(self, entity: str, plan: MergePlan) -> dict[str, int]: ...

    def table_info(self, entity: str) -> dict[str, Any]: ...


class LocalDeltaSink:
    name = "local-delta"

    def __init__(self, root: Path) -> None:
        self.root = root

    def path(self, entity: str) -> Path:
        return self.root / entity.lower()

    def _column_types(self, entity: str, plan: MergePlan) -> dict[str, str]:
        from deltalake import DeltaTable

        existing: dict[str, str] = {}
        if (self.path(entity) / "_delta_log").exists():
            for f in DeltaTable(str(self.path(entity))).schema().fields:
                existing[f.name] = f.type.type
        types = {KEY: "string", **META_TYPES}
        for col in plan.data_columns:
            types[col] = existing.get(col) or infer_type([r[col] for r in plan.rows])
        return types

    def apply(self, entity: str, plan: MergePlan) -> dict[str, int]:
        from deltalake import DeltaTable, Field, write_deltalake
        from deltalake.schema import PrimitiveType

        if not plan.rows:
            return {"inserted": 0, "updated": 0, "deleted": 0}
        types = self._column_types(entity, plan)
        arrays = {c: pa.array([coerce(r[c], types[c]) for r in plan.rows], ARROW_TYPES[types[c]]) for c in plan.all_columns}
        arrays["_op"] = pa.array([r["_op"] for r in plan.rows], pa.string())
        arrays["_changed"] = pa.array([r["_changed"] for r in plan.rows], pa.string())
        source = pa.table(arrays)

        path = str(self.path(entity))
        if not (self.path(entity) / "_delta_log").exists():
            self.root.mkdir(parents=True, exist_ok=True)
            empty = pa.schema([(c, ARROW_TYPES[types[c]]) for c in plan.all_columns])
            write_deltalake(path, empty.empty_table())
            logger.info("delta table created", extra={"entity": entity, "path": path})

        table = DeltaTable(path)
        current = {f.name for f in table.schema().fields}
        new_cols = [c for c in plan.all_columns if c not in current]
        if new_cols:
            table.alter.add_columns([Field(c, PrimitiveType(types[c]), nullable=True) for c in new_cols])
            logger.info("schema evolved", extra={"entity": entity, "added_columns": new_cols})
            table = DeltaTable(path)

        metrics = (
            table.merge(source, plan.join_predicate, source_alias="s", target_alias="t")
            .when_matched_delete(plan.delete_predicate)
            .when_matched_update(plan.update_set(), predicate=plan.update_predicate)
            .when_not_matched_insert(plan.insert_values(), predicate=plan.insert_predicate)
            .execute()
        )
        return {
            "inserted": metrics.get("num_target_rows_inserted", 0),
            "updated": metrics.get("num_target_rows_updated", 0),
            "deleted": metrics.get("num_target_rows_deleted", 0),
        }

    def read(self, entity: str) -> list[dict[str, Any]]:
        from deltalake import DeltaTable

        if not (self.path(entity) / "_delta_log").exists():
            return []
        return DeltaTable(str(self.path(entity))).to_pyarrow_table().to_pylist()

    def table_info(self, entity: str) -> dict[str, Any]:
        from deltalake import DeltaTable

        if not (self.path(entity) / "_delta_log").exists():
            return {"entity": entity, "exists": False}
        table = DeltaTable(str(self.path(entity)))
        return {
            "entity": entity,
            "exists": True,
            "version": table.version(),
            "rows": table.to_pyarrow_dataset().count_rows(),
            "columns": [f.name for f in table.schema().fields],
        }


SQL_TYPES = {"string": "STRING", "double": "DOUBLE", "boolean": "BOOLEAN", "long": "BIGINT"}


class DatabricksSqlSink:
    """MERGE INTO a Unity Catalog table through a Databricks SQL warehouse.

    `connection` is any DB-API connection; by default one is opened with
    databricks-sql-connector from DATABRICKS_HOST / DATABRICKS_HTTP_PATH /
    DATABRICKS_TOKEN.
    """

    name = "databricks-sql"

    def __init__(self, catalog: str, schema: str, connection: Any | None = None) -> None:
        for part in (catalog, schema):
            if not part.replace("_", "").isalnum():
                raise ValueError(f"unsafe catalog/schema name {part!r}")
        self.catalog, self.schema = catalog, schema
        self._conn = connection or self._connect()

    @staticmethod
    def _connect() -> Any:
        import os

        try:
            from databricks import sql
        except ImportError as exc:
            raise RuntimeError("SINK=databricks requires databricks-sql-connector (requirements-cloud.txt)") from exc
        return sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://"),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )

    def table(self, entity: str) -> str:
        return f"`{self.catalog}`.`{self.schema}`.`{entity.lower()}`"

    def statements(self, entity: str, plan: MergePlan, existing: dict[str, str]) -> list[tuple[str, dict[str, Any]]]:
        """Build (sql, params) pairs. Pure function, so it is unit-tested without a warehouse.

        `existing` maps current column names to their kind (string/double/boolean/long).
        """
        types = {KEY: "string", **META_TYPES}
        types.update({c: existing.get(c) or infer_type([r[c] for r in plan.rows]) for c in plan.data_columns})
        table = self.table(entity)
        stmts: list[tuple[str, dict[str, Any]]] = []

        if not existing:
            cols = ", ".join(f"`{c}` {SQL_TYPES[types[c]]}" for c in plan.all_columns)
            stmts.append((f"CREATE TABLE IF NOT EXISTS {table} ({cols}) USING DELTA", {}))
        else:
            new_cols = [c for c in plan.all_columns if c not in existing]
            if new_cols:
                cols = ", ".join(f"`{c}` {SQL_TYPES[types[c]]}" for c in new_cols)
                stmts.append((f"ALTER TABLE {table} ADD COLUMNS ({cols})", {}))

        params: dict[str, Any] = {}
        value_rows = []
        source_cols = plan.source_columns
        for i, row in enumerate(plan.rows):
            placeholders = []
            for j, col in enumerate(source_cols):
                key = f"p{i}_{j}"
                kind = types.get(col, "string")
                params[key] = coerce(row[col], kind)
                placeholders.append(f"CAST(:{key} AS {SQL_TYPES[kind]})")
            value_rows.append("(" + ", ".join(placeholders) + ")")

        update_set = ", ".join(f"{k} = {v}" for k, v in plan.update_set(quote="`").items())
        insert = plan.insert_values(quote="`")
        insert_cols, insert_vals = ", ".join(insert), ", ".join(insert.values())
        merge_sql = (
            f"MERGE INTO {table} AS t USING (SELECT * FROM VALUES {', '.join(value_rows)} "
            f"AS s({', '.join(f'`{c}`' for c in source_cols)})) AS s ON {plan.join_predicate} "
            f"WHEN MATCHED AND {plan.delete_predicate} THEN DELETE "
            f"WHEN MATCHED AND {plan.update_predicate} THEN UPDATE SET {update_set} "
            f"WHEN NOT MATCHED AND {plan.insert_predicate} THEN INSERT ({insert_cols}) VALUES ({insert_vals})"
        )
        stmts.append((merge_sql, params))
        return stmts

    def _existing_columns(self, entity: str) -> dict[str, str]:
        kinds = {v: k for k, v in SQL_TYPES.items()}
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT column_name, data_type FROM system.information_schema.columns "
                "WHERE table_catalog = :c AND table_schema = :s AND table_name = :t",
                {"c": self.catalog, "s": self.schema, "t": entity.lower()},
            )
            return {r[0]: kinds.get(str(r[1]).upper(), "string") for r in cur.fetchall()}

    def apply(self, entity: str, plan: MergePlan) -> dict[str, int]:
        if not plan.rows:
            return {"inserted": 0, "updated": 0, "deleted": 0}
        existing = self._existing_columns(entity)
        with self._conn.cursor() as cur:
            for sql, params in self.statements(entity, plan, existing):
                cur.execute(sql, params or None)
            row = cur.fetchone() if cur.description else None
        if row and len(row) >= 4:  # MERGE returns num_affected, num_updated, num_deleted, num_inserted
            return {"updated": int(row[1]), "deleted": int(row[2]), "inserted": int(row[3])}
        return {"inserted": -1, "updated": -1, "deleted": -1}

    def table_info(self, entity: str) -> dict[str, Any]:
        cols = self._existing_columns(entity)
        return {"entity": entity, "exists": bool(cols), "columns": sorted(cols), "table": self.table(entity)}
