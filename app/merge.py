"""MERGE semantics shared by the local Delta sink and the Databricks SQL sink.

One MERGE per entity per batch:

* deletes remove the row;
* upserts update only the columns that actually changed (CDC partial updates),
  using `CASE WHEN instr(s._changed, ',Col,') > 0 THEN s.Col ELSE t.Col END`
  (`instr`, not `LIKE`, because `_` is a LIKE wildcard and common in field names);
* updates and deletes apply only if they are not older than the row
  (`s._sf_commit_ts >= t._sf_commit_ts`), so replays and late events are no-ops.

Column names are validated identifiers (see events.FIELD_NAME_RE); values are
never interpolated into SQL.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from app.compaction import RecordChange
from app.events import FIELD_NAME_RE

KEY = "Id"
META_COLUMNS = ("_sf_commit_ts", "_sf_change_type", "_ingested_at")
SOURCE_ONLY = ("_op", "_changed")


def _check(name: str) -> str:
    if not FIELD_NAME_RE.match(name.lstrip("_")):
        raise ValueError(f"unsafe column name {name!r}")
    return name


@dataclass(frozen=True)
class MergePlan:
    data_columns: list[str]
    rows: list[dict[str, Any]]

    @property
    def all_columns(self) -> list[str]:
        return [KEY, *self.data_columns, *META_COLUMNS]

    @property
    def source_columns(self) -> list[str]:
        return [*self.all_columns, *SOURCE_ONLY]

    def update_set(self, quote: str = "") -> dict[str, str]:
        q = lambda c: f"{quote}{c}{quote}"  # noqa: E731
        sets = {q(c): f"CASE WHEN instr(s._changed, ',{c},') > 0 THEN s.{q(c)} ELSE t.{q(c)} END" for c in self.data_columns}
        sets.update({c: f"s.{c}" for c in META_COLUMNS})
        return sets

    def insert_values(self, quote: str = "") -> dict[str, str]:
        return {f"{quote}{c}{quote}": f"s.{quote}{c}{quote}" for c in self.all_columns}

    _not_stale = "(t._sf_commit_ts IS NULL OR s._sf_commit_ts >= t._sf_commit_ts)"
    delete_predicate = f"s._op = 'delete' AND {_not_stale}"
    update_predicate = f"s._op = 'upsert' AND {_not_stale}"
    insert_predicate = "s._op = 'upsert'"
    join_predicate = f"t.{KEY} = s.{KEY}"


def plan(changes: list[RecordChange]) -> MergePlan:
    columns = sorted({_check(c) for ch in changes for c in ch.changed if c != KEY})
    now = datetime.now(UTC).isoformat()
    rows = []
    for ch in changes:
        row: dict[str, Any] = {c: ch.fields.get(c) for c in columns}
        row.update(
            {
                KEY: ch.record_id,
                "_sf_commit_ts": ch.commit_ts,
                "_sf_change_type": ch.change_type,
                "_ingested_at": now,
                "_op": ch.op,
                "_changed": "," + ",".join(sorted(ch.changed)) + ",",
            }
        )
        rows.append(row)
    return MergePlan(data_columns=columns, rows=rows)
