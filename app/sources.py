"""Event sources.

`FileEventSource` replays CDC events from JSONL (offline demo, tests, and
replaying captured production events). A JSON snapshot stands in for the
Salesforce REST API when gap events require re-fetching records.

`SalesforceRestSource` polls an object through the REST API using
`SystemModstamp` as the watermark and `queryAll` to see deletes. It emits
CDC-shaped events, so the rest of the pipeline is identical. It works in any
org without CDC entitlements; for push-based streaming at scale, a Pub/Sub API
subscriber would produce the same events (see README).
"""

from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from app.events import FIELD_NAME_RE

logger = logging.getLogger(__name__)
SF_ID_RE = re.compile(r"^[a-zA-Z0-9]{15,18}$")


class Source(Protocol):
    name: str

    def read(self, after: int | None, limit: int) -> list[dict[str, Any]]: ...

    def fetch_records(self, entity: str, ids: list[str]) -> dict[str, dict[str, Any]]: ...

    def fetch_all(self, entity: str) -> list[dict[str, Any]]: ...


class FileEventSource:
    def __init__(self, events_path: Path, snapshot_path: Path | None = None) -> None:
        self.events_path = events_path
        self.snapshot_path = snapshot_path
        self.name = f"file:{events_path.name}"

    def read(self, after: int | None, limit: int) -> list[dict[str, Any]]:
        out = []
        with self.events_path.open(encoding="utf-8") as fh:
            for lineno, line in enumerate(fh, start=1):
                if not line.strip():
                    continue
                try:
                    raw = json.loads(line)
                    replay = int(raw.get("replayId", lineno))
                except (json.JSONDecodeError, ValueError, TypeError):
                    raw, replay = {"replayId": lineno, "unparseable": line.strip()}, lineno
                if after is not None and replay <= after:
                    continue
                out.append(raw)
                if len(out) == limit:
                    break
        return out

    def _snapshot(self) -> dict[str, list[dict[str, Any]]]:
        if not self.snapshot_path or not self.snapshot_path.exists():
            raise RuntimeError("gap or overflow event received but no snapshot is configured to re-fetch records")
        return json.loads(self.snapshot_path.read_text(encoding="utf-8"))

    def fetch_records(self, entity: str, ids: list[str]) -> dict[str, dict[str, Any]]:
        wanted = set(ids)
        return {r["Id"]: r for r in self._snapshot().get(entity, []) if r["Id"] in wanted}

    def fetch_all(self, entity: str) -> list[dict[str, Any]]:
        return list(self._snapshot().get(entity, []))


class SalesforceRestSource:
    def __init__(self, entity: str, fields: list[str], client: Any | None = None) -> None:
        for name in [entity, *fields]:
            if not FIELD_NAME_RE.match(name):
                raise ValueError(f"unsafe Salesforce name {name!r}")
        self.entity = entity
        self.fields = [f for f in fields if f not in {"Id", "IsDeleted", "SystemModstamp", "LastModifiedById"}]
        self.name = f"salesforce-rest:{entity}"
        self._sf = client or self._connect()

    @staticmethod
    def _connect() -> Any:
        import os

        try:
            from simple_salesforce import Salesforce
        except ImportError as exc:
            raise RuntimeError("SOURCE=salesforce requires simple-salesforce (requirements-cloud.txt)") from exc
        return Salesforce(
            username=os.environ["SF_USERNAME"],
            password=os.environ["SF_PASSWORD"],
            security_token=os.environ["SF_SECURITY_TOKEN"],
            domain=os.getenv("SF_DOMAIN", "login"),
        )

    def _select(self) -> str:
        return ", ".join(["Id", "IsDeleted", "SystemModstamp", "LastModifiedById", *self.fields])

    def read(self, after: int | None, limit: int) -> list[dict[str, Any]]:
        where = ""
        if after:
            ts = datetime.fromtimestamp(after / 1000, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            where = f" WHERE SystemModstamp >= {ts}"  # >= plus idempotent MERGE: no gaps on equal timestamps
        soql = f"SELECT {self._select()} FROM {self.entity}{where} ORDER BY SystemModstamp ASC LIMIT {int(limit)}"
        rows = self._sf.query_all(soql)["records"]
        return [self._to_event(r) for r in rows]

    def _to_event(self, row: dict[str, Any]) -> dict[str, Any]:
        stamp = datetime.strptime(row["SystemModstamp"].replace("+0000", "+00:00"), "%Y-%m-%dT%H:%M:%S.%f%z")
        commit_ts = int(stamp.timestamp() * 1000)
        record = {k: v for k, v in row.items() if k not in {"attributes", "IsDeleted", "SystemModstamp", "LastModifiedById"}}
        header = {
            "entityName": self.entity,
            "recordIds": [row["Id"]],
            "changeType": "DELETE" if row.get("IsDeleted") else "UPDATE",
            "commitTimestamp": commit_ts,
            "commitUser": row.get("LastModifiedById", ""),
            "changeOrigin": "rest-poll",
            "changedFields": [k for k in record if k != "Id"],
            "nulledFields": [k for k, v in record.items() if v is None],
        }
        return {"replayId": commit_ts, "payload": {"ChangeEventHeader": header, **record}}

    def fetch_records(self, entity: str, ids: list[str]) -> dict[str, dict[str, Any]]:
        safe = [i for i in ids if SF_ID_RE.match(i)]
        if len(safe) != len(ids):
            logger.warning("dropping malformed record ids", extra={"dropped": sorted(set(ids) - set(safe))})
        if not safe:
            return {}
        id_list = ", ".join(f"'{i}'" for i in safe)
        rows = self._sf.query_all(f"SELECT {self._select()} FROM {entity} WHERE Id IN ({id_list})")["records"]
        return {r["Id"]: {k: v for k, v in r.items() if k != "attributes"} for r in rows if not r.get("IsDeleted")}

    def fetch_all(self, entity: str) -> list[dict[str, Any]]:
        rows = self._sf.query_all(f"SELECT {self._select()} FROM {entity}")["records"]
        return [{k: v for k, v in r.items() if k != "attributes"} for r in rows]
