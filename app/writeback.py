"""Databricks to Salesforce write-back (for example, ML scores onto Account).

Every write is tagged with `Sforce-Call-Options: client=<WRITEBACK_CLIENT_ID>`.
Salesforce records that value in the CDC `changeOrigin` of the resulting change
events, and the connector drops events carrying it. That breaks the
Salesforce -> Delta -> Salesforce feedback loop without hiding real user edits.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from app.events import FIELD_NAME_RE
from app.sources import SF_ID_RE

logger = logging.getLogger(__name__)
BATCH = 200  # composite sObject collection limit


@dataclass
class WritebackResult:
    sent: int = 0
    succeeded: int = 0
    failed: list[dict[str, Any]] = field(default_factory=list)
    skipped: list[dict[str, Any]] = field(default_factory=list)


class Writer(Protocol):
    def update(self, entity: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]: ...


class SalesforceWriter:
    def __init__(self, client_id: str, client: Any | None = None) -> None:
        if client is None:
            import os

            from simple_salesforce import Salesforce

            client = Salesforce(
                username=os.environ["SF_USERNAME"],
                password=os.environ["SF_PASSWORD"],
                security_token=os.environ["SF_SECURITY_TOKEN"],
                domain=os.getenv("SF_DOMAIN", "login"),
            )
        self._sf = client
        self._sf.headers["Sforce-Call-Options"] = f"client={client_id}"

    def update(self, entity: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        body = {"allOrNone": False, "records": [{"attributes": {"type": entity}, **r} for r in records]}
        return self._sf.restful("composite/sobjects", method="PATCH", data=json.dumps(body))


class FileWriter:
    """Offline stand-in that records what would be sent to Salesforce."""

    def __init__(self, path: Path, client_id: str) -> None:
        self.path = path
        self.client_id = client_id

    def update(self, entity: str, records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as fh:
            for r in records:
                fh.write(json.dumps({"entity": entity, "client": self.client_id, **r}) + "\n")
        return [{"id": r["Id"], "success": True, "errors": []} for r in records]


def write_back(writer: Writer, entity: str, rows: list[dict[str, Any]], fields: list[str]) -> WritebackResult:
    for name in [entity, *fields]:
        if not FIELD_NAME_RE.match(name):
            raise ValueError(f"unsafe Salesforce name {name!r}")
    result = WritebackResult()
    records = []
    for row in rows:
        rid = str(row.get("Id", ""))
        if not SF_ID_RE.match(rid):
            result.skipped.append({"row": row, "reason": "missing or malformed Id"})
            continue
        records.append({"Id": rid, **{f: row.get(f) for f in fields}})

    for i in range(0, len(records), BATCH):
        chunk = records[i : i + BATCH]
        result.sent += len(chunk)
        try:
            responses = writer.update(entity, chunk)
        except Exception as exc:
            logger.exception("write-back batch failed", extra={"entity": entity, "batch_start": i})
            result.failed.extend({"id": r["Id"], "errors": [str(exc)]} for r in chunk)
            continue
        for rec, resp in zip(chunk, responses, strict=True):
            if resp.get("success"):
                result.succeeded += 1
            else:
                result.failed.append({"id": rec["Id"], "errors": resp.get("errors", [])})
    logger.info(
        "write-back complete",
        extra={"entity": entity, "sent": result.sent, "succeeded": result.succeeded, "failed": len(result.failed)},
    )
    return result
