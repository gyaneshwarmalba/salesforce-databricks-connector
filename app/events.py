"""Salesforce Change Data Capture (CDC) event model.

Events follow the decoded CDC payload shape delivered by the Pub/Sub API:
a `ChangeEventHeader` plus the record fields. Important CDC semantics handled
here:

* UPDATE events carry only changed fields (`changedFields`); fields set to null
  are listed in `nulledFields` and are absent from the payload.
* Compound fields (e.g. BillingAddress) arrive nested and are flattened to
  `BillingAddress_City`; `changedFields` refers to them as `BillingAddress.City`.
* One event can apply to many records (`recordIds`).
* GAP_* events carry no field data: the records must be re-fetched.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

CHANGE_TYPES = {"CREATE", "UPDATE", "DELETE", "UNDELETE"}
GAP_TYPES = {"GAP_CREATE", "GAP_UPDATE", "GAP_DELETE", "GAP_UNDELETE"}
OVERFLOW = "GAP_OVERFLOW"
FIELD_NAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,79}$")
HEADER = "ChangeEventHeader"


class InvalidEventError(ValueError):
    pass


def column_name(sf_field: str) -> str:
    name = sf_field.replace(".", "_")
    if not FIELD_NAME_RE.match(name):
        raise InvalidEventError(f"unsafe field name {sf_field!r}")
    return name


def flatten(record: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in record.items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            out.update(flatten(value, f"{name}_"))
        else:
            out[column_name(name)] = value
    return out


@dataclass(frozen=True)
class ChangeEvent:
    replay_id: int
    entity: str
    change_type: str
    record_ids: tuple[str, ...]
    commit_ts: int
    commit_user: str
    change_origin: str
    fields: dict[str, Any] = field(default_factory=dict)
    changed: frozenset[str] = frozenset()

    @property
    def is_gap(self) -> bool:
        return self.change_type in GAP_TYPES

    @property
    def is_overflow(self) -> bool:
        return self.change_type == OVERFLOW


def parse_event(raw: dict[str, Any]) -> ChangeEvent:
    try:
        payload = dict(raw["payload"])
        header = payload.pop(HEADER)
        change_type = header["changeType"]
        entity = header["entityName"]
    except (KeyError, TypeError) as exc:
        raise InvalidEventError(f"malformed CDC event: missing {exc}") from exc
    if change_type not in CHANGE_TYPES | GAP_TYPES | {OVERFLOW}:
        raise InvalidEventError(f"unknown changeType {change_type!r}")
    if not FIELD_NAME_RE.match(entity):
        raise InvalidEventError(f"unsafe entity name {entity!r}")

    fields = flatten({k: v for k, v in payload.items() if v is not None})
    nulled = {column_name(f) for f in header.get("nulledFields", [])}
    if change_type == "UPDATE":
        changed = {column_name(f) for f in header.get("changedFields", [])} | nulled
        fields = {k: v for k, v in fields.items() if k in changed}
        fields.update({k: None for k in nulled})
    elif change_type in {"CREATE", "UNDELETE"}:
        changed = set(fields)
    else:
        changed, fields = set(), {}

    return ChangeEvent(
        replay_id=int(raw["replayId"]),
        entity=entity,
        change_type=change_type,
        record_ids=tuple(header.get("recordIds", [])),
        commit_ts=int(header.get("commitTimestamp", 0)),
        commit_user=header.get("commitUser", ""),
        change_origin=header.get("changeOrigin", ""),
        fields=fields,
        changed=frozenset(changed),
    )
