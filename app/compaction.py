"""Fold a batch of CDC events into one net change per record.

Compaction makes each batch a single atomic MERGE and makes replays harmless:
applying the same batch twice yields the same table.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from app.events import ChangeEvent


@dataclass
class RecordChange:
    entity: str
    record_id: str
    op: str  # "upsert" | "delete"
    commit_ts: int
    change_type: str
    fields: dict[str, Any] = field(default_factory=dict)
    changed: set[str] = field(default_factory=set)


def compact(events: Iterable[ChangeEvent]) -> dict[str, list[RecordChange]]:
    """Return net changes grouped by entity. Events must be in stream (replay) order."""
    net: dict[tuple[str, str], RecordChange] = {}
    for ev in events:
        if ev.is_gap or ev.is_overflow:
            continue
        for rid in ev.record_ids:
            key = (ev.entity, rid)
            current = net.get(key)
            if ev.change_type == "DELETE":
                net[key] = RecordChange(ev.entity, rid, "delete", ev.commit_ts, ev.change_type)
            elif current is None or current.op == "delete":
                net[key] = RecordChange(ev.entity, rid, "upsert", ev.commit_ts, ev.change_type, dict(ev.fields), set(ev.changed))
            else:
                current.fields.update(ev.fields)
                current.changed |= ev.changed
                current.commit_ts = max(current.commit_ts, ev.commit_ts)
                current.change_type = ev.change_type
    grouped: dict[str, list[RecordChange]] = {}
    for change in net.values():
        grouped.setdefault(change.entity, []).append(change)
    return grouped
