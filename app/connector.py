"""Sync orchestration: read -> validate -> filter echoes -> resolve gaps -> compact -> MERGE -> checkpoint.

Delivery is at-least-once: the checkpoint advances only after every entity in
the batch has been merged. The MERGE is idempotent (commit-timestamp guard), so
re-processing a batch after a crash does not change the result.
"""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from app.compaction import compact
from app.events import ChangeEvent, InvalidEventError, flatten, parse_event
from app.merge import plan
from app.reliability import CheckpointStore, DeadLetterQueue, retry
from app.sinks import Sink
from app.sources import Source

logger = logging.getLogger(__name__)


@dataclass
class SyncStats:
    events_read: int = 0
    events_applied: int = 0
    skipped_echo: int = 0
    dead_lettered: int = 0
    gaps_resolved: int = 0
    full_resyncs: int = 0
    inserted: int = 0
    updated: int = 0
    deleted: int = 0
    batches: int = 0
    last_replay_id: int | None = None
    lag_ms: int | None = None
    entities: dict[str, int] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class Connector:
    def __init__(
        self,
        source: Source,
        sink: Sink,
        checkpoints: CheckpointStore,
        dead_letters: DeadLetterQueue,
        *,
        writeback_client_id: str = "sf-dbx-writeback",
        integration_user_id: str | None = None,
        clock=time.time,
    ) -> None:
        self.source = source
        self.sink = sink
        self.checkpoints = checkpoints
        self.dead_letters = dead_letters
        self.writeback_client_id = writeback_client_id
        self.integration_user_id = integration_user_id
        self._clock = clock

    def _is_echo(self, ev: ChangeEvent) -> bool:
        if self.writeback_client_id and f"client={self.writeback_client_id}" in ev.change_origin:
            return True
        return bool(self.integration_user_id) and ev.commit_user == self.integration_user_id

    @staticmethod
    def _synthetic(template: ChangeEvent, record_id: str, record: dict[str, Any] | None, kind: str = "REFETCH") -> ChangeEvent:
        """A full-record event built from re-fetched source state (or a delete if the record is gone)."""
        if record is None:
            return ChangeEvent(template.replay_id, template.entity, "DELETE", (record_id,), template.commit_ts, "", kind.lower())
        fields = {k: v for k, v in flatten({k: v for k, v in record.items() if k != "attributes"}).items() if k != "Id"}
        return ChangeEvent(
            template.replay_id,
            template.entity,
            kind,
            (record_id,),
            template.commit_ts,
            template.commit_user,
            kind.lower(),
            fields,
            frozenset(fields),
        )

    def _resolve(self, ev: ChangeEvent, stats: SyncStats) -> list[ChangeEvent]:
        """Replace GAP_* events with re-fetched records and GAP_OVERFLOW with a full entity resync."""
        if ev.is_overflow:
            records = retry(lambda: self.source.fetch_all(ev.entity), what=f"fetch_all {ev.entity}")
            stats.full_resyncs += 1
            logger.warning("overflow event: full resync of entity", extra={"entity": ev.entity, "records": len(records)})
            return [self._synthetic(ev, r["Id"], r) for r in records]
        found = retry(lambda: self.source.fetch_records(ev.entity, list(ev.record_ids)), what=f"fetch {ev.entity}")
        stats.gaps_resolved += len(ev.record_ids)
        return [self._synthetic(ev, rid, found.get(rid)) for rid in ev.record_ids]

    def sync_once(self, batch_size: int = 500) -> SyncStats:
        stats = SyncStats()
        after = self.checkpoints.get(self.source.name)
        raws = retry(lambda: self.source.read(after, batch_size), what="read events")
        if not raws:
            return stats
        stats.events_read = len(raws)

        events: list[ChangeEvent] = []
        for raw in raws:
            try:
                ev = parse_event(raw)
            except (InvalidEventError, KeyError, ValueError, TypeError) as exc:
                self.dead_letters.put(raw, str(exc))
                stats.dead_lettered += 1
                continue
            if self._is_echo(ev):
                stats.skipped_echo += 1
                continue
            events.extend(self._resolve(ev, stats) if ev.is_gap or ev.is_overflow else [ev])

        stats.events_applied = len(events)
        for entity, changes in compact(events).items():
            merge_plan = plan(changes)
            result = retry(lambda e=entity, p=merge_plan: self.sink.apply(e, p), what=f"merge {entity}")
            stats.inserted += result["inserted"]
            stats.updated += result["updated"]
            stats.deleted += result["deleted"]
            stats.entities[entity] = len(changes)

        last = max(int(r.get("replayId", 0)) for r in raws)
        self.checkpoints.set(self.source.name, last)
        stats.batches = 1
        stats.last_replay_id = last
        if events:
            stats.lag_ms = int(self._clock() * 1000) - max(e.commit_ts for e in events)
        logger.info("batch committed", extra=stats.to_dict())
        return stats

    def sync(self, batch_size: int = 500, max_batches: int | None = None) -> SyncStats:
        """Drain the source until it is empty (or `max_batches` is reached)."""
        total = SyncStats()
        while max_batches is None or total.batches < max_batches:
            s = self.sync_once(batch_size)
            if not s.events_read:
                break
            for k in ("events_read", "events_applied", "skipped_echo", "dead_lettered", "gaps_resolved", "full_resyncs",
                      "inserted", "updated", "deleted", "batches"):  # fmt: skip
                setattr(total, k, getattr(total, k) + getattr(s, k))
            for entity, n in s.entities.items():
                total.entities[entity] = total.entities.get(entity, 0) + n
            total.last_replay_id, total.lag_ms = s.last_replay_id, s.lag_ms
        return total

    def backfill(self, entity: str) -> dict[str, int]:
        """Initial load of an entity from the source's current state, before streaming starts."""
        records = retry(lambda: self.source.fetch_all(entity), what=f"fetch_all {entity}")
        template = ChangeEvent(0, entity, "BACKFILL", (), int(self._clock() * 1000), "", "backfill")
        events = [self._synthetic(template, r["Id"], r, kind="BACKFILL") for r in records]
        changes = compact(events).get(entity, [])
        result = retry(lambda: self.sink.apply(entity, plan(changes)), what=f"backfill {entity}")
        logger.info("backfill complete", extra={"entity": entity, "records": len(records), **result})
        return result
