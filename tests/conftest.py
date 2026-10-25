from __future__ import annotations

from pathlib import Path

import pytest

from app.connector import Connector
from app.reliability import CheckpointStore, DeadLetterQueue
from app.sinks import LocalDeltaSink
from app.sources import FileEventSource

DATA = Path(__file__).resolve().parent.parent / "data" / "sample_events"
EVENTS = DATA / "account_changes.jsonl"
SNAPSHOT = DATA / "snapshot.json"


def make_connector(tmp_path: Path, events: Path = EVENTS, sink=None, **kwargs) -> Connector:
    return Connector(
        FileEventSource(events, SNAPSHOT),
        sink or LocalDeltaSink(tmp_path / "delta"),
        CheckpointStore(tmp_path / "checkpoints.json"),
        DeadLetterQueue(tmp_path / "dead_letter.jsonl"),
        **kwargs,
    )


def rows(sink: LocalDeltaSink, entity: str) -> dict[str, dict]:
    return {r["Id"]: {k: v for k, v in r.items() if k != "_ingested_at"} for r in sink.read(entity)}


@pytest.fixture()
def connector(tmp_path) -> Connector:
    return make_connector(tmp_path)
