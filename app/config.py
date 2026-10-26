"""Environment-driven configuration and component factories."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    source: str = field(default_factory=lambda: _env("SOURCE", "file"))
    sink: str = field(default_factory=lambda: _env("SINK", "local"))
    events_path: Path = field(
        default_factory=lambda: Path(_env("EVENTS_PATH", str(PROJECT_ROOT / "data/sample_events/account_changes.jsonl")))
    )
    snapshot_path: Path = field(
        default_factory=lambda: Path(_env("SNAPSHOT_PATH", str(PROJECT_ROOT / "data/sample_events/snapshot.json")))
    )
    state_dir: Path = field(default_factory=lambda: Path(_env("STATE_DIR", str(PROJECT_ROOT / "data/state"))))
    sf_entity: str = field(default_factory=lambda: _env("SF_ENTITY", "Account"))
    sf_fields: tuple[str, ...] = field(
        default_factory=lambda: tuple(f for f in _env("SF_FIELDS", "Name,Industry,AnnualRevenue,Rating").split(",") if f)
    )
    databricks_catalog: str = field(default_factory=lambda: _env("DATABRICKS_CATALOG", "main"))
    databricks_schema: str = field(default_factory=lambda: _env("DATABRICKS_SCHEMA", "salesforce"))
    writeback_client_id: str = field(default_factory=lambda: _env("WRITEBACK_CLIENT_ID", "sf-dbx-writeback"))
    integration_user_id: str | None = field(default_factory=lambda: _env("INTEGRATION_USER_ID") or None)

    @property
    def delta_root(self) -> Path:
        return Path(_env("DELTA_ROOT", str(self.state_dir / "delta")))


def build_connector(s: Settings):
    from app.connector import Connector
    from app.reliability import CheckpointStore, DeadLetterQueue
    from app.sinks import DatabricksSqlSink, LocalDeltaSink
    from app.sources import FileEventSource, SalesforceRestSource

    if s.source == "file":
        source = FileEventSource(s.events_path, s.snapshot_path)
    elif s.source == "salesforce":
        source = SalesforceRestSource(s.sf_entity, list(s.sf_fields))
    else:
        raise ValueError(f"unknown SOURCE {s.source!r}")

    if s.sink == "local":
        sink = LocalDeltaSink(s.delta_root)
    elif s.sink == "databricks":
        sink = DatabricksSqlSink(s.databricks_catalog, s.databricks_schema)
    else:
        raise ValueError(f"unknown SINK {s.sink!r}")

    return Connector(
        source,
        sink,
        CheckpointStore(s.state_dir / "checkpoints.json"),
        DeadLetterQueue(s.state_dir / "dead_letter.jsonl"),
        writeback_client_id=s.writeback_client_id,
        integration_user_id=s.integration_user_id,
    )
