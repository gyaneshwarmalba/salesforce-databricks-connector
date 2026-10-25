"""CLI: `python -m app.main {sync,backfill,writeback,status}`."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from app.config import Settings, build_connector
from app.logging_config import configure_logging

logger = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="salesforce-databricks-connector")
    sub = parser.add_subparsers(dest="command", required=True)

    sync = sub.add_parser("sync", help="apply pending change events to the sink")
    sync.add_argument("--batch-size", type=int, default=500)
    sync.add_argument("--follow", action="store_true", help="keep polling for new events")
    sync.add_argument("--interval", type=float, default=30.0, help="seconds between polls with --follow")

    backfill = sub.add_parser("backfill", help="initial load of an object from the source")
    backfill.add_argument("--entity", default=None)

    wb = sub.add_parser("writeback", help="write fields from a local Delta table back to Salesforce")
    wb.add_argument("--from-table", required=True, help="path to a Delta table with Id and the fields to write")
    wb.add_argument("--entity", required=True)
    wb.add_argument("--fields", required=True, help="comma-separated Salesforce field API names")
    wb.add_argument("--dry-run", action="store_true", help="record payloads to data/state/writeback.jsonl instead")

    sub.add_parser("status", help="checkpoints, dead letters, and table versions")
    args = parser.parse_args(argv)

    configure_logging()
    settings = Settings()
    try:
        if args.command == "writeback":
            return _writeback(settings, args)
        connector = build_connector(settings)
        if args.command == "sync":
            while True:
                stats = connector.sync(batch_size=args.batch_size)
                print(json.dumps(stats.to_dict(), indent=2))
                if not args.follow:
                    break
                time.sleep(args.interval)
        elif args.command == "backfill":
            print(json.dumps(connector.backfill(args.entity or settings.sf_entity)))
        else:
            entities = sorted({e for e in ("Account", "Contact", settings.sf_entity)})
            print(
                json.dumps(
                    {
                        "source": connector.source.name,
                        "sink": connector.sink.name,
                        "checkpoints": connector.checkpoints.all(),
                        "dead_letters": connector.dead_letters.count(),
                        "tables": [connector.sink.table_info(e) for e in entities],
                    },
                    indent=2,
                )
            )
    except KeyboardInterrupt:
        return 130
    except (ValueError, RuntimeError, OSError) as exc:
        logger.exception("command failed", extra={"command": args.command})
        print(f"error: {exc}", file=sys.stderr)
        return 1
    return 0


def _writeback(settings: Settings, args: argparse.Namespace) -> int:
    from deltalake import DeltaTable

    from app.writeback import FileWriter, SalesforceWriter, write_back

    fields = [f.strip() for f in args.fields.split(",") if f.strip()]
    rows = DeltaTable(args.from_table).to_pyarrow_table(columns=["Id", *fields]).to_pylist()
    if args.dry_run:
        writer = FileWriter(settings.state_dir / "writeback.jsonl", settings.writeback_client_id)
    else:
        writer = SalesforceWriter(settings.writeback_client_id)
    result = write_back(writer, args.entity, rows, fields)
    print(
        json.dumps({"sent": result.sent, "succeeded": result.succeeded, "failed": result.failed, "skipped": len(result.skipped)})
    )
    return 0 if not result.failed else 1


if __name__ == "__main__":
    sys.exit(main())
