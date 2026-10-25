import json

import pyarrow as pa
from deltalake import write_deltalake

from app.main import main


def test_sync_status_and_writeback_dry_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("STATE_DIR", str(tmp_path))
    assert main(["sync", "--batch-size", "5"]) == 0
    stats = json.loads(capsys.readouterr().out)
    assert stats["events_read"] == 13 and stats["batches"] == 3

    assert main(["status"]) == 0
    status = json.loads(capsys.readouterr().out)
    account = next(t for t in status["tables"] if t["entity"] == "Account")
    assert account["rows"] == 3 and status["dead_letters"] == 1

    scores = tmp_path / "scores"
    write_deltalake(str(scores), pa.table({"Id": ["001A000000000001AA"], "Churn_Score__c": [0.82]}))
    assert main(["writeback", "--from-table", str(scores), "--entity", "Account", "--fields", "Churn_Score__c", "--dry-run"]) == 0
    assert json.loads(capsys.readouterr().out)["succeeded"] == 1
    sent = json.loads((tmp_path / "writeback.jsonl").read_text().splitlines()[0])
    assert sent == {"entity": "Account", "client": "sf-dbx-writeback", "Id": "001A000000000001AA", "Churn_Score__c": 0.82}


def test_bad_configuration_fails_cleanly(monkeypatch, capsys):
    monkeypatch.setenv("SINK", "oracle")
    assert main(["status"]) == 1
    assert "unknown SINK" in capsys.readouterr().err
