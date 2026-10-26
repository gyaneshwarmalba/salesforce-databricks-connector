import json

import pytest

from app.sinks import LocalDeltaSink
from tests.conftest import make_connector, rows

A, B, C = "001A000000000001AA", "001A000000000002AA", "001A000000000003AA"


def test_sample_stream_produces_expected_tables(connector):
    stats = connector.sync()
    assert stats.events_read == 13
    assert stats.skipped_echo == 1 and stats.dead_lettered == 1 and stats.gaps_resolved == 1

    accounts = rows(connector.sink, "Account")
    assert set(accounts) == {A, B, C}
    # partial updates kept untouched columns; compound field flattened and updated
    assert accounts[A]["Name"] == "Acme Analytics" and accounts[A]["AnnualRevenue"] == 5_500_000
    assert accounts[A]["BillingAddress_City"] == "Seattle" and accounts[A]["BillingAddress_State"] == "OR"
    assert accounts[A]["Rating"] == "Hot"
    # write-back echo was dropped, so the column never appeared
    assert "Churn_Score__c" not in accounts[A]
    # nulled field cleared; gap re-fetch brought in a new column (schema evolution)
    assert accounts[B]["Industry"] is None and accounts[B]["NumberOfEmployees"] == 800
    assert accounts[B]["_sf_change_type"] == "REFETCH"
    # delete followed by undelete
    assert accounts[C]["Name"] == "Initech" and accounts[C]["_sf_change_type"] == "UNDELETE"

    assert rows(connector.sink, "Contact")["003A000000000001AA"]["LastName"] == "Lovelace"
    assert connector.checkpoints.get(connector.source.name) == 13


@pytest.mark.parametrize("batch_size", [1, 2, 3, 5])
def test_result_is_independent_of_batch_size(tmp_path, batch_size):
    reference = make_connector(tmp_path / "ref")
    reference.sync()
    chunked = make_connector(tmp_path / f"b{batch_size}")
    stats = chunked.sync(batch_size=batch_size)
    assert stats.batches == -(-13 // batch_size)
    strip = lambda t: {k: {c: v for c, v in r.items() if c != "_sf_change_type"} for k, r in t.items()}  # noqa: E731
    assert strip(rows(chunked.sink, "Account")) == strip(rows(reference.sink, "Account"))


def test_replaying_from_scratch_is_idempotent(tmp_path, connector):
    connector.sync()
    before = rows(connector.sink, "Account")
    (tmp_path / "checkpoints.json").unlink()
    connector.sync()
    assert rows(connector.sink, "Account") == before


def test_no_new_events_is_a_noop(connector):
    connector.sync()
    assert connector.sync_once().events_read == 0


def test_stale_events_do_not_overwrite_newer_rows(tmp_path):
    events = tmp_path / "events.jsonl"
    header = lambda ts, **kw: {"entityName": "Account", "recordIds": [A], "commitTimestamp": ts, **kw}  # noqa: E731
    events.write_text(
        "\n".join(
            json.dumps(e)
            for e in [
                {"replayId": 1, "payload": {"ChangeEventHeader": header(200, changeType="CREATE"), "Name": "New"}},
                {
                    "replayId": 2,
                    "payload": {"ChangeEventHeader": header(100, changeType="UPDATE", changedFields=["Name"]), "Name": "Old"},
                },
            ]
        )
    )
    c = make_connector(tmp_path, events=events)
    c.sync(batch_size=1)
    assert rows(c.sink, "Account")[A]["Name"] == "New"


class FlakySink(LocalDeltaSink):
    def __init__(self, root, failures):
        super().__init__(root)
        self.failures = failures

    def apply(self, entity, plan):
        if self.failures:
            self.failures -= 1
            raise ConnectionError("warehouse unavailable")
        return super().apply(entity, plan)


def test_transient_sink_errors_are_retried(tmp_path, monkeypatch):
    monkeypatch.setattr("app.reliability.time.sleep", lambda s: None)
    monkeypatch.setattr("app.reliability.random.random", lambda: 0.0)
    c = make_connector(tmp_path, sink=FlakySink(tmp_path / "delta", failures=2))
    c.sync()
    assert len(rows(c.sink, "Account")) == 3


def test_checkpoint_is_not_advanced_when_merge_fails(tmp_path, monkeypatch):
    monkeypatch.setattr("app.reliability.time.sleep", lambda s: None)
    c = make_connector(tmp_path, sink=FlakySink(tmp_path / "delta", failures=99))
    with pytest.raises(ConnectionError):
        c.sync()
    assert c.checkpoints.get(c.source.name) is None


def test_integration_user_filter(tmp_path):
    c = make_connector(tmp_path, writeback_client_id="", integration_user_id="005A000000000009AA")
    stats = c.sync()
    assert stats.skipped_echo == 1


def test_backfill_loads_snapshot(tmp_path):
    c = make_connector(tmp_path)
    result = c.backfill("Account")
    assert result["inserted"] == 3
    assert rows(c.sink, "Account")[B]["_sf_change_type"] == "BACKFILL"
