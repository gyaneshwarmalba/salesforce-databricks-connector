"""Cloud adapters tested against fakes: SQL generation, SOQL generation, and write-back payloads."""

import json

import pytest

from app.compaction import compact
from app.events import parse_event
from app.merge import plan
from app.sinks import DatabricksSqlSink
from app.sources import SalesforceRestSource
from app.writeback import FileWriter, SalesforceWriter, write_back
from tests.test_events_and_compaction import raw


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn
        self.description = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.conn.executed.append((sql, params))
        self.description = [("num_affected_rows",)] if sql.startswith("MERGE") else None
        self._rows = self.conn.columns if "information_schema" in sql else [(3, 1, 0, 2)]

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0]


class FakeConnection:
    def __init__(self, columns=()):
        self.columns = list(columns)
        self.executed = []

    def cursor(self):
        return FakeCursor(self)


def sample_plan():
    events = [
        parse_event(raw("CREATE", ids=["001A000000000001AA"], ts=1, Name="Acme", AnnualRevenue=5.0)),
        parse_event(raw("DELETE", ids=["001A000000000002AA"], ts=2)),
    ]
    return plan(compact(events)["Account"])


def test_databricks_first_write_creates_table_then_merges():
    conn = FakeConnection()
    sink = DatabricksSqlSink("main", "salesforce", connection=conn)
    result = sink.apply("Account", sample_plan())
    sqls = [s for s, _ in conn.executed]
    assert sqls[1].startswith("CREATE TABLE IF NOT EXISTS `main`.`salesforce`.`account`")
    assert "`AnnualRevenue` DOUBLE" in sqls[1]
    merge, params = conn.executed[2]
    assert merge.startswith("MERGE INTO `main`.`salesforce`.`account` AS t")
    assert "WHEN MATCHED AND s._op = 'delete'" in merge
    assert "CASE WHEN instr(s._changed, ',Name,') > 0 THEN s.`Name` ELSE t.`Name` END" in merge
    assert "Acme" not in merge and "Acme" in params.values()
    assert result == {"updated": 1, "deleted": 0, "inserted": 2}


def test_databricks_adds_new_columns_with_existing_types():
    conn = FakeConnection(columns=[("Id", "STRING"), ("Name", "STRING"), ("_sf_commit_ts", "BIGINT"),
                                   ("_sf_change_type", "STRING"), ("_ingested_at", "STRING")])  # fmt: skip
    DatabricksSqlSink("main", "salesforce", connection=conn).apply("Account", sample_plan())
    alter = conn.executed[1][0]
    assert alter == "ALTER TABLE `main`.`salesforce`.`account` ADD COLUMNS (`AnnualRevenue` DOUBLE)"


def test_databricks_rejects_unsafe_namespace():
    with pytest.raises(ValueError):
        DatabricksSqlSink("main; DROP", "x", connection=FakeConnection())


class FakeSalesforce:
    def __init__(self, records=(), responses=None):
        self.records = list(records)
        self.queries = []
        self.headers = {}
        self.calls = []
        self.responses = responses

    def query_all(self, soql):
        self.queries.append(soql)
        return {"records": self.records}

    def restful(self, path, method="GET", data=None, **kw):
        body = json.loads(data)
        self.calls.append((path, method, body))
        return self.responses or [{"id": r["Id"], "success": True, "errors": []} for r in body["records"]]


def test_rest_source_converts_rows_to_cdc_events_with_watermark():
    sf = FakeSalesforce(
        [
            {"attributes": {}, "Id": "001A000000000001AA", "IsDeleted": False, "SystemModstamp": "2025-01-01T00:00:05.000+0000",
             "LastModifiedById": "005A000000000002AA", "Name": "Acme", "Rating": None},
            {"attributes": {}, "Id": "001A000000000002AA", "IsDeleted": True, "SystemModstamp": "2025-01-01T00:00:06.000+0000",
             "LastModifiedById": "005A000000000002AA", "Name": "Gone", "Rating": None},
        ]
    )  # fmt: skip
    source = SalesforceRestSource("Account", ["Name", "Rating"], client=sf)
    events = source.read(after=1735689600000, limit=100)
    assert "WHERE SystemModstamp >= 2025-01-01T00:00:00.000Z" in sf.queries[0]
    assert "LIMIT 100" in sf.queries[0]
    first, second = (parse_event(e) for e in events)
    assert first.change_type == "UPDATE" and first.fields == {"Name": "Acme", "Rating": None}
    assert second.change_type == "DELETE"
    assert first.replay_id == 1735689605000


def test_rest_source_rejects_unsafe_names_and_ids():
    with pytest.raises(ValueError):
        SalesforceRestSource("Account", ["Name FROM User--"], client=FakeSalesforce())
    sf = FakeSalesforce()
    assert SalesforceRestSource("Account", ["Name"], client=sf).fetch_records("Account", ["x' OR Id != '"]) == {}
    assert sf.queries == []


def test_writeback_tags_calls_and_reports_partial_failures():
    sf = FakeSalesforce(
        responses=[{"id": "001A000000000001AA", "success": True}, {"success": False, "errors": ["FIELD_INTEGRITY"]}]
    )
    writer = SalesforceWriter("sf-dbx-writeback", client=sf)
    rows = [{"Id": "001A000000000001AA", "Churn_Score__c": 0.8}, {"Id": "001A000000000002AA", "Churn_Score__c": 0.1},
            {"Id": "bad", "Churn_Score__c": 0.5}]  # fmt: skip
    result = write_back(writer, "Account", rows, ["Churn_Score__c"])
    assert sf.headers["Sforce-Call-Options"] == "client=sf-dbx-writeback"
    path, method, body = sf.calls[0]
    assert (path, method) == ("composite/sobjects", "PATCH") and body["allOrNone"] is False
    assert body["records"][0] == {"attributes": {"type": "Account"}, "Id": "001A000000000001AA", "Churn_Score__c": 0.8}
    assert result.succeeded == 1 and result.failed[0]["id"] == "001A000000000002AA" and len(result.skipped) == 1


def test_writeback_batches_at_200(tmp_path):
    writer = FileWriter(tmp_path / "wb.jsonl", "sf-dbx-writeback")
    rows = [{"Id": f"001A{i:014d}", "Score__c": i} for i in range(450)]
    result = write_back(writer, "Account", rows, ["Score__c"])
    assert result.sent == result.succeeded == 450
    assert len((tmp_path / "wb.jsonl").read_text().splitlines()) == 450
