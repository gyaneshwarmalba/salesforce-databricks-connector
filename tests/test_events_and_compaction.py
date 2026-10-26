import pytest

from app.compaction import compact
from app.events import InvalidEventError, parse_event
from app.merge import plan


def raw(change_type, ids=("001A000000000001AA",), ts=1, changed=(), nulled=(), origin="", **fields):
    header = {
        "entityName": "Account",
        "recordIds": list(ids),
        "changeType": change_type,
        "commitTimestamp": ts,
        "changeOrigin": origin,
        "changedFields": list(changed),
        "nulledFields": list(nulled),
    }
    return {"replayId": ts, "payload": {"ChangeEventHeader": header, **fields}}


def test_create_keeps_all_fields_and_flattens_compound_fields():
    ev = parse_event(raw("CREATE", Name="Acme", BillingAddress={"City": "Portland", "State": "OR"}))
    assert ev.fields == {"Name": "Acme", "BillingAddress_City": "Portland", "BillingAddress_State": "OR"}
    assert ev.changed == {"Name", "BillingAddress_City", "BillingAddress_State"}


def test_update_only_contains_changed_and_nulled_fields():
    ev = parse_event(raw("UPDATE", changed=["Rating", "Industry"], nulled=["Industry"], Rating="Hot", Name="ignored"))
    assert ev.fields == {"Rating": "Hot", "Industry": None}
    assert ev.changed == {"Rating", "Industry"}


def test_compound_field_update_maps_dotted_names():
    ev = parse_event(raw("UPDATE", changed=["BillingAddress.City"], BillingAddress={"City": "Seattle"}))
    assert ev.fields == {"BillingAddress_City": "Seattle"}


@pytest.mark.parametrize(
    "bad",
    [
        {"replayId": 1, "payload": {"Name": "x"}},
        raw("TRUNCATE"),
        raw("CREATE", **{"Name; DROP TABLE": "x"}),
        {"replayId": 1, "payload": {"ChangeEventHeader": {"entityName": "Acc`ount", "changeType": "CREATE"}}},
    ],
)
def test_invalid_events_are_rejected(bad):
    with pytest.raises(InvalidEventError):
        parse_event(bad)


def test_compaction_folds_updates_and_respects_delete_and_undelete():
    rid = "001A000000000001AA"
    events = [
        parse_event(raw("CREATE", ts=1, Name="Acme", Rating="Cold")),
        parse_event(raw("UPDATE", ts=2, changed=["Rating"], Rating="Hot")),
        parse_event(raw("UPDATE", ts=3, changed=["Industry"], Industry="Retail")),
    ]
    change = compact(events)["Account"][0]
    assert change.op == "upsert" and change.commit_ts == 3
    assert change.fields == {"Name": "Acme", "Rating": "Hot", "Industry": "Retail"}

    deleted = compact([*events, parse_event(raw("DELETE", ts=4))])["Account"][0]
    assert deleted.op == "delete" and deleted.fields == {}

    revived = compact([*events, parse_event(raw("DELETE", ts=4)), parse_event(raw("UNDELETE", ts=5, Name="Acme"))])
    assert revived["Account"][0].op == "upsert" and revived["Account"][0].fields == {"Name": "Acme"}
    assert rid == revived["Account"][0].record_id


def test_multi_record_events_fan_out():
    ev = parse_event(raw("UPDATE", ids=["001A000000000001AA", "001A000000000002AA"], changed=["Rating"], Rating="Hot"))
    assert sorted(c.record_id for c in compact([ev])["Account"]) == ["001A000000000001AA", "001A000000000002AA"]


def test_merge_plan_tracks_changed_columns_per_row():
    events = [
        parse_event(raw("UPDATE", ids=["001A000000000001AA"], ts=1, changed=["Rating"], Rating="Hot")),
        parse_event(raw("UPDATE", ids=["001A000000000002AA"], ts=2, changed=["Name"], Name="Globex")),
    ]
    p = plan(compact(events)["Account"])
    assert p.data_columns == ["Name", "Rating"]
    by_id = {r["Id"]: r for r in p.rows}
    assert by_id["001A000000000001AA"]["_changed"] == ",Rating,"
    assert by_id["001A000000000002AA"]["_changed"] == ",Name,"
    assert "instr(s._changed, ',Rating,')" in p.update_set()["Rating"]
