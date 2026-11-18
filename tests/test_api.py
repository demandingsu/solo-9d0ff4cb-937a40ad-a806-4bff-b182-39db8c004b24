"""End-to-end tests for the groundwater sample delivery API.

Run with:  python -m pytest -q
"""
from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("GSB_DB_PATH", str(tmp_path / "test.sqlite3"))
    from app import db

    if hasattr(db, "_initialised"):
        db._initialised = False
    importlib.reload(db)
    from app import domain, main

    importlib.reload(domain)
    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def make_ready_sample(client, *, collector="Li Wei", sampled_at="2026-11-18T02:00:00Z",
                      location="MW-12", lab="Central Water Laboratory", analyte="nitrate-N",
                      concentration=4.2, unit="mg/L", reporting_lab=None, code=None):
    payload = {
        "location": location,
        "collector": collector,
        "sampled_at_utc": sampled_at,
    }
    if reporting_lab is not None:
        payload["reporting_lab"] = reporting_lab
    if code is not None:
        payload["sample_code"] = code
    created = client.post("/samples", json=payload)
    assert created.status_code == 201, created.text
    sample = created.json()
    sid = sample["id"]
    base = datetime.fromisoformat(sampled_at.replace("Z", "+00:00"))
    t1 = (base + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    t2 = (base + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    # custody: collector -> courier -> lab
    r1 = client.post(
        f"/samples/{sid}/handovers",
        json={
            "seq": 1,
            "from_party": collector,
            "to_party": "Field Logistics",
            "handed_at_utc": t1,
        },
    )
    assert r1.status_code == 201, r1.text
    r2 = client.post(
        f"/samples/{sid}/handovers",
        json={
            "seq": 2,
            "from_party": "Field Logistics",
            "to_party": lab,
            "handed_at_utc": t2,
        },
    )
    assert r2.status_code == 201, r2.text
    m = client.post(
        f"/samples/{sid}/measurements",
        json={"lab": lab, "analyte": analyte, "concentration": concentration, "unit": unit},
    )
    assert m.status_code == 201, m.text
    return sample


# ---------------------------------------------------------------------------
# Registration and custody basics
# ---------------------------------------------------------------------------

def test_register_sample_and_generate_code(client):
    r = client.post(
        "/samples",
        json={"location": "MW-1", "collector": "A", "sampled_at_utc": "2026-11-18T01:00:00Z"},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["sample_code"] == "SMP-20261118-0001"
    assert body["sample_date"] == "2026-11-18"
    assert body["sampled_at_utc"] == "2026-11-18T01:00:00.000000Z"
    assert body["eligible_for_delivery"] is False


def test_sample_requires_utc_timestamp(client):
    r = client.post(
        "/samples",
        json={"location": "MW-1", "collector": "A", "sampled_at_utc": "2026-11-18T01:00:00"},
    )
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "timestamp_missing_timezone"


def test_handover_out_of_order_and_idempotent_retry(client):
    s = client.post(
        "/samples",
        json={"location": "MW-1", "collector": "Collector", "sampled_at_utc": "2026-11-18T01:00:00Z"},
    ).json()
    sid = s["id"]
    # report seq 2 first
    r2 = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 2, "from_party": "Courier", "to_party": "Lab", "handed_at_utc": "2026-11-18T04:00:00Z"},
    )
    assert r2.status_code == 201, r2.text
    assert r2.json()["chain"]["ok"] is False  # gap at seq 1
    r1 = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Collector", "to_party": "Courier", "handed_at_utc": "2026-11-18T02:00:00Z"},
    )
    assert r1.status_code == 201, r1.text
    assert r1.json()["chain"]["ok"] is True
    # identical retry is idempotent
    again = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Collector", "to_party": "Courier", "handed_at_utc": "2026-11-18T02:00:00Z"},
    )
    assert again.status_code == 200
    assert again.json()["idempotent"] is True
    # different content is refused without explicit correction
    changed = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Collector", "to_party": "Courier", "handed_at_utc": "2026-11-18T03:00:00Z"},
    )
    assert changed.status_code == 409
    assert changed.json()["detail"]["code"] == "handover_already_exists"


def test_handover_rules(client):
    s = client.post(
        "/samples",
        json={"location": "MW-1", "collector": "Collector", "sampled_at_utc": "2026-11-18T01:00:00Z"},
    ).json()
    sid = s["id"]
    # time must be after sampling
    r = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Collector", "to_party": "Courier", "handed_at_utc": "2026-11-18T00:00:00Z"},
    )
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "handover_not_after_sampling"
    # seq 1 must leave from the collector
    r = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Someone", "to_party": "Courier", "handed_at_utc": "2026-11-18T02:00:00Z"},
    )
    assert r.status_code == 422
    # chain link mismatch
    client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Collector", "to_party": "Courier", "handed_at_utc": "2026-11-18T02:00:00Z"},
    )
    r = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 2, "from_party": "Wrong", "to_party": "Lab", "handed_at_utc": "2026-11-18T03:00:00Z"},
    )
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "handover_party_mismatch"


def test_correction_requires_reason_and_is_audited(client):
    sample = make_ready_sample(client)
    sid = sample["id"]
    r = client.patch(
        f"/samples/{sid}/handovers/1",
        json={"from_party": "Li Wei", "to_party": "Field Logistics", "handed_at_utc": "2026-11-18T02:30:00Z"},
    )
    assert r.status_code == 422  # reason is required by the schema
    r = client.patch(
        f"/samples/{sid}/handovers/1",
        json={
            "from_party": "Li Wei",
            "to_party": "Field Logistics",
            "handed_at_utc": "2026-11-18T02:45:00Z",
            "reason": "field log says 02:45",
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["substantive"] is True
    rev = client.get(f"/samples/{sid}/handovers/revisions").json()["revisions"]
    assert [x["action"] for x in rev] == ["create", "create", "correct"]
    assert rev[-1]["reason"] == "field log says 02:45"


# ---------------------------------------------------------------------------
# Delivery and revocation
# ---------------------------------------------------------------------------

def test_delivery_only_includes_eligible_samples(client):
    good = make_ready_sample(client, code="GOOD")
    # second sample still missing measurements
    bad = client.post(
        "/samples",
        json={"location": "MW-2", "collector": "Z", "sampled_at_utc": "2026-11-18T03:00:00Z", "sample_code": "BAD"},
    ).json()
    sid = bad["id"]
    client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "Z", "to_party": "Central Water Laboratory", "handed_at_utc": "2026-11-18T04:00:00Z"},
    )
    r = client.post("/deliveries", json={"sample_date": "2026-11-18"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["delivery_no"] == "DLV-20261118-001"
    assert body["sample_count"] == 1
    assert body["samples"][0]["sample_id"] == good["id"]
    assert body["excluded_samples"][0]["sample_id"] == sid

    # no eligible samples -> no delivery number consumed
    r = client.post("/deliveries", json={"sample_date": "2026-11-19"})
    assert r.status_code == 409
    assert client.get("/deliveries", params={"sample_date": "2026-11-19"}).json()["total"] == 0


def test_delivery_number_is_per_sampling_date(client):
    make_ready_sample(client, sampled_at="2026-11-18T02:00:00Z", code="A")
    make_ready_sample(client, sampled_at="2026-11-19T02:00:00Z", code="B")
    d1 = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    d2 = client.post("/deliveries", json={"sample_date": "2026-11-19"}).json()
    d3 = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    assert d1["delivery_no"] == "DLV-20261118-001"
    assert d2["delivery_no"] == "DLV-20261119-001"
    assert d3["delivery_no"] == "DLV-20261118-002"


def test_substantive_handover_correction_withdraws_only_that_sample_and_stays_withdrawn(client):
    first = make_ready_sample(client, code="FIRST")
    second = make_ready_sample(client, code="SECOND", location="MW-2", sampled_at="2026-11-18T05:00:00Z")
    delivery = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    no = delivery["delivery_no"]
    assert delivery["sample_count"] == 2

    # correct handover 1 of first sample
    corr = client.patch(
        f"/samples/{first['id']}/handovers/1",
        json={
            "from_party": "Li Wei",
            "to_party": "Field Logistics",
            "handed_at_utc": "2026-11-18T02:45:00Z",
            "reason": "field log correction",
        },
    )
    assert corr.status_code == 200, corr.text
    assert corr.json()["revoked_deliveries"][0]["delivery_no"] == no

    # old delivery still provides the other sample, explains withdrawal of first
    old = client.get(f"/deliveries/{no}").json()
    assert old["sample_count"] == 1
    assert old["samples"][0]["sample_id"] == second["id"]
    assert old["revoked_count"] == 1
    revoked = old["revoked_samples"][0]
    assert revoked["sample_id"] == first["id"]
    assert "substantively corrected" in revoked["revoke_reason"]
    assert "02:30" in revoked["revoke_reason"] and "02:45" in revoked["revoke_reason"]

    # repairing the chain does NOT restore the old delivery
    repair = client.patch(
        f"/samples/{first['id']}/handovers/1",
        json={
            "from_party": "Li Wei",
            "to_party": "Field Logistics",
            "handed_at_utc": "2026-11-18T02:30:00Z",
            "reason": "restore original record",
        },
    )
    assert repair.status_code == 200, repair.text
    old2 = client.get(f"/deliveries/{no}").json()
    assert old2["sample_count"] == 1
    assert old2["revoked_count"] == 1

    # a fresh delivery includes the repaired sample again
    fresh = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    assert fresh["delivery_no"] == "DLV-20261118-002"
    assert {s["sample_id"] for s in fresh["samples"]} == {first["id"], second["id"]}
    assert fresh["revoked_count"] == 0


def test_correction_leaving_broken_chain_keeps_sample_out_of_delivery(client):
    sample = make_ready_sample(client, code="ONE")
    # break the chain by changing seq1 recipient (allowed as explicit correction)
    r = client.patch(
        f"/samples/{sample['id']}/handovers/1",
        json={
            "from_party": "Li Wei",
            "to_party": "Different Courier",
            "handed_at_utc": "2026-11-18T02:30:00Z",
            "reason": "courier identity corrected",
        },
    )
    assert r.status_code == 200, r.text
    assert r.json()["chain"]["ok"] is False
    d = client.post("/deliveries", json={"sample_date": "2026-11-18"})
    assert d.status_code == 409
    detail = d.json()["detail"]
    assert detail["code"] == "no_eligible_samples"


def test_delivery_terminal_must_be_reporting_lab(client):
    s = client.post(
        "/samples",
        json={
            "location": "MW-9",
            "collector": "A",
            "sampled_at_utc": "2026-11-18T01:00:00Z",
            "reporting_lab": "Reporting Lab",
        },
    ).json()
    sid = s["id"]
    client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": "A", "to_party": "Wrong Lab", "handed_at_utc": "2026-11-18T02:00:00Z"},
    )
    client.post(
        f"/samples/{sid}/measurements",
        json={"lab": "Reporting Lab", "analyte": "arsenic", "concentration": 0.002, "unit": "mg/L"},
    )
    d = client.post("/deliveries", json={"sample_date": "2026-11-18"})
    assert d.status_code == 409
    problems = d.json()["detail"]["excluded"][0]["problems"]
    assert any("terminal party" in p for p in problems)


def test_measurement_idempotency_and_key_conflict(client):
    s = client.post(
        "/samples",
        json={"location": "MW-1", "collector": "A", "sampled_at_utc": "2026-11-18T01:00:00Z"},
    ).json()
    sid = s["id"]
    m = {"lab": "Lab", "analyte": "nitrate-N", "concentration": 2.0, "unit": "mg/L"}
    assert client.post(f"/samples/{sid}/measurements", json=m).status_code == 201
    again = client.post(f"/samples/{sid}/measurements", json=m)
    assert again.status_code == 200 and again.json()["idempotent"] is True
    clash = client.post(f"/samples/{sid}/measurements", json={**m, "concentration": 2.1})
    assert clash.status_code == 409


def test_openapi_and_docs(client):
    assert client.get("/docs").status_code == 200
    schema = client.get("/openapi.json").json()
    assert schema["info"]["title"]
    for path in ["/samples", "/samples/{sample_id}/handovers", "/deliveries", "/deliveries/{delivery_no}"]:
        assert path in schema["paths"]


def test_stale_expected_revision_token_blocks_publish_without_consuming_number(client):
    first = make_ready_sample(client, code="TOK1")
    d1 = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    token = d1["revision_token"]

    # change committed state, then try to publish with the stale token
    corr = client.patch(
        f"/samples/{first['id']}/handovers/2",
        json={
            "from_party": "Field Logistics",
            "to_party": "Central Water Laboratory",
            "handed_at_utc": "2026-11-18T05:00:00Z",
            "reason": "receipt time corrected",
        },
    )
    assert corr.status_code == 200, corr.text
    blocked = client.post(
        "/deliveries",
        json={"sample_date": "2026-11-18", "expected_revision_token": token},
    )
    assert blocked.status_code == 409
    assert blocked.json()["detail"]["code"] == "delivery_state_changed"
    # no number consumed by the rejected publish
    ok = client.post("/deliveries", json={"sample_date": "2026-11-18"})
    assert ok.status_code == 201, ok.text
    assert ok.json()["delivery_no"] == "DLV-20261118-002"


def test_measurement_correction_withdraws_sample_with_reason(client):
    s = make_ready_sample(client, code="MEAS")
    d = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    mid = client.get(f"/samples/{s['id']}/measurements").json()["measurements"][0]["id"]
    corr = client.patch(
        f"/samples/{s['id']}/measurements/{mid}",
        json={
            "lab": "Central Water Laboratory",
            "analyte": "nitrate-N",
            "concentration": 4.0,
            "unit": "mg/L",
            "reason": "recalculated from lab report",
        },
    )
    assert corr.status_code == 200, corr.text
    assert corr.json()["revoked_deliveries"][0]["delivery_no"] == d["delivery_no"]
    old = client.get(f"/deliveries/{d['delivery_no']}").json()
    assert old["sample_count"] == 0 and old["revoked_count"] == 1
    assert "recalculated from lab report" in old["revoked_samples"][0]["revoke_reason"]
    # sample view lists full delivery history incl. withdrawal
    view = client.get(f"/samples/{s['id']}").json()
    assert view["deliveries"][0]["delivery_no"] == d["delivery_no"]
    assert view["deliveries"][0]["revoked_at_utc"] is not None


def test_adding_handover_after_delivery_withdraws_snapshot(client):
    s = make_ready_sample(client, code="EXTRA")
    d = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    add = client.post(
        f"/samples/{s['id']}/handovers",
        json={"seq": 3, "from_party": "Central Water Laboratory", "to_party": "Archive", "handed_at_utc": "2026-11-18T06:00:00Z"},
    )
    assert add.status_code == 201, add.text
    assert add.json()["revoked_deliveries"][0]["delivery_no"] == d["delivery_no"]
    old = client.get(f"/deliveries/{d['delivery_no']}").json()
    assert old["sample_count"] == 0 and old["revoked_count"] == 1


def test_patch_with_unchanged_content_is_not_substantive(client):
    s = make_ready_sample(client, code="SAME")
    d = client.post("/deliveries", json={"sample_date": "2026-11-18"}).json()
    r = client.patch(
        f"/samples/{s['id']}/handovers/1",
        json={
            "from_party": "Li Wei",
            "to_party": "Field Logistics",
            "handed_at_utc": "2026-11-18T02:30:00Z",
            "reason": "no-op re-submission",
        },
    )
    assert r.status_code == 200
    assert r.json()["substantive"] is False
    assert client.get(f"/deliveries/{d['delivery_no']}").json()["sample_count"] == 1


def test_failed_correction_leaves_no_partial_state(client):
    s = make_ready_sample(client, code="ROLL")
    before = client.get(f"/samples/{s['id']}").json()
    # invalid correction: time earlier than sampling -> rejected
    r = client.patch(
        f"/samples/{s['id']}/handovers/2",
        json={
            "from_party": "Field Logistics",
            "to_party": "Central Water Laboratory",
            "handed_at_utc": "2026-11-18T00:30:00Z",
            "reason": "bad",
        },
    )
    assert r.status_code == 422
    after = client.get(f"/samples/{s['id']}").json()
    assert before["handovers"] == after["handovers"]
    assert after["chain"]["ok"] is True
    revs = client.get(f"/samples/{s['id']}/handovers/revisions").json()["revisions"]
    assert [x["action"] for x in revs] == ["create", "create"]
