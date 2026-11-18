"""End-to-end tests for the well removal-mass accounting API.

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


LAB = "Central Water Laboratory"
ANALYTE = "nitrate-N"


def add_reading(client, well, read_at, m3):
    r = client.post(f"/wells/{well}/readings", json={"read_at_utc": read_at, "cumulative_m3": m3})
    assert r.status_code == 201, r.text
    return r.json()["reading"]


def make_sample(client, well, sampled_at, concentration, *, lab=LAB, analyte=ANALYTE,
                unit="mg/L", collector="Li Wei", code=None):
    payload = {"location": well, "collector": collector, "sampled_at_utc": sampled_at}
    if code is not None:
        payload["sample_code"] = code
    created = client.post("/samples", json=payload)
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    base = datetime.fromisoformat(sampled_at.replace("Z", "+00:00"))
    t1 = (base + timedelta(minutes=30)).isoformat().replace("+00:00", "Z")
    t2 = (base + timedelta(hours=2)).isoformat().replace("+00:00", "Z")
    r1 = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 1, "from_party": collector, "to_party": "Field Logistics", "handed_at_utc": t1},
    )
    assert r1.status_code == 201, r1.text
    r2 = client.post(
        f"/samples/{sid}/handovers",
        json={"seq": 2, "from_party": "Field Logistics", "to_party": lab, "handed_at_utc": t2},
    )
    assert r2.status_code == 201, r2.text
    m = client.post(
        f"/samples/{sid}/measurements",
        json={"lab": lab, "analyte": analyte, "concentration": concentration, "unit": unit},
    )
    assert m.status_code == 201, m.text
    return created.json()


def calculate(client, well, day, *, lab=LAB, analyte=ANALYTE, carry_over=None, expect=201):
    body = {"lab": lab, "analyte": analyte, "day": day}
    if carry_over is not None:
        body["carry_over_hours"] = carry_over
    r = client.post(f"/wells/{well}/accounting", json=body)
    assert r.status_code == expect, r.text
    return r.json()


# ---------------------------------------------------------------------------
# Readings
# ---------------------------------------------------------------------------

def test_reading_register_idempotent_conflict_and_correct(client):
    r = client.post("/wells/MW-1/readings", json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": 100.0})
    assert r.status_code == 201, r.text
    rid = r.json()["reading"]["id"]

    again = client.post("/wells/MW-1/readings", json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": 100.0})
    assert again.status_code == 200 and again.json()["idempotent"] is True

    clash = client.post("/wells/MW-1/readings", json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": 101.0})
    assert clash.status_code == 409
    assert clash.json()["detail"]["code"] == "reading_already_exists"

    # correction requires a reason (schema) and is audited
    no_reason = client.patch(f"/wells/MW-1/readings/{rid}", json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": 101.0})
    assert no_reason.status_code == 422
    corr = client.patch(
        f"/wells/MW-1/readings/{rid}",
        json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": 101.0, "reason": "meter logbook"},
    )
    assert corr.status_code == 200, corr.text
    assert corr.json()["substantive"] is True
    revs = client.get("/wells/MW-1/readings/revisions").json()["revisions"]
    assert [x["action"] for x in revs] == ["create", "correct"]
    assert revs[-1]["reason"] == "meter logbook"
    assert revs[-1]["prev"]["cumulative_m3"] == 100.0

    # timestamp collision with another reading is refused
    add_reading(client, "MW-1", "2026-11-18T20:00:00Z", 200.0)
    clash2 = client.patch(
        f"/wells/MW-1/readings/{rid}",
        json={"read_at_utc": "2026-11-18T20:00:00Z", "cumulative_m3": 101.0, "reason": "move"},
    )
    assert clash2.status_code == 409
    assert clash2.json()["detail"]["code"] == "reading_key_conflict"


def test_reading_validation_and_rollback_flag(client):
    bad = client.post("/wells/MW-2/readings", json={"read_at_utc": "2026-11-18T08:00:00", "cumulative_m3": 1.0})
    assert bad.status_code == 422
    assert bad.json()["detail"]["code"] == "timestamp_missing_timezone"
    neg = client.post("/wells/MW-2/readings", json={"read_at_utc": "2026-11-18T08:00:00Z", "cumulative_m3": -1.0})
    assert neg.status_code == 422
    assert neg.json()["detail"]["code"] == "invalid_reading"

    add_reading(client, "MW-2", "2026-11-18T00:00:00Z", 100.0)
    add_reading(client, "MW-2", "2026-11-18T12:00:00Z", 90.0)  # rollback is data, not an error
    items = client.get("/wells/MW-2/readings").json()["readings"]
    assert [i["rollback_from_previous"] for i in items] == [False, True]


def test_wells_listing_unites_readings_and_locations(client):
    add_reading(client, "MW-3", "2026-11-18T00:00:00Z", 1.0)
    make_sample(client, "MW-4", "2026-11-18T01:00:00Z", 1.0)
    wells = {w["well_no"]: w for w in client.get("/wells").json()["items"]}
    assert wells["MW-3"]["reading_count"] == 1 and wells["MW-3"]["sample_count"] == 0
    assert wells["MW-4"]["sample_count"] == 1


# ---------------------------------------------------------------------------
# Accounting results
# ---------------------------------------------------------------------------

def test_accounting_basic_day_full_coverage(client):
    add_reading(client, "MW-A", "2026-11-18T00:00:00Z", 100.0)
    add_reading(client, "MW-A", "2026-11-18T12:00:00Z", 160.0)
    add_reading(client, "MW-A", "2026-11-19T00:00:00Z", 220.0)
    make_sample(client, "MW-A", "2026-11-17T08:00:00Z", 4.2)

    run = calculate(client, "MW-A", "2026-11-18")["run"]
    assert run["status"] == "current"
    assert run["removal_mass_g"] == pytest.approx(504.0)  # 120 m3 * 4.2 mg/L
    assert run["complete"] is True
    result = run["result"]
    assert result["pending_intervals"] == []
    assert len(result["segments"]) == 2
    assert {s["reading_ids"][0] for s in result["segments"]}  # reading provenance
    assert result["sources"]["samples"][0]["used_in_segments"] is True
    assert len(result["sources"]["readings"]) == 3


def test_accounting_cross_day_split(client):
    for t, v in [
        ("2026-11-18T00:00:00Z", 940.0),
        ("2026-11-18T20:00:00Z", 1000.0),
        ("2026-11-19T04:00:00Z", 1060.0),
        ("2026-11-19T20:00:00Z", 1120.0),
        ("2026-11-20T04:00:00Z", 1180.0),
    ]:
        add_reading(client, "MW-B", t, v)
    make_sample(client, "MW-B", "2026-11-17T00:00:00Z", 5.0)

    d18 = calculate(client, "MW-B", "2026-11-18")["run"]
    d19 = calculate(client, "MW-B", "2026-11-19")["run"]
    assert d18["removal_mass_g"] == pytest.approx(450.0)  # 60 + 30 m3
    assert d19["removal_mass_g"] == pytest.approx(600.0)  # 30 + 60 + 30 m3
    assert d18["complete"] is True and d19["complete"] is True


def test_accounting_new_sample_switches_basis(client):
    add_reading(client, "MW-C", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-C", "2026-11-19T00:00:00Z", 120.0)
    make_sample(client, "MW-C", "2026-11-17T00:00:00Z", 4.0, code="S-OLD")
    make_sample(client, "MW-C", "2026-11-18T12:00:00Z", 6.0, code="S-NEW")

    run = calculate(client, "MW-C", "2026-11-18")["run"]
    assert run["removal_mass_g"] == pytest.approx(600.0)  # 60*4 + 60*6
    used = {s["sample_code"] for s in run["result"]["segments"]}
    assert used == {"S-OLD", "S-NEW"}


def test_accounting_carry_over_expiry(client):
    for t, v in [("2026-11-18T00:00:00Z", 0.0), ("2026-11-19T00:00:00Z", 48.0), ("2026-11-20T00:00:00Z", 96.0)]:
        add_reading(client, "MW-D", t, v)
    make_sample(client, "MW-D", "2026-11-18T06:00:00Z", 3.0)

    d18 = calculate(client, "MW-D", "2026-11-18", carry_over=24.0)["run"]
    assert d18["removal_mass_g"] == pytest.approx(108.0)  # 36 m3 * 3 mg/L after 06:00
    assert any("no_valid_concentration" in p["reasons"] for p in d18["result"]["pending_intervals"])

    d19 = calculate(client, "MW-D", "2026-11-19", carry_over=24.0)["run"]
    assert d19["removal_mass_g"] == pytest.approx(36.0)  # only 00:00-06:00 still carried
    expired = [p for p in d19["result"]["pending_intervals"] if "carry_over_expired" in p["reasons"]]
    assert expired and expired[0]["start_utc"].startswith("2026-11-19T06:00:00")
    assert d19["complete"] is False


def test_accounting_rollback_and_long_interval_are_pending_not_zero(client):
    add_reading(client, "MW-E", "2026-11-18T00:00:00Z", 100.0)
    add_reading(client, "MW-E", "2026-11-18T12:00:00Z", 90.0)
    add_reading(client, "MW-E", "2026-11-19T00:00:00Z", 150.0)
    make_sample(client, "MW-E", "2026-11-17T00:00:00Z", 2.0)
    run = calculate(client, "MW-E", "2026-11-18")["run"]
    assert run["removal_mass_g"] == pytest.approx(120.0)  # only the good 60 m3 interval
    pend = run["result"]["pending_intervals"]
    assert any("reading_rollback" in p["reasons"] for p in pend)
    assert run["complete"] is False

    add_reading(client, "MW-F", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-F", "2026-11-19T12:00:00Z", 100.0)  # 36 h apart
    make_sample(client, "MW-F", "2026-11-17T00:00:00Z", 2.0)
    run2 = calculate(client, "MW-F", "2026-11-18")["run"]
    assert run2["removal_mass_g"] is None
    assert any("interval_exceeds_one_day" in p["reasons"] for p in run2["result"]["pending_intervals"])


def test_accounting_missing_or_wrong_unit_concentration(client):
    add_reading(client, "MW-G", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-G", "2026-11-19T00:00:00Z", 10.0)
    run = calculate(client, "MW-G", "2026-11-18")["run"]
    assert run["removal_mass_g"] is None
    assert any("no_valid_concentration" in p["reasons"] for p in run["result"]["pending_intervals"])

    make_sample(client, "MW-G", "2026-11-17T00:00:00Z", 4200.0, unit="µg/L")
    run2 = calculate(client, "MW-G", "2026-11-18")["run"]
    assert run2["removal_mass_g"] is None
    src = run2["result"]["sources"]["samples"][0]
    assert src["qualified"] is False and src["unit"] == "µg/L"


def test_accounting_same_timestamp_conflict(client):
    add_reading(client, "MW-H", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-H", "2026-11-19T00:00:00Z", 10.0)
    make_sample(client, "MW-H", "2026-11-17T00:00:00Z", 4.0, code="C-1")
    make_sample(client, "MW-H", "2026-11-17T00:00:00Z", 5.0, code="C-2")
    run = calculate(client, "MW-H", "2026-11-18")["run"]
    assert run["removal_mass_g"] is None
    assert any("conflicting_concentrations" in p["reasons"] for p in run["result"]["pending_intervals"])


def test_accounting_uncovered_and_partially_covered_days(client):
    make_sample(client, "MW-I", "2026-11-17T00:00:00Z", 4.0)
    run = calculate(client, "MW-I", "2026-11-18")["run"]
    assert run["removal_mass_g"] is None  # unknown, not zero
    assert run["complete"] is False
    pend = run["result"]["pending_intervals"]
    assert any("no_reading_coverage" in p["reasons"] for p in pend)

    add_reading(client, "MW-J", "2026-11-18T06:00:00Z", 10.0)
    add_reading(client, "MW-J", "2026-11-18T18:00:00Z", 34.0)
    make_sample(client, "MW-J", "2026-11-17T00:00:00Z", 2.0)
    run2 = calculate(client, "MW-J", "2026-11-18")["run"]
    assert run2["removal_mass_g"] == pytest.approx(48.0)  # 24 m3 * 2 mg/L
    cov = [p for p in run2["result"]["pending_intervals"] if "no_reading_coverage" in p["reasons"]]
    assert len(cov) == 2  # 00:00-06:00 and 18:00-24:00


def test_accounting_unknown_well_404(client):
    r = client.post("/wells/NOPE/accounting", json={"lab": LAB, "analyte": ANALYTE, "day": "2026-11-18"})
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "well_not_found"


# ---------------------------------------------------------------------------
# Run lifecycle: expire on writes, recalculate to current
# ---------------------------------------------------------------------------

def test_reading_correction_expires_and_recalculation_supersedes(client):
    add_reading(client, "MW-K", "2026-11-18T00:00:00Z", 0.0)
    second = add_reading(client, "MW-K", "2026-11-19T00:00:00Z", 120.0)
    make_sample(client, "MW-K", "2026-11-17T00:00:00Z", 4.0)
    run1 = calculate(client, "MW-K", "2026-11-18")["run"]
    assert run1["removal_mass_g"] == pytest.approx(480.0)

    # idempotent recalculation reuses the current run
    again = client.post(
        "/wells/MW-K/accounting", json={"lab": LAB, "analyte": ANALYTE, "day": "2026-11-18"}
    )
    assert again.status_code == 200
    assert again.json()["recomputed"] is False
    assert again.json()["run"]["id"] == run1["id"]

    # correcting the reading expires the run (kept, with a reason)
    corr = client.patch(
        f"/wells/MW-K/readings/{second['id']}",
        json={"read_at_utc": "2026-11-19T00:00:00Z", "cumulative_m3": 240.0, "reason": "meter misread"},
    )
    assert corr.status_code == 200, corr.text
    assert corr.json()["expired_accounting"][0]["run_id"] == run1["id"]
    old = client.get(f"/wells/MW-K/accounting/{run1['id']}").json()
    assert old["status"] == "expired"
    assert "meter misread" in old["expire_reason"]

    # recalculation becomes the new current run; the expired one is kept
    run2 = calculate(client, "MW-K", "2026-11-18")["run"]
    assert run2["id"] != run1["id"]
    assert run2["status"] == "current"
    assert run2["removal_mass_g"] == pytest.approx(960.0)
    runs = client.get("/wells/MW-K/accounting", params={"day": "2026-11-18"}).json()["items"]
    by_id = {r["id"]: r["status"] for r in runs}
    assert by_id[run1["id"]] == "expired"
    assert by_id[run2["id"]] == "current"
    current = client.get("/wells/MW-K/accounting", params={"run_status": "current"}).json()
    assert current["total"] == 1


def test_late_sample_insertion_expires_and_recalculates(client):
    add_reading(client, "MW-L", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-L", "2026-11-19T00:00:00Z", 120.0)
    make_sample(client, "MW-L", "2026-11-17T00:00:00Z", 4.0, code="L-1")
    run1 = calculate(client, "MW-L", "2026-11-18")["run"]
    assert run1["removal_mass_g"] == pytest.approx(480.0)

    # late-reported sample (补报) sampled mid-day expires the run
    created = make_sample(client, "MW-L", "2026-11-18T12:00:00Z", 6.0, code="L-2")
    assert created["expired_accounting"][0]["run_id"] == run1["id"]
    assert client.get(f"/wells/MW-K/accounting/{run1['id']}").status_code == 404  # different well
    assert client.get(f"/wells/MW-L/accounting/{run1['id']}").json()["status"] == "expired"

    run2 = calculate(client, "MW-L", "2026-11-18")["run"]
    assert run2["removal_mass_g"] == pytest.approx(600.0)  # 60*4 + 60*6
    assert run2["status"] == "current"


def test_measurement_and_handover_corrections_expire_runs(client):
    add_reading(client, "MW-M", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-M", "2026-11-19T00:00:00Z", 120.0)
    sample = make_sample(client, "MW-M", "2026-11-17T00:00:00Z", 4.0)
    run1 = calculate(client, "MW-M", "2026-11-18")["run"]

    mid = client.get(f"/samples/{sample['id']}/measurements").json()["measurements"][0]["id"]
    corr = client.patch(
        f"/samples/{sample['id']}/measurements/{mid}",
        json={"lab": LAB, "analyte": ANALYTE, "concentration": 5.0, "unit": "mg/L", "reason": "lab report reissued"},
    )
    assert corr.status_code == 200, corr.text
    assert corr.json()["expired_accounting"][0]["run_id"] == run1["id"]
    run2 = calculate(client, "MW-M", "2026-11-18")["run"]
    assert run2["removal_mass_g"] == pytest.approx(600.0)

    # breaking custody makes the concentration unqualified -> pending, not zero
    corr2 = client.patch(
        f"/samples/{sample['id']}/handovers/2",
        json={
            "from_party": "Field Logistics",
            "to_party": "Different Lab",
            "handed_at_utc": "2026-11-17T04:00:00Z",
            "reason": "receiver corrected",
        },
    )
    assert corr2.status_code == 200, corr2.text
    assert corr2.json()["expired_accounting"][0]["run_id"] == run2["id"]
    run3 = calculate(client, "MW-M", "2026-11-18")["run"]
    assert run3["removal_mass_g"] is None
    assert any("no_valid_concentration" in p["reasons"] for p in run3["result"]["pending_intervals"])


def test_unaffected_days_and_wells_keep_current_runs(client):
    for t, v in [("2026-11-18T00:00:00Z", 0.0), ("2026-11-19T00:00:00Z", 10.0), ("2026-11-20T00:00:00Z", 20.0)]:
        add_reading(client, "MW-N", t, v)
    make_sample(client, "MW-N", "2026-11-17T00:00:00Z", 1.0)
    make_sample(client, "MW-O", "2026-11-17T00:00:00Z", 1.0)
    add_reading(client, "MW-O", "2026-11-18T00:00:00Z", 0.0)
    add_reading(client, "MW-O", "2026-11-19T00:00:00Z", 10.0)
    d18 = calculate(client, "MW-N", "2026-11-18")["run"]
    d19 = calculate(client, "MW-N", "2026-11-19")["run"]
    other = calculate(client, "MW-O", "2026-11-18")["run"]

    # a new reading on 2026-11-19 only touches the intervals around it
    add_reading(client, "MW-N", "2026-11-19T12:00:00Z", 15.0)
    assert client.get(f"/wells/MW-N/accounting/{d18['id']}").json()["status"] == "current"
    assert client.get(f"/wells/MW-N/accounting/{d19['id']}").json()["status"] == "expired"
    assert client.get(f"/wells/MW-O/accounting/{other['id']}").json()["status"] == "current"


def test_openapi_lists_accounting_paths(client):
    schema = client.get("/openapi.json").json()
    for path in [
        "/wells",
        "/wells/{well_no}/readings",
        "/wells/{well_no}/readings/{reading_id}",
        "/wells/{well_no}/readings/revisions",
        "/wells/{well_no}/accounting",
        "/wells/{well_no}/accounting/{run_id}",
    ]:
        assert path in schema["paths"], path
