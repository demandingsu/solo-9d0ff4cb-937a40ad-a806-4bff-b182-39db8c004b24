"""Accounting self-test: cross-day split, late sample insertion, correction interleaving.

Runs against a live HTTP server (default http://127.0.0.1:8000) and asserts:

1. Cross-day: a metering interval spanning UTC midnight splits its volume
   between the two days, and both days come out complete.
2. Late sample insertion (补报): registering a sample sampled earlier expires
   the affected run; recalculation switches the concentration basis mid-day.
3. Correction interleaving: racing reading corrections, a measurement
   correction and recalculations never produce a 5xx, never leave more than
   one current run per (well, lab, analyte, day), and the settled
   recalculation matches a hand-computed result from the final committed
   state.  Expired/superseded runs are retained for audit.

Usage:
    python scripts/accounting_selftest.py --base-url http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import urllib.error
import urllib.request
import uuid

LAB = "selftest-lab"
ANALYTE = "nitrate-N"


def call(base, method, path, payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(
        base.rstrip("/") + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=20) as resp:
            return resp.status, json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read() or b"null")


def must(base, method, path, payload=None, expect=(200, 201)):
    status, body = call(base, method, path, payload)
    assert status in expect, (method, path, status, body)
    return body


def make_sample(base, well, sampled_at, conc, code):
    sample = must(base, "POST", "/samples", {
        "location": well, "collector": "selftest", "sampled_at_utc": sampled_at, "sample_code": code,
    })
    sid = sample["id"]
    must(base, "POST", f"/samples/{sid}/handovers",
         {"seq": 1, "from_party": "selftest", "to_party": "courier", "handed_at_utc": sampled_at[:11] + "12:30:00Z"})
    must(base, "POST", f"/samples/{sid}/handovers",
         {"seq": 2, "from_party": "courier", "to_party": LAB, "handed_at_utc": sampled_at[:11] + "13:00:00Z"})
    must(base, "POST", f"/samples/{sid}/measurements",
         {"lab": LAB, "analyte": ANALYTE, "concentration": conc, "unit": "mg/L"})
    return sid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.base_url
    tag = uuid.uuid4().hex[:8]
    well = f"STW-{tag}"

    # -- setup: readings crossing UTC midnight, one qualified sample --------
    # intervals: [00:00,20:00) 60 m3 | [20:00,04:00) 32 m3 | [04:00,20:00) 64 m3 | [20:00,04:00) 32 m3
    readings = {}
    for t, v in [
        ("2026-11-20T00:00:00Z", 500.0),
        ("2026-11-20T20:00:00Z", 560.0),
        ("2026-11-21T04:00:00Z", 592.0),
        ("2026-11-21T20:00:00Z", 656.0),
        ("2026-11-22T04:00:00Z", 688.0),
    ]:
        body = must(base, "POST", f"/wells/{well}/readings", {"read_at_utc": t, "cumulative_m3": v})
        readings[t] = body["reading"]["id"]
    make_sample(base, well, "2026-11-19T12:00:00Z", 5.0, f"ST-S1-{tag}")

    # -- 1) cross-day accounting ---------------------------------------------
    d20 = must(base, "POST", f"/wells/{well}/accounting",
               {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-20"})["run"]
    d21 = must(base, "POST", f"/wells/{well}/accounting",
               {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-21"})["run"]
    # day 20: 60 m3 + (4/8)*32 m3 = 76 m3 * 5 mg/L = 380 g
    assert abs(d20["removal_mass_g"] - 380.0) < 1e-6, d20["removal_mass_g"]
    # day 21: (4/8)*32 + 64 + (4/8)*32 = 96 m3 * 5 mg/L = 480 g
    assert abs(d21["removal_mass_g"] - 480.0) < 1e-6, d21["removal_mass_g"]
    assert d20["complete"] is True and d21["complete"] is True

    # -- 2) late sample insertion expires and shifts the basis ---------------
    created = make_sample(base, well, "2026-11-21T06:00:00Z", 8.0, f"ST-S2-{tag}")
    expired_ids = {e["run_id"] for e in created["expired_accounting"]}
    assert d21["id"] in expired_ids, created["expired_accounting"]
    assert d20["id"] not in expired_ids  # day 20 ends before the new sample
    status, old = call(base, "GET", f"/wells/{well}/accounting/{d21['id']}")
    assert status == 200 and old["status"] == "expired" and old["expire_reason"], old

    d21b = must(base, "POST", f"/wells/{well}/accounting",
                {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-21"})["run"]
    # day 21 recalculated: [00:00,06:00) @5 -> (16 + 8) m3; [06:00,24:00) @8 -> (56 + 16) m3
    # = 24*5 + 72*8 = 120 + 576 = 696 g
    assert abs(d21b["removal_mass_g"] - 696.0) < 1e-6, d21b["removal_mass_g"]
    assert d21b["status"] == "current"

    # -- 3) race corrections against recalculation ---------------------------
    sid = None
    status, samples = call(base, "GET", f"/samples?limit=1000")
    assert status == 200
    for s in samples["items"]:
        if s["sample_code"] == f"ST-S2-{tag}":
            sid = s["id"]
    assert sid is not None
    status, measurements = call(base, "GET", f"/samples/{sid}/measurements")
    mid = measurements["measurements"][0]["id"]

    tasks = [
        ("PATCH", f"/wells/{well}/readings/{readings['2026-11-21T04:00:00Z']}",
         {"read_at_utc": "2026-11-21T04:00:00Z", "cumulative_m3": 600.0, "reason": "meter misread"}),
        ("PATCH", f"/samples/{sid}/measurements/{mid}",
         {"lab": LAB, "analyte": ANALYTE, "concentration": 8.4, "unit": "mg/L", "reason": "lab reissue"}),
        ("POST", f"/wells/{well}/accounting", {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-21"}),
        ("PATCH", f"/wells/{well}/readings/{readings['2026-11-20T20:00:00Z']}",
         {"read_at_utc": "2026-11-20T20:00:00Z", "cumulative_m3": 558.0, "reason": "logbook"}),
        ("POST", f"/wells/{well}/accounting", {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-21"}),
        ("POST", f"/wells/{well}/accounting", {"lab": LAB, "analyte": ANALYTE, "day": "2026-11-20"}),
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        results = list(pool.map(lambda t: call(base, *t), tasks))
    for status, body in results:
        assert status < 500, (status, body)
        if status >= 400:
            assert "detail" in body, body

    # settle first: a recalculation always leaves exactly one current run
    for day, expected in (("2026-11-20", 395.0), ("2026-11-21", 686.0)):
        final = must(base, "POST", f"/wells/{well}/accounting",
                     {"lab": LAB, "analyte": ANALYTE, "day": day})["run"]
        assert abs(final["removal_mass_g"] - expected) < 1e-6, (day, final["removal_mass_g"], expected)
        assert final["status"] == "current"
        status, listing = call(base, "GET", f"/wells/{well}/accounting?day={day}&run_status=current")
        assert status == 200 and listing["total"] == 1, listing
        # history is retained: the runs we saw expire/supersede are still readable
        status, all_runs = call(base, "GET", f"/wells/{well}/accounting?day={day}&limit=100")
        statuses = [r["status"] for r in all_runs["items"]]
        assert statuses.count("current") == 1, statuses
        assert len(statuses) >= 2, statuses

    # day 20 hand-check: [00:00,20:00) 58 m3 @5 = 290; [20:00,24:00) (600-558)/8*4=21 m3 @5 = 105 -> 395
    # day 21 hand-check: [00:00,04:00) 21 m3 @5 = 105; [04:00,06:00) 7 m3 @5 = 35;
    #                    [06:00,20:00) 49 m3 @8.4 = 411.6; [20:00,24:00) 16 m3 @8.4 = 134.4 -> 686

    print(json.dumps({
        "ok": True,
        "well": well,
        "cross_day": {"2026-11-20": 380.0, "2026-11-21": 480.0},
        "late_sample_expired_run": d21["id"],
        "recalculated_day21_g": d21b["removal_mass_g"],
        "race_responses": [s for s, _ in results],
        "settled": {"2026-11-20": 395.0, "2026-11-21": 686.0},
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
