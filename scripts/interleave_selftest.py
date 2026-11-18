"""Interleaving / concurrency self-test for the delivery API.

This script exercises the exact scenario the data team worries about:
handover corrections, measurement entry and delivery publishing racing each
other.  It runs against a live HTTP server (default http://127.0.0.1:8000) and
asserts the invariant that *every delivery that exists was built from one
committed state* and that a correction never leaves a half-issued delivery or
half-applied handover.

Usage:
    python scripts/interleave_selftest.py --base-url http://127.0.0.1:8000
"""
from __future__ import annotations

import argparse
import concurrent.futures
import json
import sys
import urllib.error
import urllib.request
import uuid


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


def register(base, tag):
    status, body = call(
        base,
        "POST",
        "/samples",
        {
            "location": f"selftest-{tag}",
            "collector": f"collector-{tag}",
            "sampled_at_utc": "2026-11-18T01:00:00Z",
            "sample_code": f"ST-{tag}-{uuid.uuid4().hex[:8]}",
        },
    )
    assert status == 201, (status, body)
    return body["id"]


def ready(base, tag):
    sid = register(base, tag)
    for payload in (
        {"seq": 1, "from_party": f"collector-{tag}", "to_party": "courier", "handed_at_utc": "2026-11-18T02:00:00Z"},
        {"seq": 2, "from_party": "courier", "to_party": "lab-a", "handed_at_utc": "2026-11-18T03:00:00Z"},
    ):
        status, body = call(base, "POST", f"/samples/{sid}/handovers", payload)
        assert status == 201, (status, body)
    status, body = call(
        base,
        "POST",
        f"/samples/{sid}/measurements",
        {"lab": "lab-a", "analyte": "nitrate-N", "concentration": 5.0, "unit": "mg/L"},
    )
    assert status == 201, (status, body)
    return sid


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.base_url
    date = "2026-11-18"
    tag = uuid.uuid4().hex[:6]

    ids = [ready(base, f"{tag}-{i}") for i in range(6)]
    victim = ids[0]

    # 1) Publish one delivery that serves all six samples.
    status, delivery = call(base, "POST", "/deliveries", {"sample_date": date})
    assert status == 201, (status, delivery)
    assert delivery["sample_count"] >= 6, delivery
    delivery_no = delivery["delivery_no"]
    before_token = delivery["revision_token"]

    # 2) Race: correct the victim's handover while a new delivery is published
    #    and a measurement is added to another sample.  Run each task twice to
    #    widen the interleaving window.
    correction = {
        "from_party": f"collector-{tag}-0",
        "to_party": "courier",
        "handed_at_utc": "2026-11-18T02:15:00Z",
        "reason": "interleave selftest correction",
    }
    tasks = [
        ("PATCH", f"/samples/{victim}/handovers/1", correction),
        ("POST", "/deliveries", {"sample_date": date}),
        ("POST", f"/samples/{ids[1]}/measurements", {"lab": "lab-b", "analyte": "arsenic", "concentration": 0.001, "unit": "mg/L"}),
        ("POST", "/deliveries", {"sample_date": date}),
        ("PATCH", f"/samples/{victim}/handovers/1", {**correction, "reason": "interleave selftest correction 2"}),
    ]
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        results = list(pool.map(lambda t: call(base, *t), tasks))

    # Every response is either a clean 2xx or a well-formed 4xx; no 5xx.
    for status, body in results:
        assert status < 500, (status, body)
        if status >= 400:
            assert "detail" in body, body

    # 3) The old delivery must still serve the five untouched samples and must
    #    explain the withdrawal of the victim.
    status, old = call(base, "GET", f"/deliveries/{delivery_no}")
    assert status == 200, (status, old)
    served = {s["sample_id"] for s in old["samples"]}
    revoked = {s["sample_id"] for s in old["revoked_samples"]}
    ours = set(ids)
    assert victim not in served, (served, revoked)
    assert victim in revoked, (served, revoked)
    # the racy measurement landed on ids[1], which must also be withdrawn
    assert ids[1] in revoked, (served, revoked)
    # only our racy changes touch our samples; everything else keeps serving
    assert (served | revoked) & ours == ours, (served, revoked)
    assert served & ours == ours - revoked, (served, revoked)
    for s in old["revoked_samples"]:
        assert s["revoke_reason"], s

    # 4) No delivery ever serves a sample whose current committed state differs
    #    from the snapshot it was built from.  Re-read every delivery.
    status, listing = call(base, "GET", f"/deliveries?sample_date={date}&limit=1000")
    assert status == 200, (status, listing)
    for summary in listing["items"]:
        _, full = call(base, "GET", f"/deliveries/{summary['delivery_no']}")
        for item in full["samples"]:
            _, sample = call(base, "GET", f"/samples/{item['sample_id']}")
            snapshot_state = {
                "handovers": [
                    {k: h[k] for k in ("seq", "from_party", "to_party", "handed_at_utc")}
                    for h in item["handovers"]
                ],
                "measurements": [
                    {k: m[k] for k in ("lab", "analyte", "concentration", "unit")}
                    for m in item["measurements"]
                ],
            }
            live_state = {
                "handovers": [
                    {k: h[k] for k in ("seq", "from_party", "to_party", "handed_at_utc")}
                    for h in sample["handovers"]
                ],
                "measurements": [
                    {k: m[k] for k in ("lab", "analyte", "concentration", "unit")}
                    for m in sample["measurements"]
                ],
            }
            assert snapshot_state == live_state, (
                "delivery serves a stale sample",
                full["delivery_no"],
                item["sample_id"],
            )

    # 5) After corrections settle, a fresh delivery must be issuable and the
    #    victim must be included again (the chain is still valid).
    status, fresh = call(base, "POST", "/deliveries", {"sample_date": date})
    assert status == 201, (status, fresh)
    assert victim in {s["sample_id"] for s in fresh["samples"]}, fresh
    assert fresh["revision_token"] != ""

    print(json.dumps({
        "ok": True,
        "old_delivery": delivery_no,
        "old_served": len(served),
        "old_revoked": sorted(revoked),
        "fresh_delivery": fresh["delivery_no"],
        "race_responses": [s for s, _ in results],
        "old_revision_token": before_token[:12],
        "fresh_revision_token": fresh["revision_token"][:12],
    }, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
