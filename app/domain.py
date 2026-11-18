"""Domain rules for the groundwater sample data supply API.

A *sample* is registered with location, collector and UTC sampling time.
*Measurements* record lab / analyte / concentration per sample.
*Handovers* form a chain of custody, numbered from 1 per sample:

  * seq 1:  collector -> receiver            (out party must be the collector)
  * seq n:  previous to_party -> new party    (must be contiguous, no gaps)
  * time:   handover time > sampling time and strictly increasing along chain
  * the chain's terminal party is the reporting laboratory, and every
    handover in the chain must be complete/consistent for delivery.

Handovers may be reported out of order, but seq must start at 1 and be
contiguous.  A retry with identical business content for the same seq is
idempotent; a differing payload is rejected and must go through the explicit
correction endpoint.  Corrections are substantive and revoke prior deliveries
that included the sample.

Deliveries are numbered per sampling date (UTC date).  A delivery snapshots the
samples that are *currently eligible* for that date.  If a handover used by a
delivery is substantively corrected, that delivery immediately stops serving
only the affected samples (with a reason); other samples keep being served.
The old delivery is not restored even if the chain is later repaired; a new
delivery must be issued.
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from fastapi import HTTPException


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------

class ApiError(HTTPException):
    def __init__(self, status_code: int, code: str, message: str, **extra: Any):
        self.code = code
        self.detail_obj = {"code": code, "message": message, **extra}
        super().__init__(status_code=status_code, detail=self.detail_obj)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return format_utc(utc_now())


def format_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def parse_utc(value: str, field: str) -> datetime:
    """Parse an ISO-8601 timestamp that carries an explicit UTC designator/offset."""
    raw = value.strip()
    if not raw:
        raise ApiError(422, "invalid_timestamp", f"{field} must not be empty")
    # Accept a trailing Z explicitly; datetime.fromisoformat on 3.11 handles it,
    # but be defensive about a missing offset, which is ambiguous for a data API.
    if raw.endswith(("Z", "z")):
        candidate = raw[:-1] + "+00:00"
    else:
        candidate = raw
    try:
        dt = datetime.fromisoformat(candidate)
    except ValueError:
        raise ApiError(422, "invalid_timestamp", f"{field} must be a valid ISO-8601 timestamp")
    if dt.tzinfo is None:
        raise ApiError(
            422,
            "timestamp_missing_timezone",
            f"{field} must carry an explicit UTC offset (use '...Z')",
        )
    return dt.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Content hashing / canonical JSON
# ---------------------------------------------------------------------------

def canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------

def row_to_sample(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "sample_code": row["sample_code"],
        "location": row["location"],
        "collector": row["collector"],
        "sampled_at_utc": row["sampled_at_utc"],
        "sample_date": row["sample_date"],
        "reporting_lab": row["reporting_lab"],
        "created_at_utc": row["created_at_utc"],
    }


def row_to_measurement(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "sample_id": row["sample_id"],
        "lab": row["lab"],
        "analyte": row["analyte"],
        "concentration": row["concentration"],
        "unit": row["unit"],
        "entered_at_utc": row["entered_at_utc"],
    }


def row_to_handover(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "sample_id": row["sample_id"],
        "seq": row["seq"],
        "from_party": row["from_party"],
        "to_party": row["to_party"],
        "handed_at_utc": row["handed_at_utc"],
        "created_at_utc": row["created_at_utc"],
    }


def get_sample(conn: sqlite3.Connection, sample_id: int) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM samples WHERE id = ?", (sample_id,)).fetchone()
    if row is None:
        raise ApiError(404, "sample_not_found", f"sample {sample_id} does not exist")
    return row


def get_handovers(conn: sqlite3.Connection, sample_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM handovers WHERE sample_id = ? ORDER BY seq ASC", (sample_id,)
    ).fetchall()


def get_measurements(conn: sqlite3.Connection, sample_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM measurements WHERE sample_id = ? ORDER BY id ASC", (sample_id,)
    ).fetchall()


# ---------------------------------------------------------------------------
# Chain logic
# ---------------------------------------------------------------------------

@dataclass
class ChainStatus:
    ok: bool
    errors: list[str]
    handovers: list[dict[str, Any]]
    terminal_party: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "errors": self.errors,
            "terminal_party": self.terminal_party,
            "handover_count": len(self.handovers),
        }


def evaluate_chain(sample: sqlite3.Row, handovers: list[sqlite3.Row]) -> ChainStatus:
    """Validate the whole handover chain for one sample.

    Returns a status object with human-readable errors.  ``ok`` means the chain
    is complete, contiguous, chronologically sound, correctly chained from the
    collector and ends at a reporting laboratory.
    """
    errors: list[str] = []
    ordered = sorted(handovers, key=lambda r: r["seq"])
    if not ordered:
        return ChainStatus(ok=False, errors=["no handovers registered"], handovers=[], terminal_party=None)

    seen: set[int] = set()
    prev_to: str | None = None
    prev_time: datetime | None = None
    sampled_at = parse_utc(sample["sampled_at_utc"], "sampled_at_utc")
    seqs = [r["seq"] for r in ordered]
    if sorted(seqs) != list(range(1, len(seqs) + 1)):
        missing = sorted(set(range(1, max(seqs) + 1)) - set(seqs))
        errors.append(f"handover sequence numbers must be contiguous from 1; missing {missing}")
    for index, row in enumerate(ordered):
        seq = row["seq"]
        if seq in seen:
            errors.append(f"duplicate handover seq {seq}")
            continue
        seen.add(seq)
        try:
            handed_at = parse_utc(row["handed_at_utc"], f"handover[{seq}].handed_at_utc")
        except ApiError as exc:
            errors.append(f"handover[{seq}] has an invalid timestamp")
            continue
        if handed_at <= sampled_at:
            errors.append(f"handover[{seq}] time must be later than the sampling time")
        if prev_time is not None and handed_at <= prev_time:
            errors.append(f"handover[{seq}] time must be later than handover[{seq - 1}]")
        if index == 0:
            if seq != 1:
                errors.append("handover sequence must start at 1")
            if row["from_party"] != sample["collector"]:
                errors.append(
                    f"handover[1] out party must be the collector '{sample['collector']}'"
                )
        else:
            if prev_to is not None and row["from_party"] != prev_to:
                errors.append(
                    f"handover[{seq}].from_party must match the previous recipient '{prev_to}'"
                )
        prev_to = row["to_party"]
        prev_time = handed_at

    # The chain must end at a named party; whether that party is the reporting
    # laboratory is checked against the sample's declared/inferred lab during
    # eligibility evaluation.
    terminal = ordered[-1]["to_party"] if ordered else None
    if not terminal:
        errors.append("the handover chain must end at a reporting laboratory")

    return ChainStatus(
        ok=not errors,
        errors=errors,
        handovers=[row_to_handover(r) for r in ordered],
        terminal_party=terminal,
    )


# ---------------------------------------------------------------------------
# Delivery revision tokens
# ---------------------------------------------------------------------------

def sample_state_token(
    sample: sqlite3.Row,
    handovers: list[sqlite3.Row],
    measurements: list[sqlite3.Row],
) -> str:
    """Hash the exact committed business state of a sample."""
    payload = {
        "sample": {
            "id": sample["id"],
            "sample_code": sample["sample_code"],
            "location": sample["location"],
            "collector": sample["collector"],
            "sampled_at_utc": sample["sampled_at_utc"],
            "reporting_lab": sample["reporting_lab"],
        },
        "handovers": [
            {
                "seq": r["seq"],
                "from_party": r["from_party"],
                "to_party": r["to_party"],
                "handed_at_utc": r["handed_at_utc"],
            }
            for r in sorted(handovers, key=lambda x: x["seq"])
        ],
        "measurements": [
            {
                "lab": r["lab"],
                "analyte": r["analyte"],
                "concentration": r["concentration"],
                "unit": r["unit"],
            }
            for r in sorted(measurements, key=lambda x: (x["lab"], x["analyte"]))
        ],
    }
    return sha256_text(canonical_json(payload))


def handover_hash(from_party: str, to_party: str, handed_at_utc: str) -> str:
    return sha256_text(
        canonical_json(
            {"from_party": from_party, "to_party": to_party, "handed_at_utc": handed_at_utc}
        )
    )


# ---------------------------------------------------------------------------
# Audit helpers
# ---------------------------------------------------------------------------

def record_revision(
    conn: sqlite3.Connection,
    sample_id: int,
    seq: int,
    action: str,
    prev_hash: str | None,
    new_hash: str,
    reason: str | None,
    now_iso: str,
) -> None:
    conn.execute(
        "INSERT INTO handover_revisions (sample_id, seq, action, prev_hash, new_hash, reason, created_at_utc)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (sample_id, seq, action, prev_hash, new_hash, reason, now_iso),
    )


# ---------------------------------------------------------------------------
# Delivery revocation
# ---------------------------------------------------------------------------

def revoke_sample_from_deliveries(
    conn: sqlite3.Connection,
    sample_id: int,
    reason: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    """Stop every still-active delivery from serving ``sample_id``.

    The sample may appear in several deliveries, so every non-revoked
    ``delivery_items`` row for the sample is withdrawn.  Other samples in the
    same deliveries are untouched.  A revoked item is never restored
    automatically.
    """
    active_items = conn.execute(
        "SELECT di.delivery_id, d.delivery_no"
        " FROM delivery_items di JOIN deliveries d ON d.id = di.delivery_id"
        " WHERE di.sample_id = ? AND di.revoked_at_utc IS NULL"
        " ORDER BY di.delivery_id ASC",
        (sample_id,),
    ).fetchall()
    conn.execute(
        "UPDATE delivery_items SET revoked_at_utc = ?, revoke_reason = ?"
        " WHERE sample_id = ? AND revoked_at_utc IS NULL",
        (now_iso, reason, sample_id),
    )
    return [
        {
            "delivery_id": row["delivery_id"],
            "delivery_no": row["delivery_no"],
            "sample_id": sample_id,
            "reason": reason,
            "revoked_at_utc": now_iso,
        }
        for row in active_items
    ]


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

@dataclass
class Eligibility:
    sample: sqlite3.Row
    handovers: list[sqlite3.Row]
    measurements: list[sqlite3.Row]
    chain: ChainStatus
    problems: list[str]

    @property
    def ok(self) -> bool:
        return not self.problems

    def as_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample["id"],
            "sample_code": self.sample["sample_code"],
            "eligible": self.ok,
            "problems": self.problems,
            "chain": self.chain.as_dict(),
            "measurement_count": len(self.measurements),
        }


def evaluate_eligibility(conn: sqlite3.Connection, sample: sqlite3.Row) -> Eligibility:
    handovers = get_handovers(conn, sample["id"])
    measurements = get_measurements(conn, sample["id"])
    chain = evaluate_chain(sample, handovers)
    problems: list[str] = []
    if not chain.ok:
        problems.extend(chain.errors)
    if not measurements:
        problems.append("no measurements recorded")
    lab_problem = _reporting_lab_problem(sample, measurements, chain)
    if lab_problem:
        problems.append(lab_problem)
    for m in measurements:
        if not m["lab"] or not m["analyte"]:
            problems.append(f"measurement {m['id']} has an empty lab or analyte")
        if m["concentration"] is None:
            problems.append(f"measurement {m['id']} has no concentration")
    return Eligibility(sample=sample, handovers=handovers, measurements=measurements, chain=chain, problems=problems)


def _reporting_lab_problem(
    sample: sqlite3.Row,
    measurements: list[sqlite3.Row],
    chain: ChainStatus,
) -> str | None:
    """The handover chain must end at the laboratory that reports the results.

    If the sample declares ``reporting_lab`` it must equal the terminal
    recipient.  Without a declaration the single distinct measurement lab is
    taken as the reporting laboratory (an ambiguous multi-lab sample must
    declare one).
    """
    declared = sample["reporting_lab"]
    labs = sorted({m["lab"] for m in measurements})
    if declared:
        if chain.terminal_party and declared != chain.terminal_party:
            return (
                f"handover chain terminal party '{chain.terminal_party}' is not the "
                f"declared reporting laboratory '{declared}'"
            )
        if labs and declared not in labs:
            return f"declared reporting laboratory '{declared}' has no measurements"
        return None
    if labs and chain.terminal_party and chain.terminal_party not in labs:
        return (
            f"handover chain terminal party '{chain.terminal_party}' is not one of the "
            f"laboratories that reported measurements ({', '.join(labs)})"
        )
    if len(labs) > 1:
        return (
            "measurements come from several laboratories and the reporting laboratory is "
            "not declared; set reporting_lab so the chain terminal can be verified"
        )
    return None


def delivery_revision_token(eligible: list["Eligibility"]) -> str:
    """Hash of the exact committed state a delivery would be built from."""
    return sha256_text(
        canonical_json(
            sorted(
                (
                    {
                        "sample_id": e.sample["id"],
                        "state_token": sample_state_token(e.sample, e.handovers, e.measurements),
                    }
                    for e in eligible
                ),
                key=lambda x: x["sample_id"],
            )
        )
    )


def build_snapshot(elig: Eligibility) -> dict[str, Any]:
    sample = elig.sample
    return {
        "sample": {
            "id": sample["id"],
            "sample_code": sample["sample_code"],
            "location": sample["location"],
            "collector": sample["collector"],
            "sampled_at_utc": sample["sampled_at_utc"],
            "sample_date": sample["sample_date"],
            "reporting_lab": sample["reporting_lab"],
        },
        "handovers": [row_to_handover(r) for r in elig.handovers],
        "measurements": [
            {
                "lab": m["lab"],
                "analyte": m["analyte"],
                "concentration": m["concentration"],
                "unit": m["unit"],
            }
            for m in elig.measurements
        ],
        "state_token": sample_state_token(sample, elig.handovers, elig.measurements),
    }


def next_delivery_no(conn: sqlite3.Connection, sample_date: str) -> str:
    """Sequential delivery number scoped to one UTC sampling date."""
    prefix = f"DLV-{sample_date.replace('-', '')}-"
    row = conn.execute(
        "SELECT COUNT(*) AS n FROM deliveries WHERE sample_date = ?", (sample_date,)
    ).fetchone()
    n = int(row["n"]) + 1
    # Defensive: skip numbers already used (e.g. after manual data import).
    while conn.execute(
        "SELECT 1 FROM deliveries WHERE delivery_no = ?", (f"{prefix}{n:03d}",)
    ).fetchone():
        n += 1
    return f"{prefix}{n:03d}"


# ---------------------------------------------------------------------------
# Well readings and removal-mass accounting
# ---------------------------------------------------------------------------

def row_to_reading(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "well_no": row["well_no"],
        "read_at_utc": row["read_at_utc"],
        "cumulative_m3": row["cumulative_m3"],
        "created_at_utc": row["created_at_utc"],
    }


def reading_hash(read_at_utc: str, cumulative_m3: float) -> str:
    return sha256_text(
        canonical_json({"read_at_utc": read_at_utc, "cumulative_m3": cumulative_m3})
    )


def get_readings(conn: sqlite3.Connection, well_no: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM well_readings WHERE well_no = ? ORDER BY read_at_utc ASC, id ASC",
        (well_no,),
    ).fetchall()


def well_exists(conn: sqlite3.Connection, well_no: str) -> bool:
    if conn.execute("SELECT 1 FROM well_readings WHERE well_no = ? LIMIT 1", (well_no,)).fetchone():
        return True
    return conn.execute("SELECT 1 FROM samples WHERE location = ? LIMIT 1", (well_no,)).fetchone() is not None


def load_well_inputs(
    conn: sqlite3.Connection, well_no: str
) -> tuple[list[sqlite3.Row], list[dict[str, Any]]]:
    """Load every reading and every sample (with custody + measurements) for a well.

    The accounting basis is deliberately the *whole* committed state of the
    well, loaded inside the caller's write transaction, so the resulting
    ``inputs_token`` proves exactly which committed state a run was computed
    from.
    """
    readings = get_readings(conn, well_no)
    samples = conn.execute(
        "SELECT * FROM samples WHERE location = ? ORDER BY sampled_at_utc ASC, id ASC",
        (well_no,),
    ).fetchall()
    bundles = [
        {
            "sample": s,
            "handovers": get_handovers(conn, s["id"]),
            "measurements": get_measurements(conn, s["id"]),
        }
        for s in samples
    ]
    return readings, bundles


def accounting_inputs_token(
    *,
    well_no: str,
    lab: str,
    analyte: str,
    day: str,
    carry_over_hours: float,
    max_interval_hours: float,
    readings: list[sqlite3.Row],
    bundles: list[dict[str, Any]],
) -> str:
    """Hash of the exact committed state an accounting run is computed from."""
    payload = {
        "well_no": well_no,
        "lab": lab,
        "analyte": analyte,
        "day": day,
        "carry_over_hours": carry_over_hours,
        "max_interval_hours": max_interval_hours,
        "readings": [
            {"id": r["id"], "read_at_utc": r["read_at_utc"], "cumulative_m3": r["cumulative_m3"]}
            for r in readings
        ],
        "samples": [
            {
                "id": b["sample"]["id"],
                "sample_code": b["sample"]["sample_code"],
                "sampled_at_utc": b["sample"]["sampled_at_utc"],
                "collector": b["sample"]["collector"],
                "reporting_lab": b["sample"]["reporting_lab"],
                "handovers": [
                    {
                        "seq": h["seq"],
                        "from_party": h["from_party"],
                        "to_party": h["to_party"],
                        "handed_at_utc": h["handed_at_utc"],
                    }
                    for h in sorted(b["handovers"], key=lambda x: x["seq"])
                ],
                "measurements": [
                    {
                        "lab": m["lab"],
                        "analyte": m["analyte"],
                        "concentration": m["concentration"],
                        "unit": m["unit"],
                    }
                    for m in sorted(b["measurements"], key=lambda x: (x["lab"], x["analyte"]))
                ],
            }
            for b in bundles
        ],
    }
    return sha256_text(canonical_json(payload))


def row_to_accounting_run(row: sqlite3.Row, *, include_result: bool = True) -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": row["id"],
        "well_no": row["well_no"],
        "lab": row["lab"],
        "analyte": row["analyte"],
        "day": row["day"],
        "status": row["status"],
        "carry_over_hours": row["carry_over_hours"],
        "max_interval_hours": row["max_interval_hours"],
        "inputs_token": row["inputs_token"],
        "removal_mass_g": row["removal_mass_g"],
        "complete": bool(row["complete"]),
        "created_at_utc": row["created_at_utc"],
        "expired_at_utc": row["expired_at_utc"],
        "expire_reason": row["expire_reason"],
        "superseded_at_utc": row["superseded_at_utc"],
    }
    if include_result:
        result["result"] = json.loads(row["result_json"])
    return result


def expire_accounting_runs(
    conn: sqlite3.Connection,
    well_no: str,
    day_lo: str,
    day_hi: str,
    reason: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    """Mark every *current* run for ``well_no`` with day in [day_lo, day_hi] expired.

    Expired runs are kept (status + reason + timestamp) and never served as
    current again; a successful recalculation creates a new current run.
    """
    rows = conn.execute(
        "SELECT id, lab, analyte, day FROM accounting_runs"
        " WHERE well_no = ? AND status = 'current' AND day >= ? AND day <= ?"
        " ORDER BY id ASC",
        (well_no, day_lo, day_hi),
    ).fetchall()
    conn.execute(
        "UPDATE accounting_runs SET status = 'expired', expired_at_utc = ?, expire_reason = ?"
        " WHERE well_no = ? AND status = 'current' AND day >= ? AND day <= ?",
        (now_iso, reason, well_no, day_lo, day_hi),
    )
    return [
        {"run_id": r["id"], "lab": r["lab"], "analyte": r["analyte"], "day": r["day"], "reason": reason}
        for r in rows
    ]


def expire_accounting_for_reading_change(
    conn: sqlite3.Connection,
    well_no: str,
    timestamps: list[str],
    reason: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    """Expire runs whose day overlaps the metering intervals touched by a reading change.

    ``timestamps`` are the changed readings' timestamps (old and new).  The
    affected range spans the neighbouring readings on each side, because the
    intervals between them are exactly what changed.  Must be called after the
    write is applied, inside the same transaction.
    """
    points: list[str] = []
    for t in timestamps:
        if not t:
            continue
        points.append(t)
        prev = conn.execute(
            "SELECT read_at_utc FROM well_readings WHERE well_no = ? AND read_at_utc < ?"
            " ORDER BY read_at_utc DESC LIMIT 1",
            (well_no, t),
        ).fetchone()
        nxt = conn.execute(
            "SELECT read_at_utc FROM well_readings WHERE well_no = ? AND read_at_utc > ?"
            " ORDER BY read_at_utc ASC LIMIT 1",
            (well_no, t),
        ).fetchone()
        if prev:
            points.append(prev["read_at_utc"])
        if nxt:
            points.append(nxt["read_at_utc"])
    if not points:
        return []
    return expire_accounting_runs(conn, well_no, min(points)[:10], max(points)[:10], reason, now_iso)


def expire_accounting_for_sample_change(
    conn: sqlite3.Connection,
    sample: sqlite3.Row,
    reason: str,
    now_iso: str,
) -> list[dict[str, Any]]:
    """Expire runs whose day overlaps the period whose concentration basis is this sample.

    A sample (or its custody/measurements) is the concentration basis from its
    ``sampled_at_utc`` until the next sample at the same location; only runs
    for days overlapping that window can be affected.  Must be called inside
    the same transaction as the change itself.
    """
    well_no = sample["location"]
    sampled_at = sample["sampled_at_utc"]
    nxt = conn.execute(
        "SELECT sampled_at_utc FROM samples WHERE location = ? AND id != ? AND sampled_at_utc > ?"
        " ORDER BY sampled_at_utc ASC LIMIT 1",
        (well_no, sample["id"], sampled_at),
    ).fetchone()
    day_lo = sampled_at[:10]
    day_hi = nxt["sampled_at_utc"][:10] if nxt else "9999-12-31"
    return expire_accounting_runs(conn, well_no, day_lo, day_hi, reason, now_iso)


def build_accounting_run(
    conn: sqlite3.Connection,
    *,
    well_no: str,
    lab: str,
    analyte: str,
    day: str,
    carry_over_hours: float,
    now_iso: str,
) -> tuple[sqlite3.Row, bool]:
    """Calculate one well/lab/analyte/day and persist the run, atomically.

    Returns ``(run_row, recomputed)``.  When a current run already exists for
    the same committed inputs and parameters it is returned unchanged
    (``recomputed=False``).  Otherwise the day is computed from the committed
    state visible in *this* transaction, any previous current run is
    superseded and the new run becomes the only current one.  Expired runs are
    left untouched (kept for audit).  Caller must hold a write transaction.
    """
    from . import accounting  # local import: keeps the engine dependency one-way

    readings, bundles = load_well_inputs(conn, well_no)
    token = accounting_inputs_token(
        well_no=well_no,
        lab=lab,
        analyte=analyte,
        day=day,
        carry_over_hours=carry_over_hours,
        max_interval_hours=accounting.MAX_INTERVAL_HOURS,
        readings=readings,
        bundles=bundles,
    )
    current = conn.execute(
        "SELECT * FROM accounting_runs"
        " WHERE well_no = ? AND lab = ? AND analyte = ? AND day = ? AND status = 'current'"
        " ORDER BY id DESC LIMIT 1",
        (well_no, lab, analyte, day),
    ).fetchone()
    if (
        current is not None
        and current["inputs_token"] == token
        and float(current["carry_over_hours"]) == carry_over_hours
    ):
        return current, False

    engine_readings = [
        {
            "id": r["id"],
            "read_at": parse_utc(r["read_at_utc"], "read_at_utc"),
            "cumulative_m3": float(r["cumulative_m3"]),
        }
        for r in readings
    ]
    engine_samples = []
    for bundle in bundles:
        sample = bundle["sample"]
        elig = evaluate_eligibility(conn, sample)
        measurement = next(
            (m for m in bundle["measurements"] if m["lab"] == lab and m["analyte"] == analyte),
            None,
        )
        unit = measurement["unit"] if measurement else None
        concentration = (
            float(measurement["concentration"])
            if measurement is not None and unit == accounting.REQUIRED_UNIT
            else None
        )
        problems = list(elig.problems)
        if measurement is None:
            problems.append(f"no measurement for lab '{lab}' / analyte '{analyte}'")
        elif unit != accounting.REQUIRED_UNIT:
            problems.append(f"measurement unit '{unit}' is not {accounting.REQUIRED_UNIT}")
        engine_samples.append(
            {
                "id": sample["id"],
                "sample_code": sample["sample_code"],
                "sampled_at": parse_utc(sample["sampled_at_utc"], "sampled_at_utc"),
                "qualified": elig.ok and concentration is not None,
                "concentration": concentration,
                "unit": unit,
                "problems": problems,
            }
        )
    result = accounting.compute_day(
        well_no=well_no,
        lab=lab,
        analyte=analyte,
        day=date.fromisoformat(day),
        carry_over_hours=carry_over_hours,
        readings=engine_readings,
        samples=engine_samples,
    )
    conn.execute(
        "UPDATE accounting_runs SET status = 'superseded', superseded_at_utc = ?"
        " WHERE well_no = ? AND lab = ? AND analyte = ? AND day = ? AND status = 'current'",
        (now_iso, well_no, lab, analyte, day),
    )
    cur = conn.execute(
        "INSERT INTO accounting_runs (well_no, lab, analyte, day, carry_over_hours, max_interval_hours,"
        " status, inputs_token, removal_mass_g, complete, result_json, created_at_utc)"
        " VALUES (?, ?, ?, ?, ?, ?, 'current', ?, ?, ?, ?, ?)",
        (
            well_no,
            lab,
            analyte,
            day,
            carry_over_hours,
            accounting.MAX_INTERVAL_HOURS,
            token,
            result["removal_mass_g"],
            1 if result["complete"] else 0,
            canonical_json(result),
            now_iso,
        ),
    )
    run = conn.execute("SELECT * FROM accounting_runs WHERE id = ?", (int(cur.lastrowid),)).fetchone()
    return run, True



def delivery_public(conn: sqlite3.Connection, delivery: sqlite3.Row, include_revoked: bool = True) -> dict[str, Any]:
    items = conn.execute(
        "SELECT * FROM delivery_items WHERE delivery_id = ? ORDER BY id ASC", (delivery["id"],)
    ).fetchall()
    delivered: list[dict[str, Any]] = []
    revoked: list[dict[str, Any]] = []
    for item in items:
        snap = json.loads(item["snapshot_json"])
        entry = {
            "sample_id": item["sample_id"],
            "sample_code": snap["sample"]["sample_code"],
            "location": snap["sample"]["location"],
            "collector": snap["sample"]["collector"],
            "sampled_at_utc": snap["sample"]["sampled_at_utc"],
            "reporting_lab": snap["sample"].get("reporting_lab"),
            "handovers": snap["handovers"],
            "measurements": snap["measurements"],
            "revoked_at_utc": item["revoked_at_utc"],
            "revoke_reason": item["revoke_reason"],
        }
        if item["revoked_at_utc"] is not None:
            revoked.append(entry)
        else:
            delivered.append(entry)
    result = {
        "delivery_no": delivery["delivery_no"],
        "sample_date": delivery["sample_date"],
        "created_at_utc": delivery["created_at_utc"],
        "samples": delivered,
        "revoked_samples": revoked if include_revoked else [],
        "sample_count": len(delivered),
        "revoked_count": len(revoked),
        "revision_token": delivery["revision_token"],
    }
    return result
