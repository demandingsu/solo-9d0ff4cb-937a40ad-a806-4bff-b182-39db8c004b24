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
from datetime import datetime, timezone
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
