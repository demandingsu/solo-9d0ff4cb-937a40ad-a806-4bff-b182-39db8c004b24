"""FastAPI application: groundwater sample delivery data supply API."""
from __future__ import annotations

import json
import math
import sqlite3
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

from fastapi import FastAPI, Path, Query, Response, status
from pydantic import BaseModel, ConfigDict, Field

from . import db, domain
from .domain import ApiError

APP_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class SampleCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "location": "MW-12, north wellfield",
                "collector": "Li Wei",
                "sampled_at_utc": "2026-11-18T02:15:00Z",
                "sample_code": "MW12-20261118-01",
            }
        }
    )
    location: str = Field(min_length=1, max_length=500)
    collector: str = Field(min_length=1, max_length=200)
    sampled_at_utc: str = Field(description="ISO-8601 timestamp with explicit UTC offset, e.g. 2026-11-18T02:15:00Z")
    sample_code: str | None = Field(default=None, max_length=100, description="Optional external sample code; generated when omitted")
    reporting_lab: str | None = Field(
        default=None,
        max_length=200,
        description="Optional declared reporting lab. When omitted, the terminal handover recipient must match a measurement lab.",
    )


class HandoverCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "seq": 1,
                "from_party": "Li Wei",
                "to_party": "Field Logistics",
                "handed_at_utc": "2026-11-18T02:40:00Z",
            }
        }
    )
    seq: int = Field(ge=1)
    from_party: str = Field(min_length=1, max_length=200)
    to_party: str = Field(min_length=1, max_length=200)
    handed_at_utc: str = Field(description="ISO-8601 timestamp with explicit UTC offset")


class HandoverCorrect(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "from_party": "Li Wei",
                "to_party": "Field Logistics",
                "handed_at_utc": "2026-11-18T02:45:00Z",
                "reason": "field log shows 02:45, not 02:40",
            }
        }
    )
    from_party: str = Field(min_length=1, max_length=200)
    to_party: str = Field(min_length=1, max_length=200)
    handed_at_utc: str
    reason: str = Field(min_length=1, max_length=1000, description="Mandatory: corrections are explicit and auditable")


class MeasurementCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "lab": "Central Water Laboratory",
                "analyte": "nitrate-N",
                "concentration": 4.2,
                "unit": "mg/L",
            }
        }
    )
    lab: str = Field(min_length=1, max_length=200)
    analyte: str = Field(min_length=1, max_length=200)
    concentration: float
    unit: str = Field(min_length=1, max_length=50)


class MeasurementCorrect(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "lab": "Central Water Laboratory",
                "analyte": "nitrate-N",
                "concentration": 4.1,
                "unit": "mg/L",
                "reason": "decimal transcription error corrected against lab report",
            }
        }
    )
    lab: str = Field(min_length=1, max_length=200)
    analyte: str = Field(min_length=1, max_length=200)
    concentration: float
    unit: str = Field(min_length=1, max_length=50)
    reason: str = Field(min_length=1, max_length=1000)


class DeliveryCreate(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"sample_date": "2026-11-18"}}
    )
    sample_date: str = Field(description="Sampling date in UTC, YYYY-MM-DD")
    expected_revision_token: str | None = Field(
        default=None,
        description=(
            "Optional optimistic guard. When supplied and it does not equal the "
            "exact committed state being published, the request is rejected with "
            "409 and no delivery number is consumed. Pass the token returned by a "
            "previous delivery to prove that nothing changed since you inspected it."
        ),
    )


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _clean(value: str, field_name: str) -> str:
    trimmed = value.strip()
    if not trimmed:
        raise ApiError(422, "invalid_field", f"{field_name} must not be blank")
    return trimmed


def _parse_date(value: str) -> str:
    try:
        parsed = date.fromisoformat(value.strip())
    except ValueError:
        raise ApiError(422, "invalid_date", "sample_date must be YYYY-MM-DD")
    return parsed.isoformat()


def _finite_concentration(value: float) -> float:
    if not math.isfinite(value):
        raise ApiError(422, "invalid_concentration", "concentration must be a finite number")
    if value < 0:
        raise ApiError(422, "invalid_concentration", "concentration must not be negative")
    return float(value)


def _sample_code(conn: sqlite3.Connection, sample_date: str) -> str:
    row = conn.execute(
        "SELECT value FROM sequence_counters WHERE name = ?", (f"sample:{sample_date}",)
    ).fetchone()
    if row is None:
        value = 1
        conn.execute(
            "INSERT INTO sequence_counters (name, value) VALUES (?, ?)",
            (f"sample:{sample_date}", value),
        )
    else:
        value = int(row["value"]) + 1
        conn.execute(
            "UPDATE sequence_counters SET value = ? WHERE name = ?",
            (value, f"sample:{sample_date}"),
        )
    return f"SMP-{sample_date.replace('-', '')}-{value:04d}"


def _load_sample_or_404(conn: sqlite3.Connection, sample_id: int) -> sqlite3.Row:
    return domain.get_sample(conn, sample_id)


def _validate_handover(
    conn: sqlite3.Connection,
    sample: sqlite3.Row,
    seq: int,
    from_party: str,
    to_party: str,
    handed_dt,
    *,
    ignore_seq: int | None = None,
    check_neighbours: bool = True,
) -> None:
    """Validate a handover against intrinsic rules and existing neighbours.

    Missing neighbours are allowed: handovers may be reported out of order.
    Full-chain validity (continuity from 1, terminal lab) is enforced at
    delivery time by :func:`app.domain.evaluate_chain`.  Corrections may
    temporarily leave the chain inconsistent (``check_neighbours=False``) so
    that a broken chain can be repaired link by link; the sample simply stays
    ineligible for delivery until it is whole again.
    """
    if seq < 1:
        raise ApiError(422, "invalid_sequence", "seq must be >= 1")
    if from_party == to_party:
        raise ApiError(422, "invalid_parties", "from_party and to_party must differ")
    sampled_at = domain.parse_utc(sample["sampled_at_utc"], "sampled_at_utc")
    if handed_dt <= sampled_at:
        raise ApiError(
            422,
            "handover_not_after_sampling",
            "handed_at_utc must be later than the sample's sampled_at_utc",
        )
    if seq == 1 and from_party != sample["collector"]:
        raise ApiError(
            422,
            "chain_must_start_with_collector",
            f"handover[1].from_party must be the collector '{sample['collector']}'",
        )

    if not check_neighbours:
        return

    rows = [
        r
        for r in domain.get_handovers(conn, sample["id"])
        if r["seq"] != (ignore_seq if ignore_seq is not None else -1)
    ]
    lower = max((r for r in rows if r["seq"] < seq), key=lambda r: r["seq"], default=None)
    higher = min((r for r in rows if r["seq"] > seq), key=lambda r: r["seq"], default=None)

    if lower is not None:
        if from_party != lower["to_party"]:
            raise ApiError(
                409,
                "handover_party_mismatch",
                f"handover[{seq}].from_party must equal the previous recipient '{lower['to_party']}' (seq {lower['seq']})",
            )
        lower_time = domain.parse_utc(lower["handed_at_utc"], "handed_at_utc")
        if handed_dt <= lower_time:
            raise ApiError(
                409,
                "handover_time_not_increasing",
                f"handover[{seq}] time must be later than handover[{lower['seq']}]",
            )
    if higher is not None:
        if to_party != higher["from_party"]:
            raise ApiError(
                409,
                "handover_party_mismatch",
                f"handover[{seq}].to_party must equal the next sender '{higher['from_party']}' (seq {higher['seq']})",
            )
        higher_time = domain.parse_utc(higher["handed_at_utc"], "handed_at_utc")
        if handed_dt >= higher_time:
            raise ApiError(
                409,
                "handover_time_not_increasing",
                f"handover[{seq}] time must be earlier than handover[{higher['seq']}]",
            )


def _measurement_content(row: sqlite3.Row) -> tuple[Any, ...]:
    return (row["lab"], row["analyte"], float(row["concentration"]), row["unit"])


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    db.init_db()
    yield


app = FastAPI(
    title="GSB Groundwater Sample Delivery API",
    version=APP_VERSION,
    description=(
        "Data-supply API for groundwater sampling with a numbered chain of custody "
        "and per-sampling-date delivery numbers.\n\n"
        "**Workflow**\n"
        "1. `POST /samples` registers location, collector and UTC sampling time.\n"
        "2. `POST /samples/{id}/handovers` reports handovers by sequence number. "
        "Sequence 1 must leave from the collector, each later handover must leave "
        "from the previous recipient, times must increase and be later than sampling. "
        "Handovers may arrive out of order; identical retries are idempotent; "
        "changing an existing handover requires `PATCH` with an explicit reason.\n"
        "3. `POST /samples/{id}/measurements` records lab / analyte / concentration.\n"
        "4. `POST /deliveries` issues a delivery number for a sampling date using "
        "only samples eligible at that moment. The delivery snapshots the exact "
        "committed source state.\n\n"
        "**Corrections and deliveries**\n"
        "A substantive handover correction (or measurement change) immediately stops "
        "every active delivery from serving only the affected sample, recording the "
        "reason. Other samples are unaffected. A corrected sample is not restored to "
        "an old delivery even if its chain is later repaired: issue a new delivery.\n\n"
        "Every write runs in a single SQLite `BEGIN IMMEDIATE` transaction, so "
        "corrections, measurement entry and publishing never observe or persist a "
        "partially committed state."
    ),
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
def root() -> dict[str, str]:
    return {"service": "GSB groundwater sample delivery API", "docs": "/docs", "openapi": "/openapi.json"}


@app.get("/healthz", tags=["system"], summary="Health check")
def healthz() -> dict[str, str]:
    with db.read_conn() as conn:
        conn.execute("SELECT 1").fetchone()
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Samples
# ---------------------------------------------------------------------------

@app.post("/samples", status_code=status.HTTP_201_CREATED, tags=["samples"], summary="Register a sample")
def create_sample(body: SampleCreate) -> dict[str, Any]:
    location = _clean(body.location, "location")
    collector = _clean(body.collector, "collector")
    reporting_lab = _clean(body.reporting_lab, "reporting_lab") if body.reporting_lab else None
    sampled_dt = domain.parse_utc(body.sampled_at_utc, "sampled_at_utc")
    sampled_date = sampled_dt.date().isoformat()
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        code = _clean(body.sample_code, "sample_code") if body.sample_code else _sample_code(conn, sampled_date)
        if conn.execute("SELECT 1 FROM samples WHERE sample_code = ?", (code,)).fetchone():
            raise ApiError(409, "sample_code_conflict", f"sample_code '{code}' already exists")
        cur = conn.execute(
            "INSERT INTO samples (sample_code, location, collector, sampled_at_utc, sample_date, created_at_utc, reporting_lab)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (code, location, collector, domain.format_utc(sampled_dt), sampled_date, now, reporting_lab),
        )
        row = _load_sample_or_404(conn, int(cur.lastrowid))
        elig = domain.evaluate_eligibility(conn, row)
        result = domain.row_to_sample(row)
        result["handovers"] = []
        result["measurements"] = []
        result["chain"] = elig.chain.as_dict()
        result["eligible_for_delivery"] = elig.ok
        result["eligibility_problems"] = elig.problems
    return result


@app.get("/samples", tags=["samples"], summary="List samples")
def list_samples(
    sample_date: str | None = Query(default=None, description="UTC sampling date YYYY-MM-DD"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    where = ""
    params: list[Any] = []
    if sample_date is not None:
        sd = _parse_date(sample_date)
        where = " WHERE sample_date = ?"
        params.append(sd)
    with db.read_conn() as conn:
        total = int(conn.execute(f"SELECT COUNT(*) AS n FROM samples{where}", params).fetchone()["n"])
        rows = conn.execute(
            f"SELECT * FROM samples{where} ORDER BY id ASC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
        items = [domain.row_to_sample(r) for r in rows]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get("/samples/{sample_id}", tags=["samples"], summary="Get a sample with custody, measurements and eligibility")
def get_sample(sample_id: int = Path(ge=1)) -> dict[str, Any]:
    with db.read_conn() as conn:
        sample = _load_sample_or_404(conn, sample_id)
        elig = domain.evaluate_eligibility(conn, sample)
        handovers = [domain.row_to_handover(r) for r in elig.handovers]
        measurements = [domain.row_to_measurement(r) for r in elig.measurements]
        delivery_rows = conn.execute(
            "SELECT di.delivery_id, d.delivery_no, d.sample_date, di.revoked_at_utc, di.revoke_reason,"
            " di.snapshot_json"
            " FROM delivery_items di JOIN deliveries d ON d.id = di.delivery_id"
            " WHERE di.sample_id = ? ORDER BY di.delivery_id DESC",
            (sample_id,),
        ).fetchall()
        result = domain.row_to_sample(sample)
        result["reporting_lab"] = sample["reporting_lab"]
        result["handovers"] = handovers
        result["measurements"] = measurements
        result["chain"] = elig.chain.as_dict()
        result["eligible_for_delivery"] = elig.ok
        result["eligibility_problems"] = elig.problems
        result["deliveries"] = [
            {
                "delivery_no": r["delivery_no"],
                "sample_date": r["sample_date"],
                "snapshot_state_token": json.loads(r["snapshot_json"])["state_token"],
                "revoked_at_utc": r["revoked_at_utc"],
                "revoke_reason": r["revoke_reason"],
            }
            for r in delivery_rows
        ]
    return result


# ---------------------------------------------------------------------------
# Handover / custody
# ---------------------------------------------------------------------------

@app.get("/samples/{sample_id}/handovers", tags=["custody"], summary="List a sample's handover chain")
def list_handovers(sample_id: int = Path(ge=1)) -> dict[str, Any]:
    with db.read_conn() as conn:
        sample = _load_sample_or_404(conn, sample_id)
        rows = domain.get_handovers(conn, sample_id)
        chain = domain.evaluate_chain(sample, rows)
    return {
        "sample_id": sample_id,
        "handovers": [domain.row_to_handover(r) for r in rows],
        "chain": chain.as_dict(),
    }


@app.post(
    "/samples/{sample_id}/handovers",
    status_code=status.HTTP_201_CREATED,
    tags=["custody"],
    summary="Report a handover by sequence number (order-independent, idempotent)",
)
def add_handover(body: HandoverCreate, sample_id: int = Path(ge=1), response: Response = None) -> dict[str, Any]:
    from_party = _clean(body.from_party, "from_party")
    to_party = _clean(body.to_party, "to_party")
    handed_dt = domain.parse_utc(body.handed_at_utc, "handed_at_utc")
    handed_iso = domain.format_utc(handed_dt)
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        sample = _load_sample_or_404(conn, sample_id)
        existing = conn.execute(
            "SELECT * FROM handovers WHERE sample_id = ? AND seq = ?", (sample_id, body.seq)
        ).fetchone()
        new_hash = domain.handover_hash(from_party, to_party, handed_iso)
        if existing is not None:
            if existing["content_hash"] == new_hash:
                if response is not None:
                    response.status_code = status.HTTP_200_OK
                return {
                    "idempotent": True,
                    "handover": domain.row_to_handover(existing),
                    "chain": domain.evaluate_chain(sample, domain.get_handovers(conn, sample_id)).as_dict(),
                }
            raise ApiError(
                409,
                "handover_already_exists",
                f"handover seq {body.seq} already exists with different content; use PATCH to correct it explicitly",
                existing=domain.row_to_handover(existing),
            )
        _validate_handover(conn, sample, body.seq, from_party, to_party, handed_dt)
        cur = conn.execute(
            "INSERT INTO handovers (sample_id, seq, from_party, to_party, handed_at_utc, content_hash, created_at_utc)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (sample_id, body.seq, from_party, to_party, handed_iso, new_hash, now),
        )
        domain.record_revision(
            conn, sample_id, body.seq, "create", None, new_hash, None, now
        )
        row = conn.execute("SELECT * FROM handovers WHERE id = ?", (cur.lastrowid,)).fetchone()
        chain = domain.evaluate_chain(sample, domain.get_handovers(conn, sample_id))
        revoked = domain.revoke_sample_from_deliveries(
            conn,
            sample_id,
            f"handover seq {body.seq} added after the delivery was issued; "
            "the delivery snapshot no longer matches committed custody state. Republish.",
            now,
        )
    return {
        "idempotent": False,
        "handover": domain.row_to_handover(row),
        "chain": chain.as_dict(),
        "revoked_deliveries": revoked,
    }


@app.patch(
    "/samples/{sample_id}/handovers/{seq}",
    tags=["custody"],
    summary="Explicitly correct a handover (substantive changes revoke that sample from active deliveries)",
)
def correct_handover(
    body: HandoverCorrect,
    sample_id: int = Path(ge=1),
    seq: int = Path(ge=1),
) -> dict[str, Any]:
    from_party = _clean(body.from_party, "from_party")
    to_party = _clean(body.to_party, "to_party")
    handed_dt = domain.parse_utc(body.handed_at_utc, "handed_at_utc")
    handed_iso = domain.format_utc(handed_dt)
    reason = _clean(body.reason, "reason")
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        sample = _load_sample_or_404(conn, sample_id)
        existing = conn.execute(
            "SELECT * FROM handovers WHERE sample_id = ? AND seq = ?", (sample_id, seq)
        ).fetchone()
        if existing is None:
            raise ApiError(404, "handover_not_found", f"handover seq {seq} does not exist for sample {sample_id}")
        new_hash = domain.handover_hash(from_party, to_party, handed_iso)
        if new_hash == existing["content_hash"]:
            return {
                "corrected": False,
                "substantive": False,
                "reason": "content unchanged; no correction recorded",
                "handover": domain.row_to_handover(existing),
                "chain": domain.evaluate_chain(sample, domain.get_handovers(conn, sample_id)).as_dict(),
                "revoked_deliveries": [],
            }
        _validate_handover(
            conn,
            sample,
            seq,
            from_party,
            to_party,
            handed_dt,
            ignore_seq=seq,
            check_neighbours=False,
        )
        conn.execute(
            "UPDATE handovers SET from_party = ?, to_party = ?, handed_at_utc = ?, content_hash = ? WHERE id = ?",
            (from_party, to_party, handed_iso, new_hash, existing["id"]),
        )
        domain.record_revision(
            conn, sample_id, seq, "correct", existing["content_hash"], new_hash, reason, now
        )
        revoke_reason = (
            f"handover seq {seq} substantively corrected after delivery: "
            f"({existing['from_party']} -> {existing['to_party']} @ {existing['handed_at_utc']}) "
            f"became ({from_party} -> {to_party} @ {handed_iso}); reason: {reason}. "
            "The old delivery no longer provides this sample and is not restored automatically; republish."
        )
        revoked = domain.revoke_sample_from_deliveries(conn, sample_id, revoke_reason, now)
        row = conn.execute("SELECT * FROM handovers WHERE id = ?", (existing["id"],)).fetchone()
        chain = domain.evaluate_chain(sample, domain.get_handovers(conn, sample_id))
    return {
        "corrected": True,
        "substantive": True,
        "handover": domain.row_to_handover(row),
        "chain": chain.as_dict(),
        "revoked_deliveries": revoked,
    }


@app.get(
    "/samples/{sample_id}/handovers/revisions",
    tags=["custody"],
    summary="Audit trail of handover creations and corrections",
)
def handover_revisions(sample_id: int = Path(ge=1)) -> dict[str, Any]:
    with db.read_conn() as conn:
        _load_sample_or_404(conn, sample_id)
        rows = conn.execute(
            "SELECT * FROM handover_revisions WHERE sample_id = ? ORDER BY id ASC", (sample_id,)
        ).fetchall()
    return {
        "sample_id": sample_id,
        "revisions": [
            {
                "seq": r["seq"],
                "action": r["action"],
                "prev_hash": r["prev_hash"],
                "new_hash": r["new_hash"],
                "reason": r["reason"],
                "created_at_utc": r["created_at_utc"],
            }
            for r in rows
        ],
    }


# ---------------------------------------------------------------------------
# Measurements
# ---------------------------------------------------------------------------

@app.get("/samples/{sample_id}/measurements", tags=["measurements"], summary="List measurements")
def list_measurements(sample_id: int = Path(ge=1)) -> dict[str, Any]:
    with db.read_conn() as conn:
        _load_sample_or_404(conn, sample_id)
        rows = domain.get_measurements(conn, sample_id)
    return {"sample_id": sample_id, "measurements": [domain.row_to_measurement(r) for r in rows]}


@app.post(
    "/samples/{sample_id}/measurements",
    status_code=status.HTTP_201_CREATED,
    tags=["measurements"],
    summary="Record a lab / analyte / concentration (identical retry is idempotent)",
)
def add_measurement(body: MeasurementCreate, sample_id: int = Path(ge=1), response: Response = None) -> dict[str, Any]:
    lab = _clean(body.lab, "lab")
    analyte = _clean(body.analyte, "analyte")
    unit = _clean(body.unit, "unit")
    concentration = _finite_concentration(body.concentration)
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        sample = _load_sample_or_404(conn, sample_id)
        existing = conn.execute(
            "SELECT * FROM measurements WHERE sample_id = ? AND lab = ? AND analyte = ?",
            (sample_id, lab, analyte),
        ).fetchone()
        if existing is not None:
            if _measurement_content(existing) == (lab, analyte, concentration, unit):
                if response is not None:
                    response.status_code = status.HTTP_200_OK
                return {"idempotent": True, "measurement": domain.row_to_measurement(existing)}
            raise ApiError(
                409,
                "measurement_already_exists",
                "a measurement for this lab/analyte already exists with different content; "
                f"use PATCH /samples/{sample_id}/measurements/{existing['id']} with an explicit reason",
                existing=domain.row_to_measurement(existing),
            )
        cur = conn.execute(
            "INSERT INTO measurements (sample_id, lab, analyte, concentration, unit, entered_at_utc)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (sample_id, lab, analyte, concentration, unit, now),
        )
        measurement_id = int(cur.lastrowid)
        payload = domain.canonical_json(
            {"lab": lab, "analyte": analyte, "concentration": concentration, "unit": unit}
        )
        conn.execute(
            "INSERT INTO measurement_revisions (sample_id, measurement_id, action, prev_json, new_json, reason, created_at_utc)"
            " VALUES (?, ?, 'create', NULL, ?, NULL, ?)",
            (sample_id, measurement_id, payload, now),
        )
        row = conn.execute("SELECT * FROM measurements WHERE id = ?", (measurement_id,)).fetchone()
        revoked = domain.revoke_sample_from_deliveries(
            conn,
            sample_id,
            f"measurement {lab}/{analyte} added after the delivery was issued; "
            "the delivery snapshot no longer matches committed measurement state. Republish.",
            now,
        )
    return {
        "idempotent": False,
        "measurement": domain.row_to_measurement(row),
        "revoked_deliveries": revoked,
    }


@app.patch(
    "/samples/{sample_id}/measurements/{measurement_id}",
    tags=["measurements"],
    summary="Explicitly correct a measurement (changes revoke that sample from active deliveries)",
)
def correct_measurement(
    body: MeasurementCorrect,
    sample_id: int = Path(ge=1),
    measurement_id: int = Path(ge=1),
) -> dict[str, Any]:
    lab = _clean(body.lab, "lab")
    analyte = _clean(body.analyte, "analyte")
    unit = _clean(body.unit, "unit")
    concentration = _finite_concentration(body.concentration)
    reason = _clean(body.reason, "reason")
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        _load_sample_or_404(conn, sample_id)
        existing = conn.execute(
            "SELECT * FROM measurements WHERE id = ? AND sample_id = ?", (measurement_id, sample_id)
        ).fetchone()
        if existing is None:
            raise ApiError(404, "measurement_not_found", f"measurement {measurement_id} does not exist for sample {sample_id}")
        if _measurement_content(existing) == (lab, analyte, concentration, unit):
            return {
                "corrected": False,
                "substantive": False,
                "reason": "content unchanged; no correction recorded",
                "measurement": domain.row_to_measurement(existing),
                "revoked_deliveries": [],
            }
        clash = conn.execute(
            "SELECT 1 FROM measurements WHERE sample_id = ? AND lab = ? AND analyte = ? AND id != ?",
            (sample_id, lab, analyte, measurement_id),
        ).fetchone()
        if clash:
            raise ApiError(409, "measurement_key_conflict", f"another measurement already exists for {lab}/{analyte}")
        previous_payload = domain.canonical_json(
            {
                "lab": existing["lab"],
                "analyte": existing["analyte"],
                "concentration": float(existing["concentration"]),
                "unit": existing["unit"],
            }
        )
        new_payload = domain.canonical_json(
            {"lab": lab, "analyte": analyte, "concentration": concentration, "unit": unit}
        )
        conn.execute(
            "UPDATE measurements SET lab = ?, analyte = ?, concentration = ?, unit = ? WHERE id = ?",
            (lab, analyte, concentration, unit, measurement_id),
        )
        conn.execute(
            "INSERT INTO measurement_revisions (sample_id, measurement_id, action, prev_json, new_json, reason, created_at_utc)"
            " VALUES (?, ?, 'correct', ?, ?, ?, ?)",
            (sample_id, measurement_id, previous_payload, new_payload, reason, now),
        )
        row = conn.execute("SELECT * FROM measurements WHERE id = ?", (measurement_id,)).fetchone()
        revoke_reason = (
            f"measurement {existing['lab']}/{existing['analyte']} "
            f"({existing['concentration']} {existing['unit']}) substantively corrected to "
            f"{lab}/{analyte} ({concentration} {unit}); reason: {reason}. "
            "The old delivery no longer provides this sample and is not restored automatically; republish."
        )
        revoked = domain.revoke_sample_from_deliveries(conn, sample_id, revoke_reason, now)
    return {
        "corrected": True,
        "substantive": True,
        "measurement": domain.row_to_measurement(row),
        "revoked_deliveries": revoked,
    }


# ---------------------------------------------------------------------------
# Deliveries
# ---------------------------------------------------------------------------

@app.post(
    "/deliveries",
    status_code=status.HTTP_201_CREATED,
    tags=["deliveries"],
    summary="Publish a delivery number for a sampling date (eligible samples only, atomic)",
)
def publish_delivery(body: DeliveryCreate) -> dict[str, Any]:
    sample_date = _parse_date(body.sample_date)
    now = domain.utc_now_iso()
    with db.write_tx() as conn:
        samples = conn.execute(
            "SELECT * FROM samples WHERE sample_date = ? ORDER BY id ASC", (sample_date,)
        ).fetchall()
        eligible: list[domain.Eligibility] = []
        excluded: list[dict[str, Any]] = []
        for sample in samples:
            elig = domain.evaluate_eligibility(conn, sample)
            if elig.ok:
                eligible.append(elig)
            else:
                excluded.append(elig.as_dict())
        if not eligible:
            raise ApiError(
                409,
                "no_eligible_samples",
                f"no eligible samples for sampling date {sample_date}; no delivery was issued",
                excluded=excluded,
            )
        revision_token = domain.delivery_revision_token(eligible)
        if (
            body.expected_revision_token is not None
            and body.expected_revision_token != revision_token
        ):
            raise ApiError(
                409,
                "delivery_state_changed",
                "committed sample/handover/measurement state changed since the "
                "expected revision token was issued; no delivery was created",
                expected_revision_token=body.expected_revision_token,
                actual_revision_token=revision_token,
            )
        delivery_no = domain.next_delivery_no(conn, sample_date)
        cur = conn.execute(
            "INSERT INTO deliveries (delivery_no, sample_date, revision_token, created_at_utc)"
            " VALUES (?, ?, ?, ?)",
            (delivery_no, sample_date, revision_token, now),
        )
        delivery_id = int(cur.lastrowid)
        for e in eligible:
            snapshot = domain.build_snapshot(e)
            conn.execute(
                "INSERT INTO delivery_items (delivery_id, sample_id, snapshot_json) VALUES (?, ?, ?)",
                (delivery_id, e.sample["id"], domain.canonical_json(snapshot)),
            )
        delivery = conn.execute("SELECT * FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
        result = domain.delivery_public(conn, delivery)
        result["excluded_samples"] = excluded
    return result


@app.get("/deliveries", tags=["deliveries"], summary="List deliveries for a sampling date")
def list_deliveries(
    sample_date: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    where = ""
    params: list[Any] = []
    if sample_date is not None:
        where = " WHERE sample_date = ?"
        params.append(_parse_date(sample_date))
    with db.read_conn() as conn:
        total = int(conn.execute(f"SELECT COUNT(*) AS n FROM deliveries{where}", params).fetchone()["n"])
        rows = conn.execute(
            f"SELECT * FROM deliveries{where} ORDER BY id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
        items = [domain.delivery_public(conn, r) for r in rows]
    return {"items": items, "total": total, "limit": limit, "offset": offset}


@app.get(
    "/deliveries/{delivery_no}",
    tags=["deliveries"],
    summary="Read a delivery (active samples + samples withdrawn with reasons)",
)
def get_delivery(delivery_no: str) -> dict[str, Any]:
    with db.read_conn() as conn:
        row = conn.execute("SELECT * FROM deliveries WHERE delivery_no = ?", (delivery_no,)).fetchone()
        if row is None:
            raise ApiError(404, "delivery_not_found", f"delivery '{delivery_no}' does not exist")
        result = domain.delivery_public(conn, row)
        result["revoked_at_utc"] = row["revoked_at_utc"]
        result["revoke_reason"] = row["revoke_reason"]
    return result


# ---------------------------------------------------------------------------
# Error shape
# ---------------------------------------------------------------------------

@app.exception_handler(ApiError)
async def api_error_handler(request, exc: ApiError):  # pragma: no cover - FastAPI wiring
    from fastapi.responses import JSONResponse

    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail_obj})
