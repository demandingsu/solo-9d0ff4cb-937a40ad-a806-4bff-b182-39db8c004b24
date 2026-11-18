"""Removal-mass accounting per well / laboratory / analyte / UTC day.

Pure standard-library calculation engine (no FastAPI / SQLite imports) so the
arithmetic can be verified in isolation.

Model
-----
* Cumulative meter readings (m3) per well.  Two adjacent readings form a
  *metering interval*; the pumped volume is the delta of the counter and is
  distributed uniformly over the interval's duration.
* For one UTC day the timeline [00:00, 24:00) is cut at

  - every reading timestamp inside the day,
  - every sampling timestamp of samples at the well (a new sample is a new
    concentration basis),
  - every carry-over expiry of a qualified concentration
    (``sampled_at + carry_over_hours``).

* Each resulting segment takes, at its start, the most recent concentration
  that is custody-qualified and reported in mg/L.  The segment is *pending*
  (待核) instead of determined when

  - no metering interval covers it          -> ``no_reading_coverage``
  - the counter decreases across the interval -> ``reading_rollback``
  - the interval spans more than 24 h        -> ``interval_exceeds_one_day``
  - no qualified mg/L concentration exists   -> ``no_valid_concentration``
  - the newest qualified concentration is older than the carry-over limit
                                               -> ``carry_over_expired``
  - several qualified samples share the newest sampling timestamp with
    different concentrations                 -> ``conflicting_concentrations``

  Unknown is never treated as zero: pending segments contribute no mass, and
  a day without any determinable segment reports ``removal_mass_g = None``.

Mass: 1 mg/L over 1 m3 is 1000 mg = 1 g, so ``mass_g = conc_mg_l * volume_m3``.
"""
from __future__ import annotations

import bisect
from datetime import datetime, timedelta, timezone
from typing import Any

MAX_INTERVAL_HOURS = 24.0
DEFAULT_CARRY_OVER_HOURS = 72.0
REQUIRED_UNIT = "mg/L"

NO_READING_COVERAGE = "no_reading_coverage"
READING_ROLLBACK = "reading_rollback"
INTERVAL_EXCEEDS_ONE_DAY = "interval_exceeds_one_day"
NO_VALID_CONCENTRATION = "no_valid_concentration"
CARRY_OVER_EXPIRED = "carry_over_expired"
CONFLICTING_CONCENTRATIONS = "conflicting_concentrations"

_REASON_DETAIL = {
    NO_READING_COVERAGE: "no meter readings bracket this period; pumped volume is unknown",
    READING_ROLLBACK: "the cumulative reading decreases across this metering interval; volume is unknown",
    INTERVAL_EXCEEDS_ONE_DAY: "the metering interval spans more than 24 h; the volume cannot be attributed to a single day",
    NO_VALID_CONCENTRATION: "no custody-qualified mg/L concentration exists on or before the segment start",
    CARRY_OVER_EXPIRED: "the newest qualified concentration is older than the configured carry-over limit",
    CONFLICTING_CONCENTRATIONS: "several qualified samples share the newest sampling timestamp with different concentrations",
}


def _fmt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _round(value: float | None) -> float | None:
    return None if value is None else round(value, 6)


def compute_day(
    *,
    well_no: str,
    lab: str,
    analyte: str,
    day,
    carry_over_hours: float,
    readings: list[dict[str, Any]],
    samples: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compute the accounting result for one UTC day.

    ``readings``: items ``{"id", "read_at": datetime, "cumulative_m3": float}``
    (any order; sorted here).  ``samples``: items ``{"id", "sample_code",
    "sampled_at": datetime, "qualified": bool, "concentration": float | None,
    "unit": str | None, "problems": [str]}`` where ``qualified`` already
    reflects the chain-of-custody evaluation and ``concentration`` is only set
    for measurements in mg/L for the requested lab/analyte.
    """
    day_start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)
    carry = timedelta(hours=carry_over_hours)
    max_interval = timedelta(hours=MAX_INTERVAL_HOURS)

    readings = sorted(readings, key=lambda r: (r["read_at"], r["id"]))
    reading_times = [r["read_at"] for r in readings]
    qualified = sorted(
        (
            s
            for s in samples
            if s["qualified"] and s["concentration"] is not None and s["sampled_at"] < day_end
        ),
        key=lambda s: (s["sampled_at"], s["id"]),
    )
    qualified_times = [q["sampled_at"] for q in qualified]

    # ------------------------------------------------------------------ cuts
    cuts: set[datetime] = {day_start, day_end}
    for r in readings:
        if day_start < r["read_at"] < day_end:
            cuts.add(r["read_at"])
    for s in samples:
        t = s["sampled_at"]
        if day_start <= t < day_end:
            cuts.add(t)
    for q in qualified:
        expiry = q["sampled_at"] + carry
        if day_start < expiry < day_end:
            cuts.add(expiry)
    points = sorted(cuts)

    # --------------------------------------------------------------- lookups
    def interval_index(t: datetime) -> int | None:
        i = bisect.bisect_right(reading_times, t) - 1
        return i if 0 <= i < len(readings) - 1 else None

    def concentration_at(t: datetime):
        """-> (concentration | None, basis samples, reasons)."""
        i = bisect.bisect_right(qualified_times, t) - 1
        if i < 0:
            return None, [], [NO_VALID_CONCENTRATION]
        newest_t = qualified_times[i]
        group = [q for q in qualified if q["sampled_at"] == newest_t]
        values = {q["concentration"] for q in group}
        if len(values) > 1:
            return None, group, [CONFLICTING_CONCENTRATIONS]
        if t - newest_t >= carry:
            return None, group, [CARRY_OVER_EXPIRED]
        return group[0]["concentration"], group, []

    # -------------------------------------------------------------- segments
    ok_segments: list[dict[str, Any]] = []
    pending_raw: list[tuple[datetime, datetime, tuple[str, ...], float | None]] = []
    used_sample_ids: set[int] = set()
    exact_mass = 0.0

    for t0, t1 in zip(points, points[1:]):
        reasons: list[str] = []
        volume: float | None = None
        pair: tuple[dict[str, Any], dict[str, Any]] | None = None

        i = interval_index(t0)
        if i is None:
            reasons.append(NO_READING_COVERAGE)
        else:
            r0, r1 = readings[i], readings[i + 1]
            pair = (r0, r1)
            span_s = (r1["read_at"] - r0["read_at"]).total_seconds()
            delta = r1["cumulative_m3"] - r0["cumulative_m3"]
            if delta < 0:
                reasons.append(READING_ROLLBACK)
            if r1["read_at"] - r0["read_at"] > max_interval:
                reasons.append(INTERVAL_EXCEEDS_ONE_DAY)
            if not reasons:
                volume = delta * (t1 - t0).total_seconds() / span_s

        concentration, basis, conc_reasons = concentration_at(t0)
        reasons.extend(conc_reasons)

        if reasons:
            pending_raw.append((t0, t1, tuple(reasons), volume))
            continue

        mass = volume * concentration  # mg/L * m3 == g
        exact_mass += mass
        used_sample_ids.update(s["id"] for s in basis)
        ok_segments.append(
            {
                "start_utc": _fmt(t0),
                "end_utc": _fmt(t1),
                "volume_m3": _round(volume),
                "concentration_mg_l": concentration,
                "mass_g": _round(mass),
                "sample_id": basis[0]["id"] if basis else None,
                "sample_code": basis[0]["sample_code"] if basis else None,
                "reading_ids": [pair[0]["id"], pair[1]["id"]] if pair else [],
            }
        )

    # ------------------------------------------- merge adjacent pending runs
    pending: list[dict[str, Any]] = []
    for t0, t1, reason_key, volume in pending_raw:
        if (
            pending
            and pending[-1]["_reasons"] == reason_key
            and pending[-1]["_end"] == t0
            and (pending[-1]["volume_m3"] is None) == (volume is None)
        ):
            pending[-1]["_end"] = t1
            if pending[-1]["volume_m3"] is not None:
                pending[-1]["volume_m3"] += volume  # type: ignore[operator]
        else:
            pending.append(
                {"_start": t0, "_end": t1, "_reasons": reason_key, "volume_m3": volume}
            )
    pending_intervals = [
        {
            "start_utc": _fmt(p["_start"]),
            "end_utc": _fmt(p["_end"]),
            "reasons": list(p["_reasons"]),
            "volume_m3": _round(p["volume_m3"]),
            "detail": "; ".join(_REASON_DETAIL[r] for r in p["_reasons"]),
        }
        for p in pending
    ]

    # --------------------------------------------------------------- sources
    src_readings: dict[int, dict[str, Any]] = {}
    for i in range(len(readings) - 1):
        r0, r1 = readings[i], readings[i + 1]
        if r0["read_at"] < day_end and r1["read_at"] > day_start:
            for r in (r0, r1):
                src_readings[r["id"]] = {
                    "id": r["id"],
                    "read_at_utc": _fmt(r["read_at"]),
                    "cumulative_m3": r["cumulative_m3"],
                }
    source_readings = [src_readings[k] for k in sorted(src_readings, key=lambda k: (src_readings[k]["read_at_utc"], k))]

    source_samples = [
        {
            "id": s["id"],
            "sample_code": s["sample_code"],
            "sampled_at_utc": _fmt(s["sampled_at"]),
            "qualified": bool(s["qualified"]),
            "concentration": s["concentration"],
            "unit": s["unit"],
            "used_in_segments": s["id"] in used_sample_ids,
            "problems": list(s["problems"]),
        }
        for s in sorted(samples, key=lambda s: (s["sampled_at"], s["id"]))
        if s["sampled_at"] < day_end
    ]

    return {
        "well_no": well_no,
        "lab": lab,
        "analyte": analyte,
        "day": day_start.date().isoformat(),
        "parameters": {
            "carry_over_hours": carry_over_hours,
            "max_interval_hours": MAX_INTERVAL_HOURS,
            "unit": REQUIRED_UNIT,
        },
        "removal_mass_g": _round(exact_mass) if ok_segments else None,
        "complete": not pending_intervals,
        "segments": ok_segments,
        "pending_intervals": pending_intervals,
        "sources": {"readings": source_readings, "samples": source_samples},
    }
