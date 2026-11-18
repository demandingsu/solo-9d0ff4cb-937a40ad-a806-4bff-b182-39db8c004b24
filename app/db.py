"""SQLite access helpers.

The service is intentionally a small, single-process FastAPI app backed by
SQLite.  Every write request opens its own ``BEGIN IMMEDIATE`` transaction so
that concurrent requests are serialised by SQLite instead of interleaving
partial state.  A ``threading.RLock`` also serialises writers inside the
process (uvicorn default deployment), which keeps "read the current committed
state, validate, write" sequences atomic even though the app uses threads.
"""
from __future__ import annotations

import os
import sqlite3
import threading
from contextlib import contextmanager
from typing import Iterator

DB_PATH = os.environ.get("GSB_DB_PATH", os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "gsb.sqlite3"))

_write_lock = threading.RLock()
_initialised = False
_init_lock = threading.Lock()

SCHEMA = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS samples (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_code     TEXT NOT NULL,
    location        TEXT NOT NULL,
    collector       TEXT NOT NULL,
    sampled_at_utc  TEXT NOT NULL,           -- ISO-8601, canonical UTC with Z
    sample_date     TEXT NOT NULL,           -- YYYY-MM-DD (UTC date of sampling)
    reporting_lab   TEXT,                    -- optional declared reporting laboratory
    created_at_utc  TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_samples_date ON samples(sample_date);
CREATE UNIQUE INDEX IF NOT EXISTS ux_samples_code ON samples(sample_code);

CREATE TABLE IF NOT EXISTS measurements (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id       INTEGER NOT NULL REFERENCES samples(id),
    lab             TEXT NOT NULL,
    analyte         TEXT NOT NULL,
    concentration   REAL NOT NULL,
    unit            TEXT NOT NULL,
    entered_at_utc  TEXT NOT NULL,
    UNIQUE(sample_id, lab, analyte)
);

CREATE TABLE IF NOT EXISTS measurement_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id       INTEGER NOT NULL REFERENCES samples(id),
    measurement_id  INTEGER NOT NULL REFERENCES measurements(id),
    action          TEXT NOT NULL,           -- create | correct
    prev_json       TEXT,
    new_json        TEXT NOT NULL,
    reason          TEXT,
    created_at_utc  TEXT NOT NULL
);

-- Handover links.  A link is identified by (sample_id, seq).  ``content`` is
-- the canonical JSON of the *business* content (handover parties + timestamp),
-- so an identical retry can be recognised and a real edit detected.
CREATE TABLE IF NOT EXISTS handovers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id       INTEGER NOT NULL REFERENCES samples(id),
    seq             INTEGER NOT NULL,
    from_party      TEXT NOT NULL,
    to_party        TEXT NOT NULL,
    handed_at_utc   TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    created_at_utc  TEXT NOT NULL,
    UNIQUE(sample_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_handovers_sample ON handovers(sample_id, seq);

-- Every accepted *change* to a handover link is recorded here, including the
-- initial registration (action='create').  This is the audit trail that proves
-- why an old delivery was invalidated and whether it can be republished.
CREATE TABLE IF NOT EXISTS handover_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id       INTEGER NOT NULL REFERENCES samples(id),
    seq             INTEGER NOT NULL,
    action          TEXT NOT NULL,           -- create | correct
    prev_hash       TEXT,
    new_hash        TEXT NOT NULL,
    reason          TEXT,
    created_at_utc  TEXT NOT NULL
);

-- Delivery numbers are per sample_date.  ``revision_token`` hashes the exact
-- committed sample set + the exact committed handover/measurement state the
-- delivery was built from, which lets us detect "substantive" changes.
CREATE TABLE IF NOT EXISTS deliveries (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_no     TEXT NOT NULL UNIQUE,
    sample_date     TEXT NOT NULL,
    revision_token  TEXT NOT NULL,
    created_at_utc  TEXT NOT NULL,
    revoked_at_utc  TEXT,
    revoke_reason   TEXT
);
CREATE INDEX IF NOT EXISTS idx_deliveries_date ON deliveries(sample_date);

CREATE TABLE IF NOT EXISTS delivery_items (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id     INTEGER NOT NULL REFERENCES deliveries(id),
    sample_id       INTEGER NOT NULL REFERENCES samples(id),
    snapshot_json   TEXT NOT NULL,
    revoked_at_utc  TEXT,
    revoke_reason   TEXT,
    UNIQUE(delivery_id, sample_id)
);

CREATE TABLE IF NOT EXISTS sequence_counters (
    name            TEXT PRIMARY KEY,
    value           INTEGER NOT NULL
);

-- Cumulative pumped-volume meter readings (m3) per well.  A reading is
-- identified by (well_no, read_at_utc); the well number corresponds to the
-- samples' ``location``.  ``content_hash`` covers the business content so an
-- identical retry is recognised and a real change must go through PATCH.
CREATE TABLE IF NOT EXISTS well_readings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    well_no         TEXT NOT NULL,
    read_at_utc     TEXT NOT NULL,           -- ISO-8601, canonical UTC with Z
    cumulative_m3   REAL NOT NULL,
    content_hash    TEXT NOT NULL,
    created_at_utc  TEXT NOT NULL,
    UNIQUE(well_no, read_at_utc)
);
CREATE INDEX IF NOT EXISTS idx_readings_well ON well_readings(well_no, read_at_utc);

-- Audit trail of reading registrations and corrections.
CREATE TABLE IF NOT EXISTS well_reading_revisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    reading_id      INTEGER NOT NULL REFERENCES well_readings(id),
    well_no         TEXT NOT NULL,
    action          TEXT NOT NULL,           -- create | correct
    prev_json       TEXT,
    new_json        TEXT NOT NULL,
    reason          TEXT,
    created_at_utc  TEXT NOT NULL
);

-- Persisted accounting runs, one row per calculation of
-- (well_no, lab, analyte, day).  ``status`` lifecycle:
--   current    -> the latest successful calculation, safe to serve
--   expired    -> inputs changed underneath it (kept for audit, must be recalculated)
--   superseded -> replaced by a newer successful recalculation
-- ``inputs_token`` hashes the exact committed readings/samples/custody/
-- measurement state the run was computed from (single-transaction basis).
CREATE TABLE IF NOT EXISTS accounting_runs (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    well_no            TEXT NOT NULL,
    lab                TEXT NOT NULL,
    analyte            TEXT NOT NULL,
    day                TEXT NOT NULL,        -- YYYY-MM-DD (UTC)
    carry_over_hours   REAL NOT NULL,
    max_interval_hours REAL NOT NULL,
    status             TEXT NOT NULL,        -- current | expired | superseded
    inputs_token       TEXT NOT NULL,
    removal_mass_g     REAL,                 -- NULL when nothing is determinable
    complete           INTEGER NOT NULL,     -- 1 when no pending intervals
    result_json        TEXT NOT NULL,
    created_at_utc     TEXT NOT NULL,
    expired_at_utc     TEXT,
    expire_reason      TEXT,
    superseded_at_utc  TEXT
);
CREATE INDEX IF NOT EXISTS idx_accounting_key ON accounting_runs(well_no, lab, analyte, day, status);
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_db() -> None:
    global _initialised
    if _initialised:
        return
    with _init_lock:
        if _initialised:
            return
        os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
        conn = _connect()
        try:
            conn.executescript(SCHEMA)
        finally:
            conn.close()
        _initialised = True


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """Open a serialised immediate transaction; commit on success, rollback on error."""
    init_db()
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()


@contextmanager
def read_conn() -> Iterator[sqlite3.Connection]:
    init_db()
    conn = _connect()
    try:
        # A read transaction gives the whole request one consistent snapshot.
        conn.execute("BEGIN")
        yield conn
    finally:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        conn.close()
