"""Separate SQLite databases for clinical EDC, IWRS randomization, RSI, and Part 11 audit.

Regulatory boundary note: each SQLite database is accessed via its own dedicated
connection without cross-database ATTACH so blinded site workflows remain isolated
from restricted IWRS treatment assignments.
"""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trialvault_pv.config import DataPaths, ensure_data_dirs

EDC_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS trials (
    trial_id TEXT PRIMARY KEY,
    protocol_code TEXT NOT NULL,
    investigational_product TEXT NOT NULL,
    sponsor_name TEXT NOT NULL,
    phase TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS sites (
    site_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    name TEXT NOT NULL,
    country TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS api_tokens (
    token_hash TEXT PRIMARY KEY,
    site_id TEXT REFERENCES sites(site_id),
    role_label TEXT NOT NULL,
    scopes TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subjects_blinded (
    subject_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    age INTEGER NOT NULL,
    sex TEXT NOT NULL,
    enrollment_date TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'enrolled',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS safety_cases (
    case_uid TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects_blinded(subject_id),
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    site_status TEXT NOT NULL DEFAULT 'received',
    latest_followup_num INTEGER NOT NULL DEFAULT 0,
    initial_aware_at TEXT NOT NULL,
    latest_aware_at TEXT NOT NULL,
    meddra_pt TEXT NOT NULL,
    ctcae_grade INTEGER NOT NULL,
    is_serious INTEGER NOT NULL DEFAULT 0,
    seriousness_json TEXT NOT NULL,
    investigator_causality TEXT NOT NULL,
    sponsor_causality TEXT,
    narrative TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS case_reports (
    report_id TEXT PRIMARY KEY,
    message_id TEXT NOT NULL UNIQUE,
    case_uid TEXT NOT NULL REFERENCES safety_cases(case_uid),
    subject_id TEXT NOT NULL REFERENCES subjects_blinded(subject_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    followup_num INTEGER NOT NULL,
    aware_at TEXT NOT NULL,
    meddra_pt TEXT NOT NULL,
    ctcae_grade INTEGER NOT NULL,
    is_serious INTEGER NOT NULL DEFAULT 0,
    seriousness_json TEXT NOT NULL,
    investigator_causality TEXT NOT NULL,
    sponsor_causality TEXT,
    narrative TEXT NOT NULL,
    received_at TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    UNIQUE(case_uid, followup_num)
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    site_id TEXT,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    run_after TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    executed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_subjects_site ON subjects_blinded(site_id);
CREATE INDEX IF NOT EXISTS idx_cases_site ON safety_cases(site_id);
CREATE INDEX IF NOT EXISTS idx_cases_trial ON safety_cases(trial_id);
CREATE INDEX IF NOT EXISTS idx_reports_case ON case_reports(case_uid, followup_num);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_sched_due ON scheduled_jobs(status, run_after);
"""

IWRS_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS subject_randomization (
    trial_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    treatment_arm TEXT NOT NULL,
    is_investigational_drug INTEGER NOT NULL,
    randomized_at TEXT NOT NULL,
    PRIMARY KEY (trial_id, subject_id)
);

CREATE TABLE IF NOT EXISTS iwrs_unblind_audit (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    trial_id TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    case_uid TEXT NOT NULL,
    actor TEXT NOT NULL,
    accessed_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_iwrs_audit_case ON iwrs_unblind_audit(case_uid);
"""

RSI_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS ib_versions (
    ib_version TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL,
    effective_from TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS rsi_expected_terms (
    ib_version TEXT NOT NULL REFERENCES ib_versions(ib_version),
    meddra_pt TEXT NOT NULL,
    max_expected_grade INTEGER NOT NULL,
    fatal_expected INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (ib_version, meddra_pt)
);

CREATE INDEX IF NOT EXISTS idx_ib_trial_eff ON ib_versions(trial_id, is_active, effective_from);
"""

AUDIT_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS audit_log (
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_ref TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL
);
"""


def parse_utc_iso(ts: str) -> datetime:
    normalized = ts.strip()
    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"
    dt = datetime.fromisoformat(normalized)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def utc_now_iso() -> str:
    override = os.environ.get("TRIALVAULT_PV_CLOCK_ISO")
    if override:
        return format_utc_iso(parse_utc_iso(override))
    return format_utc_iso(datetime.now(timezone.utc))


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def connect_edc(db_path: Path) -> sqlite3.Connection:
    return _connect_sqlite(db_path)


def connect_iwrs(db_path: Path) -> sqlite3.Connection:
    return _connect_sqlite(db_path)


def connect_rsi(db_path: Path) -> sqlite3.Connection:
    return _connect_sqlite(db_path)


def connect_audit(db_path: Path) -> sqlite3.Connection:
    return _connect_sqlite(db_path)


def _has_table(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def init_all_dbs(paths: DataPaths) -> None:
    ensure_data_dirs(paths)
    with connect_edc(paths.edc_db) as conn:
        if not _has_table(conn, "scheduled_jobs"):
            conn.executescript(EDC_SCHEMA_SQL)
            conn.commit()
    with connect_iwrs(paths.iwrs_db) as conn:
        if not _has_table(conn, "iwrs_unblind_audit"):
            conn.executescript(IWRS_SCHEMA_SQL)
            conn.commit()
    with connect_rsi(paths.rsi_db) as conn:
        if not _has_table(conn, "rsi_expected_terms"):
            conn.executescript(RSI_SCHEMA_SQL)
            conn.commit()
    with connect_audit(paths.audit_db) as conn:
        if not _has_table(conn, "audit_log"):
            conn.executescript(AUDIT_SCHEMA_SQL)
            conn.commit()


def lookup_auth_principal(conn: sqlite3.Connection, bearer_token: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT t.site_id, t.role_label, t.scopes, t.active AS token_active,
               s.trial_id, s.active AS site_active
        FROM api_tokens t
        LEFT JOIN sites s ON s.site_id = t.site_id
        WHERE t.token_hash = ?
        """,
        (hash_token(bearer_token),),
    ).fetchone()
    if row is None or not row["token_active"]:
        return None
    if row["site_id"] is not None and not row["site_active"]:
        return None
    scopes = {s.strip() for s in row["scopes"].split(",") if s.strip()}
    return {
        "site_id": row["site_id"],
        "trial_id": row["trial_id"],
        "actor": row["role_label"],
        "scopes": scopes,
    }


def lookup_treatment_assignment(
    iwrs_db_path: Path,
    *,
    trial_id: str,
    subject_id: str,
    case_uid: str,
    actor: str,
) -> dict[str, Any]:
    """Unblind a subject's randomized treatment assignment in IWRS for SUSAR reporting.

    Per ICH E9 / GCP, every call records an immutable `iwrs_unblind_audit` row.
    Invoke ONLY when emergency / expedited SUSAR unblinding is regulatorily required.
    """
    accessed_at = utc_now_iso()
    with connect_iwrs(iwrs_db_path) as conn:
        row = conn.execute(
            """
            SELECT trial_id, subject_id, treatment_arm, is_investigational_drug
            FROM subject_randomization
            WHERE trial_id = ? AND subject_id = ?
            """,
            (trial_id, subject_id),
        ).fetchone()
        if row is None:
            raise LookupError(f"randomization_not_found:{trial_id}:{subject_id}")
        conn.execute(
            """
            INSERT INTO iwrs_unblind_audit (trial_id, subject_id, case_uid, actor, accessed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (trial_id, subject_id, case_uid, actor, accessed_at),
        )
        conn.commit()
    return {
        "trial_id": row["trial_id"],
        "subject_id": row["subject_id"],
        "treatment_arm": row["treatment_arm"],
        "is_investigational_drug": bool(row["is_investigational_drug"]),
        "accessed_at": accessed_at,
    }
