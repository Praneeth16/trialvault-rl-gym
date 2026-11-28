"""Primary relational EDC database (SQLite WAL)."""

from __future__ import annotations

import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from trialvault.config import DataPaths, ensure_data_dirs

SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS trials (
    trial_id TEXT PRIMARY KEY,
    protocol_code TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS site_tokens (
    token_hash TEXT PRIMARY KEY,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    label TEXT NOT NULL,
    scopes TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS subjects (
    subject_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    mrn TEXT,
    full_name TEXT,
    email TEXT,
    phone TEXT,
    ssn TEXT,
    dob TEXT,
    age_at_enrollment INTEGER,
    age_bucket TEXT,
    street_address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    device_serial TEXT,
    ip_address TEXT,
    portal_url TEXT,
    emergency_contact_name TEXT,
    emergency_contact_phone TEXT,
    treatment_arm TEXT NOT NULL,
    randomization_date TEXT,
    status TEXT NOT NULL DEFAULT 'active',
    regulatory_hold INTEGER NOT NULL DEFAULT 0,
    hold_reason TEXT,
    anon_subject_ref TEXT,
    household_group_id TEXT,
    investigator_name TEXT,
    aliases_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS visits (
    visit_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    visit_number INTEGER NOT NULL,
    visit_date TEXT,
    days_from_randomization INTEGER NOT NULL,
    systolic_bp INTEGER NOT NULL,
    diastolic_bp INTEGER NOT NULL,
    clinical_notes TEXT NOT NULL,
    metadata_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS adverse_events (
    ae_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    meddra_term TEXT NOT NULL,
    ctcae_grade INTEGER NOT NULL,
    serious INTEGER NOT NULL DEFAULT 0,
    onset_date TEXT,
    onset_study_day INTEGER NOT NULL,
    relatedness TEXT NOT NULL,
    reporter_notes TEXT NOT NULL,
    payload_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS lab_results (
    lab_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    accession_no TEXT NOT NULL UNIQUE,
    analyte TEXT NOT NULL,
    value_num REAL NOT NULL,
    unit TEXT NOT NULL,
    reference_flag TEXT NOT NULL,
    collected_date TEXT,
    study_day INTEGER NOT NULL,
    raw_hl7_json TEXT NOT NULL,
    blob_key TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS data_queries (
    query_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    field_name TEXT NOT NULL,
    query_text TEXT NOT NULL,
    response_text TEXT,
    status TEXT NOT NULL DEFAULT 'open',
    opened_by TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS medical_coding_queue (
    coding_id TEXT PRIMARY KEY,
    ae_id TEXT NOT NULL REFERENCES adverse_events(ae_id),
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    verbatim_term TEXT NOT NULL,
    coded_meddra_pt TEXT NOT NULL,
    coder_notes TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'coded',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS epro_notifications (
    notif_id TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL REFERENCES subjects(subject_id) ON UPDATE CASCADE,
    site_id TEXT NOT NULL REFERENCES sites(site_id),
    channel TEXT NOT NULL,
    recipient TEXT,
    template TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    scheduled_for TEXT NOT NULL,
    sent_at TEXT
);

CREATE TABLE IF NOT EXISTS webhook_inbox (
    webhook_id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    accession_no TEXT NOT NULL UNIQUE,
    subject_id TEXT,
    mrn TEXT,
    raw_payload_json TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS scheduled_jobs (
    job_id TEXT PRIMARY KEY,
    job_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    run_after TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'scheduled',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    executed_at TEXT
);

CREATE TABLE IF NOT EXISTS export_manifests (
    snapshot_id TEXT PRIMARY KEY,
    trial_id TEXT NOT NULL REFERENCES trials(trial_id),
    file_path TEXT NOT NULL,
    sha256_checksum TEXT NOT NULL,
    subject_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    processed_at TEXT
);

CREATE TABLE IF NOT EXISTS audit_log (
    seq_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT NOT NULL,
    site_id TEXT NOT NULL,
    actor TEXT NOT NULL,
    action TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_ref TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    entry_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_pii_details (
    detail_id INTEGER PRIMARY KEY AUTOINCREMENT,
    seq_id INTEGER NOT NULL REFERENCES audit_log(seq_id),
    subject_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    detail_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_subjects_site ON subjects(site_id);
CREATE INDEX IF NOT EXISTS idx_subjects_mrn ON subjects(mrn);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox_events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_pii_subject ON audit_pii_details(subject_id);
CREATE INDEX IF NOT EXISTS idx_sched_due ON scheduled_jobs(status, run_after);
CREATE INDEX IF NOT EXISTS idx_webhook_inbox_subj ON webhook_inbox(subject_id);
"""


def utc_now_iso() -> str:
    override = os.environ.get("TRIALVAULT_CLOCK_ISO")
    if override:
        return override
    return datetime.now(timezone.utc).isoformat()


def hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def connect_edc(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_edc_db(paths: DataPaths) -> None:
    ensure_data_dirs(paths)
    with connect_edc(paths.edc_db) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()


def lookup_site_by_token(conn: sqlite3.Connection, bearer_token: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT s.site_id, s.trial_id, s.name, s.country, t.label, t.scopes,
               s.active AS site_active, t.active AS token_active
        FROM site_tokens t
        JOIN sites s ON s.site_id = t.site_id
        WHERE t.token_hash = ?
        """,
        (hash_token(bearer_token),),
    ).fetchone()
    if row is None or not row["site_active"] or not row["token_active"]:
        return None
    scopes = {s.strip() for s in row["scopes"].split(",") if s.strip()}
    return {
        "site_id": row["site_id"],
        "trial_id": row["trial_id"],
        "name": row["name"],
        "country": row["country"],
        "actor": row["label"],
        "scopes": scopes,
    }
