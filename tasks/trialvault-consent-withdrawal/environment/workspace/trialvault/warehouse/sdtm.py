"""CDISC SDTM study data tabulation warehouse (`cdisc_sdtm.db`)."""

from __future__ import annotations

import json
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from trialvault.config import DataPaths, ensure_data_dirs

SDTM_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS sdtm_dm (
    usubjid TEXT PRIMARY KEY,
    studyid TEXT NOT NULL,
    siteid TEXT NOT NULL,
    subjid TEXT NOT NULL,
    mrn TEXT,
    fullname TEXT,
    brthdtc TEXT,
    age INTEGER,
    age_bucket TEXT,
    state TEXT,
    postal_code TEXT,
    armcd TEXT NOT NULL,
    rfstdtc TEXT,
    invnam TEXT,
    status TEXT NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS sdtm_sv (
    sv_id TEXT PRIMARY KEY,
    usubjid TEXT NOT NULL REFERENCES sdtm_dm(usubjid) ON UPDATE CASCADE,
    studyid TEXT NOT NULL,
    siteid TEXT NOT NULL,
    visitnum INTEGER NOT NULL,
    visitdy INTEGER NOT NULL,
    svstdtc TEXT,
    sbp INTEGER NOT NULL,
    dbp INTEGER NOT NULL,
    coval TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sdtm_ae (
    ae_seq_id TEXT PRIMARY KEY,
    usubjid TEXT NOT NULL REFERENCES sdtm_dm(usubjid) ON UPDATE CASCADE,
    studyid TEXT NOT NULL,
    siteid TEXT NOT NULL,
    aedecod TEXT NOT NULL,
    aetoxgr INTEGER NOT NULL,
    aeser INTEGER NOT NULL,
    aestdtc TEXT,
    aestdy INTEGER NOT NULL,
    aerel TEXT NOT NULL,
    coval TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sdtm_lb (
    lb_seq_id TEXT PRIMARY KEY,
    usubjid TEXT NOT NULL REFERENCES sdtm_dm(usubjid) ON UPDATE CASCADE,
    studyid TEXT NOT NULL,
    siteid TEXT NOT NULL,
    lbrefid TEXT NOT NULL UNIQUE,
    lbtestcd TEXT NOT NULL,
    lbstresn REAL NOT NULL,
    lbstresu TEXT NOT NULL,
    lbnrind TEXT NOT NULL,
    lbdtc TEXT,
    lbdy INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS sdtm_suppqual (
    supp_id TEXT PRIMARY KEY,
    studyid TEXT NOT NULL,
    siteid TEXT NOT NULL,
    rdomain TEXT NOT NULL,
    usubjid TEXT NOT NULL REFERENCES sdtm_dm(usubjid) ON UPDATE CASCADE,
    qnam TEXT NOT NULL,
    qval TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sdtm_dm_study ON sdtm_dm(studyid);
CREATE INDEX IF NOT EXISTS idx_sdtm_supp_subj ON sdtm_suppqual(usubjid);
"""


def connect_sdtm(sdtm_db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(sdtm_db_path), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_sdtm_db(paths: DataPaths) -> None:
    ensure_data_dirs(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.executescript(SDTM_SCHEMA_SQL)
        conn.commit()


def record_sdtm_subject(
    paths: DataPaths,
    *,
    trial_id: str,
    site_id: str,
    subject_row: dict[str, Any],
) -> None:
    init_sdtm_db(paths)
    sid = subject_row["subject_id"]
    aliases = json.loads(subject_row.get("aliases_json") or "[]")
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO sdtm_dm (
                usubjid, studyid, siteid, subjid, mrn, fullname, brthdtc,
                age, age_bucket, state, postal_code, armcd, rfstdtc, invnam, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                sid,
                trial_id,
                site_id,
                sid,
                subject_row.get("mrn"),
                subject_row.get("full_name"),
                subject_row.get("dob"),
                subject_row.get("age_at_enrollment"),
                subject_row.get("age_bucket"),
                subject_row.get("state"),
                subject_row.get("postal_code"),
                subject_row["treatment_arm"],
                subject_row.get("randomization_date"),
                subject_row.get("investigator_name"),
                subject_row.get("status", "active"),
            ),
        )
        supps = [
            ("DM", "MRN", subject_row.get("mrn")),
            ("DM", "EMAIL", subject_row.get("email")),
            ("DM", "PHONE", subject_row.get("phone")),
            ("DM", "PORTAL_URL", subject_row.get("portal_url")),
            ("DM", "DEVICE_SN", subject_row.get("device_serial")),
            ("DM", "EMERG_CONTACT", subject_row.get("emergency_contact_name")),
        ]
        for alias in aliases:
            supps.append(("DM", "ALIAS", alias))
        for rdomain, qnam, qval in supps:
            if qval:
                conn.execute(
                    """
                    INSERT INTO sdtm_suppqual (supp_id, studyid, siteid, rdomain, usubjid, qnam, qval)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (f"SUPP-{uuid.uuid4().hex[:10].upper()}", trial_id, site_id, rdomain, sid, qnam, str(qval)),
                )
        conn.commit()


def record_sdtm_visit(
    paths: DataPaths,
    *,
    trial_id: str,
    site_id: str,
    subject_id: str,
    visit_row: dict[str, Any],
) -> None:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO sdtm_sv (
                sv_id, usubjid, studyid, siteid, visitnum, visitdy, svstdtc, sbp, dbp, coval
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                visit_row["visit_id"],
                subject_id,
                trial_id,
                site_id,
                int(visit_row["visit_number"]),
                int(visit_row["days_from_randomization"]),
                visit_row.get("visit_date"),
                int(visit_row["systolic_bp"]),
                int(visit_row["diastolic_bp"]),
                visit_row["clinical_notes"],
            ),
        )
        conn.commit()


def record_sdtm_ae(
    paths: DataPaths,
    *,
    trial_id: str,
    site_id: str,
    subject_id: str,
    ae_row: dict[str, Any],
) -> None:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO sdtm_ae (
                ae_seq_id, usubjid, studyid, siteid, aedecod, aetoxgr, aeser, aestdtc, aestdy, aerel, coval
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ae_row["ae_id"],
                subject_id,
                trial_id,
                site_id,
                ae_row["meddra_term"],
                int(ae_row["ctcae_grade"]),
                int(ae_row["serious"]),
                ae_row.get("onset_date"),
                int(ae_row["onset_study_day"]),
                ae_row["relatedness"],
                ae_row["reporter_notes"],
            ),
        )
        conn.commit()


def record_sdtm_lab(
    paths: DataPaths,
    *,
    trial_id: str,
    site_id: str,
    subject_id: str,
    lab_id: str,
    accession_no: str,
    analyte: str,
    value_num: float,
    unit: str,
    reference_flag: str,
    collected_date: str | None,
    study_day: int,
) -> None:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO sdtm_lb (
                lb_seq_id, usubjid, studyid, siteid, lbrefid, lbtestcd,
                lbstresn, lbstresu, lbnrind, lbdtc, lbdy
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                lab_id,
                subject_id,
                trial_id,
                site_id,
                accession_no,
                analyte,
                float(value_num),
                unit,
                reference_flag,
                collected_date,
                int(study_day),
            ),
        )
        conn.commit()


def compute_sdtm_summary(paths: DataPaths, trial_id: str) -> dict[str, Any]:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        arms = conn.execute(
            "SELECT armcd, COUNT(*) AS cnt FROM sdtm_dm WHERE studyid = ? GROUP BY armcd ORDER BY armcd ASC",
            (trial_id,),
        ).fetchall()
        if not arms:
            raise LookupError("trial_not_found")
        arm_counts = {r["armcd"]: int(r["cnt"]) for r in arms}
        sv_cnt = int(conn.execute("SELECT COUNT(*) FROM sdtm_sv WHERE studyid = ?", (trial_id,)).fetchone()[0])
        ae_cnt = int(conn.execute("SELECT COUNT(*) FROM sdtm_ae WHERE studyid = ?", (trial_id,)).fetchone()[0])
        lb_cnt = int(conn.execute("SELECT COUNT(*) FROM sdtm_lb WHERE studyid = ?", (trial_id,)).fetchone()[0])
    return {
        "trial_id": trial_id,
        "dm_count": sum(arm_counts.values()),
        "arm_counts": arm_counts,
        "sv_count": sv_cnt,
        "ae_count": ae_cnt,
        "lb_count": lb_cnt,
    }
