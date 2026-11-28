"""SOLUTION=1: Event-driven transactional outbox processor for expedited SAE & SUSAR reporting."""

from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from trialvault_pv.audit import append_audit_event
from trialvault_pv.cache import ProjectionCache
from trialvault_pv.config import DataPaths
from trialvault_pv.db import (
    connect_audit,
    connect_edc,
    connect_rsi,
    format_utc_iso,
    lookup_treatment_assignment,
    parse_utc_iso,
    utc_now_iso,
)
from trialvault_pv.fda_gateway import transmit_ind_submission
from trialvault_pv.listings import refresh_site_blinded_listing, write_dsmb_susar_listing
from trialvault_pv.search import SearchIndex

SERIOUSNESS_FLAGS: tuple[str, ...] = (
    "death",
    "life_threatening",
    "hospitalization",
    "disability",
    "congenital_anomaly",
    "medically_important",
)

CAUSALITY_SUSPECTED: frozenset[str] = frozenset({"related", "possibly_related"})

EDC_IDEMPOTENCY_SQL = """
CREATE TABLE IF NOT EXISTS e2b_idempotency_keys (
    site_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    case_uid TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (site_id, idempotency_key)
);
"""

PV_DB_SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS pv_case_assessments (
    case_uid TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    pv_status TEXT NOT NULL DEFAULT 'pending_assessment',
    is_serious INTEGER NOT NULL DEFAULT 0,
    is_suspected INTEGER NOT NULL DEFAULT 0,
    is_unexpected INTEGER NOT NULL DEFAULT 0,
    unblinded INTEGER NOT NULL DEFAULT 0,
    treatment_arm TEXT,
    is_investigational_drug INTEGER,
    latest_followup_num INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pv_ind_submissions (
    submission_id TEXT PRIMARY KEY,
    case_uid TEXT NOT NULL REFERENCES pv_case_assessments(case_uid),
    trial_id TEXT NOT NULL,
    submission_type TEXT NOT NULL,
    parent_submission_id TEXT,
    sequence_num INTEGER NOT NULL,
    report_clock TEXT NOT NULL,
    day0_at TEXT NOT NULL,
    due_at TEXT NOT NULL,
    status TEXT NOT NULL,
    gateway_ack_id TEXT,
    submitted_at TEXT,
    last_error TEXT,
    covered_followup_num INTEGER NOT NULL,
    covered_grade INTEGER NOT NULL,
    covered_seriousness_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(case_uid, sequence_num)
);

CREATE INDEX IF NOT EXISTS idx_pv_cases_trial ON pv_case_assessments(trial_id);
CREATE INDEX IF NOT EXISTS idx_pv_subs_case ON pv_ind_submissions(case_uid, sequence_num);
CREATE INDEX IF NOT EXISTS idx_pv_subs_status ON pv_ind_submissions(status);
"""


class ConflictError(Exception):
    def __init__(self, code: str, detail: str) -> None:
        super().__init__(detail)
        self.code = code
        self.detail = detail


def pv_db_path(paths: DataPaths) -> Path:
    return paths.root / "pv_cases.db"


def connect_pv(paths: DataPaths) -> sqlite3.Connection:
    db_file = pv_db_path(paths)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_file), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def ensure_pv_tables(paths: DataPaths) -> None:
    with connect_edc(paths.edc_db) as edc_conn:
        has_idem = edc_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'e2b_idempotency_keys'"
        ).fetchone()
        if has_idem is None:
            edc_conn.executescript(EDC_IDEMPOTENCY_SQL)
            edc_conn.commit()
    with connect_pv(paths) as pv_conn:
        has_pv = pv_conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'pv_ind_submissions'"
        ).fetchone()
        if has_pv is None:
            pv_conn.executescript(PV_DB_SCHEMA_SQL)
            pv_conn.commit()


def normalize_seriousness(raw: dict[str, Any] | None) -> dict[str, bool]:
    src = raw or {}
    return {flag: bool(src.get(flag, False)) for flag in SERIOUSNESS_FLAGS}


def _merge_reports(rows: list[sqlite3.Row | dict[str, Any]]) -> dict[str, Any]:
    by_fu = sorted(rows, key=lambda r: int(r["followup_num"]))
    highest = by_fu[-1]
    spon_rows = [r for r in by_fu if r["sponsor_causality"] is not None]
    sponsor_causality = spon_rows[-1]["sponsor_causality"] if spon_rows else None

    merged_ser = {flag: False for flag in SERIOUSNESS_FLAGS}
    max_grade = 1
    for r in by_fu:
        r_ser = normalize_seriousness(json.loads(r["seriousness_json"]))
        for flag in SERIOUSNESS_FLAGS:
            if r_ser[flag]:
                merged_ser[flag] = True
        if int(r["ctcae_grade"]) > max_grade:
            max_grade = int(r["ctcae_grade"])

    r0 = next((r for r in by_fu if int(r["followup_num"]) == 0), None)
    chrono = sorted(rows, key=lambda r: (parse_utc_iso(r["aware_at"]), int(r["followup_num"])))
    initial_aware_at = str(r0["aware_at"]) if r0 is not None else str(chrono[0]["aware_at"])
    latest_aware_at = str(chrono[-1]["aware_at"])
    has_unprocessed = any(int(r["processed"]) == 0 for r in rows)

    return {
        "latest_followup_num": int(highest["followup_num"]),
        "initial_aware_at": initial_aware_at,
        "latest_aware_at": latest_aware_at,
        "meddra_pt": str(highest["meddra_pt"]),
        "ctcae_grade": max_grade,
        "seriousness": merged_ser,
        "is_serious": any(merged_ser.values()),
        "investigator_causality": str(highest["investigator_causality"]),
        "sponsor_causality": sponsor_causality,
        "narrative": str(highest["narrative"]),
        "has_unprocessed": has_unprocessed,
    }


def _lookup_governing_ib(paths: DataPaths, trial_id: str, initial_aware_at: str) -> str | None:
    target_dt = parse_utc_iso(initial_aware_at)
    with connect_rsi(paths.rsi_db) as rsi_conn:
        ibs = rsi_conn.execute(
            "SELECT ib_version, effective_from FROM ib_versions WHERE trial_id = ? AND is_active = 1",
            (trial_id,),
        ).fetchall()
    eligible = [
        (parse_utc_iso(r["effective_from"]), str(r["ib_version"]))
        for r in ibs
        if parse_utc_iso(r["effective_from"]) <= target_dt
    ]
    if not eligible:
        return None
    eligible.sort(key=lambda item: item[0])
    return eligible[-1][1]


def _check_unexpected(
    paths: DataPaths,
    *,
    ib_version: str | None,
    meddra_pt: str,
    ctcae_grade: int,
    seriousness: dict[str, bool],
) -> bool:
    if ib_version is None:
        return True
    with connect_rsi(paths.rsi_db) as rsi_conn:
        term = rsi_conn.execute(
            """
            SELECT max_expected_grade, fatal_expected
            FROM rsi_expected_terms
            WHERE ib_version = ? AND meddra_pt = ?
            """,
            (ib_version, meddra_pt),
        ).fetchone()
    if term is None:
        return True
    if ctcae_grade > int(term["max_expected_grade"]):
        return True
    if (seriousness["death"] or seriousness["life_threatening"]) and int(term["fatal_expected"]) == 0:
        return True
    return False


def _evaluate_triage(
    paths: DataPaths,
    trial_id: str,
    initial_aware_at: str,
    merged: dict[str, Any],
) -> tuple[bool, bool, bool]:
    is_serious = bool(merged["is_serious"])
    inv_c = str(merged["investigator_causality"])
    spon_c = merged["sponsor_causality"]
    is_suspected = (inv_c in CAUSALITY_SUSPECTED) or (
        spon_c is not None and str(spon_c) in CAUSALITY_SUSPECTED
    )
    ib_ver = _lookup_governing_ib(paths, trial_id, initial_aware_at)
    is_unexpected = _check_unexpected(
        paths,
        ib_version=ib_ver,
        meddra_pt=str(merged["meddra_pt"]),
        ctcae_grade=int(merged["ctcae_grade"]),
        seriousness=merged["seriousness"],
    )
    return is_serious, is_suspected, is_unexpected


def ingest_e2b_report(
    paths: DataPaths,
    *,
    site_id: str,
    actor: str,
    idempotency_key: str | None,
    data: dict[str, Any],
) -> dict[str, Any]:
    ensure_pv_tables(paths)
    now = utc_now_iso()
    message_id = str(data["message_id"])
    case_uid = str(data["case_uid"])
    subject_id = str(data["subject_id"])
    followup_num = int(data.get("followup_num", 0))
    aware_at = format_utc_iso(parse_utc_iso(str(data["aware_at"])))
    meddra_pt = str(data["meddra_pt"])
    ctcae_grade = int(data["ctcae_grade"])
    seriousness = normalize_seriousness(data.get("seriousness"))
    is_serious = 1 if any(seriousness.values()) else 0
    investigator_causality = str(data["investigator_causality"])
    sponsor_causality = data.get("sponsor_causality")
    narrative = str(data["narrative"])
    cleaned_idem = idempotency_key.strip() if idempotency_key else None

    with connect_edc(paths.edc_db) as conn:
        conn.execute("BEGIN IMMEDIATE")
        subj = conn.execute(
            "SELECT subject_id, trial_id, site_id FROM subjects_blinded WHERE subject_id = ?",
            (subject_id,),
        ).fetchone()
        if subj is None:
            conn.rollback()
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            conn.rollback()
            raise PermissionError("cross_site_access")
        trial_id = str(subj["trial_id"])

        if cleaned_idem:
            idem_row = conn.execute(
                """
                SELECT case_uid, receipt_json
                FROM e2b_idempotency_keys
                WHERE site_id = ? AND idempotency_key = ?
                """,
                (site_id, cleaned_idem),
            ).fetchone()
            if idem_row is not None:
                conn.rollback()
                if idem_row["case_uid"] != case_uid:
                    raise ConflictError(
                        "idempotency_key_conflict",
                        "Idempotency-Key already bound to a different case_uid",
                    )
                return json.loads(idem_row["receipt_json"])

        existing_case = conn.execute(
            "SELECT subject_id, site_id, trial_id FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        if existing_case is not None and existing_case["subject_id"] != subject_id:
            conn.rollback()
            raise ConflictError(
                "duplicate_followup_conflict",
                "case_uid already bound to a different subject_id",
            )

        existing_slot = conn.execute(
            "SELECT message_id, followup_num, aware_at FROM case_reports WHERE case_uid = ? AND followup_num = ?",
            (case_uid, followup_num),
        ).fetchone()
        if existing_slot is not None:
            if existing_slot["message_id"] == message_id:
                receipt = {
                    "case_uid": case_uid,
                    "message_id": str(existing_slot["message_id"]),
                    "followup_num": int(existing_slot["followup_num"]),
                    "site_status": "received",
                    "aware_at": str(existing_slot["aware_at"]),
                }
                if cleaned_idem:
                    conn.execute(
                        """
                        INSERT INTO e2b_idempotency_keys (site_id, idempotency_key, case_uid, receipt_json, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (site_id, cleaned_idem, case_uid, json.dumps(receipt, sort_keys=True), now),
                    )
                    conn.commit()
                else:
                    conn.rollback()
                return receipt
            conn.rollback()
            raise ConflictError(
                "duplicate_followup_conflict",
                "Conflicting message_id for (case_uid, followup_num)",
            )

        existing_msg = conn.execute(
            "SELECT case_uid, followup_num FROM case_reports WHERE message_id = ?",
            (message_id,),
        ).fetchone()
        if existing_msg is not None:
            conn.rollback()
            raise ConflictError(
                "duplicate_followup_conflict",
                "message_id already bound to another safety report",
            )

        if existing_case is None:
            conn.execute(
                """
                INSERT INTO safety_cases (
                    case_uid, subject_id, trial_id, site_id, site_status, latest_followup_num,
                    initial_aware_at, latest_aware_at, meddra_pt, ctcae_grade, is_serious,
                    seriousness_json, investigator_causality, sponsor_causality, narrative,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'received', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    case_uid,
                    subject_id,
                    trial_id,
                    site_id,
                    followup_num,
                    aware_at,
                    aware_at,
                    meddra_pt,
                    ctcae_grade,
                    is_serious,
                    json.dumps(seriousness, sort_keys=True),
                    investigator_causality,
                    sponsor_causality,
                    narrative,
                    now,
                    now,
                ),
            )

        conn.execute(
            """
            INSERT INTO case_reports (
                report_id, message_id, case_uid, subject_id, site_id, followup_num,
                aware_at, meddra_pt, ctcae_grade, is_serious, seriousness_json,
                investigator_causality, sponsor_causality, narrative, received_at, processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                f"rep-{uuid.uuid4().hex[:12]}",
                message_id,
                case_uid,
                subject_id,
                site_id,
                followup_num,
                aware_at,
                meddra_pt,
                ctcae_grade,
                is_serious,
                json.dumps(seriousness, sort_keys=True),
                investigator_causality,
                sponsor_causality,
                narrative,
                now,
            ),
        )

        all_reports = conn.execute(
            "SELECT * FROM case_reports WHERE case_uid = ? ORDER BY rowid ASC",
            (case_uid,),
        ).fetchall()
        merged = _merge_reports(all_reports)
        conn.execute(
            """
            UPDATE safety_cases
            SET site_status = 'received',
                latest_followup_num = ?,
                initial_aware_at = ?,
                latest_aware_at = ?,
                meddra_pt = ?,
                ctcae_grade = ?,
                is_serious = ?,
                seriousness_json = ?,
                investigator_causality = ?,
                sponsor_causality = ?,
                narrative = ?,
                updated_at = ?
            WHERE case_uid = ?
            """,
            (
                merged["latest_followup_num"],
                merged["initial_aware_at"],
                merged["latest_aware_at"],
                merged["meddra_pt"],
                merged["ctcae_grade"],
                1 if merged["is_serious"] else 0,
                json.dumps(merged["seriousness"], sort_keys=True),
                merged["investigator_causality"],
                merged["sponsor_causality"],
                merged["narrative"],
                now,
                case_uid,
            ),
        )

        receipt = {
            "case_uid": case_uid,
            "message_id": message_id,
            "followup_num": followup_num,
            "site_status": "received",
            "aware_at": aware_at,
        }
        if cleaned_idem:
            conn.execute(
                """
                INSERT INTO e2b_idempotency_keys (site_id, idempotency_key, case_uid, receipt_json, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (site_id, cleaned_idem, case_uid, json.dumps(receipt, sort_keys=True), now),
            )

        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
            ) VALUES (?, 'e2b.report_received', ?, ?, ?, 'pending', ?)
            """,
            (
                f"evt-{uuid.uuid4().hex[:12]}",
                case_uid,
                site_id,
                json.dumps({"case_uid": case_uid, "followup_num": followup_num, "message_id": message_id}),
                now,
            ),
        )
        conn.commit()

    with connect_pv(paths) as pv_conn:
        pv_conn.execute("BEGIN IMMEDIATE")
        existing_pv = pv_conn.execute(
            "SELECT case_uid FROM pv_case_assessments WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        is_s, is_susp, is_unexp = _evaluate_triage(
            paths, trial_id, merged["initial_aware_at"], merged
        )
        if existing_pv is None:
            pv_conn.execute(
                """
                INSERT INTO pv_case_assessments (
                    case_uid, subject_id, trial_id, site_id, pv_status,
                    is_serious, is_suspected, is_unexpected, unblinded,
                    treatment_arm, is_investigational_drug, latest_followup_num, updated_at
                ) VALUES (?, ?, ?, ?, 'pending_assessment', ?, ?, ?, 0, NULL, NULL, ?, ?)
                """,
                (
                    case_uid,
                    subject_id,
                    trial_id,
                    site_id,
                    1 if is_s else 0,
                    1 if is_susp else 0,
                    1 if is_unexp else 0,
                    merged["latest_followup_num"],
                    now,
                ),
            )
        else:
            pv_conn.execute(
                """
                UPDATE pv_case_assessments
                SET is_serious = ?, is_suspected = ?, is_unexpected = ?,
                    latest_followup_num = ?, updated_at = ?
                WHERE case_uid = ?
                """,
                (
                    1 if is_s else 0,
                    1 if is_susp else 0,
                    1 if is_unexp else 0,
                    merged["latest_followup_num"],
                    now,
                    case_uid,
                ),
            )
        pv_conn.commit()

    with connect_audit(paths.audit_db) as audit_conn:
        append_audit_event(
            audit_conn,
            actor=actor,
            action="E2B_REPORT_RECEIVED",
            resource_type="safety_case",
            resource_ref=case_uid,
            payload={
                "case_uid": case_uid,
                "message_id": message_id,
                "followup_num": followup_num,
                "site_id": site_id,
                "aware_at": aware_at,
            },
            event_time=now,
        )
        audit_conn.commit()

    cache = ProjectionCache(paths.cache_file)
    cache.delete(f"blinded_case:{case_uid}")
    cache.delete(f"trial_blinded_summary:{trial_id}")
    return receipt


def _format_submission_row(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    return {
        "submission_id": str(row["submission_id"]),
        "case_uid": str(row["case_uid"]),
        "trial_id": str(row["trial_id"]),
        "submission_type": str(row["submission_type"]),
        "parent_submission_id": row["parent_submission_id"],
        "sequence_num": int(row["sequence_num"]),
        "report_clock": str(row["report_clock"]),
        "day0_at": str(row["day0_at"]),
        "due_at": str(row["due_at"]),
        "status": str(row["status"]),
        "gateway_ack_id": row["gateway_ack_id"],
        "submitted_at": row["submitted_at"],
        "last_error": row["last_error"],
    }


def get_pv_case_view(paths: DataPaths, case_uid: str) -> dict[str, Any]:
    ensure_pv_tables(paths)
    with connect_edc(paths.edc_db) as edc_conn:
        case_row = edc_conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        if case_row is None:
            raise LookupError("case_not_found")
        reports = edc_conn.execute(
            "SELECT * FROM case_reports WHERE case_uid = ? ORDER BY rowid ASC",
            (case_uid,),
        ).fetchall()

    merged = _merge_reports(reports)
    is_s, is_susp, is_unexp = _evaluate_triage(
        paths, str(case_row["trial_id"]), merged["initial_aware_at"], merged
    )

    with connect_pv(paths) as pv_conn:
        pv_row = pv_conn.execute(
            "SELECT * FROM pv_case_assessments WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        sub_rows = pv_conn.execute(
            "SELECT * FROM pv_ind_submissions WHERE case_uid = ? ORDER BY sequence_num ASC",
            (case_uid,),
        ).fetchall()

    subs = [_format_submission_row(s) for s in sub_rows]
    unblinded = bool(pv_row["unblinded"]) if pv_row is not None else False
    treatment_arm = str(pv_row["treatment_arm"]) if (pv_row is not None and unblinded) else None
    pv_status = str(pv_row["pv_status"]) if pv_row is not None else "pending_assessment"

    latest_sub = subs[-1] if subs else None
    return {
        "case_uid": str(case_row["case_uid"]),
        "subject_id": str(case_row["subject_id"]),
        "trial_id": str(case_row["trial_id"]),
        "site_id": str(case_row["site_id"]),
        "pv_status": pv_status,
        "is_serious": is_s,
        "is_suspected": is_susp,
        "is_unexpected": is_unexp,
        "unblinded": unblinded,
        "treatment_arm": treatment_arm,
        "report_clock": latest_sub["report_clock"] if latest_sub else None,
        "day0_at": latest_sub["day0_at"] if latest_sub else None,
        "due_at": latest_sub["due_at"] if latest_sub else None,
        "latest_followup_num": merged["latest_followup_num"],
        "submissions": subs,
    }


def list_pv_submissions(
    paths: DataPaths,
    *,
    trial_id: str | None = None,
    case_uid: str | None = None,
) -> list[dict[str, Any]]:
    ensure_pv_tables(paths)
    clauses: list[str] = []
    params: list[Any] = []
    if trial_id:
        clauses.append("trial_id = ?")
        params.append(trial_id)
    if case_uid:
        clauses.append("case_uid = ?")
        params.append(case_uid)
    where_sql = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with connect_pv(paths) as pv_conn:
        rows = pv_conn.execute(
            f"SELECT * FROM pv_ind_submissions {where_sql} ORDER BY case_uid ASC, sequence_num ASC",
            tuple(params),
        ).fetchall()
    return [_format_submission_row(r) for r in rows]


def refresh_dsmb_listing_for_trial(paths: DataPaths, trial_id: str, now_iso: str) -> None:
    ensure_pv_tables(paths)
    with connect_edc(paths.edc_db) as edc_conn:
        edc_cases = {
            str(r["case_uid"]): r
            for r in edc_conn.execute(
                "SELECT case_uid, site_id, meddra_pt, ctcae_grade FROM safety_cases WHERE trial_id = ?",
                (trial_id,),
            ).fetchall()
        }

    with connect_pv(paths) as pv_conn:
        pv_cases = pv_conn.execute(
            """
            SELECT * FROM pv_case_assessments
            WHERE trial_id = ? AND unblinded = 1 AND is_investigational_drug = 1
            ORDER BY case_uid ASC
            """,
            (trial_id,),
        ).fetchall()
        entries: list[dict[str, Any]] = []
        for pvc in pv_cases:
            cuid = str(pvc["case_uid"])
            subs = pv_conn.execute(
                "SELECT * FROM pv_ind_submissions WHERE case_uid = ? ORDER BY sequence_num ASC",
                (cuid,),
            ).fetchall()
            latest_sub = subs[-1] if subs else None
            edc_row = edc_cases.get(cuid)
            if edc_row is None:
                continue
            entries.append(
                {
                    "case_uid": cuid,
                    "subject_id": str(pvc["subject_id"]),
                    "site_id": str(edc_row["site_id"]),
                    "meddra_pt": str(edc_row["meddra_pt"]),
                    "ctcae_grade": int(edc_row["ctcae_grade"]),
                    "treatment_arm": str(pvc["treatment_arm"]),
                    "pv_status": str(pvc["pv_status"]),
                    "report_clock": str(latest_sub["report_clock"]) if latest_sub else None,
                    "day0_at": str(latest_sub["day0_at"]) if latest_sub else None,
                    "due_at": str(latest_sub["due_at"]) if latest_sub else None,
                    "latest_submission_status": str(latest_sub["status"]) if latest_sub else None,
                }
            )

    write_dsmb_susar_listing(paths, trial_id, entries, generated_at=now_iso)


def _sync_blinded_projections_for_reports(
    paths: DataPaths,
    case_uid: str,
    processed_report_ids: list[str],
    now_iso: str,
) -> None:
    with connect_edc(paths.edc_db) as edc_conn:
        edc_conn.execute("BEGIN IMMEDIATE")
        if processed_report_ids:
            placeholders = ",".join("?" for _ in processed_report_ids)
            edc_conn.execute(
                f"UPDATE case_reports SET processed = 1 WHERE report_id IN ({placeholders})",
                tuple(processed_report_ids),
            )
        all_reports = edc_conn.execute(
            "SELECT * FROM case_reports WHERE case_uid = ? ORDER BY rowid ASC",
            (case_uid,),
        ).fetchall()
        if not all_reports:
            edc_conn.rollback()
            return
        merged = _merge_reports(all_reports)
        site_status = "received" if merged["has_unprocessed"] else "assessed"
        edc_conn.execute(
            """
            UPDATE safety_cases
            SET site_status = ?,
                latest_followup_num = ?,
                initial_aware_at = ?,
                latest_aware_at = ?,
                meddra_pt = ?,
                ctcae_grade = ?,
                is_serious = ?,
                seriousness_json = ?,
                investigator_causality = ?,
                sponsor_causality = ?,
                narrative = ?,
                updated_at = ?
            WHERE case_uid = ?
            """,
            (
                site_status,
                merged["latest_followup_num"],
                merged["initial_aware_at"],
                merged["latest_aware_at"],
                merged["meddra_pt"],
                merged["ctcae_grade"],
                1 if merged["is_serious"] else 0,
                json.dumps(merged["seriousness"], sort_keys=True),
                merged["investigator_causality"],
                merged["sponsor_causality"],
                merged["narrative"],
                now_iso,
                case_uid,
            ),
        )
        row = edc_conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        edc_conn.commit()

    if row is None:
        return

    blinded_view = {
        "case_uid": str(row["case_uid"]),
        "subject_id": str(row["subject_id"]),
        "site_id": str(row["site_id"]),
        "trial_id": str(row["trial_id"]),
        "site_status": str(row["site_status"]),
        "latest_followup_num": int(row["latest_followup_num"]),
        "meddra_pt": str(row["meddra_pt"]),
        "ctcae_grade": int(row["ctcae_grade"]),
        "is_serious": bool(row["is_serious"]),
        "seriousness": normalize_seriousness(json.loads(row["seriousness_json"])),
        "investigator_causality": str(row["investigator_causality"]),
        "sponsor_causality": row["sponsor_causality"],
        "narrative": str(row["narrative"]),
    }

    SearchIndex(paths.search_db).upsert_case(
        case_uid=str(row["case_uid"]),
        subject_id=str(row["subject_id"]),
        site_id=str(row["site_id"]),
        trial_id=str(row["trial_id"]),
        content=f"{row['case_uid']} {row['subject_id']} {row['meddra_pt']} {row['narrative']}",
    )
    cache = ProjectionCache(paths.cache_file)
    cache.set(f"blinded_case:{case_uid}", blinded_view)
    cache.delete(f"trial_blinded_summary:{row['trial_id']}")
    refresh_site_blinded_listing(paths, str(row["site_id"]), generated_at=now_iso)


def converge_case(paths: DataPaths, case_uid: str, now_iso: str) -> None:
    """Atomically triage, selectively unblind, manage Day-0 clocks, and transmit IND submissions."""
    ensure_pv_tables(paths)

    with connect_edc(paths.edc_db) as edc_conn:
        case_row = edc_conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        if case_row is None:
            return
        reports = edc_conn.execute(
            "SELECT * FROM case_reports WHERE case_uid = ? ORDER BY rowid ASC",
            (case_uid,),
        ).fetchall()

    if not reports:
        return

    trial_id = str(case_row["trial_id"])
    site_id = str(case_row["site_id"])
    subject_id = str(case_row["subject_id"])
    report_ids = [str(r["report_id"]) for r in reports]
    chrono = sorted(reports, key=lambda r: (parse_utc_iso(r["aware_at"]), int(r["followup_num"])))
    merged = _merge_reports(reports)
    is_s, is_susp, is_unexp = _evaluate_triage(
        paths, trial_id, merged["initial_aware_at"], merged
    )
    qualifies_susar = is_s and is_susp and is_unexp

    # Refresh blinded site projections first (strictly decoupled from PV unblinding state)
    _sync_blinded_projections_for_reports(paths, case_uid, report_ids, now_iso)

    # Serialize concurrent workers on PV database write lock and re-read current PV state
    with connect_pv(paths) as pv_conn:
        pv_conn.execute("BEGIN IMMEDIATE")
        pv_row = pv_conn.execute(
            "SELECT * FROM pv_case_assessments WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()

        unblinded = bool(pv_row["unblinded"]) if pv_row is not None else False
        treatment_arm = pv_row["treatment_arm"] if pv_row is not None else None
        is_inv_drug = (
            bool(pv_row["is_investigational_drug"])
            if (pv_row is not None and pv_row["is_investigational_drug"] is not None)
            else None
        )

        if not qualifies_susar and not unblinded:
            if pv_row is None:
                pv_conn.execute(
                    """
                    INSERT INTO pv_case_assessments (
                        case_uid, subject_id, trial_id, site_id, pv_status,
                        is_serious, is_suspected, is_unexpected, unblinded,
                        treatment_arm, is_investigational_drug, latest_followup_num, updated_at
                    ) VALUES (?, ?, ?, ?, 'not_susar', ?, ?, ?, 0, NULL, NULL, ?, ?)
                    """,
                    (
                        case_uid,
                        subject_id,
                        trial_id,
                        site_id,
                        1 if is_s else 0,
                        1 if is_susp else 0,
                        1 if is_unexp else 0,
                        merged["latest_followup_num"],
                        now_iso,
                    ),
                )
            else:
                pv_conn.execute(
                    """
                    UPDATE pv_case_assessments
                    SET pv_status = 'not_susar',
                        is_serious = ?, is_suspected = ?, is_unexpected = ?,
                        latest_followup_num = ?, updated_at = ?
                    WHERE case_uid = ?
                    """,
                    (
                        1 if is_s else 0,
                        1 if is_susp else 0,
                        1 if is_unexp else 0,
                        merged["latest_followup_num"],
                        now_iso,
                        case_uid,
                    ),
                )
            pv_conn.commit()
            refresh_dsmb_listing_for_trial(paths, trial_id, now_iso)
            return

        # Selective IWRS unblinding — executed at most once per qualifying case
        if not unblinded:
            iwrs_info = lookup_treatment_assignment(
                paths.iwrs_db,
                trial_id=trial_id,
                subject_id=subject_id,
                case_uid=case_uid,
                actor="pv-worker",
            )
            unblinded = True
            treatment_arm = str(iwrs_info["treatment_arm"])
            is_inv_drug = bool(iwrs_info["is_investigational_drug"])

        if not is_inv_drug:
            if pv_row is None:
                pv_conn.execute(
                    """
                    INSERT INTO pv_case_assessments (
                        case_uid, subject_id, trial_id, site_id, pv_status,
                        is_serious, is_suspected, is_unexpected, unblinded,
                        treatment_arm, is_investigational_drug, latest_followup_num, updated_at
                    ) VALUES (?, ?, ?, ?, 'closed_non_investigational', ?, ?, ?, 1, ?, 0, ?, ?)
                    """,
                    (
                        case_uid,
                        subject_id,
                        trial_id,
                        site_id,
                        1 if is_s else 0,
                        1 if is_susp else 0,
                        1 if is_unexp else 0,
                        treatment_arm,
                        merged["latest_followup_num"],
                        now_iso,
                    ),
                )
            else:
                pv_conn.execute(
                    """
                    UPDATE pv_case_assessments
                    SET pv_status = 'closed_non_investigational',
                        is_serious = ?, is_suspected = ?, is_unexpected = ?,
                        unblinded = 1, treatment_arm = ?, is_investigational_drug = 0,
                        latest_followup_num = ?, updated_at = ?
                    WHERE case_uid = ?
                    """,
                    (
                        1 if is_s else 0,
                        1 if is_susp else 0,
                        1 if is_unexp else 0,
                        treatment_arm,
                        merged["latest_followup_num"],
                        now_iso,
                        case_uid,
                    ),
                )
            pv_conn.commit()
            refresh_dsmb_listing_for_trial(paths, trial_id, now_iso)
            return

        # Ensure PV assessment row exists for investigational-drug SUSAR
        if pv_row is None:
            pv_conn.execute(
                """
                INSERT INTO pv_case_assessments (
                    case_uid, subject_id, trial_id, site_id, pv_status,
                    is_serious, is_suspected, is_unexpected, unblinded,
                    treatment_arm, is_investigational_drug, latest_followup_num, updated_at
                ) VALUES (?, ?, ?, ?, 'susar_pending_submission', ?, ?, ?, 1, ?, 1, ?, ?)
                """,
                (
                    case_uid,
                    subject_id,
                    trial_id,
                    site_id,
                    1 if is_s else 0,
                    1 if is_susp else 0,
                    1 if is_unexp else 0,
                    treatment_arm,
                    merged["latest_followup_num"],
                    now_iso,
                ),
            )
        else:
            pv_conn.execute(
                """
                UPDATE pv_case_assessments
                SET is_serious = ?, is_suspected = ?, is_unexpected = ?,
                    unblinded = 1, treatment_arm = ?, is_investigational_drug = 1,
                    latest_followup_num = ?, updated_at = ?
                WHERE case_uid = ?
                """,
                (
                    1 if is_s else 0,
                    1 if is_susp else 0,
                    1 if is_unexp else 0,
                    treatment_arm,
                    merged["latest_followup_num"],
                    now_iso,
                    case_uid,
                ),
            )

        subs = pv_conn.execute(
            "SELECT * FROM pv_ind_submissions WHERE case_uid = ? ORDER BY sequence_num ASC",
            (case_uid,),
        ).fetchall()

        if not subs:
            # Find chronological report that first crossed the 3-leg SUSAR threshold
            thresh_idx = len(chrono) - 1
            thresh_merged = merged
            for idx in range(len(chrono)):
                prefix_merged = _merge_reports(chrono[: idx + 1])
                ps, psusp, punexp = _evaluate_triage(
                    paths, trial_id, merged["initial_aware_at"], prefix_merged
                )
                if ps and psusp and punexp:
                    thresh_idx = idx
                    thresh_merged = prefix_merged
                    break

            day0_dt = parse_utc_iso(str(chrono[thresh_idx]["aware_at"]))
            day0_at = format_utc_iso(day0_dt)
            thresh_ser = thresh_merged["seriousness"]
            if thresh_ser["death"] or thresh_ser["life_threatening"]:
                clock = "7_day"
                due_dt = day0_dt + timedelta(days=7)
            else:
                clock = "15_day"
                due_dt = day0_dt + timedelta(days=15)
                for later in chrono[thresh_idx + 1 :]:
                    later_ser = normalize_seriousness(json.loads(later["seriousness_json"]))
                    if later_ser["death"] or later_ser["life_threatening"]:
                        t_up = parse_utc_iso(str(later["aware_at"]))
                        clock = "7_day"
                        due_dt = min(due_dt, t_up + timedelta(days=7))
                        break

            sub_id = f"SUB-{uuid.uuid4().hex[:10].upper()}"
            pv_conn.execute(
                """
                INSERT INTO pv_ind_submissions (
                    submission_id, case_uid, trial_id, submission_type, parent_submission_id,
                    sequence_num, report_clock, day0_at, due_at, status,
                    gateway_ack_id, submitted_at, last_error,
                    covered_followup_num, covered_grade, covered_seriousness_json,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'initial', NULL, 1, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, ?, ?, ?, ?)
                """,
                (
                    sub_id,
                    case_uid,
                    trial_id,
                    clock,
                    day0_at,
                    format_utc_iso(due_dt),
                    merged["latest_followup_num"],
                    merged["ctcae_grade"],
                    json.dumps(merged["seriousness"], sort_keys=True),
                    now_iso,
                    now_iso,
                ),
            )
        else:
            latest_sub = subs[-1]
            if latest_sub["status"] in {"pending", "retry_pending"}:
                new_clock = str(latest_sub["report_clock"])
                new_due_dt = parse_utc_iso(str(latest_sub["due_at"]))
                if new_clock == "15_day" and (
                    merged["seriousness"]["death"] or merged["seriousness"]["life_threatening"]
                ):
                    for r in chrono:
                        r_ser = normalize_seriousness(json.loads(r["seriousness_json"]))
                        if r_ser["death"] or r_ser["life_threatening"]:
                            t_up = parse_utc_iso(str(r["aware_at"]))
                            new_clock = "7_day"
                            new_due_dt = min(new_due_dt, t_up + timedelta(days=7))
                            break
                pv_conn.execute(
                    """
                    UPDATE pv_ind_submissions
                    SET report_clock = ?, due_at = ?,
                        covered_followup_num = ?, covered_grade = ?, covered_seriousness_json = ?,
                        updated_at = ?
                    WHERE submission_id = ?
                    """,
                    (
                        new_clock,
                        format_utc_iso(new_due_dt),
                        merged["latest_followup_num"],
                        merged["ctcae_grade"],
                        json.dumps(merged["seriousness"], sort_keys=True),
                        now_iso,
                        str(latest_sub["submission_id"]),
                    ),
                )
            elif latest_sub["status"] == "submitted":
                prev_fu = int(latest_sub["covered_followup_num"])
                prev_grade = int(latest_sub["covered_grade"])
                prev_ser = normalize_seriousness(json.loads(latest_sub["covered_seriousness_json"]))
                newer_reports = [r for r in chrono if int(r["followup_num"]) > prev_fu]

                trig_report = None
                for r in newer_reports:
                    r_ser = normalize_seriousness(json.loads(r["seriousness_json"]))
                    grade_escalated = int(r["ctcae_grade"]) > prev_grade
                    added_flag = any(r_ser[f] and not prev_ser[f] for f in SERIOUSNESS_FLAGS)
                    if grade_escalated or added_flag:
                        trig_report = r
                        break

                if trig_report is not None:
                    t_fu_dt = parse_utc_iso(str(trig_report["aware_at"]))
                    fu_day0 = format_utc_iso(t_fu_dt)
                    trig_ser = normalize_seriousness(json.loads(trig_report["seriousness_json"]))
                    if (
                        trig_ser["death"]
                        or trig_ser["life_threatening"]
                        or prev_ser["death"]
                        or prev_ser["life_threatening"]
                    ):
                        fu_clock = "7_day"
                        fu_due_dt = t_fu_dt + timedelta(days=7)
                    else:
                        fu_clock = "15_day"
                        fu_due_dt = t_fu_dt + timedelta(days=15)
                        for r_after in newer_reports:
                            if parse_utc_iso(str(r_after["aware_at"])) >= t_fu_dt:
                                ra_ser = normalize_seriousness(json.loads(r_after["seriousness_json"]))
                                if ra_ser["death"] or ra_ser["life_threatening"]:
                                    fu_clock = "7_day"
                                    fu_due_dt = min(
                                        fu_due_dt,
                                        parse_utc_iso(str(r_after["aware_at"])) + timedelta(days=7),
                                    )
                                    break

                    fu_sub_id = f"SUB-{uuid.uuid4().hex[:10].upper()}"
                    pv_conn.execute(
                        """
                        INSERT INTO pv_ind_submissions (
                            submission_id, case_uid, trial_id, submission_type, parent_submission_id,
                            sequence_num, report_clock, day0_at, due_at, status,
                            gateway_ack_id, submitted_at, last_error,
                            covered_followup_num, covered_grade, covered_seriousness_json,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, 'followup', ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, ?, ?, ?, ?)
                        """,
                        (
                            fu_sub_id,
                            case_uid,
                            trial_id,
                            str(latest_sub["submission_id"]),
                            int(latest_sub["sequence_num"]) + 1,
                            fu_clock,
                            fu_day0,
                            format_utc_iso(fu_due_dt),
                            merged["latest_followup_num"],
                            merged["ctcae_grade"],
                            json.dumps(merged["seriousness"], sort_keys=True),
                            now_iso,
                            now_iso,
                        ),
                    )

        # Transmit any pending / retry_pending submission
        active_sub = pv_conn.execute(
            """
            SELECT * FROM pv_ind_submissions
            WHERE case_uid = ? AND status IN ('pending', 'retry_pending')
            ORDER BY sequence_num ASC LIMIT 1
            """,
            (case_uid,),
        ).fetchone()

        if active_sub is not None:
            sub_id = str(active_sub["submission_id"])
            seq_num = int(active_sub["sequence_num"])
            sub_payload = {
                "submission_id": sub_id,
                "case_uid": case_uid,
                "trial_id": trial_id,
                "site_id": site_id,
                "subject_id": subject_id,
                "treatment_arm": treatment_arm,
                "submission_type": str(active_sub["submission_type"]),
                "parent_submission_id": active_sub["parent_submission_id"],
                "sequence_num": seq_num,
                "report_clock": str(active_sub["report_clock"]),
                "day0_at": str(active_sub["day0_at"]),
                "due_at": str(active_sub["due_at"]),
                "meddra_pt": merged["meddra_pt"],
                "ctcae_grade": merged["ctcae_grade"],
                "seriousness": merged["seriousness"],
                "investigator_causality": merged["investigator_causality"],
                "sponsor_causality": merged["sponsor_causality"],
                "narrative": merged["narrative"],
            }
            sub_meta = {
                "case_uid": case_uid,
                "trial_id": trial_id,
                "submission_type": str(active_sub["submission_type"]),
                "sequence_num": seq_num,
                "report_clock": str(active_sub["report_clock"]),
            }
            try:
                gw_res = transmit_ind_submission(
                    paths.fda_gateway_dir,
                    submission_id=sub_id,
                    payload=sub_payload,
                    metadata=sub_meta,
                    transmitted_at=now_iso,
                )
                if not gw_res["accepted"]:
                    pv_conn.execute(
                        """
                        UPDATE pv_ind_submissions
                        SET status = 'retry_pending', last_error = ?, submitted_at = NULL,
                            gateway_ack_id = NULL, updated_at = ?
                        WHERE submission_id = ?
                        """,
                        (str(gw_res["error"]), now_iso, sub_id),
                    )
                    pv_conn.execute(
                        "UPDATE pv_case_assessments SET pv_status = 'susar_retry_pending', updated_at = ? WHERE case_uid = ?",
                        (now_iso, case_uid),
                    )
                else:
                    ack_id = str(gw_res["gateway_ack_id"])
                    submitted_at = str(gw_res["transmitted_at"])
                    with connect_audit(paths.audit_db) as audit_conn:
                        append_audit_event(
                            audit_conn,
                            actor="pv-worker",
                            action="IND_SAFETY_SUBMITTED",
                            resource_type="ind_submission",
                            resource_ref=sub_id,
                            payload={
                                "submission_id": sub_id,
                                "case_uid": case_uid,
                                "trial_id": trial_id,
                                "sequence_num": seq_num,
                                "gateway_ack_id": ack_id,
                            },
                            event_time=now_iso,
                        )
                        audit_conn.commit()
                    pv_conn.execute(
                        """
                        UPDATE pv_ind_submissions
                        SET status = 'submitted', gateway_ack_id = ?, submitted_at = ?,
                            last_error = NULL, updated_at = ?
                        WHERE submission_id = ?
                        """,
                        (ack_id, submitted_at, now_iso, sub_id),
                    )
                    pv_conn.execute(
                        "UPDATE pv_case_assessments SET pv_status = 'susar_submitted', updated_at = ? WHERE case_uid = ?",
                        (now_iso, case_uid),
                    )
            except Exception as exc:
                pv_conn.execute(
                    """
                    UPDATE pv_ind_submissions
                    SET status = 'retry_pending', last_error = ?, submitted_at = NULL,
                        gateway_ack_id = NULL, updated_at = ?
                    WHERE submission_id = ?
                    """,
                    (f"{type(exc).__name__}: {exc}", now_iso, sub_id),
                )
                pv_conn.execute(
                    "UPDATE pv_case_assessments SET pv_status = 'susar_retry_pending', updated_at = ? WHERE case_uid = ?",
                    (now_iso, case_uid),
                )
        else:
            pv_conn.execute(
                "UPDATE pv_case_assessments SET pv_status = 'susar_submitted', updated_at = ? WHERE case_uid = ?",
                (now_iso, case_uid),
            )

        pv_conn.commit()

    refresh_dsmb_listing_for_trial(paths, trial_id, now_iso)


def retry_pending_submissions(paths: DataPaths, now_iso: str) -> int:
    ensure_pv_tables(paths)
    with connect_pv(paths) as pv_conn:
        rows = pv_conn.execute(
            """
            SELECT DISTINCT case_uid
            FROM pv_ind_submissions
            WHERE status IN ('pending', 'retry_pending')
            ORDER BY case_uid ASC
            """
        ).fetchall()
    retried = 0
    for r in rows:
        converge_case(paths, str(r["case_uid"]), now_iso)
        retried += 1
    return retried
