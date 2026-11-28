"""SOLUTION=2: Lease-based PV submission state machine reconciler."""

from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from trialvault_pv.audit import append_audit_event
from trialvault_pv.cache import ProjectionCache
from trialvault_pv.case_reducer import (
    SERIOUSNESS_FLAGS,
    ReportEvent,
    RsiTermRule,
    SubmissionSnapshot,
    compute_initial_aware_at,
    fmt_iso_utc,
    parse_iso_utc,
    reduce_case_history,
)
from trialvault_pv.config import DataPaths
from trialvault_pv.db import (
    connect_audit,
    connect_edc,
    connect_rsi,
    lookup_treatment_assignment,
    utc_now_iso,
)
from trialvault_pv.fda_gateway import transmit_ind_submission
from trialvault_pv.listings import refresh_site_blinded_listing, write_dsmb_susar_listing
from trialvault_pv.search import SearchIndex

EDC_RECEIPT_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS e2b_receipt_ledger (
    site_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    case_uid TEXT NOT NULL,
    receipt_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    PRIMARY KEY (site_id, idempotency_key)
);
"""

STATE_MACHINE_SQL = """
PRAGMA foreign_keys = ON;
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS worker_case_leases (
    case_uid TEXT PRIMARY KEY,
    holder_token TEXT NOT NULL,
    expires_epoch REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS sm_case_records (
    case_uid TEXT PRIMARY KEY,
    subject_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    site_id TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL DEFAULT 'pending_assessment',
    unblinded INTEGER NOT NULL DEFAULT 0,
    treatment_arm TEXT,
    is_investigational_drug INTEGER,
    reconciled_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sm_submission_records (
    submission_id TEXT PRIMARY KEY,
    case_uid TEXT NOT NULL REFERENCES sm_case_records(case_uid),
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
    updated_at TEXT NOT NULL,
    UNIQUE(case_uid, sequence_num)
);
"""


class E2BConflict(Exception):
    def __init__(self, error_code: str, detail: str) -> None:
        super().__init__(detail)
        self.error_code = error_code
        self.detail = detail


def sm_db_path(paths: DataPaths) -> Path:
    return paths.root / "pv_state_machine.db"


def connect_sm(paths: DataPaths) -> sqlite3.Connection:
    target = sm_db_path(paths)
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def init_sm_storage(paths: DataPaths) -> None:
    with connect_edc(paths.edc_db) as edc_conn:
        edc_conn.executescript(EDC_RECEIPT_LEDGER_SQL)
        edc_conn.commit()
    with connect_sm(paths) as sm_conn:
        sm_conn.executescript(STATE_MACHINE_SQL)
        sm_conn.commit()


def _normalize_ser(raw: dict[str, Any] | None) -> dict[str, bool]:
    src = raw or {}
    return {flag: bool(src.get(flag, False)) for flag in SERIOUSNESS_FLAGS}


@contextmanager
def acquire_case_lease(paths: DataPaths, case_uid: str, ttl_sec: float = 30.0) -> Iterator[str]:
    """Acquire an exclusive per-case lease row, polling briefly if contended."""
    holder = uuid.uuid4().hex
    deadline = time.monotonic() + 25.0
    while True:
        now_ep = time.time()
        with connect_sm(paths) as sm_conn:
            sm_conn.execute("BEGIN IMMEDIATE")
            row = sm_conn.execute(
                "SELECT holder_token, expires_epoch FROM worker_case_leases WHERE case_uid = ?",
                (case_uid,),
            ).fetchone()
            if row is None or float(row["expires_epoch"]) <= now_ep:
                sm_conn.execute(
                    """
                    INSERT INTO worker_case_leases (case_uid, holder_token, expires_epoch)
                    VALUES (?, ?, ?)
                    ON CONFLICT(case_uid) DO UPDATE SET
                        holder_token = excluded.holder_token,
                        expires_epoch = excluded.expires_epoch
                    """,
                    (case_uid, holder, now_ep + ttl_sec),
                )
                sm_conn.commit()
                break
            sm_conn.rollback()
        if time.monotonic() >= deadline:
            raise TimeoutError(f"lease_timeout:{case_uid}")
        time.sleep(0.03)

    try:
        yield holder
    finally:
        with connect_sm(paths) as sm_conn:
            sm_conn.execute("BEGIN IMMEDIATE")
            sm_conn.execute(
                "DELETE FROM worker_case_leases WHERE case_uid = ? AND holder_token = ?",
                (case_uid, holder),
            )
            sm_conn.commit()


def _load_rsi_map(paths: DataPaths, trial_id: str, as_of_iso: str) -> dict[str, RsiTermRule] | None:
    as_of_dt = parse_iso_utc(as_of_iso)
    with connect_rsi(paths.rsi_db) as rsi_conn:
        ibs = rsi_conn.execute(
            "SELECT ib_version, effective_from FROM ib_versions WHERE trial_id = ? AND is_active = 1",
            (trial_id,),
        ).fetchall()
        valid = [
            (parse_iso_utc(str(r["effective_from"])), str(r["ib_version"]))
            for r in ibs
            if parse_iso_utc(str(r["effective_from"])) <= as_of_dt
        ]
        if not valid:
            return None
        valid.sort(key=lambda item: item[0])
        chosen_ib = valid[-1][1]
        terms = rsi_conn.execute(
            "SELECT meddra_pt, max_expected_grade, fatal_expected FROM rsi_expected_terms WHERE ib_version = ?",
            (chosen_ib,),
        ).fetchall()
    return {
        str(t["meddra_pt"]): RsiTermRule(
            meddra_pt=str(t["meddra_pt"]),
            max_expected_grade=int(t["max_expected_grade"]),
            fatal_expected=bool(t["fatal_expected"]),
        )
        for t in terms
    }


def _load_report_events(edc_conn: sqlite3.Connection, case_uid: str) -> list[ReportEvent]:
    rows = edc_conn.execute(
        "SELECT * FROM case_reports WHERE case_uid = ?",
        (case_uid,),
    ).fetchall()
    return [
        ReportEvent(
            report_id=str(r["report_id"]),
            message_id=str(r["message_id"]),
            followup_num=int(r["followup_num"]),
            aware_at=str(r["aware_at"]),
            meddra_pt=str(r["meddra_pt"]),
            ctcae_grade=int(r["ctcae_grade"]),
            seriousness=_normalize_ser(json.loads(r["seriousness_json"])),
            investigator_causality=str(r["investigator_causality"]),
            sponsor_causality=r["sponsor_causality"],
            narrative=str(r["narrative"]),
            processed=bool(r["processed"]),
        )
        for r in rows
    ]


def _load_sub_snapshots(sm_conn: sqlite3.Connection, case_uid: str) -> list[SubmissionSnapshot]:
    rows = sm_conn.execute(
        "SELECT * FROM sm_submission_records WHERE case_uid = ? ORDER BY sequence_num ASC",
        (case_uid,),
    ).fetchall()
    return [
        SubmissionSnapshot(
            submission_id=str(r["submission_id"]),
            sequence_num=int(r["sequence_num"]),
            submission_type=str(r["submission_type"]),
            parent_submission_id=r["parent_submission_id"],
            report_clock=str(r["report_clock"]),
            day0_at=str(r["day0_at"]),
            due_at=str(r["due_at"]),
            status=str(r["status"]),
            covered_followup_num=int(r["covered_followup_num"]),
            covered_grade=int(r["covered_grade"]),
            covered_seriousness=_normalize_ser(json.loads(r["covered_seriousness_json"])),
        )
        for r in rows
    ]


def receive_e2b_submission(
    paths: DataPaths,
    *,
    site_id: str,
    actor: str,
    idempotency_key: str | None,
    payload: dict[str, Any],
) -> dict[str, Any]:
    init_sm_storage(paths)
    now = utc_now_iso()
    msg_id = str(payload["message_id"])
    case_uid = str(payload["case_uid"])
    subject_id = str(payload["subject_id"])
    fu_num = int(payload.get("followup_num", 0))
    aware_at = fmt_iso_utc(parse_iso_utc(str(payload["aware_at"])))
    meddra_pt = str(payload["meddra_pt"])
    grade = int(payload["ctcae_grade"])
    ser = _normalize_ser(payload.get("seriousness"))
    inv_c = str(payload["investigator_causality"])
    spon_c = payload.get("sponsor_causality")
    narrative = str(payload["narrative"])
    idem = idempotency_key.strip() if idempotency_key else None

    with connect_edc(paths.edc_db) as edc_conn:
        edc_conn.execute("BEGIN IMMEDIATE")
        subj = edc_conn.execute(
            "SELECT subject_id, trial_id, site_id FROM subjects_blinded WHERE subject_id = ?",
            (subject_id,),
        ).fetchone()
        if subj is None:
            edc_conn.rollback()
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            edc_conn.rollback()
            raise PermissionError("cross_site_access")
        trial_id = str(subj["trial_id"])

        if idem:
            ledger = edc_conn.execute(
                "SELECT case_uid, receipt_json FROM e2b_receipt_ledger WHERE site_id = ? AND idempotency_key = ?",
                (site_id, idem),
            ).fetchone()
            if ledger is not None:
                edc_conn.rollback()
                if ledger["case_uid"] != case_uid:
                    raise E2BConflict(
                        "idempotency_key_conflict",
                        "Idempotency-Key already bound to a different case_uid",
                    )
                return json.loads(ledger["receipt_json"])

        existing_case = edc_conn.execute(
            "SELECT subject_id FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        if existing_case is not None and existing_case["subject_id"] != subject_id:
            edc_conn.rollback()
            raise E2BConflict(
                "duplicate_followup_conflict",
                "case_uid already bound to a different subject_id",
            )

        slot = edc_conn.execute(
            "SELECT message_id, followup_num, aware_at FROM case_reports WHERE case_uid = ? AND followup_num = ?",
            (case_uid, fu_num),
        ).fetchone()
        if slot is not None:
            if slot["message_id"] == msg_id:
                receipt = {
                    "case_uid": case_uid,
                    "message_id": str(slot["message_id"]),
                    "followup_num": int(slot["followup_num"]),
                    "site_status": "received",
                    "aware_at": str(slot["aware_at"]),
                }
                if idem:
                    edc_conn.execute(
                        "INSERT INTO e2b_receipt_ledger VALUES (?, ?, ?, ?, ?)",
                        (site_id, idem, case_uid, json.dumps(receipt, sort_keys=True), now),
                    )
                    edc_conn.commit()
                else:
                    edc_conn.rollback()
                return receipt
            edc_conn.rollback()
            raise E2BConflict(
                "duplicate_followup_conflict",
                "Conflicting message_id for (case_uid, followup_num)",
            )

        dup_msg = edc_conn.execute(
            "SELECT case_uid FROM case_reports WHERE message_id = ?",
            (msg_id,),
        ).fetchone()
        if dup_msg is not None:
            edc_conn.rollback()
            raise E2BConflict("duplicate_followup_conflict", "Duplicate message_id")

        if existing_case is None:
            edc_conn.execute(
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
                    fu_num,
                    aware_at,
                    aware_at,
                    meddra_pt,
                    grade,
                    1 if any(ser.values()) else 0,
                    json.dumps(ser, sort_keys=True),
                    inv_c,
                    spon_c,
                    narrative,
                    now,
                    now,
                ),
            )

        edc_conn.execute(
            """
            INSERT INTO case_reports (
                report_id, message_id, case_uid, subject_id, site_id, followup_num,
                aware_at, meddra_pt, ctcae_grade, is_serious, seriousness_json,
                investigator_causality, sponsor_causality, narrative, received_at, processed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                f"rep-{uuid.uuid4().hex[:12]}",
                msg_id,
                case_uid,
                subject_id,
                site_id,
                fu_num,
                aware_at,
                meddra_pt,
                grade,
                1 if any(ser.values()) else 0,
                json.dumps(ser, sort_keys=True),
                inv_c,
                spon_c,
                narrative,
                now,
            ),
        )

        evts = _load_report_events(edc_conn, case_uid)
        reduced = reduce_case_history(evts, None, [])
        edc_conn.execute(
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
                reduced.latest_followup_num,
                reduced.initial_aware_at,
                reduced.latest_aware_at,
                reduced.meddra_pt,
                reduced.ctcae_grade,
                1 if reduced.is_serious else 0,
                json.dumps(reduced.seriousness, sort_keys=True),
                reduced.investigator_causality,
                reduced.sponsor_causality,
                reduced.narrative,
                now,
                case_uid,
            ),
        )

        receipt = {
            "case_uid": case_uid,
            "message_id": msg_id,
            "followup_num": fu_num,
            "site_status": "received",
            "aware_at": aware_at,
        }
        if idem:
            edc_conn.execute(
                "INSERT INTO e2b_receipt_ledger VALUES (?, ?, ?, ?, ?)",
                (site_id, idem, case_uid, json.dumps(receipt, sort_keys=True), now),
            )
        edc_conn.commit()

    with connect_sm(paths) as sm_conn:
        sm_conn.execute("BEGIN IMMEDIATE")
        sm_conn.execute(
            """
            INSERT INTO sm_case_records (
                case_uid, subject_id, trial_id, site_id, lifecycle_state,
                unblinded, treatment_arm, is_investigational_drug, reconciled_at
            ) VALUES (?, ?, ?, ?, 'pending_assessment', 0, NULL, NULL, ?)
            ON CONFLICT(case_uid) DO NOTHING
            """,
            (case_uid, subject_id, trial_id, site_id, now),
        )
        sm_conn.commit()

    with connect_audit(paths.audit_db) as audit_conn:
        append_audit_event(
            audit_conn,
            actor=actor,
            action="E2B_REPORT_RECEIVED",
            resource_type="safety_case",
            resource_ref=case_uid,
            payload={
                "case_uid": case_uid,
                "message_id": msg_id,
                "followup_num": fu_num,
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


def _sub_dict(row: sqlite3.Row) -> dict[str, Any]:
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


def read_pv_case_assessment(paths: DataPaths, case_uid: str) -> dict[str, Any]:
    init_sm_storage(paths)
    with connect_edc(paths.edc_db) as edc_conn:
        case_row = edc_conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        if case_row is None:
            raise LookupError("case_not_found")
        evts = _load_report_events(edc_conn, case_uid)

    trial_id = str(case_row["trial_id"])
    init_aware = compute_initial_aware_at(evts)
    rsi_map = _load_rsi_map(paths, trial_id, init_aware)
    reduced = reduce_case_history(evts, rsi_map, [])

    with connect_sm(paths) as sm_conn:
        sm_row = sm_conn.execute(
            "SELECT * FROM sm_case_records WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        sub_rows = sm_conn.execute(
            "SELECT * FROM sm_submission_records WHERE case_uid = ? ORDER BY sequence_num ASC",
            (case_uid,),
        ).fetchall()

    subs = [_sub_dict(s) for s in sub_rows]
    unblinded = bool(sm_row["unblinded"]) if sm_row is not None else False
    arm = str(sm_row["treatment_arm"]) if (sm_row is not None and unblinded) else None
    pv_status = str(sm_row["lifecycle_state"]) if sm_row is not None else "pending_assessment"
    tail = subs[-1] if subs else None

    return {
        "case_uid": str(case_row["case_uid"]),
        "subject_id": str(case_row["subject_id"]),
        "trial_id": trial_id,
        "site_id": str(case_row["site_id"]),
        "pv_status": pv_status,
        "is_serious": reduced.is_serious,
        "is_suspected": reduced.is_suspected,
        "is_unexpected": reduced.is_unexpected,
        "unblinded": unblinded,
        "treatment_arm": arm,
        "report_clock": tail["report_clock"] if tail else None,
        "day0_at": tail["day0_at"] if tail else None,
        "due_at": tail["due_at"] if tail else None,
        "latest_followup_num": reduced.latest_followup_num,
        "submissions": subs,
    }


def query_pv_submissions(
    paths: DataPaths,
    *,
    trial_id: str | None = None,
    case_uid: str | None = None,
) -> list[dict[str, Any]]:
    init_sm_storage(paths)
    filters: list[str] = []
    args: list[Any] = []
    if trial_id:
        filters.append("trial_id = ?")
        args.append(trial_id)
    if case_uid:
        filters.append("case_uid = ?")
        args.append(case_uid)
    where = f"WHERE {' AND '.join(filters)}" if filters else ""
    with connect_sm(paths) as sm_conn:
        rows = sm_conn.execute(
            f"SELECT * FROM sm_submission_records {where} ORDER BY case_uid ASC, sequence_num ASC",
            tuple(args),
        ).fetchall()
    return [_sub_dict(r) for r in rows]


def _rebuild_dsmb_for_trial(paths: DataPaths, trial_id: str, now_iso: str) -> None:
    with connect_edc(paths.edc_db) as edc_conn:
        cases_by_id = {
            str(r["case_uid"]): r
            for r in edc_conn.execute(
                "SELECT case_uid, site_id, meddra_pt, ctcae_grade FROM safety_cases WHERE trial_id = ?",
                (trial_id,),
            ).fetchall()
        }
    with connect_sm(paths) as sm_conn:
        sm_rows = sm_conn.execute(
            """
            SELECT * FROM sm_case_records
            WHERE trial_id = ? AND unblinded = 1 AND is_investigational_drug = 1
            ORDER BY case_uid ASC
            """,
            (trial_id,),
        ).fetchall()
        susar_items: list[dict[str, Any]] = []
        for smr in sm_rows:
            cuid = str(smr["case_uid"])
            edc_c = cases_by_id.get(cuid)
            if edc_c is None:
                continue
            subs = sm_conn.execute(
                "SELECT * FROM sm_submission_records WHERE case_uid = ? ORDER BY sequence_num ASC",
                (cuid,),
            ).fetchall()
            tail = subs[-1] if subs else None
            susar_items.append(
                {
                    "case_uid": cuid,
                    "subject_id": str(smr["subject_id"]),
                    "site_id": str(edc_c["site_id"]),
                    "meddra_pt": str(edc_c["meddra_pt"]),
                    "ctcae_grade": int(edc_c["ctcae_grade"]),
                    "treatment_arm": str(smr["treatment_arm"]),
                    "pv_status": str(smr["lifecycle_state"]),
                    "report_clock": str(tail["report_clock"]) if tail else None,
                    "day0_at": str(tail["day0_at"]) if tail else None,
                    "due_at": str(tail["due_at"]) if tail else None,
                    "latest_submission_status": str(tail["status"]) if tail else None,
                }
            )
    write_dsmb_susar_listing(paths, trial_id, susar_items, generated_at=now_iso)


def _commit_blinded_views(
    paths: DataPaths,
    case_uid: str,
    seen_report_ids: list[str],
    now_iso: str,
) -> None:
    with connect_edc(paths.edc_db) as edc_conn:
        edc_conn.execute("BEGIN IMMEDIATE")
        if seen_report_ids:
            marks = ",".join("?" for _ in seen_report_ids)
            edc_conn.execute(
                f"UPDATE case_reports SET processed = 1 WHERE report_id IN ({marks})",
                tuple(seen_report_ids),
            )
        evts = _load_report_events(edc_conn, case_uid)
        if not evts:
            edc_conn.rollback()
            return
        reduced = reduce_case_history(evts, None, [])
        site_status = "received" if reduced.has_unprocessed else "assessed"
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
                reduced.latest_followup_num,
                reduced.initial_aware_at,
                reduced.latest_aware_at,
                reduced.meddra_pt,
                reduced.ctcae_grade,
                1 if reduced.is_serious else 0,
                json.dumps(reduced.seriousness, sort_keys=True),
                reduced.investigator_causality,
                reduced.sponsor_causality,
                reduced.narrative,
                now_iso,
                case_uid,
            ),
        )
        row = edc_conn.execute("SELECT * FROM safety_cases WHERE case_uid = ?", (case_uid,)).fetchone()
        edc_conn.commit()

    if row is None:
        return

    view = {
        "case_uid": str(row["case_uid"]),
        "subject_id": str(row["subject_id"]),
        "site_id": str(row["site_id"]),
        "trial_id": str(row["trial_id"]),
        "site_status": str(row["site_status"]),
        "latest_followup_num": int(row["latest_followup_num"]),
        "meddra_pt": str(row["meddra_pt"]),
        "ctcae_grade": int(row["ctcae_grade"]),
        "is_serious": bool(row["is_serious"]),
        "seriousness": _normalize_ser(json.loads(row["seriousness_json"])),
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
    cache.set(f"blinded_case:{case_uid}", view)
    cache.delete(f"trial_blinded_summary:{row['trial_id']}")
    refresh_site_blinded_listing(paths, str(row["site_id"]), generated_at=now_iso)


def reconcile_single_case(paths: DataPaths, case_uid: str, now_iso: str) -> None:
    """Reconcile a safety case under an exclusive lease using `reduce_case_history`."""
    init_sm_storage(paths)

    with acquire_case_lease(paths, case_uid):
        with connect_edc(paths.edc_db) as edc_conn:
            case_row = edc_conn.execute(
                "SELECT * FROM safety_cases WHERE case_uid = ?",
                (case_uid,),
            ).fetchone()
            if case_row is None:
                return
            evts = _load_report_events(edc_conn, case_uid)

        if not evts:
            return

        trial_id = str(case_row["trial_id"])
        site_id = str(case_row["site_id"])
        subject_id = str(case_row["subject_id"])
        seen_ids = [e.report_id for e in evts]

        with connect_sm(paths) as sm_conn:
            sm_row = sm_conn.execute(
                "SELECT * FROM sm_case_records WHERE case_uid = ?",
                (case_uid,),
            ).fetchone()
            existing_subs = _load_sub_snapshots(sm_conn, case_uid)

        init_aware = compute_initial_aware_at(evts)
        rsi_map = _load_rsi_map(paths, trial_id, init_aware)
        reduced = reduce_case_history(evts, rsi_map, existing_subs)

        was_unblinded = bool(sm_row["unblinded"]) if sm_row is not None else False
        arm = sm_row["treatment_arm"] if sm_row is not None else None
        is_inv = (
            bool(sm_row["is_investigational_drug"])
            if (sm_row is not None and sm_row["is_investigational_drug"] is not None)
            else None
        )

        if not reduced.qualifies_three_legs and not was_unblinded:
            with connect_sm(paths) as sm_conn:
                sm_conn.execute("BEGIN IMMEDIATE")
                sm_conn.execute(
                    """
                    INSERT INTO sm_case_records (
                        case_uid, subject_id, trial_id, site_id, lifecycle_state,
                        unblinded, treatment_arm, is_investigational_drug, reconciled_at
                    ) VALUES (?, ?, ?, ?, 'not_susar', 0, NULL, NULL, ?)
                    ON CONFLICT(case_uid) DO UPDATE SET
                        lifecycle_state = 'not_susar',
                        reconciled_at = excluded.reconciled_at
                    """,
                    (case_uid, subject_id, trial_id, site_id, now_iso),
                )
                sm_conn.commit()
            _commit_blinded_views(paths, case_uid, seen_ids, now_iso)
            _rebuild_dsmb_for_trial(paths, trial_id, now_iso)
            return

        if not was_unblinded:
            iwrs_res = lookup_treatment_assignment(
                paths.iwrs_db,
                trial_id=trial_id,
                subject_id=subject_id,
                case_uid=case_uid,
                actor="pv-reconciler",
            )
            was_unblinded = True
            arm = str(iwrs_res["treatment_arm"])
            is_inv = bool(iwrs_res["is_investigational_drug"])

        if not is_inv:
            with connect_sm(paths) as sm_conn:
                sm_conn.execute("BEGIN IMMEDIATE")
                sm_conn.execute(
                    """
                    INSERT INTO sm_case_records (
                        case_uid, subject_id, trial_id, site_id, lifecycle_state,
                        unblinded, treatment_arm, is_investigational_drug, reconciled_at
                    ) VALUES (?, ?, ?, ?, 'closed_non_investigational', 1, ?, 0, ?)
                    ON CONFLICT(case_uid) DO UPDATE SET
                        lifecycle_state = 'closed_non_investigational',
                        unblinded = 1,
                        treatment_arm = excluded.treatment_arm,
                        is_investigational_drug = 0,
                        reconciled_at = excluded.reconciled_at
                    """,
                    (case_uid, subject_id, trial_id, site_id, arm, now_iso),
                )
                sm_conn.commit()
            _commit_blinded_views(paths, case_uid, seen_ids, now_iso)
            _rebuild_dsmb_for_trial(paths, trial_id, now_iso)
            return

        with connect_sm(paths) as sm_conn:
            sm_conn.execute("BEGIN IMMEDIATE")
            sm_conn.execute(
                """
                INSERT INTO sm_case_records (
                    case_uid, subject_id, trial_id, site_id, lifecycle_state,
                    unblinded, treatment_arm, is_investigational_drug, reconciled_at
                ) VALUES (?, ?, ?, ?, 'susar_pending_submission', 1, ?, 1, ?)
                ON CONFLICT(case_uid) DO UPDATE SET
                    unblinded = 1,
                    treatment_arm = excluded.treatment_arm,
                    is_investigational_drug = 1,
                    reconciled_at = excluded.reconciled_at
                """,
                (case_uid, subject_id, trial_id, site_id, arm, now_iso),
            )

            d = reduced.directive
            if d is not None:
                if d.action == "create_initial":
                    new_sub_id = f"IND-{uuid.uuid4().hex[:10].upper()}"
                    sm_conn.execute(
                        """
                        INSERT INTO sm_submission_records (
                            submission_id, case_uid, trial_id, submission_type, parent_submission_id,
                            sequence_num, report_clock, day0_at, due_at, status,
                            gateway_ack_id, submitted_at, last_error,
                            covered_followup_num, covered_grade, covered_seriousness_json, updated_at
                        ) VALUES (?, ?, ?, 'initial', NULL, 1, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, ?, ?, ?)
                        """,
                        (
                            new_sub_id,
                            case_uid,
                            trial_id,
                            d.report_clock,
                            d.day0_at,
                            d.due_at,
                            d.covered_followup_num,
                            d.covered_grade,
                            json.dumps(d.covered_seriousness, sort_keys=True),
                            now_iso,
                        ),
                    )
                elif d.action == "update_pending" and d.target_submission_id:
                    sm_conn.execute(
                        """
                        UPDATE sm_submission_records
                        SET report_clock = ?, due_at = ?,
                            covered_followup_num = ?, covered_grade = ?, covered_seriousness_json = ?,
                            updated_at = ?
                        WHERE submission_id = ?
                        """,
                        (
                            d.report_clock,
                            d.due_at,
                            d.covered_followup_num,
                            d.covered_grade,
                            json.dumps(d.covered_seriousness, sort_keys=True),
                            now_iso,
                            d.target_submission_id,
                        ),
                    )
                elif d.action == "create_followup":
                    fu_sub_id = f"IND-{uuid.uuid4().hex[:10].upper()}"
                    sm_conn.execute(
                        """
                        INSERT INTO sm_submission_records (
                            submission_id, case_uid, trial_id, submission_type, parent_submission_id,
                            sequence_num, report_clock, day0_at, due_at, status,
                            gateway_ack_id, submitted_at, last_error,
                            covered_followup_num, covered_grade, covered_seriousness_json, updated_at
                        ) VALUES (?, ?, ?, 'followup', ?, ?, ?, ?, ?, 'pending', NULL, NULL, NULL, ?, ?, ?, ?)
                        """,
                        (
                            fu_sub_id,
                            case_uid,
                            trial_id,
                            d.parent_submission_id,
                            d.sequence_num,
                            d.report_clock,
                            d.day0_at,
                            d.due_at,
                            d.covered_followup_num,
                            d.covered_grade,
                            json.dumps(d.covered_seriousness, sort_keys=True),
                            now_iso,
                        ),
                    )

            pending_sub = sm_conn.execute(
                """
                SELECT * FROM sm_submission_records
                WHERE case_uid = ? AND status IN ('pending', 'retry_pending')
                ORDER BY sequence_num ASC LIMIT 1
                """,
                (case_uid,),
            ).fetchone()
            sm_conn.commit()

        if pending_sub is not None:
            sub_id = str(pending_sub["submission_id"])
            seq_num = int(pending_sub["sequence_num"])
            gw_payload = {
                "submission_id": sub_id,
                "case_uid": case_uid,
                "trial_id": trial_id,
                "site_id": site_id,
                "subject_id": subject_id,
                "treatment_arm": arm,
                "submission_type": str(pending_sub["submission_type"]),
                "parent_submission_id": pending_sub["parent_submission_id"],
                "sequence_num": seq_num,
                "report_clock": str(pending_sub["report_clock"]),
                "day0_at": str(pending_sub["day0_at"]),
                "due_at": str(pending_sub["due_at"]),
                "meddra_pt": reduced.meddra_pt,
                "ctcae_grade": reduced.ctcae_grade,
                "seriousness": reduced.seriousness,
                "investigator_causality": reduced.investigator_causality,
                "sponsor_causality": reduced.sponsor_causality,
                "narrative": reduced.narrative,
            }
            gw_meta = {
                "case_uid": case_uid,
                "trial_id": trial_id,
                "submission_type": str(pending_sub["submission_type"]),
                "sequence_num": seq_num,
                "report_clock": str(pending_sub["report_clock"]),
            }
            try:
                tx = transmit_ind_submission(
                    paths.fda_gateway_dir,
                    submission_id=sub_id,
                    payload=gw_payload,
                    metadata=gw_meta,
                    transmitted_at=now_iso,
                )
                if not tx["accepted"]:
                    with connect_sm(paths) as sm_conn:
                        sm_conn.execute("BEGIN IMMEDIATE")
                        sm_conn.execute(
                            """
                            UPDATE sm_submission_records
                            SET status = 'retry_pending', last_error = ?, gateway_ack_id = NULL,
                                submitted_at = NULL, updated_at = ?
                            WHERE submission_id = ?
                            """,
                            (str(tx["error"]), now_iso, sub_id),
                        )
                        sm_conn.execute(
                            "UPDATE sm_case_records SET lifecycle_state = 'susar_retry_pending', reconciled_at = ? WHERE case_uid = ?",
                            (now_iso, case_uid),
                        )
                        sm_conn.commit()
                else:
                    ack_id = str(tx["gateway_ack_id"])
                    sub_ts = str(tx["transmitted_at"])
                    with connect_audit(paths.audit_db) as audit_conn:
                        append_audit_event(
                            audit_conn,
                            actor="pv-reconciler",
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
                    with connect_sm(paths) as sm_conn:
                        sm_conn.execute("BEGIN IMMEDIATE")
                        sm_conn.execute(
                            """
                            UPDATE sm_submission_records
                            SET status = 'submitted', gateway_ack_id = ?, submitted_at = ?,
                                last_error = NULL, updated_at = ?
                            WHERE submission_id = ?
                            """,
                            (ack_id, sub_ts, now_iso, sub_id),
                        )
                        sm_conn.execute(
                            "UPDATE sm_case_records SET lifecycle_state = 'susar_submitted', reconciled_at = ? WHERE case_uid = ?",
                            (now_iso, case_uid),
                        )
                        sm_conn.commit()
            except Exception as exc:
                with connect_sm(paths) as sm_conn:
                    sm_conn.execute("BEGIN IMMEDIATE")
                    sm_conn.execute(
                        """
                        UPDATE sm_submission_records
                        SET status = 'retry_pending', last_error = ?, gateway_ack_id = NULL,
                            submitted_at = NULL, updated_at = ?
                        WHERE submission_id = ?
                        """,
                        (f"{type(exc).__name__}: {exc}", now_iso, sub_id),
                    )
                    sm_conn.execute(
                        "UPDATE sm_case_records SET lifecycle_state = 'susar_retry_pending', reconciled_at = ? WHERE case_uid = ?",
                        (now_iso, case_uid),
                    )
                    sm_conn.commit()
        else:
            with connect_sm(paths) as sm_conn:
                sm_conn.execute("BEGIN IMMEDIATE")
                sm_conn.execute(
                    "UPDATE sm_case_records SET lifecycle_state = 'susar_submitted', reconciled_at = ? WHERE case_uid = ?",
                    (now_iso, case_uid),
                )
                sm_conn.commit()

        _commit_blinded_views(paths, case_uid, seen_ids, now_iso)
        _rebuild_dsmb_for_trial(paths, trial_id, now_iso)


def reconcile_all_pending_cases(paths: DataPaths, now_iso: str) -> int:
    init_sm_storage(paths)
    candidate_ids: set[str] = set()
    with connect_edc(paths.edc_db) as edc_conn:
        for r in edc_conn.execute(
            "SELECT DISTINCT case_uid FROM case_reports WHERE processed = 0"
        ).fetchall():
            candidate_ids.add(str(r["case_uid"]))
        for r in edc_conn.execute(
            "SELECT case_uid FROM safety_cases WHERE site_status = 'received'"
        ).fetchall():
            candidate_ids.add(str(r["case_uid"]))

    with connect_sm(paths) as sm_conn:
        for r in sm_conn.execute(
            "SELECT case_uid FROM sm_case_records WHERE lifecycle_state IN ('pending_assessment', 'susar_pending_submission', 'susar_retry_pending')"
        ).fetchall():
            candidate_ids.add(str(r["case_uid"]))
        for r in sm_conn.execute(
            "SELECT DISTINCT case_uid FROM sm_submission_records WHERE status IN ('pending', 'retry_pending')"
        ).fetchall():
            candidate_ids.add(str(r["case_uid"]))

    count = 0
    for cuid in sorted(candidate_ids):
        reconcile_single_case(paths, cuid, now_iso)
        count += 1
    return count
