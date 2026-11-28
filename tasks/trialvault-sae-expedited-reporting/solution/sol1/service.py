"""SOLUTION=1: Domain service operations for TrialVault PV."""

from __future__ import annotations

import json
import uuid
from typing import Any

from trialvault_pv.audit import append_audit_event
from trialvault_pv.cache import ProjectionCache
from trialvault_pv.config import DataPaths
from trialvault_pv.db import connect_audit, connect_edc, connect_iwrs, utc_now_iso
from trialvault_pv.listings import refresh_site_blinded_listing
from trialvault_pv.search import SearchIndex

SERIOUSNESS_FLAGS: tuple[str, ...] = (
    "death",
    "life_threatening",
    "hospitalization",
    "disability",
    "congenital_anomaly",
    "medically_important",
)


def normalize_seriousness(raw: dict[str, Any] | None) -> dict[str, bool]:
    src = raw or {}
    return {flag: bool(src.get(flag, False)) for flag in SERIOUSNESS_FLAGS}


def enroll_subject(
    paths: DataPaths,
    *,
    actor: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    now = utc_now_iso()
    subject_id = data.get("subject_id") or f"SUBJ-{uuid.uuid4().hex[:8].upper()}"
    site_id = str(data["site_id"])
    trial_id = str(data["trial_id"])
    age = int(data["age"])
    sex = str(data["sex"])
    enrollment_date = str(data["enrollment_date"])
    treatment_arm = str(data["treatment_arm"])
    is_investigational_drug = 1 if bool(data.get("is_investigational_drug", True)) else 0

    with connect_edc(paths.edc_db) as conn:
        site_row = conn.execute(
            "SELECT site_id, trial_id FROM sites WHERE site_id = ?",
            (site_id,),
        ).fetchone()
        if site_row is None or site_row["trial_id"] != trial_id:
            raise LookupError("site_or_trial_not_found")

        conn.execute(
            """
            INSERT INTO subjects_blinded (
                subject_id, trial_id, site_id, age, sex, enrollment_date, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, 'enrolled', ?)
            """,
            (subject_id, trial_id, site_id, age, sex, enrollment_date, now),
        )
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
            ) VALUES (?, 'subject.enrolled', ?, ?, ?, 'pending', ?)
            """,
            (
                f"evt-{uuid.uuid4().hex[:12]}",
                subject_id,
                site_id,
                json.dumps({"subject_id": subject_id, "trial_id": trial_id, "site_id": site_id}),
                now,
            ),
        )
        conn.commit()

    with connect_iwrs(paths.iwrs_db) as iwrs_conn:
        iwrs_conn.execute(
            """
            INSERT INTO subject_randomization (
                trial_id, subject_id, treatment_arm, is_investigational_drug, randomized_at
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (trial_id, subject_id, treatment_arm, is_investigational_drug, now),
        )
        iwrs_conn.commit()

    with connect_audit(paths.audit_db) as audit_conn:
        append_audit_event(
            audit_conn,
            actor=actor,
            action="SUBJECT_ENROLLED",
            resource_type="subject",
            resource_ref=subject_id,
            payload={"subject_id": subject_id, "site_id": site_id, "trial_id": trial_id},
            event_time=now,
        )
        audit_conn.commit()

    ProjectionCache(paths.cache_file).delete(f"trial_blinded_summary:{trial_id}")
    return {
        "subject_id": subject_id,
        "trial_id": trial_id,
        "site_id": site_id,
        "age": age,
        "sex": sex,
        "enrollment_date": enrollment_date,
        "status": "enrolled",
    }


def log_baseline_non_expedited_ae(
    paths: DataPaths,
    *,
    site_id: str,
    actor: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    now = utc_now_iso()
    case_uid = data.get("case_uid") or f"CASE-{uuid.uuid4().hex[:8].upper()}"
    message_id = data.get("message_id") or f"MSG-{uuid.uuid4().hex[:8].upper()}"
    subject_id = str(data["subject_id"])
    aware_at = str(data.get("aware_at") or now)
    meddra_pt = str(data["meddra_pt"])
    ctcae_grade = int(data["ctcae_grade"])
    seriousness = normalize_seriousness(data.get("seriousness"))
    is_serious = 1 if any(seriousness.values()) else 0
    investigator_causality = str(data.get("investigator_causality", "unrelated"))
    narrative = str(data["narrative"])

    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute(
            "SELECT subject_id, trial_id, site_id FROM subjects_blinded WHERE subject_id = ?",
            (subject_id,),
        ).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        trial_id = subj["trial_id"]

        conn.execute(
            """
            INSERT INTO safety_cases (
                case_uid, subject_id, trial_id, site_id, site_status, latest_followup_num,
                initial_aware_at, latest_aware_at, meddra_pt, ctcae_grade, is_serious,
                seriousness_json, investigator_causality, sponsor_causality, narrative,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'received', 0, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                case_uid,
                subject_id,
                trial_id,
                site_id,
                aware_at,
                aware_at,
                meddra_pt,
                ctcae_grade,
                is_serious,
                json.dumps(seriousness, sort_keys=True),
                investigator_causality,
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
            ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, NULL, ?, ?, 0)
            """,
            (
                f"rep-{uuid.uuid4().hex[:12]}",
                message_id,
                case_uid,
                subject_id,
                site_id,
                aware_at,
                meddra_pt,
                ctcae_grade,
                is_serious,
                json.dumps(seriousness, sort_keys=True),
                investigator_causality,
                narrative,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
            ) VALUES (?, 'case.logged', ?, ?, ?, 'pending', ?)
            """,
            (
                f"evt-{uuid.uuid4().hex[:12]}",
                case_uid,
                site_id,
                json.dumps({"case_uid": case_uid, "site_id": site_id, "trial_id": trial_id}),
                now,
            ),
        )
        conn.commit()

    with connect_audit(paths.audit_db) as audit_conn:
        append_audit_event(
            audit_conn,
            actor=actor,
            action="BASELINE_AE_LOGGED",
            resource_type="safety_case",
            resource_ref=case_uid,
            payload={"case_uid": case_uid, "site_id": site_id, "meddra_pt": meddra_pt},
            event_time=now,
        )
        audit_conn.commit()

    ProjectionCache(paths.cache_file).delete(f"trial_blinded_summary:{trial_id}")
    return {"case_uid": case_uid, "site_status": "received"}


def build_blinded_case_view(row: Any) -> dict[str, Any]:
    seriousness = normalize_seriousness(json.loads(row["seriousness_json"]))
    return {
        "case_uid": str(row["case_uid"]),
        "subject_id": str(row["subject_id"]),
        "site_id": str(row["site_id"]),
        "trial_id": str(row["trial_id"]),
        "site_status": str(row["site_status"]),
        "latest_followup_num": int(row["latest_followup_num"]),
        "meddra_pt": str(row["meddra_pt"]),
        "ctcae_grade": int(row["ctcae_grade"]),
        "is_serious": bool(row["is_serious"]),
        "seriousness": seriousness,
        "investigator_causality": str(row["investigator_causality"]),
        "sponsor_causality": row["sponsor_causality"],
        "narrative": str(row["narrative"]),
    }


def get_blinded_safety_case(paths: DataPaths, case_uid: str) -> dict[str, Any]:
    cache = ProjectionCache(paths.cache_file)
    cached = cache.get(f"blinded_case:{case_uid}")
    if cached is not None:
        return cached
    with connect_edc(paths.edc_db) as conn:
        row = conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
    if row is None:
        raise LookupError("case_not_found")
    view = build_blinded_case_view(row)
    cache.set(f"blinded_case:{case_uid}", view)
    return view


def sync_blinded_case_projections(
    paths: DataPaths,
    case_uid: str,
    *,
    now_iso: str | None = None,
) -> None:
    ts = now_iso or utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        conn.execute(
            "UPDATE case_reports SET processed = 1 WHERE case_uid = ?",
            (case_uid,),
        )
        conn.execute(
            "UPDATE safety_cases SET site_status = 'assessed', updated_at = ? WHERE case_uid = ?",
            (ts, case_uid),
        )
        row = conn.execute(
            "SELECT * FROM safety_cases WHERE case_uid = ?",
            (case_uid,),
        ).fetchone()
        conn.commit()

    if row is None:
        return

    view = build_blinded_case_view(row)
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
    refresh_site_blinded_listing(paths, str(row["site_id"]), generated_at=ts)


def compute_trial_blinded_summary(paths: DataPaths, trial_id: str) -> dict[str, Any]:
    cache = ProjectionCache(paths.cache_file)
    cache_key = f"trial_blinded_summary:{trial_id}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    with connect_edc(paths.edc_db) as conn:
        trial = conn.execute(
            "SELECT trial_id, protocol_code, phase FROM trials WHERE trial_id = ?",
            (trial_id,),
        ).fetchone()
        if trial is None:
            raise LookupError(f"trial_not_found:{trial_id}")
        total_subjects = int(
            conn.execute(
                "SELECT COUNT(*) FROM subjects_blinded WHERE trial_id = ?",
                (trial_id,),
            ).fetchone()[0]
        )
        total_cases = int(
            conn.execute(
                "SELECT COUNT(*) FROM safety_cases WHERE trial_id = ?",
                (trial_id,),
            ).fetchone()[0]
        )
        serious_cases = int(
            conn.execute(
                "SELECT COUNT(*) FROM safety_cases WHERE trial_id = ? AND is_serious = 1",
                (trial_id,),
            ).fetchone()[0]
        )

    summary = {
        "trial_id": trial["trial_id"],
        "protocol_code": trial["protocol_code"],
        "phase": trial["phase"],
        "total_subjects": total_subjects,
        "total_safety_cases": total_cases,
        "serious_safety_cases": serious_cases,
    }
    cache.set(cache_key, summary)
    return summary
