"""SOLUTION=1 (v4) patched service.py (multi-store SDTM + monitoring + separate Part 11 audit + FDA pre/post specimen boundary)."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from typing import Any

from trialvault.audit import record_audit_entry
from trialvault.blobs import BlobStore
from trialvault.cache import ProjectionCache
from trialvault.config import DataPaths
from trialvault.db import connect_edc, utc_now_iso
from trialvault.monitoring.sdv_feed import append_enrollment_log_entry, append_sdv_item
from trialvault.search import SearchIndex
from trialvault.spool import SpoolStore
from trialvault.warehouse.sdtm import (
    connect_sdtm,
    init_sdtm_db,
    record_sdtm_ae,
    record_sdtm_lab,
    record_sdtm_subject,
    record_sdtm_visit,
)
from trialvault.withdrawal import (
    ContactWithdrawnError,
    is_contact_suppressed,
    is_suppressed,
    lookup_suppression,
)


def opaque_audit_ref(prefix: str, raw_id: str) -> str:
    digest = hashlib.sha256(raw_id.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}:{digest}"


def enqueue_outbox(
    conn: sqlite3.Connection,
    *,
    event_type: str,
    aggregate_id: str,
    site_id: str,
    payload: dict[str, Any],
) -> str:
    event_id = f"evt-{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO outbox_events (
            event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
        ) VALUES (?, ?, ?, ?, ?, 'pending', ?)
        """,
        (event_id, event_type, aggregate_id, site_id, json.dumps(payload, sort_keys=True), utc_now_iso()),
    )
    return event_id


def schedule_delayed_job(
    conn: sqlite3.Connection,
    *,
    job_type: str,
    subject_id: str,
    site_id: str,
    payload: dict[str, Any],
    run_after: str,
) -> str:
    job_id = f"job-{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO scheduled_jobs (
            job_id, job_type, subject_id, site_id, payload_json, run_after, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, 'scheduled', ?)
        """,
        (job_id, job_type, subject_id, site_id, json.dumps(payload, sort_keys=True), run_after, utc_now_iso()),
    )
    return job_id


def build_subject_search_text(row: dict[str, Any]) -> str:
    parts = [
        row.get("subject_id") or "", row.get("mrn") or "", row.get("full_name") or "",
        row.get("email") or "", row.get("phone") or "", row.get("ssn") or "",
        row.get("street_address") or "", row.get("city") or "", row.get("state") or "",
        row.get("postal_code") or "", row.get("device_serial") or "", row.get("portal_url") or "",
        row.get("emergency_contact_name") or "", row.get("treatment_arm") or "", row.get("investigator_name") or "",
        *json.loads(row.get("aliases_json") or "[]"),
    ]
    return " ".join(p for p in parts if p)


def build_subject_projection(conn: sqlite3.Connection, subject_id: str) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
    if row is None:
        return None
    subj = dict(row)
    subj["visits"] = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE subject_id = ? ORDER BY visit_number ASC", (subject_id,)).fetchall()]
    subj["adverse_events"] = [dict(r) for r in conn.execute("SELECT * FROM adverse_events WHERE subject_id = ? ORDER BY onset_study_day ASC", (subject_id,)).fetchall()]
    subj["lab_results"] = [dict(r) for r in conn.execute("SELECT * FROM lab_results WHERE subject_id = ? ORDER BY study_day ASC", (subject_id,)).fetchall()]
    subj["data_queries"] = [dict(r) for r in conn.execute("SELECT * FROM data_queries WHERE subject_id = ? ORDER BY created_at ASC", (subject_id,)).fetchall()]
    return subj


def cache_subject_projection(conn: sqlite3.Connection, cache: ProjectionCache, subject_id: str) -> dict[str, Any] | None:
    proj = build_subject_projection(conn, subject_id)
    if proj is None:
        return None
    site_id = proj["site_id"]
    if proj.get("status") == "withdrawn":
        return proj
    cache.set(f"site:{site_id}:subject:{subject_id}:profile", proj)
    cache.set(
        f"site:{site_id}:subject:{subject_id}:timeline",
        {
            "subject_id": subject_id, "full_name": proj.get("full_name"), "mrn": proj.get("mrn"),
            "visit_count": len(proj["visits"]), "ae_count": len(proj["adverse_events"]), "lab_count": len(proj["lab_results"]),
        },
    )
    if proj.get("mrn"):
        cache.set(
            f"site:{site_id}:mrn:{proj['mrn']}",
            {"subject_id": subject_id, "full_name": proj.get("full_name"), "email": proj.get("email")},
        )
    return proj


def enroll_subject(paths: DataPaths, *, site_id: str, trial_id: str, actor: str, data: dict[str, Any]) -> dict[str, Any]:
    now = utc_now_iso()
    subject_id = data.get("subject_id") or f"SUBJ-{uuid.uuid4().hex[:8].upper()}"
    age = int(data["age_at_enrollment"])
    age_bucket = "90+" if age > 89 else str(age)
    with connect_edc(paths.edc_db) as conn:
        conn.execute(
            """
            INSERT INTO subjects (
                subject_id, trial_id, site_id, mrn, full_name, email, phone, ssn,
                dob, age_at_enrollment, age_bucket, street_address, city, state, postal_code,
                device_serial, ip_address, portal_url, emergency_contact_name,
                emergency_contact_phone, treatment_arm, randomization_date, status,
                regulatory_hold, hold_reason, anon_subject_ref, household_group_id,
                investigator_name, aliases_json, created_at, updated_at
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, 'active',
                ?, ?, NULL, ?,
                ?, ?, ?, ?
            )
            """,
            (
                subject_id, trial_id, site_id, data["mrn"], data["full_name"], data["email"],
                data["phone"], data["ssn"], data["dob"], age, age_bucket, data["street_address"],
                data["city"], data["state"], data["postal_code"], data["device_serial"],
                data["ip_address"], data["portal_url"], data["emergency_contact_name"],
                data["emergency_contact_phone"], data["treatment_arm"], data["randomization_date"],
                1 if data.get("regulatory_hold") else 0, data.get("hold_reason"),
                data.get("household_group_id"), data.get("investigator_name", "Dr. Hannah Lin"), json.dumps(data.get("aliases") or []), now, now,
            ),
        )
        enqueue_outbox(
            conn, event_type="subject.enrolled", aggregate_id=subject_id, site_id=site_id,
            payload={"subject_id": subject_id, "trial_id": trial_id, "site_id": site_id, "mrn": data["mrn"], "full_name": data["full_name"], "email": data["email"]},
        )
        schedule_delayed_job(
            conn, job_type="epro.day30_followup", subject_id=subject_id, site_id=site_id,
            payload={"subject_id": subject_id, "mrn": data["mrn"], "full_name": data["full_name"], "recipient": data["email"], "phone": data["phone"], "template": "day30_followup_diary"},
            run_after="2026-04-15T00:00:00+00:00",
        )
        conn.commit()
        row = dict(conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone())

    record_audit_entry(
        paths, site_id=site_id, actor=actor, action="SUBJECT_ENROLLED", resource_type="subject",
        resource_ref=opaque_audit_ref("subj", subject_id),
        structural_payload={"trial_id": trial_id, "treatment_arm": data["treatment_arm"], "regulatory_hold": bool(data.get("regulatory_hold"))},
        pii_subject_id=subject_id,
        pii_detail={"subject_id": subject_id, "mrn": data["mrn"], "full_name": data["full_name"], "email": data["email"], "phone": data["phone"], "ssn": data["ssn"], "dob": data["dob"], "street_address": data["street_address"]},
    )
    record_sdtm_subject(paths, trial_id=trial_id, site_id=site_id, subject_row=row)
    append_enrollment_log_entry(paths, site_id=site_id, trial_id=trial_id, subject_row=row)
    BlobStore(paths.blobs_dir).put_json(
        f"econsent/{site_id}/{subject_id}_signed.json",
        payload={"subject_id": subject_id, "mrn": data["mrn"], "full_name": data["full_name"], "dob": data["dob"], "email": data["email"], "street_address": data["street_address"], "signature_ip": data["ip_address"], "signed_at": now, "consent_version": "v3.2"},
        metadata={"doc_type": "econsent", "site_id": site_id, "subject_id": subject_id, "mrn": data["mrn"], "patient_name": data["full_name"], "email": data["email"]},
    )
    ProjectionCache(paths.cache_file).delete(f"trial:{trial_id}:safety_summary")
    return row


def record_visit(paths: DataPaths, *, site_id: str, subject_id: str, actor: str, data: dict[str, Any]) -> dict[str, Any]:
    visit_id = data.get("visit_id") or f"VIS-{uuid.uuid4().hex[:8].upper()}"
    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        conn.execute(
            "INSERT INTO visits (visit_id, subject_id, site_id, visit_number, visit_date, days_from_randomization, systolic_bp, diastolic_bp, clinical_notes, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (visit_id, subject_id, site_id, int(data["visit_number"]), data["visit_date"], int(data["days_from_randomization"]), int(data["systolic_bp"]), int(data["diastolic_bp"]), data["clinical_notes"], json.dumps(data.get("metadata", {}), sort_keys=True)),
        )
        enqueue_outbox(conn, event_type="visit.recorded", aggregate_id=subject_id, site_id=site_id, payload={"visit_id": visit_id, "subject_id": subject_id, "mrn": subj["mrn"], "clinical_notes": data["clinical_notes"]})
        conn.commit()
        visit_row = dict(conn.execute("SELECT * FROM visits WHERE visit_id = ?", (visit_id,)).fetchone())
        subj_dict = dict(subj)

    record_audit_entry(
        paths, site_id=site_id, actor=actor, action="VISIT_RECORDED", resource_type="visit",
        resource_ref=opaque_audit_ref("visit", visit_id),
        structural_payload={"visit_number": int(data["visit_number"]), "days_from_randomization": int(data["days_from_randomization"])},
        pii_subject_id=subject_id, pii_detail={"subject_id": subject_id, "visit_date": data["visit_date"], "notes": data["clinical_notes"]},
    )
    record_sdtm_visit(paths, trial_id=subj_dict["trial_id"], site_id=site_id, subject_id=subject_id, visit_row=visit_row)
    append_sdv_item(
        paths, site_id,
        {"item_type": "visit", "ref_id": visit_id, "subject_id": subject_id, "mrn": subj_dict.get("mrn"), "patient_name": subj_dict.get("full_name"), "study_day": int(data["days_from_randomization"]), "recorded_date": data["visit_date"], "note": data["clinical_notes"]},
    )
    return visit_row


def record_adverse_event(paths: DataPaths, *, site_id: str, subject_id: str, actor: str, data: dict[str, Any]) -> dict[str, Any]:
    ae_id = data.get("ae_id") or f"AE-{uuid.uuid4().hex[:8].upper()}"
    coding_id = f"COD-{uuid.uuid4().hex[:8].upper()}"
    now = utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        conn.execute(
            "INSERT INTO adverse_events (ae_id, subject_id, trial_id, site_id, meddra_term, ctcae_grade, serious, onset_date, onset_study_day, relatedness, reporter_notes, payload_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ae_id, subject_id, subj["trial_id"], site_id, data["meddra_term"], int(data["ctcae_grade"]), 1 if data.get("serious") else 0, data["onset_date"], int(data["onset_study_day"]), data["relatedness"], data["reporter_notes"], json.dumps(data.get("payload", {}), sort_keys=True)),
        )
        conn.execute(
            "INSERT INTO medical_coding_queue (coding_id, ae_id, subject_id, site_id, verbatim_term, coded_meddra_pt, coder_notes, status, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, 'coded', ?)",
            (coding_id, ae_id, subject_id, site_id, f"{data['meddra_term']} reported for {subj['full_name']} ({subj['mrn']})", data["meddra_term"], f"Coded from AE {ae_id} ({subj['email']}): {data['reporter_notes']}", now),
        )
        enqueue_outbox(conn, event_type="ae.recorded", aggregate_id=subject_id, site_id=site_id, payload={"ae_id": ae_id, "subject_id": subject_id, "trial_id": subj["trial_id"], "mrn": subj["mrn"], "reporter_notes": data["reporter_notes"]})
        conn.commit()
        row = dict(conn.execute("SELECT * FROM adverse_events WHERE ae_id = ?", (ae_id,)).fetchone())
        subj_dict = dict(subj)

    record_audit_entry(
        paths, site_id=site_id, actor=actor, action="ADVERSE_EVENT_RECORDED", resource_type="adverse_event",
        resource_ref=opaque_audit_ref("ae", ae_id),
        structural_payload={"meddra_term": data["meddra_term"], "ctcae_grade": int(data["ctcae_grade"]), "serious": bool(data.get("serious"))},
        pii_subject_id=subject_id, pii_detail={"subject_id": subject_id, "onset_date": data["onset_date"], "reporter_notes": data["reporter_notes"]},
    )
    record_sdtm_ae(paths, trial_id=subj_dict["trial_id"], site_id=site_id, subject_id=subject_id, ae_row=row)
    append_sdv_item(
        paths, site_id,
        {"item_type": "adverse_event", "ref_id": ae_id, "subject_id": subject_id, "mrn": subj_dict.get("mrn"), "patient_name": subj_dict.get("full_name"), "study_day": int(data["onset_study_day"]), "recorded_date": data["onset_date"], "note": data["reporter_notes"]},
    )
    ProjectionCache(paths.cache_file).delete(f"trial:{row['trial_id']}:safety_summary")
    return row


def schedule_epro_reminder(paths: DataPaths, *, site_id: str, subject_id: str, data: dict[str, Any]) -> dict[str, Any]:
    notif_id = data.get("notif_id") or f"EPRO-{uuid.uuid4().hex[:8].upper()}"
    st = data.get("status", "pending")
    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        if is_contact_suppressed(conn, subject_id=subject_id):
            raise ContactWithdrawnError(subject_id)
        recipient = data.get("recipient") or subj["email"]
        payload = data.get("payload") or {
            "subject_id": subject_id, "mrn": subj["mrn"], "full_name": subj["full_name"],
            "phone": subj["phone"], "portal_url": subj["portal_url"],
            "message": f"Reminder for {subj['full_name']}: complete your daily ePRO symptom diary.",
        }
        conn.execute(
            "INSERT INTO epro_notifications (notif_id, subject_id, site_id, channel, recipient, template, payload_json, status, scheduled_for, sent_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (notif_id, subject_id, site_id, data.get("channel", "email"), recipient, data.get("template", "daily_diary_reminder"), json.dumps(payload, sort_keys=True), st, data.get("scheduled_for", utc_now_iso()), data.get("sent_at")),
        )
        if st == "pending":
            enqueue_outbox(conn, event_type="epro.scheduled", aggregate_id=subject_id, site_id=site_id, payload={"notif_id": notif_id, "subject_id": subject_id, "mrn": subj["mrn"], "recipient": recipient, "full_name": subj["full_name"], "message": payload.get("message", "")})
        conn.commit()
        return dict(conn.execute("SELECT * FROM epro_notifications WHERE notif_id = ?", (notif_id,)).fetchone())


def _resolve_sdtm_subject_key(paths: DataPaths, anon_ref: str, fallback: str) -> str:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        if conn.execute("SELECT 1 FROM sdtm_dm WHERE usubjid = ?", (anon_ref,)).fetchone():
            return anon_ref
    return fallback


def ingest_central_lab_webhook(paths: DataPaths, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """Ingest central-lab HL7 webhook with FDA pre-vs-post withdrawal specimen boundary."""
    subject_id = payload.get("subject_id")
    mrn = payload.get("mrn")
    accession_no = payload["accession_no"]
    site_id = payload["site_id"]
    collected_date = str(payload["collected_date"])[:10]
    now = utc_now_iso()

    with connect_edc(paths.edc_db) as conn:
        dup = conn.execute("SELECT status FROM webhook_inbox WHERE accession_no = ?", (accession_no,)).fetchone()
        if dup is not None:
            return 200, {"status": "duplicate", "accession_no": accession_no}

        supp = lookup_suppression(conn, subject_id=subject_id, mrn=mrn)
        if supp is not None:
            if collected_date > supp["effective_date"]:
                return 202, {"status": "suppressed", "accession_no": accession_no}

            anon_ref = supp["anon_subject_ref"]
            trial_id = supp["trial_id"]
            subj_key = anon_ref
            if conn.execute("SELECT 1 FROM subjects WHERE subject_id = ?", (anon_ref,)).fetchone() is None:
                if subject_id and conn.execute("SELECT 1 FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone():
                    subj_key = subject_id
                else:
                    return 202, {"status": "suppressed", "accession_no": accession_no}

            blob_key = f"labs/{site_id}/{accession_no}.json"
            lab_id = f"LAB-{uuid.uuid4().hex[:8].upper()}"
            clean_hl7 = {
                "message_type": "ORU^R01",
                "accession_no": accession_no,
                "PID": {"anon_subject_ref": anon_ref},
                "OBX": {
                    "analyte": payload["analyte"],
                    "value_num": float(payload["value_num"]),
                    "unit": payload["unit"],
                    "reference_flag": payload.get("reference_flag", "NORMAL"),
                    "study_day": int(payload["study_day"]),
                },
            }
            conn.execute(
                "INSERT INTO webhook_inbox (webhook_id, source, accession_no, subject_id, mrn, raw_payload_json, received_at, status) VALUES (?, 'central_lab', ?, ?, NULL, ?, ?, 'ingested_deidentified')",
                (f"wh-{uuid.uuid4().hex[:12]}", accession_no, anon_ref, json.dumps({"deidentified": True, "anon_subject_ref": anon_ref}), now),
            )
            conn.execute(
                "INSERT INTO lab_results (lab_id, subject_id, trial_id, site_id, accession_no, analyte, value_num, unit, reference_flag, collected_date, study_day, raw_hl7_json, blob_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)",
                (lab_id, subj_key, trial_id, site_id, accession_no, payload["analyte"], float(payload["value_num"]), payload["unit"], payload.get("reference_flag", "NORMAL"), int(payload["study_day"]), json.dumps(clean_hl7, sort_keys=True), blob_key),
            )
            conn.commit()

            record_audit_entry(
                paths, site_id=site_id, actor="central-lab-webhook", action="LAB_RESULT_INGESTED_DEIDENTIFIED",
                resource_type="lab_result", resource_ref=opaque_audit_ref("lab", accession_no),
                structural_payload={"analyte": payload["analyte"], "study_day": int(payload["study_day"]), "anon_subject_ref": anon_ref},
            )
            sdtm_key = _resolve_sdtm_subject_key(paths, anon_ref, subj_key)
            record_sdtm_lab(
                paths, trial_id=trial_id, site_id=site_id, subject_id=sdtm_key, lab_id=lab_id,
                accession_no=accession_no, analyte=payload["analyte"], value_num=float(payload["value_num"]),
                unit=payload["unit"], reference_flag=payload.get("reference_flag", "NORMAL"),
                collected_date=None, study_day=int(payload["study_day"]),
            )
            BlobStore(paths.blobs_dir).put_json(
                blob_key,
                payload={
                    "accession_no": accession_no,
                    "anon_subject_ref": anon_ref,
                    "study_day": int(payload["study_day"]),
                    "results": [{"analyte": payload["analyte"], "value_num": float(payload["value_num"]), "unit": payload["unit"], "reference_flag": payload.get("reference_flag", "NORMAL")}],
                },
                metadata={"doc_type": "central_lab", "site_id": site_id, "trial_id": trial_id, "accession_no": accession_no, "anon_subject_ref": anon_ref},
            )
            if payload.get("shipment_id"):
                record_shipment_specimen(
                    BlobStore(paths.blobs_dir), site_id=site_id, trial_id=trial_id, shipment_id=payload["shipment_id"],
                    specimen={"accession_no": accession_no, "anon_subject_ref": anon_ref, "analyte": payload["analyte"]},
                )
            ProjectionCache(paths.cache_file).delete(f"trial:{trial_id}:safety_summary")
            return 201, {
                "status": "ingested_deidentified",
                "lab_id": lab_id,
                "accession_no": accession_no,
                "anon_subject_ref": anon_ref,
                "blob_key": blob_key,
            }

        subj = None
        if subject_id:
            subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None and mrn:
            subj = conn.execute("SELECT * FROM subjects WHERE mrn = ?", (mrn,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")

        resolved_subject_id = subj["subject_id"]
        trial_id = subj["trial_id"]
        blob_key = f"labs/{site_id}/{accession_no}.json"
        lab_id = f"LAB-{uuid.uuid4().hex[:8].upper()}"
        demo = payload.get("patient_demographics", {})
        raw_hl7 = {
            "message_type": "ORU^R01",
            "accession_no": accession_no,
            "PID": {
                "subject_id": resolved_subject_id,
                "mrn": mrn or subj["mrn"],
                "patient_name": demo.get("full_name", subj["full_name"]),
                "dob": demo.get("dob", subj["dob"]),
                "email": demo.get("email", subj["email"]),
                "phone": demo.get("phone", subj["phone"]),
                "ssn": demo.get("ssn", subj["ssn"]),
                "street_address": demo.get("street_address", subj["street_address"]),
                "postal_code": demo.get("postal_code", subj["postal_code"]),
            },
            "OBX": {
                "analyte": payload["analyte"],
                "value_num": float(payload["value_num"]),
                "unit": payload["unit"],
                "reference_flag": payload.get("reference_flag", "NORMAL"),
                "collected_date": payload["collected_date"],
                "study_day": int(payload["study_day"]),
            },
        }
        conn.execute(
            "INSERT INTO webhook_inbox (webhook_id, source, accession_no, subject_id, mrn, raw_payload_json, received_at, status) VALUES (?, 'central_lab', ?, ?, ?, ?, ?, 'ingested')",
            (f"wh-{uuid.uuid4().hex[:12]}", accession_no, resolved_subject_id, raw_hl7["PID"]["mrn"], json.dumps(payload, sort_keys=True), now),
        )
        conn.execute(
            "INSERT INTO lab_results (lab_id, subject_id, trial_id, site_id, accession_no, analyte, value_num, unit, reference_flag, collected_date, study_day, raw_hl7_json, blob_key) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (lab_id, resolved_subject_id, trial_id, site_id, accession_no, payload["analyte"], float(payload["value_num"]), payload["unit"], payload.get("reference_flag", "NORMAL"), payload["collected_date"], int(payload["study_day"]), json.dumps(raw_hl7, sort_keys=True), blob_key),
        )
        enqueue_outbox(conn, event_type="lab.received", aggregate_id=resolved_subject_id, site_id=site_id, payload={"lab_id": lab_id, "subject_id": resolved_subject_id, "trial_id": trial_id, "mrn": raw_hl7["PID"]["mrn"], "accession_no": accession_no, "analyte": payload["analyte"], "patient_name": raw_hl7["PID"]["patient_name"]})
        conn.commit()
        eff_mrn = raw_hl7["PID"]["mrn"]

    record_audit_entry(
        paths, site_id=site_id, actor="central-lab-webhook", action="LAB_RESULT_INGESTED",
        resource_type="lab_result", resource_ref=opaque_audit_ref("lab", accession_no),
        structural_payload={"analyte": payload["analyte"], "study_day": int(payload["study_day"]), "reference_flag": payload.get("reference_flag", "NORMAL")},
        pii_subject_id=resolved_subject_id,
        pii_detail={"subject_id": resolved_subject_id, "mrn": eff_mrn, "patient_name": raw_hl7["PID"]["patient_name"], "collected_date": payload["collected_date"]},
    )
    record_sdtm_lab(
        paths, trial_id=trial_id, site_id=site_id, subject_id=resolved_subject_id, lab_id=lab_id,
        accession_no=accession_no, analyte=payload["analyte"], value_num=float(payload["value_num"]),
        unit=payload["unit"], reference_flag=payload.get("reference_flag", "NORMAL"),
        collected_date=payload["collected_date"], study_day=int(payload["study_day"]),
    )
    BlobStore(paths.blobs_dir).put_json(
        blob_key,
        payload={
            "accession_no": accession_no, "subject_id": resolved_subject_id, "mrn": raw_hl7["PID"]["mrn"],
            "patient_name": raw_hl7["PID"]["patient_name"], "dob": raw_hl7["PID"]["dob"],
            "email": raw_hl7["PID"]["email"], "phone": raw_hl7["PID"]["phone"], "ssn": raw_hl7["PID"]["ssn"],
            "street_address": raw_hl7["PID"]["street_address"], "postal_code": raw_hl7["PID"]["postal_code"],
            "collected_date": payload["collected_date"], "study_day": int(payload["study_day"]),
            "results": [{"analyte": payload["analyte"], "value_num": float(payload["value_num"]), "unit": payload["unit"], "reference_flag": payload.get("reference_flag", "NORMAL")}],
        },
        metadata={"doc_type": "central_lab", "site_id": site_id, "trial_id": trial_id, "accession_no": accession_no, "subject_id": resolved_subject_id, "mrn": raw_hl7["PID"]["mrn"], "patient_name": raw_hl7["PID"]["patient_name"], "collected_date": payload["collected_date"]},
    )
    if payload.get("shipment_id"):
        record_shipment_specimen(
            BlobStore(paths.blobs_dir), site_id=site_id, trial_id=trial_id, shipment_id=payload["shipment_id"],
            specimen={
                "accession_no": accession_no, "subject_id": resolved_subject_id, "mrn": raw_hl7["PID"]["mrn"],
                "patient_name": raw_hl7["PID"]["patient_name"], "collected_date": payload["collected_date"], "analyte": payload["analyte"],
            },
        )
    ProjectionCache(paths.cache_file).delete(f"trial:{trial_id}:safety_summary")
    return 201, {"status": "ingested", "lab_id": lab_id, "accession_no": accession_no, "blob_key": blob_key}


def record_shipment_specimen(
    blobs: BlobStore,
    *,
    site_id: str,
    trial_id: str,
    shipment_id: str,
    specimen: dict[str, Any],
) -> None:
    """Append a specimen to the multi-participant central-lab shipment manifest."""
    key = f"labs/{site_id}/shipments/{shipment_id}.json"
    existing = blobs.get_json(key)
    if existing is None:
        doc: dict[str, Any] = {"shipment_id": shipment_id, "site_id": site_id, "specimens": []}
        meta: dict[str, Any] = {
            "doc_type": "lab_shipment_manifest",
            "site_id": site_id,
            "trial_id": trial_id,
            "shipment_id": shipment_id,
            "subject_ids": [],
            "accession_nos": [],
        }
    else:
        doc, meta = existing
    doc["specimens"].append(specimen)
    ref = specimen.get("subject_id") or specimen.get("anon_subject_ref")
    if ref and ref not in meta["subject_ids"]:
        meta["subject_ids"].append(ref)
    meta["accession_nos"].append(specimen["accession_no"])
    blobs.put_json(key, doc, meta)


def compute_safety_summary(paths: DataPaths, trial_id: str) -> dict[str, Any]:
    cache = ProjectionCache(paths.cache_file)
    cache_key = f"trial:{trial_id}:safety_summary"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached
    with connect_edc(paths.edc_db) as conn:
        trial = conn.execute("SELECT trial_id, protocol_code FROM trials WHERE trial_id = ?", (trial_id,)).fetchone()
        if trial is None:
            raise LookupError("trial_not_found")
        arm_breakdown = {
            r["treatment_arm"]: int(r["cnt"])
            for r in conn.execute("SELECT treatment_arm, COUNT(*) AS cnt FROM subjects WHERE trial_id = ? GROUP BY treatment_arm ORDER BY treatment_arm ASC", (trial_id,)).fetchall()
        }
        ae_stats = conn.execute(
            "SELECT COUNT(*) AS total_aes, COALESCE(SUM(CASE WHEN serious = 1 THEN 1 ELSE 0 END), 0) AS serious_aes, COALESCE(SUM(CASE WHEN ctcae_grade >= 3 THEN 1 ELSE 0 END), 0) AS grade3_plus_aes FROM adverse_events WHERE trial_id = ?",
            (trial_id,),
        ).fetchone()
        lab_summary = {
            r["analyte"]: {"count": int(r["cnt"]), "mean": float(r["mean_value"])}
            for r in conn.execute("SELECT analyte, COUNT(*) AS cnt, ROUND(AVG(value_num), 4) AS mean_value FROM lab_results WHERE trial_id = ? GROUP BY analyte ORDER BY analyte ASC", (trial_id,)).fetchall()
        }
    summary = {
        "trial_id": trial_id,
        "protocol_code": trial["protocol_code"],
        "total_subjects": sum(arm_breakdown.values()),
        "arm_breakdown": arm_breakdown,
        "total_adverse_events": int(ae_stats["total_aes"]),
        "serious_adverse_events": int(ae_stats["serious_aes"]),
        "grade3_plus_adverse_events": int(ae_stats["grade3_plus_aes"]),
        "lab_summary": lab_summary,
    }
    cache.set(cache_key, summary)
    return summary


def sync_subject_projections(paths: DataPaths, subject_id: str) -> None:
    with connect_edc(paths.edc_db) as conn:
        if is_suppressed(conn, subject_id=subject_id):
            return
        proj = cache_subject_projection(conn, ProjectionCache(paths.cache_file), subject_id)
        if proj is None or proj.get("status") == "withdrawn":
            return
        site_id = proj["site_id"]
        search = SearchIndex(paths.search_db)
        search.upsert_document(subject_id, site_id, "profile", build_subject_search_text(proj))
        notes = " ".join(v["clinical_notes"] for v in proj["visits"])
        if notes.strip():
            search.upsert_document(subject_id, site_id, "visits", notes)
        ae_notes = " ".join(a["reporter_notes"] for a in proj["adverse_events"])
        if ae_notes.strip():
            search.upsert_document(subject_id, site_id, "adverse_events", ae_notes)
        lab_notes = " ".join(f"{lb['accession_no']} {lb['analyte']} {lb['raw_hl7_json']}" for lb in proj["lab_results"])
        if lab_notes.strip():
            search.upsert_document(subject_id, site_id, "labs", lab_notes)
        qry_notes = " ".join(f"{q['field_name']} {q['query_text']} {q.get('response_text') or ''}" for q in proj["data_queries"])
        if qry_notes.strip():
            search.upsert_document(subject_id, site_id, "queries", qry_notes)


def flush_epro_to_spool(paths: DataPaths, payload: dict[str, Any]) -> None:
    SpoolStore(paths.outbound_queue, paths.dead_letters).append_outbound(payload)
