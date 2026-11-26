"""Deterministic representative seed script for local TrialVault development."""

from __future__ import annotations

import json

from trialvault.config import DataPaths, get_data_paths
from trialvault.db import connect_edc, hash_token, init_edc_db
from trialvault.exports import generate_dsmb_snapshot
from trialvault.queries import open_data_query
from trialvault.service import (
    compute_safety_summary,
    enroll_subject,
    ingest_central_lab_webhook,
    record_adverse_event,
    record_visit,
    schedule_epro_reminder,
)
from trialvault.spool import SpoolStore
from trialvault.worker import process_outbox_once

BOS_TOKEN = "tv_site_bos_full_dev_token"
BOS_READONLY_TOKEN = "tv_site_bos_readonly_dev_token"
LON_TOKEN = "tv_site_lon_full_dev_token"


def seed_trialvault(paths: DataPaths) -> dict[str, str]:
    init_edc_db(paths)
    with connect_edc(paths.edc_db) as conn:
        conn.execute(
            """
            INSERT OR IGNORE INTO trials (trial_id, protocol_code, sponsor_name, phase, active)
            VALUES ('TRIAL-ONC-301', 'ONC-301-P3', 'Meridian BioPharma', 'Phase III', 1)
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO sites (site_id, trial_id, name, country, active)
            VALUES
                ('SITE-BOS-01', 'TRIAL-ONC-301', 'Boston Oncology Center', 'US', 1),
                ('SITE-LON-02', 'TRIAL-ONC-301', 'London Clinical Research Unit', 'UK', 1)
            """
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO site_tokens (token_hash, site_id, label, scopes, active)
            VALUES
                (?, 'SITE-BOS-01', 'coord-bos-01', 'edc:read,edc:write,privacy:withdraw', 1),
                (?, 'SITE-BOS-01', 'monitor-bos-ro', 'edc:read', 1),
                (?, 'SITE-LON-02', 'coord-lon-02', 'edc:read,edc:write,privacy:withdraw', 1)
            """,
            (hash_token(BOS_TOKEN), hash_token(BOS_READONLY_TOKEN), hash_token(LON_TOKEN)),
        )
        conn.commit()

    enroll_subject(
        paths,
        site_id="SITE-BOS-01",
        trial_id="TRIAL-ONC-301",
        actor="seed",
        data={
            "subject_id": "SUBJ-BOS-1001",
            "mrn": "MRN-8841201",
            "full_name": "Elena Rostova",
            "email": "elena.rostova@example.org",
            "phone": "+1-617-555-0142",
            "ssn": "123-45-6789",
            "dob": "1974-08-19",
            "age_at_enrollment": 52,
            "street_address": "742 Beacon St Apt 4B",
            "city": "Boston",
            "state": "MA",
            "postal_code": "02115",
            "device_serial": "ECG-9941-BOS",
            "ip_address": "198.51.100.42",
            "portal_url": "https://portal.trialvault.example/p/elena-1001",
            "emergency_contact_name": "Viktor Rostov",
            "emergency_contact_phone": "+1-617-555-0199",
            "treatment_arm": "DRUG_A_100MG",
            "randomization_date": "2026-03-14",
            "household_group_id": "HH-BEACON-742",
            "investigator_name": "Dr. Hannah Lin",
        },
    )
    enroll_subject(
        paths,
        site_id="SITE-BOS-01",
        trial_id="TRIAL-ONC-301",
        actor="seed",
        data={
            "subject_id": "SUBJ-BOS-1002",
            "mrn": "MRN-8841202",
            "full_name": "Marcus Rostov",
            "email": "marcus.rostov@example.org",
            "phone": "+1-617-555-0143",
            "ssn": "234-56-7890",
            "dob": "1971-04-03",
            "age_at_enrollment": 55,
            "street_address": "742 Beacon St Apt 4B",
            "city": "Boston",
            "state": "MA",
            "postal_code": "02115",
            "device_serial": "ECG-9942-BOS",
            "ip_address": "198.51.100.43",
            "portal_url": "https://portal.trialvault.example/p/marcus-1002",
            "emergency_contact_name": "Viktor Rostov",
            "emergency_contact_phone": "+1-617-555-0199",
            "treatment_arm": "PLACEBO",
            "randomization_date": "2026-03-18",
            "household_group_id": "HH-BEACON-742",
            "investigator_name": "Dr. Hannah Lin",
        },
    )
    enroll_subject(
        paths,
        site_id="SITE-BOS-01",
        trial_id="TRIAL-ONC-301",
        actor="seed",
        data={
            "subject_id": "SUBJ-BOS-1003",
            "mrn": "MRN-8841203",
            "full_name": "Arthur Pendelton",
            "email": "arthur.pendelton@example.org",
            "phone": "+1-603-555-0188",
            "ssn": "345-67-8901",
            "dob": "1933-11-02",
            "age_at_enrollment": 92,
            "street_address": "19 Pine Hollow Rd",
            "city": "Charlestown",
            "state": "NH",
            "postal_code": "03603",
            "device_serial": "ECG-9943-NH",
            "ip_address": "198.51.100.77",
            "portal_url": "https://portal.trialvault.example/p/arthur-1003",
            "emergency_contact_name": "Clara Pendelton",
            "emergency_contact_phone": "+1-603-555-0189",
            "treatment_arm": "DRUG_A_100MG",
            "randomization_date": "2026-04-01",
            "regulatory_hold": False,
            "investigator_name": "Dr. Hannah Lin",
        },
    )
    enroll_subject(
        paths,
        site_id="SITE-BOS-01",
        trial_id="TRIAL-ONC-301",
        actor="seed",
        data={
            "subject_id": "SUBJ-BOS-HOLD",
            "mrn": "MRN-8841299",
            "full_name": "Priya Nair",
            "email": "priya.nair@example.org",
            "phone": "+1-617-555-0177",
            "ssn": "456-78-9012",
            "dob": "1980-02-12",
            "age_at_enrollment": 46,
            "street_address": "88 Newbury St",
            "city": "Boston",
            "state": "MA",
            "postal_code": "02116",
            "device_serial": "ECG-9999-HOLD",
            "ip_address": "198.51.100.99",
            "portal_url": "https://portal.trialvault.example/p/priya-hold",
            "emergency_contact_name": "Rohan Nair",
            "emergency_contact_phone": "+1-617-555-0178",
            "treatment_arm": "DRUG_A_100MG",
            "randomization_date": "2026-03-22",
            "regulatory_hold": True,
            "hold_reason": "FDA Form 483 safety inspection freeze",
            "investigator_name": "Dr. Hannah Lin",
        },
    )

    record_visit(
        paths,
        site_id="SITE-BOS-01",
        subject_id="SUBJ-BOS-1001",
        actor="seed",
        data={
            "visit_number": 1,
            "visit_date": "2026-03-28",
            "days_from_randomization": 14,
            "systolic_bp": 124,
            "diastolic_bp": 78,
            "clinical_notes": "Elena Rostova (MRN-8841201, phone +1-617-555-0142) tolerated dose well on ECG-9941-BOS.",
            "metadata": {"subject_name": "Elena Rostova", "mrn": "MRN-8841201", "visit_room": "B2"},
        },
    )
    record_adverse_event(
        paths,
        site_id="SITE-BOS-01",
        subject_id="SUBJ-BOS-1001",
        actor="seed",
        data={
            "meddra_term": "Fatigue",
            "ctcae_grade": 2,
            "serious": False,
            "onset_date": "2026-04-02",
            "onset_study_day": 19,
            "relatedness": "possibly_related",
            "reporter_notes": "Reported by Elena Rostova via elena.rostova@example.org (SUBJ-BOS-1001).",
            "payload": {"subject_id": "SUBJ-BOS-1001", "patient_name": "Elena Rostova", "grade": 2},
        },
    )
    open_data_query(
        paths,
        site_id="SITE-BOS-01",
        subject_id="SUBJ-BOS-1001",
        actor="monitor-bos",
        data={
            "field_name": "adverse_events.ctcae_grade",
            "query_text": "Confirm CTCAE grade 2 Fatigue for Elena Rostova (MRN-8841201, DOB 1974-08-19).",
            "response_text": "Confirmed by PI for Elena Rostova on 2026-04-03.",
            "status": "resolved",
        },
    )
    ingest_central_lab_webhook(
        paths,
        {
            "site_id": "SITE-BOS-01",
            "subject_id": "SUBJ-BOS-1001",
            "mrn": "MRN-8841201",
            "accession_no": "ACC-BOS-5001",
            "analyte": "ALT",
            "value_num": 42.5,
            "unit": "U/L",
            "reference_flag": "HIGH",
            "collected_date": "2026-03-28",
            "study_day": 14,
        },
    )
    schedule_epro_reminder(
        paths,
        site_id="SITE-BOS-01",
        subject_id="SUBJ-BOS-1001",
        data={"status": "pending"},
    )
    SpoolStore(paths.outbound_queue, paths.dead_letters).append_dead_letter(
        {
            "dlq_id": "DLQ-1001",
            "subject_id": "SUBJ-BOS-1001",
            "mrn": "MRN-8841201",
            "email": "elena.rostova@example.org",
            "error": "smtp_timeout",
        }
    )

    process_outbox_once(paths)
    snap = generate_dsmb_snapshot(paths, "TRIAL-ONC-301")
    compute_safety_summary(paths, "TRIAL-ONC-301")
    return {
        "trial_id": "TRIAL-ONC-301",
        "snapshot_id": snap["snapshot_id"],
        "bos_token": BOS_TOKEN,
        "bos_readonly_token": BOS_READONLY_TOKEN,
        "lon_token": LON_TOKEN,
    }


def main() -> None:
    paths = get_data_paths()
    info = seed_trialvault(paths)
    print(json.dumps(info, indent=2))


if __name__ == "__main__":
    main()
