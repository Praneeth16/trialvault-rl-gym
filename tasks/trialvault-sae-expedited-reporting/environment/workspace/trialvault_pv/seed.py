"""Seed fixture generator for local development and baseline PV tests."""

from __future__ import annotations

from typing import Any

from trialvault_pv.config import DataPaths, get_data_paths
from trialvault_pv.db import connect_edc, connect_rsi, hash_token, init_all_dbs
from trialvault_pv.listings import refresh_site_blinded_listing, write_dsmb_susar_listing
from trialvault_pv.service import enroll_subject, log_baseline_non_expedited_ae
from trialvault_pv.worker import process_once

BOS_SITE_TOKEN = "tok-site-bos-safety-01"
SEA_SITE_TOKEN = "tok-site-sea-safety-02"
SPONSOR_PV_TOKEN = "tok-sponsor-pv-officer-01"


def seed_trialvault_pv(paths: DataPaths | None = None) -> dict[str, Any]:
    target = paths or get_data_paths()
    init_all_dbs(target)

    trial_id = "TRIAL-ONC-402"
    site_bos = "SITE-BOS-01"
    site_sea = "SITE-SEA-02"

    with connect_edc(target.edc_db) as conn:
        conn.execute(
            """
            INSERT INTO trials (trial_id, protocol_code, investigational_product, sponsor_name, phase, active)
            VALUES (?, 'NVX-402-P3', 'DRUG_NVX_200MG', 'Novavex Oncology', 'Phase III', 1)
            """,
            (trial_id,),
        )
        conn.execute(
            """
            INSERT INTO sites (site_id, trial_id, name, country, active)
            VALUES (?, ?, 'Boston Dana Clinical Center', 'US', 1),
                   (?, ?, 'Seattle Fred Hutch Site', 'US', 1)
            """,
            (site_bos, trial_id, site_sea, trial_id),
        )
        conn.execute(
            """
            INSERT INTO api_tokens (token_hash, site_id, role_label, scopes, active)
            VALUES (?, ?, 'coord-bos-01', 'site:safety', 1),
                   (?, ?, 'coord-sea-02', 'site:safety', 1),
                   (?, NULL, 'pv-officer-01', 'pv:unblinded', 1)
            """,
            (
                hash_token(BOS_SITE_TOKEN),
                site_bos,
                hash_token(SEA_SITE_TOKEN),
                site_sea,
                hash_token(SPONSOR_PV_TOKEN),
            ),
        )
        conn.commit()

    with connect_rsi(target.rsi_db) as rsi_conn:
        rsi_conn.execute(
            """
            INSERT INTO ib_versions (ib_version, trial_id, effective_from, is_active)
            VALUES ('IB-402-v1', ?, '2026-01-01T00:00:00+00:00', 1)
            """,
            (trial_id,),
        )
        rsi_conn.execute(
            """
            INSERT INTO rsi_expected_terms (ib_version, meddra_pt, max_expected_grade, fatal_expected)
            VALUES ('IB-402-v1', 'Nausea', 2, 0),
                   ('IB-402-v1', 'Neutropenia', 3, 0),
                   ('IB-402-v1', 'Disease progression', 5, 1)
            """
        )
        rsi_conn.commit()

    subjects = [
        {
            "subject_id": "SUBJ-BOS-1001",
            "trial_id": trial_id,
            "site_id": site_bos,
            "age": 58,
            "sex": "F",
            "enrollment_date": "2026-02-01",
            "treatment_arm": "DRUG_NVX_200MG",
            "is_investigational_drug": True,
        },
        {
            "subject_id": "SUBJ-BOS-1002",
            "trial_id": trial_id,
            "site_id": site_bos,
            "age": 63,
            "sex": "M",
            "enrollment_date": "2026-02-03",
            "treatment_arm": "PLACEBO",
            "is_investigational_drug": False,
        },
        {
            "subject_id": "SUBJ-SEA-2001",
            "trial_id": trial_id,
            "site_id": site_sea,
            "age": 49,
            "sex": "F",
            "enrollment_date": "2026-02-05",
            "treatment_arm": "DRUG_NVX_200MG",
            "is_investigational_drug": True,
        },
        {
            "subject_id": "SUBJ-SEA-2002",
            "trial_id": trial_id,
            "site_id": site_sea,
            "age": 67,
            "sex": "M",
            "enrollment_date": "2026-02-06",
            "treatment_arm": "PLACEBO",
            "is_investigational_drug": False,
        },
    ]
    for subj in subjects:
        enroll_subject(target, actor="seed-admin", data=subj)

    base_case = log_baseline_non_expedited_ae(
        target,
        site_id=site_bos,
        actor="coord-bos-01",
        data={
            "case_uid": "CASE-BOS-BASE-01",
            "message_id": "MSG-BOS-BASE-01",
            "subject_id": "SUBJ-BOS-1001",
            "aware_at": "2026-02-15T09:00:00+00:00",
            "meddra_pt": "Nausea",
            "ctcae_grade": 1,
            "seriousness": {},
            "investigator_causality": "possibly_related",
            "narrative": "Mild transient post-dose nausea resolved without intervention.",
        },
    )

    process_once(target, now_iso="2026-02-15T09:05:00+00:00")
    refresh_site_blinded_listing(target, site_bos, generated_at="2026-02-15T09:05:00+00:00")
    refresh_site_blinded_listing(target, site_sea, generated_at="2026-02-15T09:05:00+00:00")
    write_dsmb_susar_listing(target, trial_id, [], generated_at="2026-02-15T09:05:00+00:00")

    return {
        "trial_id": trial_id,
        "site_bos": site_bos,
        "site_sea": site_sea,
        "baseline_case_uid": base_case["case_uid"],
    }
