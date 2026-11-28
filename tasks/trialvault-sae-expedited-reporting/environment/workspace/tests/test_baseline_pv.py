"""Baseline pharmacovigilance & safety intake tests (starter functionality)."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from trialvault_pv.api import app
from trialvault_pv.audit import verify_audit_chain
from trialvault_pv.config import get_data_paths
from trialvault_pv.db import connect_audit, connect_edc, connect_iwrs, lookup_treatment_assignment
from trialvault_pv.fda_gateway import transmit_ind_submission
from trialvault_pv.seed import (
    BOS_SITE_TOKEN,
    SEA_SITE_TOKEN,
    SPONSOR_PV_TOKEN,
    seed_trialvault_pv,
)


def test_seed_and_baseline_pv_endpoints(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TRIALVAULT_PV_DATA_DIR", str(tmp_path))
    paths = get_data_paths(tmp_path)
    info = seed_trialvault_pv(paths)

    client = TestClient(app)
    assert client.get("/health").status_code == 200

    bos_headers = {"Authorization": f"Bearer {BOS_SITE_TOKEN}"}
    sea_headers = {"Authorization": f"Bearer {SEA_SITE_TOKEN}"}
    pv_headers = {"Authorization": f"Bearer {SPONSOR_PV_TOKEN}"}

    # Blinded FTS5 search works for own site and hides cases from other sites
    bos_search = client.get("/v1/safety-cases/search?q=Nausea", headers=bos_headers)
    assert bos_search.status_code == 200
    assert len(bos_search.json()["hits"]) == 1
    assert bos_search.json()["hits"][0]["case_uid"] == info["baseline_case_uid"]

    sea_search = client.get("/v1/safety-cases/search?q=Nausea", headers=sea_headers)
    assert sea_search.status_code == 200
    assert len(sea_search.json()["hits"]) == 0

    # Site blinded line listing verifies checksum and contains zero treatment arm leak
    bos_listing = client.get("/v1/sites/SITE-BOS-01/blinded-line-listing", headers=bos_headers)
    assert bos_listing.status_code == 200
    assert bos_listing.json()["checksum_valid"] is True
    assert len(bos_listing.json()["listing"]["cases"]) == 1
    serialized_listing = json.dumps(bos_listing.json())
    assert "DRUG_NVX_200MG" not in serialized_listing
    assert "PLACEBO" not in serialized_listing

    # Cross-site listing access rejected for site:safety
    assert client.get("/v1/sites/SITE-BOS-01/blinded-line-listing", headers=sea_headers).status_code == 403

    # DSMB unblinded listing requires pv:unblinded scope
    assert client.get("/v1/pv/trials/TRIAL-ONC-402/dsmb-line-listing", headers=bos_headers).status_code == 403
    dsmb_resp = client.get("/v1/pv/trials/TRIAL-ONC-402/dsmb-line-listing", headers=pv_headers)
    assert dsmb_resp.status_code == 200
    assert dsmb_resp.json()["checksum_valid"] is True
    assert dsmb_resp.json()["listing"]["susar_cases"] == []

    # Blinded trial summary
    summary = client.get("/v1/trials/TRIAL-ONC-402/blinded-summary", headers=bos_headers)
    assert summary.status_code == 200
    assert summary.json()["total_subjects"] == 4
    assert summary.json()["total_safety_cases"] == 1
    assert summary.json()["serious_safety_cases"] == 0

    # Blinded EDC table has no treatment_arm column and IWRS has zero unblind audits initially
    with connect_edc(paths.edc_db) as edc_conn:
        cols = [r["name"] for r in edc_conn.execute("PRAGMA table_info(subjects_blinded)").fetchall()]
        assert "treatment_arm" not in cols

    with connect_iwrs(paths.iwrs_db) as iwrs_conn:
        initial_audits = iwrs_conn.execute("SELECT COUNT(*) FROM iwrs_unblind_audit").fetchone()[0]
        assert initial_audits == 0

    # IWRS emergency lookup helper records audit entry
    unblinded = lookup_treatment_assignment(
        paths.iwrs_db,
        trial_id="TRIAL-ONC-402",
        subject_id="SUBJ-BOS-1001",
        case_uid="CASE-TEST-UNBLIND",
        actor="pv-officer-01",
    )
    assert unblinded["treatment_arm"] == "DRUG_NVX_200MG"
    assert unblinded["is_investigational_drug"] is True
    with connect_iwrs(paths.iwrs_db) as iwrs_conn:
        after_audits = iwrs_conn.execute("SELECT COUNT(*) FROM iwrs_unblind_audit").fetchone()[0]
        assert after_audits == 1

    # FDA gateway helper ACK and NACK modes
    ack = transmit_ind_submission(
        paths.fda_gateway_dir,
        submission_id="SUB-SMOKE-01",
        payload={"case_uid": "CASE-TEST-UNBLIND", "sequence_num": 1},
        metadata={"case_uid": "CASE-TEST-UNBLIND", "trial_id": "TRIAL-ONC-402"},
    )
    assert ack["accepted"] is True
    assert ack["gateway_ack_id"].startswith("ESG-ACK-")

    # Part 11 SHA-256 audit chain intact
    with connect_audit(paths.audit_db) as audit_conn:
        ok, msg = verify_audit_chain(audit_conn)
        assert ok, msg
