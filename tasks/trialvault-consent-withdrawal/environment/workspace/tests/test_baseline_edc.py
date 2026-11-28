"""Baseline EDC tests (existing functionality)."""

from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from trialvault.api import app
from trialvault.audit import connect_audit, verify_audit_chain
from trialvault.config import get_data_paths
from trialvault.seed import BOS_TOKEN, LON_TOKEN, seed_trialvault


def test_seed_and_baseline_edc_endpoints(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TRIALVAULT_DATA_DIR", str(tmp_path))
    paths = get_data_paths(tmp_path)
    info = seed_trialvault(paths)

    client = TestClient(app)
    assert client.get("/health").status_code == 200

    headers = {"Authorization": f"Bearer {BOS_TOKEN}"}
    resp = client.get("/v1/subjects/SUBJ-BOS-1001", headers=headers)
    assert resp.status_code == 200
    assert resp.json()["full_name"] == "Elena Rostova"

    search_resp = client.get("/v1/subjects/search?q=Rostova", headers=headers)
    assert search_resp.status_code == 200
    assert len(search_resp.json()["hits"]) >= 1

    qry_resp = client.get("/v1/subjects/SUBJ-BOS-1001/queries", headers=headers)
    assert qry_resp.status_code == 200
    assert len(qry_resp.json()["queries"]) == 1

    snap_resp = client.get(
        f"/v1/trials/TRIAL-ONC-301/dsmb-snapshots/{info['snapshot_id']}", headers=headers
    )
    assert snap_resp.status_code == 200
    assert snap_resp.json()["checksum_valid"] is True

    # Cross-site read is rejected
    lon_headers = {"Authorization": f"Bearer {LON_TOKEN}"}
    assert client.get("/v1/subjects/SUBJ-BOS-1001", headers=lon_headers).status_code == 403
    assert client.get("/v1/sites/SITE-BOS-01/enrollment-log", headers=lon_headers).status_code == 403

    summary = client.get("/v1/trials/TRIAL-ONC-301/safety-summary", headers=headers)
    assert summary.status_code == 200
    assert summary.json()["total_subjects"] == 4
    assert summary.json()["total_adverse_events"] == 1
    assert summary.json()["lab_summary"]["ALT"]["mean"] == 42.5

    sdtm = client.get("/v1/trials/TRIAL-ONC-301/sdtm-summary", headers=headers)
    assert sdtm.status_code == 200
    assert sdtm.json()["dm_count"] == 4
    assert sdtm.json()["ae_count"] == 1
    assert sdtm.json()["lb_count"] == 1

    enr_log = client.get("/v1/sites/SITE-BOS-01/enrollment-log", headers=headers)
    assert enr_log.status_code == 200
    assert enr_log.json()["checksum_valid"] is True
    assert enr_log.json()["entry_count"] == 4

    with connect_audit(paths.audit_db) as conn:
        ok, msg = verify_audit_chain(conn)
        assert ok, msg
