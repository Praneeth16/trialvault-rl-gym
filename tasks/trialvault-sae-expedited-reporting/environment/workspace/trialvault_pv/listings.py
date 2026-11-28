"""Blinded site safety line listings and unblinded DSMB SUSAR line listings with SHA-256 sidecars."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from trialvault_pv.config import DataPaths
from trialvault_pv.db import connect_edc, utc_now_iso


def compute_file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def sidecar_sha256_path(json_path: Path) -> Path:
    return Path(f"{json_path}.sha256")


def write_json_with_sha256(json_path: Path, payload: dict[str, Any]) -> str:
    """Atomically write a JSON listing file and its `.sha256` sidecar."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()

    fd, tmp_json = tempfile.mkstemp(dir=str(json_path.parent), suffix=".json.tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(raw)
        os.replace(tmp_json, json_path)
    except Exception:
        if os.path.exists(tmp_json):
            os.unlink(tmp_json)
        raise

    sha_path = sidecar_sha256_path(json_path)
    fd_s, tmp_sha = tempfile.mkstemp(dir=str(json_path.parent), suffix=".sha256.tmp")
    try:
        with os.fdopen(fd_s, "w", encoding="utf-8") as fh:
            fh.write(f"{digest}\n")
        os.replace(tmp_sha, sha_path)
    except Exception:
        if os.path.exists(tmp_sha):
            os.unlink(tmp_sha)
        raise

    return digest


def site_blinded_listing_path(paths: DataPaths, site_id: str) -> Path:
    return paths.line_listings_dir / "site" / site_id / "blinded_safety_listing.json"


def dsmb_susar_listing_path(paths: DataPaths, trial_id: str) -> Path:
    return paths.line_listings_dir / "dsmb" / trial_id / "unblinded_susar_listing.json"


def refresh_site_blinded_listing(
    paths: DataPaths,
    site_id: str,
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Rebuild and sign the strictly blinded safety case line listing for `site_id`."""
    ts = generated_at or utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        site = conn.execute("SELECT site_id, trial_id FROM sites WHERE site_id = ?", (site_id,)).fetchone()
        if site is None:
            raise LookupError(f"site_not_found:{site_id}")
        rows = conn.execute(
            """
            SELECT case_uid, subject_id, trial_id, site_status, latest_followup_num,
                   meddra_pt, ctcae_grade, is_serious, investigator_causality
            FROM safety_cases
            WHERE site_id = ?
            ORDER BY case_uid ASC
            """,
            (site_id,),
        ).fetchall()

    cases = [
        {
            "case_uid": r["case_uid"],
            "subject_id": r["subject_id"],
            "trial_id": r["trial_id"],
            "site_status": r["site_status"],
            "latest_followup_num": int(r["latest_followup_num"]),
            "meddra_pt": r["meddra_pt"],
            "ctcae_grade": int(r["ctcae_grade"]),
            "is_serious": bool(r["is_serious"]),
            "investigator_causality": r["investigator_causality"],
        }
        for r in rows
    ]
    payload = {
        "site_id": site_id,
        "generated_at": ts,
        "cases": cases,
    }
    digest = write_json_with_sha256(site_blinded_listing_path(paths, site_id), payload)
    return {"site_id": site_id, "sha256_checksum": digest, "case_count": len(cases)}


def read_site_blinded_listing(paths: DataPaths, site_id: str) -> dict[str, Any]:
    with connect_edc(paths.edc_db) as conn:
        site = conn.execute("SELECT site_id FROM sites WHERE site_id = ?", (site_id,)).fetchone()
        if site is None:
            raise LookupError(f"site_not_found:{site_id}")

    json_path = site_blinded_listing_path(paths, site_id)
    sha_path = sidecar_sha256_path(json_path)
    if not json_path.is_file():
        return {
            "site_id": site_id,
            "checksum_valid": False,
            "sha256_checksum": None,
            "listing": {"site_id": site_id, "generated_at": None, "cases": []},
        }

    actual_sha = compute_file_sha256(json_path)
    recorded_sha = sha_path.read_text(encoding="utf-8").strip() if sha_path.is_file() else None
    listing = json.loads(json_path.read_text(encoding="utf-8"))
    return {
        "site_id": site_id,
        "checksum_valid": bool(recorded_sha and actual_sha == recorded_sha),
        "sha256_checksum": recorded_sha,
        "listing": listing,
    }


def write_dsmb_susar_listing(
    paths: DataPaths,
    trial_id: str,
    susar_cases: list[dict[str, Any]],
    *,
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Write and sign the unblinded DSMB SUSAR line listing for `trial_id`."""
    ts = generated_at or utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        trial = conn.execute("SELECT trial_id FROM trials WHERE trial_id = ?", (trial_id,)).fetchone()
        if trial is None:
            raise LookupError(f"trial_not_found:{trial_id}")

    ordered = sorted(susar_cases, key=lambda c: str(c.get("case_uid", "")))
    payload = {
        "trial_id": trial_id,
        "generated_at": ts,
        "susar_cases": ordered,
    }
    digest = write_json_with_sha256(dsmb_susar_listing_path(paths, trial_id), payload)
    return {"trial_id": trial_id, "sha256_checksum": digest, "susar_count": len(ordered)}


def read_dsmb_susar_listing(paths: DataPaths, trial_id: str) -> dict[str, Any]:
    with connect_edc(paths.edc_db) as conn:
        trial = conn.execute("SELECT trial_id FROM trials WHERE trial_id = ?", (trial_id,)).fetchone()
        if trial is None:
            raise LookupError(f"trial_not_found:{trial_id}")

    json_path = dsmb_susar_listing_path(paths, trial_id)
    sha_path = sidecar_sha256_path(json_path)
    if not json_path.is_file():
        return {
            "trial_id": trial_id,
            "checksum_valid": False,
            "sha256_checksum": None,
            "listing": {"trial_id": trial_id, "generated_at": None, "susar_cases": []},
        }

    actual_sha = compute_file_sha256(json_path)
    recorded_sha = sha_path.read_text(encoding="utf-8").strip() if sha_path.is_file() else None
    listing = json.loads(json_path.read_text(encoding="utf-8"))
    return {
        "trial_id": trial_id,
        "checksum_valid": bool(recorded_sha and actual_sha == recorded_sha),
        "sha256_checksum": recorded_sha,
        "listing": listing,
    }
