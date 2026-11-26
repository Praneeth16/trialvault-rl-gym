"""DSMB interim clinical safety snapshots with SHA-256 manifest verification."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import uuid
from pathlib import Path
from typing import Any

from trialvault.config import DataPaths
from trialvault.db import connect_edc, utc_now_iso


def compute_file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write_snapshot_atomic(path: Path, payload: dict[str, Any]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(raw)
        os.replace(tmp_name, path)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
    return hashlib.sha256(raw).hexdigest()


def generate_dsmb_snapshot(paths: DataPaths, trial_id: str) -> dict[str, Any]:
    """Export an interim subject-level DSMB safety snapshot and record its manifest checksum."""
    snapshot_id = f"DSMB-{uuid.uuid4().hex[:10].upper()}"
    now = utc_now_iso()

    with connect_edc(paths.edc_db) as conn:
        trial = conn.execute("SELECT * FROM trials WHERE trial_id = ?", (trial_id,)).fetchone()
        if trial is None:
            raise LookupError("trial_not_found")
        subjects = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM subjects WHERE trial_id = ? ORDER BY subject_id ASC",
                (trial_id,),
            ).fetchall()
        ]
        entries: list[dict[str, Any]] = []
        for s in subjects:
            sid = s["subject_id"]
            aes = [
                dict(a)
                for a in conn.execute(
                    "SELECT ae_id, meddra_term, ctcae_grade, serious, onset_date, onset_study_day, relatedness, reporter_notes FROM adverse_events WHERE subject_id = ?",
                    (sid,),
                ).fetchall()
            ]
            labs = [
                dict(lb)
                for lb in conn.execute(
                    "SELECT accession_no, analyte, value_num, unit, reference_flag, collected_date, study_day FROM lab_results WHERE subject_id = ?",
                    (sid,),
                ).fetchall()
            ]
            entries.append(
                {
                    "subject_id": sid,
                    "site_id": s["site_id"],
                    "mrn": s["mrn"],
                    "full_name": s["full_name"],
                    "email": s["email"],
                    "dob": s["dob"],
                    "age_at_enrollment": s["age_at_enrollment"],
                    "age_bucket": s["age_bucket"],
                    "state": s["state"],
                    "postal_code": s["postal_code"],
                    "treatment_arm": s["treatment_arm"],
                    "randomization_date": s["randomization_date"],
                    "status": s["status"],
                    "adverse_events": aes,
                    "labs": labs,
                }
            )

        rel_path = f"{trial_id}/{snapshot_id}.json"
        full_path = paths.exports_dir / rel_path
        payload = {
            "snapshot_id": snapshot_id,
            "trial_id": trial_id,
            "protocol_code": trial["protocol_code"],
            "generated_at": now,
            "subjects": entries,
        }
        checksum = write_snapshot_atomic(full_path, payload)

        conn.execute(
            """
            INSERT INTO export_manifests (
                snapshot_id, trial_id, file_path, sha256_checksum, subject_count, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (snapshot_id, trial_id, rel_path, checksum, len(entries), now, now),
        )
        conn.commit()

    return {
        "snapshot_id": snapshot_id,
        "trial_id": trial_id,
        "file_path": rel_path,
        "sha256_checksum": checksum,
        "subject_count": len(entries),
        "created_at": now,
    }


def read_dsmb_snapshot(paths: DataPaths, trial_id: str, snapshot_id: str) -> dict[str, Any]:
    with connect_edc(paths.edc_db) as conn:
        row = conn.execute(
            "SELECT * FROM export_manifests WHERE trial_id = ? AND snapshot_id = ?",
            (trial_id, snapshot_id),
        ).fetchone()
        if row is None:
            raise LookupError("snapshot_not_found")
    snap_path = paths.exports_dir / row["file_path"]
    if not snap_path.exists():
        raise LookupError("snapshot_file_missing")
    actual_sha = compute_file_sha256(snap_path)
    return {
        "manifest": dict(row),
        "checksum_valid": actual_sha == row["sha256_checksum"],
        "payload": json.loads(snap_path.read_text(encoding="utf-8")),
    }
