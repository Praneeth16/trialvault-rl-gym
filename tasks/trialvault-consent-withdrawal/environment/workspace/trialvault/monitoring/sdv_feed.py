"""ICH E6(R3) §8 site enrollment log & CRA source-data-verification (SDV) feed."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from trialvault.config import DataPaths, ensure_data_dirs


def _write_atomic_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp_name, path)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise


def enrollment_log_paths(paths: DataPaths, site_id: str) -> tuple[Path, Path]:
    site_dir = paths.monitoring_dir / site_id
    doc_path = site_dir / "screening_enrollment_log.json"
    sha_path = site_dir / "screening_enrollment_log.json.sha256"
    return doc_path, sha_path


def sdv_worklist_path(paths: DataPaths, site_id: str) -> Path:
    return paths.monitoring_dir / site_id / "cra_sdv_worklist.jsonl"


def write_enrollment_log_signed(paths: DataPaths, site_id: str, doc: dict[str, Any]) -> str:
    ensure_data_dirs(paths)
    doc_path, sha_path = enrollment_log_paths(paths, site_id)
    raw = json.dumps(doc, indent=2, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    _write_atomic_bytes(doc_path, raw)
    _write_atomic_bytes(sha_path, f"{digest}\n".encode("utf-8"))
    return digest


def append_enrollment_log_entry(
    paths: DataPaths,
    *,
    site_id: str,
    trial_id: str,
    subject_row: dict[str, Any],
) -> None:
    doc_path, _ = enrollment_log_paths(paths, site_id)
    if doc_path.exists():
        doc = json.loads(doc_path.read_text(encoding="utf-8"))
    else:
        doc = {"site_id": site_id, "trial_id": trial_id, "entries": []}
    aliases = json.loads(subject_row.get("aliases_json") or "[]")
    doc["entries"].append(
        {
            "subject_id": subject_row["subject_id"],
            "mrn": subject_row.get("mrn"),
            "full_name": subject_row.get("full_name"),
            "aliases": aliases,
            "dob": subject_row.get("dob"),
            "age_at_enrollment": subject_row.get("age_at_enrollment"),
            "age_bucket": subject_row.get("age_bucket"),
            "state": subject_row.get("state"),
            "postal_code": subject_row.get("postal_code"),
            "treatment_arm": subject_row["treatment_arm"],
            "randomization_date": subject_row.get("randomization_date"),
            "investigator_name": subject_row.get("investigator_name"),
            "status": subject_row.get("status", "active"),
        }
    )
    write_enrollment_log_signed(paths, site_id, doc)


def append_sdv_item(paths: DataPaths, site_id: str, item: dict[str, Any]) -> None:
    ensure_data_dirs(paths)
    wl_path = sdv_worklist_path(paths, site_id)
    wl_path.parent.mkdir(parents=True, exist_ok=True)
    with wl_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(item, sort_keys=True) + "\n")


def read_sdv_items(paths: DataPaths, site_id: str) -> list[dict[str, Any]]:
    wl_path = sdv_worklist_path(paths, site_id)
    if not wl_path.exists():
        return []
    out: list[dict[str, Any]] = []
    for line in wl_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            out.append(json.loads(line))
    return out


def replace_sdv_items(paths: DataPaths, site_id: str, items: list[dict[str, Any]]) -> None:
    wl_path = sdv_worklist_path(paths, site_id)
    raw = "".join(json.dumps(it, sort_keys=True) + "\n" for it in items).encode("utf-8")
    _write_atomic_bytes(wl_path, raw)


def read_site_enrollment_log(paths: DataPaths, site_id: str) -> dict[str, Any]:
    doc_path, sha_path = enrollment_log_paths(paths, site_id)
    if not doc_path.exists() or not sha_path.exists():
        raise LookupError("enrollment_log_not_found")
    raw = doc_path.read_bytes()
    actual = hashlib.sha256(raw).hexdigest()
    recorded = sha_path.read_text(encoding="utf-8").strip()
    doc = json.loads(raw.decode("utf-8"))
    return {
        "site_id": site_id,
        "checksum_valid": actual == recorded,
        "sha256_checksum": recorded,
        "entry_count": len(doc.get("entries", [])),
        "log": doc,
    }
