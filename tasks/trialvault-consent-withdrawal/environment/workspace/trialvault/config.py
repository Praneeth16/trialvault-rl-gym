"""Configuration and path helpers for TrialVault's data stores."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    root: Path
    edc_db: Path
    audit_db: Path
    sdtm_db: Path
    search_db: Path
    cache_file: Path
    blobs_dir: Path
    exports_dir: Path
    spool_dir: Path
    monitoring_dir: Path
    outbound_queue: Path
    dead_letters: Path


def get_data_paths(data_dir: str | Path | None = None) -> DataPaths:
    raw = data_dir if data_dir is not None else os.environ.get("TRIALVAULT_DATA_DIR", "/app/data")
    root = Path(raw).resolve()
    blobs_dir = root / "blobs"
    exports_dir = root / "exports"
    spool_dir = root / "spool"
    monitoring_dir = root / "monitoring"
    return DataPaths(
        root=root,
        edc_db=root / "edc.db",
        audit_db=root / "part11_audit.db",
        sdtm_db=root / "cdisc_sdtm.db",
        search_db=root / "search.db",
        cache_file=root / "cache.json",
        blobs_dir=blobs_dir,
        exports_dir=exports_dir,
        spool_dir=spool_dir,
        monitoring_dir=monitoring_dir,
        outbound_queue=spool_dir / "outbound_queue.jsonl",
        dead_letters=spool_dir / "dead_letters.jsonl",
    )


def ensure_data_dirs(paths: DataPaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    paths.blobs_dir.mkdir(parents=True, exist_ok=True)
    (paths.blobs_dir / "econsent").mkdir(parents=True, exist_ok=True)
    (paths.blobs_dir / "labs").mkdir(parents=True, exist_ok=True)
    paths.exports_dir.mkdir(parents=True, exist_ok=True)
    paths.spool_dir.mkdir(parents=True, exist_ok=True)
    paths.monitoring_dir.mkdir(parents=True, exist_ok=True)
    if not paths.outbound_queue.exists():
        paths.outbound_queue.touch()
    if not paths.dead_letters.exists():
        paths.dead_letters.touch()
