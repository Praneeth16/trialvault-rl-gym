"""Configuration and filesystem path helpers for TrialVault PV stores."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class DataPaths:
    root: Path
    edc_db: Path
    iwrs_db: Path
    rsi_db: Path
    audit_db: Path
    search_db: Path
    cache_file: Path
    line_listings_dir: Path
    fda_gateway_dir: Path


def get_data_paths(data_dir: str | Path | None = None) -> DataPaths:
    raw = data_dir if data_dir is not None else os.environ.get("TRIALVAULT_PV_DATA_DIR", "/app/data")
    root = Path(raw).resolve()
    return DataPaths(
        root=root,
        edc_db=root / "clinical_edc.db",
        iwrs_db=root / "randomization_iwrs.db",
        rsi_db=root / "protocol_rsi.db",
        audit_db=root / "part11_pv_audit.db",
        search_db=root / "search.db",
        cache_file=root / "cache.json",
        line_listings_dir=root / "line_listings",
        fda_gateway_dir=root / "fda_gateway",
    )


def ensure_data_dirs(paths: DataPaths) -> None:
    paths.root.mkdir(parents=True, exist_ok=True)
    (paths.line_listings_dir / "site").mkdir(parents=True, exist_ok=True)
    (paths.line_listings_dir / "dsmb").mkdir(parents=True, exist_ok=True)
    (paths.fda_gateway_dir / "submissions").mkdir(parents=True, exist_ok=True)
    cfg_path = paths.fda_gateway_dir / "gateway_config.json"
    if not cfg_path.exists():
        cfg_path.write_text(json.dumps({"mode": "ack"}, indent=2), encoding="utf-8")
    ack_log = paths.fda_gateway_dir / "ack_log.jsonl"
    if not ack_log.exists():
        ack_log.touch()
