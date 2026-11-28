"""Local FDA Electronic Submissions Gateway (ESG) adapter for IND safety reports."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from trialvault_pv.db import utc_now_iso


def read_gateway_config(fda_gateway_dir: Path) -> dict[str, Any]:
    cfg_path = fda_gateway_dir / "gateway_config.json"
    if not cfg_path.exists():
        return {"mode": "ack"}
    raw = cfg_path.read_text(encoding="utf-8").strip()
    if not raw:
        return {"mode": "ack"}
    return json.loads(raw)


def _write_atomic_json(target_path: Path, data: dict[str, Any]) -> bytes:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
    fd, tmp_name = tempfile.mkstemp(dir=str(target_path.parent), suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(raw)
        os.replace(tmp_name, target_path)
    except Exception:
        if os.path.exists(tmp_name):
            os.unlink(tmp_name)
        raise
    return raw


def transmit_ind_submission(
    fda_gateway_dir: Path,
    *,
    submission_id: str,
    payload: dict[str, Any],
    metadata: dict[str, Any],
    transmitted_at: str | None = None,
) -> dict[str, Any]:
    """Transmit an IND safety submission package to the local FDA ESG gateway.

    Reads `gateway_config.json`:
      - `{"mode": "ack"}` (default): writes `submissions/{submission_id}.json` +
        `submissions/{submission_id}.json.meta.json`, appends an accepted receipt to
        `ack_log.jsonl`, and returns `{"accepted": True, "gateway_ack_id": ..., ...}`.
        Idempotent if `submission_id` was already accepted on disk.
      - `{"mode": "nack_retryable", "reason": "..."}`: appends a NACK entry to
        `ack_log.jsonl` without writing accepted submission files and returns
        `{"accepted": False, "gateway_ack_id": None, "error": reason, ...}`.
    """
    ts = transmitted_at or utc_now_iso()
    cfg = read_gateway_config(fda_gateway_dir)
    mode = str(cfg.get("mode", "ack"))
    ack_log_path = fda_gateway_dir / "ack_log.jsonl"
    submissions_dir = fda_gateway_dir / "submissions"

    if mode != "ack":
        reason = str(cfg.get("reason") or f"ESG_NACK_{mode.upper()}")
        ack_log_path.parent.mkdir(parents=True, exist_ok=True)
        with ack_log_path.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    {
                        "submission_id": submission_id,
                        "accepted": False,
                        "mode": mode,
                        "reason": reason,
                        "timestamp": ts,
                    },
                    sort_keys=True,
                )
                + "\n"
            )
        return {
            "accepted": False,
            "gateway_ack_id": None,
            "transmitted_at": ts,
            "error": reason,
        }

    sub_path = submissions_dir / f"{submission_id}.json"
    meta_path = submissions_dir / f"{submission_id}.json.meta.json"

    if sub_path.is_file() and meta_path.is_file():
        existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
        existing_ack = existing_meta.get("gateway_ack_id")
        if existing_ack:
            return {
                "accepted": True,
                "gateway_ack_id": str(existing_ack),
                "transmitted_at": str(existing_meta.get("transmitted_at", ts)),
                "error": None,
            }

    ack_id = f"ESG-ACK-{hashlib.sha256(submission_id.encode('utf-8')).hexdigest()[:12].upper()}"
    raw_bytes = _write_atomic_json(sub_path, payload)
    payload_sha = hashlib.sha256(raw_bytes).hexdigest()
    meta_doc = {
        **metadata,
        "submission_id": submission_id,
        "gateway_ack_id": ack_id,
        "payload_sha256": payload_sha,
        "transmitted_at": ts,
    }
    _write_atomic_json(meta_path, meta_doc)

    ack_log_path.parent.mkdir(parents=True, exist_ok=True)
    with ack_log_path.open("a", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "submission_id": submission_id,
                    "accepted": True,
                    "gateway_ack_id": ack_id,
                    "mode": "ack",
                    "timestamp": ts,
                },
                sort_keys=True,
            )
            + "\n"
        )

    return {
        "accepted": True,
        "gateway_ack_id": ack_id,
        "transmitted_at": ts,
        "error": None,
    }
