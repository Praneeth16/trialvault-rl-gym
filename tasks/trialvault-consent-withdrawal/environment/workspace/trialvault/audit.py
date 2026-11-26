"""FDA 21 CFR Part 11 SHA-256 hash-chained audit trail."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from trialvault.db import utc_now_iso

GENESIS_HASH = "0" * 64


def _compute_entry_hash(
    event_time: str,
    site_id: str,
    actor: str,
    action: str,
    resource_type: str,
    resource_ref: str,
    payload_hash: str,
    prev_hash: str,
) -> str:
    canonical = "|".join(
        [
            event_time,
            site_id,
            actor,
            action,
            resource_type,
            resource_ref,
            payload_hash,
            prev_hash,
        ]
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def append_audit_event(
    conn: sqlite3.Connection,
    *,
    site_id: str,
    actor: str,
    action: str,
    resource_type: str,
    resource_ref: str,
    structural_payload: dict[str, Any],
    pii_subject_id: str | None = None,
    pii_detail: dict[str, Any] | None = None,
) -> int:
    """Append an immutable 21 CFR Part 11 audit log entry.

    Structural metadata is hashed into `audit_log`. Optional participant PII details
    are written to `audit_pii_details` so they remain separable from the hash chain.
    """
    event_time = utc_now_iso()
    payload_bytes = json.dumps(structural_payload, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    payload_hash = hashlib.sha256(payload_bytes).hexdigest()

    prev_row = conn.execute(
        "SELECT entry_hash FROM audit_log ORDER BY seq_id DESC LIMIT 1"
    ).fetchone()
    prev_hash = prev_row["entry_hash"] if prev_row else GENESIS_HASH

    entry_hash = _compute_entry_hash(
        event_time=event_time,
        site_id=site_id,
        actor=actor,
        action=action,
        resource_type=resource_type,
        resource_ref=resource_ref,
        payload_hash=payload_hash,
        prev_hash=prev_hash,
    )

    cur = conn.execute(
        """
        INSERT INTO audit_log (
            event_time, site_id, actor, action, resource_type,
            resource_ref, payload_hash, prev_hash, entry_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event_time,
            site_id,
            actor,
            action,
            resource_type,
            resource_ref,
            payload_hash,
            prev_hash,
            entry_hash,
        ),
    )
    seq_id = int(cur.lastrowid)

    if pii_subject_id and pii_detail is not None:
        conn.execute(
            """
            INSERT INTO audit_pii_details (seq_id, subject_id, site_id, detail_json)
            VALUES (?, ?, ?, ?)
            """,
            (seq_id, pii_subject_id, site_id, json.dumps(pii_detail, sort_keys=True)),
        )

    return seq_id


def verify_audit_chain(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Verify cryptographic integrity of the entire 21 CFR Part 11 audit chain."""
    rows = conn.execute(
        """
        SELECT seq_id, event_time, site_id, actor, action, resource_type,
               resource_ref, payload_hash, prev_hash, entry_hash
        FROM audit_log
        ORDER BY seq_id ASC
        """
    ).fetchall()

    if not rows:
        return True, "empty"

    expected_prev = GENESIS_HASH
    prev_seq = 0
    for row in rows:
        if row["seq_id"] <= prev_seq:
            return False, f"non-monotonic seq_id at {row['seq_id']}"
        if row["prev_hash"] != expected_prev:
            return False, f"broken prev_hash at seq_id={row['seq_id']}"
        computed = _compute_entry_hash(
            event_time=row["event_time"],
            site_id=row["site_id"],
            actor=row["actor"],
            action=row["action"],
            resource_type=row["resource_type"],
            resource_ref=row["resource_ref"],
            payload_hash=row["payload_hash"],
            prev_hash=row["prev_hash"],
        )
        if computed != row["entry_hash"]:
            return False, f"hash mismatch at seq_id={row['seq_id']}"
        expected_prev = row["entry_hash"]
        prev_seq = row["seq_id"]

    return True, f"verified {len(rows)} entries"
