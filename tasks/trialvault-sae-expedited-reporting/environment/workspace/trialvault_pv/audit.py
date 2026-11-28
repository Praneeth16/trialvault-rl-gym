"""FDA 21 CFR Part 11 SHA-256 hash-chained pharmacovigilance audit trail."""

from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from trialvault_pv.db import utc_now_iso

GENESIS_HASH = "0" * 64


def compute_entry_hash(
    *,
    event_time: str,
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
    actor: str,
    action: str,
    resource_type: str,
    resource_ref: str,
    payload: dict[str, Any],
    event_time: str | None = None,
) -> int:
    """Append an immutable hash-chained 21 CFR Part 11 audit event."""
    ts = event_time or utc_now_iso()
    payload_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    payload_hash = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")

    prev_row = conn.execute(
        "SELECT entry_hash FROM audit_log ORDER BY seq_id DESC LIMIT 1"
    ).fetchone()
    prev_hash = prev_row["entry_hash"] if prev_row else GENESIS_HASH

    entry_hash = compute_entry_hash(
        event_time=ts,
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
            event_time, actor, action, resource_type, resource_ref,
            payload_json, payload_hash, prev_hash, entry_hash
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            actor,
            action,
            resource_type,
            resource_ref,
            payload_json,
            payload_hash,
            prev_hash,
            entry_hash,
        ),
    )
    return int(cur.lastrowid)


def verify_audit_chain(conn: sqlite3.Connection) -> tuple[bool, str]:
    """Verify the entire SHA-256 hash chain from GENESIS_HASH."""
    rows = conn.execute(
        """
        SELECT seq_id, event_time, actor, action, resource_type, resource_ref,
               payload_json, payload_hash, prev_hash, entry_hash
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
            return False, f"non-monotonic seq_id={row['seq_id']}"
        if row["prev_hash"] != expected_prev:
            return False, f"broken prev_hash at seq_id={row['seq_id']}"
        actual_payload_hash = hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest()
        if actual_payload_hash != row["payload_hash"]:
            return False, f"payload_hash mismatch at seq_id={row['seq_id']}"
        computed = compute_entry_hash(
            event_time=row["event_time"],
            actor=row["actor"],
            action=row["action"],
            resource_type=row["resource_type"],
            resource_ref=row["resource_ref"],
            payload_hash=row["payload_hash"],
            prev_hash=row["prev_hash"],
        )
        if computed != row["entry_hash"]:
            return False, f"entry_hash mismatch at seq_id={row['seq_id']}"
        expected_prev = row["entry_hash"]
        prev_seq = row["seq_id"]

    return True, f"verified {len(rows)} entries"
