"""SOLUTION=1 (v4): HIPAA Safe Harbor de-identification and outbox-driven withdrawal engine across multi-DB stores."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
import uuid
from typing import Any

from trialvault.audit import append_audit_event, connect_audit, init_audit_db
from trialvault.blobs import BlobStore
from trialvault.cache import ProjectionCache
from trialvault.config import DataPaths
from trialvault.db import connect_edc, utc_now_iso
from trialvault.exports import write_snapshot_atomic
from trialvault.monitoring.sdv_feed import (
    enrollment_log_paths,
    read_sdv_items,
    replace_sdv_items,
    write_enrollment_log_signed,
)
from trialvault.search import SearchIndex
from trialvault.spool import SpoolStore
from trialvault.warehouse.sdtm import connect_sdtm, init_sdtm_db

RESTRICTED_ZIP3 = frozenset(
    {
        "036", "059", "063", "102", "203", "556", "692", "790",
        "821", "823", "830", "831", "878", "879", "884", "890", "893",
    }
)

PHI_JSON_KEYS = frozenset(
    {
        "subject_id", "mrn", "full_name", "patient_name", "subject_name",
        "email", "recipient", "phone", "ssn", "dob", "street_address", "city",
        "postal_code", "device_serial", "ip_address", "signature_ip", "portal_url",
        "emergency_contact_name", "emergency_contact_phone",
        "collected_date", "visit_date", "onset_date", "randomization_date", "recorded_date",
    }
)

WITHDRAWAL_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS withdrawal_requests (
    request_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    subject_id_hash TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'full',
    anon_subject_ref TEXT UNIQUE,
    effective_date TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    lease_owner TEXT,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    UNIQUE (subject_id_hash, scope)
);

CREATE TABLE IF NOT EXISTS contact_suppressions (
    id_type TEXT NOT NULL,
    id_hash TEXT NOT NULL,
    request_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (id_type, id_hash)
);

CREATE TABLE IF NOT EXISTS withdrawal_idempotency (
    site_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    subject_id_hash TEXT NOT NULL,
    request_id TEXT NOT NULL REFERENCES withdrawal_requests(request_id),
    created_at TEXT NOT NULL,
    PRIMARY KEY (site_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS withdrawal_suppressions (
    id_type TEXT NOT NULL,
    id_hash TEXT NOT NULL,
    site_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    anon_subject_ref TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    request_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (id_type, id_hash)
);
"""


class ContactWithdrawnError(Exception):
    pass


def sha256_id(value: str) -> str:
    return hashlib.sha256(value.strip().encode("utf-8")).hexdigest()


def ensure_withdrawal_tables(conn: sqlite3.Connection) -> None:
    conn.executescript(WITHDRAWAL_SCHEMA_SQL)


def generalize_zip(postal_code: str | None) -> str | None:
    if not postal_code:
        return None
    digits = "".join(ch for ch in postal_code if ch.isdigit())
    if len(digits) < 3:
        return "000"
    zip3 = digits[:3]
    return "000" if zip3 in RESTRICTED_ZIP3 else zip3


def generalize_age_and_dob(age: int | None, dob: str | None) -> tuple[int | None, str | None, str | None]:
    if age is not None and age > 89:
        return None, "90+", None
    year_only = dob[:4] if dob and len(dob) >= 4 else None
    bucket = str(age) if age is not None else None
    return age, bucket, year_only


def collect_phi_tokens(subj: dict[str, Any], extra_dates: list[str] | None = None) -> list[str]:
    tokens: set[str] = set()
    for f in (
        "subject_id", "mrn", "full_name", "email", "phone", "ssn", "dob",
        "street_address", "city", "device_serial", "ip_address", "portal_url",
        "emergency_contact_name", "emergency_contact_phone", "randomization_date",
    ):
        val = subj.get(f)
        if val and isinstance(val, str) and val.strip():
            tokens.add(val.strip())
    names = [subj.get("full_name") or "", *json.loads(subj.get("aliases_json") or "[]")]
    for name in names:
        parts = [p for p in re.split(r"\s+", name.strip()) if p]
        if name.strip():
            tokens.add(name.strip())
        for part in parts:
            if len(part) >= 3:
                tokens.add(part)
        if len(parts) >= 2:
            tokens.add(f"{parts[0][0]}.{parts[-1][0]}.")
    if extra_dates:
        for d in extra_dates:
            if d and d.strip():
                tokens.add(d.strip())
    return sorted(tokens, key=len, reverse=True)


def redact_text(text: str, tokens: list[str]) -> str:
    out = text
    for tok in tokens:
        if tok:
            out = re.sub(re.escape(tok), "[REDACTED]", out, flags=re.IGNORECASE)
    return out


def redact_json_value(obj: Any, tokens: list[str], anon_ref: str) -> Any:
    if isinstance(obj, dict):
        cleaned: dict[str, Any] = {}
        for k, v in obj.items():
            if k in PHI_JSON_KEYS:
                if k == "subject_id":
                    cleaned["anon_subject_ref"] = anon_ref
                continue
            cleaned[k] = redact_json_value(v, tokens, anon_ref)
        return cleaned
    if isinstance(obj, list):
        return [redact_json_value(item, tokens, anon_ref) for item in obj]
    if isinstance(obj, str):
        return redact_text(obj, tokens)
    return obj


def lookup_suppression(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> dict[str, Any] | None:
    ensure_withdrawal_tables(conn)
    if subject_id:
        row = conn.execute(
            "SELECT * FROM withdrawal_suppressions WHERE id_type = 'subject_id' AND id_hash = ?",
            (sha256_id(subject_id),),
        ).fetchone()
        if row is not None:
            return dict(row)
    if mrn:
        row = conn.execute(
            "SELECT * FROM withdrawal_suppressions WHERE id_type = 'mrn' AND id_hash = ?",
            (sha256_id(mrn),),
        ).fetchone()
        if row is not None:
            return dict(row)
    return None


def is_suppressed(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> bool:
    return lookup_suppression(conn, subject_id=subject_id, mrn=mrn) is not None


def is_contact_suppressed(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> bool:
    if is_suppressed(conn, subject_id=subject_id, mrn=mrn):
        return True
    for id_type, value in (("subject_id", subject_id), ("mrn", mrn)):
        if value and conn.execute(
            "SELECT 1 FROM contact_suppressions WHERE id_type = ? AND id_hash = ?",
            (id_type, sha256_id(value)),
        ).fetchone():
            return True
    return False


def format_request_response(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    r = dict(row)
    return {
        "request_id": r["request_id"],
        "site_id": r["site_id"],
        "status": r["status"],
        "scope": r["scope"],
        "anon_subject_ref": r["anon_subject_ref"],
        "effective_date": r["effective_date"],
        "created_at": r["created_at"],
        "completed_at": r["completed_at"],
        "error": r["error"],
    }


def create_withdrawal_request(
    paths: DataPaths,
    *,
    site_id: str,
    actor: str,
    subject_id: str,
    reason: str,
    requested_by: str,
    effective_date: str | None,
    idempotency_key: str | None,
    scope: str = "full",
) -> tuple[int, dict[str, Any]]:
    subj_hash = sha256_id(subject_id)
    now = utc_now_iso()
    eff_date = effective_date or now[:10]

    with connect_edc(paths.edc_db) as conn:
        ensure_withdrawal_tables(conn)

        if idempotency_key:
            idem = conn.execute(
                "SELECT subject_id_hash, request_id FROM withdrawal_idempotency WHERE site_id = ? AND idempotency_key = ?",
                (site_id, idempotency_key),
            ).fetchone()
            if idem is not None:
                if idem["subject_id_hash"] != subj_hash:
                    return 409, {
                        "error": "idempotency_key_conflict",
                        "detail": "Idempotency-Key already bound to a different subject",
                    }
                existing_req = conn.execute(
                    "SELECT * FROM withdrawal_requests WHERE request_id = ?", (idem["request_id"],)
                ).fetchone()
                assert existing_req is not None
                return 202, format_request_response(existing_req)

        prior = conn.execute(
            "SELECT * FROM withdrawal_requests WHERE subject_id_hash = ?", (subj_hash,)
        ).fetchall()
        existing = next((r for r in prior if r["scope"] == "full"), None) or next(
            (r for r in prior if r["scope"] == scope), None
        )
        if existing is not None:
            if existing["site_id"] != site_id:
                raise PermissionError("cross_site_access")
            if idempotency_key:
                conn.execute(
                    "INSERT OR IGNORE INTO withdrawal_idempotency (site_id, idempotency_key, subject_id_hash, request_id, created_at) VALUES (?, ?, ?, ?, ?)",
                    (site_id, idempotency_key, subj_hash, existing["request_id"], now),
                )
                conn.commit()
            return 202, format_request_response(existing)

        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")

        if scope == "full" and int(subj["regulatory_hold"]) == 1:
            return 409, {
                "error": "blocked_regulatory_hold",
                "status": "blocked_regulatory_hold",
                "detail": subj["hold_reason"] or "Subject is under an active FDA regulatory hold",
            }

        request_id = f"wd-{uuid.uuid4().hex[:12]}"
        anon_ref = f"ANON-{uuid.uuid4().hex[:12].upper()}" if scope == "full" else None
        trial_id = subj["trial_id"]

        conn.execute(
            """
            INSERT INTO withdrawal_requests (
                request_id, site_id, trial_id, subject_id_hash, scope, anon_subject_ref,
                effective_date, status, reason, requested_by, lease_owner, created_at, completed_at, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, NULL, ?, NULL, NULL)
            """,
            (request_id, site_id, trial_id, subj_hash, scope, anon_ref, eff_date, reason, requested_by, now),
        )
        if idempotency_key:
            conn.execute(
                "INSERT INTO withdrawal_idempotency (site_id, idempotency_key, subject_id_hash, request_id, created_at) VALUES (?, ?, ?, ?, ?)",
                (site_id, idempotency_key, subj_hash, request_id, now),
            )
        for id_type, value in (("subject_id", subject_id), ("mrn", subj["mrn"])):
            if value:
                conn.execute(
                    "INSERT OR IGNORE INTO contact_suppressions (id_type, id_hash, request_id, created_at) VALUES (?, ?, ?, ?)",
                    (id_type, sha256_id(value), request_id, now),
                )

        if scope == "follow_up_only":
            conn.execute(
                """
                INSERT INTO outbox_events (
                    event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
                ) VALUES (?, 'subject.follow_up_withdrawal_requested', ?, ?, ?, 'pending', ?)
                """,
                (
                    f"evt-{uuid.uuid4().hex[:12]}",
                    request_id,
                    site_id,
                    json.dumps(
                        {"request_id": request_id, "subject_id": subject_id, "site_id": site_id, "actor": actor},
                        sort_keys=True,
                    ),
                    now,
                ),
            )
            conn.commit()
            created = conn.execute(
                "SELECT * FROM withdrawal_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            assert created is not None
            return 202, format_request_response(created)

        conn.execute(
            """
            INSERT OR IGNORE INTO withdrawal_suppressions (
                id_type, id_hash, site_id, trial_id, anon_subject_ref, effective_date, request_id, created_at
            ) VALUES ('subject_id', ?, ?, ?, ?, ?, ?, ?)
            """,
            (subj_hash, site_id, trial_id, anon_ref, eff_date, request_id, now),
        )
        if subj["mrn"]:
            conn.execute(
                """
                INSERT OR IGNORE INTO withdrawal_suppressions (
                    id_type, id_hash, site_id, trial_id, anon_subject_ref, effective_date, request_id, created_at
                ) VALUES ('mrn', ?, ?, ?, ?, ?, ?, ?)
                """,
                (sha256_id(subj["mrn"]), site_id, trial_id, anon_ref, eff_date, request_id, now),
            )

        event_id = f"evt-{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
            ) VALUES (?, 'subject.withdrawal_requested', ?, ?, ?, 'pending', ?)
            """,
            (
                event_id,
                request_id,
                site_id,
                json.dumps(
                    {
                        "request_id": request_id,
                        "subject_id": subject_id,
                        "site_id": site_id,
                        "trial_id": trial_id,
                        "anon_subject_ref": anon_ref,
                        "actor": actor,
                    },
                    sort_keys=True,
                ),
                now,
            ),
        )
        conn.commit()
        created = conn.execute(
            "SELECT * FROM withdrawal_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        assert created is not None
        return 202, format_request_response(created)


def get_withdrawal_request(paths: DataPaths, *, site_id: str, request_id: str) -> dict[str, Any]:
    with connect_edc(paths.edc_db) as conn:
        ensure_withdrawal_tables(conn)
        row = conn.execute(
            "SELECT * FROM withdrawal_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if row is None:
            raise LookupError("withdrawal_request_not_found")
        if row["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        return format_request_response(row)


def _deidentify_dsmb_snapshots(
    paths: DataPaths,
    conn: sqlite3.Connection,
    *,
    trial_id: str,
    orig_subject_id: str,
    anon_ref: str,
    new_zip: str | None,
    new_age: int | None,
    new_age_bucket: str | None,
    new_dob: str | None,
    tokens: list[str],
    now: str,
) -> None:
    manifests = conn.execute(
        "SELECT snapshot_id, file_path FROM export_manifests WHERE trial_id = ?", (trial_id,)
    ).fetchall()
    for m in manifests:
        snap_path = paths.exports_dir / m["file_path"]
        if not snap_path.exists():
            continue
        payload = json.loads(snap_path.read_text(encoding="utf-8"))
        changed = False
        for entry in payload.get("subjects", []):
            if entry.get("subject_id") == orig_subject_id or entry.get("anon_subject_ref") == anon_ref:
                entry["subject_id"] = anon_ref
                entry["anon_subject_ref"] = anon_ref
                entry["mrn"] = None
                entry["full_name"] = None
                entry["email"] = None
                entry["dob"] = new_dob
                entry["age_at_enrollment"] = new_age
                entry["age_bucket"] = new_age_bucket
                entry["postal_code"] = new_zip
                entry["randomization_date"] = None
                entry["status"] = "withdrawn"
                for ae in entry.get("adverse_events", []):
                    ae["onset_date"] = None
                    if ae.get("reporter_notes"):
                        ae["reporter_notes"] = redact_text(ae["reporter_notes"], tokens)
                for lb in entry.get("labs", []):
                    lb["collected_date"] = None
                changed = True
        if changed:
            new_sha = write_snapshot_atomic(snap_path, payload)
            conn.execute(
                "UPDATE export_manifests SET sha256_checksum = ?, updated_at = ? WHERE snapshot_id = ?",
                (new_sha, now, m["snapshot_id"]),
            )


def _deidentify_sdtm_warehouse(
    paths: DataPaths,
    *,
    orig_subject_id: str,
    anon_ref: str,
    new_zip: str | None,
    new_age: int | None,
    new_age_bucket: str | None,
    new_dob: str | None,
    tokens: list[str],
) -> None:
    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM sdtm_suppqual WHERE usubjid IN (?, ?)", (orig_subject_id, anon_ref))
        for sv in conn.execute("SELECT sv_id, coval FROM sdtm_sv WHERE usubjid IN (?, ?)", (orig_subject_id, anon_ref)).fetchall():
            conn.execute(
                "UPDATE sdtm_sv SET svstdtc = NULL, coval = ? WHERE sv_id = ?",
                (redact_text(sv["coval"], tokens), sv["sv_id"]),
            )
        for ae in conn.execute("SELECT ae_seq_id, coval FROM sdtm_ae WHERE usubjid IN (?, ?)", (orig_subject_id, anon_ref)).fetchall():
            conn.execute(
                "UPDATE sdtm_ae SET aestdtc = NULL, coval = ? WHERE ae_seq_id = ?",
                (redact_text(ae["coval"], tokens), ae["ae_seq_id"]),
            )
        conn.execute("UPDATE sdtm_lb SET lbdtc = NULL WHERE usubjid IN (?, ?)", (orig_subject_id, anon_ref))
        conn.execute(
            """
            UPDATE sdtm_dm
            SET usubjid = ?, subjid = ?, mrn = NULL, fullname = NULL, brthdtc = ?,
                age = ?, age_bucket = ?, postal_code = ?, rfstdtc = NULL, status = 'withdrawn'
            WHERE usubjid IN (?, ?)
            """,
            (anon_ref, anon_ref, new_dob, new_age, new_age_bucket, new_zip, orig_subject_id, anon_ref),
        )
        conn.commit()


def _deidentify_site_monitoring(
    paths: DataPaths,
    *,
    site_id: str,
    orig_subject_id: str,
    orig_mrn: str | None,
    anon_ref: str,
    new_zip: str | None,
    new_age: int | None,
    new_age_bucket: str | None,
    new_dob: str | None,
    tokens: list[str],
) -> None:
    doc_path, _ = enrollment_log_paths(paths, site_id)
    if doc_path.exists():
        doc = json.loads(doc_path.read_text(encoding="utf-8"))
        changed = False
        for entry in doc.get("entries", []):
            if entry.get("subject_id") in {orig_subject_id, anon_ref}:
                entry["subject_id"] = anon_ref
                entry["mrn"] = None
                entry["full_name"] = None
                entry["aliases"] = []
                entry["dob"] = new_dob
                entry["age_at_enrollment"] = new_age
                entry["age_bucket"] = new_age_bucket
                entry["postal_code"] = new_zip
                entry["randomization_date"] = None
                entry["status"] = "withdrawn"
                changed = True
        if changed:
            write_enrollment_log_signed(paths, site_id, doc)

    items = read_sdv_items(paths, site_id)
    if items:
        updated: list[dict[str, Any]] = []
        sdv_changed = False
        for item in items:
            if item.get("subject_id") in {orig_subject_id, anon_ref} or (orig_mrn and item.get("mrn") == orig_mrn):
                clean_item: dict[str, Any] = {
                    "item_type": item.get("item_type"),
                    "ref_id": item.get("ref_id"),
                    "anon_subject_ref": anon_ref,
                }
                if "study_day" in item:
                    clean_item["study_day"] = item["study_day"]
                if "field_name" in item:
                    clean_item["field_name"] = item["field_name"]
                if item.get("note"):
                    clean_item["note"] = redact_text(str(item["note"]), tokens)
                if item.get("response"):
                    clean_item["response"] = redact_text(str(item["response"]), tokens)
                updated.append(clean_item)
                sdv_changed = True
            else:
                updated.append(item)
        if sdv_changed:
            replace_sdv_items(paths, site_id, updated)


def execute_withdrawal_job(paths: DataPaths, job_payload: dict[str, Any]) -> bool:
    """Converge all active stores, warehouses, logs, exports, archives, and queues idempotently.

    Returns True when the request is completed, or False if another concurrent worker holds the lease.
    """
    request_id = job_payload["request_id"]
    orig_subject_id = job_payload["subject_id"]
    site_id = job_payload["site_id"]
    trial_id = job_payload["trial_id"]
    anon_ref = job_payload["anon_subject_ref"]
    actor = job_payload.get("actor", f"site:{site_id}")
    worker_lease = f"wrk-{uuid.uuid4().hex[:12]}"

    with connect_edc(paths.edc_db) as conn:
        ensure_withdrawal_tables(conn)
        conn.execute("BEGIN IMMEDIATE")
        req = conn.execute(
            "SELECT * FROM withdrawal_requests WHERE request_id = ?", (request_id,)
        ).fetchone()
        if req is None or req["status"] == "completed":
            conn.rollback()
            return True
        if req["lease_owner"] is not None:
            conn.rollback()
            return False
        conn.execute(
            "UPDATE withdrawal_requests SET status = 'in_progress', lease_owner = ?, error = NULL WHERE request_id = ? AND status != 'completed'",
            (worker_lease, request_id),
        )
        conn.commit()

        subj_row = conn.execute(
            "SELECT * FROM subjects WHERE subject_id IN (?, ?)", (orig_subject_id, anon_ref)
        ).fetchone()
        if subj_row is None:
            return True
        subj = dict(subj_row)
        cur_sid = subj["subject_id"]

        visits = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE subject_id = ?", (cur_sid,)).fetchall()]
        aes = [dict(r) for r in conn.execute("SELECT * FROM adverse_events WHERE subject_id = ?", (cur_sid,)).fetchall()]
        labs = [dict(r) for r in conn.execute("SELECT * FROM lab_results WHERE subject_id = ?", (cur_sid,)).fetchall()]
        notifs = [dict(r) for r in conn.execute("SELECT * FROM epro_notifications WHERE subject_id = ?", (cur_sid,)).fetchall()]
        queries = [dict(r) for r in conn.execute("SELECT * FROM data_queries WHERE subject_id = ?", (cur_sid,)).fetchall()]
        codings = [dict(r) for r in conn.execute("SELECT * FROM medical_coding_queue WHERE subject_id = ?", (cur_sid,)).fetchall()]

    extra_dates = [
        *(v["visit_date"] for v in visits if v.get("visit_date")),
        *(a["onset_date"] for a in aes if a.get("onset_date")),
        *(lb["collected_date"] for lb in labs if lb.get("collected_date")),
    ]
    tokens = collect_phi_tokens(subj, extra_dates=extra_dates)
    if orig_subject_id not in tokens:
        tokens.append(orig_subject_id)
        tokens.sort(key=len, reverse=True)
    orig_mrn = subj.get("mrn")
    new_zip = generalize_zip(subj.get("postal_code"))
    new_age, new_age_bucket, new_dob = generalize_age_and_dob(
        subj.get("age_at_enrollment"), subj.get("dob")
    )
    now = utc_now_iso()

    try:
        # 1. SQLite FTS5 Search Index
        SearchIndex(paths.search_db).delete_subject(orig_subject_id)
        SearchIndex(paths.search_db).delete_subject(anon_ref)

        # 2. Projection Cache
        cache = ProjectionCache(paths.cache_file)
        cache.delete_by_prefix(f"site:{site_id}:subject:{orig_subject_id}:")
        if orig_mrn:
            cache.delete(f"site:{site_id}:mrn:{orig_mrn}")
        for k, val in list(cache.all_items().items()):
            serialized = json.dumps(val)
            if orig_subject_id in k or orig_subject_id in serialized or (orig_mrn and orig_mrn in serialized):
                cache.delete(k)
        cache.delete(f"trial:{trial_id}:safety_summary")

        # 3. Blob Store (delete eConsent, de-identify central-lab blobs + shipment manifests)
        blobs = BlobStore(paths.blobs_dir)
        blobs.delete(f"econsent/{site_id}/{orig_subject_id}_signed.json")
        for key in blobs.list_keys(f"econsent/{site_id}/"):
            if orig_subject_id in key:
                blobs.delete(key)
        for lab in labs:
            existing_blob = blobs.get_json(lab["blob_key"])
            if existing_blob is not None:
                payload, meta = existing_blob
                clean_payload = {
                    "accession_no": payload.get("accession_no", lab["accession_no"]),
                    "anon_subject_ref": anon_ref,
                    "study_day": payload.get("study_day", lab["study_day"]),
                    "results": payload.get("results", []),
                }
                clean_meta = {
                    "doc_type": meta.get("doc_type", "central_lab"),
                    "site_id": site_id,
                    "trial_id": trial_id,
                    "accession_no": lab["accession_no"],
                    "anon_subject_ref": anon_ref,
                }
                blobs.put_json(lab["blob_key"], clean_payload, clean_meta)
        _deidentify_shipment_manifests(blobs, site_id=site_id, orig_subject_id=orig_subject_id, orig_mrn=orig_mrn, anon_ref=anon_ref)

        # 4. Outbound ePRO Spool & Dead-Letter Queue
        spool = SpoolStore(paths.outbound_queue, paths.dead_letters)

        def belongs(rec: dict[str, Any]) -> bool:
            return rec.get("subject_id") == orig_subject_id or (
                bool(orig_mrn) and rec.get("mrn") == orig_mrn
            )

        spool.replace_outbound([r for r in spool.read_outbound() if not belongs(r)])
        spool.replace_dead_letters([r for r in spool.read_dead_letters() if not belongs(r)])

        # 5. CDISC SDTM Warehouse (`cdisc_sdtm.db`) & Site Monitoring Logs (`monitoring/`)
        _deidentify_sdtm_warehouse(
            paths,
            orig_subject_id=orig_subject_id,
            anon_ref=anon_ref,
            new_zip=new_zip,
            new_age=new_age,
            new_age_bucket=new_age_bucket,
            new_dob=new_dob,
            tokens=tokens,
        )
        _deidentify_site_monitoring(
            paths,
            site_id=site_id,
            orig_subject_id=orig_subject_id,
            orig_mrn=orig_mrn,
            anon_ref=anon_ref,
            new_zip=new_zip,
            new_age=new_age,
            new_age_bucket=new_age_bucket,
            new_dob=new_dob,
            tokens=tokens,
        )

        # 6. DSMB Export Snapshots (before mutating subjects in edc.db so retry keeps full tokens)
        with connect_edc(paths.edc_db) as conn:
            conn.execute("BEGIN IMMEDIATE")
            _deidentify_dsmb_snapshots(
                paths,
                conn,
                trial_id=trial_id,
                orig_subject_id=orig_subject_id,
                anon_ref=anon_ref,
                new_zip=new_zip,
                new_age=new_age,
                new_age_bucket=new_age_bucket,
                new_dob=new_dob,
                tokens=tokens,
                now=now,
            )
            conn.commit()

        # 7. Separate 21 CFR Part 11 Audit Ledger (`part11_audit.db`): purge PII & idempotently append CONSENT_WITHDRAWN
        init_audit_db(paths)
        with connect_audit(paths.audit_db) as audit_conn:
            audit_conn.execute("BEGIN IMMEDIATE")
            audit_conn.execute("DELETE FROM audit_pii_details WHERE subject_id IN (?, ?)", (orig_subject_id, anon_ref))
            already_logged = audit_conn.execute(
                "SELECT 1 FROM audit_log WHERE action = 'CONSENT_WITHDRAWN' AND resource_ref = ?",
                (anon_ref,),
            ).fetchone()
            if already_logged is None:
                append_audit_event(
                    audit_conn,
                    site_id=site_id,
                    actor=actor,
                    action="CONSENT_WITHDRAWN",
                    resource_type="subject",
                    resource_ref=anon_ref,
                    structural_payload={
                        "request_id": request_id,
                        "trial_id": trial_id,
                        "anon_subject_ref": anon_ref,
                        "safe_harbor_applied": True,
                    },
                )
            audit_conn.commit()

        # 8. Finalize relational EDC (`edc.db`) & mark withdrawal completed
        with connect_edc(paths.edc_db) as conn:
            ensure_withdrawal_tables(conn)
            conn.execute("BEGIN IMMEDIATE")
            current = conn.execute(
                "SELECT status FROM withdrawal_requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if current["status"] == "completed":
                conn.rollback()
                return True

            for v in visits:
                conn.execute(
                    "UPDATE visits SET visit_date = NULL, clinical_notes = ?, metadata_json = ? WHERE visit_id = ?",
                    (
                        redact_text(v["clinical_notes"], tokens),
                        json.dumps(redact_json_value(json.loads(v["metadata_json"]), tokens, anon_ref), sort_keys=True),
                        v["visit_id"],
                    ),
                )
            for a in aes:
                conn.execute(
                    "UPDATE adverse_events SET onset_date = NULL, reporter_notes = ?, payload_json = ? WHERE ae_id = ?",
                    (
                        redact_text(a["reporter_notes"], tokens),
                        json.dumps(redact_json_value(json.loads(a["payload_json"]), tokens, anon_ref), sort_keys=True),
                        a["ae_id"],
                    ),
                )
            for q in queries:
                conn.execute(
                    "UPDATE data_queries SET query_text = ?, response_text = ? WHERE query_id = ?",
                    (
                        redact_text(q["query_text"], tokens),
                        redact_text(q["response_text"], tokens) if q.get("response_text") else None,
                        q["query_id"],
                    ),
                )
            for c in codings:
                conn.execute(
                    "UPDATE medical_coding_queue SET verbatim_term = ?, coder_notes = ? WHERE coding_id = ?",
                    (
                        redact_text(c["verbatim_term"], tokens),
                        redact_text(c["coder_notes"], tokens),
                        c["coding_id"],
                    ),
                )
            for lab in labs:
                raw = json.loads(lab["raw_hl7_json"])
                clean_raw = {
                    "message_type": raw.get("message_type", "ORU^R01"),
                    "accession_no": lab["accession_no"],
                    "PID": {"anon_subject_ref": anon_ref},
                    "OBX": {
                        "analyte": lab["analyte"],
                        "value_num": lab["value_num"],
                        "unit": lab["unit"],
                        "reference_flag": lab["reference_flag"],
                        "study_day": lab["study_day"],
                    },
                }
                conn.execute(
                    "UPDATE lab_results SET collected_date = NULL, raw_hl7_json = ? WHERE lab_id = ?",
                    (json.dumps(clean_raw, sort_keys=True), lab["lab_id"]),
                )
            for n in notifs:
                new_status = "cancelled" if n["status"] == "pending" else n["status"]
                conn.execute(
                    "UPDATE epro_notifications SET status = ?, recipient = NULL, payload_json = ? WHERE notif_id = ?",
                    (
                        new_status,
                        json.dumps(redact_json_value(json.loads(n["payload_json"]), tokens, anon_ref), sort_keys=True),
                        n["notif_id"],
                    ),
                )

            conn.execute(
                """
                UPDATE webhook_inbox
                SET subject_id = ?, mrn = NULL, raw_payload_json = ?
                WHERE subject_id = ? OR (? IS NOT NULL AND mrn = ?)
                """,
                (anon_ref, json.dumps({"redacted": True, "anon_subject_ref": anon_ref}), orig_subject_id, orig_mrn, orig_mrn),
            )
            conn.execute(
                """
                UPDATE scheduled_jobs
                SET subject_id = ?, payload_json = ?, status = CASE WHEN status = 'scheduled' THEN 'cancelled' ELSE status END
                WHERE subject_id = ?
                """,
                (anon_ref, json.dumps({"cancelled": True, "anon_subject_ref": anon_ref}), orig_subject_id),
            )
            conn.execute(
                "UPDATE outbox_events SET aggregate_id = ?, payload_json = ? WHERE aggregate_id = ?",
                (anon_ref, json.dumps({"redacted": True, "anon_subject_ref": anon_ref}), orig_subject_id),
            )
            conn.execute(
                "UPDATE outbox_events SET payload_json = ? WHERE aggregate_id = ? AND event_type = 'subject.withdrawal_requested'",
                (json.dumps({"request_id": request_id, "anon_subject_ref": anon_ref}), request_id),
            )

            if cur_sid == orig_subject_id:
                conn.execute(
                    """
                    UPDATE subjects
                    SET subject_id = ?, mrn = NULL, full_name = NULL, email = NULL, phone = NULL,
                        ssn = NULL, dob = ?, age_at_enrollment = ?, age_bucket = ?,
                        street_address = NULL, city = NULL, postal_code = ?,
                        device_serial = NULL, ip_address = NULL, portal_url = NULL,
                        emergency_contact_name = NULL, emergency_contact_phone = NULL,
                        randomization_date = NULL, status = 'withdrawn', anon_subject_ref = ?,
                        household_group_id = NULL, aliases_json = '[]', updated_at = ?
                    WHERE subject_id = ?
                    """,
                    (anon_ref, new_dob, new_age, new_age_bucket, new_zip, anon_ref, now, orig_subject_id),
                )

            conn.execute(
                "UPDATE withdrawal_requests SET status = 'completed', lease_owner = NULL, completed_at = ?, error = NULL WHERE request_id = ?",
                (now, request_id),
            )
            conn.commit()
            return True
    except Exception as exc:
        with connect_edc(paths.edc_db) as conn:
            ensure_withdrawal_tables(conn)
            conn.execute(
                "UPDATE withdrawal_requests SET status = 'in_progress', lease_owner = NULL, completed_at = NULL, error = ? WHERE request_id = ? AND status != 'completed'",
                (f"{type(exc).__name__}: {exc}", request_id),
            )
            conn.commit()
        raise


def _deidentify_shipment_manifests(
    blobs: BlobStore,
    *,
    site_id: str,
    orig_subject_id: str,
    orig_mrn: str | None,
    anon_ref: str,
) -> None:
    for key in blobs.list_keys(f"labs/{site_id}/shipments/"):
        got = blobs.get_json(key)
        if got is None:
            continue
        doc, meta = got
        changed = False
        specimens = []
        for sp in doc.get("specimens", []):
            if sp.get("subject_id") == orig_subject_id or (orig_mrn and sp.get("mrn") == orig_mrn):
                sp = {"accession_no": sp.get("accession_no"), "anon_subject_ref": anon_ref, "analyte": sp.get("analyte")}
                changed = True
            specimens.append(sp)
        subject_ids = meta.get("subject_ids", [])
        if orig_subject_id in subject_ids:
            subject_ids = [anon_ref if s == orig_subject_id else s for s in subject_ids]
            changed = True
        if changed:
            doc["specimens"] = specimens
            meta["subject_ids"] = list(dict.fromkeys(subject_ids))
            blobs.put_json(key, doc, meta)


def execute_follow_up_job(paths: DataPaths, job_payload: dict[str, Any]) -> bool:
    """Stop all participant contact without de-identifying retained data."""
    from trialvault.service import sync_subject_projections

    request_id = job_payload["request_id"]
    subject_id = job_payload["subject_id"]
    site_id = job_payload["site_id"]

    with connect_edc(paths.edc_db) as conn:
        ensure_withdrawal_tables(conn)
        req = conn.execute("SELECT status FROM withdrawal_requests WHERE request_id = ?", (request_id,)).fetchone()
        subj = conn.execute("SELECT mrn FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
    if req is None or req["status"] == "completed":
        return True
    mrn = subj["mrn"] if subj else None

    spool = SpoolStore(paths.outbound_queue, paths.dead_letters)

    def belongs(rec: dict[str, Any]) -> bool:
        return rec.get("subject_id") == subject_id or (bool(mrn) and rec.get("mrn") == mrn)

    spool.replace_outbound([r for r in spool.read_outbound() if not belongs(r)])
    spool.replace_dead_letters([r for r in spool.read_dead_letters() if not belongs(r)])

    init_sdtm_db(paths)
    with connect_sdtm(paths.sdtm_db) as sdtm_conn:
        sdtm_conn.execute(
            "UPDATE sdtm_dm SET status = 'follow_up_withdrawn' WHERE usubjid = ? AND status = 'active'",
            (subject_id,),
        )
        sdtm_conn.commit()

    doc_path, _ = enrollment_log_paths(paths, site_id)
    if doc_path.exists():
        doc = json.loads(doc_path.read_text(encoding="utf-8"))
        changed = False
        for entry in doc.get("entries", []):
            if entry.get("subject_id") == subject_id and entry.get("status") == "active":
                entry["status"] = "follow_up_withdrawn"
                changed = True
        if changed:
            write_enrollment_log_signed(paths, site_id, doc)

    init_audit_db(paths)
    with connect_audit(paths.audit_db) as audit_conn:
        audit_conn.execute("BEGIN IMMEDIATE")
        if audit_conn.execute(
            "SELECT 1 FROM audit_log WHERE action = 'FOLLOW_UP_WITHDRAWN' AND resource_ref = ?",
            (request_id,),
        ).fetchone() is None:
            append_audit_event(
                audit_conn,
                site_id=site_id,
                actor=job_payload.get("actor", f"site:{site_id}"),
                action="FOLLOW_UP_WITHDRAWN",
                resource_type="withdrawal_request",
                resource_ref=request_id,
                structural_payload={"request_id": request_id, "scope": "follow_up_only"},
            )
        audit_conn.commit()

    now = utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        conn.execute("BEGIN IMMEDIATE")
        current = conn.execute("SELECT status FROM withdrawal_requests WHERE request_id = ?", (request_id,)).fetchone()
        if current["status"] == "completed":
            conn.rollback()
            return True
        conn.execute(
            "UPDATE epro_notifications SET status = 'cancelled' WHERE subject_id = ? AND status = 'pending'",
            (subject_id,),
        )
        conn.execute(
            "UPDATE scheduled_jobs SET status = 'cancelled' WHERE subject_id = ? AND status = 'scheduled' AND job_type LIKE 'epro.%'",
            (subject_id,),
        )
        conn.execute(
            "UPDATE outbox_events SET status = 'suppressed' WHERE aggregate_id = ? AND event_type = 'epro.scheduled' AND status = 'pending'",
            (subject_id,),
        )
        conn.execute(
            "UPDATE subjects SET status = 'follow_up_withdrawn', updated_at = ? WHERE subject_id = ? AND status = 'active'",
            (now, subject_id),
        )
        conn.execute(
            "UPDATE withdrawal_requests SET status = 'completed', lease_owner = NULL, completed_at = ?, error = NULL WHERE request_id = ?",
            (now, request_id),
        )
        conn.commit()
    sync_subject_projections(paths, subject_id)
    return True
