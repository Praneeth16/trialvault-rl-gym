"""SOLUTION=2 (v2): Checkpointed Saga State Machine for consent withdrawal."""

from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from trialvault.audit import append_audit_event
from trialvault.blobs import BlobStore
from trialvault.cache import ProjectionCache
from trialvault.config import DataPaths
from trialvault.db import connect_edc, utc_now_iso
from trialvault.exports import write_snapshot_atomic
from trialvault.safeharbor import (
    digest_identifier,
    extract_sensitive_strings,
    safe_harbor_demographics,
    safe_harbor_zip3,
    scrub_payload,
    scrub_string,
)
from trialvault.search import SearchIndex
from trialvault.spool import SpoolStore

SAGA_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS privacy_sagas (
    request_id TEXT PRIMARY KEY,
    site_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    subject_digest TEXT NOT NULL,
    scope TEXT NOT NULL DEFAULT 'full',
    anon_subject_ref TEXT UNIQUE,
    effective_date TEXT NOT NULL,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    actor TEXT NOT NULL,
    working_subject_id TEXT,
    step_search_done INTEGER NOT NULL DEFAULT 0,
    step_cache_done INTEGER NOT NULL DEFAULT 0,
    step_blobs_done INTEGER NOT NULL DEFAULT 0,
    step_spool_done INTEGER NOT NULL DEFAULT 0,
    step_exports_done INTEGER NOT NULL DEFAULT 0,
    step_edc_done INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    UNIQUE (subject_digest, scope)
);

CREATE TABLE IF NOT EXISTS contact_blocks (
    kind TEXT NOT NULL,
    digest TEXT NOT NULL,
    request_id TEXT NOT NULL,
    PRIMARY KEY (kind, digest)
);

CREATE TABLE IF NOT EXISTS saga_idempotency_keys (
    site_id TEXT NOT NULL,
    idem_key TEXT NOT NULL,
    subject_digest TEXT NOT NULL,
    request_id TEXT NOT NULL REFERENCES privacy_sagas(request_id),
    PRIMARY KEY (site_id, idem_key)
);

CREATE TABLE IF NOT EXISTS suppression_digests (
    kind TEXT NOT NULL,
    digest TEXT NOT NULL,
    site_id TEXT NOT NULL,
    trial_id TEXT NOT NULL,
    anon_subject_ref TEXT NOT NULL,
    effective_date TEXT NOT NULL,
    request_id TEXT NOT NULL,
    PRIMARY KEY (kind, digest)
);
"""


class ContactWithdrawnError(Exception):
    pass


def init_saga_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SAGA_SCHEMA_SQL)


def find_suppression_digest(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> dict[str, Any] | None:
    init_saga_schema(conn)
    checks = []
    if subject_id:
        checks.append(("subject_id", digest_identifier(subject_id)))
    if mrn:
        checks.append(("mrn", digest_identifier(mrn)))
    for kind, dg in checks:
        hit = conn.execute(
            "SELECT * FROM suppression_digests WHERE kind = ? AND digest = ?",
            (kind, dg),
        ).fetchone()
        if hit is not None:
            return dict(hit)
    return None


def check_suppressed(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> bool:
    return find_suppression_digest(conn, subject_id=subject_id, mrn=mrn) is not None


def check_contact_blocked(
    conn: sqlite3.Connection,
    *,
    subject_id: str | None = None,
    mrn: str | None = None,
) -> bool:
    if check_suppressed(conn, subject_id=subject_id, mrn=mrn):
        return True
    for kind, raw in (("subject_id", subject_id), ("mrn", mrn)):
        if raw and conn.execute(
            "SELECT 1 FROM contact_blocks WHERE kind = ? AND digest = ?", (kind, digest_identifier(raw))
        ).fetchone():
            return True
    return False


def _serialize_saga(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "request_id": row["request_id"],
        "site_id": row["site_id"],
        "status": row["status"],
        "scope": row["scope"],
        "anon_subject_ref": row["anon_subject_ref"],
        "effective_date": row["effective_date"],
        "created_at": row["created_at"],
        "completed_at": row["completed_at"],
        "error": row["error"],
    }


def start_withdrawal_saga(
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
    subj_digest = digest_identifier(subject_id)
    now = utc_now_iso()
    eff_date = effective_date or now[:10]

    with connect_edc(paths.edc_db) as conn:
        init_saga_schema(conn)

        if idempotency_key:
            key_row = conn.execute(
                "SELECT subject_digest, request_id FROM saga_idempotency_keys WHERE site_id = ? AND idem_key = ?",
                (site_id, idempotency_key),
            ).fetchone()
            if key_row is not None:
                if key_row["subject_digest"] != subj_digest:
                    return 409, {
                        "error": "idempotency_key_conflict",
                        "detail": "Idempotency-Key already bound to another subject",
                    }
                saga = conn.execute(
                    "SELECT * FROM privacy_sagas WHERE request_id = ?", (key_row["request_id"],)
                ).fetchone()
                assert saga is not None
                return 202, _serialize_saga(saga)

        by_scope = {
            r["scope"]: r
            for r in conn.execute("SELECT * FROM privacy_sagas WHERE subject_digest = ?", (subj_digest,)).fetchall()
        }
        existing = by_scope.get("full") or by_scope.get(scope)
        if existing is not None:
            if existing["site_id"] != site_id:
                raise PermissionError("cross_site_access")
            if idempotency_key:
                conn.execute(
                    "INSERT OR IGNORE INTO saga_idempotency_keys (site_id, idem_key, subject_digest, request_id) VALUES (?, ?, ?, ?)",
                    (site_id, idempotency_key, subj_digest, existing["request_id"]),
                )
                conn.commit()
            return 202, _serialize_saga(existing)

        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        if scope == "full" and int(subj["regulatory_hold"]) == 1:
            return 409, {
                "error": "blocked_regulatory_hold",
                "status": "blocked_regulatory_hold",
                "detail": subj["hold_reason"] or "Active FDA regulatory hold",
            }

        request_id = f"wdsaga-{uuid.uuid4().hex[:12]}"
        anon_ref = f"ANON-{uuid.uuid4().hex[:12].upper()}" if scope == "full" else None
        trial_id = subj["trial_id"]

        conn.execute(
            """
            INSERT INTO privacy_sagas (
                request_id, site_id, trial_id, subject_digest, scope, anon_subject_ref,
                effective_date, status, reason, requested_by, actor, working_subject_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, ?, ?)
            """,
            (request_id, site_id, trial_id, subj_digest, scope, anon_ref, eff_date, reason, requested_by, actor, subject_id, now),
        )
        for kind, raw in (("subject_id", subject_id), ("mrn", subj["mrn"])):
            if raw:
                conn.execute(
                    "INSERT OR IGNORE INTO contact_blocks (kind, digest, request_id) VALUES (?, ?, ?)",
                    (kind, digest_identifier(raw), request_id),
                )
        if idempotency_key:
            conn.execute(
                "INSERT INTO saga_idempotency_keys (site_id, idem_key, subject_digest, request_id) VALUES (?, ?, ?, ?)",
                (site_id, idempotency_key, subj_digest, request_id),
            )
        if scope == "full":
            conn.execute(
                "INSERT OR IGNORE INTO suppression_digests (kind, digest, site_id, trial_id, anon_subject_ref, effective_date, request_id) VALUES ('subject_id', ?, ?, ?, ?, ?, ?)",
                (subj_digest, site_id, trial_id, anon_ref, eff_date, request_id),
            )
        if scope == "full" and subj["mrn"]:
            conn.execute(
                "INSERT OR IGNORE INTO suppression_digests (kind, digest, site_id, trial_id, anon_subject_ref, effective_date, request_id) VALUES ('mrn', ?, ?, ?, ?, ?, ?)",
                (digest_identifier(subj["mrn"]), site_id, trial_id, anon_ref, eff_date, request_id),
            )
        conn.commit()
        created = conn.execute("SELECT * FROM privacy_sagas WHERE request_id = ?", (request_id,)).fetchone()
        assert created is not None
        return 202, _serialize_saga(created)


def read_withdrawal_saga(paths: DataPaths, *, site_id: str, request_id: str) -> dict[str, Any]:
    with connect_edc(paths.edc_db) as conn:
        init_saga_schema(conn)
        row = conn.execute("SELECT * FROM privacy_sagas WHERE request_id = ?", (request_id,)).fetchone()
        if row is None:
            raise LookupError("withdrawal_request_not_found")
        if row["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        return _serialize_saga(row)


def advance_pending_sagas(paths: DataPaths) -> dict[str, int]:
    stats = {"completed": 0, "failed": 0}
    with connect_edc(paths.edc_db) as conn:
        init_saga_schema(conn)
        sagas = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM privacy_sagas WHERE status IN ('pending', 'in_progress') ORDER BY created_at ASC"
            ).fetchall()
        ]

    for saga in sagas:
        request_id = saga["request_id"]
        orig_subject_id = saga["working_subject_id"]
        site_id = saga["site_id"]
        trial_id = saga["trial_id"]
        anon_ref = saga["anon_subject_ref"]
        if not orig_subject_id:
            continue
        if saga["scope"] == "follow_up_only":
            _advance_follow_up_saga(paths, saga)
            stats["completed"] += 1
            continue

        try:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE privacy_sagas SET status = 'in_progress', error = NULL WHERE request_id = ? AND status != 'completed'",
                    (request_id,),
                )
                conn.commit()
                subj_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (orig_subject_id,)).fetchone()
                if subj_row is None:
                    continue
                subj = dict(subj_row)
                visits = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE subject_id = ?", (orig_subject_id,)).fetchall()]
                aes = [dict(r) for r in conn.execute("SELECT * FROM adverse_events WHERE subject_id = ?", (orig_subject_id,)).fetchall()]
                labs = [dict(r) for r in conn.execute("SELECT * FROM lab_results WHERE subject_id = ?", (orig_subject_id,)).fetchall()]
                notifs = [dict(r) for r in conn.execute("SELECT * FROM epro_notifications WHERE subject_id = ?", (orig_subject_id,)).fetchall()]
                queries = [dict(r) for r in conn.execute("SELECT * FROM data_queries WHERE subject_id = ?", (orig_subject_id,)).fetchall()]
                codings = [dict(r) for r in conn.execute("SELECT * FROM medical_coding_queue WHERE subject_id = ?", (orig_subject_id,)).fetchall()]

            orig_mrn = subj.get("mrn")
            dates = [
                *(v["visit_date"] for v in visits if v.get("visit_date")),
                *(a["onset_date"] for a in aes if a.get("onset_date")),
                *(lb["collected_date"] for lb in labs if lb.get("collected_date")),
            ]
            sensitive = extract_sensitive_strings(subj, dates)
            demo = safe_harbor_demographics(subj.get("age_at_enrollment"), subj.get("dob"))
            zip3 = safe_harbor_zip3(subj.get("postal_code"))

            if not saga["step_search_done"]:
                SearchIndex(paths.search_db).delete_subject(orig_subject_id)
                with connect_edc(paths.edc_db) as conn:
                    conn.execute("UPDATE privacy_sagas SET step_search_done = 1 WHERE request_id = ?", (request_id,))
                    conn.commit()

            if not saga["step_cache_done"]:
                cache = ProjectionCache(paths.cache_file)
                cache.delete_by_prefix(f"site:{site_id}:subject:{orig_subject_id}:")
                if orig_mrn:
                    cache.delete(f"site:{site_id}:mrn:{orig_mrn}")
                for k, v in list(cache.all_items().items()):
                    raw_s = json.dumps(v)
                    if orig_subject_id in k or orig_subject_id in raw_s or (orig_mrn and orig_mrn in raw_s):
                        cache.delete(k)
                cache.delete(f"trial:{trial_id}:safety_summary")
                with connect_edc(paths.edc_db) as conn:
                    conn.execute("UPDATE privacy_sagas SET step_cache_done = 1 WHERE request_id = ?", (request_id,))
                    conn.commit()

            if not saga["step_blobs_done"]:
                blobs = BlobStore(paths.blobs_dir)
                blobs.delete(f"econsent/{site_id}/{orig_subject_id}_signed.json")
                for k in blobs.list_keys(f"econsent/{site_id}/"):
                    if orig_subject_id in k:
                        blobs.delete(k)
                for lb in labs:
                    got = blobs.get_json(lb["blob_key"])
                    if got is not None:
                        b_payload, b_meta = got
                        blobs.put_json(
                            lb["blob_key"],
                            {"accession_no": lb["accession_no"], "anon_subject_ref": anon_ref, "study_day": lb["study_day"], "results": b_payload.get("results", [])},
                            {"doc_type": b_meta.get("doc_type", "central_lab"), "site_id": site_id, "trial_id": trial_id, "accession_no": lb["accession_no"], "anon_subject_ref": anon_ref},
                        )
                for key in blobs.list_keys(f"labs/{site_id}/shipments/"):
                    got = blobs.get_json(key)
                    if got is None:
                        continue
                    manifest, m_meta = got
                    mine = [
                        sp.get("subject_id") == orig_subject_id or (orig_mrn and sp.get("mrn") == orig_mrn)
                        for sp in manifest.get("specimens", [])
                    ]
                    if not any(mine) and orig_subject_id not in m_meta.get("subject_ids", []):
                        continue
                    manifest["specimens"] = [
                        {"accession_no": sp.get("accession_no"), "anon_subject_ref": anon_ref, "analyte": sp.get("analyte")} if hit else sp
                        for sp, hit in zip(manifest.get("specimens", []), mine)
                    ]
                    m_meta["subject_ids"] = list(dict.fromkeys(anon_ref if x == orig_subject_id else x for x in m_meta.get("subject_ids", [])))
                    blobs.put_json(key, manifest, m_meta)
                with connect_edc(paths.edc_db) as conn:
                    conn.execute("UPDATE privacy_sagas SET step_blobs_done = 1 WHERE request_id = ?", (request_id,))
                    conn.commit()

            if not saga["step_spool_done"]:
                spool = SpoolStore(paths.outbound_queue, paths.dead_letters)

                def is_target(rec: dict[str, Any]) -> bool:
                    return rec.get("subject_id") == orig_subject_id or (
                        bool(orig_mrn) and rec.get("mrn") == orig_mrn
                    )

                spool.replace_outbound([r for r in spool.read_outbound() if not is_target(r)])
                spool.replace_dead_letters([r for r in spool.read_dead_letters() if not is_target(r)])
                with connect_edc(paths.edc_db) as conn:
                    conn.execute("UPDATE privacy_sagas SET step_spool_done = 1 WHERE request_id = ?", (request_id,))
                    conn.commit()

            done_at = utc_now_iso()
            with connect_edc(paths.edc_db) as conn:
                conn.execute("BEGIN IMMEDIATE")
                if conn.execute("SELECT status FROM privacy_sagas WHERE request_id = ?", (request_id,)).fetchone()["status"] == "completed":
                    conn.rollback()
                    continue
                if not saga["step_exports_done"]:
                    manifests = conn.execute("SELECT snapshot_id, file_path FROM export_manifests WHERE trial_id = ?", (trial_id,)).fetchall()
                    for m in manifests:
                        snap_path = paths.exports_dir / m["file_path"]
                        if not snap_path.exists():
                            continue
                        doc = json.loads(snap_path.read_text(encoding="utf-8"))
                        touched = False
                        for entry in doc.get("subjects", []):
                            if entry.get("subject_id") == orig_subject_id or entry.get("anon_subject_ref") == anon_ref:
                                entry["subject_id"] = anon_ref
                                entry["anon_subject_ref"] = anon_ref
                                entry["mrn"] = None
                                entry["full_name"] = None
                                entry["email"] = None
                                entry["dob"] = demo["dob"]
                                entry["age_at_enrollment"] = demo["age_at_enrollment"]
                                entry["age_bucket"] = demo["age_bucket"]
                                entry["postal_code"] = zip3
                                entry["randomization_date"] = None
                                entry["status"] = "withdrawn"
                                for ae in entry.get("adverse_events", []):
                                    ae["onset_date"] = None
                                    if ae.get("reporter_notes"):
                                        ae["reporter_notes"] = scrub_string(ae["reporter_notes"], sensitive)
                                for lb in entry.get("labs", []):
                                    lb["collected_date"] = None
                                touched = True
                        if touched:
                            new_sha = write_snapshot_atomic(snap_path, doc)
                            conn.execute("UPDATE export_manifests SET sha256_checksum = ?, updated_at = ? WHERE snapshot_id = ?", (new_sha, done_at, m["snapshot_id"]))
                    conn.execute("UPDATE privacy_sagas SET step_exports_done = 1 WHERE request_id = ?", (request_id,))

                for v in visits:
                    conn.execute(
                        "UPDATE visits SET visit_date = NULL, clinical_notes = ?, metadata_json = ? WHERE visit_id = ?",
                        (scrub_string(v["clinical_notes"], sensitive), json.dumps(scrub_payload(json.loads(v["metadata_json"]), sensitive, anon_ref), sort_keys=True), v["visit_id"]),
                    )
                for a in aes:
                    conn.execute(
                        "UPDATE adverse_events SET onset_date = NULL, reporter_notes = ?, payload_json = ? WHERE ae_id = ?",
                        (scrub_string(a["reporter_notes"], sensitive), json.dumps(scrub_payload(json.loads(a["payload_json"]), sensitive, anon_ref), sort_keys=True), a["ae_id"]),
                    )
                for q in queries:
                    conn.execute(
                        "UPDATE data_queries SET query_text = ?, response_text = ? WHERE query_id = ?",
                        (scrub_string(q["query_text"], sensitive), scrub_string(q["response_text"], sensitive) if q.get("response_text") else None, q["query_id"]),
                    )
                for c in codings:
                    conn.execute(
                        "UPDATE medical_coding_queue SET verbatim_term = ?, coder_notes = ? WHERE coding_id = ?",
                        (scrub_string(c["verbatim_term"], sensitive), scrub_string(c["coder_notes"], sensitive), c["coding_id"]),
                    )
                for lb in labs:
                    clean_hl7 = {
                        "message_type": "ORU^R01",
                        "accession_no": lb["accession_no"],
                        "PID": {"anon_subject_ref": anon_ref},
                        "OBX": {"analyte": lb["analyte"], "value_num": lb["value_num"], "unit": lb["unit"], "reference_flag": lb["reference_flag"], "study_day": lb["study_day"]},
                    }
                    conn.execute("UPDATE lab_results SET collected_date = NULL, raw_hl7_json = ? WHERE lab_id = ?", (json.dumps(clean_hl7, sort_keys=True), lb["lab_id"]))
                for n in notifs:
                    st = "cancelled" if n["status"] == "pending" else n["status"]
                    conn.execute(
                        "UPDATE epro_notifications SET status = ?, recipient = NULL, payload_json = ? WHERE notif_id = ?",
                        (st, json.dumps(scrub_payload(json.loads(n["payload_json"]), sensitive, anon_ref), sort_keys=True), n["notif_id"]),
                    )

                conn.execute(
                    "UPDATE webhook_inbox SET subject_id = ?, mrn = NULL, raw_payload_json = ? WHERE subject_id = ? OR (? IS NOT NULL AND mrn = ?)",
                    (anon_ref, json.dumps({"redacted": True, "anon_subject_ref": anon_ref}), orig_subject_id, orig_mrn, orig_mrn),
                )
                conn.execute(
                    "UPDATE scheduled_jobs SET subject_id = ?, payload_json = ?, status = CASE WHEN status = 'scheduled' THEN 'cancelled' ELSE status END WHERE subject_id = ?",
                    (anon_ref, json.dumps({"cancelled": True, "anon_subject_ref": anon_ref}), orig_subject_id),
                )
                conn.execute("DELETE FROM audit_pii_details WHERE subject_id = ?", (orig_subject_id,))
                conn.execute(
                    "UPDATE outbox_events SET aggregate_id = ?, payload_json = ? WHERE aggregate_id = ?",
                    (anon_ref, json.dumps({"redacted": True, "anon_subject_ref": anon_ref}), orig_subject_id),
                )
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
                    (anon_ref, demo["dob"], demo["age_at_enrollment"], demo["age_bucket"], zip3, anon_ref, done_at, orig_subject_id),
                )
                append_audit_event(
                    conn, site_id=site_id, actor=saga["actor"], action="CONSENT_WITHDRAWN",
                    resource_type="subject", resource_ref=anon_ref,
                    structural_payload={"request_id": request_id, "trial_id": trial_id, "anon_subject_ref": anon_ref, "engine": "saga"},
                )
                conn.execute(
                    "UPDATE privacy_sagas SET step_edc_done = 1, working_subject_id = NULL, status = 'completed', completed_at = ?, error = NULL WHERE request_id = ?",
                    (done_at, request_id),
                )
                conn.commit()
            stats["completed"] += 1
        except Exception as exc:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE privacy_sagas SET status = 'in_progress', completed_at = NULL, error = ? WHERE request_id = ? AND status != 'completed'",
                    (f"{type(exc).__name__}: {exc}", request_id),
                )
                conn.commit()
            stats["failed"] += 1

    return stats


def _advance_follow_up_saga(paths: DataPaths, saga: dict[str, Any]) -> None:
    from trialvault.service import sync_subject_projections

    request_id = saga["request_id"]
    subject_id = saga["working_subject_id"]
    with connect_edc(paths.edc_db) as conn:
        row = conn.execute("SELECT mrn FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
    mrn = row["mrn"] if row else None

    spool = SpoolStore(paths.outbound_queue, paths.dead_letters)
    keep = [r for r in spool.read_outbound() if r.get("subject_id") != subject_id and not (mrn and r.get("mrn") == mrn)]
    spool.replace_outbound(keep)
    keep_dlq = [r for r in spool.read_dead_letters() if r.get("subject_id") != subject_id and not (mrn and r.get("mrn") == mrn)]
    spool.replace_dead_letters(keep_dlq)

    done_at = utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        conn.execute("BEGIN IMMEDIATE")
        if conn.execute("SELECT status FROM privacy_sagas WHERE request_id = ?", (request_id,)).fetchone()["status"] == "completed":
            conn.rollback()
            return
        conn.execute("UPDATE epro_notifications SET status = 'cancelled' WHERE subject_id = ? AND status = 'pending'", (subject_id,))
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
            (done_at, subject_id),
        )
        append_audit_event(
            conn, site_id=saga["site_id"], actor=saga["actor"], action="FOLLOW_UP_WITHDRAWN",
            resource_type="withdrawal_request", resource_ref=request_id,
            structural_payload={"request_id": request_id, "engine": "saga", "scope": "follow_up_only"},
        )
        conn.execute(
            "UPDATE privacy_sagas SET status = 'completed', completed_at = ?, working_subject_id = NULL, error = NULL WHERE request_id = ?",
            (done_at, request_id),
        )
        conn.commit()
    sync_subject_projections(paths, subject_id)
