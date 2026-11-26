"""Clinical monitor data queries and MedDRA medical coding workflow."""

from __future__ import annotations

import uuid
from typing import Any

from trialvault.audit import append_audit_event
from trialvault.config import DataPaths
from trialvault.db import connect_edc, utc_now_iso
from trialvault.service import enqueue_outbox, opaque_audit_ref


def open_data_query(
    paths: DataPaths,
    *,
    site_id: str,
    subject_id: str,
    actor: str,
    data: dict[str, Any],
) -> dict[str, Any]:
    query_id = data.get("query_id") or f"QRY-{uuid.uuid4().hex[:8].upper()}"
    now = utc_now_iso()
    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        conn.execute(
            """
            INSERT INTO data_queries (
                query_id, subject_id, site_id, field_name, query_text, response_text, status, opened_by, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                query_id,
                subject_id,
                site_id,
                data["field_name"],
                data["query_text"],
                data.get("response_text"),
                data.get("status", "open"),
                actor,
                now,
            ),
        )
        append_audit_event(
            conn,
            site_id=site_id,
            actor=actor,
            action="DATA_QUERY_OPENED",
            resource_type="data_query",
            resource_ref=opaque_audit_ref("qry", query_id),
            structural_payload={"field_name": data["field_name"], "status": data.get("status", "open")},
            pii_subject_id=subject_id,
            pii_detail={"subject_id": subject_id, "query_text": data["query_text"]},
        )
        enqueue_outbox(
            conn,
            event_type="query.opened",
            aggregate_id=subject_id,
            site_id=site_id,
            payload={
                "query_id": query_id,
                "subject_id": subject_id,
                "mrn": subj["mrn"],
                "query_text": data["query_text"],
            },
        )
        conn.commit()
        return dict(conn.execute("SELECT * FROM data_queries WHERE query_id = ?", (query_id,)).fetchone())


def list_subject_queries(paths: DataPaths, *, site_id: str, subject_id: str) -> list[dict[str, Any]]:
    with connect_edc(paths.edc_db) as conn:
        subj = conn.execute("SELECT site_id FROM subjects WHERE subject_id = ?", (subject_id,)).fetchone()
        if subj is None:
            raise LookupError("subject_not_found")
        if subj["site_id"] != site_id:
            raise PermissionError("cross_site_access")
        rows = conn.execute(
            "SELECT * FROM data_queries WHERE subject_id = ? ORDER BY created_at ASC",
            (subject_id,),
        ).fetchall()
        return [dict(r) for r in rows]
