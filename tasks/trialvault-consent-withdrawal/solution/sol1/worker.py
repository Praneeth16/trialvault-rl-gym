"""SOLUTION=1 (v2) patched worker.py (outbox + scheduled jobs + replay suppression)."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from trialvault.config import DataPaths, get_data_paths
from trialvault.db import connect_edc, init_edc_db, utc_now_iso
from trialvault.service import flush_epro_to_spool, sync_subject_projections
from trialvault.withdrawal import execute_withdrawal_job, is_suppressed


def process_outbox_once(paths: DataPaths, now_iso: str | None = None) -> dict[str, int]:
    init_edc_db(paths)
    clock_now = now_iso or utc_now_iso()
    counts = {"processed": 0, "failed": 0, "suppressed": 0, "scheduled_ran": 0}

    with connect_edc(paths.edc_db) as conn:
        events = conn.execute(
            """
            SELECT event_id, event_type, aggregate_id, site_id, payload_json, attempts
            FROM outbox_events
            WHERE status = 'pending'
            ORDER BY
                CASE WHEN event_type = 'subject.withdrawal_requested' THEN 0 ELSE 1 END,
                created_at ASC,
                rowid ASC
            """
        ).fetchall()

    for evt in events:
        event_id = evt["event_id"]
        event_type = evt["event_type"]
        aggregate_id = evt["aggregate_id"]
        payload: dict[str, Any] = json.loads(evt["payload_json"])

        try:
            if event_type == "subject.withdrawal_requested":
                execute_withdrawal_job(paths, payload)
                with connect_edc(paths.edc_db) as conn:
                    conn.execute(
                        "UPDATE outbox_events SET status = 'processed', attempts = attempts + 1, processed_at = ?, last_error = NULL WHERE event_id = ?",
                        (clock_now, event_id),
                    )
                    conn.commit()
                counts["processed"] += 1
                continue

            subj_candidate = payload.get("subject_id") or aggregate_id
            mrn_candidate = payload.get("mrn")
            with connect_edc(paths.edc_db) as conn:
                if is_suppressed(conn, subject_id=subj_candidate, mrn=mrn_candidate):
                    conn.execute(
                        """
                        UPDATE outbox_events
                        SET status = 'suppressed', aggregate_id = 'SUPPRESSED',
                            payload_json = '{"suppressed": true}',
                            attempts = attempts + 1, processed_at = ?, last_error = NULL
                        WHERE event_id = ?
                        """,
                        (clock_now, event_id),
                    )
                    conn.commit()
                    counts["suppressed"] += 1
                    continue

            if event_type in {"subject.enrolled", "visit.recorded", "ae.recorded", "lab.received", "query.opened"}:
                sync_subject_projections(paths, aggregate_id)
            elif event_type == "epro.scheduled":
                flush_epro_to_spool(paths, payload)

            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE outbox_events SET status = 'processed', attempts = attempts + 1, processed_at = ?, last_error = NULL WHERE event_id = ?",
                    (clock_now, event_id),
                )
                conn.commit()
            counts["processed"] += 1
        except Exception as exc:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE outbox_events SET attempts = attempts + 1, last_error = ? WHERE event_id = ?",
                    (f"{type(exc).__name__}: {exc}", event_id),
                )
                conn.commit()
            counts["failed"] += 1

    # Process due scheduled jobs with suppression guard
    with connect_edc(paths.edc_db) as conn:
        due_jobs = conn.execute(
            """
            SELECT job_id, job_type, subject_id, site_id, payload_json
            FROM scheduled_jobs
            WHERE status = 'scheduled' AND run_after <= ?
            ORDER BY run_after ASC, rowid ASC
            """,
            (clock_now,),
        ).fetchall()

    for job in due_jobs:
        job_id = job["job_id"]
        job_type = job["job_type"]
        subject_id = job["subject_id"]
        payload = json.loads(job["payload_json"])
        try:
            with connect_edc(paths.edc_db) as conn:
                if is_suppressed(
                    conn,
                    subject_id=payload.get("subject_id") or subject_id,
                    mrn=payload.get("mrn"),
                ):
                    conn.execute(
                        """
                        UPDATE scheduled_jobs
                        SET status = 'suppressed', subject_id = 'SUPPRESSED',
                            payload_json = '{"suppressed": true}',
                            attempts = attempts + 1, executed_at = ?, last_error = NULL
                        WHERE job_id = ?
                        """,
                        (clock_now, job_id),
                    )
                    conn.commit()
                    counts["suppressed"] += 1
                    continue

            if job_type == "epro.day30_followup":
                flush_epro_to_spool(paths, payload)
            elif job_type == "dsmb.refresh_subject_cache":
                sync_subject_projections(paths, subject_id)

            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE scheduled_jobs SET status = 'executed', attempts = attempts + 1, executed_at = ?, last_error = NULL WHERE job_id = ?",
                    (clock_now, job_id),
                )
                conn.commit()
            counts["scheduled_ran"] += 1
        except Exception as exc:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    "UPDATE scheduled_jobs SET attempts = attempts + 1, last_error = ? WHERE job_id = ?",
                    (f"{type(exc).__name__}: {exc}", job_id),
                )
                conn.commit()
            counts["failed"] += 1

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="TrialVault background worker")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--now", default=None)
    parser.add_argument("--interval", type=float, default=1.0)
    args = parser.parse_args()

    if args.now:
        os.environ["TRIALVAULT_CLOCK_ISO"] = args.now

    paths = get_data_paths()
    if args.once:
        print(json.dumps(process_outbox_once(paths, now_iso=args.now)))
        return
    while True:
        process_outbox_once(paths)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
