"""Background outbox and scheduled-job worker for TrialVault."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from trialvault.config import DataPaths, get_data_paths
from trialvault.db import connect_edc, init_edc_db, utc_now_iso
from trialvault.service import flush_epro_to_spool, sync_subject_projections


def process_outbox_once(paths: DataPaths, now_iso: str | None = None) -> dict[str, int]:
    """Process pending outbox events and due scheduled jobs once."""
    init_edc_db(paths)
    clock_now = now_iso or utc_now_iso()
    counts = {"processed": 0, "failed": 0, "suppressed": 0, "scheduled_ran": 0}

    with connect_edc(paths.edc_db) as conn:
        events = conn.execute(
            """
            SELECT event_id, event_type, aggregate_id, site_id, payload_json, attempts
            FROM outbox_events
            WHERE status = 'pending'
            ORDER BY created_at ASC, rowid ASC
            """
        ).fetchall()

    for evt in events:
        event_id = evt["event_id"]
        event_type = evt["event_type"]
        aggregate_id = evt["aggregate_id"]
        payload: dict[str, Any] = json.loads(evt["payload_json"])

        try:
            if event_type in {
                "subject.enrolled",
                "visit.recorded",
                "ae.recorded",
                "lab.received",
                "query.opened",
            }:
                sync_subject_projections(paths, aggregate_id)
            elif event_type == "epro.scheduled":
                flush_epro_to_spool(paths, payload)

            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    """
                    UPDATE outbox_events
                    SET status = 'processed', attempts = attempts + 1, processed_at = ?, last_error = NULL
                    WHERE event_id = ?
                    """,
                    (clock_now, event_id),
                )
                conn.commit()
            counts["processed"] += 1
        except Exception as exc:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    """
                    UPDATE outbox_events
                    SET attempts = attempts + 1, last_error = ?
                    WHERE event_id = ?
                    """,
                    (f"{type(exc).__name__}: {exc}", event_id),
                )
                conn.commit()
            counts["failed"] += 1

    # Execute due scheduled jobs (run_after <= clock_now)
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
            if job_type == "epro.day30_followup":
                flush_epro_to_spool(paths, payload)
            elif job_type == "dsmb.refresh_subject_cache":
                sync_subject_projections(paths, subject_id)

            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    """
                    UPDATE scheduled_jobs
                    SET status = 'executed', attempts = attempts + 1, executed_at = ?, last_error = NULL
                    WHERE job_id = ?
                    """,
                    (clock_now, job_id),
                )
                conn.commit()
            counts["scheduled_ran"] += 1
        except Exception as exc:
            with connect_edc(paths.edc_db) as conn:
                conn.execute(
                    """
                    UPDATE scheduled_jobs
                    SET attempts = attempts + 1, last_error = ?
                    WHERE job_id = ?
                    """,
                    (f"{type(exc).__name__}: {exc}", job_id),
                )
                conn.commit()
            counts["failed"] += 1

    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="TrialVault background worker")
    parser.add_argument("--once", action="store_true", help="Drain pending jobs once and exit")
    parser.add_argument("--now", default=None, help="Optional ISO-8601 clock override for due scheduled jobs")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    args = parser.parse_args()

    if args.now:
        os.environ["TRIALVAULT_CLOCK_ISO"] = args.now

    paths = get_data_paths()
    if args.once:
        result = process_outbox_once(paths, now_iso=args.now)
        print(json.dumps(result))
        return

    while True:
        process_outbox_once(paths)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
