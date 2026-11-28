"""Background worker for TrialVault PV outbox and scheduled jobs."""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any

from trialvault_pv.cache import ProjectionCache
from trialvault_pv.config import DataPaths, get_data_paths
from trialvault_pv.db import connect_edc, init_all_dbs, utc_now_iso
from trialvault_pv.service import compute_trial_blinded_summary, sync_blinded_case_projections


def process_once(paths: DataPaths, now_iso: str | None = None) -> dict[str, int]:
    """Process pending outbox events and due scheduled jobs once."""
    init_all_dbs(paths)
    clock_now = now_iso or utc_now_iso()
    counts = {"processed": 0, "failed": 0, "scheduled_ran": 0}

    with connect_edc(paths.edc_db) as conn:
        events = conn.execute(
            """
            SELECT event_id, event_type, aggregate_id, site_id, payload_json
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
            if event_type == "subject.enrolled":
                trial_id = payload.get("trial_id")
                if trial_id:
                    ProjectionCache(paths.cache_file).delete(f"trial_blinded_summary:{trial_id}")
                    compute_trial_blinded_summary(paths, str(trial_id))
            elif event_type == "case.logged":
                sync_blinded_case_projections(paths, aggregate_id, now_iso=clock_now)

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

    with connect_edc(paths.edc_db) as conn:
        due_jobs = conn.execute(
            """
            SELECT job_id, job_type, aggregate_id, payload_json
            FROM scheduled_jobs
            WHERE status = 'scheduled' AND run_after <= ?
            ORDER BY run_after ASC, rowid ASC
            """,
            (clock_now,),
        ).fetchall()

    for job in due_jobs:
        job_id = job["job_id"]
        job_type = job["job_type"]
        aggregate_id = job["aggregate_id"]
        try:
            if job_type == "projections.refresh_case":
                sync_blinded_case_projections(paths, aggregate_id, now_iso=clock_now)

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
    parser = argparse.ArgumentParser(description="TrialVault PV background worker")
    parser.add_argument("--once", action="store_true", help="Drain pending work once and exit")
    parser.add_argument("--now", default=None, help="Optional ISO-8601 UTC clock override")
    parser.add_argument("--interval", type=float, default=1.0, help="Polling interval in seconds")
    args = parser.parse_args()

    if args.now:
        os.environ["TRIALVAULT_PV_CLOCK_ISO"] = args.now

    paths = get_data_paths()
    if args.once:
        result = process_once(paths, now_iso=args.now)
        print(json.dumps(result))
        return

    while True:
        process_once(paths)
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
