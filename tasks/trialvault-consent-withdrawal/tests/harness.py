"""Black-box, fresh-seeded, 3-run minimum TrialVault v2 verifier harness.

Spawns the submitted FastAPI application (`uvicorn trialvault.api:app`) and worker CLI
(`python -m trialvault.worker --once`) as isolated subprocesses against a fresh
temporary TRIALVAULT_DATA_DIR with randomized identifiers on each run, then probes
the public HTTP API and inspects all active stores, exports, archives, and queues.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

ALL_CHECK_IDS: tuple[str, ...] = (
    # 1. api_contract (10.0)
    "post_returns_202_pending",
    "get_status_contract",
    "unknown_subject_404",
    "idempotency_replay_same_subject",
    "idempotency_conflict_diff_subject_409",
    # 2. active_store_deid (26.0)
    "subjects_direct_phi_scrubbed",
    "safe_harbor_zip3_and_restricted_zeroed",
    "safe_harbor_age_over_89_and_dates",
    "freetext_notes_and_hl7_redacted",
    "fts5_search_index_clean",
    "projection_cache_clean",
    "blobs_and_metadata_clean",
    "aliases_and_initials_redacted",
    "sdtm_warehouse_deidentified_and_retained",
    # 3. clinical_retention
    "safety_summary_arm_totals_preserved",
    "clinical_rows_retained_and_rekeyed",
    "part11_audit_chain_valid",
    "audit_pii_purged_and_withdrawal_event_logged",
    # 4. async_convergence
    "not_completed_before_convergence",
    "converges_to_completed_after_worker",
    "epro_notifications_spool_and_dlq_sanitized",
    "concurrent_workers_exactly_once",
    # 5. replay_and_late_webhooks
    "late_hl7_webhook_suppressed",
    "replayed_outbox_event_suppressed",
    "active_subject_webhook_still_ingests",
    # 6. failure_and_recovery
    "transient_fault_prevents_premature_completion",
    "retry_after_fault_converges_cleanly",
    "late_stage_audit_fault_recovery",
    # 7. isolation_and_survivors
    "cross_site_and_scope_isolation",
    "regulatory_hold_blocks_409_no_mutation",
    "co_enrolled_household_survivor_untouched",
    "shared_shipment_manifest_scoped",
    "site_monitoring_logs_and_sdv_scoped",
    # 8. baseline_health
    "existing_edc_workflows_preserved_post_withdrawal",
    # 9. withdrawal_scope
    "follow_up_only_stops_contact_keeps_data",
    "scope_escalation_to_full",
)

_EVAL_LOCK = threading.Lock()
_EVAL_CACHE: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _json(resp: httpx.Response | None) -> dict[str, Any]:
    if resp is None:
        return {}
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _alt_count(summary: dict[str, Any]) -> int:
    return int(summary.get("lab_summary", {}).get("ALT", {}).get("count", -1))


class _Phase:
    """Isolates a harness phase: an exception zeroes only checks that have not been scored yet."""

    def __init__(self, checks: dict[str, dict[str, Any]], name: str) -> None:
        self.checks = checks
        self.name = name

    def __enter__(self) -> _Phase:
        return self

    def __exit__(self, exc_type: Any, exc: BaseException | None, tb: Any) -> bool:
        if exc is None or not isinstance(exc, Exception):
            return False
        for entry in self.checks.values():
            if entry["reasoning"] == "not reached":
                entry["reasoning"] = f"{self.name} aborted: {type(exc).__name__}: {exc}"
        return True


def _verify_part11_chain(edc_db: Path) -> tuple[bool, str, list[dict[str, Any]]]:
    conn = sqlite3.connect(str(edc_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT seq_id, event_time, site_id, actor, action, resource_type,
                       resource_ref, payload_hash, prev_hash, entry_hash
                FROM audit_log
                ORDER BY seq_id ASC
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    if not rows:
        return False, "audit_log is empty", rows

    expected_prev = "0" * 64
    prev_seq = 0
    for row in rows:
        if row["seq_id"] <= prev_seq:
            return False, f"non-monotonic seq_id={row['seq_id']}", rows
        if row["prev_hash"] != expected_prev:
            return False, f"broken prev_hash at seq_id={row['seq_id']}", rows
        canonical = "|".join(
            [
                row["event_time"],
                row["site_id"],
                row["actor"],
                row["action"],
                row["resource_type"],
                row["resource_ref"],
                row["payload_hash"],
                row["prev_hash"],
            ]
        )
        computed = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        if computed != row["entry_hash"]:
            return False, f"entry_hash mismatch at seq_id={row['seq_id']}", rows
        expected_prev = row["entry_hash"]
        prev_seq = row["seq_id"]

    return True, f"verified {len(rows)} hash-chained entries", rows


@dataclass
class _SeedFixture:
    trial_id: str
    site1_id: str
    site2_id: str
    tok_s1_full: str
    tok_s1_ro: str
    tok_s2_full: str
    primary: dict[str, Any]
    survivor: dict[str, Any]
    elder: dict[str, Any]
    hold: dict[str, Any]
    fault: dict[str, Any]
    acc_primary: str
    acc_survivor: str
    acc_elder: str
    acc_hold: str
    acc_fault: str
    fu: dict[str, Any]
    race: dict[str, Any]
    audit_fault: dict[str, Any]
    acc_race: str
    acc_audit_fault: str
    shipment_id: str
    acc_tag: str
    alias_notes: dict[str, str]


def _make_fixture(run_idx: int) -> _SeedFixture:
    tag = f"R{run_idx}{uuid.uuid4().hex[:5].upper()}"
    acc_tag = uuid.uuid4().hex[:8].upper()
    trial_id = f"TRIAL-{tag}"
    site1_id = f"SITE1-{tag}"
    site2_id = f"SITE2-{tag}"
    hh_id = f"HH-{tag}"
    shared_addr = f"904 Harbor Way Suite {tag}"
    shared_ec_phone = f"+1-617-555-9{run_idx:03d}"

    primary = {
        "subject_id": f"SUBJ-PRI-{tag}",
        "mrn": f"MRN-PRI-{tag}",
        "full_name": f"Nadia Kowalski{tag}",
        "email": f"nadia.{tag.lower()}@clinic-test.example",
        "phone": f"+1-617-555-1{run_idx:03d}",
        "ssn": f"11{run_idx}-22-3344",
        "dob": "1972-05-11",
        "age_at_enrollment": 54,
        "street_address": shared_addr,
        "city": "Boston",
        "state": "MA",
        "postal_code": "02115",
        "device_serial": f"ECG-PRI-{tag}",
        "ip_address": f"198.51.100.{10 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-pri",
        "emergency_contact_name": f"Marek Kowalski{tag}",
        "emergency_contact_phone": shared_ec_phone,
        "treatment_arm": "DRUG_A_100MG",
        "randomization_date": "2026-02-10",
        "household_group_id": hh_id,
        "investigator_name": "Dr. Priya Raman",
        "aliases": ["Nadz", f"Nadia Nowak{tag}"],
    }
    survivor = {
        "subject_id": f"SUBJ-SURV-{tag}",
        "mrn": f"MRN-SURV-{tag}",
        "full_name": f"Tomasz Kowalski{tag}",
        "email": f"tomasz.{tag.lower()}@clinic-test.example",
        "phone": f"+1-617-555-2{run_idx:03d}",
        "ssn": f"22{run_idx}-33-4455",
        "dob": "1969-09-21",
        "age_at_enrollment": 57,
        "street_address": shared_addr,
        "city": "Boston",
        "state": "MA",
        "postal_code": "02115",
        "device_serial": f"ECG-SURV-{tag}",
        "ip_address": f"198.51.100.{20 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-surv",
        "emergency_contact_name": f"Marek Kowalski{tag}",
        "emergency_contact_phone": shared_ec_phone,
        "treatment_arm": "PLACEBO",
        "randomization_date": "2026-02-12",
        "household_group_id": hh_id,
        "investigator_name": "Dr. Priya Raman",
        "aliases": ["Tomek"],
    }
    elder = {
        "subject_id": f"SUBJ-ELD-{tag}",
        "mrn": f"MRN-ELD-{tag}",
        "full_name": f"Bernard Whitman{tag}",
        "email": f"bernard.{tag.lower()}@clinic-test.example",
        "phone": f"+1-603-555-3{run_idx:03d}",
        "ssn": f"33{run_idx}-44-5566",
        "dob": "1934-06-14",
        "age_at_enrollment": 92,
        "street_address": f"44 Birch Hill Rd {tag}",
        "city": "Charlestown",
        "state": "NH",
        "postal_code": "03603",
        "device_serial": f"ECG-ELD-{tag}",
        "ip_address": f"198.51.100.{30 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-eld",
        "emergency_contact_name": f"Helen Whitman{tag}",
        "emergency_contact_phone": f"+1-603-555-8{run_idx:03d}",
        "treatment_arm": "DRUG_A_100MG",
        "randomization_date": "2026-02-18",
        "investigator_name": "Dr. Priya Raman",
    }
    hold = {
        "subject_id": f"SUBJ-HOLD-{tag}",
        "mrn": f"MRN-HOLD-{tag}",
        "full_name": f"Sonia Alvarez{tag}",
        "email": f"sonia.{tag.lower()}@clinic-test.example",
        "phone": f"+1-617-555-4{run_idx:03d}",
        "ssn": f"44{run_idx}-55-6677",
        "dob": "1981-01-30",
        "age_at_enrollment": 45,
        "street_address": f"12 Boylston Ave {tag}",
        "city": "Boston",
        "state": "MA",
        "postal_code": "02116",
        "device_serial": f"ECG-HOLD-{tag}",
        "ip_address": f"198.51.100.{40 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-hold",
        "emergency_contact_name": f"Diego Alvarez{tag}",
        "emergency_contact_phone": f"+1-617-555-7{run_idx:03d}",
        "treatment_arm": "DRUG_A_100MG",
        "randomization_date": "2026-02-20",
        "regulatory_hold": True,
        "hold_reason": "FDA Form 483 clinical inspection hold",
        "investigator_name": "Dr. Priya Raman",
    }
    fault = {
        "subject_id": f"SUBJ-FLT-{tag}",
        "mrn": f"MRN-FLT-{tag}",
        "full_name": f"Gideon Mercer{tag}",
        "email": f"gideon.{tag.lower()}@clinic-test.example",
        "phone": f"+1-775-555-5{run_idx:03d}",
        "ssn": f"55{run_idx}-66-7788",
        "dob": "1985-10-08",
        "age_at_enrollment": 41,
        "street_address": f"77 Sagebrush Ln {tag}",
        "city": "Ely",
        "state": "NV",
        "postal_code": "89301",
        "device_serial": f"ECG-FLT-{tag}",
        "ip_address": f"198.51.100.{50 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-flt",
        "emergency_contact_name": f"Claire Mercer{tag}",
        "emergency_contact_phone": f"+1-775-555-6{run_idx:03d}",
        "treatment_arm": "PLACEBO",
        "randomization_date": "2026-02-22",
        "investigator_name": "Dr. Priya Raman",
    }

    fu = {
        "subject_id": f"SUBJ-FU-{tag}",
        "mrn": f"MRN-FU-{tag}",
        "full_name": f"Leah Okafor{tag}",
        "email": f"leah.{tag.lower()}@clinic-test.example",
        "phone": f"+1-617-555-6{run_idx:03d}",
        "ssn": f"66{run_idx}-77-8899",
        "dob": "1990-04-02",
        "age_at_enrollment": 36,
        "street_address": f"5 Prospect St {tag}",
        "city": "Cambridge",
        "state": "MA",
        "postal_code": "02139",
        "device_serial": f"ECG-FU-{tag}",
        "ip_address": f"198.51.100.{60 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-fu",
        "emergency_contact_name": f"Ada Okafor{tag}",
        "emergency_contact_phone": f"+1-617-555-5{run_idx:03d}",
        "treatment_arm": "PLACEBO",
        "randomization_date": "2026-02-27",
        "investigator_name": "Dr. Priya Raman",
    }
    race = {
        "subject_id": f"SUBJ-RACE-{tag}",
        "mrn": f"MRN-RACE-{tag}",
        "full_name": f"Owen Brandt{tag}",
        "email": f"owen.{tag.lower()}@clinic-test.example",
        "phone": f"+1-617-555-3{run_idx:03d}",
        "ssn": f"77{run_idx}-88-9900",
        "dob": "1979-12-12",
        "age_at_enrollment": 46,
        "street_address": f"300 Comm Ave {tag}",
        "city": "Boston",
        "state": "MA",
        "postal_code": "02134",
        "device_serial": f"ECG-RACE-{tag}",
        "ip_address": f"198.51.100.{70 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-race",
        "emergency_contact_name": f"Ivy Brandt{tag}",
        "emergency_contact_phone": f"+1-617-555-4{run_idx:03d}",
        "treatment_arm": "DRUG_A_100MG",
        "randomization_date": "2026-02-25",
        "investigator_name": "Dr. Priya Raman",
    }
    audit_fault = {
        "subject_id": f"SUBJ-AFLT-{tag}",
        "mrn": f"MRN-AFLT-{tag}",
        "full_name": f"Vera Lindqvist{tag}",
        "email": f"vera.{tag.lower()}@clinic-test.example",
        "phone": f"+1-775-555-7{run_idx:03d}",
        "ssn": f"88{run_idx}-99-1122",
        "dob": "1983-03-19",
        "age_at_enrollment": 43,
        "street_address": f"19 Aspen Ridge {tag}",
        "city": "Ely",
        "state": "NV",
        "postal_code": "89301",
        "device_serial": f"ECG-AFLT-{tag}",
        "ip_address": f"198.51.100.{80 + run_idx}",
        "portal_url": f"https://portal.trialvault.example/s/{tag.lower()}-aflt",
        "emergency_contact_name": f"Nils Lindqvist{tag}",
        "emergency_contact_phone": f"+1-775-555-8{run_idx:03d}",
        "treatment_arm": "DRUG_A_100MG",
        "randomization_date": "2026-02-23",
        "investigator_name": "Dr. Priya Raman",
        "aliases": ["Veruschka"],
    }
    alias_notes = {
        primary["subject_id"]: f" Prefers to be called Nadz; prior records filed under Nadia Nowak{tag}. Diary card initialled N.K.",
        survivor["subject_id"]: " Prefers to be called Tomek. Diary card initialled T.K.",
        audit_fault["subject_id"]: " Known by nickname Veruschka.",
    }

    return _SeedFixture(
        trial_id=trial_id,
        site1_id=site1_id,
        site2_id=site2_id,
        tok_s1_full=f"tok-s1-full-{tag}",
        tok_s1_ro=f"tok-s1-ro-{tag}",
        tok_s2_full=f"tok-s2-full-{tag}",
        primary=primary,
        survivor=survivor,
        elder=elder,
        hold=hold,
        fault=fault,
        acc_primary=f"ACC-PRI-{acc_tag}",
        acc_survivor=f"ACC-SURV-{acc_tag}",
        acc_elder=f"ACC-ELD-{acc_tag}",
        acc_hold=f"ACC-HOLD-{acc_tag}",
        acc_fault=f"ACC-FLT-{acc_tag}",
        fu=fu,
        race=race,
        audit_fault=audit_fault,
        acc_race=f"ACC-RACE-{acc_tag}",
        acc_audit_fault=f"ACC-AFLT-{acc_tag}",
        shipment_id=f"SHIP-{acc_tag}",
        acc_tag=acc_tag,
        alias_notes=alias_notes,
    )


def _run_worker(workspace: Path, data_dir: Path, now_iso: str | None = None) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_DATA_DIR"] = str(data_dir)
    cmd = [sys.executable, "-m", "trialvault.worker", "--once"]
    if now_iso:
        env["TRIALVAULT_CLOCK_ISO"] = now_iso
        cmd.extend(["--now", now_iso])
    return subprocess.run(cmd, cwd=str(workspace), env=env, capture_output=True, text=True, timeout=30)


def _run_workers_concurrently(
    workspace: Path, data_dir: Path, n: int, now_iso: str
) -> list[subprocess.CompletedProcess[str]]:
    """Launch n worker processes that all start draining at the same instant (overlapping cron runs)."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_DATA_DIR"] = str(data_dir)
    env["TRIALVAULT_CLOCK_ISO"] = now_iso
    start_at = time.time() + 2.5
    code = (
        "import runpy, sys, time\n"
        f"sys.argv = ['trialvault.worker', '--once', '--now', {now_iso!r}]\n"
        "import trialvault.worker\n"
        f"time.sleep(max(0.0, {start_at!r} - time.time()))\n"
        "runpy.run_module('trialvault.worker', run_name='__main__')\n"
    )
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", code], cwd=str(workspace), env=env,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for _ in range(n)
    ]
    results = []
    for proc in procs:
        try:
            out, err = proc.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            proc.kill()
            out, err = proc.communicate()
        results.append(subprocess.CompletedProcess(proc.args, proc.returncode, out, err))
    return results


def _start_server(workspace: Path, data_dir: Path, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_DATA_DIR"] = str(data_dir)
    env["TRIALVAULT_CLOCK_ISO"] = "2026-03-10T12:00:00+00:00"
    proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "trialvault.api:app", "--host", "127.0.0.1", "--port", str(port), "--log-level", "error"],
        cwd=str(workspace),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 15.0
    url = f"http://127.0.0.1:{port}/health"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=2)
            raise RuntimeError(f"uvicorn exited early ({proc.returncode}): {err or out}")
        try:
            r = httpx.get(url, timeout=1.0)
            if r.status_code == 200:
                return proc
        except Exception:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("uvicorn healthcheck timed out")


def _contains_any(text: str, needles: list[str]) -> list[str]:
    lower = text.lower()
    return [n for n in needles if n and n.lower() in lower]


def _seed_via_public_api(client: httpx.Client, data_dir: Path, fx: _SeedFixture) -> str:
    edc_db = data_dir / "edc.db"
    conn = sqlite3.connect(str(edc_db))
    try:
        conn.execute(
            "INSERT INTO trials (trial_id, protocol_code, sponsor_name, phase, active) VALUES (?, ?, 'Apex Bio', 'Phase III', 1)",
            (fx.trial_id, f"PROT-{fx.trial_id}"),
        )
        conn.execute(
            "INSERT INTO sites (site_id, trial_id, name, country, active) VALUES (?, ?, 'Site One', 'US', 1), (?, ?, 'Site Two', 'UK', 1)",
            (fx.site1_id, fx.trial_id, fx.site2_id, fx.trial_id),
        )
        conn.execute(
            "INSERT INTO site_tokens (token_hash, site_id, label, scopes, active) VALUES (?, ?, 'coord-1', 'edc:read,edc:write,privacy:withdraw', 1), (?, ?, 'monitor-1', 'edc:read', 1), (?, ?, 'coord-2', 'edc:read,edc:write,privacy:withdraw', 1)",
            (_sha256_hex(fx.tok_s1_full), fx.site1_id, _sha256_hex(fx.tok_s1_ro), fx.site1_id, _sha256_hex(fx.tok_s2_full), fx.site2_id),
        )
        conn.commit()
    finally:
        conn.close()

    h1 = {"Authorization": f"Bearer {fx.tok_s1_full}"}
    for subj in (fx.primary, fx.survivor, fx.elder, fx.hold, fx.fault, fx.fu, fx.race, fx.audit_fault):
        r = client.post("/v1/subjects", headers=h1, json=subj)
        if r.status_code != 201:
            raise RuntimeError(f"seed enroll failed for {subj['subject_id']}: {r.status_code} {r.text}")

    for subj, vdate, sbp, dbp in (
        (fx.primary, "2026-02-24", 126, 80),
        (fx.survivor, "2026-02-26", 118, 74),
        (fx.elder, "2026-03-04", 134, 82),
        (fx.hold, "2026-03-06", 122, 76),
        (fx.fault, "2026-03-08", 120, 75),
        (fx.fu, "2026-03-05", 116, 72),
        (fx.race, "2026-03-07", 128, 84),
        (fx.audit_fault, "2026-03-06", 124, 79),
    ):
        r = client.post(
            f"/v1/subjects/{subj['subject_id']}/visits",
            headers=h1,
            json={
                "visit_number": 1,
                "visit_date": vdate,
                "days_from_randomization": 14,
                "systolic_bp": sbp,
                "diastolic_bp": dbp,
                "clinical_notes": f"Evaluated {subj['full_name']} ({subj['mrn']}, phone {subj['phone']}, device {subj['device_serial']}) on {vdate}. Stable hemodynamics."
                + fx.alias_notes.get(subj["subject_id"], ""),
                "metadata": {"subject_id": subj["subject_id"], "patient_name": subj["full_name"], "email": subj["email"], "room": "C4"},
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed visit failed: {r.status_code} {r.text}")

    for subj, term, grade, serious, odate in (
        (fx.primary, "Neutropenia", 3, True, "2026-03-01"),
        (fx.survivor, "Headache", 1, False, "2026-03-03"),
        (fx.elder, "Hypertension", 2, False, "2026-03-09"),
    ):
        r = client.post(
            f"/v1/subjects/{subj['subject_id']}/adverse-events",
            headers=h1,
            json={
                "meddra_term": term,
                "ctcae_grade": grade,
                "serious": serious,
                "onset_date": odate,
                "onset_study_day": 19,
                "relatedness": "possibly_related",
                "reporter_notes": f"Reported by {subj['full_name']} via {subj['email']} ({subj['subject_id']}, SSN {subj['ssn']}).",
                "payload": {"subject_id": subj["subject_id"], "mrn": subj["mrn"], "patient_name": subj["full_name"], "ctcae_grade": grade},
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed AE failed: {r.status_code} {r.text}")

    # Clinical monitor data queries (with embedded PHI)
    for subj in (fx.primary, fx.elder, fx.survivor):
        r = client.post(
            f"/v1/subjects/{subj['subject_id']}/queries",
            headers=h1,
            json={
                "field_name": "adverse_events.ctcae_grade",
                "query_text": f"Monitor verify grade for {subj['full_name']} ({subj['mrn']}, DOB {subj['dob']})." + fx.alias_notes.get(subj["subject_id"], ""),
                "response_text": f"Confirmed for {subj['full_name']} via {subj['email']}.",
                "status": "resolved",
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed query failed: {r.status_code} {r.text}")

    # Central-lab webhooks
    for subj, acc, analyte, val, unit, cdate in (
        (fx.primary, fx.acc_primary, "ALT", 44.0, "U/L", "2026-02-24"),
        (fx.survivor, fx.acc_survivor, "ALT", 28.0, "U/L", "2026-02-26"),
        (fx.elder, fx.acc_elder, "AST", 36.5, "U/L", "2026-03-04"),
        (fx.hold, fx.acc_hold, "ALT", 31.0, "U/L", "2026-03-06"),
        (fx.fault, fx.acc_fault, "eGFR", 78.0, "mL/min", "2026-03-08"),
        (fx.race, fx.acc_race, "ALT", 39.0, "U/L", "2026-03-07"),
        (fx.audit_fault, fx.acc_audit_fault, "ALT", 41.0, "U/L", "2026-03-06"),
    ):
        r = client.post(
            "/v1/webhooks/central-lab",
            json={
                "shipment_id": fx.shipment_id if subj in (fx.primary, fx.survivor) else None,
                "site_id": fx.site1_id,
                "trial_id": fx.trial_id,
                "subject_id": subj["subject_id"],
                "mrn": subj["mrn"],
                "accession_no": acc,
                "analyte": analyte,
                "value_num": val,
                "unit": unit,
                "reference_flag": "NORMAL",
                "collected_date": cdate,
                "study_day": 14,
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed lab webhook failed: {r.status_code} {r.text}")

    # ePRO reminders
    for subj in (fx.primary, fx.survivor, fx.elder, fx.fault, fx.fu):
        r = client.post(f"/v1/subjects/{subj['subject_id']}/epro-reminders", headers=h1, json={"status": "pending"})
        if r.status_code != 201:
            raise RuntimeError(f"seed pending epro failed: {r.status_code} {r.text}")
    r = client.post(
        f"/v1/subjects/{fx.primary['subject_id']}/epro-reminders",
        headers=h1,
        json={"status": "sent", "sent_at": "2026-02-20T09:00:00+00:00"},
    )
    if r.status_code != 201:
        raise RuntimeError(f"seed sent epro failed: {r.status_code} {r.text}")

    # Dead letters
    dlq_path = data_dir / "spool" / "dead_letters.jsonl"
    dlq_path.parent.mkdir(parents=True, exist_ok=True)
    with dlq_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps({"dlq_id": f"DLQ-PRI-{fx.trial_id}", "subject_id": fx.primary["subject_id"], "mrn": fx.primary["mrn"], "email": fx.primary["email"], "error": "smtp_timeout"}) + "\n")
        fh.write(json.dumps({"dlq_id": f"DLQ-SURV-{fx.trial_id}", "subject_id": fx.survivor["subject_id"], "mrn": fx.survivor["mrn"], "email": fx.survivor["email"], "error": "smtp_timeout"}) + "\n")
        fh.write(json.dumps({"dlq_id": f"DLQ-FU-{fx.trial_id}", "subject_id": fx.fu["subject_id"], "mrn": fx.fu["mrn"], "email": fx.fu["email"], "error": "smtp_timeout"}) + "\n")

    # Generate interim DSMB snapshot BEFORE withdrawal
    snap_resp = client.post(f"/v1/trials/{fx.trial_id}/dsmb-snapshots", headers=h1)
    if snap_resp.status_code != 201:
        raise RuntimeError(f"seed DSMB snapshot failed: {snap_resp.status_code} {snap_resp.text}")
    return snap_resp.json()["snapshot_id"]


def _run_single_trial(workspace: Path, run_idx: int) -> dict[str, dict[str, Any]]:
    checks: dict[str, dict[str, Any]] = {
        cid: {"score": 0.0, "reasoning": "not reached"} for cid in ALL_CHECK_IDS
    }
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"tv_eval_r{run_idx}_"))
    port = _find_free_port()
    proc: subprocess.Popen[str] | None = None

    try:
        proc = _start_server(workspace, tmp_dir, port)
        base_url = f"http://127.0.0.1:{port}"
        fx = _make_fixture(run_idx)

        with httpx.Client(base_url=base_url, timeout=15.0) as client:
            snapshot_id = _seed_via_public_api(client, tmp_dir, fx)
            w0 = _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:00:00+00:00")
            if w0.returncode != 0:
                raise RuntimeError(f"initial worker failed: {w0.stderr}")

            h1 = {"Authorization": f"Bearer {fx.tok_s1_full}"}
            h1_ro = {"Authorization": f"Bearer {fx.tok_s1_ro}"}
            h2 = {"Authorization": f"Bearer {fx.tok_s2_full}"}

            pre_summary_resp = client.get(f"/v1/trials/{fx.trial_id}/safety-summary", headers=h1)
            if pre_summary_resp.status_code != 200:
                raise RuntimeError(f"baseline safety summary failed: {pre_summary_resp.status_code}")
            pre_summary = pre_summary_resp.json()
            pre_sdtm_resp = client.get(f"/v1/trials/{fx.trial_id}/sdtm-summary", headers=h1)
            if pre_sdtm_resp.status_code != 200:
                raise RuntimeError(f"baseline sdtm summary failed: {pre_sdtm_resp.status_code}")
            pre_sdtm = pre_sdtm_resp.json()
            client.get(f"/v1/subjects/{fx.primary['subject_id']}", headers=h1)
            client.get(f"/v1/subjects/{fx.survivor['subject_id']}", headers=h1)

            _, _, pre_audit_rows = _verify_part11_chain(tmp_dir / "part11_audit.db")
            pre_audit_count = len(pre_audit_rows)

            pri_needles = [
                fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"],
                fx.primary["email"], fx.primary["phone"], fx.primary["ssn"],
                fx.primary["device_serial"], fx.primary["street_address"], fx.primary["dob"], "2026-02-24",
            ]
            eld_needles = [
                fx.elder["subject_id"], fx.elder["mrn"], fx.elder["full_name"],
                fx.elder["email"], fx.elder["phone"], fx.elder["ssn"],
                fx.elder["device_serial"], fx.elder["dob"],
            ]
            req_pri_id: str | None = None
            anon_pri: str | None = None
            anon_eld: str | None = None
            req_eld_id: str | None = None
            converged_ok = False
            pri_withdrawn = False
            fu_req_id: str | None = None
            fu_post: httpx.Response | None = None

            with _Phase(checks, "phase a"):
                # ------------------------------------------------------------------
                # PHASE A: API Contract, Async Pending, Idempotency, Auth, Hold
                # ------------------------------------------------------------------
                idem_primary = f"idem-pri-{fx.trial_id}"
                eff_date = "2026-03-10"
                post_pri = client.post(
                    f"/v1/subjects/{fx.primary['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": idem_primary},
                    json={"reason": "participant_withdrew_consent", "requested_by": "coord-1", "effective_date": eff_date},
                )

                pri_body = _json(post_pri)
                req_pri_id = pri_body.get("request_id")
                anon_pri = pri_body.get("anon_subject_ref")
                unlinkable_ref = (
                    isinstance(anon_pri, str)
                    and len(anon_pri) >= 6
                    and fx.primary["subject_id"].lower() not in anon_pri.lower()
                    and fx.primary["mrn"].lower() not in anon_pri.lower()
                )
                post_ok = (
                    post_pri.status_code == 202
                    and isinstance(req_pri_id, str)
                    and bool(req_pri_id)
                    and pri_body.get("site_id") == fx.site1_id
                    and pri_body.get("status") == "pending"
                    and pri_body.get("completed_at") is None
                    and pri_body.get("effective_date") == eff_date
                    and unlinkable_ref
                )
                checks["post_returns_202_pending"] = {
                    "score": 1.0 if post_ok else 0.0,
                    "reasoning": f"POST HTTP {post_pri.status_code} body={str(pri_body)[:300]}",
                }

                get_before = client.get(f"/v1/withdrawal-requests/{req_pri_id}", headers=h1)
                get_before_body = _json(get_before) if get_before.status_code == 200 else {}
                not_premature = (
                    get_before.status_code == 200
                    and get_before_body.get("status") in {"pending", "in_progress"}
                    and get_before_body.get("completed_at") is None
                )
                checks["not_completed_before_convergence"] = {
                    "score": 1.0 if not_premature else 0.0,
                    "reasoning": f"GET before worker: HTTP {get_before.status_code}, status={get_before_body.get('status')}",
                }

                unk = client.post(f"/v1/subjects/SUBJ-UNKNOWN-{fx.trial_id}/withdrawal-requests", headers=h1, json={})
                checks["unknown_subject_404"] = {
                    "score": 1.0 if (unk.status_code == 404 and post_pri.status_code == 202) else 0.0,
                    "reasoning": f"unknown subject POST returned HTTP {unk.status_code} (known subject POST={post_pri.status_code})",
                }

                replay_same = client.post(
                    f"/v1/subjects/{fx.primary['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": idem_primary},
                    json={"reason": "retry"},
                )
                dup_nokey = client.post(
                    f"/v1/subjects/{fx.primary['subject_id']}/withdrawal-requests",
                    headers=h1,
                    json={"reason": "dup"},
                )
                idem_replay_ok = (
                    replay_same.status_code == 202
                    and _json(replay_same).get("request_id") == req_pri_id
                    and dup_nokey.status_code == 202
                    and _json(dup_nokey).get("request_id") == req_pri_id
                )
                checks["idempotency_replay_same_subject"] = {
                    "score": 1.0 if idem_replay_ok else 0.0,
                    "reasoning": f"same-key={replay_same.status_code}, no-key={dup_nokey.status_code}",
                }

                idem_conflict = client.post(
                    f"/v1/subjects/{fx.elder['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": idem_primary},
                    json={"reason": "wrong_reuse"},
                )
                checks["idempotency_conflict_diff_subject_409"] = {
                    "score": 1.0 if idem_conflict.status_code == 409 else 0.0,
                    "reasoning": f"cross-subject idempotency key reuse returned HTTP {idem_conflict.status_code}",
                }

                ro_post = client.post(f"/v1/subjects/{fx.elder['subject_id']}/withdrawal-requests", headers=h1_ro, json={})
                ro_get = client.get(f"/v1/withdrawal-requests/{req_pri_id}", headers=h1_ro)
                xs_post = client.post(f"/v1/subjects/{fx.elder['subject_id']}/withdrawal-requests", headers=h2, json={})
                xs_get = client.get(f"/v1/withdrawal-requests/{req_pri_id}", headers=h2)
                iso_ok = (ro_post.status_code == 403 and ro_get.status_code == 403 and xs_post.status_code == 403 and xs_get.status_code == 403)
                checks["cross_site_and_scope_isolation"] = {
                    "score": 1.0 if iso_ok else 0.0,
                    "reasoning": f"ro_post={ro_post.status_code}, ro_get={ro_get.status_code}, xs_post={xs_post.status_code}, xs_get={xs_get.status_code}",
                }

                hold_resp = client.post(f"/v1/subjects/{fx.hold['subject_id']}/withdrawal-requests", headers=h1, json={"reason": "attempt_on_hold"})
                hold_body = _json(hold_resp) if hold_resp.headers.get("content-type", "").startswith("application/json") else {}
                hold_get = client.get(f"/v1/subjects/{fx.hold['subject_id']}", headers=h1)
                hold_econsent = tmp_dir / "blobs" / "econsent" / fx.site1_id / f"{fx.hold['subject_id']}_signed.json"
                hold_ok = (
                    hold_resp.status_code == 409
                    and (hold_body.get("error") == "blocked_regulatory_hold" or hold_body.get("status") == "blocked_regulatory_hold")
                    and hold_get.status_code == 200
                    and _json(hold_get).get("full_name") == fx.hold["full_name"]
                    and _json(hold_get).get("status") == "active"
                    and hold_econsent.exists()
                )
                checks["regulatory_hold_blocks_409_no_mutation"] = {
                    "score": 1.0 if hold_ok else 0.0,
                    "reasoning": f"hold POST={hold_resp.status_code} body={hold_body}",
                }

                post_eld = client.post(
                    f"/v1/subjects/{fx.elder['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": f"idem-eld-{fx.trial_id}"},
                    json={"reason": "participant_request", "effective_date": eff_date},
                )
                req_eld_id = _json(post_eld).get("request_id") if post_eld.status_code == 202 else None
                anon_eld = _json(post_eld).get("anon_subject_ref") if post_eld.status_code == 202 else None

                fu_post = client.post(
                    f"/v1/subjects/{fx.fu['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": f"idem-fu-{fx.trial_id}"},
                    json={"reason": "stop_contact", "scope": "follow_up_only"},
                )
                fu_req_id = _json(fu_post).get("request_id") if fu_post.status_code == 202 else None

            with _Phase(checks, "phase b"):
                # ------------------------------------------------------------------
                # PHASE B: Worker convergence & full multi-store + DSMB inspection
                # ------------------------------------------------------------------
                w1 = _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:05:00+00:00")
                get_pri_after = client.get(f"/v1/withdrawal-requests/{req_pri_id}", headers=h1)
                get_eld_after = client.get(f"/v1/withdrawal-requests/{req_eld_id}", headers=h1) if req_eld_id else None
                pri_after_body = _json(get_pri_after) if get_pri_after.status_code == 200 else {}
                eld_after_body = _json(get_eld_after) if get_eld_after and get_eld_after.status_code == 200 else {}
                anon_pri = pri_after_body.get("anon_subject_ref") or anon_pri
                anon_eld = eld_after_body.get("anon_subject_ref") or anon_eld

                get_shape_ok = (
                    get_pri_after.status_code == 200
                    and {"request_id", "site_id", "status", "anon_subject_ref", "effective_date", "created_at", "completed_at", "error"}.issubset(
                        set(pri_after_body.keys())
                    )
                    and fx.primary["subject_id"] not in json.dumps(pri_after_body)
                    and fx.primary["mrn"] not in json.dumps(pri_after_body)
                )
                checks["get_status_contract"] = {
                    "score": 1.0 if get_shape_ok else 0.0,
                    "reasoning": f"GET status HTTP {get_pri_after.status_code} keys={sorted(pri_after_body.keys())}",
                }

                converged_ok = (
                    w1.returncode == 0
                    and pri_after_body.get("status") == "completed"
                    and bool(pri_after_body.get("completed_at"))
                    and pri_after_body.get("error") is None
                    and eld_after_body.get("status") == "completed"
                    and bool(eld_after_body.get("completed_at"))
                )
                checks["converges_to_completed_after_worker"] = {
                    "score": 1.0 if converged_ok else 0.0,
                    "reasoning": f"worker_rc={w1.returncode}, primary={pri_after_body.get('status')}, elder={eld_after_body.get('status')}",
                }

                # Post-withdrawal GET /v1/subjects/{orig} -> 404 and GET /v1/subjects/{anon} -> 200 de-identified
                orig_read = client.get(f"/v1/subjects/{fx.primary['subject_id']}", headers=h1)
                anon_read = client.get(f"/v1/subjects/{anon_pri}", headers=h1) if anon_pri else None
                anon_read_ok = (
                    orig_read.status_code == 404
                    and anon_read is not None
                    and anon_read.status_code == 200
                    and _json(anon_read).get("status") == "withdrawn"
                    and _json(anon_read).get("full_name") is None
                    and len(_json(anon_read).get("adverse_events", [])) == 1
                    and len(_json(anon_read).get("lab_results", [])) == 1
                )

                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                conn.row_factory = sqlite3.Row
                try:
                    old_pri_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (fx.primary["subject_id"],)).fetchone()
                    new_pri_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (anon_pri,)).fetchone() if anon_pri else None
                    old_eld_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (fx.elder["subject_id"],)).fetchone()
                    new_eld_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (anon_eld,)).fetchone() if anon_eld else None

                    direct_phi_cols = (
                        "mrn", "full_name", "email", "phone", "ssn", "street_address", "city",
                        "device_serial", "ip_address", "portal_url", "emergency_contact_name", "emergency_contact_phone",
                    )
                    direct_ok = (
                        old_pri_row is None
                        and old_eld_row is None
                        and new_pri_row is not None
                        and new_eld_row is not None
                        and new_pri_row["status"] == "withdrawn"
                        and new_eld_row["status"] == "withdrawn"
                        and all(new_pri_row[c] is None for c in direct_phi_cols)
                        and all(new_eld_row[c] is None for c in direct_phi_cols)
                        and anon_read_ok
                    )
                    pri_withdrawn = old_pri_row is None and new_pri_row is not None
                    checks["subjects_direct_phi_scrubbed"] = {
                        "score": 1.0 if direct_ok else 0.0,
                        "reasoning": f"old_gone={old_pri_row is None and old_eld_row is None}, anon_read_ok={anon_read_ok} (orig_http={orig_read.status_code})",
                    }

                    zip_ok = (
                        new_pri_row is not None
                        and new_eld_row is not None
                        and new_pri_row["postal_code"] == "021"
                        and new_pri_row["state"] == "MA"
                        and new_eld_row["postal_code"] == "000"
                        and new_eld_row["state"] == "NH"
                    )
                    checks["safe_harbor_zip3_and_restricted_zeroed"] = {
                        "score": 1.0 if zip_ok else 0.0,
                        "reasoning": f"pri_zip={new_pri_row['postal_code'] if new_pri_row else None}, eld_zip={new_eld_row['postal_code'] if new_eld_row else None}",
                    }

                    pri_visits = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    pri_aes = [dict(r) for r in conn.execute("SELECT * FROM adverse_events WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    pri_labs = [dict(r) for r in conn.execute("SELECT * FROM lab_results WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    pri_queries = [dict(r) for r in conn.execute("SELECT * FROM data_queries WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    pri_codings = [dict(r) for r in conn.execute("SELECT * FROM medical_coding_queue WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    eld_visits = [dict(r) for r in conn.execute("SELECT * FROM visits WHERE subject_id = ?", (anon_eld,)).fetchall()] if anon_eld else []
                    eld_labs = [dict(r) for r in conn.execute("SELECT * FROM lab_results WHERE subject_id = ?", (anon_eld,)).fetchall()] if anon_eld else []
                    eld_queries = [dict(r) for r in conn.execute("SELECT * FROM data_queries WHERE subject_id = ?", (anon_eld,)).fetchall()] if anon_eld else []
                    eld_codings = [dict(r) for r in conn.execute("SELECT * FROM medical_coding_queue WHERE subject_id = ?", (anon_eld,)).fetchall()] if anon_eld else []

                    age_dates_ok = (
                        new_pri_row is not None
                        and new_eld_row is not None
                        and new_pri_row["dob"] == "1972"
                        and new_pri_row["age_at_enrollment"] == 54
                        and new_pri_row["randomization_date"] is None
                        and new_eld_row["age_bucket"] == "90+"
                        and new_eld_row["age_at_enrollment"] is None
                        and new_eld_row["dob"] is None
                        and new_eld_row["randomization_date"] is None
                        and len(pri_visits) == 1
                        and pri_visits[0]["visit_date"] is None
                        and pri_visits[0]["days_from_randomization"] == 14
                        and len(pri_aes) == 1
                        and pri_aes[0]["onset_date"] is None
                        and pri_aes[0]["onset_study_day"] == 19
                        and len(pri_labs) == 1
                        and pri_labs[0]["collected_date"] is None
                        and pri_labs[0]["study_day"] == 14
                        and len(eld_visits) == 1
                        and eld_visits[0]["visit_date"] is None
                        and len(eld_labs) == 1
                        and eld_labs[0]["collected_date"] is None
                    )
                    checks["safe_harbor_age_over_89_and_dates"] = {
                        "score": 1.0 if age_dates_ok else 0.0,
                        "reasoning": f"pri(dob={new_pri_row['dob'] if new_pri_row else None}), eld(dob={new_eld_row['dob'] if new_eld_row else None}, age={new_eld_row['age_at_enrollment'] if new_eld_row else None}, bucket={new_eld_row['age_bucket'] if new_eld_row else None})",
                    }

                    # Free-text notes, raw_hl7_json, data_queries, medical_coding_queue, webhook_inbox, outbox_events
                    pri_dump = json.dumps({"visits": pri_visits, "aes": pri_aes, "labs": pri_labs, "queries": pri_queries, "codings": pri_codings})
                    eld_dump = json.dumps({"visits": eld_visits, "labs": eld_labs, "queries": eld_queries, "codings": eld_codings})
                    inbox_dump = json.dumps([dict(r) for r in conn.execute("SELECT subject_id, mrn, raw_payload_json FROM webhook_inbox").fetchall()])
                    outbox_dump = json.dumps([dict(r) for r in conn.execute("SELECT aggregate_id, payload_json FROM outbox_events").fetchall()])
                    leaks_text = _contains_any(pri_dump, pri_needles) + _contains_any(eld_dump, eld_needles)
                    leaks_inbox = _contains_any(inbox_dump, [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"], fx.elder["subject_id"], fx.elder["mrn"], fx.elder["full_name"]])
                    leaks_outbox = _contains_any(outbox_dump, [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"]])
                    hl7_keeps_obx = len(pri_labs) == 1 and "ALT" in pri_labs[0]["raw_hl7_json"] and "44" in pri_labs[0]["raw_hl7_json"]
                    coding_kept = len(pri_codings) == 1 and pri_codings[0]["coded_meddra_pt"] == "Neutropenia" and len(pri_queries) == 1 and pri_queries[0]["status"] == "resolved"
                    freetext_ok = (
                        len(pri_visits) == 1 and len(pri_aes) == 1 and len(pri_labs) == 1
                        and not leaks_text and not leaks_inbox and not leaks_outbox
                        and hl7_keeps_obx and coding_kept
                    )
                    checks["freetext_notes_and_hl7_redacted"] = {
                        "score": 1.0 if freetext_ok else 0.0,
                        "reasoning": f"leaks_text={leaks_text[:3]}, leaks_inbox={leaks_inbox[:3]}, leaks_outbox={leaks_outbox[:3]}, coding_kept={coding_kept}",
                    }

                    retained_ok = (
                        new_pri_row is not None and new_pri_row["treatment_arm"] == "DRUG_A_100MG"
                        and len(pri_visits) == 1 and pri_visits[0]["systolic_bp"] == 126
                        and len(pri_aes) == 1 and pri_aes[0]["meddra_term"] == "Neutropenia" and pri_aes[0]["ctcae_grade"] == 3 and pri_aes[0]["serious"] == 1
                        and len(pri_labs) == 1 and abs(float(pri_labs[0]["value_num"]) - 44.0) < 1e-6
                        and new_eld_row is not None and new_eld_row["treatment_arm"] == "DRUG_A_100MG"
                        and len(eld_labs) == 1 and abs(float(eld_labs[0]["value_num"]) - 36.5) < 1e-6
                    )
                    checks["clinical_rows_retained_and_rekeyed"] = {
                        "score": 1.0 if retained_ok else 0.0,
                        "reasoning": f"pri(v={len(pri_visits)},ae={len(pri_aes)},l={len(pri_labs)}), eld(l={len(eld_labs)})",
                    }

                    # ePRO DB + spool + DLQ + scheduled_jobs future clock check
                    pri_notifs = [dict(r) for r in conn.execute("SELECT * FROM epro_notifications WHERE subject_id = ?", (anon_pri,)).fetchall()] if anon_pri else []
                    orig_notifs = conn.execute("SELECT COUNT(*) FROM epro_notifications WHERE subject_id = ?", (fx.primary["subject_id"],)).fetchone()[0]
                    notifs_clean = (
                        orig_notifs == 0
                        and all(n["status"] != "pending" for n in pri_notifs)
                        and all(n["recipient"] is None for n in pri_notifs)
                        and not _contains_any(json.dumps(pri_notifs), pri_needles)
                    )
                    sched_leaks = _contains_any(
                        json.dumps([dict(r) for r in conn.execute("SELECT subject_id, payload_json, status FROM scheduled_jobs").fetchall()]),
                        [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["email"], fx.elder["subject_id"], fx.elder["mrn"]],
                    )
                finally:
                    conn.close()

                aconn = sqlite3.connect(str(tmp_dir / "part11_audit.db"))
                try:
                    pii_left = aconn.execute(
                        "SELECT COUNT(*) FROM audit_pii_details WHERE subject_id IN (?, ?)",
                        (fx.primary["subject_id"], fx.elder["subject_id"]),
                    ).fetchone()[0]
                finally:
                    aconn.close()

                # Advance worker clock past scheduled day30 followup (2026-04-15) -> survivor job fires, withdrawn subject does NOT!
                _run_worker(workspace, tmp_dir, now_iso="2026-05-01T00:00:00+00:00")
                outbound_raw = (tmp_dir / "spool" / "outbound_queue.jsonl").read_text(encoding="utf-8")
                dlq_raw = (tmp_dir / "spool" / "dead_letters.jsonl").read_text(encoding="utf-8")
                spool_leaks = _contains_any(
                    outbound_raw + "\n" + dlq_raw,
                    [
                        fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"],
                        fx.elder["subject_id"], fx.elder["mrn"], fx.elder["full_name"], fx.elder["email"],
                    ],
                )
                survivor_day30_fired = fx.survivor["subject_id"] in outbound_raw and "day30_followup_diary" in outbound_raw and fx.survivor["subject_id"] in dlq_raw
                checks["epro_notifications_spool_and_dlq_sanitized"] = {
                    "score": 1.0 if (notifs_clean and not sched_leaks and not spool_leaks and survivor_day30_fired) else 0.0,
                    "reasoning": f"db_notifs={notifs_clean}, sched_leaks={sched_leaks[:3]}, spool_leaks={spool_leaks[:3]}, surv_day30={survivor_day30_fired}",
                }

                chain_ok, chain_msg, post_audit_rows = _verify_part11_chain(tmp_dir / "part11_audit.db")
                checks["part11_audit_chain_valid"] = {
                    "score": 1.0 if (chain_ok and len(post_audit_rows) >= pre_audit_count + 2) else 0.0,
                    "reasoning": f"chain_ok={chain_ok} ({chain_msg}), rows={pre_audit_count}->{len(post_audit_rows)}",
                }

                wd_events = [r for r in post_audit_rows if r["action"] == "CONSENT_WITHDRAWN"]
                wd_refs = {r["resource_ref"] for r in wd_events}
                audit_no_phi = not _contains_any(
                    json.dumps(post_audit_rows),
                    [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.elder["subject_id"], fx.elder["mrn"]],
                )
                checks["audit_pii_purged_and_withdrawal_event_logged"] = {
                    "score": 1.0 if (pii_left == 0 and anon_pri in wd_refs and anon_eld in wd_refs and audit_no_phi) else 0.0,
                    "reasoning": f"pii_left={pii_left}, wd_refs={sorted(wd_refs)}, audit_no_phi={audit_no_phi}",
                }

                # Store 2: FTS5 Search Index
                sconn = sqlite3.connect(str(tmp_dir / "search.db"))
                try:
                    fts_rows = sconn.execute("SELECT subject_id, content FROM subject_search_fts").fetchall()
                finally:
                    sconn.close()
                fts_leaks = _contains_any(
                    json.dumps(fts_rows),
                    [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"], fx.primary["ssn"],
                     fx.elder["subject_id"], fx.elder["mrn"], fx.elder["full_name"], fx.elder["email"], fx.elder["ssn"]],
                )
                api_search_pri = client.get(f"/v1/subjects/search?q={fx.primary['mrn']}", headers=h1)
                api_search_surv = client.get(f"/v1/subjects/search?q={fx.survivor['mrn']}", headers=h1)
                fts_ok = (
                    not fts_leaks
                    and api_search_pri.status_code == 200 and len(_json(api_search_pri).get("hits", [])) == 0
                    and api_search_surv.status_code == 200 and len(_json(api_search_surv).get("hits", [])) >= 1
                )
                checks["fts5_search_index_clean"] = {
                    "score": 1.0 if fts_ok else 0.0,
                    "reasoning": f"fts_leaks={fts_leaks[:3]}, surv_hits={len(_json(api_search_surv).get('hits', [])) if api_search_surv.status_code == 200 else 'err'}",
                }

                # Store 3: Projection Cache
                cache_raw = (tmp_dir / "cache.json").read_text(encoding="utf-8") if (tmp_dir / "cache.json").exists() else "{}"
                cache_leaks = _contains_any(
                    cache_raw,
                    [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"], fx.primary["ssn"],
                     fx.elder["subject_id"], fx.elder["mrn"], fx.elder["full_name"], fx.elder["email"]],
                )
                cache_surv_ok = fx.survivor["subject_id"] in cache_raw
                checks["projection_cache_clean"] = {
                    "score": 1.0 if (not cache_leaks and cache_surv_ok) else 0.0,
                    "reasoning": f"cache_leaks={cache_leaks[:3]}, survivor_cached={cache_surv_ok}",
                }

                # Store 4: Blobs (.json + .meta.json) AND DSMB interim export snapshot + SHA-256 manifest checksum!
                econsent_pri = tmp_dir / "blobs" / "econsent" / fx.site1_id / f"{fx.primary['subject_id']}_signed.json"
                econsent_pri_meta = tmp_dir / "blobs" / "econsent" / fx.site1_id / f"{fx.primary['subject_id']}_signed.json.meta.json"
                econsent_eld = tmp_dir / "blobs" / "econsent" / fx.site1_id / f"{fx.elder['subject_id']}_signed.json"
                lab_pri = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_primary}.json"
                lab_pri_meta = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_primary}.json.meta.json"
                lab_eld = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_elder}.json"
                lab_eld_meta = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_elder}.json.meta.json"

                blobs_exist_ok = (
                    not econsent_pri.exists() and not econsent_pri_meta.exists() and not econsent_eld.exists()
                    and lab_pri.exists() and lab_pri_meta.exists() and lab_eld.exists() and lab_eld_meta.exists()
                )
                blob_leaks: list[str] = []
                lab_values_ok = False
                if blobs_exist_ok:
                    lp = json.loads(lab_pri.read_text(encoding="utf-8"))
                    lpm = json.loads(lab_pri_meta.read_text(encoding="utf-8"))
                    le = json.loads(lab_eld.read_text(encoding="utf-8"))
                    lem = json.loads(lab_eld_meta.read_text(encoding="utf-8"))
                    blob_leaks = _contains_any(json.dumps(lp) + json.dumps(lpm), pri_needles) + _contains_any(json.dumps(le) + json.dumps(lem), eld_needles)
                    def _has_anon(doc: dict[str, Any], ref: str | None) -> bool:
                        return bool(ref) and (doc.get("anon_subject_ref") == ref or doc.get("subject_id") == ref)

                    lab_values_ok = (
                        _has_anon(lp, anon_pri) and _has_anon(lpm, anon_pri)
                        and len(lp.get("results", [])) == 1 and abs(float(lp["results"][0]["value_num"]) - 44.0) < 1e-6
                        and _has_anon(le, anon_eld) and _has_anon(lem, anon_eld)
                        and len(le.get("results", [])) == 1 and abs(float(le["results"][0]["value_num"]) - 36.5) < 1e-6
                    )

                snap_resp = client.get(f"/v1/trials/{fx.trial_id}/dsmb-snapshots/{snapshot_id}", headers=h1)
                snap_data = _json(snap_resp) if snap_resp.status_code == 200 else {}
                snap_payload_str = json.dumps(snap_data.get("payload", {}))
                snap_leaks = _contains_any(snap_payload_str, pri_needles + eld_needles)
                snap_subjects = snap_data.get("payload", {}).get("subjects", [])
                snap_pri = next((s for s in snap_subjects if s.get("subject_id") == anon_pri), None)
                snap_eld = next((s for s in snap_subjects if s.get("subject_id") == anon_eld), None)
                dsmb_ok = (
                    snap_resp.status_code == 200
                    and snap_data.get("checksum_valid") is True
                    and not snap_leaks
                    and snap_pri is not None and snap_pri.get("postal_code") == "021" and len(snap_pri.get("adverse_events", [])) == 1
                    and snap_eld is not None and snap_eld.get("postal_code") == "000" and snap_eld.get("age_bucket") == "90+" and snap_eld.get("dob") is None
                )
                checks["blobs_and_metadata_clean"] = {
                    "score": 1.0 if (blobs_exist_ok and not blob_leaks and lab_values_ok and dsmb_ok) else 0.0,
                    "reasoning": f"blobs_ok={blobs_exist_ok and not blob_leaks and lab_values_ok}, dsmb_ok={dsmb_ok}, snap_leaks={snap_leaks[:3]}, snap_sha={snap_data.get('checksum_valid')}",
                }

                alias_tokens = ["Nadz", fx.primary["aliases"][1].split()[-1], "N.K."]
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                conn.row_factory = sqlite3.Row
                try:
                    surv_alias_row = conn.execute("SELECT aliases_json FROM subjects WHERE subject_id = ?", (fx.survivor["subject_id"],)).fetchone()
                    surv_notes = " ".join(r["clinical_notes"] for r in conn.execute("SELECT clinical_notes FROM visits WHERE subject_id = ?", (fx.survivor["subject_id"],)))
                    pri_alias_row = conn.execute("SELECT aliases_json FROM subjects WHERE subject_id = ?", (anon_pri,)).fetchone() if anon_pri else None
                    outbox_all = json.dumps([r["payload_json"] for r in conn.execute("SELECT payload_json FROM outbox_events")])
                finally:
                    conn.close()
                pri_text = json.dumps(
                    [v["clinical_notes"] for v in pri_visits]
                    + [q["query_text"] for q in pri_queries]
                    + [q.get("response_text") for q in pri_queries]
                )
                alias_leaks = _contains_any(
                    pri_text + json.dumps(fts_rows) + cache_raw + outbox_all + (pri_alias_row["aliases_json"] or "" if pri_alias_row else ""),
                    alias_tokens,
                )
                s_alias = client.get("/v1/subjects/search", params={"q": "Nadz"}, headers=h1)
                s_surv_alias = client.get("/v1/subjects/search", params={"q": "Tomek"}, headers=h1)
                alias_ok = (
                    pri_alias_row is not None
                    and not alias_leaks
                    and len(pri_visits) == 1
                    and "Stable hemodynamics" in pri_visits[0]["clinical_notes"]
                    and s_alias.status_code == 200 and len(_json(s_alias).get("hits", [])) == 0
                    and any(h.get("subject_id") == fx.survivor["subject_id"] for h in _json(s_surv_alias).get("hits", []))
                    and "Tomek" in surv_notes and "T.K." in surv_notes
                    and surv_alias_row is not None and "Tomek" in surv_alias_row["aliases_json"]
                )
                checks["aliases_and_initials_redacted"] = {
                    "score": 1.0 if alias_ok else 0.0,
                    "reasoning": f"alias_leaks={alias_leaks}, notes_kept={'Stable hemodynamics' in pri_text}, survivor_alias_intact={'Tomek' in surv_notes}",
                }

                post_summary_resp = client.get(f"/v1/trials/{fx.trial_id}/safety-summary", headers=h1)
                post_summary = _json(post_summary_resp) if post_summary_resp.status_code == 200 else {}
                checks["safety_summary_arm_totals_preserved"] = {
                    "score": 1.0 if (pri_withdrawn and post_summary_resp.status_code == 200 and post_summary == pre_summary) else 0.0,
                    "reasoning": f"primary_withdrawn={pri_withdrawn}, matches_pre={post_summary == pre_summary}",
                }

                surv_resp = client.get(f"/v1/subjects/{fx.survivor['subject_id']}", headers=h1)
                surv_econsent = tmp_dir / "blobs" / "econsent" / fx.site1_id / f"{fx.survivor['subject_id']}_signed.json"
                surv_lab = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_survivor}.json"
                surv_ok = (
                    pri_withdrawn
                    and surv_resp.status_code == 200
                    and _json(surv_resp).get("full_name") == fx.survivor["full_name"]
                    and _json(surv_resp).get("street_address") == fx.survivor["street_address"]
                    and _json(surv_resp).get("postal_code") == fx.survivor["postal_code"]
                    and _json(surv_resp).get("emergency_contact_phone") == fx.survivor["emergency_contact_phone"]
                    and _json(surv_resp).get("status") == "active"
                    and surv_econsent.exists() and surv_lab.exists()
                )
                checks["co_enrolled_household_survivor_untouched"] = {
                    "score": 1.0 if surv_ok else 0.0,
                    "reasoning": f"primary_withdrawn={pri_withdrawn}, survivor_http={surv_resp.status_code}, intact={surv_ok}",
                }

                # Store 5: CDISC SDTM Warehouse (`cdisc_sdtm.db`)
                post_sdtm_resp = client.get(f"/v1/trials/{fx.trial_id}/sdtm-summary", headers=h1)
                post_sdtm = _json(post_sdtm_resp) if post_sdtm_resp.status_code == 200 else {}
                wconn = sqlite3.connect(str(tmp_dir / "cdisc_sdtm.db"))
                wconn.row_factory = sqlite3.Row
                try:
                    dm_rows = [dict(r) for r in wconn.execute("SELECT * FROM sdtm_dm").fetchall()]
                    sv_rows = [dict(r) for r in wconn.execute("SELECT * FROM sdtm_sv").fetchall()]
                    ae_sdtm_rows = [dict(r) for r in wconn.execute("SELECT * FROM sdtm_ae").fetchall()]
                    lb_sdtm_rows = [dict(r) for r in wconn.execute("SELECT * FROM sdtm_lb").fetchall()]
                    supp_rows = [dict(r) for r in wconn.execute("SELECT * FROM sdtm_suppqual").fetchall()]
                finally:
                    wconn.close()
                sdtm_dump = json.dumps([dm_rows, sv_rows, ae_sdtm_rows, lb_sdtm_rows, supp_rows])
                sdtm_leaks = _contains_any(sdtm_dump, pri_needles + eld_needles + alias_tokens)
                dm_pri = next((r for r in dm_rows if r.get("usubjid") == anon_pri), None)
                dm_eld = next((r for r in dm_rows if r.get("usubjid") == anon_eld), None)
                dm_surv = next((r for r in dm_rows if r.get("usubjid") == fx.survivor["subject_id"]), None)
                sv_pri = [r for r in sv_rows if r.get("usubjid") == anon_pri]
                sdtm_phase_b_ok = (
                    pri_withdrawn
                    and post_sdtm_resp.status_code == 200
                    and post_sdtm == pre_sdtm
                    and not sdtm_leaks
                    and dm_pri is not None and dm_pri.get("postal_code") == "021" and dm_pri.get("rfstdtc") is None and dm_pri.get("armcd") == "DRUG_A_100MG"
                    and dm_eld is not None and dm_eld.get("postal_code") == "000" and dm_eld.get("age") is None and dm_eld.get("age_bucket") == "90+" and dm_eld.get("brthdtc") is None
                    and dm_surv is not None and dm_surv.get("fullname") == fx.survivor["full_name"]
                    and len(sv_pri) == 1 and sv_pri[0].get("svstdtc") is None and "Stable hemodynamics" in (sv_pri[0].get("coval") or "")
                )
                checks["sdtm_warehouse_deidentified_and_retained"] = {
                    "score": 1.0 if sdtm_phase_b_ok else 0.0,
                    "reasoning": f"summary_match={post_sdtm == pre_sdtm}, leaks={sdtm_leaks[:3]}, dm_pri={dm_pri is not None}, dm_eld={dm_eld is not None}, dm_surv={dm_surv is not None}",
                }

                # Store 6: Site Monitoring Enrollment Log (signed SHA-256) & CRA SDV Worklist
                enr_resp = client.get(f"/v1/sites/{fx.site1_id}/enrollment-log", headers=h1)
                enr_data = _json(enr_resp) if enr_resp.status_code == 200 else {}
                enr_entries = enr_data.get("log", {}).get("entries", [])
                enr_pri = next((e for e in enr_entries if e.get("subject_id") == anon_pri or e.get("anon_subject_ref") == anon_pri), None)
                enr_eld = next((e for e in enr_entries if e.get("subject_id") == anon_eld or e.get("anon_subject_ref") == anon_eld), None)
                enr_surv = next((e for e in enr_entries if e.get("subject_id") == fx.survivor["subject_id"]), None)
                sdv_path = tmp_dir / "monitoring" / fx.site1_id / "cra_sdv_worklist.jsonl"
                sdv_raw = sdv_path.read_text(encoding="utf-8") if sdv_path.exists() else ""
                mon_leaks = _contains_any(json.dumps(enr_entries) + "\n" + sdv_raw, pri_needles + eld_needles + alias_tokens)
                mon_ok = (
                    pri_withdrawn
                    and enr_resp.status_code == 200
                    and enr_data.get("checksum_valid") is True
                    and not mon_leaks
                    and enr_pri is not None and enr_pri.get("postal_code") == "021" and enr_pri.get("randomization_date") is None
                    and enr_eld is not None and enr_eld.get("postal_code") == "000" and enr_eld.get("age_bucket") == "90+" and enr_eld.get("dob") is None
                    and enr_surv is not None and enr_surv.get("full_name") == fx.survivor["full_name"] and enr_surv.get("mrn") == fx.survivor["mrn"]
                    and "Stable hemodynamics" in sdv_raw and fx.survivor["subject_id"] in sdv_raw and fx.survivor["full_name"] in sdv_raw
                )
                checks["site_monitoring_logs_and_sdv_scoped"] = {
                    "score": 1.0 if mon_ok else 0.0,
                    "reasoning": f"enr_http={enr_resp.status_code}, sha_valid={enr_data.get('checksum_valid')}, leaks={mon_leaks[:3]}, surv_intact={enr_surv is not None}",
                }

            with _Phase(checks, "phase c"):
                # ------------------------------------------------------------------
                # PHASE C: FDA 2008 Pre-vs-Post Withdrawal Specimen Boundary + Replay Suppression
                # ------------------------------------------------------------------
                # 1) Post-withdrawal specimen (2026-03-15 > 2026-03-10) -> MUST be suppressed (202)
                late_post_acc = f"ACC-LATEPOST-{fx.acc_tag}"
                late_post_wh = client.post(
                    "/v1/webhooks/central-lab",
                    json={
                        "site_id": fx.site1_id,
                        "trial_id": fx.trial_id,
                        "mrn": fx.primary["mrn"],
                        "accession_no": late_post_acc,
                        "analyte": "ALT",
                        "value_num": 999.0,
                        "unit": "U/L",
                        "reference_flag": "CRITICAL",
                        "collected_date": "2026-03-15",
                        "study_day": 33,
                        "shipment_id": fx.shipment_id,
                        "patient_demographics": {"full_name": fx.primary["full_name"], "email": fx.primary["email"], "dob": fx.primary["dob"]},
                    },
                )
                late_post_blob = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{late_post_acc}.json"

                # 2) Pre-withdrawal specimen arriving late (2026-03-08 <= 2026-03-10) -> MUST ingest de-identified (201)!
                late_pre_acc = f"ACC-LATEPRE-{fx.acc_tag}"
                late_pre_wh = client.post(
                    "/v1/webhooks/central-lab",
                    json={
                        "site_id": fx.site1_id,
                        "trial_id": fx.trial_id,
                        "mrn": fx.primary["mrn"],
                        "accession_no": late_pre_acc,
                        "analyte": "ALT",
                        "value_num": 50.0,
                        "unit": "U/L",
                        "reference_flag": "HIGH",
                        "collected_date": "2026-03-08",
                        "study_day": 26,
                        "shipment_id": fx.shipment_id,
                        "patient_demographics": {"full_name": fx.primary["full_name"], "email": fx.primary["email"], "dob": fx.primary["dob"], "ssn": fx.primary["ssn"]},
                    },
                )
                late_pre_blob = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{late_pre_acc}.json"
                late_pre_meta = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{late_pre_acc}.json.meta.json"

                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                conn.row_factory = sqlite3.Row
                try:
                    post_cnt = conn.execute("SELECT COUNT(*) FROM lab_results WHERE accession_no = ?", (late_post_acc,)).fetchone()[0]
                    pre_row = conn.execute("SELECT * FROM lab_results WHERE accession_no = ?", (late_pre_acc,)).fetchone()
                    inbox_pre = conn.execute("SELECT subject_id, mrn, raw_payload_json FROM webhook_inbox WHERE accession_no = ?", (late_pre_acc,)).fetchone()
                finally:
                    conn.close()

                pre_blob_clean = (
                    late_pre_blob.exists() and late_pre_meta.exists()
                    and not _contains_any(late_pre_blob.read_text(encoding="utf-8") + late_pre_meta.read_text(encoding="utf-8"), pri_needles)
                    and "50.0" in late_pre_blob.read_text(encoding="utf-8")
                )
                sum_after_pre = _json(client.get(f"/v1/trials/{fx.trial_id}/safety-summary", headers=h1))
                fda_boundary_ok = (
                    late_post_wh.status_code == 202 and _json(late_post_wh).get("status") == "suppressed"
                    and not late_post_blob.exists() and post_cnt == 0
                    and late_pre_wh.status_code == 201 and _json(late_pre_wh).get("status") == "ingested_deidentified"
                    and _json(late_pre_wh).get("anon_subject_ref") == anon_pri
                    and pre_row is not None and pre_row["subject_id"] == anon_pri and pre_row["collected_date"] is None
                    and not _contains_any(pre_row["raw_hl7_json"], pri_needles)
                    and inbox_pre is not None and not _contains_any(json.dumps(dict(inbox_pre)), pri_needles)
                    and pre_blob_clean
                    and _alt_count(sum_after_pre) == _alt_count(pre_summary) + 1
                )
                checks["late_hl7_webhook_suppressed"] = {
                    "score": 1.0 if fda_boundary_ok else 0.0,
                    "reasoning": f"post_wh={late_post_wh.status_code}, pre_wh={late_pre_wh.status_code}/{late_pre_wh.text[:100]}, pre_blob_clean={pre_blob_clean}",
                }

                # Verify late pre-withdrawal lab also updated SDTM warehouse without leaking PHI or post-withdrawal lab
                sdtm_after_pre = _json(client.get(f"/v1/trials/{fx.trial_id}/sdtm-summary", headers=h1))
                wconn = sqlite3.connect(str(tmp_dir / "cdisc_sdtm.db"))
                wconn.row_factory = sqlite3.Row
                try:
                    sdtm_pre_lb = wconn.execute("SELECT * FROM sdtm_lb WHERE lbrefid = ?", (late_pre_acc,)).fetchone()
                    sdtm_post_cnt = wconn.execute("SELECT COUNT(*) FROM sdtm_lb WHERE lbrefid = ?", (late_post_acc,)).fetchone()[0]
                finally:
                    wconn.close()
                if checks["sdtm_warehouse_deidentified_and_retained"]["score"] == 1.0:
                    sdtm_late_ok = (
                        sdtm_after_pre.get("lb_count") == pre_sdtm.get("lb_count", -9) + 1
                        and sdtm_pre_lb is not None and sdtm_pre_lb["usubjid"] == anon_pri and sdtm_pre_lb["lbdtc"] is None
                        and sdtm_post_cnt == 0
                    )
                    if not sdtm_late_ok:
                        checks["sdtm_warehouse_deidentified_and_retained"] = {
                            "score": 0.0,
                            "reasoning": f"SDTM late-lab mismatch: lb_count={sdtm_after_pre.get('lb_count')} (expected {pre_sdtm.get('lb_count', -9) + 1}), pre_lb={dict(sdtm_pre_lb) if sdtm_pre_lb else None}, post_cnt={sdtm_post_cnt}",
                        }

                ship_path = tmp_dir / "blobs" / "labs" / fx.site1_id / "shipments" / f"{fx.shipment_id}.json"
                ship_meta_path = ship_path.parent / f"{ship_path.name}.meta.json"
                ship_doc = json.loads(ship_path.read_text(encoding="utf-8")) if ship_path.exists() else {}
                ship_meta = json.loads(ship_meta_path.read_text(encoding="utf-8")) if ship_meta_path.exists() else {}
                by_acc = {sp.get("accession_no"): sp for sp in ship_doc.get("specimens", []) if isinstance(sp, dict)}

                def _anon_entry(sp: dict[str, Any] | None) -> bool:
                    return sp is not None and bool(anon_pri) and anon_pri in (sp.get("anon_subject_ref"), sp.get("subject_id"))

                surv_sp = by_acc.get(fx.acc_survivor) or {}
                ship_leaks = _contains_any(json.dumps(ship_doc) + json.dumps(ship_meta), pri_needles)
                ship_ok = (
                    bool(ship_doc)
                    and not ship_leaks
                    and _anon_entry(by_acc.get(fx.acc_primary))
                    and _anon_entry(by_acc.get(late_pre_acc))
                    and late_post_acc not in by_acc
                    and surv_sp.get("subject_id") == fx.survivor["subject_id"]
                    and surv_sp.get("mrn") == fx.survivor["mrn"]
                    and surv_sp.get("patient_name") == fx.survivor["full_name"]
                    and surv_sp.get("collected_date") == "2026-02-26"
                    and fx.survivor["subject_id"] in ship_meta.get("subject_ids", [])
                    and anon_pri in ship_meta.get("subject_ids", [])
                )
                checks["shared_shipment_manifest_scoped"] = {
                    "score": 1.0 if ship_ok else 0.0,
                    "reasoning": f"leaks={ship_leaks[:3]}, accs={sorted(k for k in by_acc if k)}, survivor_entry={surv_sp}",
                }

                # Replayed outbox events + re-injected scheduled job
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                try:
                    conn.execute(
                        """
                        INSERT INTO outbox_events (event_id, event_type, aggregate_id, site_id, payload_json, status, created_at)
                        VALUES (?, 'subject.enrolled', ?, ?, ?, 'pending', '2026-03-20T00:00:00+00:00'),
                               (?, 'epro.scheduled', ?, ?, ?, 'pending', '2026-03-20T00:00:01+00:00')
                        """,
                        (
                            f"evt-replay-1-{fx.trial_id}", fx.primary["subject_id"], fx.site1_id,
                            json.dumps({"subject_id": fx.primary["subject_id"], "mrn": fx.primary["mrn"], "full_name": fx.primary["full_name"]}),
                            f"evt-replay-2-{fx.trial_id}", fx.primary["subject_id"], fx.site1_id,
                            json.dumps({"subject_id": fx.primary["subject_id"], "mrn": fx.primary["mrn"], "recipient": fx.primary["email"], "full_name": fx.primary["full_name"]}),
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO scheduled_jobs (job_id, job_type, subject_id, site_id, payload_json, run_after, status, created_at)
                        VALUES (?, 'epro.day30_followup', ?, ?, ?, '2026-05-02T00:00:00+00:00', 'scheduled', '2026-03-20T00:00:02+00:00')
                        """,
                        (
                            f"job-replay-{fx.trial_id}", fx.primary["subject_id"], fx.site1_id,
                            json.dumps({"subject_id": fx.primary["subject_id"], "mrn": fx.primary["mrn"], "recipient": fx.primary["email"], "full_name": fx.primary["full_name"]}),
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                w_replay = _run_worker(workspace, tmp_dir, now_iso="2026-05-03T00:00:00+00:00")

                replay_outbound = (tmp_dir / "spool" / "outbound_queue.jsonl").read_text(encoding="utf-8")
                replay_cache = (tmp_dir / "cache.json").read_text(encoding="utf-8")
                sconn = sqlite3.connect(str(tmp_dir / "search.db"))
                try:
                    replay_fts = json.dumps(sconn.execute("SELECT subject_id, content FROM subject_search_fts").fetchall())
                finally:
                    sconn.close()
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                try:
                    replay_db = json.dumps(
                        conn.execute("SELECT status, payload_json FROM outbox_events WHERE event_id LIKE 'evt-replay-%'").fetchall()
                        + conn.execute("SELECT status, payload_json FROM scheduled_jobs WHERE job_id LIKE 'job-replay-%'").fetchall()
                    )
                finally:
                    conn.close()
                replay_leaks = _contains_any(
                    replay_outbound + replay_cache + replay_fts + replay_db,
                    [fx.primary["subject_id"], fx.primary["mrn"], fx.primary["full_name"], fx.primary["email"]],
                )
                checks["replayed_outbox_event_suppressed"] = {
                    "score": 1.0 if (w_replay.returncode == 0 and not replay_leaks) else 0.0,
                    "reasoning": f"worker_rc={w_replay.returncode}, replay_leaks={replay_leaks[:4]}",
                }

                # Paired guard: active survivor webhook still ingests!
                surv_acc_new = f"ACC-SURV2-{fx.acc_tag}"
                expected_alt_before = _alt_count(sum_after_pre)
                active_wh = client.post(
                    "/v1/webhooks/central-lab",
                    json={
                        "site_id": fx.site1_id,
                        "trial_id": fx.trial_id,
                        "subject_id": fx.survivor["subject_id"],
                        "mrn": fx.survivor["mrn"],
                        "accession_no": surv_acc_new,
                        "analyte": "ALT",
                        "value_num": 32.0,
                        "unit": "U/L",
                        "reference_flag": "NORMAL",
                        "collected_date": "2026-03-16",
                        "study_day": 32,
                    },
                )
                active_blob = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{surv_acc_new}.json"
                sum_after_active = _json(client.get(f"/v1/trials/{fx.trial_id}/safety-summary", headers=h1))
                checks["active_subject_webhook_still_ingests"] = {
                    "score": 1.0 if (pri_withdrawn and active_wh.status_code == 201 and active_blob.exists() and _alt_count(sum_after_active) == expected_alt_before + 1) else 0.0,
                    "reasoning": f"primary_withdrawn={pri_withdrawn}, active_wh={active_wh.status_code}, blob={active_blob.exists()}",
                }

            with _Phase(checks, "phase d"):
                # ------------------------------------------------------------------
                # PHASE D: Transient Store Fault Injection & Crash Recovery
                # ------------------------------------------------------------------
                post_flt = client.post(
                    f"/v1/subjects/{fx.fault['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": f"idem-flt-{fx.trial_id}"},
                    json={"reason": "fault_recovery_test", "effective_date": eff_date},
                )
                req_flt_id = _json(post_flt).get("request_id") if post_flt.status_code == 202 else None
                anon_flt = _json(post_flt).get("anon_subject_ref") if post_flt.status_code == 202 else None
                flt_blob_path = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_fault}.json"
                flt_backup_path = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_fault}.json.bak"

                if req_flt_id and flt_blob_path.exists():
                    flt_blob_path.rename(flt_backup_path)
                    flt_blob_path.mkdir()
                    try:
                        _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:10:00+00:00")
                        get_during_fault = client.get(f"/v1/withdrawal-requests/{req_flt_id}", headers=h1)
                        body_during_fault = _json(get_during_fault) if get_during_fault.status_code == 200 else {}
                        fault_blocked_ok = (
                            get_during_fault.status_code == 200
                            and body_during_fault.get("status") != "completed"
                            and body_during_fault.get("completed_at") is None
                        )
                        checks["transient_fault_prevents_premature_completion"] = {
                            "score": 1.0 if fault_blocked_ok else 0.0,
                            "reasoning": f"during fault: status={body_during_fault.get('status')}, error={body_during_fault.get('error')}",
                        }
                    finally:
                        shutil.rmtree(flt_blob_path, ignore_errors=True)
                        if flt_backup_path.exists():
                            flt_backup_path.rename(flt_blob_path)

                    w_rec = _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:15:00+00:00")
                    get_after_rec = client.get(f"/v1/withdrawal-requests/{req_flt_id}", headers=h1)
                    body_after_rec = _json(get_after_rec) if get_after_rec.status_code == 200 else {}
                    anon_flt = body_after_rec.get("anon_subject_ref") or anon_flt
                    flt_blob_text = flt_blob_path.read_text(encoding="utf-8") if flt_blob_path.exists() else ""
                    flt_meta_path = tmp_dir / "blobs" / "labs" / fx.site1_id / f"{fx.acc_fault}.json.meta.json"
                    flt_meta_text = flt_meta_path.read_text(encoding="utf-8") if flt_meta_path.exists() else ""
                    conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                    conn.row_factory = sqlite3.Row
                    try:
                        flt_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (anon_flt,)).fetchone() if anon_flt else None
                    finally:
                        conn.close()
                    rec_ok = (
                        fault_blocked_ok
                        and w_rec.returncode == 0
                        and body_after_rec.get("status") == "completed"
                        and bool(body_after_rec.get("completed_at"))
                        and body_after_rec.get("error") is None
                        and flt_row is not None and flt_row["postal_code"] == "000"
                        and not _contains_any(flt_blob_text + flt_meta_text, [fx.fault["subject_id"], fx.fault["mrn"], fx.fault["full_name"]])
                        and "78.0" in flt_blob_text
                    )
                    checks["retry_after_fault_converges_cleanly"] = {
                        "score": 1.0 if rec_ok else 0.0,
                        "reasoning": f"post-recovery: status={body_after_rec.get('status')}, zip={flt_row['postal_code'] if flt_row else None}",
                    }

                # Late-stage cross-DB fault: part11_audit.db fails AFTER downstream stores commit!
                aflt = fx.audit_fault
                post_aflt = client.post(
                    f"/v1/subjects/{aflt['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": f"idem-aflt-{fx.trial_id}"},
                    json={"reason": "cross_db_audit_fault_test", "effective_date": eff_date},
                )
                req_aflt_id = _json(post_aflt).get("request_id") if post_aflt.status_code == 202 else None
                anon_aflt = _json(post_aflt).get("anon_subject_ref") if post_aflt.status_code == 202 else None
                if req_aflt_id:
                    aconn = sqlite3.connect(str(tmp_dir / "part11_audit.db"))
                    try:
                        aconn.execute(
                            """
                            CREATE TRIGGER tv_simulate_audit_outage
                            BEFORE INSERT ON audit_log
                            BEGIN
                                SELECT RAISE(ABORT, 'simulated_part11_ledger_outage');
                            END;
                            """
                        )
                        aconn.commit()
                    finally:
                        aconn.close()

                    try:
                        _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:20:00+00:00")
                        during_aflt = _json(client.get(f"/v1/withdrawal-requests/{req_aflt_id}", headers=h1))
                        aflt_blocked = during_aflt.get("status") != "completed" and during_aflt.get("completed_at") is None
                    finally:
                        aconn = sqlite3.connect(str(tmp_dir / "part11_audit.db"))
                        try:
                            aconn.execute("DROP TRIGGER IF EXISTS tv_simulate_audit_outage")
                            aconn.commit()
                        finally:
                            aconn.close()

                    w_aflt_rec = _run_worker(workspace, tmp_dir, now_iso="2026-03-10T12:25:00+00:00")
                    after_aflt = _json(client.get(f"/v1/withdrawal-requests/{req_aflt_id}", headers=h1))
                    anon_aflt = after_aflt.get("anon_subject_ref") or anon_aflt
                    aflt_orig_get = client.get(f"/v1/subjects/{aflt['subject_id']}", headers=h1)
                    aflt_anon_get = client.get(f"/v1/subjects/{anon_aflt}", headers=h1) if anon_aflt else None
                    aflt_chain_ok, _, aflt_audit_rows = _verify_part11_chain(tmp_dir / "part11_audit.db")
                    aflt_wd_cnt = sum(1 for r in aflt_audit_rows if r["action"] == "CONSENT_WITHDRAWN" and r["resource_ref"] == anon_aflt)

                    wconn = sqlite3.connect(str(tmp_dir / "cdisc_sdtm.db"))
                    try:
                        aflt_sdtm_dump = json.dumps(
                            wconn.execute("SELECT * FROM sdtm_dm WHERE usubjid IN (?, ?)", (aflt["subject_id"], anon_aflt)).fetchall()
                            + wconn.execute("SELECT * FROM sdtm_sv WHERE usubjid IN (?, ?)", (aflt["subject_id"], anon_aflt)).fetchall()
                            + wconn.execute("SELECT * FROM sdtm_suppqual WHERE usubjid IN (?, ?)", (aflt["subject_id"], anon_aflt)).fetchall()
                        )
                    finally:
                        wconn.close()
                    enr_after_aflt = _json(client.get(f"/v1/sites/{fx.site1_id}/enrollment-log", headers=h1))
                    aflt_needles = [aflt["subject_id"], aflt["mrn"], aflt["full_name"], aflt["email"], "Veruschka"]
                    aflt_leaks = _contains_any(
                        json.dumps(_json(aflt_anon_get)) + aflt_sdtm_dump + json.dumps(enr_after_aflt.get("log", {})),
                        aflt_needles,
                    )
                    aflt_ok = (
                        aflt_blocked
                        and w_aflt_rec.returncode == 0
                        and after_aflt.get("status") == "completed"
                        and bool(after_aflt.get("completed_at"))
                        and aflt_orig_get.status_code == 404
                        and aflt_anon_get is not None and aflt_anon_get.status_code == 200
                        and _json(aflt_anon_get).get("postal_code") == "000"
                        and aflt_chain_ok and aflt_wd_cnt == 1
                        and enr_after_aflt.get("checksum_valid") is True
                        and not aflt_leaks
                    )
                    checks["late_stage_audit_fault_recovery"] = {
                        "score": 1.0 if aflt_ok else 0.0,
                        "reasoning": (
                            f"blocked_during_fault={aflt_blocked}, after_status={after_aflt.get('status')}, "
                            f"orig_http={aflt_orig_get.status_code}, wd_cnt={aflt_wd_cnt}, leaks={aflt_leaks[:3]}"
                        ),
                    }

            with _Phase(checks, "phase e"):
                # ------------------------------------------------------------------
                # PHASE E: Post-Withdrawal Baseline Health
                # ------------------------------------------------------------------
                new_subj_id = f"SUBJ-POST-{fx.trial_id}"
                post_enroll = client.post(
                    "/v1/subjects",
                    headers=h1,
                    json={
                        "subject_id": new_subj_id, "mrn": f"MRN-POST-{fx.trial_id}",
                        "full_name": f"Iris Chen{fx.trial_id}", "email": f"iris.{fx.trial_id.lower()}@clinic-test.example",
                        "phone": "+1-617-555-8811", "ssn": "999-11-2233", "dob": "1988-07-07", "age_at_enrollment": 38,
                        "street_address": "10 Longwood Ave", "city": "Boston", "state": "MA", "postal_code": "02115",
                        "device_serial": f"ECG-POST-{fx.trial_id}", "ip_address": "198.51.100.88",
                        "portal_url": f"https://portal.trialvault.example/s/{fx.trial_id.lower()}-post",
                        "emergency_contact_name": "Wei Chen", "emergency_contact_phone": "+1-617-555-8812",
                        "treatment_arm": "DRUG_A_100MG", "randomization_date": "2026-03-25",
                    },
                )
                _run_worker(workspace, tmp_dir, now_iso="2026-03-25T12:00:00+00:00")
                post_search = client.get("/v1/subjects/search?q=Iris", headers=h1)
                final_chain_ok, _, _ = _verify_part11_chain(tmp_dir / "part11_audit.db")
                baseline_ok = (
                    converged_ok
                    and post_enroll.status_code == 201
                    and post_search.status_code == 200
                    and any(h.get("subject_id") == new_subj_id for h in _json(post_search).get("hits", []))
                    and final_chain_ok
                )
                checks["existing_edc_workflows_preserved_post_withdrawal"] = {
                    "score": 1.0 if baseline_ok else 0.0,
                    "reasoning": f"converged={converged_ok}, post_enroll={post_enroll.status_code}, final_chain={final_chain_ok}",
                }

            with _Phase(checks, "phase f"):
                # ------------------------------------------------------------------
                # PHASE F: follow_up_only scope stops contact, keeps identifiable data
                # ------------------------------------------------------------------
                fu = fx.fu
                fu_body = _json(fu_post)
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                try:
                    conn.execute(
                        """
                        INSERT INTO outbox_events (event_id, event_type, aggregate_id, site_id, payload_json, status, created_at)
                        VALUES (?, 'epro.scheduled', ?, ?, ?, 'pending', '2026-03-26T00:00:00+00:00')
                        """,
                        (
                            f"evt-replay-fu-{fx.trial_id}", fu["subject_id"], fx.site1_id,
                            json.dumps({"subject_id": fu["subject_id"], "mrn": fu["mrn"], "recipient": fu["email"], "full_name": fu["full_name"]}),
                        ),
                    )
                    conn.commit()
                finally:
                    conn.close()
                fu_epro = client.post(f"/v1/subjects/{fu['subject_id']}/epro-reminders", headers=h1, json={"status": "pending"})
                fu_lab_acc = f"ACC-FU-{fx.acc_tag}"
                fu_lab = client.post(
                    "/v1/webhooks/central-lab",
                    json={
                        "site_id": fx.site1_id, "trial_id": fx.trial_id, "subject_id": fu["subject_id"], "mrn": fu["mrn"],
                        "accession_no": fu_lab_acc, "analyte": "AST", "value_num": 22.0, "unit": "U/L",
                        "reference_flag": "NORMAL", "collected_date": "2026-03-20", "study_day": 25,
                    },
                )
                w_fu = _run_worker(workspace, tmp_dir, now_iso="2026-06-01T00:00:00+00:00")
                fu_get = client.get(f"/v1/withdrawal-requests/{fu_req_id}", headers=h1)
                fu_get_body = _json(fu_get)
                fu_subj = client.get(f"/v1/subjects/{fu['subject_id']}", headers=h1)
                fu_subj_body = _json(fu_subj)
                fu_search = client.get("/v1/subjects/search", params={"q": fu["mrn"]}, headers=h1)
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                try:
                    fu_pending = conn.execute(
                        "SELECT COUNT(*) FROM epro_notifications WHERE subject_id = ? AND status = 'pending'", (fu["subject_id"],)
                    ).fetchone()[0]
                    fu_live_jobs = conn.execute(
                        "SELECT COUNT(*) FROM scheduled_jobs WHERE subject_id = ? AND job_type LIKE 'epro.%' AND status IN ('scheduled', 'executed')",
                        (fu["subject_id"],),
                    ).fetchone()[0]
                    fu_lab_row = conn.execute("SELECT subject_id FROM lab_results WHERE accession_no = ?", (fu_lab_acc,)).fetchone()
                finally:
                    conn.close()
                fu_spool = (tmp_dir / "spool" / "outbound_queue.jsonl").read_text(encoding="utf-8") + (
                    tmp_dir / "spool" / "dead_letters.jsonl"
                ).read_text(encoding="utf-8")
                fu_contact_leaks = _contains_any(fu_spool, [fu["subject_id"], fu["email"], fu["mrn"]])
                fu_ok = (
                    fu_post is not None and fu_post.status_code == 202
                    and fu_body.get("status") == "pending" and fu_body.get("scope") == "follow_up_only"
                    and "anon_subject_ref" in fu_body and fu_body.get("anon_subject_ref") is None
                    and w_fu.returncode == 0
                    and fu_get_body.get("status") == "completed" and bool(fu_get_body.get("completed_at"))
                    and fu_subj.status_code == 200
                    and fu_subj_body.get("full_name") == fu["full_name"]
                    and fu_subj_body.get("email") == fu["email"]
                    and fu_subj_body.get("status") == "follow_up_withdrawn"
                    and fu_pending == 0 and fu_live_jobs == 0
                    and not fu_contact_leaks
                    and fu_epro.status_code == 409 and _json(fu_epro).get("error") == "contact_withdrawn"
                    and fu_lab.status_code == 201 and _json(fu_lab).get("status") == "ingested"
                    and fu_lab_row is not None and fu_lab_row[0] == fu["subject_id"]
                    and fu_search.status_code == 200 and len(_json(fu_search).get("hits", [])) >= 1
                )
                checks["follow_up_only_stops_contact_keeps_data"] = {
                    "score": 1.0 if fu_ok else 0.0,
                    "reasoning": (
                        f"post={fu_post.status_code if fu_post is not None else None}/{str(fu_body)[:120]}, req={fu_get_body.get('status')}, "
                        f"subject={fu_subj.status_code}/{fu_subj_body.get('status')}, pending={fu_pending}, live_jobs={fu_live_jobs}, "
                        f"spool_leaks={fu_contact_leaks[:2]}, epro={fu_epro.status_code}, lab={fu_lab.status_code}"
                    ),
                }

            with _Phase(checks, "phase g"):
                # ------------------------------------------------------------------
                # PHASE G: escalation from follow_up_only to full withdrawal
                # ------------------------------------------------------------------
                fu = fx.fu
                fu_path = f"/v1/subjects/{fu['subject_id']}/withdrawal-requests"
                esc = client.post(fu_path, headers=h1, json={"reason": "full_withdrawal", "scope": "full", "effective_date": "2026-03-21"})
                esc_body = _json(esc)
                esc_id = esc_body.get("request_id")
                anon_fu = esc_body.get("anon_subject_ref")
                again_fu = client.post(fu_path, headers=h1, json={"scope": "follow_up_only"})
                again_default = client.post(fu_path, headers=h1, json={})
                w_esc = _run_worker(workspace, tmp_dir, now_iso="2026-06-02T00:00:00+00:00")
                esc_get = client.get(f"/v1/withdrawal-requests/{esc_id}", headers=h1)
                old_fu_get = client.get(f"/v1/withdrawal-requests/{fu_req_id}", headers=h1)
                orig_fu_read = client.get(f"/v1/subjects/{fu['subject_id']}", headers=h1)
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                conn.row_factory = sqlite3.Row
                try:
                    anon_fu_row = conn.execute("SELECT * FROM subjects WHERE subject_id = ?", (anon_fu,)).fetchone() if anon_fu else None
                    fu_lab_after = conn.execute("SELECT subject_id, raw_hl7_json FROM lab_results WHERE accession_no = ?", (f"ACC-FU-{fx.acc_tag}",)).fetchone()
                finally:
                    conn.close()
                fu_needles = [fu["subject_id"], fu["mrn"], fu["full_name"], fu["email"], fu["phone"], fu["ssn"]]
                esc_ok = (
                    esc.status_code == 202
                    and isinstance(esc_id, str) and esc_id != fu_req_id
                    and esc_body.get("scope") == "full" and esc_body.get("status") == "pending"
                    and isinstance(anon_fu, str) and fu["subject_id"] not in anon_fu
                    and _json(again_fu).get("request_id") == esc_id
                    and _json(again_default).get("request_id") == esc_id
                    and w_esc.returncode == 0
                    and _json(esc_get).get("status") == "completed"
                    and _json(old_fu_get).get("status") == "completed"
                    and _json(old_fu_get).get("scope") == "follow_up_only"
                    and orig_fu_read.status_code == 404
                    and anon_fu_row is not None and anon_fu_row["full_name"] is None and anon_fu_row["status"] == "withdrawn"
                    and fu_lab_after is not None and fu_lab_after["subject_id"] == anon_fu
                    and not _contains_any(fu_lab_after["raw_hl7_json"], fu_needles)
                )
                checks["scope_escalation_to_full"] = {
                    "score": 1.0 if esc_ok else 0.0,
                    "reasoning": (
                        f"esc={esc.status_code}/{str(esc_body)[:120]}, again_fu={_json(again_fu).get('request_id')}, "
                        f"default={_json(again_default).get('request_id')}, esc_status={_json(esc_get).get('status')}, "
                        f"orig_read={orig_fu_read.status_code}, anon_row={anon_fu_row is not None}"
                    ),
                }

            with _Phase(checks, "phase h"):
                # ------------------------------------------------------------------
                # PHASE H: overlapping worker processes must apply a withdrawal exactly once
                # ------------------------------------------------------------------
                race = fx.race
                post_race = client.post(
                    f"/v1/subjects/{race['subject_id']}/withdrawal-requests",
                    headers={**h1, "Idempotency-Key": f"idem-race-{fx.trial_id}"},
                    json={"reason": "participant_request", "effective_date": eff_date},
                )
                race_body = _json(post_race)
                race_req = race_body.get("request_id")
                _, _, rows_before = _verify_part11_chain(tmp_dir / "part11_audit.db")
                wd_before = sum(1 for r in rows_before if r["action"] == "CONSENT_WITHDRAWN")
                race_runs = _run_workers_concurrently(workspace, tmp_dir, n=4, now_iso="2026-06-03T00:00:00+00:00")
                race_get = _json(client.get(f"/v1/withdrawal-requests/{race_req}", headers=h1))
                anon_race = race_get.get("anon_subject_ref") or race_body.get("anon_subject_ref")
                race_chain_ok, race_chain_msg, rows_after = _verify_part11_chain(tmp_dir / "part11_audit.db")
                wd_delta = sum(1 for r in rows_after if r["action"] == "CONSENT_WITHDRAWN") - wd_before
                conn = sqlite3.connect(str(tmp_dir / "edc.db"))
                try:
                    orig_left = conn.execute("SELECT COUNT(*) FROM subjects WHERE subject_id = ?", (race["subject_id"],)).fetchone()[0]
                    anon_rows = conn.execute("SELECT COUNT(*) FROM subjects WHERE subject_id = ?", (anon_race,)).fetchone()[0] if anon_race else 0
                    anon_labs = conn.execute("SELECT COUNT(*) FROM lab_results WHERE subject_id = ?", (anon_race,)).fetchone()[0] if anon_race else 0
                finally:
                    conn.close()
                race_ok = (
                    post_race.status_code == 202
                    and all(r.returncode == 0 for r in race_runs)
                    and race_get.get("status") == "completed" and bool(race_get.get("completed_at"))
                    and race_chain_ok and wd_delta == 1
                    and orig_left == 0 and anon_rows == 1 and anon_labs == 1
                )
                checks["concurrent_workers_exactly_once"] = {
                    "score": 1.0 if race_ok else 0.0,
                    "reasoning": (
                        f"post={post_race.status_code}, worker_rcs={[r.returncode for r in race_runs]}, status={race_get.get('status')}, "
                        f"chain={race_chain_ok} ({race_chain_msg}), consent_withdrawn_delta={wd_delta}, orig_left={orig_left}, anon_rows={anon_rows}"
                    ),
                }

        return checks
    except Exception as exc:
        err = f"harness error: {type(exc).__name__}: {exc}"
        for cid in ALL_CHECK_IDS:
            if checks[cid]["reasoning"] == "not reached":
                checks[cid] = {"score": 0.0, "reasoning": err}
        return checks
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        shutil.rmtree(tmp_dir, ignore_errors=True)


def evaluate_workspace(workspace: Path, runs: int = 3) -> dict[str, dict[str, Any]]:
    resolved = str(workspace.resolve())
    cache_key = (resolved, runs)
    with _EVAL_LOCK:
        if cache_key in _EVAL_CACHE:
            return _EVAL_CACHE[cache_key]

        per_run = [_run_single_trial(Path(resolved), idx) for idx in range(runs)]
        aggregated: dict[str, dict[str, Any]] = {}
        for cid in ALL_CHECK_IDS:
            scores = [r[cid]["score"] for r in per_run]
            min_score = min(scores)
            chosen = next((r[cid]["reasoning"] for r in per_run if r[cid]["score"] == min_score), "")
            aggregated[cid] = {
                "score": min_score,
                "reasoning": f"[min of {runs} fresh runs: {scores}] {chosen}",
            }

        _EVAL_CACHE[cache_key] = aggregated
        return aggregated
