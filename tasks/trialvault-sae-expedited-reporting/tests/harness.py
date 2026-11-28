"""Black-box, fresh-seeded, 3-run TrialVault PV expedited SAE & SUSAR verifier harness.

Spawns the submitted FastAPI application (`uvicorn trialvault_pv.api:app`) and worker CLI
(`python -m trialvault_pv.worker --once`) as isolated subprocesses against a fresh
temporary TRIALVAULT_PV_DATA_DIR with randomized identifiers on each run, probing the
public HTTP contract and starter-wired backing stores.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx

ALL_CHECK_IDS: tuple[str, ...] = (
    # 1. e2b_api_and_idempotency (14.0%, 5 checks)
    "e2b_post_returns_202_received",
    "auth_and_tenant_site_enforcement",
    "idempotency_key_replay_and_conflict",
    "followup_duplicate_vs_conflict_409",
    "out_of_order_followup_monotonic_projection",
    # 2. susar_triage_and_rsi (16.0%, 5 checks)
    "three_legged_susar_positive_triage",
    "or_causality_sponsor_overrides_unrelated_investigator",
    "rsi_expected_term_and_grade_not_susar",
    "rsi_severity_exceeded_marks_unexpected",
    "rsi_fatal_or_lt_on_nonfatal_term_unexpected",
    # 3. selective_unblinding_and_iwrs (12.0%, 3 checks)
    "non_susar_cases_never_query_iwrs",
    "placebo_susar_candidate_closed_without_fda_submission",
    "single_iwrs_unblind_audit_across_followups_and_retries",
    # 4. blinding_wall_and_projections (16.0%, 4 checks)
    "blinded_case_endpoint_zero_arm_or_susar_leak",
    "blinded_sqlite_fts_and_cache_zero_leak",
    "site_blinded_line_listing_and_checksum",
    "dsmb_unblinded_susar_listing_and_checksum",
    # 5. day0_clock_and_tightening (16.0%, 4 checks)
    "fifteen_day_and_seven_day_initial_clocks",
    "delayed_susar_qualification_sets_threshold_day0",
    "pre_submission_15d_to_7d_retroactive_tightening",
    "post_submission_followup_vs_benign_amendment",
    # 6. fda_gateway_and_fault_recovery (11.0%, 4 checks)
    "fda_gateway_payload_meta_and_ack_log",
    "gateway_nack_marks_retry_pending_not_submitted",
    "filesystem_fault_blocks_false_submission_and_recovers",
    "audit_db_abort_trigger_prevents_split_brain_and_recovers",
    # 7. part11_audit_and_concurrency (10.0%, 3 checks)
    "part11_hash_chain_intact_across_lifecycle",
    "ind_safety_submitted_logged_only_on_ack",
    "concurrent_workers_single_submission_and_unblind",
    # 8. baseline_pv_preservation (5.0%, 2 checks)
    "existing_subjects_and_blinded_summary_intact",
    "post_sae_enrollment_search_and_health_workflows",
)

_EVAL_LOCK = threading.Lock()
_EVAL_CACHE: dict[tuple[str, int], dict[str, dict[str, Any]]] = {}


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts or not isinstance(ts, str):
        return None
    cleaned = ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(cleaned)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _ts_eq(actual: str | None, expected: str) -> bool:
    da = _parse_iso(actual)
    de = _parse_iso(expected)
    return da is not None and de is not None and da == de


def _json(resp: httpx.Response | None) -> dict[str, Any]:
    if resp is None:
        return {}
    try:
        body = resp.json()
    except ValueError:
        return {}
    return body if isinstance(body, dict) else {}


def _extract_submissions(resp: httpx.Response | None) -> list[dict[str, Any]]:
    if resp is None or resp.status_code != 200:
        return []
    try:
        body = resp.json()
    except ValueError:
        return []
    if isinstance(body, dict) and isinstance(body.get("submissions"), list):
        return [x for x in body["submissions"] if isinstance(x, dict)]
    if isinstance(body, list):
        return [x for x in body if isinstance(x, dict)]
    return []


def _contains_any(text: str, needles: list[str]) -> list[str]:
    lower = text.lower()
    return [n for n in needles if n and n.lower() in lower]


class _Phase:
    """Isolates a harness phase: an exception zeroes only checks not yet scored."""

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


def _verify_part11_chain(audit_db: Path) -> tuple[bool, str, list[dict[str, Any]]]:
    if not audit_db.exists():
        return False, "part11_pv_audit.db missing", []
    conn = sqlite3.connect(str(audit_db))
    conn.row_factory = sqlite3.Row
    try:
        rows = [
            dict(r)
            for r in conn.execute(
                """
                SELECT seq_id, event_time, actor, action, resource_type, resource_ref,
                       payload_json, payload_hash, prev_hash, entry_hash
                FROM audit_log
                ORDER BY seq_id ASC
                """
            ).fetchall()
        ]
    finally:
        conn.close()

    if not rows:
        return False, "audit_log empty", rows

    expected_prev = "0" * 64
    prev_seq = 0
    for row in rows:
        if int(row["seq_id"]) <= prev_seq:
            return False, f"non-monotonic seq_id={row['seq_id']}", rows
        if row["prev_hash"] != expected_prev:
            return False, f"broken prev_hash at seq_id={row['seq_id']}", rows
        actual_payload_hash = _sha256_hex(str(row["payload_json"]))
        if actual_payload_hash != row["payload_hash"]:
            return False, f"payload_hash mismatch at seq_id={row['seq_id']}", rows
        canonical = "|".join(
            [
                str(row["event_time"]),
                str(row["actor"]),
                str(row["action"]),
                str(row["resource_type"]),
                str(row["resource_ref"]),
                str(row["payload_hash"]),
                str(row["prev_hash"]),
            ]
        )
        computed = _sha256_hex(canonical)
        if computed != row["entry_hash"]:
            return False, f"entry_hash mismatch at seq_id={row['seq_id']}", rows
        expected_prev = str(row["entry_hash"])
        prev_seq = int(row["seq_id"])

    return True, f"verified {len(rows)} entries", rows


@dataclass
class _Fixture:
    tag: str
    trial_id: str
    site1_id: str
    site2_id: str
    drug_arm: str
    placebo_arm: str
    tok_s1: str
    tok_s2: str
    tok_pv: str
    subj_drug: list[str]
    subj_placebo: str
    subj_site2: str
    baseline_case_uid: str


def _make_fixture(run_idx: int) -> _Fixture:
    tag = f"R{run_idx}{uuid.uuid4().hex[:5].upper()}"
    return _Fixture(
        tag=tag,
        trial_id=f"TRIAL-{tag}",
        site1_id=f"SITE1-{tag}",
        site2_id=f"SITE2-{tag}",
        drug_arm=f"DRUG_NVX_{tag}",
        placebo_arm="PLACEBO",
        tok_s1=f"tok-s1-safety-{tag}",
        tok_s2=f"tok-s2-safety-{tag}",
        tok_pv=f"tok-pv-unblind-{tag}",
        subj_drug=[f"SUBJ-D{i}-{tag}" for i in range(1, 9)],
        subj_placebo=f"SUBJ-PLB-{tag}",
        subj_site2=f"SUBJ-S2D-{tag}",
        baseline_case_uid=f"CASE-BASE-{tag}",
    )


def _start_server(workspace: Path, data_dir: Path, port: int) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_PV_DATA_DIR"] = str(data_dir)
    env["TRIALVAULT_PV_CLOCK_ISO"] = "2026-03-01T12:00:00+00:00"
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "trialvault_pv.api:app",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "error",
        ],
        cwd=str(workspace),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    deadline = time.monotonic() + 15.0
    health_url = f"http://127.0.0.1:{port}/health"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=2)
            raise RuntimeError(f"uvicorn exited early ({proc.returncode}): {err or out}")
        try:
            resp = httpx.get(health_url, timeout=1.0)
            if resp.status_code == 200:
                return proc
        except Exception:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("uvicorn healthcheck timed out")


def _run_worker(
    workspace: Path,
    data_dir: Path,
    now_iso: str | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_PV_DATA_DIR"] = str(data_dir)
    cmd = [sys.executable, "-m", "trialvault_pv.worker", "--once"]
    if now_iso:
        env["TRIALVAULT_PV_CLOCK_ISO"] = now_iso
        cmd.extend(["--now", now_iso])
    return subprocess.run(cmd, cwd=str(workspace), env=env, capture_output=True, text=True, timeout=30)


def _run_workers_concurrently(
    workspace: Path,
    data_dir: Path,
    n: int,
    now_iso: str,
) -> list[subprocess.CompletedProcess[str]]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(workspace)
    env["TRIALVAULT_PV_DATA_DIR"] = str(data_dir)
    env["TRIALVAULT_PV_CLOCK_ISO"] = now_iso
    barrier_dir = Path(tempfile.mkdtemp(prefix="tv_pv_barrier_"))
    try:
        deadline = time.time() + 10.0
        procs: list[subprocess.Popen[str]] = []
        for idx in range(n):
            ready_file = str(barrier_dir / f"w{idx}.ready")
            code = (
                "import os, sys, time\n"
                f"sys.argv = ['trialvault_pv.worker', '--once', '--now', {now_iso!r}]\n"
                "import trialvault_pv.worker as _worker_mod\n"
                f"open({ready_file!r}, 'w').close()\n"
                f"while len(os.listdir({str(barrier_dir)!r})) < {n} and time.time() < {deadline!r}:\n"
                "    pass\n"
                "if hasattr(_worker_mod, 'main'):\n"
                "    _worker_mod.main()\n"
                "else:\n"
                "    import runpy\n"
                "    runpy.run_module('trialvault_pv.worker', run_name='__main__')\n"
            )
            procs.append(
                subprocess.Popen(
                    [sys.executable, "-c", code],
                    cwd=str(workspace),
                    env=env,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
            )
        results: list[subprocess.CompletedProcess[str]] = []
        for p in procs:
            try:
                out, err = p.communicate(timeout=45)
            except subprocess.TimeoutExpired:
                p.kill()
                out, err = p.communicate()
            results.append(subprocess.CompletedProcess(p.args, p.returncode, out, err))
        return results
    finally:
        shutil.rmtree(barrier_dir, ignore_errors=True)


def _seed_trial_state(client: httpx.Client, workspace: Path, data_dir: Path, fx: _Fixture) -> None:
    edc_db = data_dir / "clinical_edc.db"
    rsi_db = data_dir / "protocol_rsi.db"
    audit_db = data_dir / "part11_pv_audit.db"

    conn = sqlite3.connect(str(edc_db))
    try:
        conn.execute(
            """
            INSERT INTO trials (trial_id, protocol_code, investigational_product, sponsor_name, phase, active)
            VALUES (?, ?, ?, 'Apex Oncology', 'Phase III', 1)
            """,
            (fx.trial_id, f"PROT-{fx.tag}", fx.drug_arm),
        )
        conn.execute(
            """
            INSERT INTO sites (site_id, trial_id, name, country, active)
            VALUES (?, ?, 'Boston Clinical Site', 'US', 1),
                   (?, ?, 'Seattle Clinical Site', 'US', 1)
            """,
            (fx.site1_id, fx.trial_id, fx.site2_id, fx.trial_id),
        )
        conn.execute(
            """
            INSERT INTO api_tokens (token_hash, site_id, role_label, scopes, active)
            VALUES (?, ?, 'coord-site1', 'site:safety', 1),
                   (?, ?, 'coord-site2', 'site:safety', 1),
                   (?, NULL, 'sponsor-pv-officer', 'pv:unblinded', 1)
            """,
            (
                _sha256_hex(fx.tok_s1),
                fx.site1_id,
                _sha256_hex(fx.tok_s2),
                fx.site2_id,
                _sha256_hex(fx.tok_pv),
            ),
        )
        conn.commit()
    finally:
        conn.close()

    rconn = sqlite3.connect(str(rsi_db))
    try:
        ib_v1 = f"IB-{fx.tag}-v1"
        ib_v2_future = f"IB-{fx.tag}-v2"
        rconn.execute(
            """
            INSERT INTO ib_versions (ib_version, trial_id, effective_from, is_active)
            VALUES (?, ?, '2026-01-01T00:00:00+00:00', 1),
                   (?, ?, '2026-07-01T00:00:00+00:00', 1)
            """,
            (ib_v1, fx.trial_id, ib_v2_future, fx.trial_id),
        )
        rconn.execute(
            """
            INSERT INTO rsi_expected_terms (ib_version, meddra_pt, max_expected_grade, fatal_expected)
            VALUES (?, 'Nausea', 2, 0),
                   (?, 'Neutropenia', 3, 0),
                   (?, 'Infusion related reaction', 3, 0),
                   (?, 'Pneumonitis', 4, 1)
            """,
            (ib_v1, ib_v1, ib_v1, ib_v2_future),
        )
        rconn.commit()
    finally:
        rconn.close()

    h_pv = {"Authorization": f"Bearer {fx.tok_pv}"}
    for idx, sid in enumerate(fx.subj_drug):
        r = client.post(
            "/v1/subjects",
            headers=h_pv,
            json={
                "subject_id": sid,
                "trial_id": fx.trial_id,
                "site_id": fx.site1_id,
                "age": 45 + idx,
                "sex": "F" if idx % 2 == 0 else "M",
                "enrollment_date": "2026-02-01",
                "treatment_arm": fx.drug_arm,
                "is_investigational_drug": True,
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed enroll failed for {sid}: {r.status_code} {r.text}")

    for sid, site_id, arm, is_inv in (
        (fx.subj_placebo, fx.site1_id, fx.placebo_arm, False),
        (fx.subj_site2, fx.site2_id, fx.drug_arm, True),
    ):
        r = client.post(
            "/v1/subjects",
            headers=h_pv,
            json={
                "subject_id": sid,
                "trial_id": fx.trial_id,
                "site_id": site_id,
                "age": 59,
                "sex": "M",
                "enrollment_date": "2026-02-02",
                "treatment_arm": arm,
                "is_investigational_drug": is_inv,
            },
        )
        if r.status_code != 201:
            raise RuntimeError(f"seed enroll failed for {sid}: {r.status_code} {r.text}")

    # Seed one baseline non-serious AE case via starter table + case.logged outbox event
    now_seed = "2026-02-10T09:00:00+00:00"
    ser_zero = json.dumps(
        {
            "death": False,
            "life_threatening": False,
            "hospitalization": False,
            "disability": False,
            "congenital_anomaly": False,
            "medically_important": False,
        },
        sort_keys=True,
    )
    conn = sqlite3.connect(str(edc_db))
    try:
        conn.execute(
            """
            INSERT INTO safety_cases (
                case_uid, subject_id, trial_id, site_id, site_status, latest_followup_num,
                initial_aware_at, latest_aware_at, meddra_pt, ctcae_grade, is_serious,
                seriousness_json, investigator_causality, sponsor_causality, narrative,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, 'received', 0, ?, ?, 'Fatigue', 1, 0, ?, 'unrelated', NULL, ?, ?, ?)
            """,
            (
                fx.baseline_case_uid,
                fx.subj_drug[0],
                fx.trial_id,
                fx.site1_id,
                now_seed,
                now_seed,
                ser_zero,
                f"Baseline mild fatigue {fx.tag}",
                now_seed,
                now_seed,
            ),
        )
        conn.execute(
            """
            INSERT INTO case_reports (
                report_id, message_id, case_uid, subject_id, site_id, followup_num,
                aware_at, meddra_pt, ctcae_grade, is_serious, seriousness_json,
                investigator_causality, sponsor_causality, narrative, received_at, processed
            ) VALUES (?, ?, ?, ?, ?, 0, ?, 'Fatigue', 1, 0, ?, 'unrelated', NULL, ?, ?, 0)
            """,
            (
                f"rep-base-{fx.tag}",
                f"MSG-BASE-{fx.tag}",
                fx.baseline_case_uid,
                fx.subj_drug[0],
                fx.site1_id,
                now_seed,
                ser_zero,
                f"Baseline mild fatigue {fx.tag}",
                now_seed,
            ),
        )
        conn.execute(
            """
            INSERT INTO outbox_events (
                event_id, event_type, aggregate_id, site_id, payload_json, status, created_at
            ) VALUES (?, 'case.logged', ?, ?, ?, 'pending', ?)
            """,
            (
                f"evt-base-{fx.tag}",
                fx.baseline_case_uid,
                fx.site1_id,
                json.dumps({"case_uid": fx.baseline_case_uid, "site_id": fx.site1_id, "trial_id": fx.trial_id}),
                now_seed,
            ),
        )
        conn.commit()
    finally:
        conn.close()

    w0 = _run_worker(workspace, data_dir, now_iso="2026-02-10T09:05:00+00:00")
    if w0.returncode != 0:
        raise RuntimeError(f"seed worker failed: {w0.stderr}")

    ok_chain, msg_chain, _ = _verify_part11_chain(audit_db)
    if not ok_chain:
        raise RuntimeError(f"seed audit chain invalid: {msg_chain}")


def _iwrs_audit_counts(iwrs_db: Path) -> dict[str, int]:
    conn = sqlite3.connect(str(iwrs_db))
    try:
        rows = conn.execute(
            "SELECT case_uid, COUNT(*) AS cnt FROM iwrs_unblind_audit GROUP BY case_uid"
        ).fetchall()
    finally:
        conn.close()
    return {str(r[0]): int(r[1]) for r in rows}


def _run_single_trial(workspace: Path, run_idx: int) -> dict[str, dict[str, Any]]:
    checks: dict[str, dict[str, Any]] = {
        cid: {"score": 0.0, "reasoning": "not reached"} for cid in ALL_CHECK_IDS
    }
    tmp_dir = Path(tempfile.mkdtemp(prefix=f"tv_pv_r{run_idx}_"))
    port = _find_free_port()
    proc: subprocess.Popen[str] | None = None

    try:
        proc = _start_server(workspace, tmp_dir, port)
        fx = _make_fixture(run_idx)
        base_url = f"http://127.0.0.1:{port}"

        with httpx.Client(base_url=base_url, timeout=15.0) as client:
            _seed_trial_state(client, workspace, tmp_dir, fx)

            h_s1 = {"Authorization": f"Bearer {fx.tok_s1}"}
            h_s2 = {"Authorization": f"Bearer {fx.tok_s2}"}
            h_pv = {"Authorization": f"Bearer {fx.tok_pv}"}
            pre_blinded_summary = _json(client.get(f"/v1/trials/{fx.trial_id}/blinded-summary", headers=h_s1))

            case_15d = f"CASE-15D-{fx.tag}"
            case_ooo = f"CASE-OOO-{fx.tag}"
            case_7d = f"CASE-7D-{fx.tag}"
            case_or_caus = f"CASE-ORCAUS-{fx.tag}"
            case_both_unrel = f"CASE-UNREL-{fx.tag}"
            case_non_ser = f"CASE-NONSER-{fx.tag}"
            case_rsi_exp = f"CASE-RSIEXP-{fx.tag}"
            case_rsi_grd = f"CASE-RSIGRD-{fx.tag}"
            case_rsi_lt = f"CASE-RSILT-{fx.tag}"
            case_plb = f"CASE-PLB-{fx.tag}"
            case_s2 = f"CASE-S2-{fx.tag}"
            s2_unique_kw = f"EncephalopathyMarker{fx.tag}"

            susar_converged_ok = False
            expected_e2b_audit_count = 0

            # ==================================================================
            # PHASE A: E2B Intake API, Auth, Idempotency & Out-of-Order Merge
            # ==================================================================
            with _Phase(checks, "phase a"):
                idem_15d = f"idem-15d-{fx.tag}"
                msg_15d_0 = f"MSG-15D-0-{fx.tag}"
                t0_15d = "2026-03-01T10:00:00+00:00"
                # Note: 'Pneumonitis' is ONLY in future IB-v2 (2026-07-01), NOT in active IB-v1 (2026-01-01)!
                post_15d = client.post(
                    "/v1/safety-reports/e2b",
                    headers={**h_s1, "Idempotency-Key": idem_15d},
                    json={
                        "message_id": msg_15d_0,
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 0,
                        "aware_at": t0_15d,
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 3,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": f"Grade 3 hospitalized pneumonitis {fx.tag}.",
                    },
                )
                if post_15d.status_code == 202:
                    expected_e2b_audit_count += 1

                p15_body = _json(post_15d)
                blinded_pre = client.get(f"/v1/safety-cases/{case_15d}", headers=h_s1)
                pv_pre = client.get(f"/v1/pv/cases/{case_15d}", headers=h_pv)
                iwrs_pre = _iwrs_audit_counts(tmp_dir / "randomization_iwrs.db")
                _, _, audit_after_p15 = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                e2b_audits_p15 = [
                    r
                    for r in audit_after_p15
                    if r["action"] == "E2B_REPORT_RECEIVED"
                    and (
                        case_15d in str(r["resource_ref"])
                        or msg_15d_0 in str(r["resource_ref"])
                        or case_15d in str(r["payload_json"])
                        or msg_15d_0 in str(r["payload_json"])
                    )
                ]

                post_202_ok = (
                    post_15d.status_code == 202
                    and p15_body.get("case_uid") == case_15d
                    and p15_body.get("message_id") == msg_15d_0
                    and p15_body.get("followup_num") == 0
                    and p15_body.get("site_status") == "received"
                    and _ts_eq(p15_body.get("aware_at"), t0_15d)
                    and blinded_pre.status_code == 200
                    and _json(blinded_pre).get("site_status") == "received"
                    and pv_pre.status_code == 200
                    and _json(pv_pre).get("pv_status") == "pending_assessment"
                    and _json(pv_pre).get("unblinded") is False
                    and _json(pv_pre).get("treatment_arm") is None
                    and sum(iwrs_pre.values()) == 0
                    and len(e2b_audits_p15) == 1
                )
                checks["e2b_post_returns_202_received"] = {
                    "score": 1.0 if post_202_ok else 0.0,
                    "reasoning": (
                        f"post={post_15d.status_code} body={p15_body}, blinded_pre={blinded_pre.status_code}/"
                        f"{_json(blinded_pre).get('site_status')}, pv_pre={pv_pre.status_code}/"
                        f"{_json(pv_pre).get('pv_status')}, iwrs_pre={iwrs_pre}, e2b_audit={len(e2b_audits_p15)}"
                    ),
                }

                # Auth & multi-site tenant checks
                no_tok = client.post("/v1/safety-reports/e2b", json={"message_id": "m", "case_uid": "c", "subject_id": fx.subj_drug[0], "aware_at": t0_15d, "meddra_pt": "Nausea", "ctcae_grade": 1, "seriousness": {}, "investigator_causality": "unrelated", "narrative": "n"})
                bad_tok = client.get(f"/v1/safety-cases/{case_15d}", headers={"Authorization": "Bearer invalid-token"})
                pv_post_e2b = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_pv,
                    json={
                        "message_id": f"MSG-PV-{fx.tag}",
                        "case_uid": f"CASE-PV-{fx.tag}",
                        "subject_id": fx.subj_drug[0],
                        "aware_at": t0_15d,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "unrelated",
                        "narrative": "PV officer cannot originate site E2B.",
                    },
                )
                s2_post_s1_subj = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s2,
                    json={
                        "message_id": f"MSG-XPOST-{fx.tag}",
                        "case_uid": f"CASE-XPOST-{fx.tag}",
                        "subject_id": fx.subj_drug[0],
                        "aware_at": t0_15d,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "unrelated",
                        "narrative": "Cross-site post attempt.",
                    },
                )
                s2_get_s1_case = client.get(f"/v1/safety-cases/{case_15d}", headers=h_s2)
                s1_get_pv_case = client.get(f"/v1/pv/cases/{case_15d}", headers=h_s1)
                s1_get_pv_subs = client.get("/v1/pv/submissions", headers=h_s1)
                s1_get_dsmb = client.get(f"/v1/pv/trials/{fx.trial_id}/dsmb-line-listing", headers=h_s1)
                unk_subj_post = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-UNK-{fx.tag}",
                        "case_uid": f"CASE-UNK-{fx.tag}",
                        "subject_id": f"SUBJ-MISSING-{fx.tag}",
                        "aware_at": t0_15d,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "unrelated",
                        "narrative": "Unknown subject.",
                    },
                )
                unk_case_blinded = client.get(f"/v1/safety-cases/CASE-DOES-NOT-EXIST-{fx.tag}", headers=h_s1)
                unk_case_pv = client.get(f"/v1/pv/cases/CASE-DOES-NOT-EXIST-{fx.tag}", headers=h_pv)

                auth_ok = (
                    post_15d.status_code == 202
                    and no_tok.status_code == 401
                    and bad_tok.status_code == 401
                    and pv_post_e2b.status_code == 403
                    and s2_post_s1_subj.status_code == 403
                    and s2_get_s1_case.status_code == 403
                    and s1_get_pv_case.status_code == 403
                    and s1_get_pv_subs.status_code == 403
                    and s1_get_dsmb.status_code == 403
                    and unk_subj_post.status_code == 404
                    and unk_case_blinded.status_code == 404
                    and unk_case_pv.status_code == 404
                )
                checks["auth_and_tenant_site_enforcement"] = {
                    "score": 1.0 if auth_ok else 0.0,
                    "reasoning": (
                        f"401s=({no_tok.status_code},{bad_tok.status_code}), "
                        f"403s=({pv_post_e2b.status_code},{s2_post_s1_subj.status_code},{s2_get_s1_case.status_code},"
                        f"{s1_get_pv_case.status_code},{s1_get_pv_subs.status_code},{s1_get_dsmb.status_code}), "
                        f"404s=({unk_subj_post.status_code},{unk_case_blinded.status_code},{unk_case_pv.status_code})"
                    ),
                }

                # Idempotency-Key replay vs cross-case conflict
                idem_replay = client.post(
                    "/v1/safety-reports/e2b",
                    headers={**h_s1, "Idempotency-Key": idem_15d},
                    json={
                        "message_id": f"MSG-IGNORED-ON-REPLAY-{fx.tag}",
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 0,
                        "aware_at": t0_15d,
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 3,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": "Replay body.",
                    },
                )
                idem_conflict = client.post(
                    "/v1/safety-reports/e2b",
                    headers={**h_s1, "Idempotency-Key": idem_15d},
                    json={
                        "message_id": f"MSG-DIFFCASE-{fx.tag}",
                        "case_uid": f"CASE-DIFF-{fx.tag}",
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 0,
                        "aware_at": t0_15d,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "unrelated",
                        "narrative": "Different case reusing idempotency key.",
                    },
                )
                _, _, audit_after_idem = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                e2b_audits_after_idem = [
                    r
                    for r in audit_after_idem
                    if r["action"] == "E2B_REPORT_RECEIVED"
                    and (
                        case_15d in str(r["resource_ref"])
                        or msg_15d_0 in str(r["resource_ref"])
                        or case_15d in str(r["payload_json"])
                        or msg_15d_0 in str(r["payload_json"])
                    )
                ]
                idem_ok = (
                    post_15d.status_code == 202
                    and idem_replay.status_code == 202
                    and _json(idem_replay).get("message_id") == msg_15d_0
                    and _json(idem_replay).get("case_uid") == case_15d
                    and idem_conflict.status_code == 409
                    and _json(idem_conflict).get("error") == "idempotency_key_conflict"
                    and len(e2b_audits_after_idem) == 1
                )
                checks["idempotency_key_replay_and_conflict"] = {
                    "score": 1.0 if idem_ok else 0.0,
                    "reasoning": (
                        f"replay={idem_replay.status_code}/{_json(idem_replay).get('message_id')}, "
                        f"conflict={idem_conflict.status_code}/{_json(idem_conflict)}, audits={len(e2b_audits_after_idem)}"
                    ),
                }

                # Duplicate (case_uid, followup_num) with same message_id vs conflicting message_id
                dup_same_msg = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": msg_15d_0,
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 0,
                        "aware_at": t0_15d,
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 3,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": f"Grade 3 hospitalized pneumonitis {fx.tag}.",
                    },
                )
                dup_diff_msg = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-CONFLICT-FU0-{fx.tag}",
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 0,
                        "aware_at": t0_15d,
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 3,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": "Conflicting message_id for followup 0.",
                    },
                )
                fu_dup_ok = (
                    post_15d.status_code == 202
                    and dup_same_msg.status_code == 202
                    and _json(dup_same_msg).get("message_id") == msg_15d_0
                    and dup_diff_msg.status_code == 409
                    and _json(dup_diff_msg).get("error") == "duplicate_followup_conflict"
                )
                checks["followup_duplicate_vs_conflict_409"] = {
                    "score": 1.0 if fu_dup_ok else 0.0,
                    "reasoning": f"same_msg={dup_same_msg.status_code}, diff_msg={dup_diff_msg.status_code}/{_json(dup_diff_msg)}",
                }

                # Out-of-order follow-ups: F0, then F2, then delayed F1!
                # Neutropenia max_expected_grade=3, non-fatal -> RSI expected (not_susar)
                r_ooo_0 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-OOO-0-{fx.tag}",
                        "case_uid": case_ooo,
                        "subject_id": fx.subj_drug[1],
                        "followup_num": 0,
                        "aware_at": "2026-03-02T08:00:00+00:00",
                        "meddra_pt": "Neutropenia",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "possibly_related",
                        "narrative": "Initial F0 observation.",
                    },
                )
                r_ooo_2 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-OOO-2-{fx.tag}",
                        "case_uid": case_ooo,
                        "subject_id": fx.subj_drug[1],
                        "followup_num": 2,
                        "aware_at": "2026-03-02T12:00:00+00:00",
                        "meddra_pt": "Neutropenia",
                        "ctcae_grade": 2,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "possibly_related",
                        "narrative": f"Newest F2 authoritative narrative {fx.tag}",
                    },
                )
                r_ooo_1 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-OOO-1-{fx.tag}",
                        "case_uid": case_ooo,
                        "subject_id": fx.subj_drug[1],
                        "followup_num": 1,
                        "aware_at": "2026-03-02T10:00:00+00:00",
                        "meddra_pt": "Neutropenia",
                        "ctcae_grade": 3,
                        "seriousness": {"medically_important": True},
                        "investigator_causality": "possibly_related",
                        "narrative": "Delayed older F1 narrative that must not overwrite F2",
                    },
                )
                for resp_x in (r_ooo_0, r_ooo_2, r_ooo_1):
                    if resp_x.status_code == 202:
                        expected_e2b_audit_count += 1

                ooo_pre = _json(client.get(f"/v1/safety-cases/{case_ooo}", headers=h_s1))
                ooo_ser = ooo_pre.get("seriousness") or {}
                ooo_ok_pre = (
                    r_ooo_0.status_code == 202
                    and r_ooo_2.status_code == 202
                    and r_ooo_1.status_code == 202
                    and ooo_pre.get("latest_followup_num") == 2
                    and ooo_pre.get("narrative") == f"Newest F2 authoritative narrative {fx.tag}"
                    and ooo_pre.get("ctcae_grade") == 3
                    and ooo_ser.get("hospitalization") is True
                    and ooo_ser.get("medically_important") is True
                    and ooo_pre.get("is_serious") is True
                )

            # ==================================================================
            # PHASE B: 3-Leg Triage, RSI Rules, Selective Unblinding & Blinding Wall
            # ==================================================================
            with _Phase(checks, "phase b"):
                matrix_reports = [
                    # 1. 7-day fatal/LT positive SUSAR on drug
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-7D-{fx.tag}",
                            "case_uid": case_7d,
                            "subject_id": fx.subj_drug[0],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T06:00:00+00:00",
                            "meddra_pt": "Hepatic necrosis",
                            "ctcae_grade": 4,
                            "seriousness": {"life_threatening": True, "hospitalization": True},
                            "investigator_causality": "possibly_related",
                            "narrative": f"Life-threatening hepatic necrosis {fx.tag}.",
                        },
                    ),
                    # 2. OR-causality: investigator unrelated, sponsor possibly_related -> SUSAR
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-ORCAUS-{fx.tag}",
                            "case_uid": case_or_caus,
                            "subject_id": fx.subj_drug[1],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T09:00:00+00:00",
                            "meddra_pt": "Myocarditis",
                            "ctcae_grade": 3,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "unrelated",
                            "sponsor_causality": "possibly_related",
                            "narrative": f"Autoimmune myocarditis assessed related by sponsor {fx.tag}.",
                        },
                    ),
                    # 3. Both investigator & sponsor unrelated -> not_susar
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-UNREL-{fx.tag}",
                            "case_uid": case_both_unrel,
                            "subject_id": fx.subj_drug[1],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T09:30:00+00:00",
                            "meddra_pt": "Myocarditis",
                            "ctcae_grade": 3,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "unrelated",
                            "sponsor_causality": "unrelated",
                            "narrative": "Traumatic chest contusion unrelated to study drug.",
                        },
                    ),
                    # 4. Non-serious unexpected related -> not_susar
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-NONSER-{fx.tag}",
                            "case_uid": case_non_ser,
                            "subject_id": fx.subj_drug[2],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T10:00:00+00:00",
                            "meddra_pt": "Alopecia",
                            "ctcae_grade": 2,
                            "seriousness": {},
                            "investigator_causality": "related",
                            "narrative": "Non-serious Grade 2 alopecia.",
                        },
                    ),
                    # 5. RSI expected term within expected grade (Neutropenia <= 3, non-fatal) -> not_susar
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-RSIEXP-{fx.tag}",
                            "case_uid": case_rsi_exp,
                            "subject_id": fx.subj_drug[2],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T11:00:00+00:00",
                            "meddra_pt": "Neutropenia",
                            "ctcae_grade": 3,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": "Expected Grade 3 febrile neutropenia hospitalization.",
                        },
                    ),
                    # 6. RSI expected term with exceeded grade (Neutropenia Grade 4 > 3) -> SUSAR
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-RSIGRD-{fx.tag}",
                            "case_uid": case_rsi_grd,
                            "subject_id": fx.subj_drug[2],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T12:00:00+00:00",
                            "meddra_pt": "Neutropenia",
                            "ctcae_grade": 4,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": f"Unexpected Grade 4 neutropenia exceeding IB RSI {fx.tag}.",
                        },
                    ),
                    # 7. RSI term within expected grade (<=3) but life_threatening when fatal_expected=0 -> SUSAR
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-RSILT-{fx.tag}",
                            "case_uid": case_rsi_lt,
                            "subject_id": fx.subj_drug[3],
                            "followup_num": 0,
                            "aware_at": "2026-03-03T13:00:00+00:00",
                            "meddra_pt": "Infusion related reaction",
                            "ctcae_grade": 3,
                            "seriousness": {"life_threatening": True},
                            "investigator_causality": "related",
                            "narrative": f"Life-threatening bronchospasm during infusion {fx.tag}.",
                        },
                    ),
                    # 8. Placebo 3-leg SUSAR candidate -> unblinded once, closed_non_investigational, 0 FDA submissions
                    (
                        h_s1,
                        {
                            "message_id": f"MSG-PLB-{fx.tag}",
                            "case_uid": case_plb,
                            "subject_id": fx.subj_placebo,
                            "followup_num": 0,
                            "aware_at": "2026-03-03T14:00:00+00:00",
                            "meddra_pt": "Anaphylactic shock",
                            "ctcae_grade": 4,
                            "seriousness": {"life_threatening": True, "hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": f"Grade 4 anaphylactic reaction on blinded therapy {fx.tag}.",
                        },
                    ),
                    # 9. Site 2 drug SUSAR (for cross-site search & listing isolation)
                    (
                        h_s2,
                        {
                            "message_id": f"MSG-S2-{fx.tag}",
                            "case_uid": case_s2,
                            "subject_id": fx.subj_site2,
                            "followup_num": 0,
                            "aware_at": "2026-03-03T15:00:00+00:00",
                            "meddra_pt": "Encephalopathy",
                            "ctcae_grade": 3,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": f"Site 2 hospitalized encephalopathy {s2_unique_kw}.",
                        },
                    ),
                ]

                for hdrs, body in matrix_reports:
                    mr = client.post("/v1/safety-reports/e2b", headers=hdrs, json=body)
                    if mr.status_code == 202:
                        expected_e2b_audit_count += 1

                w_b = _run_worker(workspace, tmp_dir, now_iso="2026-03-03T16:00:00+00:00")

                # Re-verify out-of-order case after worker convergence
                ooo_post = _json(client.get(f"/v1/safety-cases/{case_ooo}", headers=h_s1))
                ooo_ok_post = (
                    w_b.returncode == 0
                    and ooo_post.get("site_status") == "assessed"
                    and ooo_post.get("latest_followup_num") == 2
                    and ooo_post.get("narrative") == f"Newest F2 authoritative narrative {fx.tag}"
                    and ooo_post.get("ctcae_grade") == 3
                )
                checks["out_of_order_followup_monotonic_projection"] = {
                    "score": 1.0 if (ooo_ok_pre and ooo_ok_post) else 0.0,
                    "reasoning": f"pre_ok={ooo_ok_pre}, post={ooo_post}",
                }

                pv_15d = _json(client.get(f"/v1/pv/cases/{case_15d}", headers=h_pv))
                pv_7d = _json(client.get(f"/v1/pv/cases/{case_7d}", headers=h_pv))
                pv_orc = _json(client.get(f"/v1/pv/cases/{case_or_caus}", headers=h_pv))
                pv_unrel = _json(client.get(f"/v1/pv/cases/{case_both_unrel}", headers=h_pv))
                pv_nonser = _json(client.get(f"/v1/pv/cases/{case_non_ser}", headers=h_pv))
                pv_rsi_exp = _json(client.get(f"/v1/pv/cases/{case_rsi_exp}", headers=h_pv))
                pv_rsi_grd = _json(client.get(f"/v1/pv/cases/{case_rsi_grd}", headers=h_pv))
                pv_rsi_lt = _json(client.get(f"/v1/pv/cases/{case_rsi_lt}", headers=h_pv))
                pv_plb = _json(client.get(f"/v1/pv/cases/{case_plb}", headers=h_pv))

                pos_triage_ok = (
                    w_b.returncode == 0
                    and pv_15d.get("pv_status") == "susar_submitted"
                    and pv_15d.get("is_serious") is True
                    and pv_15d.get("is_suspected") is True
                    and pv_15d.get("is_unexpected") is True
                    and pv_15d.get("unblinded") is True
                    and pv_15d.get("treatment_arm") == fx.drug_arm
                    and len(pv_15d.get("submissions", [])) == 1
                    and pv_7d.get("pv_status") == "susar_submitted"
                    and pv_7d.get("treatment_arm") == fx.drug_arm
                )
                susar_converged_ok = pos_triage_ok
                checks["three_legged_susar_positive_triage"] = {
                    "score": 1.0 if pos_triage_ok else 0.0,
                    "reasoning": f"pv_15d={pv_15d.get('pv_status')}/{pv_15d.get('treatment_arm')}, pv_7d={pv_7d.get('pv_status')}",
                }

                or_caus_ok = (
                    pv_orc.get("pv_status") == "susar_submitted"
                    and pv_orc.get("is_suspected") is True
                    and pv_orc.get("unblinded") is True
                    and len(pv_orc.get("submissions", [])) == 1
                    and pv_unrel.get("pv_status") == "not_susar"
                    and pv_unrel.get("is_suspected") is False
                    and pv_unrel.get("unblinded") is False
                    and pv_unrel.get("treatment_arm") is None
                    and pv_unrel.get("submissions") == []
                )
                checks["or_causality_sponsor_overrides_unrelated_investigator"] = {
                    "score": 1.0 if or_caus_ok else 0.0,
                    "reasoning": f"sponsor_related={pv_orc.get('pv_status')}, both_unrelated={pv_unrel.get('pv_status')}",
                }

                rsi_exp_ok = (
                    susar_converged_ok
                    and pv_rsi_exp.get("pv_status") == "not_susar"
                    and pv_rsi_exp.get("is_serious") is True
                    and pv_rsi_exp.get("is_suspected") is True
                    and pv_rsi_exp.get("is_unexpected") is False
                    and pv_rsi_exp.get("submissions") == []
                    and pv_nonser.get("pv_status") == "not_susar"
                    and pv_nonser.get("is_serious") is False
                )
                checks["rsi_expected_term_and_grade_not_susar"] = {
                    "score": 1.0 if rsi_exp_ok else 0.0,
                    "reasoning": f"rsi_exp={pv_rsi_exp.get('pv_status')}/unexp={pv_rsi_exp.get('is_unexpected')}, nonser={pv_nonser.get('pv_status')}",
                }

                rsi_grd_ok = (
                    pv_rsi_grd.get("pv_status") == "susar_submitted"
                    and pv_rsi_grd.get("is_unexpected") is True
                    and len(pv_rsi_grd.get("submissions", [])) == 1
                )
                checks["rsi_severity_exceeded_marks_unexpected"] = {
                    "score": 1.0 if rsi_grd_ok else 0.0,
                    "reasoning": f"rsi_grade_exc={pv_rsi_grd.get('pv_status')}/unexp={pv_rsi_grd.get('is_unexpected')}",
                }

                # Also verifies Pneumonitis (case_15d) respected IB-v1 instead of future IB-v2!
                rsi_lt_ok = (
                    pv_rsi_lt.get("pv_status") == "susar_submitted"
                    and pv_rsi_lt.get("is_unexpected") is True
                    and pv_rsi_lt.get("report_clock") == "7_day"
                    and pv_15d.get("is_unexpected") is True
                )
                checks["rsi_fatal_or_lt_on_nonfatal_term_unexpected"] = {
                    "score": 1.0 if rsi_lt_ok else 0.0,
                    "reasoning": f"rsi_lt={pv_rsi_lt.get('pv_status')}/clock={pv_rsi_lt.get('report_clock')}",
                }

                # Selective IWRS unblinding checks
                iwrs_counts_b = _iwrs_audit_counts(tmp_dir / "randomization_iwrs.db")
                non_susars = [case_ooo, case_both_unrel, case_non_ser, case_rsi_exp]
                non_susar_iwrs_ok = (
                    susar_converged_ok
                    and all(iwrs_counts_b.get(cid, 0) == 0 for cid in non_susars)
                    and all(
                        _json(client.get(f"/v1/pv/cases/{cid}", headers=h_pv)).get("unblinded") is False
                        and _json(client.get(f"/v1/pv/cases/{cid}", headers=h_pv)).get("treatment_arm") is None
                        for cid in non_susars
                    )
                    and iwrs_counts_b.get(case_15d, 0) == 1
                )
                checks["non_susar_cases_never_query_iwrs"] = {
                    "score": 1.0 if non_susar_iwrs_ok else 0.0,
                    "reasoning": f"iwrs_counts={iwrs_counts_b}",
                }

                plb_subs = _extract_submissions(client.get(f"/v1/pv/submissions?case_uid={case_plb}", headers=h_pv))
                plb_ok = (
                    susar_converged_ok
                    and pv_plb.get("pv_status") == "closed_non_investigational"
                    and pv_plb.get("unblinded") is True
                    and pv_plb.get("treatment_arm") == fx.placebo_arm
                    and pv_plb.get("report_clock") is None
                    and pv_plb.get("day0_at") is None
                    and pv_plb.get("due_at") is None
                    and pv_plb.get("submissions") == []
                    and plb_subs == []
                    and iwrs_counts_b.get(case_plb, 0) == 1
                )
                checks["placebo_susar_candidate_closed_without_fda_submission"] = {
                    "score": 1.0 if plb_ok else 0.0,
                    "reasoning": f"pv_plb={pv_plb}, iwrs={iwrs_counts_b.get(case_plb, 0)}",
                }

                # Study Blinding Wall across HTTP, SQLite, FTS5, Cache, and Line Listings
                forbidden_blinded_tokens = [
                    fx.drug_arm,
                    "PLACEBO",
                    "closed_non_investigational",
                    "susar_submitted",
                    "susar_pending_submission",
                    "susar_retry_pending",
                    "treatment_arm",
                ]
                b_15d = _json(client.get(f"/v1/safety-cases/{case_15d}", headers=h_s1))
                b_plb = _json(client.get(f"/v1/safety-cases/{case_plb}", headers=h_s1))
                b_exp = _json(client.get(f"/v1/safety-cases/{case_rsi_exp}", headers=h_s1))
                blinded_dump = json.dumps([b_15d, b_plb, b_exp])
                endpoint_leaks = _contains_any(blinded_dump, forbidden_blinded_tokens)
                required_blinded_keys = {
                    "case_uid",
                    "subject_id",
                    "site_id",
                    "trial_id",
                    "site_status",
                    "latest_followup_num",
                    "meddra_pt",
                    "ctcae_grade",
                    "is_serious",
                    "seriousness",
                    "investigator_causality",
                    "sponsor_causality",
                    "narrative",
                }
                blinded_ep_ok = (
                    susar_converged_ok
                    and required_blinded_keys.issubset(set(b_15d.keys()))
                    and b_15d.get("site_status") == "assessed"
                    and b_plb.get("site_status") == "assessed"
                    and b_exp.get("site_status") == "assessed"
                    and not endpoint_leaks
                )
                checks["blinded_case_endpoint_zero_arm_or_susar_leak"] = {
                    "score": 1.0 if blinded_ep_ok else 0.0,
                    "reasoning": (
                        f"statuses=({b_15d.get('site_status')},{b_plb.get('site_status')},{b_exp.get('site_status')}), "
                        f"leaks={endpoint_leaks}"
                    ),
                }

                # Inspect blinded EDC tables, FTS5 search.db + search API, and cache.json
                econn = sqlite3.connect(str(tmp_dir / "clinical_edc.db"))
                try:
                    edc_blinded_dump = json.dumps(
                        {
                            "subjects_blinded": econn.execute("SELECT * FROM subjects_blinded").fetchall(),
                            "safety_cases": econn.execute("SELECT * FROM safety_cases").fetchall(),
                            "case_reports": econn.execute("SELECT * FROM case_reports").fetchall(),
                            "outbox_events": econn.execute("SELECT * FROM outbox_events").fetchall(),
                            "scheduled_jobs": econn.execute("SELECT * FROM scheduled_jobs").fetchall(),
                        }
                    )
                finally:
                    econn.close()

                sconn = sqlite3.connect(str(tmp_dir / "search.db"))
                try:
                    fts_dump = json.dumps(sconn.execute("SELECT * FROM case_search_fts").fetchall())
                finally:
                    sconn.close()

                cache_raw = (
                    (tmp_dir / "cache.json").read_text(encoding="utf-8")
                    if (tmp_dir / "cache.json").exists()
                    else "{}"
                )
                store_forbidden = [
                    fx.drug_arm,
                    "PLACEBO",
                    "closed_non_investigational",
                    "susar_submitted",
                    "susar_pending_submission",
                    "susar_retry_pending",
                ]
                store_leaks = _contains_any(edc_blinded_dump + fts_dump + cache_raw, store_forbidden)

                s1_search_own = client.get("/v1/safety-cases/search", params={"q": "Pneumonitis"}, headers=h_s1)
                s1_search_s2 = client.get("/v1/safety-cases/search", params={"q": s2_unique_kw}, headers=h_s1)
                s2_search_own = client.get("/v1/safety-cases/search", params={"q": s2_unique_kw}, headers=h_s2)
                pv_search_all = client.get("/v1/safety-cases/search", params={"q": s2_unique_kw}, headers=h_pv)

                fts_cache_ok = (
                    susar_converged_ok
                    and not store_leaks
                    and s1_search_own.status_code == 200
                    and any(h.get("case_uid") == case_15d for h in _json(s1_search_own).get("hits", []))
                    and s1_search_s2.status_code == 200
                    and len(_json(s1_search_s2).get("hits", [])) == 0
                    and s2_search_own.status_code == 200
                    and any(h.get("case_uid") == case_s2 for h in _json(s2_search_own).get("hits", []))
                    and pv_search_all.status_code == 200
                    and any(h.get("case_uid") == case_s2 for h in _json(pv_search_all).get("hits", []))
                )
                checks["blinded_sqlite_fts_and_cache_zero_leak"] = {
                    "score": 1.0 if fts_cache_ok else 0.0,
                    "reasoning": (
                        f"store_leaks={store_leaks}, s1_own={len(_json(s1_search_own).get('hits', []))}, "
                        f"s1_s2={len(_json(s1_search_s2).get('hits', []))}, s2_own={len(_json(s2_search_own).get('hits', []))}"
                    ),
                }

                # Site blinded line listing + sidecar checksum
                site_list_resp = client.get(f"/v1/sites/{fx.site1_id}/blinded-line-listing", headers=h_s1)
                site_list_body = _json(site_list_resp)
                site_file = tmp_dir / "line_listings" / "site" / fx.site1_id / "blinded_safety_listing.json"
                site_sha_file = Path(f"{site_file}.sha256")
                site_disk_ok = (
                    site_file.is_file()
                    and site_sha_file.is_file()
                    and hashlib.sha256(site_file.read_bytes()).hexdigest()
                    == site_sha_file.read_text(encoding="utf-8").strip()
                )
                listed_uids = {
                    c.get("case_uid")
                    for c in site_list_body.get("listing", {}).get("cases", [])
                    if isinstance(c, dict)
                }
                site_leaks = _contains_any(
                    json.dumps(site_list_body) + (site_file.read_text(encoding="utf-8") if site_file.is_file() else ""),
                    forbidden_blinded_tokens,
                )
                site_listing_ok = (
                    susar_converged_ok
                    and site_list_resp.status_code == 200
                    and site_list_body.get("checksum_valid") is True
                    and site_disk_ok
                    and {case_15d, case_7d, case_plb, case_rsi_exp}.issubset(listed_uids)
                    and case_s2 not in listed_uids
                    and not site_leaks
                )
                checks["site_blinded_line_listing_and_checksum"] = {
                    "score": 1.0 if site_listing_ok else 0.0,
                    "reasoning": (
                        f"http={site_list_resp.status_code}, checksum={site_list_body.get('checksum_valid')}, "
                        f"disk_ok={site_disk_ok}, count={len(listed_uids)}, leaks={site_leaks}"
                    ),
                }

                # DSMB unblinded SUSAR listing + sidecar checksum
                dsmb_resp = client.get(f"/v1/pv/trials/{fx.trial_id}/dsmb-line-listing", headers=h_pv)
                dsmb_body = _json(dsmb_resp)
                dsmb_file = tmp_dir / "line_listings" / "dsmb" / fx.trial_id / "unblinded_susar_listing.json"
                dsmb_sha_file = Path(f"{dsmb_file}.sha256")
                dsmb_disk_ok = (
                    dsmb_file.is_file()
                    and dsmb_sha_file.is_file()
                    and hashlib.sha256(dsmb_file.read_bytes()).hexdigest()
                    == dsmb_sha_file.read_text(encoding="utf-8").strip()
                )
                dsmb_cases = {
                    c.get("case_uid"): c
                    for c in dsmb_body.get("listing", {}).get("susar_cases", [])
                    if isinstance(c, dict)
                }
                expected_dsmb_susars = {case_15d, case_7d, case_or_caus, case_rsi_grd, case_rsi_lt, case_s2}
                excluded_from_dsmb = {case_plb, case_both_unrel, case_non_ser, case_rsi_exp, case_ooo}
                dsmb_ok = (
                    susar_converged_ok
                    and dsmb_resp.status_code == 200
                    and dsmb_body.get("checksum_valid") is True
                    and dsmb_disk_ok
                    and expected_dsmb_susars.issubset(set(dsmb_cases.keys()))
                    and not (excluded_from_dsmb & set(dsmb_cases.keys()))
                    and all(
                        dsmb_cases[k].get("treatment_arm") == fx.drug_arm
                        and dsmb_cases[k].get("pv_status") == "susar_submitted"
                        and dsmb_cases[k].get("report_clock") in {"7_day", "15_day"}
                        for k in expected_dsmb_susars
                    )
                )
                checks["dsmb_unblinded_susar_listing_and_checksum"] = {
                    "score": 1.0 if dsmb_ok else 0.0,
                    "reasoning": (
                        f"http={dsmb_resp.status_code}, checksum={dsmb_body.get('checksum_valid')}, "
                        f"dsmb_uids={sorted(k for k in dsmb_cases if k)}"
                    ),
                }

                # Initial 15-day vs 7-day regulatory clocks
                clocks_ok = (
                    pv_15d.get("report_clock") == "15_day"
                    and _ts_eq(pv_15d.get("day0_at"), "2026-03-01T10:00:00+00:00")
                    and _ts_eq(pv_15d.get("due_at"), "2026-03-16T10:00:00+00:00")
                    and pv_7d.get("report_clock") == "7_day"
                    and _ts_eq(pv_7d.get("day0_at"), "2026-03-03T06:00:00+00:00")
                    and _ts_eq(pv_7d.get("due_at"), "2026-03-10T06:00:00+00:00")
                )
                checks["fifteen_day_and_seven_day_initial_clocks"] = {
                    "score": 1.0 if clocks_ok else 0.0,
                    "reasoning": (
                        f"15d=({pv_15d.get('report_clock')},{pv_15d.get('day0_at')}->{pv_15d.get('due_at')}), "
                        f"7d=({pv_7d.get('report_clock')},{pv_7d.get('day0_at')}->{pv_7d.get('due_at')})"
                    ),
                }

                # FDA gateway submission files (.json + .meta.json) & ack_log.jsonl
                all_subs_b = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"trial_id": fx.trial_id}, headers=h_pv)
                )
                sub_15d = next((s for s in all_subs_b if s.get("case_uid") == case_15d), None)
                ack_log_text = (tmp_dir / "fda_gateway" / "ack_log.jsonl").read_text(encoding="utf-8")
                gw_files_ok = False
                if sub_15d and sub_15d.get("submission_id"):
                    sid_15 = str(sub_15d["submission_id"])
                    sf = tmp_dir / "fda_gateway" / "submissions" / f"{sid_15}.json"
                    smf = tmp_dir / "fda_gateway" / "submissions" / f"{sid_15}.json.meta.json"
                    gw_files_ok = (
                        sf.is_file()
                        and smf.is_file()
                        and sub_15d.get("status") == "submitted"
                        and bool(sub_15d.get("gateway_ack_id"))
                        and bool(sub_15d.get("submitted_at"))
                        and sub_15d.get("last_error") is None
                        and sid_15 in ack_log_text
                        and str(sub_15d["gateway_ack_id"]) in ack_log_text
                    )
                checks["fda_gateway_payload_meta_and_ack_log"] = {
                    "score": 1.0 if (len(all_subs_b) == 6 and gw_files_ok) else 0.0,
                    "reasoning": f"subs_count={len(all_subs_b)}, sub_15d={sub_15d}, gw_files_ok={gw_files_ok}",
                }

            # ==================================================================
            # PHASE C: Delayed Day-0 Qualification, Pre-Submission Tightening & Follow-Ups
            # ==================================================================
            with _Phase(checks, "phase c"):
                # 1. Delayed SUSAR qualification in single batch: F0 at T0 is Grade 2 expected Nausea (not SUSAR),
                #    F1 at T1 escalates to Grade 4 hospitalized Nausea (> max_expected_grade 2 -> SUSAR!).
                case_delay = f"CASE-DELAY-{fx.tag}"
                t0_delay = "2026-03-04T08:00:00+00:00"
                t1_delay = "2026-03-07T14:30:00+00:00"
                rd0 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-DLY-0-{fx.tag}",
                        "case_uid": case_delay,
                        "subject_id": fx.subj_drug[3],
                        "followup_num": 0,
                        "aware_at": t0_delay,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 2,
                        "seriousness": {},
                        "investigator_causality": "related",
                        "narrative": "Initial mild expected Grade 2 nausea.",
                    },
                )
                rd1 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-DLY-1-{fx.tag}",
                        "case_uid": case_delay,
                        "subject_id": fx.subj_drug[3],
                        "followup_num": 1,
                        "aware_at": t1_delay,
                        "meddra_pt": "Nausea",
                        "ctcae_grade": 4,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": "Escalated to Grade 4 intractable nausea requiring TPN hospitalization.",
                    },
                )
                for rx in (rd0, rd1):
                    if rx.status_code == 202:
                        expected_e2b_audit_count += 1

                _run_worker(workspace, tmp_dir, now_iso="2026-03-07T15:00:00+00:00")
                pv_delay = _json(client.get(f"/v1/pv/cases/{case_delay}", headers=h_pv))
                delay_ok = (
                    pv_delay.get("pv_status") == "susar_submitted"
                    and pv_delay.get("report_clock") == "15_day"
                    and _ts_eq(pv_delay.get("day0_at"), t1_delay)
                    and _ts_eq(pv_delay.get("due_at"), "2026-03-22T14:30:00+00:00")
                )
                checks["delayed_susar_qualification_sets_threshold_day0"] = {
                    "score": 1.0 if delay_ok else 0.0,
                    "reasoning": f"day0={pv_delay.get('day0_at')} (expected {t1_delay}), due={pv_delay.get('due_at')}",
                }

                # 2. Pre-submission 15d -> 7d retroactive tightening: test BOTH branches of min(existing_due, T_up + 7d)!
                gw_cfg_path = tmp_dir / "fda_gateway" / "gateway_config.json"
                gw_cfg_path.write_text(
                    json.dumps({"mode": "nack_retryable", "reason": "ESG_503_MAINTENANCE"}),
                    encoding="utf-8",
                )
                case_tight_a = f"CASE-TIGHTA-{fx.tag}"  # T_up + 7d < existing_due
                case_tight_b = f"CASE-TIGHTB-{fx.tag}"  # existing_due < T_up + 7d
                for cuid_t in (case_tight_a, case_tight_b):
                    rt0 = client.post(
                        "/v1/safety-reports/e2b",
                        headers=h_s1,
                        json={
                            "message_id": f"MSG-{cuid_t}-0",
                            "case_uid": cuid_t,
                            "subject_id": fx.subj_drug[4],
                            "followup_num": 0,
                            "aware_at": "2026-03-05T10:00:00+00:00",
                            "meddra_pt": "Acute kidney injury",
                            "ctcae_grade": 3,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": "Hospitalized Grade 3 acute kidney injury.",
                        },
                    )
                    if rt0.status_code == 202:
                        expected_e2b_audit_count += 1

                # Initial worker run under NACK -> both have 15_day pending/retry_pending due 2026-03-20T10:00:00+00:00
                _run_worker(workspace, tmp_dir, now_iso="2026-03-05T10:05:00+00:00")
                sub_ta_before = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_tight_a}, headers=h_pv)
                )
                orig_ta_id = sub_ta_before[0].get("submission_id") if sub_ta_before else None

                # Tight A: life-threatening upgrade at 2026-03-07T10:00 (+7d = 2026-03-14T10:00 < 2026-03-20T10:00)
                rta1 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-{case_tight_a}-1",
                        "case_uid": case_tight_a,
                        "subject_id": fx.subj_drug[4],
                        "followup_num": 1,
                        "aware_at": "2026-03-07T10:00:00+00:00",
                        "meddra_pt": "Acute kidney injury",
                        "ctcae_grade": 4,
                        "seriousness": {"hospitalization": True, "life_threatening": True},
                        "investigator_causality": "related",
                        "narrative": "Progressed to life-threatening uremic crisis requiring CRRT.",
                    },
                )
                # Tight B: fatal upgrade at 2026-03-16T10:00 (+7d = 2026-03-23T10:00 > existing 2026-03-20T10:00!)
                rtb1 = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-{case_tight_b}-1",
                        "case_uid": case_tight_b,
                        "subject_id": fx.subj_drug[4],
                        "followup_num": 1,
                        "aware_at": "2026-03-16T10:00:00+00:00",
                        "meddra_pt": "Acute kidney injury",
                        "ctcae_grade": 5,
                        "seriousness": {"hospitalization": True, "death": True},
                        "investigator_causality": "related",
                        "narrative": "Fatal outcome prior to initial 15-day transmission.",
                    },
                )
                for rx in (rta1, rtb1):
                    if rx.status_code == 202:
                        expected_e2b_audit_count += 1

                gw_cfg_path.write_text(json.dumps({"mode": "ack"}), encoding="utf-8")
                _run_worker(workspace, tmp_dir, now_iso="2026-03-16T10:15:00+00:00")

                subs_ta = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_tight_a}, headers=h_pv)
                )
                subs_tb = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_tight_b}, headers=h_pv)
                )
                tight_ok = (
                    len(subs_ta) == 1
                    and subs_ta[0].get("submission_id") == orig_ta_id
                    and subs_ta[0].get("sequence_num") == 1
                    and subs_ta[0].get("report_clock") == "7_day"
                    and _ts_eq(subs_ta[0].get("due_at"), "2026-03-14T10:00:00+00:00")
                    and subs_ta[0].get("status") == "submitted"
                    and len(subs_tb) == 1
                    and subs_tb[0].get("sequence_num") == 1
                    and subs_tb[0].get("report_clock") == "7_day"
                    and _ts_eq(subs_tb[0].get("due_at"), "2026-03-20T10:00:00+00:00")
                    and subs_tb[0].get("status") == "submitted"
                )
                checks["pre_submission_15d_to_7d_retroactive_tightening"] = {
                    "score": 1.0 if tight_ok else 0.0,
                    "reasoning": f"tight_a={subs_ta}, tight_b={subs_tb}",
                }

                # 3. Post-submission benign narrative amendment vs material severity/seriousness escalation
                r_benign = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-15D-1-BENIGN-{fx.tag}",
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 1,
                        "aware_at": "2026-03-08T09:00:00+00:00",
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 3,
                        "seriousness": {"hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": f"Typo amendment only on chest imaging note {fx.tag}.",
                    },
                )
                if r_benign.status_code == 202:
                    expected_e2b_audit_count += 1
                _run_worker(workspace, tmp_dir, now_iso="2026-03-08T09:05:00+00:00")
                subs_after_benign = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_15d}, headers=h_pv)
                )
                b_after_benign = _json(client.get(f"/v1/safety-cases/{case_15d}", headers=h_s1))

                t_fu2 = "2026-03-09T11:00:00+00:00"
                r_escal = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-15D-2-ESCAL-{fx.tag}",
                        "case_uid": case_15d,
                        "subject_id": fx.subj_drug[0],
                        "followup_num": 2,
                        "aware_at": t_fu2,
                        "meddra_pt": "Pneumonitis",
                        "ctcae_grade": 4,
                        "seriousness": {"hospitalization": True, "life_threatening": True},
                        "investigator_causality": "related",
                        "narrative": f"Escalated to Grade 4 intubated respiratory failure {fx.tag}.",
                    },
                )
                if r_escal.status_code == 202:
                    expected_e2b_audit_count += 1
                _run_worker(workspace, tmp_dir, now_iso="2026-03-09T11:10:00+00:00")
                subs_after_escal = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_15d}, headers=h_pv)
                )

                post_fu_ok = (
                    len(subs_after_benign) == 1
                    and b_after_benign.get("latest_followup_num") == 1
                    and f"Typo amendment only on chest imaging note {fx.tag}." == b_after_benign.get("narrative")
                    and len(subs_after_escal) == 2
                    and subs_after_escal[0].get("submission_type") == "initial"
                    and subs_after_escal[0].get("sequence_num") == 1
                    and subs_after_escal[1].get("submission_type") == "followup"
                    and subs_after_escal[1].get("sequence_num") == 2
                    and subs_after_escal[1].get("parent_submission_id") == subs_after_escal[0].get("submission_id")
                    and subs_after_escal[1].get("report_clock") == "7_day"
                    and _ts_eq(subs_after_escal[1].get("day0_at"), t_fu2)
                    and _ts_eq(subs_after_escal[1].get("due_at"), "2026-03-16T11:00:00+00:00")
                    and subs_after_escal[1].get("status") == "submitted"
                )
                checks["post_submission_followup_vs_benign_amendment"] = {
                    "score": 1.0 if post_fu_ok else 0.0,
                    "reasoning": f"after_benign={len(subs_after_benign)}, after_escal={subs_after_escal}",
                }

                # Verify single IWRS unblinding audit across follow-ups AND NACK retries
                iwrs_counts_c = _iwrs_audit_counts(tmp_dir / "randomization_iwrs.db")
                single_unblind_ok = (
                    iwrs_counts_c.get(case_15d, 0) == 1
                    and iwrs_counts_c.get(case_tight_a, 0) == 1
                    and iwrs_counts_c.get(case_tight_b, 0) == 1
                    and iwrs_counts_c.get(case_plb, 0) == 1
                )
                checks["single_iwrs_unblind_audit_across_followups_and_retries"] = {
                    "score": 1.0 if single_unblind_ok else 0.0,
                    "reasoning": (
                        f"c15d={iwrs_counts_c.get(case_15d)}, tightA={iwrs_counts_c.get(case_tight_a)}, "
                        f"tightB={iwrs_counts_c.get(case_tight_b)}, plb={iwrs_counts_c.get(case_plb)}"
                    ),
                }

            # ==================================================================
            # PHASE D: Gateway NACK, Filesystem Fault & Audit DB Trigger Recovery
            # ==================================================================
            with _Phase(checks, "phase d"):
                gw_cfg_path = tmp_dir / "fda_gateway" / "gateway_config.json"
                gw_cfg_path.write_text(
                    json.dumps({"mode": "nack_retryable", "reason": "ESG_503_UPSTREAM"}),
                    encoding="utf-8",
                )
                case_nack = f"CASE-NACK-{fx.tag}"
                case_fs = f"CASE-FSFLT-{fx.tag}"
                for cuid_f, subj_f in ((case_nack, fx.subj_drug[5]), (case_fs, fx.subj_drug[5])):
                    rf = client.post(
                        "/v1/safety-reports/e2b",
                        headers=h_s1,
                        json={
                            "message_id": f"MSG-{cuid_f}",
                            "case_uid": cuid_f,
                            "subject_id": subj_f,
                            "followup_num": 0,
                            "aware_at": "2026-03-10T08:00:00+00:00",
                            "meddra_pt": "Agranulocytosis",
                            "ctcae_grade": 4,
                            "seriousness": {"hospitalization": True},
                            "investigator_causality": "related",
                            "narrative": f"Fault-resilience test case {cuid_f}.",
                        },
                    )
                    if rf.status_code == 202:
                        expected_e2b_audit_count += 1

                _run_worker(workspace, tmp_dir, now_iso="2026-03-10T08:05:00+00:00")
                pv_nack_during = _json(client.get(f"/v1/pv/cases/{case_nack}", headers=h_pv))
                subs_nack_during = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_nack}, headers=h_pv)
                )
                subs_fs_during = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_fs}, headers=h_pv)
                )
                _, _, aud_nack = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                false_nack_audits = [
                    r
                    for r in aud_nack
                    if r["action"] == "IND_SAFETY_SUBMITTED"
                    and (case_nack in str(r["payload_json"]) or case_fs in str(r["payload_json"]))
                ]
                nack_blocked_ok = (
                    pv_nack_during.get("pv_status") == "susar_retry_pending"
                    and len(subs_nack_during) == 1
                    and subs_nack_during[0].get("status") == "retry_pending"
                    and subs_nack_during[0].get("submitted_at") is None
                    and subs_nack_during[0].get("gateway_ack_id") is None
                    and "ESG_503_UPSTREAM" in str(subs_nack_during[0].get("last_error") or "")
                    and len(subs_fs_during) == 1
                    and subs_fs_during[0].get("status") == "retry_pending"
                    and len(false_nack_audits) == 0
                )

                # Restore gateway to ACK, but inject OS directory fault at fda_gateway/submissions/{fs_sub_id}.json!
                gw_cfg_path.write_text(json.dumps({"mode": "ack"}), encoding="utf-8")
                fs_sub_id = str(subs_fs_during[0]["submission_id"]) if subs_fs_during else f"MISSING-{fx.tag}"
                fs_fault_dir = tmp_dir / "fda_gateway" / "submissions" / f"{fs_sub_id}.json"
                fs_blocked_ok = False
                fs_recovered_ok = False

                fs_fault_dir.mkdir(parents=True, exist_ok=True)
                try:
                    _run_worker(workspace, tmp_dir, now_iso="2026-03-10T08:10:00+00:00")
                    pv_nack_rec = _json(client.get(f"/v1/pv/cases/{case_nack}", headers=h_pv))
                    subs_nack_rec = _extract_submissions(
                        client.get("/v1/pv/submissions", params={"case_uid": case_nack}, headers=h_pv)
                    )
                    pv_fs_mid = _json(client.get(f"/v1/pv/cases/{case_fs}", headers=h_pv))
                    subs_fs_mid = _extract_submissions(
                        client.get("/v1/pv/submissions", params={"case_uid": case_fs}, headers=h_pv)
                    )
                    _, _, aud_fs_mid = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                    false_fs_audits = [
                        r
                        for r in aud_fs_mid
                        if r["action"] == "IND_SAFETY_SUBMITTED" and case_fs in str(r["payload_json"])
                    ]
                    nack_recovered_ok = (
                        pv_nack_rec.get("pv_status") == "susar_submitted"
                        and len(subs_nack_rec) == 1
                        and subs_nack_rec[0].get("status") == "submitted"
                        and bool(subs_nack_rec[0].get("gateway_ack_id"))
                        and subs_nack_rec[0].get("last_error") is None
                    )
                    fs_blocked_ok = (
                        pv_fs_mid.get("pv_status") == "susar_retry_pending"
                        and len(subs_fs_mid) == 1
                        and subs_fs_mid[0].get("submission_id") == fs_sub_id
                        and subs_fs_mid[0].get("status") == "retry_pending"
                        and subs_fs_mid[0].get("submitted_at") is None
                        and len(false_fs_audits) == 0
                    )
                finally:
                    shutil.rmtree(fs_fault_dir, ignore_errors=True)

                checks["gateway_nack_marks_retry_pending_not_submitted"] = {
                    "score": 1.0 if (nack_blocked_ok and nack_recovered_ok) else 0.0,
                    "reasoning": f"nack_blocked={nack_blocked_ok}, nack_recovered={nack_recovered_ok}",
                }

                w_fs_rec = _run_worker(workspace, tmp_dir, now_iso="2026-03-10T08:15:00+00:00")
                pv_fs_after = _json(client.get(f"/v1/pv/cases/{case_fs}", headers=h_pv))
                subs_fs_after = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_fs}, headers=h_pv)
                )
                fs_recovered_ok = (
                    w_fs_rec.returncode == 0
                    and fs_blocked_ok
                    and pv_fs_after.get("pv_status") == "susar_submitted"
                    and len(subs_fs_after) == 1
                    and subs_fs_after[0].get("submission_id") == fs_sub_id
                    and subs_fs_after[0].get("status") == "submitted"
                    and subs_fs_after[0].get("last_error") is None
                    and (tmp_dir / "fda_gateway" / "submissions" / f"{fs_sub_id}.json").is_file()
                )
                checks["filesystem_fault_blocks_false_submission_and_recovers"] = {
                    "score": 1.0 if fs_recovered_ok else 0.0,
                    "reasoning": f"fs_blocked={fs_blocked_ok}, after={subs_fs_after}",
                }

                # SQLite ABORT trigger on part11_pv_audit.db during worker submission
                case_aud_flt = f"CASE-AUDFLT-{fx.tag}"
                raud = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-{case_aud_flt}",
                        "case_uid": case_aud_flt,
                        "subject_id": fx.subj_drug[6],
                        "followup_num": 0,
                        "aware_at": "2026-03-11T09:00:00+00:00",
                        "meddra_pt": "Fulminant hepatitis",
                        "ctcae_grade": 4,
                        "seriousness": {"life_threatening": True},
                        "investigator_causality": "related",
                        "narrative": "Part 11 audit DB fault test case.",
                    },
                )
                if raud.status_code == 202:
                    expected_e2b_audit_count += 1

                aconn = sqlite3.connect(str(tmp_dir / "part11_pv_audit.db"))
                aud_blocked_ok = False
                try:
                    aconn.execute(
                        """
                        CREATE TRIGGER temp_block_audit_writes
                        BEFORE INSERT ON audit_log
                        BEGIN
                            SELECT RAISE(ABORT, 'simulated_part11_storage_abort');
                        END;
                        """
                    )
                    aconn.commit()
                    _run_worker(workspace, tmp_dir, now_iso="2026-03-11T09:05:00+00:00")
                    pv_aud_mid = _json(client.get(f"/v1/pv/cases/{case_aud_flt}", headers=h_pv))
                    subs_aud_mid = _extract_submissions(
                        client.get("/v1/pv/submissions", params={"case_uid": case_aud_flt}, headers=h_pv)
                    )
                    aud_blocked_ok = (
                        pv_aud_mid.get("pv_status") == "susar_retry_pending"
                        and len(subs_aud_mid) == 1
                        and subs_aud_mid[0].get("status") == "retry_pending"
                        and subs_aud_mid[0].get("submitted_at") is None
                    )
                finally:
                    aconn.execute("DROP TRIGGER IF EXISTS temp_block_audit_writes;")
                    aconn.commit()
                    aconn.close()

                w_aud_rec = _run_worker(workspace, tmp_dir, now_iso="2026-03-11T09:10:00+00:00")
                pv_aud_after = _json(client.get(f"/v1/pv/cases/{case_aud_flt}", headers=h_pv))
                subs_aud_after = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_aud_flt}, headers=h_pv)
                )
                chain_aud_ok, _, aud_rows_after = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                aud_sub_events = [
                    r
                    for r in aud_rows_after
                    if r["action"] == "IND_SAFETY_SUBMITTED" and case_aud_flt in str(r["payload_json"])
                ]
                aud_rec_ok = (
                    aud_blocked_ok
                    and w_aud_rec.returncode == 0
                    and pv_aud_after.get("pv_status") == "susar_submitted"
                    and len(subs_aud_after) == 1
                    and subs_aud_after[0].get("status") == "submitted"
                    and len(aud_sub_events) == 1
                    and chain_aud_ok
                )
                checks["audit_db_abort_trigger_prevents_split_brain_and_recovers"] = {
                    "score": 1.0 if aud_rec_ok else 0.0,
                    "reasoning": (
                        f"blocked={aud_blocked_ok}, after_subs={subs_aud_after}, "
                        f"audit_events={len(aud_sub_events)}, chain_ok={chain_aud_ok}"
                    ),
                }

            # ==================================================================
            # PHASE E: 4-Worker Race & Part 11 Cryptographic Chain Verification
            # ==================================================================
            with _Phase(checks, "phase e"):
                case_race = f"CASE-RACE-{fx.tag}"
                r_race = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-{case_race}",
                        "case_uid": case_race,
                        "subject_id": fx.subj_drug[7],
                        "followup_num": 0,
                        "aware_at": "2026-03-12T07:00:00+00:00",
                        "meddra_pt": "Stevens-Johnson syndrome",
                        "ctcae_grade": 4,
                        "seriousness": {"life_threatening": True, "hospitalization": True},
                        "investigator_causality": "related",
                        "narrative": f"Concurrent worker race SUSAR {fx.tag}.",
                    },
                )
                if r_race.status_code == 202:
                    expected_e2b_audit_count += 1

                race_procs = _run_workers_concurrently(
                    workspace, tmp_dir, n=4, now_iso="2026-03-12T07:05:00+00:00"
                )
                pv_race = _json(client.get(f"/v1/pv/cases/{case_race}", headers=h_pv))
                subs_race = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"case_uid": case_race}, headers=h_pv)
                )
                iwrs_race = _iwrs_audit_counts(tmp_dir / "randomization_iwrs.db")
                chain_e_ok, chain_e_msg, audit_rows_e = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")
                race_ind_audits = [
                    r
                    for r in audit_rows_e
                    if r["action"] == "IND_SAFETY_SUBMITTED" and case_race in str(r["payload_json"])
                ]
                race_ok = (
                    r_race.status_code == 202
                    and all(p.returncode == 0 for p in race_procs)
                    and pv_race.get("pv_status") == "susar_submitted"
                    and len(subs_race) == 1
                    and subs_race[0].get("sequence_num") == 1
                    and subs_race[0].get("status") == "submitted"
                    and iwrs_race.get(case_race, 0) == 1
                    and len(race_ind_audits) == 1
                    and chain_e_ok
                )
                checks["concurrent_workers_single_submission_and_unblind"] = {
                    "score": 1.0 if race_ok else 0.0,
                    "reasoning": (
                        f"rcs={[p.returncode for p in race_procs]}, subs={len(subs_race)}, "
                        f"iwrs={iwrs_race.get(case_race, 0)}, ind_audits={len(race_ind_audits)}, chain={chain_e_ok}"
                    ),
                }

                e2b_audit_rows = [r for r in audit_rows_e if r["action"] == "E2B_REPORT_RECEIVED"]
                chain_lifecycle_ok = (
                    susar_converged_ok
                    and chain_e_ok
                    and expected_e2b_audit_count >= 15
                    and len(e2b_audit_rows) == expected_e2b_audit_count
                )
                checks["part11_hash_chain_intact_across_lifecycle"] = {
                    "score": 1.0 if chain_lifecycle_ok else 0.0,
                    "reasoning": (
                        f"chain={chain_e_ok} ({chain_e_msg}), e2b_audits={len(e2b_audit_rows)}/"
                        f"{expected_e2b_audit_count}"
                    ),
                }

                all_trial_subs = _extract_submissions(
                    client.get("/v1/pv/submissions", params={"trial_id": fx.trial_id}, headers=h_pv)
                )
                ind_audit_rows = [r for r in audit_rows_e if r["action"] == "IND_SAFETY_SUBMITTED"]
                ind_by_sub_id: dict[str, dict[str, Any]] = {}
                for r in ind_audit_rows:
                    p = json.loads(r["payload_json"])
                    if isinstance(p, dict) and p.get("submission_id"):
                        ind_by_sub_id[str(p["submission_id"])] = p

                submitted_subs = [s for s in all_trial_subs if s.get("status") == "submitted"]
                forbidden_cases_in_ind = [
                    r
                    for r in ind_audit_rows
                    if any(
                        bad in str(r["payload_json"])
                        for bad in (case_plb, case_non_ser, case_both_unrel, case_rsi_exp, case_ooo)
                    )
                ]
                ind_ack_only_ok = (
                    susar_converged_ok
                    and len(submitted_subs) >= 12
                    and len(ind_audit_rows) == len(submitted_subs)
                    and len(forbidden_cases_in_ind) == 0
                    and all(
                        str(s["submission_id"]) in ind_by_sub_id
                        and ind_by_sub_id[str(s["submission_id"])].get("case_uid") == s.get("case_uid")
                        and int(ind_by_sub_id[str(s["submission_id"])].get("sequence_num", -1))
                        == int(s.get("sequence_num", -2))
                        and ind_by_sub_id[str(s["submission_id"])].get("gateway_ack_id") == s.get("gateway_ack_id")
                        for s in submitted_subs
                    )
                )
                checks["ind_safety_submitted_logged_only_on_ack"] = {
                    "score": 1.0 if ind_ack_only_ok else 0.0,
                    "reasoning": (
                        f"submitted_subs={len(submitted_subs)}, ind_audits={len(ind_audit_rows)}, "
                        f"forbidden={len(forbidden_cases_in_ind)}"
                    ),
                }

            # ==================================================================
            # PHASE F: Baseline PV & EDC Workflow Preservation
            # ==================================================================
            with _Phase(checks, "phase f"):
                base_search = client.get("/v1/safety-cases/search", params={"q": "Fatigue"}, headers=h_s1)
                summary_resp = client.get(f"/v1/trials/{fx.trial_id}/blinded-summary", headers=h_s1)
                summary_body = _json(summary_resp)
                exp_subj_cnt = int(pre_blinded_summary.get("total_subjects", 0))
                # 18 distinct E2B cases ingested across Phases A-E (17 serious, 1 non-serious case_non_ser)
                exp_case_cnt = int(pre_blinded_summary.get("total_safety_cases", 0)) + 18
                exp_ser_cnt = int(pre_blinded_summary.get("serious_safety_cases", 0)) + 17

                summary_leaks = _contains_any(json.dumps(summary_body), [fx.drug_arm, "PLACEBO", "susar"])
                base_intact_ok = (
                    susar_converged_ok
                    and base_search.status_code == 200
                    and any(h.get("case_uid") == fx.baseline_case_uid for h in _json(base_search).get("hits", []))
                    and summary_resp.status_code == 200
                    and summary_body.get("total_subjects") == exp_subj_cnt
                    and summary_body.get("total_safety_cases") == exp_case_cnt
                    and summary_body.get("serious_safety_cases") == exp_ser_cnt
                    and not summary_leaks
                )
                checks["existing_subjects_and_blinded_summary_intact"] = {
                    "score": 1.0 if base_intact_ok else 0.0,
                    "reasoning": f"summary={summary_body}, expected=({exp_subj_cnt},{exp_case_cnt},{exp_ser_cnt})",
                }

                post_subj = f"SUBJ-POST-{fx.tag}"
                enr_post = client.post(
                    "/v1/subjects",
                    headers=h_s1,
                    json={
                        "subject_id": post_subj,
                        "trial_id": fx.trial_id,
                        "site_id": fx.site1_id,
                        "age": 52,
                        "sex": "F",
                        "enrollment_date": "2026-03-15",
                        "treatment_arm": fx.drug_arm,
                        "is_investigational_drug": True,
                    },
                )
                post_case = f"CASE-POST-{fx.tag}"
                post_kw = f"PostWorkflowHeadache{fx.tag}"
                r_post_case = client.post(
                    "/v1/safety-reports/e2b",
                    headers=h_s1,
                    json={
                        "message_id": f"MSG-{post_case}",
                        "case_uid": post_case,
                        "subject_id": post_subj,
                        "followup_num": 0,
                        "aware_at": "2026-03-15T10:00:00+00:00",
                        "meddra_pt": "Headache",
                        "ctcae_grade": 1,
                        "seriousness": {},
                        "investigator_causality": "unrelated",
                        "narrative": f"Mild post-enrollment {post_kw}.",
                    },
                )
                w_post = _run_worker(workspace, tmp_dir, now_iso="2026-03-15T10:05:00+00:00")
                s_post = client.get("/v1/safety-cases/search", params={"q": post_kw}, headers=h_s1)
                health_post = client.get("/health")
                final_chain_ok, _, _ = _verify_part11_chain(tmp_dir / "part11_pv_audit.db")

                post_wf_ok = (
                    susar_converged_ok
                    and enr_post.status_code == 201
                    and "treatment_arm" not in _json(enr_post)
                    and r_post_case.status_code == 202
                    and w_post.returncode == 0
                    and s_post.status_code == 200
                    and any(h.get("case_uid") == post_case for h in _json(s_post).get("hits", []))
                    and health_post.status_code == 200
                    and final_chain_ok
                )
                checks["post_sae_enrollment_search_and_health_workflows"] = {
                    "score": 1.0 if post_wf_ok else 0.0,
                    "reasoning": (
                        f"enr={enr_post.status_code}, e2b={r_post_case.status_code}, "
                        f"search_hits={len(_json(s_post).get('hits', []))}, chain={final_chain_ok}"
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
