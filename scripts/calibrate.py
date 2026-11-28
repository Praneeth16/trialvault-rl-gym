"""Local multi-model calibration harness for TrialVault RL Gym.

Runs headless coding agents (Claude Code CLI and/or Codex CLI) inside a strictly
isolated temporary copy of ONLY `environment/workspace/` with ONLY `instruction.md`,
then grades the resulting workspace using the hidden RewardKit 3-run black-box
verifier and stores per-run results under `benchmarking-runs/`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS_DIR = REPO_ROOT / "benchmarking-runs"


def _task_paths(task_slug: str) -> tuple[Path, Path, Path]:
    task_dir = REPO_ROOT / "tasks" / task_slug
    if not task_dir.is_dir():
        raise FileNotFoundError(f"Unknown task directory: {task_dir}")
    return (
        task_dir / "environment" / "workspace",
        task_dir / "instruction.md",
        task_dir / "tests",
    )


def _build_prompt(instruction_md: Path, isolated_dir: Path) -> str:
    raw = instruction_md.read_text(encoding="utf-8")
    return raw.replace("`/app`", f"`{isolated_dir}`").replace("at /app", f"at {isolated_dir}")


def _run_agent(
    harness: str,
    model: str | None,
    isolated_dir: Path,
    prompt: str,
    timeout_sec: int,
) -> tuple[int, str, str, float]:
    env = os.environ.copy()
    venv_bin = str(Path(sys.executable).parent)
    env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"
    env["PYTHONPATH"] = str(isolated_dir)
    env["TRIALVAULT_DATA_DIR"] = str(isolated_dir / "data")
    env["TRIALVAULT_PV_DATA_DIR"] = str(isolated_dir / "data")
    env.pop("CLAUDE_PROJECT_DIR", None)

    if harness == "claude":
        cmd = [
            "claude",
            "-p",
            prompt,
            "--dangerously-skip-permissions",
            "--no-session-persistence",
            "--strict-mcp-config",
            "--output-format",
            "json",
        ]
        if model:
            cmd.extend(["--model", model])
    elif harness == "codex":
        cmd = [
            "codex",
            "exec",
            "-C",
            str(isolated_dir),
            "--skip-git-repo-check",
            "-s",
            "workspace-write",
            "--json",
        ]
        if model:
            cmd.extend(["-m", model])
        cmd.append(prompt)
    else:
        raise ValueError(f"Unsupported harness: {harness}")

    t0 = time.monotonic()
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(isolated_dir),
            env=env,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=timeout_sec,
            check=False,
        )
        elapsed = time.monotonic() - t0
        return proc.returncode, proc.stdout, proc.stderr, elapsed
    except subprocess.TimeoutExpired as exc:
        elapsed = time.monotonic() - t0
        out = exc.stdout.decode("utf-8", errors="replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        err = exc.stderr.decode("utf-8", errors="replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        return 124, out, f"TIMEOUT after {timeout_sec}s\n{err}", elapsed


def _grade_workspace(tests_dir: Path, isolated_dir: Path, eval_runs: int) -> tuple[dict[str, float], dict[str, Any]]:
    out_dir = Path(tempfile.mkdtemp(prefix="tv_grade_"))
    try:
        out_json = out_dir / "reward.json"
        rewardkit_bin = Path(sys.executable).parent / "rewardkit"
        env = os.environ.copy()
        env["TRIALVAULT_EVAL_RUNS"] = str(eval_runs)
        env["TRIALVAULT_PV_EVAL_RUNS"] = str(eval_runs)
        proc = subprocess.run(
            [
                str(rewardkit_bin),
                str(tests_dir),
                "--workspace",
                str(isolated_dir),
                "--output",
                str(out_json),
            ],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0 or not out_json.exists():
            raise RuntimeError(f"rewardkit failed ({proc.returncode}): {proc.stderr}\n{proc.stdout}")
        scores = json.loads(out_json.read_text(encoding="utf-8"))
        details_path = out_dir / "reward-details.json"
        details = json.loads(details_path.read_text(encoding="utf-8")) if details_path.exists() else {}
        return scores, details
    finally:
        shutil.rmtree(out_dir, ignore_errors=True)


def run_calibration(
    *,
    task_slug: str,
    harness: str,
    model: str | None,
    label: str,
    run_idx: int,
    timeout_sec: int,
    eval_runs: int,
    out_subdir: str | None = None,
) -> dict[str, Any]:
    starter_dir, instruction_md, tests_dir = _task_paths(task_slug)
    sandbox_parent = Path(tempfile.mkdtemp(prefix=f"tv_cal_{task_slug[:12]}_{label}_r{run_idx}_"))
    isolated_dir = sandbox_parent / "app"
    shutil.copytree(starter_dir, isolated_dir)
    subprocess.run(["git", "init", "-q"], cwd=str(isolated_dir), check=True)
    subprocess.run(["git", "add", "."], cwd=str(isolated_dir), check=True)
    subprocess.run(
        ["git", "-c", "user.name=bench", "-c", "user.email=bench@local", "commit", "--no-verify", "-q", "-m", "starter"],
        cwd=str(isolated_dir),
        check=True,
    )

    try:
        prompt = _build_prompt(instruction_md, isolated_dir)
        rc, stdout, stderr, elapsed = _run_agent(harness, model, isolated_dir, prompt, timeout_sec)
        if rc != 0:
            print(f"[{task_slug}:{label} run {run_idx}] agent exited rc={rc} in {elapsed:.1f}s")
        diff_stat = subprocess.run(
            ["git", "status", "--short"], cwd=str(isolated_dir), capture_output=True, text=True
        ).stdout.strip()
        scores, details = _grade_workspace(tests_dir, isolated_dir, eval_runs=eval_runs)
    finally:
        shutil.rmtree(sandbox_parent, ignore_errors=True)

    dest_dir = RESULTS_DIR / out_subdir if out_subdir else RESULTS_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "task": f"praneeth16/{task_slug}",
        "task_slug": task_slug,
        "label": label,
        "harness": harness,
        "model": model or "default",
        "run_index": run_idx,
        "agent_exit_code": rc,
        "elapsed_sec": round(elapsed, 2),
        "changed_files": diff_stat.splitlines(),
        "scores": scores,
        "details": details,
        "agent_stderr_tail": "",
    }
    out_path = dest_dir / f"{label}_run{run_idx}.json"
    out_path.write_text(json.dumps(record, indent=2), encoding="utf-8")
    print(
        f"[{task_slug}:{label} run {run_idx}] reward={scores.get('reward', 0.0):.4f} "
        f"({scores.get('reward', 0.0) * 100:.1f}%) elapsed={elapsed:.1f}s rc={rc} -> {out_path.relative_to(REPO_ROOT)}"
    )
    for k, v in sorted(scores.items()):
        if k != "reward":
            print(f"    {k:28s}: {v:.3f}")
    return record


def main() -> None:
    parser = argparse.ArgumentParser(description="Calibrate TrialVault RL Gym against live coding agents")
    parser.add_argument("--task", default="trialvault-consent-withdrawal", help="Task directory slug under tasks/")
    parser.add_argument("--harness", choices=["claude", "codex"], required=True)
    parser.add_argument("--model", default=None, help="Model alias or slug")
    parser.add_argument("--label", required=True, help="Output file label, e.g. claude-sonnet-5")
    parser.add_argument("--runs", type=int, default=2, help="Number of independent trials")
    parser.add_argument("--timeout-sec", type=int, default=1800)
    parser.add_argument("--eval-runs", type=int, default=3)
    parser.add_argument("--first-run", type=int, default=1, help="Index of the first trial (for parallel launches)")
    parser.add_argument("--out-subdir", default=None, help="Optional subdirectory under benchmarking-runs/")
    args = parser.parse_args()

    for idx in range(args.first_run, args.first_run + args.runs):
        run_calibration(
            task_slug=args.task,
            harness=args.harness,
            model=args.model,
            label=args.label,
            run_idx=idx,
            timeout_sec=args.timeout_sec,
            eval_runs=args.eval_runs,
            out_subdir=args.out_subdir,
        )


if __name__ == "__main__":
    main()
