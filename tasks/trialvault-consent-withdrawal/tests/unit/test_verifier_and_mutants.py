"""Verifier self-tests: boundary scores (starter=0.0, sol1=1.0, sol2=1.0) and 10/10 mutant kills."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

TASK_DIR = Path(__file__).resolve().parents[2]
STARTER_DIR = TASK_DIR / "environment" / "workspace"
SOLUTION_SH = TASK_DIR / "solution" / "solve.sh"
TESTS_DIR = TASK_DIR / "tests"
MUTANTS_DIR = TESTS_DIR / "mutants"

sys.path.insert(0, str(TESTS_DIR))
from harness import evaluate_workspace  # noqa: E402


def _make_workspace(tmp_path: Path, solution_variant: str | None = None) -> Path:
    ws = tmp_path / "app"
    shutil.copytree(STARTER_DIR, ws)
    if solution_variant is not None:
        env = os.environ.copy()
        env["SOLUTION"] = solution_variant
        env["APP_DIR"] = str(ws)
        subprocess.run(["bash", str(SOLUTION_SH)], env=env, check=True)
    return ws


def _run_rewardkit(workspace: Path, out_dir: Path, runs: int = 1) -> dict[str, float]:
    out_file = out_dir / "reward.json"
    env = os.environ.copy()
    env["TRIALVAULT_EVAL_RUNS"] = str(runs)
    rewardkit_bin = Path(sys.executable).parent / "rewardkit"
    r = subprocess.run(
        [
            str(rewardkit_bin),
            str(TESTS_DIR),
            "--workspace",
            str(workspace),
            "--output",
            str(out_file),
        ],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert r.returncode == 0, f"rewardkit failed: {r.stderr}\n{r.stdout}"
    return json.loads(out_file.read_text(encoding="utf-8"))


def test_starter_scores_zero(tmp_path: Path) -> None:
    ws = _make_workspace(tmp_path, solution_variant=None)
    scores = _run_rewardkit(ws, tmp_path / "out", runs=1)
    assert scores["reward"] == 0.0, scores


@pytest.mark.parametrize("solution_variant", ["1", "2"])
def test_reference_solutions_score_one_across_three_runs(
    tmp_path: Path, solution_variant: str
) -> None:
    ws = _make_workspace(tmp_path, solution_variant=solution_variant)
    scores = _run_rewardkit(ws, tmp_path / "out", runs=3)
    assert scores["reward"] == 1.0, scores


MUTANTS = json.loads((MUTANTS_DIR / "manifest.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("mutant", MUTANTS, ids=[m["id"] for m in MUTANTS])
def test_mutant_is_killed_by_targeted_checks(tmp_path: Path, mutant: dict) -> None:
    ws = _make_workspace(tmp_path, solution_variant="1")
    patch_file = MUTANTS_DIR / mutant["patch"]
    patch_res = subprocess.run(
        ["patch", "-p1", "--batch", "--forward", "-i", str(patch_file)],
        cwd=str(ws),
        capture_output=True,
        text=True,
        check=False,
    )
    assert patch_res.returncode == 0, f"patch {mutant['patch']} failed: {patch_res.stdout}\n{patch_res.stderr}"

    results = evaluate_workspace(ws, runs=1)
    for expected_fail in mutant["must_fail_checks"]:
        assert results[expected_fail]["score"] == 0.0, (
            f"Mutant {mutant['id']} escaped check {expected_fail}: {results[expected_fail]}"
        )
    # Overall reward via RewardKit must also drop strictly below 1.0
    scores = _run_rewardkit(ws, tmp_path / "out", runs=1)
    assert scores["reward"] < 1.0, f"Mutant {mutant['id']} still scored 1.0: {scores}"
