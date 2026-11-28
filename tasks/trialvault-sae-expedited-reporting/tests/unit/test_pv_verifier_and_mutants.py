"""Verifier self-tests: boundary scores (starter=0.0, sol1=1.0, sol2=1.0) and 12/12 mutant kills."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

TASK_DIR = Path(__file__).resolve().parents[2]
STARTER_DIR = TASK_DIR / "environment" / "workspace"
SOLUTION_SH = TASK_DIR / "solution" / "solve.sh"
TESTS_DIR = TASK_DIR / "tests"
MUTANTS_DIR = TESTS_DIR / "mutants"

_SPEC = importlib.util.spec_from_file_location("pv_harness", TESTS_DIR / "harness.py")
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
sys.modules["pv_harness"] = _MOD
_SPEC.loader.exec_module(_MOD)
ALL_CHECK_IDS = _MOD.ALL_CHECK_IDS
evaluate_workspace = _MOD.evaluate_workspace


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
    out_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["TRIALVAULT_PV_EVAL_RUNS"] = str(runs)
    rewardkit_bin = Path(sys.executable).parent / "rewardkit"
    res = subprocess.run(
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
    assert res.returncode == 0, f"rewardkit failed: {res.stderr}\n{res.stdout}"
    return json.loads(out_file.read_text(encoding="utf-8"))


def test_reward_weights_and_check_budget() -> None:
    import ast

    reward_cfg = tomllib.loads((TESTS_DIR / "reward.toml").read_text(encoding="utf-8"))
    weights: dict[str, float] = reward_cfg["reward"][0]["weights"]
    assert len(weights) == 8
    assert abs(sum(weights.values()) - 100.0) < 1e-9
    assert len(ALL_CHECK_IDS) == 30

    seen_checks: list[str] = []
    for crit_name, crit_weight in weights.items():
        checks_file = TESTS_DIR / crit_name / "checks.py"
        assert checks_file.is_file(), f"missing {checks_file}"
        tree = ast.parse(checks_file.read_text(encoding="utf-8"))
        local_weights: list[tuple[str, float]] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and node.args:
                cid_node = node.args[0]
                if isinstance(cid_node, ast.Constant) and isinstance(cid_node.value, str):
                    w_val = next(
                        (
                            float(kw.value.value)
                            for kw in node.keywords
                            if kw.arg == "weight" and isinstance(kw.value, ast.Constant)
                        ),
                        1.0,
                    )
                    local_weights.append((cid_node.value, w_val))
        total_local = sum(w for _, w in local_weights)
        assert total_local > 0
        for cid, lw in local_weights:
            eff_pct = crit_weight * (lw / total_local)
            assert eff_pct <= 5.0 + 1e-9, f"Check {cid} exceeds 5.0% effective weight: {eff_pct}%"
            seen_checks.append(cid)

    assert sorted(seen_checks) == sorted(ALL_CHECK_IDS)


def test_starter_scores_zero(tmp_path: Path) -> None:
    ws = _make_workspace(tmp_path, solution_variant=None)
    scores = _run_rewardkit(ws, tmp_path / "out", runs=1)
    assert scores["reward"] == 0.0, scores


@pytest.mark.parametrize("solution_variant", ["1", "2"])
def test_reference_solutions_score_one_across_three_runs(
    tmp_path: Path,
    solution_variant: str,
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
    assert (
        patch_res.returncode == 0
    ), f"patch {mutant['patch']} failed: {patch_res.stdout}\n{patch_res.stderr}"

    results = evaluate_workspace(ws, runs=1)
    for expected_fail in mutant["must_fail_checks"]:
        assert results[expected_fail]["score"] == 0.0, (
            f"Mutant {mutant['id']} escaped check {expected_fail}: {results[expected_fail]}"
        )
    scores = _run_rewardkit(ws, tmp_path / "out", runs=1)
    assert scores["reward"] < 1.0, f"Mutant {mutant['id']} still scored 1.0: {scores}"
