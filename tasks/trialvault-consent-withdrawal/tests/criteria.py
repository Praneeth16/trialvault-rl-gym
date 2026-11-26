"""Shared RewardKit criterion delegating to the 3-run black-box TrialVault harness."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

_TESTS_DIR = str(Path(__file__).resolve().parent)
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)

from harness import evaluate_workspace  # noqa: E402
from rewardkit import criterion  # noqa: E402


@criterion(shared=True, description="TrialVault black-box check: {check_id}")
def trialvault_check(workspace: Path, check_id: str) -> dict[str, Any]:
    runs = int(os.environ.get("TRIALVAULT_EVAL_RUNS", "3"))
    results = evaluate_workspace(workspace, runs=runs)
    entry = results[check_id]
    return {
        "score": float(entry["score"]),
        "reasoning": str(entry["reasoning"]),
    }
