"""Criterion 6: Transient Failure & Crash Recovery (8.0% total weight, 2 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "transient_fault_prevents_premature_completion",
    name="transient_fault_prevents_premature_completion",
    weight=4.0,
)
criteria.trialvault_check(
    "retry_after_fault_converges_cleanly",
    name="retry_after_fault_converges_cleanly",
    weight=4.0,
)
