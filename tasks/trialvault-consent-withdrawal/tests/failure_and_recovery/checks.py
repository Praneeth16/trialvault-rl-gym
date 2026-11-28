"""Criterion 6: Transient Fault & Cross-Store Crash Recovery (9.0% total weight, 3 checks)."""

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
criteria.trialvault_check(
    "late_stage_audit_fault_recovery",
    name="late_stage_audit_fault_recovery",
    weight=4.0,
)
