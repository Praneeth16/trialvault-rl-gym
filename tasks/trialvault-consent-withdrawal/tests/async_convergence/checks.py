"""Criterion 4: Async Convergence & Queue Sanitization (12.0% total weight, 4 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "not_completed_before_convergence", name="not_completed_before_convergence", weight=5.0
)
criteria.trialvault_check(
    "converges_to_completed_after_worker", name="converges_to_completed_after_worker", weight=3.0
)
criteria.trialvault_check(
    "epro_notifications_spool_and_dlq_sanitized",
    name="epro_notifications_spool_and_dlq_sanitized",
    weight=4.0,
)
criteria.trialvault_check(
    "concurrent_workers_exactly_once", name="concurrent_workers_exactly_once", weight=4.0
)
