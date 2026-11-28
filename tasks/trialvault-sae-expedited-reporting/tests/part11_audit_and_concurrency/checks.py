"""Criterion 7: 21 CFR Part 11 Audit Chain & Concurrent Worker Safety (10.0% total weight, 3 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "part11_hash_chain_intact_across_lifecycle",
    name="part11_hash_chain_intact_across_lifecycle",
    weight=3.5,
)
criteria.trialvault_pv_check(
    "ind_safety_submitted_logged_only_on_ack",
    name="ind_safety_submitted_logged_only_on_ack",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "concurrent_workers_single_submission_and_unblind",
    name="concurrent_workers_single_submission_and_unblind",
    weight=3.5,
)
