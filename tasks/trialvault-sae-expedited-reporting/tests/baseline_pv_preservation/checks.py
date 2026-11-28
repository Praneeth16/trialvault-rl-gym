"""Criterion 8: Baseline PV & EDC Workflow Preservation (5.0% total weight, 2 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "existing_subjects_and_blinded_summary_intact",
    name="existing_subjects_and_blinded_summary_intact",
    weight=2.5,
)
criteria.trialvault_pv_check(
    "post_sae_enrollment_search_and_health_workflows",
    name="post_sae_enrollment_search_and_health_workflows",
    weight=2.5,
)
