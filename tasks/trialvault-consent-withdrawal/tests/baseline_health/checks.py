"""Criterion 8: Post-Withdrawal Baseline EDC Health (2.0% total weight, 1 check)."""

from rewardkit import criteria

criteria.trialvault_check(
    "existing_edc_workflows_preserved_post_withdrawal",
    name="existing_edc_workflows_preserved_post_withdrawal",
    weight=4.0,
)
