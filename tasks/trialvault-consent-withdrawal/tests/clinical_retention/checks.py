"""Criterion 3: FDA 21 CFR Part 11 & GCP Clinical Retention (14.0% total weight, 4 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "safety_summary_arm_totals_preserved", name="safety_summary_arm_totals_preserved", weight=5.0
)
criteria.trialvault_check(
    "clinical_rows_retained_and_rekeyed", name="clinical_rows_retained_and_rekeyed", weight=4.0
)
criteria.trialvault_check("part11_audit_chain_valid", name="part11_audit_chain_valid", weight=4.0)
criteria.trialvault_check(
    "audit_pii_purged_and_withdrawal_event_logged",
    name="audit_pii_purged_and_withdrawal_event_logged",
    weight=3.0,
)
