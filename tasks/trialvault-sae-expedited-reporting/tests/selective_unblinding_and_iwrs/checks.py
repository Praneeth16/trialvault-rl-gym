"""Criterion 3: ICH E9 Selective IWRS Unblinding (12.0% total weight, 3 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "non_susar_cases_never_query_iwrs",
    name="non_susar_cases_never_query_iwrs",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "placebo_susar_candidate_closed_without_fda_submission",
    name="placebo_susar_candidate_closed_without_fda_submission",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "single_iwrs_unblind_audit_across_followups_and_retries",
    name="single_iwrs_unblind_audit_across_followups_and_retries",
    weight=4.0,
)
