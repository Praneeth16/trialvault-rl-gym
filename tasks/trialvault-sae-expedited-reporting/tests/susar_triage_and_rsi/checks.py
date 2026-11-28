"""Criterion 2: 3-Legged SUSAR Triage & Active RSI Expectedness (16.0% total weight, 5 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "three_legged_susar_positive_triage",
    name="three_legged_susar_positive_triage",
    weight=3.5,
)
criteria.trialvault_pv_check(
    "or_causality_sponsor_overrides_unrelated_investigator",
    name="or_causality_sponsor_overrides_unrelated_investigator",
    weight=3.5,
)
criteria.trialvault_pv_check(
    "rsi_expected_term_and_grade_not_susar",
    name="rsi_expected_term_and_grade_not_susar",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "rsi_severity_exceeded_marks_unexpected",
    name="rsi_severity_exceeded_marks_unexpected",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "rsi_fatal_or_lt_on_nonfatal_term_unexpected",
    name="rsi_fatal_or_lt_on_nonfatal_term_unexpected",
    weight=3.0,
)
