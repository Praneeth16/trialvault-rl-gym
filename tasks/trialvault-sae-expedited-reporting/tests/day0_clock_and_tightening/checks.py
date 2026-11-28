"""Criterion 5: FDA 21 CFR 312.32 Day-0 Clock, Tightening & Follow-Ups (16.0% total weight, 4 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "fifteen_day_and_seven_day_initial_clocks",
    name="fifteen_day_and_seven_day_initial_clocks",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "delayed_susar_qualification_sets_threshold_day0",
    name="delayed_susar_qualification_sets_threshold_day0",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "pre_submission_15d_to_7d_retroactive_tightening",
    name="pre_submission_15d_to_7d_retroactive_tightening",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "post_submission_followup_vs_benign_amendment",
    name="post_submission_followup_vs_benign_amendment",
    weight=4.0,
)
