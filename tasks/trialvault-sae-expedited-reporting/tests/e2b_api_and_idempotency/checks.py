"""Criterion 1: E2B Intake API, Auth & Idempotency (14.0% total weight, 5 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "e2b_post_returns_202_received",
    name="e2b_post_returns_202_received",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "auth_and_tenant_site_enforcement",
    name="auth_and_tenant_site_enforcement",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "idempotency_key_replay_and_conflict",
    name="idempotency_key_replay_and_conflict",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "followup_duplicate_vs_conflict_409",
    name="followup_duplicate_vs_conflict_409",
    weight=2.5,
)
criteria.trialvault_pv_check(
    "out_of_order_followup_monotonic_projection",
    name="out_of_order_followup_monotonic_projection",
    weight=2.5,
)
