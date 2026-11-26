"""Criterion 1: API Contract & Idempotency (10.0% total weight, 5 checks)."""

from rewardkit import criteria

criteria.trialvault_check("post_returns_202_pending", name="post_returns_202_pending", weight=2.5)
criteria.trialvault_check("get_status_contract", name="get_status_contract", weight=2.0)
criteria.trialvault_check("unknown_subject_404", name="unknown_subject_404", weight=1.0)
criteria.trialvault_check(
    "idempotency_replay_same_subject", name="idempotency_replay_same_subject", weight=2.0
)
criteria.trialvault_check(
    "idempotency_conflict_diff_subject_409",
    name="idempotency_conflict_diff_subject_409",
    weight=2.5,
)
