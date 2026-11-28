"""Criterion 6: FDA Gateway Submissions & Transient Fault Recovery (11.0% total weight, 4 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "fda_gateway_payload_meta_and_ack_log",
    name="fda_gateway_payload_meta_and_ack_log",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "gateway_nack_marks_retry_pending_not_submitted",
    name="gateway_nack_marks_retry_pending_not_submitted",
    weight=3.0,
)
criteria.trialvault_pv_check(
    "filesystem_fault_blocks_false_submission_and_recovers",
    name="filesystem_fault_blocks_false_submission_and_recovers",
    weight=2.5,
)
criteria.trialvault_pv_check(
    "audit_db_abort_trigger_prevents_split_brain_and_recovers",
    name="audit_db_abort_trigger_prevents_split_brain_and_recovers",
    weight=2.5,
)
