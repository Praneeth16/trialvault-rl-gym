"""Criterion 5: Replay & Late Central-Lab Webhook Suppression (12.0% total weight, 3 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "late_hl7_webhook_suppressed", name="late_hl7_webhook_suppressed", weight=5.0
)
criteria.trialvault_check(
    "replayed_outbox_event_suppressed", name="replayed_outbox_event_suppressed", weight=5.0
)
criteria.trialvault_check(
    "active_subject_webhook_still_ingests",
    name="active_subject_webhook_still_ingests",
    weight=4.0,
)
