"""Criterion 9: Follow-Up-Only Withdrawal Scope & Escalation (10.0% total weight, 2 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "follow_up_only_stops_contact_keeps_data",
    name="follow_up_only_stops_contact_keeps_data",
    weight=5.0,
)
criteria.trialvault_check("scope_escalation_to_full", name="scope_escalation_to_full", weight=5.0)
