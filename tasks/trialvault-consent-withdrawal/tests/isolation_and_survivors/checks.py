"""Criterion 7: Multi-Site Isolation, Regulatory Hold, Shared Artifacts & Survivors (10.0% weight, 5 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "cross_site_and_scope_isolation", name="cross_site_and_scope_isolation", weight=3.5
)
criteria.trialvault_check(
    "regulatory_hold_blocks_409_no_mutation",
    name="regulatory_hold_blocks_409_no_mutation",
    weight=3.5,
)
criteria.trialvault_check(
    "co_enrolled_household_survivor_untouched",
    name="co_enrolled_household_survivor_untouched",
    weight=3.0,
)
criteria.trialvault_check(
    "shared_shipment_manifest_scoped", name="shared_shipment_manifest_scoped", weight=3.0
)
criteria.trialvault_check(
    "site_monitoring_logs_and_sdv_scoped",
    name="site_monitoring_logs_and_sdv_scoped",
    weight=3.5,
)
