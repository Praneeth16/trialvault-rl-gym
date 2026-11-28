"""Criterion 4: Study Blinding Wall, Search, Cache & Line Listings (16.0% total weight, 4 checks)."""

from rewardkit import criteria

criteria.trialvault_pv_check(
    "blinded_case_endpoint_zero_arm_or_susar_leak",
    name="blinded_case_endpoint_zero_arm_or_susar_leak",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "blinded_sqlite_fts_and_cache_zero_leak",
    name="blinded_sqlite_fts_and_cache_zero_leak",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "site_blinded_line_listing_and_checksum",
    name="site_blinded_line_listing_and_checksum",
    weight=4.0,
)
criteria.trialvault_pv_check(
    "dsmb_unblinded_susar_listing_and_checksum",
    name="dsmb_unblinded_susar_listing_and_checksum",
    weight=4.0,
)
