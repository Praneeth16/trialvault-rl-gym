"""Criterion 2: Active-Store HIPAA Safe Harbor De-Identification (24.0% total weight, 8 checks)."""

from rewardkit import criteria

criteria.trialvault_check(
    "subjects_direct_phi_scrubbed", name="subjects_direct_phi_scrubbed", weight=4.0
)
criteria.trialvault_check(
    "safe_harbor_zip3_and_restricted_zeroed",
    name="safe_harbor_zip3_and_restricted_zeroed",
    weight=4.0,
)
criteria.trialvault_check(
    "safe_harbor_age_over_89_and_dates", name="safe_harbor_age_over_89_and_dates", weight=4.0
)
criteria.trialvault_check(
    "freetext_notes_and_hl7_redacted", name="freetext_notes_and_hl7_redacted", weight=4.0
)
criteria.trialvault_check("fts5_search_index_clean", name="fts5_search_index_clean", weight=3.0)
criteria.trialvault_check("projection_cache_clean", name="projection_cache_clean", weight=3.0)
criteria.trialvault_check("blobs_and_metadata_clean", name="blobs_and_metadata_clean", weight=4.0)
criteria.trialvault_check(
    "aliases_and_initials_redacted", name="aliases_and_initials_redacted", weight=3.0
)
