# Add Expedited IND Safety (SUSAR) Reporting to TrialVault PV

TrialVault PV is a multi-site Phase III Clinical Trial Pharmacovigilance and Safety Intake platform used jointly by blinded clinical investigative sites and unblinded sponsor pharmacovigilance (PV) officers.

Under FDA 21 CFR §312.32, sponsors must evaluate incoming individual case safety reports (ICSRs) and submit expedited IND Safety Reports to the FDA within 7 or 15 calendar days whenever an event is **Serious**, **Suspected** (reasonable possibility of causal relationship by either investigator or sponsor), and **Unexpected** against the active Investigator's Brochure Reference Safety Information (RSI) for a participant receiving the investigational drug. At the same time, ICH E9 Good Statistical Principles require strict preservation of the study blind for site personnel and prohibit unnecessary unblinding of non-qualifying cases.

Build expedited SAE intake, SUSAR triage, selective unblinding, and IND safety submission workflows for TrialVault PV.

## User Outcomes

- **Blinded clinical site coordinators** can submit initial and follow-up E2B safety reports for subjects enrolled at their site, safely retry submissions idempotently, and view or search their site's safety cases and blinded safety line listings without ever being exposed to randomized treatment assignments or unblinded SUSAR submission outcomes.
- **Unblinded sponsor PV officers** can inspect full pharmacovigilance case assessments, verify selective treatment-arm unblinding, track 7-day and 15-day regulatory reporting clocks (including delayed qualification, pre-submission clock tightening, and post-submission follow-up reports), and review IND safety submissions and unblinded Data Safety Monitoring Board (DSMB) SUSAR line listings.
- **Regulatory & operational teams** can rely on an unbroken 21 CFR Part 11 cryptographic audit trail, resilient retry behavior when the FDA submission gateway or backing stores encounter transient faults, and exactly-once unblinding and submission when concurrent background workers overlap.

## Regulatory & Blinding Compliance

- Only cases that satisfy all three SUSAR criteria (Serious, Suspected, Unexpected) may trigger emergency treatment-assignment unblinding, and each unblinded case must be unblinded once across follow-ups and retries.
- Blinded endpoints, search results, caches, blinded clinical records, and site safety line listings must never leak treatment arms or unblinded SUSAR outcomes, while unblinded DSMB listings and submission logs stay accurate with valid integrity checksums.
- Existing enrolled subjects, baseline non-serious safety workflows, and trial-level blinded summaries must continue to work normally.

## Public API Contract & Specifications

Implement these public HTTP endpoints (alongside asynchronous convergence in the existing background worker):
- `POST /v1/safety-reports/e2b`
- `GET /v1/safety-cases/{case_uid}`
- `GET /v1/pv/cases/{case_uid}`
- `GET /v1/pv/submissions`

Follow the HTTP contract, data shapes, and regulatory rules documented in:
- `docs/sae-reporting-api.md`
- `docs/fda-21cfr312-and-blinding-policy.md`

The code repository is at `/app`. Work autonomously to complete the task.
