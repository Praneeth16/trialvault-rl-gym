# Add Participant Consent Withdrawal to TrialVault

TrialVault is a multi-site Phase III Clinical Trial Electronic Data Capture (EDC) platform used by clinical research sites to manage enrolled subjects, study visits, adverse events, central-lab results, and electronic patient-reported outcomes (ePRO).

Study participants have the right to withdraw consent and request removal of their Protected Health Information (PHI) under HIPAA Safe Harbor rules. At the same time, FDA Good Clinical Practice (GCP) and 21 CFR Part 11 require TrialVault to retain de-identified safety and efficacy records and preserve an unbroken regulatory audit trail.

Build participant consent withdrawal for TrialVault.

## User Outcomes

An authorized clinical site coordinator should be able to:
- Request consent withdrawal and PHI de-identification for one of their site's enrolled subjects.
- Record a participant who only wants to stop being contacted (no more diaries, reminders, or follow-up messages) while still allowing their data to be used, and later escalate that to a full withdrawal if the participant asks.
- Check the status of a withdrawal request.
- Receive a clear outcome when a withdrawal request cannot be initiated or completed (for example, an active FDA regulatory hold, missing permissions, or an unknown subject).
- Trust that a completed withdrawal request means the subject's direct and quasi-identifying PHI has been de-identified across all active systems and queued work, and cannot be reintroduced by delayed central-lab feeds or replayed events.

## Clinical Retention & Compliance

- Clinical records collected prior to withdrawal (randomized treatment arm, study visits, adverse events, and central-lab biomarker measurements) must remain intact for trial safety and efficacy analysis while no longer identifying the withdrawn participant.
- The 21 CFR Part 11 audit trail must remain cryptographically verifiable, with participant PHI removed from audit detail records and the withdrawal event recorded.
- Other enrolled participants—including active subjects who share a household address or site investigator—must not be affected, and existing EDC operations must continue to work normally.

## Public API Contract & Specifications

Implement these public HTTP endpoints:
- `POST /v1/subjects/{subject_id}/withdrawal-requests`
- `GET /v1/withdrawal-requests/{request_id}`

Follow the API contract and HIPAA Safe Harbor / 21 CFR Part 11 retention rules documented in:
- `docs/withdrawal-api.md`
- `docs/hipaa-and-21cfr11-retention.md`

The code repository is at `/app`. Work autonomously to complete the task.
