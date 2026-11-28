# TrialVault Participant Consent Withdrawal API Contract

This document defines the HTTP contract for participant consent withdrawal and related post-withdrawal behavior in TrialVault. Existing EDC endpoints, authentication rules, and response shapes must remain backwards compatible.

## Authentication & Authorization

All `/v1/*` endpoints (except `POST /v1/webhooks/central-lab` and `GET /health`) require:

```
Authorization: Bearer <site_token>
```

Scopes:
- `edc:read` — read subject records, coordinator search, queries, DSMB snapshots, and trial safety summaries
- `edc:write` — enroll subjects, record visits, adverse events, monitor queries, ePRO reminders, and generate DSMB snapshots
- `privacy:withdraw` — initiate and read participant consent withdrawal requests

Authorization rules:
- Missing or invalid token → `401 Unauthorized`
- Valid token lacking `privacy:withdraw` on withdrawal endpoints → `403 Forbidden`
- Accessing or withdrawing a subject or withdrawal request owned by another site → `403 Forbidden`
- Unknown subject or withdrawal request → `404 Not Found`

## Endpoints to Implement

### 1. `POST /v1/subjects/{subject_id}/withdrawal-requests`

Initiates an asynchronous consent withdrawal and HIPAA Safe Harbor de-identification workflow for `subject_id`.

**Headers**
- `Authorization: Bearer <site_token>` (required, scope `privacy:withdraw`)
- `Idempotency-Key: <string>` (optional)

**Request Body (JSON, optional)**
```json
{
  "reason": "participant_request",
  "requested_by": "coord-bos-01",
  "effective_date": "2026-03-10",
  "scope": "full"
}
```
All fields are optional. `reason` defaults to `"participant_request"`, `requested_by` defaults to the authenticated actor, `effective_date` (`YYYY-MM-DD`) defaults to the current UTC date, and `scope` defaults to `"full"`. See [Withdrawal Scope](#withdrawal-scope) for `"follow_up_only"`.

**Status Codes & Rules**
- `401 Unauthorized` — missing or invalid bearer token.
- `403 Forbidden` — missing `privacy:withdraw` scope, or subject belongs to another site.
- `404 Not Found` — unknown subject (not active and not previously withdrawn at this site).
- `409 Conflict` —
  - **Regulatory Hold** (`scope = "full"` only): Subject is under an active FDA inspection or safety hold. Respond with:
    ```json
    {
      "error": "blocked_regulatory_hold",
      "status": "blocked_regulatory_hold",
      "detail": "<reason>"
    }
    ```
    No records or downstream systems may be modified.
  - **Idempotency Conflict**: `Idempotency-Key` was previously used by this site for a different subject. Respond with:
    ```json
    {
      "error": "idempotency_key_conflict",
      "detail": "Idempotency-Key already bound to a different subject"
    }
    ```
- `202 Accepted` — Withdrawal request accepted.
  - Replaying the same `Idempotency-Key` for the same subject returns `202` with the exact same `request_id` (the request originally bound to that key, regardless of the replayed body).
  - Submitting another withdrawal request for a subject that already has a withdrawal request (pending, in progress, or completed) returns `202` with the existing `request_id` without starting a competing workflow, subject to the escalation rule in [Withdrawal Scope](#withdrawal-scope).
  - A newly created request MUST return `"status": "pending"` and `"completed_at": null`. Convergence across TrialVault's systems happens asynchronously through the background worker (`python -m trialvault.worker --once`).

**Response Shape (`202 Accepted`)**
```json
{
  "request_id": "wd-...",
  "site_id": "SITE-BOS-01",
  "status": "pending",
  "scope": "full",
  "anon_subject_ref": "ANON-...",
  "effective_date": "2026-03-10",
  "created_at": "2026-03-10T12:00:00+00:00",
  "completed_at": null,
  "error": null
}
```
The response must never expose the original plaintext `subject_id` or `mrn` once withdrawn, and `anon_subject_ref` must be an opaque, unlinkable identifier containing no substring of the original `subject_id` or `mrn`. For `"follow_up_only"` requests `anon_subject_ref` is `null`, because the participant is not de-identified.

### 2. `GET /v1/withdrawal-requests/{request_id}`

Returns the status of a withdrawal request using the same JSON shape as `POST`.
- Requires `privacy:withdraw` scope and same-site ownership (`403` otherwise, `404` if unknown).
- `status` is one of `"pending"`, `"in_progress"`, `"completed"`, `"failed"`.
- `completed_at` MUST remain `null` until `status == "completed"`.

## Withdrawal Scope

ICH E6(R3) distinguishes a participant who withdraws from all further study contact from one who withdraws consent for continued use of their identifiable data.

- **`"full"`** (default): consent withdrawal with HIPAA Safe Harbor de-identification, as described throughout this document and `docs/hipaa-and-21cfr11-retention.md`.
- **`"follow_up_only"`**: the participant stops all study contact but keeps consent for the use of identifiable data already collected and for continued central-lab results. Nothing is de-identified, and regulatory holds do not block it. The request may only reach `"completed"` once:
  - every pending ePRO notification for the subject is `cancelled`;
  - every scheduled contact job (`job_type` starting with `epro.`) for the subject is cancelled and can never execute, including jobs or contact events replayed later;
  - the subject's entries are removed from the outbound spool and the dead-letter queue;
  - `GET /v1/subjects/{subject_id}` returns the unchanged record with `"status": "follow_up_withdrawn"`.

  Once a `follow_up_only` request exists, `POST /v1/subjects/{subject_id}/epro-reminders` returns `409 Conflict` with `{"error": "contact_withdrawn"}`. Central-lab webhooks for the subject keep returning `201 Created` with normal ingestion, and search continues to find the subject.
- **Escalation**: a `"full"` request for a subject whose only existing request is `"follow_up_only"` creates a new full request (new `request_id`, `202`, `"status": "pending"`). The earlier follow-up request is left as is. Once a full request exists, any further request for that subject (either scope) returns the full request. A repeated `"follow_up_only"` request returns the existing follow-up request.

## Post-Withdrawal Subject Reads & Central-Lab Webhooks

### Subject Read Behavior
- After withdrawal converges, `GET /v1/subjects/{original_subject_id}` must return `404 Not Found`.
- `GET /v1/subjects/{anon_subject_ref}` (by an authorized token for the same site) must return `200 OK` with the de-identified clinical record (`status = "withdrawn"`, visits, adverse events, and labs intact, zero PHI).

### Central-Lab Webhooks (`POST /v1/webhooks/central-lab`)
Central labs can deliver results asynchronously after a participant withdraws consent:
- **Post-withdrawal collection (`collected_date > effective_date`)**: Must be suppressed. Return `202 Accepted` with `{"status": "suppressed", "accession_no": "<accession_no>"}`. Write no clinical rows, blobs, inbox PHI, search entries, cache entries, or outbox events.
- **Pre-withdrawal collection (`collected_date <= effective_date`)**: Under FDA GCP data-retention guidance, specimens collected *before* consent withdrawal remain part of the clinical record. Return `201 Created` with `{"status": "ingested_deidentified", "accession_no": "<accession_no>", "anon_subject_ref": "<anon_subject_ref>"}`, persist the numeric lab result and de-identified lab report under `anon_subject_ref` with `collected_date = null` and zero PHI anywhere (including inbox, audit details, cache, search, and report metadata), and ensure `GET /v1/trials/{trial_id}/safety-summary` reflects the new measurement.
- **Active subjects**: Continue to return `201 Created` with normal ingestion.
- **Shipment manifests**: Webhooks may carry a `shipment_id`. Each specimen is also appended to the multi-participant shipment manifest at `labs/{site_id}/shipments/{shipment_id}.json` in the blob store. A pre-withdrawal specimen ingested de-identified is appended under `anon_subject_ref` with no PHI.
