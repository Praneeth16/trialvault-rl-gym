# TrialVault PV — Expedited SAE & IND Safety Reporting API Contract

This document defines the HTTP contract for E2B individual case safety report (ICSR) intake, blinded site case views, unblinded sponsor pharmacovigilance (PV) assessments, and IND safety submissions in TrialVault PV. Existing baseline endpoints, authentication rules, and response shapes must remain backwards compatible.

---

## Authentication & Authorization

All `/v1/*` endpoints require a bearer token (`GET /health` is unauthenticated):

```
Authorization: Bearer <api_token>
```

Token scopes:
- `site:safety` — clinical investigative site coordinator / safety reporter, bound to a single `site_id`. Blinded to randomized treatment assignments and SUSAR submission outcomes.
- `pv:unblinded` — sponsor pharmacovigilance officer with cross-site trial visibility and unblinded SUSAR/DSMB access.

Authorization status codes:
- `401 Unauthorized` — missing, malformed, unknown, or inactive bearer token.
- `403 Forbidden` — valid token lacking the required scope for the endpoint, or a `site:safety` caller attempting to report on or read a subject, case, site listing, or trial belonging to another site.
- `404 Not Found` — unknown `subject_id`, `case_uid`, `site_id`, or `trial_id`.

---

## Endpoints to Implement

### 1. `POST /v1/safety-reports/e2b`

Ingests an initial (`followup_num = 0`) or follow-up (`followup_num >= 1`) ICH E2B individual case safety report from a clinical investigative site.

**Headers**
- `Authorization: Bearer <token>` (required; must hold `site:safety` scope for the subject's enrolled `site_id`)
- `Idempotency-Key: <string>` (optional)

**Request Body (JSON)**
```json
{
  "message_id": "MSG-2026-0001",
  "case_uid": "CASE-BOS-001",
  "subject_id": "SUBJ-BOS-1001",
  "followup_num": 0,
  "aware_at": "2026-03-01T10:00:00+00:00",
  "meddra_pt": "Hepatic failure",
  "ctcae_grade": 4,
  "seriousness": {
    "death": false,
    "life_threatening": false,
    "hospitalization": true,
    "disability": false,
    "congenital_anomaly": false,
    "medically_important": false
  },
  "investigator_causality": "related",
  "sponsor_causality": null,
  "narrative": "Grade 4 acute hepatic failure requiring ICU admission."
}
```

Field rules:
- `followup_num`: integer `>= 0` (defaults to `0` if omitted).
- `aware_at`: ISO-8601 UTC timestamp indicating sponsor/site regulatory awareness of this report.
- `ctcae_grade`: integer `1..5` (CTCAE v5 severity grade).
- `seriousness`: optional object (defaults to all `false` if omitted) with the six boolean ICH E2B seriousness flags (`death`, `life_threatening`, `hospitalization`, `disability`, `congenital_anomaly`, `medically_important`; any omitted boolean key defaults to `false`).
- `investigator_causality`: one of `"related"`, `"possibly_related"`, `"unrelated"`.
- `sponsor_causality`: optional (`null` by default), or one of `"related"`, `"possibly_related"`, `"unrelated"`.
- `narrative`: non-empty clinical case narrative string.

**Status Codes, Idempotency & Conflict Rules**
- `401 Unauthorized` — missing or invalid bearer token.
- `403 Forbidden` — caller lacks `site:safety` scope, or `subject_id` belongs to a different site than the caller's `site_id`.
- `404 Not Found` — unknown `subject_id`.
- `409 Conflict` —
  - **Idempotency Key Conflict**: `Idempotency-Key` was previously used by this site for a different `case_uid`:
    ```json
    {
      "error": "idempotency_key_conflict",
      "detail": "Idempotency-Key already bound to a different case_uid"
    }
    ```
  - **Duplicate Follow-Up / Message Conflict**: `(case_uid, followup_num)` was already recorded with a *different* `message_id` (or `message_id` was already used for a different `(case_uid, followup_num)`, or `case_uid` is already bound to a different `subject_id`):
    ```json
    {
      "error": "duplicate_followup_conflict",
      "detail": "Conflicting report for (case_uid, followup_num) or message_id"
    }
    ```
- `202 Accepted` —
  - Replaying the same `Idempotency-Key` with the same `case_uid` returns `202 Accepted` with the **originally bound receipt** without creating duplicate reports or duplicate audit entries.
  - Replaying an existing `(case_uid, followup_num)` with the **exact same `message_id`** returns `202 Accepted` idempotently with the original receipt without creating duplicate reports or duplicate audit entries.
  - A newly ingested report returns `202 Accepted` synchronously with `"site_status": "received"` and logs an `E2B_REPORT_RECEIVED` Part 11 audit event. Treatment-arm unblinding, SUSAR triage, projection updates, and FDA submission NEVER run synchronously during `POST`; asynchronous convergence happens in `python -m trialvault_pv.worker --once`.

**Response Shape (`202 Accepted`)**
```json
{
  "case_uid": "CASE-BOS-001",
  "message_id": "MSG-2026-0001",
  "followup_num": 0,
  "site_status": "received",
  "aware_at": "2026-03-01T10:00:00+00:00"
}
```

---

### 2. `GET /v1/safety-cases/{case_uid}`

Returns the **strictly blinded** cumulative case view for clinical sites and PV readers.

**Authorization**
- Allowed scopes: `site:safety` (must match the case's `site_id`, else `403`) or `pv:unblinded`.
- Unknown `case_uid` → `404 Not Found`.

**Cumulative Case Merge Rules (Including Out-of-Order Follow-Ups)**
Follow-up reports may arrive out of sequence (for example, `followup_num = 2` arriving before `followup_num = 1`). Across all received reports for `case_uid`:
- `site_status`: `"received"` while any received report for `case_uid` has not yet been processed by the background worker; `"assessed"` once all received reports for `case_uid` have been processed by the worker. `site_status` MUST NEVER reveal treatment arm or whether a SUSAR was submitted or closed as non-investigational.
- `latest_followup_num`: `max(followup_num)` across all received reports.
- `meddra_pt`: `meddra_pt` from the report with `latest_followup_num`.
- `narrative`: `narrative` from the report with `latest_followup_num` (so a delayed lower `followup_num` never overwrites a newer narrative).
- `investigator_causality`: `investigator_causality` from the report with `latest_followup_num`.
- `sponsor_causality`: `sponsor_causality` from the highest-`followup_num` report where `sponsor_causality` is non-null (`null` if all received reports have `sponsor_causality == null`).
- `ctcae_grade`: `max(ctcae_grade)` across all received reports.
- `seriousness`: boolean OR across all received reports for each of the 6 seriousness flags (`death`, `life_threatening`, `hospitalization`, `disability`, `congenital_anomaly`, `medically_important`).
- `is_serious`: `true` if any flag in `seriousness` is `true`, else `false`.

**Response Shape (`200 OK`)**
```json
{
  "case_uid": "CASE-BOS-001",
  "subject_id": "SUBJ-BOS-1001",
  "site_id": "SITE-BOS-01",
  "trial_id": "TRIAL-ONC-402",
  "site_status": "assessed",
  "latest_followup_num": 1,
  "meddra_pt": "Hepatic failure",
  "ctcae_grade": 4,
  "is_serious": true,
  "seriousness": {
    "death": false,
    "life_threatening": false,
    "hospitalization": true,
    "disability": false,
    "congenital_anomaly": false,
    "medically_important": false
  },
  "investigator_causality": "related",
  "sponsor_causality": null,
  "narrative": "Updated Grade 4 hepatic failure narrative."
}
```
This endpoint must never include `treatment_arm`, drug/placebo strings, or SUSAR disposition fields.

---

### 3. `GET /v1/pv/cases/{case_uid}`

Returns the **unblinded sponsor pharmacovigilance assessment** and regulatory reporting state for `case_uid`.

**Authorization**
- Requires `pv:unblinded` scope ONLY (`403 Forbidden` for `site:safety`).
- Unknown `case_uid` → `404 Not Found`.

**PV Status Values (`pv_status`)**
- `"pending_assessment"` — case has received report(s) not yet evaluated by the background worker (prior to initial assessment).
- `"not_susar"` — evaluated and failed at least one of the three SUSAR legs (`is_serious`, `is_suspected`, `is_unexpected`). Never unblinded (`unblinded = false`, `treatment_arm = null`).
- `"closed_non_investigational"` — satisfied all three SUSAR legs, was selectively unblinded, and the participant's randomized arm is non-investigational (`is_investigational_drug == 0`, e.g., placebo/control). No FDA IND safety submission is generated (`report_clock = null`, `day0_at = null`, `due_at = null`, `submissions = []`).
- `"susar_pending_submission"` — investigational-drug SUSAR with a queued submission in `"pending"` state.
- `"susar_retry_pending"` — investigational-drug SUSAR whose active submission encountered a gateway NACK or transient backing-store/audit fault and is in `"retry_pending"` state awaiting worker retry.
- `"susar_submitted"` — investigational-drug SUSAR whose required IND safety submission(s) have all been acknowledged by the FDA gateway (`"submitted"`).

Field semantics:
- `is_serious`, `is_suspected`, `is_unexpected`: booleans computed per `docs/fda-21cfr312-and-blinding-policy.md`.
- `unblinded`: `true` iff emergency treatment-assignment unblinding has been executed for this case; otherwise `false`.
- `treatment_arm`: the participant's randomized arm string when `unblinded == true`; MUST be `null` whenever `unblinded == false`.
- `report_clock`: `"7_day"` or `"15_day"` from the latest IND submission for investigational-drug SUSARs; `null` otherwise.
- `day0_at`, `due_at`: ISO-8601 UTC timestamps from the latest IND submission for investigational-drug SUSARs; `null` otherwise.
- `latest_followup_num`: `max(followup_num)` across received reports.
- `submissions`: list of IND submission summary objects for this `case_uid`, ordered by `sequence_num` ascending (same schema as `GET /v1/pv/submissions` items).

**Response Shape (`200 OK`)**
```json
{
  "case_uid": "CASE-BOS-001",
  "subject_id": "SUBJ-BOS-1001",
  "trial_id": "TRIAL-ONC-402",
  "site_id": "SITE-BOS-01",
  "pv_status": "susar_submitted",
  "is_serious": true,
  "is_suspected": true,
  "is_unexpected": true,
  "unblinded": true,
  "treatment_arm": "DRUG_NVX_200MG",
  "report_clock": "15_day",
  "day0_at": "2026-03-01T10:00:00+00:00",
  "due_at": "2026-03-16T10:00:00+00:00",
  "latest_followup_num": 0,
  "submissions": [
    {
      "submission_id": "SUB-001",
      "case_uid": "CASE-BOS-001",
      "trial_id": "TRIAL-ONC-402",
      "submission_type": "initial",
      "parent_submission_id": null,
      "sequence_num": 1,
      "report_clock": "15_day",
      "day0_at": "2026-03-01T10:00:00+00:00",
      "due_at": "2026-03-16T10:00:00+00:00",
      "status": "submitted",
      "gateway_ack_id": "ESG-ACK-9F2A1C",
      "submitted_at": "2026-03-01T10:05:00+00:00",
      "last_error": null
    }
  ]
}
```

---

### 4. `GET /v1/pv/submissions`

Lists IND Safety Submissions ordered by `(case_uid ASC, sequence_num ASC)` (or `sequence_num ASC` when filtered to a single `case_uid`).

**Authorization & Query Parameters**
- Requires `pv:unblinded` scope ONLY (`403 Forbidden` for `site:safety`).
- Optional query parameters:
  - `trial_id`: filter submissions to a specific trial.
  - `case_uid`: filter submissions to a specific safety case.

Submission lifecycle fields:
- `submission_id`: unique identifier assigned when the submission obligation is created; remains stable across retries until `"submitted"`.
- `submission_type`: `"initial"` (`sequence_num == 1`, `parent_submission_id == null`) or `"followup"` (`sequence_num == prev + 1`, `parent_submission_id == <previous_submission_id>`).
- `sequence_num`: 1-based monotonic integer per `case_uid`.
- `report_clock`: `"7_day"` or `"15_day"`.
- `day0_at`: ISO-8601 UTC regulatory Day-0 timestamp for this submission.
- `due_at`: ISO-8601 UTC deadline timestamp for this submission.
- `status`: `"pending"` | `"retry_pending"` | `"submitted"`.
- `gateway_ack_id`: FDA gateway acknowledgment ID once `"submitted"`, else `null`.
- `submitted_at`: ISO-8601 UTC timestamp once `"submitted"`, else `null`.
- `last_error`: error message/code while in `"retry_pending"`, cleared to `null` once `"submitted"`.

**Response Shape (`200 OK`)**
```json
{
  "submissions": [
    {
      "submission_id": "SUB-001",
      "case_uid": "CASE-BOS-001",
      "trial_id": "TRIAL-ONC-402",
      "submission_type": "initial",
      "parent_submission_id": null,
      "sequence_num": 1,
      "report_clock": "15_day",
      "day0_at": "2026-03-01T10:00:00+00:00",
      "due_at": "2026-03-16T10:00:00+00:00",
      "status": "submitted",
      "gateway_ack_id": "ESG-ACK-9F2A1C",
      "submitted_at": "2026-03-01T10:05:00+00:00",
      "last_error": null
    }
  ]
}
```

---

## Existing Endpoints & Projections That Must Stay Synchronized

After `python -m trialvault_pv.worker --once` processes incoming E2B reports:
1. **`GET /v1/safety-cases/search?q=<term>`** (`site:safety` scoped to own `site_id`, or `pv:unblinded`):
   - Must index newly ingested/updated E2B safety cases (case UID, subject ID, MedDRA PT, blinded narrative) so site coordinators can search them, while never indexing or exposing treatment arms or unblinded SUSAR statuses.
2. **`GET /v1/sites/{site_id}/blinded-line-listing`** (`site:safety` for own `site_id`, or `pv:unblinded`):
   - Must include all safety cases for `site_id` with updated `site_status`, `latest_followup_num`, `meddra_pt`, `ctcae_grade`, `is_serious`, `investigator_causality`, valid SHA-256 sidecar (`"checksum_valid": true`), and zero treatment arm or SUSAR disposition leak.
3. **`GET /v1/pv/trials/{trial_id}/dsmb-line-listing`** (`pv:unblinded` ONLY):
   - Must list all unblinded investigational-drug SUSAR cases for `trial_id` (excluding `not_susar` and `closed_non_investigational` placebo/control cases) with `treatment_arm`, `pv_status`, `report_clock`, `day0_at`, `due_at`, `latest_submission_status`, and valid SHA-256 sidecar (`"checksum_valid": true`).
4. **`GET /v1/trials/{trial_id}/blinded-summary`** (`site:safety` in `trial_id`, or `pv:unblinded`):
   - Must reflect accurate total enrolled subjects, total safety cases, and total serious safety cases (`is_serious == true`) without exposing per-arm counts or treatment assignments.
