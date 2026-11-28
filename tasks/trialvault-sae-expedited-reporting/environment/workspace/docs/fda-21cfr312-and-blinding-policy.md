# FDA 21 CFR §312.32 Expedited Reporting, ICH E9 Blinding & 21 CFR Part 11 Policy

TrialVault PV must simultaneously comply with **FDA 21 CFR §312.32** (IND Safety Reporting), **ICH E9 / E2A** (Selective Unblinding and Study Blind Preservation), and **FDA 21 CFR Part 11** (Electronic Records Integrity).

---

## 1. Three-Legged SUSAR Triage (21 CFR §312.32(c)(1)(i))

An individual case safety report qualifies as a Suspected Unexpected Serious Adverse Reaction (**SUSAR**) candidate if and only if **all three** legs hold on the cumulative case state:

### 1.1 Serious (`is_serious`)
`is_serious` is `true` iff at least one of the six ICH E2B seriousness criteria (`death`, `life_threatening`, `hospitalization`, `disability`, `congenital_anomaly`, `medically_important`) is `true` across received reports for the case.

### 1.2 Suspected Causality (`is_suspected` — Composite OR Rule)
Under FDA 21 CFR §312.32(c)(1)(i) and FDA's Final Guidance on Safety Reporting Requirements for INDs, an adverse event is suspected if **either** the clinical investigator **or** the sponsor believes there is a reasonable possibility that the drug caused the event:
- `is_suspected` is `true` iff cumulative `investigator_causality` is in `{"related", "possibly_related"}` **OR** `sponsor_causality` is in `{"related", "possibly_related"}`.
- Even when `investigator_causality == "unrelated"`, a `sponsor_causality` of `"possibly_related"` or `"related"` makes `is_suspected = true`.
- Only when `investigator_causality == "unrelated"` and `sponsor_causality` is `null` or `"unrelated"` is `is_suspected = false`.

### 1.3 Unexpected Against Active RSI (`is_unexpected`)
Expectedness is evaluated against the Trial's **Reference Safety Information (RSI)** in the active Investigator's Brochure (`is_active == 1`) that was effective at the case's initial awareness timestamp (`effective_from <= initial_aware_at`, selecting the active version with the latest `effective_from` not after `initial_aware_at`, where `initial_aware_at` is the `aware_at` of `followup_num == 0`, or the earliest `aware_at` received for the case if `followup_num == 0` has not yet arrived):

`is_unexpected` is `true` if **any** of the following holds:
1. No active Investigator's Brochure was effective at `initial_aware_at`, or the case's `meddra_pt` is **not listed** in that IB version's RSI expected terms; **OR**
2. The case's converged `ctcae_grade > max_expected_grade` (severity exceeds the maximum expected CTCAE grade documented in the RSI — 21 CFR §312.32(a) greater-severity unexpectedness); **OR**
3. `seriousness.death` or `seriousness.life_threatening` is `true` while the RSI term has `fatal_expected == 0` (fatal or life-threatening outcome not expected in the RSI).

Otherwise, `is_unexpected` is `false`.

---

## 2. Selective IWRS Unblinding & Study Blind Wall (ICH E9)

### 2.1 Selective Unblinding Gate
Breaking the randomization blind unnecessarily compromises trial statistical validity under ICH E9:
- **Never unblind synchronously during `POST /v1/safety-reports/e2b`.**
- **Never query or unblind treatment assignment when `not (is_serious and is_suspected and is_unexpected)`.** Non-serious cases, double-unrelated cases, and RSI-expected cases must produce **zero** IWRS unblinding audit records and keep `unblinded = false` and `treatment_arm = null`.
- **Unblind at most once per qualifying case:** When a case first satisfies `is_serious and is_suspected and is_unexpected` in the background worker, look up its treatment assignment once via the IWRS unblinding facility (which records an immutable IWRS unblind audit entry) and persist the unblinded result inside restricted PV state. Subsequent follow-up reports or submission retries for that `case_uid` MUST reuse the persisted PV state and MUST NOT generate duplicate IWRS unblind audit rows.

### 2.2 Investigational Drug vs. Placebo / Non-Investigational Arm
After selective unblinding of a 3-leg SUSAR candidate:
- If `is_investigational_drug == 0` (e.g., `PLACEBO` or non-investigational control arm):
  - Under FDA IND safety guidance, placebo events generally do not satisfy causal attribution to the investigational product and are not submitted as expedited IND Safety Reports.
  - Transition the PV case to `pv_status = "closed_non_investigational"`, keep `report_clock = null`, `day0_at = null`, `due_at = null`, generate **zero** FDA IND submissions, and omit the case from the DSMB SUSAR listing.
- If `is_investigational_drug == 1`:
  - Proceed with expedited FDA IND safety reporting under Section 3.

### 2.3 Strict Blinding Wall Across Site Endpoints, Caches, Search & Listings
Site personnel must never be able to infer a participant's randomized arm—neither directly from a `treatment_arm` string nor indirectly by seeing whether a case became a submitted SUSAR vs. `closed_non_investigational`!
- `GET /v1/safety-cases/{case_uid}` transitions `"site_status"` solely from `"received"` to `"assessed"` once processed by the worker, identically for investigational-drug SUSARs, placebo candidates, and non-SUSARs.
- Blinded clinical database records, the full-text case search index, shared/blinded projection caches, `GET /v1/trials/{trial_id}/blinded-summary`, and site safety line listings (`GET /v1/sites/{site_id}/blinded-line-listing` and backing files) MUST NEVER contain randomized `treatment_arm` strings (such as `"PLACEBO"` or `"DRUG_..."`) or unblinded PV statuses (`"closed_non_investigational"`, `"susar_pending_submission"`, `"susar_retry_pending"`, `"susar_submitted"`).

---

## 3. Regulatory Reporting Clocks, Tightening & Follow-Ups (21 CFR §312.32(c)(1)–(2))

### 3.1 Chronological Day-0 Threshold Anchoring
Reports for a case must be evaluated chronologically by `(aware_at, followup_num)` to determine the exact report that **first** caused all three legs (`is_serious and is_suspected and is_unexpected`) to become `true`:
- Let `R_threshold` be the earliest chronological prefix report at which cumulative `is_serious and is_suspected and is_unexpected` first becomes `true`, with `day0_at = R_threshold.aware_at`.
  - *Example (Delayed Qualification)*: Report `followup_num = 0` at `T0` is Grade 2 non-serious or RSI-expected (`not_susar`). Later, `followup_num = 1` at `T1` escalates to Grade 4 hospitalized unexpected. Whether processed across two worker runs or in the same worker batch, `day0_at` MUST be `T1`, **not** `T0`.
- Initial clock at `R_threshold`:
  - **7-Day Fatal / Life-Threatening Clock (`"7_day"`)**: If cumulative `death` or `life_threatening` is `true` at `R_threshold`, `report_clock = "7_day"` and `due_at = day0_at + 7 calendar days` (`+ 168 hours`, exact UTC timestamp).
  - **15-Day Serious Clock (`"15_day"`)**: Otherwise, `report_clock = "15_day"` and `due_at = day0_at + 15 calendar days` (`+ 360 hours`, exact UTC timestamp).

### 3.2 Pre-Submission 15-Day → 7-Day Retroactive Clock Tightening
If a case has an **unsubmitted** initial submission (`status` in `{"pending", "retry_pending"}`) currently on a `"15_day"` clock (`due_at = existing_due_at`), and a subsequent report with `aware_at = T_up` adds `death = true` or `life_threatening = true` **before** that initial submission reaches `"submitted"`:
- Upgrade the existing initial submission in place (`sequence_num` remains `1`, `submission_id` and `day0_at` remain unchanged):
  - `report_clock = "7_day"`
  - `due_at = min(existing_due_at, T_up + 7 calendar days)` (exact UTC instant comparison).

### 3.3 Post-Submission Follow-Up IND Safety Reports vs. Benign Amendments
Once the latest submission for an investigational-drug SUSAR has reached `status == "submitted"`:
- **Material Safety Escalation (Spawns Follow-Up Submission)**: If a newly received report with `followup_num` higher than the follow-up number covered by the previous submitted report either:
  1. Increases cumulative `ctcae_grade` above the `ctcae_grade` already submitted, **OR**
  2. Sets any of the 6 seriousness flags to `true` that was `false` in the previous submitted report,
  then create a new IND Safety Submission:
  - `submission_type = "followup"`
  - `parent_submission_id = <previous_submission_id>`
  - `sequence_num = previous.sequence_num + 1`
  - `day0_at = T_fu` (the `aware_at` of the escalating follow-up report)
  - `report_clock = "7_day"` (`due_at = T_fu + 7 days`) if `death` or `life_threatening` is newly added or `true` on the case; otherwise `"15_day"` (`due_at = T_fu + 15 days`).
- **Benign Narrative Amendment (No Duplicate Submission)**: If a post-submission follow-up report only amends/clarifies narrative text (or repeats existing grade and seriousness flags without increasing `ctcae_grade` or adding any new `true` seriousness flag), update the blinded case narrative, `latest_followup_num`, search index, cache, and listings, and mark `site_status = "assessed"`, **without** creating a new FDA IND submission.

---

## 4. 21 CFR Part 11 Audit Trail, Gateway Resilience & Concurrency

### 4.1 Immutable Hash-Chained Audit Trail
- Existing entries in the 21 CFR Part 11 audit trail must never be modified or deleted; full SHA-256 chain verification from the genesis hash (`"0" * 64`) must succeed at all times.
- Required audit actions:
  - `E2B_REPORT_RECEIVED`: appended when `POST /v1/safety-reports/e2b` accepts a new report (not duplicated on idempotent replays).
  - `IND_SAFETY_SUBMITTED`: appended **if and only if** the FDA Electronic Submissions Gateway acknowledges the submission (`accepted == true`), recording `submission_id`, `case_uid`, `sequence_num`, and `gateway_ack_id` in the audited payload.

### 4.2 Fault Handling & Recovery
- If the FDA gateway rejects a transmission with a retryable NACK (e.g., `nack_retryable` / `ESG_503_UPSTREAM`), or an OS/filesystem fault occurs when writing gateway submission files, or the Part 11 audit database aborts when recording `IND_SAFETY_SUBMITTED`:
  - The submission MUST NOT be marked `"submitted"` (`status` must become/remain `"retry_pending"`, `pv_status = "susar_retry_pending"`, `submitted_at = null`, `last_error` populated).
  - No `IND_SAFETY_SUBMITTED` entry may remain committed for an unacknowledged or failed submission, and a fault on one case must not prevent other independent cases from converging.
  - Once the transient gateway, filesystem, or database fault clears, a subsequent `python -m trialvault_pv.worker --once` run must cleanly converge the same `submission_id` to `"submitted"` with valid line-listing checksums, no duplicate `sequence_num` submissions, and an intact Part 11 hash chain.

### 4.3 Multi-Worker Concurrency
- Overlapping `python -m trialvault_pv.worker --once` processes may run concurrently against the same data directory.
- Racing workers processing a pending SUSAR must coordinate atomically so that **at most one** IWRS unblinding audit entry, **at most one** initial FDA submission (`sequence_num = 1`), and **at most one** `IND_SAFETY_SUBMITTED` audit event are produced, while keeping the Part 11 hash chain intact.
