# TrialVault HIPAA Safe Harbor & FDA 21 CFR Part 11 Retention Policy

Participant consent withdrawal in TrialVault must simultaneously satisfy **HIPAA Safe Harbor de-identification (45 CFR §164.514(b)(2))** and **FDA Good Clinical Practice (GCP) / 21 CFR Part 11** electronic record integrity.

---

## 1. HIPAA Safe Harbor De-Identification

When a participant withdraws consent, all direct and quasi-identifying Protected Health Information (PHI) for that individual must be removed or generalized across **every active database, projection, search index, cache, document store, interim export snapshot, webhook archive, notification queue, dead-letter queue, and scheduled job**.

### 1.1 Direct Identifiers
Remove or null all direct identifiers across structured fields, free-text narratives, JSON payloads, document bodies, and document metadata:
- Personal names (full name and individual name tokens in free-text narratives)
- Contact details (email, phone, emergency contact name/phone)
- Government and institutional identifiers (SSN, MRN)
- Geographic subdivisions smaller than a state (street address, city)
- Device serial numbers, IP addresses, and patient portal URLs
- Original `subject_id` (Safe Harbor unique identifying code — must be replaced by an unlinkable opaque `anon_subject_ref` everywhere in retained records)

### 1.2 Geographic Generalization (ZIP3 Rule)
- Street address and city must be removed; state may be retained.
- Postal codes must be generalized to their first 3 digits (`ZIP3`), **except** for the 17 HHS/Census low-population 3-digit prefixes (<= 20,000 residents), which must be replaced with `"000"`:
  ```
  036  059  063  102  203  556  692  790  821  823
  830  831  878  879  884  890  893
  ```

### 1.3 Dates and Ages Over 89
- Exact calendar dates directly related to the participant (randomization date, visit dates, adverse-event onset dates, specimen collection dates, consent signature dates) must be removed while retaining relative study-day offsets (`days_from_randomization`, `onset_study_day`, `study_day`).
- For participants aged 89 or younger at enrollment: generalize date of birth to the 4-digit birth year (`YYYY`), keep enrollment age, and record the age string in the age bucket.
- For participants older than 89 at enrollment: aggregate age into `"90+"` in the age bucket, and remove both exact enrollment age and date of birth (including birth year).

### 1.4 Minimal Privacy Tracking & Suppression
- Minimal privacy request and suppression records may store **only cryptographic SHA-256 hashes** (never plaintext) of the original `subject_id` and `mrn` to enforce idempotency, route pre-withdrawal late specimens, and suppress post-withdrawal feeds, delayed jobs, and replayed events.

---

## 2. FDA GCP & 21 CFR Part 11 Clinical Retention

### 2.1 Preserved Scientific & Safety Data
- Deleting clinical records on withdrawal is prohibited under FDA GCP. Randomized treatment arm, study visit vitals and study days, adverse event MedDRA terms / CTCAE grades / seriousness / relatedness, coded medical terminology records, monitor query resolution states, and central-lab numeric biomarker values collected on or before the withdrawal effective date must remain intact under `anon_subject_ref`.
- Trial-level safety summaries must remain mathematically unchanged after withdrawal (and must incorporate any late-arriving assay from a specimen collected on or before `effective_date`).
- Existing interim Data Safety Monitoring Board (DSMB) export snapshots must retain the withdrawn participant's de-identified clinical data with zero PHI and with valid updated manifest SHA-256 checksums.

### 2.2 21 CFR Part 11 Immutable Audit Trail
- Existing entries in the cryptographic hash-chained audit trail must never be modified or deleted; full chain verification must succeed before and after withdrawal.
- Separable participant PHI attached to audit entries must be purged.
- A new hash-chained `CONSENT_WITHDRAWN` audit entry referencing only `anon_subject_ref` (zero plaintext PHI) must be appended upon completion.

### 2.3 Regulatory Hold Precedence
- Participants under an active FDA inspection or safety hold cannot be de-identified while the hold is active. Withdrawal requests for held participants must be rejected (`409 Conflict`) with zero side effects.

---

## 3. Operational Guarantees

- **Convergence before completion**: A withdrawal request may transition to `"completed"` (with non-null `completed_at`) only after every downstream store, export, archive, queue, dead-letter file, and scheduled job has been cleaned and the withdrawal audit entry has been recorded.
- **Delayed work & replay resilience**: Scheduled future jobs and replayed historical events referencing a withdrawn participant must not reintroduce PHI into any store, cache, index, export, or outbound queue.
- **Fault recovery**: Transient downstream store errors must prevent premature `"completed"` status while preserving clinical data consistency; a subsequent worker retry after the fault clears must converge cleanly to `"completed"`.
- **Isolation & survivors**: Multi-site tenant boundaries must be strictly enforced, and active participants—including co-enrolled household members sharing an address, postal code, emergency contact, or investigator—must remain completely unaffected.
