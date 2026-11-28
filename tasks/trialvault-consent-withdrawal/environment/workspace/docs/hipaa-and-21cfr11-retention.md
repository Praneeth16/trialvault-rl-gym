# TrialVault HIPAA Safe Harbor & FDA 21 CFR Part 11 Retention Policy

Participant consent withdrawal in TrialVault must simultaneously satisfy **HIPAA Safe Harbor de-identification (45 CFR §164.514(b)(2))** and **FDA Good Clinical Practice (GCP) / 21 CFR Part 11** electronic record integrity.

---

## 1. HIPAA Safe Harbor De-Identification

When a participant withdraws consent (`scope = "full"`), all direct and quasi-identifying Protected Health Information (PHI) for that individual must be removed or generalized across **every persistence layer, analytical/tabulation store, index, cache, stored document/manifest (and its metadata), site or trial log/export, inbound archive, and asynchronous queue or spool** maintained by TrialVault.

### 1.1 Direct Identifiers
Remove or null all direct identifiers across structured columns/attributes, supplemental qualifier rows, free-text narratives, JSON payloads, document bodies, and metadata:
- Personal names: the full name, each individual name token, every recorded alias in the subject's `aliases` (nicknames, maiden or prior names, and their tokens), and initials derived from the full name or a multi-word alias written in dotted form (for example `N.K.`), wherever they appear in free-text narratives
- Contact details (email, phone, emergency contact name/phone)
- Government and institutional identifiers (SSN, MRN)
- Geographic subdivisions smaller than a state (street address, city)
- Device serial numbers, IP addresses, and patient portal URLs
- Original `subject_id` (Safe Harbor unique identifying code — must be replaced by an unlinkable opaque `anon_subject_ref` everywhere in retained records, and signed consent artifacts identifying the participant must not remain stored)

Free-text redaction removes the identifying spans only; the surrounding clinical narrative is part of the retained clinical record and must be kept.

### 1.2 Geographic Generalization (ZIP3 Rule)
- Street address and city must be removed; state may be retained.
- Postal codes must be generalized to their first 3 digits (`ZIP3`), **except** for the 17 HHS/Census low-population 3-digit prefixes (<= 20,000 residents), which must be replaced with `"000"`:
  ```
  036  059  063  102  203  556  692  790  821  823
  830  831  878  879  884  890  893
  ```

### 1.3 Dates and Ages Over 89
- Exact calendar dates directly related to the participant (randomization/reference start dates, visit dates, adverse-event onset dates, specimen collection dates, verification/recording dates, consent signature dates) must be removed while retaining relative study-day offsets.
- For participants aged 89 or younger at enrollment: generalize date of birth to the 4-digit birth year (`YYYY`), keep enrollment age, and record the age string in the age bucket.
- For participants older than 89 at enrollment: aggregate age into `"90+"` in the age bucket, and remove both exact enrollment age and date of birth (including birth year).

### 1.4 Multi-Participant Artifacts & Signed Logs
- Some TrialVault artifacts record multiple participants together (for example, shared laboratory shipment manifests, trial-wide interim snapshots, and site-level enrollment or monitoring logs). Only the withdrawn participant's entries and subject-reference lists may be de-identified (`anon_subject_ref` with PHI removed and Safe Harbor quasi-identifiers generalized). Entries and metadata for other participants in the same artifact must keep every field value unchanged.
- Whenever a retained artifact carries a SHA-256 checksum or manifest record, its checksum must be updated atomically so integrity verification continues to pass.

### 1.5 Minimal Privacy Tracking & Suppression
- Minimal privacy request and suppression records may store **only cryptographic SHA-256 hashes** (never plaintext) of the original `subject_id` and `mrn` once withdrawal completes, to enforce idempotency, route pre-withdrawal late specimens, and suppress post-withdrawal feeds, delayed jobs, and replayed events.

---

## 2. FDA GCP & 21 CFR Part 11 Clinical Retention

### 2.1 Preserved Scientific, Safety & Study-Tabulation Data
- Deleting clinical or study-tabulation records on withdrawal is prohibited under FDA GCP. Randomized treatment arm, study visit vitals and study days, adverse event MedDRA terms / CTCAE grades / seriousness / relatedness, coded medical terminology records, monitor query resolution states, CDISC domain tabulations, and central-lab numeric biomarker values collected on or before `effective_date` must remain intact under `anon_subject_ref`.
- Trial-level safety summaries and study-tabulation summaries must preserve identical subject, arm, visit, adverse-event, and lab counts/means after withdrawal (and incorporate any late-arriving assay collected on or before `effective_date`).

### 2.2 21 CFR Part 11 Immutable Audit Trail
- Existing entries in the cryptographic hash-chained regulatory audit ledger must never be modified or deleted; full chain verification must succeed before and after withdrawal.
- Separable participant PHI attached to audit entries must be purged.
- A new hash-chained `CONSENT_WITHDRAWN` audit entry referencing only `anon_subject_ref` (zero plaintext PHI) must be appended upon completion.

### 2.3 Regulatory Hold Precedence
- Participants under an active FDA inspection or safety hold cannot be de-identified while the hold is active. Full withdrawal requests for held participants must be rejected (`409 Conflict`) with zero side effects.

---

## 3. Operational Guarantees

- **Convergence before completion**: A withdrawal request may transition to `"completed"` (with non-null `completed_at`) only after every store, warehouse, log, export, archive, queue, dead-letter file, and scheduled job has converged and the withdrawal audit entry has been recorded.
- **Delayed work & replay resilience**: Scheduled future jobs and replayed historical events referencing a withdrawn participant must never reintroduce PHI into any store, warehouse, index, cache, log, export, or outbound queue.
- **Concurrent workers**: Multiple `python -m trialvault.worker --once` processes may run simultaneously. Each withdrawal request must take effect at most once across all stores and the regulatory audit ledger: exactly one `CONSENT_WITHDRAWN` audit entry per request, a valid hash chain, and no regression from `"completed"`.
- **Fault recovery across independent stores**: Transient filesystem or database errors at any stage of execution (including late-stage export or audit-ledger failures after earlier stores have been updated) must prevent premature `"completed"` status; a subsequent worker retry after the fault clears must converge cleanly to `"completed"` with zero orphaned PHI and intact clinical counts.
- **Isolation & survivors**: Multi-site tenant boundaries must be strictly enforced, and active participants—including co-enrolled household members sharing an address, postal code, emergency contact, or investigator—must remain completely unaffected.
