# TrialVault Clinical Trial EDC

TrialVault is a multi-site Phase III Clinical Trial Electronic Data Capture (EDC) platform built with FastAPI and file-backed stores under `TRIALVAULT_DATA_DIR`.

## Components

- `trialvault/api.py` — HTTP API for site enrollment, visits, adverse events, monitor queries, ePRO reminders, DSMB snapshots, central-lab webhooks, and safety summaries
- `trialvault/worker.py` — Background worker processing outbox events and due scheduled jobs (`python -m trialvault.worker --once`)
- `trialvault/audit.py` — FDA 21 CFR Part 11 SHA-256 hash-chained audit trail
- `trialvault/queries.py` — Clinical monitor data queries and MedDRA medical coding queue
- `trialvault/exports.py` — DSMB interim safety snapshot generator with SHA-256 manifest checksums
- `trialvault/search.py`, `trialvault/cache.py`, `trialvault/blobs.py`, `trialvault/spool.py` — Coordinator search, projection cache, document store, and notification spool

## Specifications

- `docs/withdrawal-api.md`
- `docs/hipaa-and-21cfr11-retention.md`
