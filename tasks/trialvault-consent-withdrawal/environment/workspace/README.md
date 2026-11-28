# TrialVault Clinical Trial EDC

TrialVault is a multi-site Phase III Clinical Trial Electronic Data Capture (EDC) backend built with FastAPI.

## Running Locally

```bash
# Run HTTP API
uvicorn trialvault.api:app --host 127.0.0.1 --port 8000

# Drain asynchronous outbox & due scheduled work once
python -m trialvault.worker --once

# Run baseline tests
pytest -v tests/test_baseline_edc.py
```

Store locations default to `/app/data` and can be overridden with `TRIALVAULT_DATA_DIR`.

## Specifications

- `docs/withdrawal-api.md`
- `docs/hipaa-and-21cfr11-retention.md`
