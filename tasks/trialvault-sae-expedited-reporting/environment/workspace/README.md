# TrialVault PV — Clinical Trial Pharmacovigilance & Safety Intake

TrialVault PV is a multi-site Phase III clinical trial safety intake and pharmacovigilance backend built with FastAPI.

## Running Locally

```bash
# Run HTTP API
uvicorn trialvault_pv.api:app --host 127.0.0.1 --port 8000

# Drain asynchronous safety & gateway work once
python -m trialvault_pv.worker --once

# Run baseline tests
pytest -v tests/test_baseline_pv.py
```

Store locations default to `/app/data` and can be overridden with `TRIALVAULT_PV_DATA_DIR`.

## Specifications

- `docs/sae-reporting-api.md`
- `docs/fda-21cfr312-and-blinding-policy.md`
