#!/usr/bin/env bash
set -euo pipefail

SOLUTION_VARIANT="${SOLUTION:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${APP_DIR:-/app}"

case "$SOLUTION_VARIANT" in
  1)
    echo "Applying SOLUTION=1 (Transactional Outbox + Suppression Ledger)"
    cp "$SCRIPT_DIR/sol1/withdrawal.py" "$TARGET_DIR/trialvault/withdrawal.py"
    cp "$SCRIPT_DIR/sol1/service.py" "$TARGET_DIR/trialvault/service.py"
    cp "$SCRIPT_DIR/sol1/api.py" "$TARGET_DIR/trialvault/api.py"
    cp "$SCRIPT_DIR/sol1/worker.py" "$TARGET_DIR/trialvault/worker.py"
    ;;
  2)
    echo "Applying SOLUTION=2 (Checkpointed Saga State Machine)"
    cp "$SCRIPT_DIR/sol2/safeharbor.py" "$TARGET_DIR/trialvault/safeharbor.py"
    cp "$SCRIPT_DIR/sol2/saga_engine.py" "$TARGET_DIR/trialvault/saga_engine.py"
    cp "$SCRIPT_DIR/sol2/service.py" "$TARGET_DIR/trialvault/service.py"
    cp "$SCRIPT_DIR/sol2/api.py" "$TARGET_DIR/trialvault/api.py"
    cp "$SCRIPT_DIR/sol2/worker.py" "$TARGET_DIR/trialvault/worker.py"
    ;;
  *)
    echo "Unsupported SOLUTION=$SOLUTION_VARIANT (expected 1 or 2)" >&2
    exit 1
    ;;
esac
