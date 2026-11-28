#!/usr/bin/env bash
set -euo pipefail

SOLUTION_VARIANT="${SOLUTION:-1}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET_DIR="${APP_DIR:-/app}"

case "$SOLUTION_VARIANT" in
  1)
    echo "Applying SOLUTION=1 (Transactional Outbox Event Processor)"
    cp "$SCRIPT_DIR/sol1/pv_outbox.py" "$TARGET_DIR/trialvault_pv/pv_outbox.py"
    cp "$SCRIPT_DIR/sol1/service.py" "$TARGET_DIR/trialvault_pv/service.py"
    cp "$SCRIPT_DIR/sol1/api.py" "$TARGET_DIR/trialvault_pv/api.py"
    cp "$SCRIPT_DIR/sol1/worker.py" "$TARGET_DIR/trialvault_pv/worker.py"
    ;;
  2)
    echo "Applying SOLUTION=2 (Declarative Case Reducer + Lease-Based Submission State Machine)"
    cp "$SCRIPT_DIR/sol2/case_reducer.py" "$TARGET_DIR/trialvault_pv/case_reducer.py"
    cp "$SCRIPT_DIR/sol2/submission_lease.py" "$TARGET_DIR/trialvault_pv/submission_lease.py"
    cp "$SCRIPT_DIR/sol2/service.py" "$TARGET_DIR/trialvault_pv/service.py"
    cp "$SCRIPT_DIR/sol2/api.py" "$TARGET_DIR/trialvault_pv/api.py"
    cp "$SCRIPT_DIR/sol2/worker.py" "$TARGET_DIR/trialvault_pv/worker.py"
    ;;
  *)
    echo "Unsupported SOLUTION=$SOLUTION_VARIANT (expected 1 or 2)" >&2
    exit 1
    ;;
esac
