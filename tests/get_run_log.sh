#!/bin/bash
# ─────────────────────────────────────────────────────────
# get_run_log.sh — Fetch run output/error for a run_id
#                  and save to tests/logs/
#
# Usage:
#   bash tests/get_run_log.sh <run_id>
#
# Example:
#   bash tests/get_run_log.sh 256539215790668
# ─────────────────────────────────────────────────────────

set -e

RUN_ID="${1:?Usage: $0 <run_id>}"
HOST="https://adb-7405612978007880.0.azuredatabricks.net"
LOG_DIR="$(dirname "$0")/logs"
mkdir -p "$LOG_DIR"

# Get token
TOKEN=$(az account get-access-token \
  --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --query accessToken -o tsv)

# Get run status
STATUS=$(curl -s "${HOST}/api/2.1/jobs/runs/get?run_id=${RUN_ID}" \
  -H "Authorization: Bearer $TOKEN")

# Get run output (error trace)
OUTPUT=$(curl -s "${HOST}/api/2.1/jobs/runs/get-output?run_id=${RUN_ID}" \
  -H "Authorization: Bearer $TOKEN")

# Parse result state
RESULT=$(echo "$STATUS" | python3 -c "
import sys,json
d=json.load(sys.stdin)
s=d.get('state',{})
print(s.get('result_state') or s.get('life_cycle_state','UNKNOWN'))
")

RUN_URL="${HOST}/#job/runs/${RUN_ID}"
TS=$(date +%Y%m%d_%H%M%S)
LOG_PATH="${LOG_DIR}/run_${TS}_${RUN_ID}_${RESULT}.log"

# Write log
{
  echo "════════════════════════════════════════════════════"
  echo "  Run ID  : $RUN_ID"
  echo "  Result  : $RESULT"
  echo "  Run URL : $RUN_URL"
  echo "  Fetched : $(date)"
  echo "════════════════════════════════════════════════════"
  echo ""
  echo "STATUS:"
  echo "$STATUS" | python3 -m json.tool
  echo ""
  echo "OUTPUT / ERROR:"
  echo "$OUTPUT" | python3 -m json.tool
} | tee "$LOG_PATH"

echo ""
echo "Log saved: $LOG_PATH"
