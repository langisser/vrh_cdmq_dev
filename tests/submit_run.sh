#!/bin/bash
# ─────────────────────────────────────────────────────────
# submit_run.sh — Submit a Databricks notebook run and
#                 print the Run URL immediately (no wait)
#
# Usage:
#   source setup_env.sh
#   bash tests/submit_run.sh <table_name> <data_date> [notebook_mode]
#
# Examples:
#   bash tests/submit_run.sh tc_case2_similarity 2015-12-28
#   bash tests/submit_run.sh SOURCE_MOTOR 2026-01-05 match
#
# notebook_mode:
#   main    (default) = vrh_chv_main — full pipeline
#   pre_vld           = vrh_chv_pre_validation only
#   match             = vrh_chv_match only
# ─────────────────────────────────────────────────────────

set -e

TABLE_NAME="${1:?Usage: $0 <table_name> <data_date> [main|pre_vld|match]}"
DATA_DATE="${2:?Usage: $0 <table_name> <data_date> [main|pre_vld|match]}"
MODE="${3:-main}"

HOST="https://adb-7405612978007880.0.azuredatabricks.net"
CLUSTER_ID="0130-031624-0nmpnh8g"
WORKSPACE_BASE="/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge"

# Notebook path
case "$MODE" in
  main)    NOTEBOOK="$WORKSPACE_BASE/vrh_chv_main" ;;
  pre_vld) NOTEBOOK="$WORKSPACE_BASE/vrh_chv_pre_validation" ;;
  match)   NOTEBOOK="$WORKSPACE_BASE/vrh_chv_match" ;;
  *)       echo "Unknown mode: $MODE"; exit 1 ;;
esac

# Build params
CATALOG="viriyah_cdqm_poc"
FULL_TABLE="${CATALOG}.silver.${TABLE_NAME}"
PRCS_NM="EDP_MATCHING_${TABLE_NAME^^}_DATE_${DATA_DATE}"

case "$MODE" in
  main)
    PARAMS="{\"table_name\": \"${TABLE_NAME}\", \"data_date\": \"${DATA_DATE}\"}"
    ;;
  pre_vld)
    PARAMS="{\"PARAMS\": \"${FULL_TABLE}^|${CATALOG}.control_fw.CHV_PRE_VALIDATION_RESULT^|${DATA_DATE}^|EDP_PRE_VLD_${TABLE_NAME^^}^|1^|EDP_PRE_VLD_${TABLE_NAME^^}^|1\", \"ENV\": \"dev\"}"
    ;;
  match)
    PARAMS="{\"PARAMS\": \"${FULL_TABLE}^|${DATA_DATE}^|${PRCS_NM}^|1^|${PRCS_NM}^|1\", \"ENV\": \"dev\"}"
    ;;
esac

# Get Azure CLI token
echo "Getting Azure CLI token..."
TOKEN=$(az account get-access-token \
  --resource 2ff814a6-3304-4ab8-85cb-cd0e6f879c1d \
  --query accessToken -o tsv)

RUN_NAME="vrh_${MODE}_${TABLE_NAME}_${DATA_DATE}"

# Submit
echo "Submitting: $NOTEBOOK"
RESPONSE=$(curl -s -X POST \
  "${HOST}/api/2.1/jobs/runs/submit" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"run_name\": \"${RUN_NAME}\",
    \"existing_cluster_id\": \"${CLUSTER_ID}\",
    \"notebook_task\": {
      \"notebook_path\": \"${NOTEBOOK}\",
      \"base_parameters\": ${PARAMS}
    }
  }")

RUN_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('run_id','ERROR: '+str(d)))")

echo ""
echo "════════════════════════════════════════════════════"
echo "  Table    : $TABLE_NAME"
echo "  Date     : $DATA_DATE"
echo "  Mode     : $MODE"
echo "  Run ID   : $RUN_ID"
echo "  Run URL  : ${HOST}/#job/runs/${RUN_ID}"
echo "════════════════════════════════════════════════════"
echo ""
echo "To get error log after run completes:"
echo "  bash tests/get_run_log.sh $RUN_ID"
