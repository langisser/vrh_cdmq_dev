#!/usr/bin/env bash
# Run full dedup test pipeline on Databricks cluster
# Usage: bash scripts/run_dedup_pipeline.sh

set -e

export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg

CLUSTER_ID="0130-031624-0nmpnh8g"
WS_MATCH="/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge"
WS_DEDUP="/Workspace/Users/khachornpop@inteltion.com/vrh/dedup"

# Run a notebook synchronously and print result
# $1 = label, $2 = notebook path, $3 = base_parameters JSON object (or empty string)
run_notebook() {
  local label="$1"
  local nb_path="$2"
  local base_params="$3"

  echo ""
  echo ">>> [$label] Starting..."

  local params_block=""
  if [ -n "$base_params" ]; then
    params_block=", \"base_parameters\": $base_params"
  fi

  local json_payload
  json_payload=$(cat <<EOF
{
  "run_name": "dedup_pipeline_${label}",
  "tasks": [{
    "task_key": "main",
    "existing_cluster_id": "${CLUSTER_ID}",
    "notebook_task": {
      "notebook_path": "${nb_path}"${params_block}
    }
  }],
  "timeout_seconds": 600
}
EOF
)

  # Submit and get run_id
  local submit_out
  submit_out=$(databricks jobs submit --no-wait --json "$json_payload" 2>&1)
  echo "    submit: $submit_out"

  local run_id
  run_id=$(echo "$submit_out" | python3 -c "import sys,json; print(json.load(sys.stdin)['run_id'])" 2>/dev/null || echo "")

  if [ -z "$run_id" ]; then
    echo "ERROR: Could not parse run_id for [$label]"
    exit 1
  fi
  echo "    run_id: $run_id — polling..."

  # Poll until terminal state
  local state="PENDING"
  while [ "$state" != "TERMINATED" ] && [ "$state" != "INTERNAL_ERROR" ] && [ "$state" != "SKIPPED" ]; do
    sleep 15
    state=$(databricks jobs get-run "$run_id" -o json 2>&1 \
      | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['state']['life_cycle_state'])" 2>/dev/null || echo "UNKNOWN")
    echo "    state: $state"
  done

  local result_state
  result_state=$(databricks jobs get-run "$run_id" -o json 2>&1 \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['state'].get('result_state','UNKNOWN'))" 2>/dev/null || echo "UNKNOWN")
  echo "    result: $result_state"

  if [ "$result_state" != "SUCCESS" ]; then
    echo "FAILED [$label] — result_state=$result_state  run_id=$run_id"
    databricks jobs get-run-output "$run_id" -o json 2>&1 \
      | python3 -c "
import sys, json
d = json.load(sys.stdin)
tasks = d.get('task_runs', [d])
for t in tasks:
    nb = t.get('notebook_output', {})
    print('--- notebook output ---')
    print(nb.get('result', '(no output)'))
" 2>/dev/null || true
    exit 1
  fi

  echo "    DONE [$label]"
}

echo "============================================================"
echo "Dedup Test Pipeline"
echo "Cluster: $CLUSTER_ID"
echo "============================================================"

# Step 1a: DDL — create 5 dedup output tables (idempotent)
run_notebook "1a_ddl_dedup_tables" \
  "$WS_MATCH/insert_scripts/ddl_dedup_tables" \
  ""

# Step 1b: DDL — create devtest source tables (idempotent)
run_notebook "1b_ddl_source_devtest" \
  "$WS_MATCH/insert_scripts/ddl_source_devtest" \
  ""

# Step 2: Config — insert PK + matching rules
run_notebook "2_config_devtest" \
  "$WS_MATCH/insert_scripts/config_devtest" \
  ""

# Step 3: Data prep — load 9+1 test rows
run_notebook "3_data_prep_dedup" \
  "$WS_MATCH/unittest/dedup/data_prep_dedup" \
  ""

# Step 4: Match — assign BKEYs via vrh_chv_match_v2
run_notebook "4_vrh_chv_match_v2" \
  "$WS_MATCH/vrh_chv_match_v2" \
  '{"PARAMS": "viriyah_cdqm_poc.silver.source_motor_devtest^|2026-02-26^|TEST_MATCH_DEDUP^|1^|TEST_MATCH_DEDUP^|1", "ENV": "dev"}'

# Step 5: Dedup — build 5 dedup tables
run_notebook "5_vrh_chv_dedup_v2" \
  "$WS_DEDUP/vrh_chv_dedup_v2" \
  '{"PARAMS": "viriyah_cdqm_poc.silver.source_motor_devtest^|2026-02-26^|TEST_DEDUP^|1^|TEST_DEDUP^|1", "ENV": "dev", "SOURCE_TABLE": "viriyah_cdqm_poc.silver.source_motor_devtest", "TRUST_TABLE": "viriyah_cdqm_poc.silver.trust_source_devtest"}'

# Step 6: Assert results
run_notebook "6_test_dedup_v2" \
  "$WS_DEDUP/unittest/dedup/test_dedup_v2" \
  ""

echo ""
echo "============================================================"
echo "ALL STEPS PASSED"
echo "============================================================"
