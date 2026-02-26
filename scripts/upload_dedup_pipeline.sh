#!/usr/bin/env bash
# Upload dedup pipeline notebooks to Databricks workspace
# Usage: bash scripts/upload_dedup_pipeline.sh

set -e

export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg

BASE=/home/khaw/ClaudeCode/vrh_cdmq_dev/notebooks/work
WS=/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge

echo "=== Creating workspace directories ==="
databricks workspace mkdirs "$WS/insert_scripts"
databricks workspace mkdirs "$WS/unittest/dedup"

echo ""
echo "=== Uploading notebooks ==="

upload() {
  local label="$1"
  local local_file="$2"
  local ws_path="$3"
  echo -n "  [$label] ... "
  databricks workspace import \
    --file "$local_file" \
    --language PYTHON \
    --format SOURCE \
    --overwrite \
    "$ws_path"
  echo "OK"
}

upload "ddl_dedup_tables"    "$BASE/match_and_merge/insert_scripts/ddl_dedup_tables.py"    "$WS/insert_scripts/ddl_dedup_tables"
upload "ddl_source_devtest"  "$BASE/match_and_merge/insert_scripts/ddl_source_devtest.py"  "$WS/insert_scripts/ddl_source_devtest"
upload "config_devtest"      "$BASE/match_and_merge/insert_scripts/config_devtest.py"       "$WS/insert_scripts/config_devtest"
upload "data_prep_dedup"     "$BASE/unittest/dedup/data_prep_dedup.py"                      "$WS/unittest/dedup/data_prep_dedup"
upload "vrh_chv_dedup_v2"   "$BASE/match_and_merge/vrh_chv_dedup_v2.py"                    "$WS/vrh_chv_dedup_v2"
upload "test_dedup_v2"       "$BASE/unittest/dedup/test_dedup_v2.py"                        "$WS/unittest/dedup/test_dedup_v2"

echo ""
echo "=== All uploads complete ==="
echo ""
echo "Run order in Databricks:"
echo "  1. insert_scripts/ddl_source_devtest      (once — create tables)"
echo "  2. insert_scripts/config_devtest           (once — insert config rows)"
echo "  3. unittest/dedup/data_prep_dedup          (load 9+1 test rows + pre-validation results)"
echo "  4. vrh_chv_match_v2  PARAMS: viriyah_cdqm_poc.silver.source_motor_devtest^|test-2026-02-26^|TEST_MATCH_DEDUP^|1^|TEST_MATCH_DEDUP^|1"
echo "  5. vrh_chv_dedup_v2  PARAMS: viriyah_cdqm_poc.silver.source_motor_devtest^|test-2026-02-26^|TEST_DEDUP^|1^|TEST_DEDUP^|1"
echo "     Widgets: SOURCE_TABLE=viriyah_cdqm_poc.silver.source_motor_devtest"
echo "              TRUST_TABLE=viriyah_cdqm_poc.silver.trust_source_devtest"
echo "  6. unittest/dedup/test_dedup_v2            (assert results)"
