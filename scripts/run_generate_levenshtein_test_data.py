#!/usr/bin/env python3
"""
run_generate_levenshtein_test_data.py — Upload levenshtein test notebook then run it

Generates ~20K rows for DATA_DT='2025-02-01' where policies sharing the same
id_card have similar (levenshtein distance 1) fname/lname — exercises matching
rules 32/33/35/36/38/39.

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_generate_levenshtein_test_data.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error, upload_to_workspace, get_workspace_client

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_PERF    = '/Workspace/Users/khachornpop@inteltion.com/vrh/perf'
LOCAL_BASE = '/home/khaw/ClaudeCode/vrh_cdmq_dev/notebooks/work'

# Step 1: Upload notebook
print("=== Step 1: Upload generate_levenshtein_test_data notebook ===")
w = get_workspace_client()
w.workspace.mkdirs(WS_PERF)
print(f"  Workspace dir ready: {WS_PERF}")

result = upload_to_workspace(
    f"{LOCAL_BASE}/perf/generate_levenshtein_test_data.py",
    f"{WS_PERF}/generate_levenshtein_test_data",
    auth_method="azure_cli",
)
if result["status"] != "SUCCESS":
    print(f"Upload FAILED: {result.get('error')}")
    sys.exit(1)
print("  Upload OK")

# Step 2: Run notebook
print("\n=== Step 2: Run generate_levenshtein_test_data ===")
result = run_notebook(
    notebook_path=f"{WS_PERF}/generate_levenshtein_test_data",
    params={'ENV': 'dev'},
    cluster_id=CLUSTER_ID,
    auth_method='azure_cli',
)
status = result['status']
print(f"→ {status}")

if status == 'FAILED':
    for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
        print(f"  CELL ERROR: {e['summary']}")
        print(f"  {e['error_detail']}")
    sys.exit(1)

print("\nLevenshtein test data generated successfully.")
print("DATA_DT       : 2025-02-01")
print("SOURCE_TABLE  : viriyah_cdqm_poc.silver.source_motor_devtest")
print("TRUST_TABLE   : viriyah_cdqm_poc.silver.trust_source_devtest")
print("Rows in source: ~20,000  (8K singletons + ~12K levenshtein groups)")
print()
print("Spot-check after running match pipeline for 2025-02-01:")
print("  SELECT id_card, COUNT(DISTINCT bkey) AS n_bkeys")
print("  FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2")
print("  WHERE data_dt = '2025-02-01' AND id_card LIKE '7%'")
print("  GROUP BY id_card HAVING n_bkeys > 1  -- expect 0 rows")
