#!/usr/bin/env python3
"""
run_generate_perf_data.py — Upload perf notebook then generate 100K perf data

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_generate_perf_data.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error, upload_to_workspace, get_workspace_client

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_PERF    = '/Workspace/Users/khachornpop@inteltion.com/vrh/perf'
LOCAL_BASE = '/home/khaw/ClaudeCode/vrh_cdmq_dev/notebooks/work'

# Step 1: Upload notebook
print("=== Step 1: Upload generate_perf_data notebook ===")
w = get_workspace_client()
w.workspace.mkdirs(WS_PERF)
print(f"  Workspace dir ready: {WS_PERF}")

result = upload_to_workspace(
    f"{LOCAL_BASE}/perf/generate_perf_data.py",
    f"{WS_PERF}/generate_perf_data",
    auth_method="azure_cli",
)
if result["status"] != "SUCCESS":
    print(f"Upload FAILED: {result.get('error')}")
    sys.exit(1)
print("  Upload OK")

# Step 2: Run notebook
print("\n=== Step 2: Run generate_perf_data ===")
result = run_notebook(
    notebook_path=f"{WS_PERF}/generate_perf_data",
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

print("\nPerf data generated successfully.")
print("QA can now run the match & merge pipeline with:")
print("  DATA_DT       : 2025-02-01")
print("  SOURCE_TABLE  : viriyah_cdqm_poc.silver.source_motor_devtest")
print("  TRUST_TABLE   : viriyah_cdqm_poc.silver.trust_source_devtest")
print("  Rows in source: 100,000")
