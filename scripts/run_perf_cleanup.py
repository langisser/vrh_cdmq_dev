#!/usr/bin/env python3
"""
run_perf_cleanup.py — Remove all perf test data (DATA_DT='2025-02-01')

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_perf_cleanup.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_PERF    = '/Workspace/Users/khachornpop@inteltion.com/vrh/perf'

print("Running cleanup_perf_data notebook...")
result = run_notebook(
    notebook_path=f"{WS_PERF}/cleanup_perf_data",
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

print("Perf data cleanup complete.")
