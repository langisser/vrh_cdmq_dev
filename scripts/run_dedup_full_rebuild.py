#!/usr/bin/env python3
"""
run_dedup_full_rebuild.py — DROP all dedup output tables then re-run dedup pipeline

Use this to recover from data corruption (e.g. duplicate rows from NULL MERGE bug).
Steps:
  1. Run ddl_dedup_tables  → DROP + recreate 5 dedup tables (clean slate)
  2. Run dedup notebooks   → populate from source data (2025-01-01 and 2025-01-02)

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_dedup_full_rebuild.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_DEDUP   = '/Workspace/Users/khachornpop@inteltion.com/vrh/dedup'

SOURCE_TABLE = 'viriyah_cdqm_poc.silver.source_motor_devtest'
DATA_DTS     = ['2025-01-01', '2025-01-02']

def run(label, path, params):
    print(f"\n[{label}] running...", flush=True)
    result = run_notebook(
        notebook_path=path,
        params=params,
        cluster_id=CLUSTER_ID,
        auth_method='azure_cli',
    )
    status = result['status']
    print(f"[{label}] → {status}")
    if status == 'FAILED':
        for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
            print(f"  CELL ERROR: {e['summary']}")
            print(f"  {e['error_detail']}")
        sys.exit(1)
    return result

# ── Step 1: DROP + recreate dedup tables (clean slate) ─────────────────────
run(
    "ddl_dedup_tables",
    f"{WS_DEDUP}/ddl_dedup_tables",
    {'ENV': 'dev'},
)

# ── Step 2: Run all 5 dedup notebooks for each data_dt ─────────────────────
DEDUP_NOTEBOOKS = [
    ("dedup_customer_name", f"{WS_DEDUP}/dedup_customer_name"),
    ("dedup_province",      f"{WS_DEDUP}/dedup_province"),
    ("dedup_gender",        f"{WS_DEDUP}/dedup_gender"),
    ("dedup_email",         f"{WS_DEDUP}/dedup_email"),
    ("dedup_phone",         f"{WS_DEDUP}/dedup_phone"),
]

for data_dt in DATA_DTS:
    for label, path in DEDUP_NOTEBOOKS:
        prcs_nm = f"EDP_DEDUP_{label.upper().replace('DEDUP_', '')}_{data_dt}"
        params_str = f"{SOURCE_TABLE}^|{data_dt}^|{prcs_nm}^|1"
        run(f"{label} [{data_dt}]", path, {'PARAMS': params_str, 'ENV': 'dev'})

print("\n=== Full rebuild complete ===")
print("Run QA verification queries to confirm no duplicates remain.")
