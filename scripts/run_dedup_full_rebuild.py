#!/usr/bin/env python3
"""
run_dedup_full_rebuild.py — DROP all dedup output tables then re-run dedup pipeline

Use this to recover from data corruption (e.g. duplicate rows from NULL MERGE bug).
Steps:
  1. Run ddl_dedup_tables  → DROP + recreate 5 dedup tables (clean slate)
  2. Run dedup notebooks   → populate from source data, 5 notebooks in parallel per data_dt

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_dedup_full_rebuild.py
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_DEDUP   = '/Workspace/Users/khachornpop@inteltion.com/vrh/dedup'

SOURCE_TABLE = 'viriyah_cdqm_poc.silver.source_motor_devtest'
DATA_DTS     = ['2025-01-01', '2025-01-02', '2025-02-02', '2025-02-03']

DEDUP_NOTEBOOKS = [
    ("dedup_customer_name", f"{WS_DEDUP}/dedup_customer_name"),
    ("dedup_province",      f"{WS_DEDUP}/dedup_province"),
    ("dedup_gender",        f"{WS_DEDUP}/dedup_gender"),
    ("dedup_email",         f"{WS_DEDUP}/dedup_email"),
    ("dedup_phone",         f"{WS_DEDUP}/dedup_phone"),
]

timings = []  # [(label, elapsed, status)]

def run_one(label, path, params):
    t0 = time.time()
    result = run_notebook(
        notebook_path=path,
        params=params,
        cluster_id=CLUSTER_ID,
        auth_method='azure_cli',
    )
    elapsed = time.time() - t0
    status = result['status']
    if status == 'FAILED':
        errors = get_run_cell_error(result['run_id'], auth_method='azure_cli')
        return label, elapsed, status, errors
    return label, elapsed, status, []

def print_summary():
    print("\n" + "=" * 62)
    print(f"{'Stage':<40} {'Time':>8}  {'Status'}")
    print("-" * 62)
    total = 0
    for label, elapsed, status in timings:
        mark = "✓" if status == "SUCCESS" else "✗"
        print(f"  {mark} {label:<38} {elapsed:>6.0f}s  {status}")
        total += elapsed
    print("-" * 62)
    print(f"  {'TOTAL (wall clock)':<40} {total:>6.0f}s")
    print("=" * 62)

# ── Step 1: DROP + recreate dedup tables (clean slate) ─────────────────────
print("[ddl_dedup_tables] running...", flush=True)
t0 = time.time()
result = run_notebook(
    notebook_path=f"{WS_DEDUP}/ddl_dedup_tables",
    params={'ENV': 'dev'},
    cluster_id=CLUSTER_ID,
    auth_method='azure_cli',
)
elapsed = time.time() - t0
status = result['status']
timings.append(("ddl_dedup_tables", elapsed, status))
print(f"[ddl_dedup_tables] → {status}  ({elapsed:.0f}s)")
if status == 'FAILED':
    print_summary()
    sys.exit(1)

# ── Step 2: Run 5 dedup notebooks in parallel per data_dt ──────────────────
failed = False
for data_dt in DATA_DTS:
    print(f"\n=== data_dt={data_dt} — launching 5 notebooks in parallel ===", flush=True)
    futures = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for label, path in DEDUP_NOTEBOOKS:
            prcs_nm = f"EDP_DEDUP_{label.upper().replace('DEDUP_', '')}_{data_dt}"
            params_str = f"{SOURCE_TABLE}^|{data_dt}^|{prcs_nm}^|1"
            full_label = f"{label} [{data_dt}]"
            print(f"  [{full_label}] submitted", flush=True)
            future = executor.submit(run_one, full_label, path, {'PARAMS': params_str, 'ENV': 'dev'})
            futures[future] = full_label

        for future in as_completed(futures):
            label, elapsed, status, errors = future.result()
            timings.append((label, elapsed, status))
            mark = "✓" if status == "SUCCESS" else "✗"
            print(f"  {mark} [{label}] → {status}  ({elapsed:.0f}s)", flush=True)
            if status == 'FAILED':
                failed = True
                for e in errors:
                    print(f"    CELL ERROR: {e['summary']}")
                    print(f"    {e['error_detail']}")

    if failed:
        print_summary()
        sys.exit(1)

print_summary()
print("\n=== Full rebuild complete ===")
