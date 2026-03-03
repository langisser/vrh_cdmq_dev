#!/usr/bin/env python3
"""
run_full_rebuild.py — Clear bkey + dedup tables, then run full pipeline per data_dt

Steps:
  1. TRUNCATE chv_table_bkey_v2
  2. DROP + recreate all 5 dedup tables (ddl_dedup_tables)
  3. For each data_dt:
       a. pre_val SOURCE_MOTOR  (sequential)
       b. pre_val TRUST_SOURCE  (sequential)
       c. match SOURCE_MOTOR    (sequential)
       d. dedup x5              (parallel)

DATA_DTs processed in order:
  2025-01-01, 2025-01-02, 2025-02-01, 2025-02-02, 2025-02-03

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_full_rebuild.py
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error
from databricks.connect import DatabricksSession

CLUSTER_ID   = '0130-031624-0nmpnh8g'
WS_BASE      = '/Workspace/Users/khachornpop@inteltion.com/vrh'
WS_MATCH     = f'{WS_BASE}/match_and_merge'
WS_DEDUP     = f'{WS_BASE}/dedup'

SOURCE_TABLE = 'viriyah_cdqm_poc.silver.source_motor_devtest'
TRUST_TABLE  = 'viriyah_cdqm_poc.silver.trust_source_devtest'
VLD_TABLE    = 'viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2'

DATA_DTS = ['2025-01-01', '2025-01-02', '2025-02-01', '2025-02-02', '2025-02-03']

DEDUP_NOTEBOOKS = [
    ("dedup_customer_name", f"{WS_DEDUP}/dedup_customer_name"),
    ("dedup_province",      f"{WS_DEDUP}/dedup_province"),
    ("dedup_gender",        f"{WS_DEDUP}/dedup_gender"),
    ("dedup_email",         f"{WS_DEDUP}/dedup_email"),
    ("dedup_phone",         f"{WS_DEDUP}/dedup_phone"),
]

timings = []  # [(label, elapsed, status)]

def run(label, path, params):
    print(f"\n[{label}] running...", flush=True)
    t0 = time.time()
    result = run_notebook(
        notebook_path=path,
        params=params,
        cluster_id=CLUSTER_ID,
        auth_method='azure_cli',
    )
    elapsed = time.time() - t0
    status = result['status']
    timings.append((label, elapsed, status))
    print(f"[{label}] → {status}  ({elapsed:.0f}s)", flush=True)
    if status == 'FAILED':
        for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
            print(f"  CELL ERROR: {e['summary']}")
            print(f"  {e['error_detail']}")
        print_summary()
        sys.exit(1)

def run_dedup_parallel(data_dt):
    print(f"\n  [dedup parallel] launching 5 notebooks...", flush=True)
    futures = {}
    with ThreadPoolExecutor(max_workers=5) as executor:
        for label, path in DEDUP_NOTEBOOKS:
            suffix = label.upper().replace("DEDUP_", "")
            prcs_nm = f"EDP_DEDUP_{suffix}_{data_dt}"
            params_str = f"{SOURCE_TABLE}^|{data_dt}^|{prcs_nm}^|1"
            future = executor.submit(
                run_notebook,
                notebook_path=path,
                params={'PARAMS': params_str, 'ENV': 'dev'},
                cluster_id=CLUSTER_ID,
                auth_method='azure_cli',
            )
            futures[future] = label

        failed = False
        for future in as_completed(futures):
            label = futures[future]
            result = future.result()
            elapsed = result.get('duration') or 0
            status = result['status']
            full_label = f"{label} [{data_dt}]"
            timings.append((full_label, elapsed, status))
            mark = "✓" if status == "SUCCESS" else "✗"
            print(f"  {mark} [{full_label}] → {status}  ({elapsed:.0f}s)", flush=True)
            if status == 'FAILED':
                failed = True
                for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
                    print(f"    CELL ERROR: {e['summary']}")
                    print(f"    {e['error_detail']}")

    if failed:
        print_summary()
        sys.exit(1)

def print_summary():
    print("\n" + "=" * 62)
    print(f"{'Stage':<42} {'Time':>6}  {'Status'}")
    print("-" * 62)
    total = 0
    for label, elapsed, status in timings:
        mark = "✓" if status == "SUCCESS" else "✗"
        print(f"  {mark} {label:<40} {elapsed:>6.0f}s  {status}")
        total += elapsed
    print("-" * 62)
    print(f"  {'TOTAL':<42} {total:>6.0f}s")
    print("=" * 62)

# ── Step 1: TRUNCATE bkey table ─────────────────────────────────────────────
print("=== Step 1: Truncate chv_table_bkey_v2 ===")
spark = DatabricksSession.builder.getOrCreate()
spark.sql("TRUNCATE TABLE viriyah_cdqm_poc.silver.chv_table_bkey_v2")
print("  chv_table_bkey_v2 truncated")

# ── Step 2: DROP + recreate dedup tables ────────────────────────────────────
print("\n=== Step 2: Recreate dedup tables ===")
run("ddl_dedup_tables", f"{WS_DEDUP}/ddl_dedup_tables", {'ENV': 'dev'})

# ── Step 3: Full pipeline per data_dt ───────────────────────────────────────
for data_dt in DATA_DTS:
    print(f"\n{'='*62}")
    print(f"=== data_dt = {data_dt} ===")
    print(f"{'='*62}")

    # pre_val SOURCE_MOTOR
    run(
        f"pre_val SOURCE_MOTOR [{data_dt}]",
        f"{WS_MATCH}/vrh_chv_pre_validation_v2",
        {'PARAMS': f"{SOURCE_TABLE}^|{VLD_TABLE}^|{data_dt}^|EDP_PRE_VLD_V2_SOURCE_MOTOR_{data_dt}^|1^|EDP_PRE_VLD_V2_SOURCE_MOTOR_{data_dt}^|1", 'ENV': 'dev'},
    )

    # pre_val TRUST_SOURCE
    run(
        f"pre_val TRUST_SOURCE [{data_dt}]",
        f"{WS_MATCH}/vrh_chv_pre_validation_v2",
        {'PARAMS': f"{TRUST_TABLE}^|{VLD_TABLE}^|{data_dt}^|EDP_PRE_VLD_V2_TRUST_SOURCE_{data_dt}^|1^|EDP_PRE_VLD_V2_TRUST_SOURCE_{data_dt}^|1", 'ENV': 'dev'},
    )

    # match
    match_prcs = f"EDP_MATCHING_V2_SOURCE_MOTOR_{data_dt}"
    run(
        f"match SOURCE_MOTOR [{data_dt}]",
        f"{WS_MATCH}/vrh_chv_match_v2",
        {'PARAMS': f"{SOURCE_TABLE}^|{data_dt}^|{match_prcs}^|1^|{match_prcs}^|1", 'ENV': 'dev'},
    )

    # dedup x5 parallel
    run_dedup_parallel(data_dt)

print_summary()
print("\n=== Full rebuild complete ===")
