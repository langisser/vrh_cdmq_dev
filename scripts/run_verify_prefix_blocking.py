#!/usr/bin/env python3
"""
run_verify_prefix_blocking.py — Verify prefix blocking fix produces identical bkey results

Steps:
  1. Upload verify notebooks
  2. Backup chv_table_bkey_v2 -> chv_table_bkey_v2_backup_prefixfix, then truncate
  3. Rerun full match pipeline for DATA_DT = 2025-01-01 and 2025-01-02
  4. Compare new bkey results vs backup — must be identical

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_verify_prefix_blocking.py
"""
import os, sys, time
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error, upload_to_workspace

CLUSTER_ID = '0130-031624-0nmpnh8g'
WS_MATCH   = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge'
WS_BACKUP  = f'{WS_MATCH}/verify_prefix_blocking_backup'
WS_COMPARE = f'{WS_MATCH}/verify_prefix_blocking_compare'

SOURCE_TABLE = 'viriyah_cdqm_poc.silver.source_motor_devtest'
TRUST_TABLE  = 'viriyah_cdqm_poc.silver.trust_source_devtest'
VLD_TABLE    = 'viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2'

timings = []

def run(label, path, params, *, fail_ok=False):
    print(f"\n[{label}] running...", flush=True)
    t0 = time.time()
    result = run_notebook(notebook_path=path, params=params, cluster_id=CLUSTER_ID, auth_method='azure_cli')
    elapsed = time.time() - t0
    status = result['status']
    timings.append((label, elapsed, status))
    print(f"[{label}] -> {status}  ({elapsed:.0f}s)", flush=True)
    if status == 'FAILED':
        for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
            print(f"  CELL ERROR: {e['summary']}")
            print(f"  {e['error_detail']}")
        if not fail_ok:
            print_summary()
            sys.exit(1)
    return result

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
    print(f"  {'TOTAL':<40} {total:>6.0f}s")
    print("=" * 62)

# ── Step 1: Upload notebooks ───────────────────────────────────────────────────

print("=== Step 1: Upload verify notebooks ===")
uploads = [
    ('verify_prefix_blocking_backup',  'notebooks/work/match_and_merge/verify_prefix_blocking_backup.py',  WS_BACKUP),
    ('verify_prefix_blocking_compare', 'notebooks/work/match_and_merge/verify_prefix_blocking_compare.py', WS_COMPARE),
]
for label, local, ws in uploads:
    r = upload_to_workspace(local, ws, auth_method='azure_cli')
    status = r['status']
    print(f"  [{label}] ... {status}")
    if status != 'SUCCESS':
        print(f"  Upload failed: {r.get('error')}")
        sys.exit(1)

# ── Step 2: Backup + Truncate ─────────────────────────────────────────────────

print("\n=== Step 2: Backup + Truncate bkey table ===")
run("backup+truncate bkey", WS_BACKUP, {'ENV': 'dev'})

# ── Step 3: Rerun match for 2025-01-01 and 2025-01-02 ────────────────────────

print("\n=== Step 3: Rerun match pipeline ===")

for DATA_DT in ['2025-01-01', '2025-01-02']:
    dt_tag = DATA_DT.replace('-', '')

    run(f"pre_val SOURCE_MOTOR {DATA_DT}", f"{WS_MATCH}/vrh_chv_pre_validation_v2", {
        'PARAMS': f"{SOURCE_TABLE}^|{VLD_TABLE}^|{DATA_DT}^|EDP_PRE_VLD_V2_SOURCE_MOTOR_{dt_tag}^|1^|EDP_PRE_VLD_V2_SOURCE_MOTOR_{dt_tag}^|1",
        'ENV': 'dev'
    })

    run(f"pre_val TRUST_SOURCE {DATA_DT}", f"{WS_MATCH}/vrh_chv_pre_validation_v2", {
        'PARAMS': f"{TRUST_TABLE}^|{VLD_TABLE}^|{DATA_DT}^|EDP_PRE_VLD_V2_TRUST_SOURCE_{dt_tag}^|1^|EDP_PRE_VLD_V2_TRUST_SOURCE_{dt_tag}^|1",
        'ENV': 'dev'
    })

    prcs = f"EDP_MATCHING_V2_SOURCE_MOTOR_{dt_tag}"
    run(f"match SOURCE_MOTOR {DATA_DT}", f"{WS_MATCH}/vrh_chv_match_v2", {
        'PARAMS': f"{SOURCE_TABLE}^|{DATA_DT}^|{prcs}^|1^|{prcs}^|1",
        'ENV': 'dev'
    })

# ── Step 4: Compare ───────────────────────────────────────────────────────────

print("\n=== Step 4: Compare new bkey vs backup ===")
run("compare bkey vs backup", WS_COMPARE, {'ENV': 'dev'})

print_summary()
