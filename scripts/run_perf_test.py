#!/usr/bin/env python3
"""
run_perf_test.py — Performance test: full pipeline on 100K row source_motor_devtest

Steps:
  1. Upload perf notebooks to Databricks workspace
  2. Generate 100K synthetic rows (DATA_DT='2025-02-03')
  3. Run full pipeline: pre-val → match → 5 dedup notebooks
  4. Print per-stage elapsed time summary

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_perf_test.py
"""
import os, sys, time
from concurrent.futures import ThreadPoolExecutor, as_completed
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error, upload_to_workspace, get_workspace_client

# ── Constants ──────────────────────────────────────────────────────────────────

CLUSTER_ID   = '0130-031624-0nmpnh8g'
WS_BASE      = '/Workspace/Users/khachornpop@inteltion.com/vrh'
WS_MATCH     = f'{WS_BASE}/match_and_merge'
WS_DEDUP     = f'{WS_BASE}/dedup'
WS_PERF      = f'{WS_BASE}/perf'

LOCAL_BASE   = '/home/khaw/ClaudeCode/vrh_cdmq_dev/notebooks/work'

SOURCE_TABLE = 'viriyah_cdqm_poc.silver.source_motor_devtest'
TRUST_TABLE  = 'viriyah_cdqm_poc.silver.trust_source_devtest'
VLD_TABLE    = 'viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2'
DATA_DT      = '2025-02-03'

# ── Helpers ────────────────────────────────────────────────────────────────────

timings = []  # [(label, elapsed_seconds)]

def run(label, path, params, *, optional=False):
    """Run a notebook and record elapsed time. Exits on failure."""
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
        if optional:
            print(f"  (non-fatal — continuing)")
        else:
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

# ── Step 1: Upload perf notebooks ─────────────────────────────────────────────

print("=== Step 1: Upload perf notebooks ===")

w = get_workspace_client()
w.workspace.mkdirs(WS_PERF)
print(f"  Workspace dir ready: {WS_PERF}")

PERF_UPLOADS = [
    ("generate_perf_data", f"{LOCAL_BASE}/perf/generate_perf_data.py", f"{WS_PERF}/generate_perf_data"),
    ("cleanup_perf_data",  f"{LOCAL_BASE}/perf/cleanup_perf_data.py",  f"{WS_PERF}/cleanup_perf_data"),
]

upload_failed = False
for label, local_path, ws_path in PERF_UPLOADS:
    print(f"  [{label}] ... ", end="", flush=True)
    result = upload_to_workspace(local_path, ws_path, auth_method="azure_cli")
    if result["status"] == "SUCCESS":
        print("OK")
    else:
        print(f"FAILED: {result.get('error')}")
        upload_failed = True

if upload_failed:
    print("Upload failed — aborting.")
    sys.exit(1)

print("Uploads complete.")

# ── Step 2: Generate 100K rows ─────────────────────────────────────────────────

print("\n=== Step 2: Generate 100K perf data ===")
run("generate_perf_data", f"{WS_PERF}/generate_perf_data", {'ENV': 'dev'})

# ── Step 3: Full pipeline ──────────────────────────────────────────────────────

print("\n=== Step 3: Full pipeline run ===")

# Pre-val: SOURCE_MOTOR
preval_motor_params = (
    f"{SOURCE_TABLE}^|{VLD_TABLE}^|{DATA_DT}"
    f"^|EDP_PRE_VLD_V2_SOURCE_MOTOR_PERF^|1^|EDP_PRE_VLD_V2_SOURCE_MOTOR_PERF^|1"
)
run(
    "pre_val SOURCE_MOTOR",
    f"{WS_MATCH}/vrh_chv_pre_validation_v2",
    {'PARAMS': preval_motor_params, 'ENV': 'dev'},
)

# Pre-val: TRUST_SOURCE
preval_trust_params = (
    f"{TRUST_TABLE}^|{VLD_TABLE}^|{DATA_DT}"
    f"^|EDP_PRE_VLD_V2_TRUST_SOURCE_PERF^|1^|EDP_PRE_VLD_V2_TRUST_SOURCE_PERF^|1"
)
run(
    "pre_val TRUST_SOURCE",
    f"{WS_MATCH}/vrh_chv_pre_validation_v2",
    {'PARAMS': preval_trust_params, 'ENV': 'dev'},
)

# Match: SOURCE_MOTOR
match_prcs = f"EDP_MATCHING_V2_SOURCE_MOTOR_PERF_{DATA_DT}"
match_params = f"{SOURCE_TABLE}^|{DATA_DT}^|{match_prcs}^|1^|{match_prcs}^|1"
run(
    "match SOURCE_MOTOR",
    f"{WS_MATCH}/vrh_chv_match_v2",
    {'PARAMS': match_params, 'ENV': 'dev'},
)

# Dedup notebooks — run in parallel
DEDUP_NOTEBOOKS = [
    ("dedup_customer_name",   f"{WS_DEDUP}/dedup_customer_name"),
    ("dedup_province",        f"{WS_DEDUP}/dedup_province"),
    ("dedup_gender",          f"{WS_DEDUP}/dedup_gender"),
    ("dedup_email",           f"{WS_DEDUP}/dedup_email"),
    ("dedup_phone",           f"{WS_DEDUP}/dedup_phone"),
]

print("\n=== Dedup: launching 5 notebooks in parallel ===", flush=True)
dedup_futures = {}
with ThreadPoolExecutor(max_workers=5) as executor:
    for label, path in DEDUP_NOTEBOOKS:
        suffix = label.upper().replace("DEDUP_", "")
        prcs_nm = f"EDP_DEDUP_{suffix}_PERF"
        params_str = f"{SOURCE_TABLE}^|{DATA_DT}^|{prcs_nm}^|1"
        print(f"  [{label}] submitted", flush=True)
        future = executor.submit(
            run_notebook,
            notebook_path=path,
            params={'PARAMS': params_str, 'ENV': 'dev'},
            cluster_id=CLUSTER_ID,
            auth_method='azure_cli',
        )
        dedup_futures[future] = label

    failed = False
    for future in as_completed(dedup_futures):
        label = dedup_futures[future]
        result = future.result()
        elapsed = result.get('duration') or 0
        status = result['status']
        timings.append((label, elapsed, status))
        mark = "✓" if status == "SUCCESS" else "✗"
        print(f"  {mark} [{label}] → {status}  ({elapsed:.0f}s)", flush=True)
        if status == 'FAILED':
            failed = True
            for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
                print(f"    CELL ERROR: {e['summary']}")
                print(f"    {e['error_detail']}")
    if failed:
        print_summary()
        sys.exit(1)

# ── Step 4: Print timing summary ───────────────────────────────────────────────

print_summary()
print()
print("Run the following in Databricks SQL or via spark_wrapper to verify:")
print(f"  SELECT COUNT(*) FROM viriyah_cdqm_poc.silver.source_motor_devtest WHERE DATA_DT = '{DATA_DT}';")
print(f"  SELECT COUNT(DISTINCT bkey) FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 WHERE data_dt = '{DATA_DT}';")
print(f"  SELECT COUNT(*) FROM viriyah_cdqm_poc.silver.dedup_customer_name;")
print()
print("To clean up perf data afterward, run:")
print("  python3 scripts/run_perf_cleanup.py")
