#!/usr/bin/env python3
"""
CHV Match & Merge Pipeline Runner — v2 (TIER + SUBJECT)
=========================================================
Usage:
    source setup_env.sh && source venv/bin/activate
    python3 tests/run_chv_v2.py [mode] [table_name] [data_date]

Modes:
    setup_ddl     — run ddl_v2_tables (create _v2 tables)
    setup_copy    — run chv_config_copy_to_v2 (copy config data)
    setup_config  — run chv_config_matching_v2 (insert TIER+SUBJECT rules)
    setup         — run all 3 setup steps in sequence
    main          — run vrh_chv_main_v2 (full pipeline)
    all           — setup + main

Examples:
    python3 tests/run_chv_v2.py setup
    python3 tests/run_chv_v2.py main tc_case2_similarity 2015-12-28
    python3 tests/run_chv_v2.py all  tc_case2_similarity 2015-12-28
"""

import os, sys, json, time, requests
from datetime import datetime
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
MODE       = sys.argv[1] if len(sys.argv) > 1 else "main"
TABLE_NAME = sys.argv[2] if len(sys.argv) > 2 else "tc_case2_similarity"
DATA_DATE  = sys.argv[3] if len(sys.argv) > 3 else "2015-12-28"
ENV        = "dev"

WORKSPACE_BASE = "/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge"
TIMEOUT_SECONDS = 1800
POLL_INTERVAL   = 10
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

NOTEBOOK_PATHS = {
    "setup_ddl"    : f"{WORKSPACE_BASE}/insert_scripts/ddl_v2_tables",
    "setup_copy"   : f"{WORKSPACE_BASE}/insert_scripts/chv_config_copy_to_v2",
    "setup_config" : f"{WORKSPACE_BASE}/insert_scripts/chv_config_matching_v2",
    "main"         : f"{WORKSPACE_BASE}/vrh_chv_main_v2",
    "pre_vld"      : f"{WORKSPACE_BASE}/vrh_chv_pre_validation_v2",
    "match"        : f"{WORKSPACE_BASE}/vrh_chv_match_v2",
}

# ── Databricks auth ─────────────────────────────────────────────────────────
DATABRICKS_HOST  = ""
DATABRICKS_TOKEN = ""

def _parse_cfg(path):
    vals = {}
    for line in open(path):
        line = line.strip()
        if not line or line.startswith("[") or line.startswith("#"):
            continue
        if "=" in line:
            k, v = line.split("=", 1)
            vals[k.strip()] = v.strip()
    return vals

def load_auth():
    global DATABRICKS_HOST, DATABRICKS_TOKEN
    cfg_path = Path(__file__).parent.parent / ".databrickscfg"
    cfg = _parse_cfg(cfg_path)
    DATABRICKS_HOST = cfg["host"].rstrip("/")
    if cfg.get("token"):
        DATABRICKS_TOKEN = cfg["token"]
        return
    if "azure-cli" in cfg.get("auth_type", ""):
        import subprocess
        r = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d", "--output", "json"],
            capture_output=True, text=True, timeout=30)
        DATABRICKS_TOKEN = json.loads(r.stdout)["accessToken"]

def load_cluster_id():
    cfg_path = Path(__file__).parent.parent / ".databrickscfg"
    return _parse_cfg(cfg_path)["cluster_id"]

def hdrs():
    return {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}

# ── API helpers ──────────────────────────────────────────────────────────────
def submit(cluster_id, notebook_path, params, run_name):
    url  = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"
    body = {
        "run_name": run_name,
        "existing_cluster_id": cluster_id,
        "timeout_seconds": TIMEOUT_SECONDS,
        "notebook_task": {"notebook_path": notebook_path, "base_parameters": params},
    }
    r = requests.post(url, headers=hdrs(), json=body, timeout=30)
    r.raise_for_status()
    data = r.json()
    run_id = data["run_id"]
    return run_id, f"{DATABRICKS_HOST}/#job/runs/{run_id}"

def get_status(run_id):
    r = requests.get(f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get",
                     headers=hdrs(), params={"run_id": run_id}, timeout=30)
    r.raise_for_status()
    d = r.json(); s = d.get("state", {})
    return s.get("life_cycle_state"), s.get("result_state"), s.get("state_message", "")

def get_output(run_id):
    r = requests.get(f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output",
                     headers=hdrs(), params={"run_id": run_id}, timeout=30)
    r.raise_for_status()
    d = r.json()
    return d.get("error"), d.get("error_trace"), d.get("notebook_output", {})

def wait(run_id, url):
    print(f"  Run URL : {url}")
    terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    start = time.time()
    while True:
        lc, rs, msg = get_status(run_id)
        elapsed = int(time.time() - start)
        print(f"  [{datetime.now().strftime('%H:%M:%S')}] +{elapsed:>4}s  {lc:<20} result={rs or '—'}")
        if lc in terminal:
            return lc, rs, msg
        if elapsed > TIMEOUT_SECONDS:
            return lc, None, "timeout"
        time.sleep(POLL_INTERVAL)

def run_notebook(cluster_id, step_name, notebook_path, params):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = f"vrh_v2_{step_name}_{ts}"
    print(f"\n{'='*60}")
    print(f"  Step    : {step_name}")
    print(f"  Notebook: {notebook_path}")
    print(f"  Params  : {json.dumps(params)}")
    run_id, url = submit(cluster_id, notebook_path, params, run_name)
    print(f"  Run ID  : {run_id}")
    lc, rs, msg = wait(run_id, url)
    err, trace, nb_out = get_output(run_id)

    result = rs or lc
    log_path = LOG_DIR / f"run_{ts}_{step_name}_{result}.log"
    with open(log_path, "w") as f:
        f.write(f"Step: {step_name}\nNotebook: {notebook_path}\nRun ID: {run_id}\nURL: {url}\nResult: {result}\n\n")
        if err:   f.write(f"ERROR:\n{err}\n\n")
        if trace: f.write(f"TRACE:\n{trace}\n\n")
        f.write(f"PARAMS:\n{json.dumps(params, indent=2)}\n")

    if result == "SUCCESS":
        print(f"  ✓ {step_name} SUCCESS")
    else:
        print(f"  ✗ {step_name} FAILED — result={result}")
        if err:   print(f"\n  ERROR:\n{err[:500]}")
        if trace: print(f"\n  TRACE (last 1000 chars):\n...{trace[-1000:]}")
        print(f"\n  Log: {log_path}")

    return result, log_path

# ── Step param builders ──────────────────────────────────────────────────────
def params_for(step, table_name, data_date):
    catalog   = "viriyah_cdqm_poc"
    fw_schema = "control_fw"
    full_table = f"{catalog}.silver.{table_name}"
    prcs_nm    = f"EDP_MATCHING_V2_{table_name.upper()}_DATE_{data_date}"
    if step in ("setup_ddl", "setup_copy", "setup_config"):
        return {}   # these notebooks take no parameters
    if step == "main":
        return {"table_name": table_name, "data_date": data_date}
    if step == "pre_vld":
        return {
            "PARAMS": f"{full_table}^|{catalog}.{fw_schema}.chv_pre_validation_result_v2^|{data_date}^|EDP_PRE_VLD_V2_{table_name.upper()}^|1^|EDP_PRE_VLD_V2_{table_name.upper()}^|1",
            "ENV": ENV,
        }
    if step == "match":
        return {
            "PARAMS": f"{full_table}^|{data_date}^|{prcs_nm}^|1^|{prcs_nm}^|1",
            "ENV": ENV,
        }
    return {}

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("VRH CHV v2 Runner")
    print(f"Mode      : {MODE}")
    print(f"Table     : {TABLE_NAME}")
    print(f"Data Date : {DATA_DATE}")
    print("=" * 60)

    load_auth()
    cluster_id = load_cluster_id()
    print(f"Host      : {DATABRICKS_HOST}")
    print(f"Cluster   : {cluster_id}")

    # Determine steps to run
    if MODE == "setup":
        steps = ["setup_ddl", "setup_copy", "setup_config"]
    elif MODE == "all":
        steps = ["setup_ddl", "setup_copy", "setup_config", "main"]
    elif MODE in NOTEBOOK_PATHS:
        steps = [MODE]
    else:
        print(f"Unknown mode: {MODE}")
        sys.exit(1)

    results = {}
    for step in steps:
        params = params_for(step, TABLE_NAME, DATA_DATE)
        result, log = run_notebook(cluster_id, step, NOTEBOOK_PATHS[step], params)
        results[step] = result
        if result != "SUCCESS":
            print(f"\n✗ Stopping — {step} failed")
            sys.exit(1)

    print(f"\n{'='*60}")
    for step, r in results.items():
        mark = "✓" if r == "SUCCESS" else "✗"
        print(f"  {mark} {step}: {r}")
    print(f"{'='*60}")
    sys.exit(0)

if __name__ == "__main__":
    main()
