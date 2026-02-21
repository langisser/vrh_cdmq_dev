#!/usr/bin/env python3
"""
CHV Match & Merge Pipeline Runner
===================================
Submit vrh_chv_main (or individual notebooks) to Databricks via runs/submit API.
Waits for completion, then saves full output + error trace to a local log file.

Usage:
    source setup_env.sh && source venv/bin/activate
    python3 tests/run_chv_pipeline.py

After run:
    - If SUCCESS: tests/logs/run_<timestamp>_SUCCESS.log
    - If FAILED:  tests/logs/run_<timestamp>_FAILED.log  ← share this with Claude

Dev loop:
    run → error → Claude reads log → fix code → run again
"""

import os
import sys
import json
import time
import requests
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────
# CONFIG — แก้ตรงนี้ก่อน run
# ─────────────────────────────────────────────

DATABRICKS_HOST   = ""   # จะดึงจาก .databrickscfg อัตโนมัติ ถ้าปล่าว
DATABRICKS_TOKEN  = ""   # จะดึงจาก .databrickscfg อัตโนมัติ ถ้าปล่าว

# เลือก notebook ที่จะ run
# "main"       = vrh_chv_main  (run ทั้ง pipeline)
# "pre_vld"    = vrh_chv_pre_validation (run เฉพาะ pre-validation)
# "match"      = vrh_chv_match (run เฉพาะ matching)
RUN_MODE = "main"

# Parameters
TABLE_NAME = "SOURCE_MOTOR"
DATA_DATE  = "2026-01-05"
ENV        = "dev"

# Workspace paths
WORKSPACE_BASE = "/Workspace/Users/khachornpop@inteltion.com/vrh"

NOTEBOOK_PATHS = {
    "main"    : f"{WORKSPACE_BASE}/match_and_merge/vrh_chv_main",
    "pre_vld" : f"{WORKSPACE_BASE}/match_and_merge/vrh_chv_pre_validation",
    "match"   : f"{WORKSPACE_BASE}/match_and_merge/vrh_chv_match",
}

# Params per notebook
def build_params(mode, table_name, data_date, env):
    catalog    = "viriyah_cdqm_poc"
    fw_schema  = "control_fw"
    full_table = f"{catalog}.silver.{table_name}"
    prcs_nm    = f"EDP_MATCHING_{table_name.upper()}_DATE_{data_date}"

    if mode == "main":
        return {
            "table_name": table_name,
            "data_date":  data_date,
        }
    elif mode == "pre_vld":
        return {
            "PARAMS": f"{full_table}^|{catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT^|{data_date}^|EDP_PRE_VLD_{table_name.upper()}^|1^|EDP_PRE_VLD_{table_name.upper()}^|1",
            "ENV":    env,
        }
    elif mode == "match":
        return {
            "PARAMS": f"{full_table}^|{data_date}^|{prcs_nm}^|1^|{prcs_nm}^|1",
            "ENV":    env,
        }

# Timeout & polling
TIMEOUT_SECONDS = 1800   # 30 min max wait
POLL_INTERVAL   = 10     # check every 10 sec

# Log directory
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)

# ─────────────────────────────────────────────
# Read .databrickscfg if HOST/TOKEN not set
# ─────────────────────────────────────────────

def _parse_cfg(cfg_path):
    """Parse key=value pairs from .databrickscfg (handles spaces around =)"""
    values = {}
    with open(cfg_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("[") or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                values[k.strip()] = v.strip()
    return values


def load_databricks_config():
    global DATABRICKS_HOST, DATABRICKS_TOKEN

    if DATABRICKS_HOST and DATABRICKS_TOKEN:
        return

    cfg_path = Path(__file__).parent.parent / ".databrickscfg"
    if not cfg_path.exists():
        raise FileNotFoundError(f".databrickscfg not found at {cfg_path}")

    cfg = _parse_cfg(cfg_path)

    host = cfg.get("host", "")
    if not host:
        raise ValueError("host not found in .databrickscfg")
    DATABRICKS_HOST = host.rstrip("/")

    # Token in config — use directly
    if cfg.get("token"):
        DATABRICKS_TOKEN = cfg["token"]
        return

    # azure-cli auth — get token via az CLI
    auth_type = cfg.get("auth_type", "")
    if "azure-cli" in auth_type:
        import subprocess, json as _json
        print("  Auth: azure-cli — fetching token via az CLI ...")
        result = subprocess.run(
            ["az", "account", "get-access-token",
             "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
             "--output", "json"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0:
            raise RuntimeError(f"az CLI error: {result.stderr.strip()}")
        token_data = _json.loads(result.stdout)
        DATABRICKS_TOKEN = token_data["accessToken"]
        print("  Token obtained from azure-cli\n")
        return

    raise ValueError("Cannot obtain token: no 'token' and auth_type is not azure-cli")


# ─────────────────────────────────────────────
# Read cluster_id from .databrickscfg
# ─────────────────────────────────────────────

def load_cluster_id():
    cfg_path = Path(__file__).parent.parent / ".databrickscfg"
    cfg = _parse_cfg(cfg_path)
    cluster_id = cfg.get("cluster_id", "")
    if not cluster_id:
        raise ValueError("cluster_id not found in .databrickscfg")
    return cluster_id

# ─────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────

def headers():
    return {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type":  "application/json",
    }


def submit_run(cluster_id, notebook_path, params):
    url     = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/submit"
    payload = {
        "run_name":            f"vrh_pipeline_{RUN_MODE}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        "existing_cluster_id": cluster_id,
        "timeout_seconds":     TIMEOUT_SECONDS,
        "notebook_task": {
            "notebook_path":   notebook_path,
            "base_parameters": params,
        },
    }
    resp = requests.post(url, headers=headers(), json=payload, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    run_id       = data["run_id"]
    run_page_url = f"{DATABRICKS_HOST}/#job/runs/{run_id}"
    return run_id, run_page_url


def get_run_status(run_id):
    url    = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get"
    resp   = requests.get(url, headers=headers(), params={"run_id": run_id}, timeout=30)
    resp.raise_for_status()
    result = resp.json()
    state  = result.get("state", {})
    return {
        "life_cycle_state": state.get("life_cycle_state"),
        "result_state":     state.get("result_state"),
        "state_message":    state.get("state_message", ""),
        "run_page_url":     result.get("run_page_url"),
        "start_time":       result.get("start_time"),
        "end_time":         result.get("end_time"),
    }


def get_run_output(run_id):
    url  = f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get-output"
    resp = requests.get(url, headers=headers(), params={"run_id": run_id}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return {
        "notebook_output": data.get("notebook_output", {}),
        "error":           data.get("error"),
        "error_trace":     data.get("error_trace"),
    }

# ─────────────────────────────────────────────
# Wait loop
# ─────────────────────────────────────────────

def wait_for_run(run_id, run_page_url):
    print(f"\n  Run URL : {run_page_url}")
    print(f"  Polling every {POLL_INTERVAL}s ...\n")

    terminal = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}
    start    = time.time()

    while True:
        elapsed = int(time.time() - start)
        status  = get_run_status(run_id)
        lc      = status["life_cycle_state"]
        rs      = status.get("result_state") or "—"
        ts      = datetime.now().strftime("%H:%M:%S")

        print(f"  [{ts}] +{elapsed:>4}s  {lc:<20} result={rs}")

        if lc in terminal:
            print()
            return status

        if elapsed > TIMEOUT_SECONDS:
            print(f"\n  Timeout after {TIMEOUT_SECONDS}s")
            return status

        time.sleep(POLL_INTERVAL)

# ─────────────────────────────────────────────
# Save log
# ─────────────────────────────────────────────

def save_log(run_id, run_page_url, params, notebook_path,
             final_status, output, log_lines):

    result_state = final_status.get("result_state") or final_status.get("life_cycle_state")
    ts           = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path     = LOG_DIR / f"run_{ts}_{result_state}.log"

    with open(log_path, "w") as f:
        f.write("=" * 80 + "\n")
        f.write(f"VRH Pipeline Run Log\n")
        f.write(f"Timestamp   : {datetime.now().isoformat()}\n")
        f.write(f"Mode        : {RUN_MODE}\n")
        f.write(f"Notebook    : {notebook_path}\n")
        f.write(f"Run ID      : {run_id}\n")
        f.write(f"Run URL     : {run_page_url}\n")
        f.write(f"Result      : {result_state}\n")
        f.write("=" * 80 + "\n\n")

        f.write("PARAMETERS:\n")
        f.write(json.dumps(params, indent=2) + "\n\n")

        f.write("STATUS:\n")
        f.write(json.dumps(final_status, indent=2) + "\n\n")

        if output.get("error"):
            f.write("=" * 80 + "\n")
            f.write("ERROR:\n")
            f.write(output["error"] + "\n\n")

        if output.get("error_trace"):
            f.write("ERROR TRACE:\n")
            f.write(output["error_trace"] + "\n\n")

        if output.get("notebook_output"):
            f.write("NOTEBOOK OUTPUT:\n")
            f.write(json.dumps(output["notebook_output"], indent=2) + "\n\n")

        if log_lines:
            f.write("=" * 80 + "\n")
            f.write("EXECUTION LOG:\n")
            f.write("\n".join(log_lines) + "\n")

    return log_path

# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────

def main():
    print("=" * 80)
    print("VRH CHV Pipeline Runner")
    print(f"Mode      : {RUN_MODE}")
    print(f"Table     : {TABLE_NAME}")
    print(f"Data Date : {DATA_DATE}")
    print(f"Started   : {datetime.now()}")
    print("=" * 80)

    # Load config
    load_databricks_config()
    cluster_id = load_cluster_id()
    print(f"\nHost      : {DATABRICKS_HOST}")
    print(f"Cluster   : {cluster_id}")

    # Build params
    notebook_path = NOTEBOOK_PATHS[RUN_MODE]
    params        = build_params(RUN_MODE, TABLE_NAME, DATA_DATE, ENV)

    print(f"\nNotebook  : {notebook_path}")
    print(f"Params    : {json.dumps(params, indent=2)}\n")

    # Submit
    print("Submitting run ...")
    run_id, run_page_url = submit_run(cluster_id, notebook_path, params)
    print(f"  Run ID  : {run_id}")

    log_lines = [f"Submitted run_id={run_id}  url={run_page_url}"]

    # Wait
    final_status = wait_for_run(run_id, run_page_url)
    result_state = final_status.get("result_state") or final_status.get("life_cycle_state")

    # Get output / error
    output = get_run_output(run_id)

    # Save log
    log_path = save_log(run_id, run_page_url, params, notebook_path,
                        final_status, output, log_lines)

    # Print summary
    print("=" * 80)
    if result_state == "SUCCESS":
        print(f"  RESULT  : SUCCESS")
    else:
        print(f"  RESULT  : {result_state}  ← FAILED")
        if output.get("error"):
            print(f"\n  ERROR   : {output['error'][:200]}...")
        print(f"\n  → Share this log with Claude to investigate:")

    print(f"  Log     : {log_path}")
    print(f"  Run URL : {run_page_url}")
    print("=" * 80)

    return 0 if result_state == "SUCCESS" else 1


if __name__ == "__main__":
    sys.exit(main())
