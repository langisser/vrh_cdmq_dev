#!/usr/bin/env python3
"""
Databricks SQL Query Helper
============================
Run SQL against the vrh cluster without needing a SQL warehouse.
Uses Databricks Command Execution API (api/1.2) — same cluster as jobs.

Usage:
    # Inline SQL
    python3 tests/query.py "SELECT * FROM ..."

    # From .sql file
    python3 tests/query.py --file tests/sql/my_query.sql

    # Save result to file
    python3 tests/query.py "SELECT ..." --out tests/sql/result.txt

Auth: azure-cli (auto — no token needed in config)
"""

import sys
import os
import json
import time
import subprocess
import requests
import argparse
from pathlib import Path

# ── Config ────────────────────────────────────────────────
CFG_PATH    = Path(__file__).parent.parent / ".databrickscfg"
CTX_CACHE   = Path(__file__).parent / ".ctx_cache"   # reuse context between runs
HOST        = "https://adb-7405612978007880.0.azuredatabricks.net"
CLUSTER_ID  = "0130-031624-0nmpnh8g"
POLL_SLEEP  = 1.5
MAX_POLLS   = 60

# ── Auth ──────────────────────────────────────────────────

def get_token():
    r = subprocess.run(
        ["az", "account", "get-access-token",
         "--resource", "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True, timeout=30
    )
    if r.returncode != 0:
        raise RuntimeError(f"az CLI error: {r.stderr.strip()}")
    return r.stdout.strip()

# ── Context (reuse if alive) ──────────────────────────────

def get_or_create_context(token):
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # try cached context
    if CTX_CACHE.exists():
        ctx_id = CTX_CACHE.read_text().strip()
        r = requests.get(f"{HOST}/api/1.2/contexts/status",
                         params={"clusterId": CLUSTER_ID, "contextId": ctx_id},
                         headers=hdrs, timeout=10)
        if r.ok and r.json().get("status") == "Running":
            return ctx_id

    # create new
    r = requests.post(f"{HOST}/api/1.2/contexts/create",
                      headers=hdrs,
                      json={"language": "sql", "clusterId": CLUSTER_ID},
                      timeout=30)
    r.raise_for_status()
    ctx_id = r.json()["id"]
    CTX_CACHE.write_text(ctx_id)
    return ctx_id

# ── Execute & Poll ────────────────────────────────────────

def run_sql(sql, token, ctx_id):
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # submit
    r = requests.post(f"{HOST}/api/1.2/commands/execute",
                      headers=hdrs,
                      json={"language": "sql", "clusterId": CLUSTER_ID,
                            "contextId": ctx_id, "command": sql},
                      timeout=30)
    r.raise_for_status()
    cmd_id = r.json()["id"]

    # poll
    for _ in range(MAX_POLLS):
        time.sleep(POLL_SLEEP)
        r = requests.get(f"{HOST}/api/1.2/commands/status",
                         params={"clusterId": CLUSTER_ID,
                                 "contextId": ctx_id,
                                 "commandId": cmd_id},
                         headers=hdrs, timeout=15)
        r.raise_for_status()
        data = r.json()
        status = data.get("status")
        if status in ("Finished", "Error", "Cancelled"):
            return data.get("results", {})

    raise TimeoutError("Query did not finish in time")

# ── Format result ─────────────────────────────────────────

def format_table(results):
    if results.get("resultType") == "error":
        return f"ERROR:\n{results.get('cause', '')}"

    if results.get("resultType") != "table":
        return json.dumps(results, indent=2, ensure_ascii=False)

    schema  = results.get("schema", [])
    data    = results.get("data", [])
    headers = [col["name"] for col in schema]

    if not data:
        return "(no rows)\n\n" + " | ".join(headers)

    col_w = [
        max(len(h), max((len(str(row[i])) for row in data), default=0))
        for i, h in enumerate(headers)
    ]

    sep   = " | "
    lines = []
    lines.append(sep.join(h.ljust(col_w[i]) for i, h in enumerate(headers)))
    lines.append("-+-".join("-" * w for w in col_w))
    for row in data:
        lines.append(sep.join(str(v).ljust(col_w[i]) for i, v in enumerate(row)))
    lines.append(f"\n  {len(data)} row(s)")
    return "\n".join(lines)

# ── Main ──────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run SQL on Databricks cluster")
    parser.add_argument("sql",        nargs="?", help="Inline SQL string")
    parser.add_argument("--file","-f",           help="Path to .sql file")
    parser.add_argument("--out", "-o",           help="Save result to file")
    args = parser.parse_args()

    # resolve SQL
    if args.file:
        sql = Path(args.file).read_text().strip()
    elif args.sql:
        sql = args.sql.strip()
    else:
        parser.print_help()
        sys.exit(1)

    print(f"SQL:\n  {sql[:200]}{'...' if len(sql)>200 else ''}\n")

    token  = get_token()
    ctx_id = get_or_create_context(token)
    print(f"Context: {ctx_id}")

    results = run_sql(sql, token, ctx_id)
    output  = format_table(results)

    print("\n" + "=" * 60)
    print(output)
    print("=" * 60)

    if args.out:
        Path(args.out).parent.mkdir(parents=True, exist_ok=True)
        Path(args.out).write_text(output)
        print(f"\nSaved to: {args.out}")

    return 1 if results.get("resultType") == "error" else 0


if __name__ == "__main__":
    sys.exit(main())
