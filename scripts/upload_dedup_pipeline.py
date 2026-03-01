#!/usr/bin/env python3
"""
upload_dedup_pipeline.py — Upload dedup pipeline notebooks to Databricks workspace

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/upload_dedup_pipeline.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import get_workspace_client, upload_to_workspace

BASE     = '/home/khaw/ClaudeCode/vrh_cdmq_dev/notebooks/work'
WS_MATCH = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge'
WS_DEDUP = '/Workspace/Users/khachornpop@inteltion.com/vrh/dedup'

UPLOADS = [
    ("ddl_dedup_tables",      f"{BASE}/match_and_merge/insert_scripts/ddl_dedup_tables.py",   f"{WS_MATCH}/insert_scripts/ddl_dedup_tables"),
    ("ddl_source_devtest",    f"{BASE}/match_and_merge/insert_scripts/ddl_source_devtest.py", f"{WS_MATCH}/insert_scripts/ddl_source_devtest"),
    ("config_devtest",        f"{BASE}/match_and_merge/insert_scripts/config_devtest.py",      f"{WS_MATCH}/insert_scripts/config_devtest"),
    ("data_prep_dedup",       f"{BASE}/unittest/dedup/data_prep_dedup.py",                     f"{WS_DEDUP}/unittest/dedup/data_prep_dedup"),
    ("ddl_dedup_outputs",     f"{BASE}/dedup/ddl_dedup_tables.py",                             f"{WS_DEDUP}/ddl_dedup_tables"),
    ("dedup_customer_name",   f"{BASE}/dedup/dedup_customer_name.py",                          f"{WS_DEDUP}/dedup_customer_name"),
    ("dedup_province",        f"{BASE}/dedup/dedup_province.py",                               f"{WS_DEDUP}/dedup_province"),
    ("dedup_gender",          f"{BASE}/dedup/dedup_gender.py",                                 f"{WS_DEDUP}/dedup_gender"),
    ("dedup_email",           f"{BASE}/dedup/dedup_email.py",                                  f"{WS_DEDUP}/dedup_email"),
    ("dedup_phone",           f"{BASE}/dedup/dedup_phone.py",                                  f"{WS_DEDUP}/dedup_phone"),
    ("dedup_name_variant_report", f"{BASE}/dedup/dedup_name_variant_report.py",              f"{WS_DEDUP}/dedup_name_variant_report"),
    ("test_dedup_v2",         f"{BASE}/unittest/dedup/test_dedup_v2.py",                       f"{WS_DEDUP}/unittest/dedup/test_dedup_v2"),
]

w = get_workspace_client()
w.workspace.mkdirs(f"{WS_MATCH}/insert_scripts")
w.workspace.mkdirs(f"{WS_DEDUP}/unittest/dedup")
print("Workspace directories ready")

print("=== Uploading notebooks ===")
failed = False
for label, local_path, ws_path in UPLOADS:
    print(f"  [{label}] ... ", end="", flush=True)
    result = upload_to_workspace(local_path, ws_path, auth_method="azure_cli")
    if result["status"] == "SUCCESS":
        print("OK")
    else:
        print(f"FAILED: {result.get('error')}")
        failed = True

print()
if failed:
    print("Some uploads failed — check errors above.")
    sys.exit(1)

print("=== All uploads complete ===")
print()
print("Run order in Databricks:")
print("  1. match_and_merge/insert_scripts/ddl_source_devtest      (once — create tables)")
print("  2. match_and_merge/insert_scripts/config_devtest           (once — insert config rows)")
print("  3. dedup/unittest/dedup/data_prep_dedup                   (load 9+1 test rows + pre-validation results)")
print("  4. match_and_merge/vrh_chv_match_v2  PARAMS: viriyah_cdqm_poc.silver.source_motor_devtest^|test-2026-02-26^|TEST_MATCH_DEDUP^|1^|TEST_MATCH_DEDUP^|1")
print("  5. dedup/vrh_chv_dedup_v2  PARAMS: viriyah_cdqm_poc.silver.source_motor_devtest^|test-2026-02-26^|TEST_DEDUP^|1^|TEST_DEDUP^|1")
print("     Widgets: SOURCE_TABLE=viriyah_cdqm_poc.silver.source_motor_devtest")
print("              TRUST_TABLE=viriyah_cdqm_poc.silver.trust_source_devtest")
print("  6. dedup/unittest/dedup/test_dedup_v2                     (assert results)")
