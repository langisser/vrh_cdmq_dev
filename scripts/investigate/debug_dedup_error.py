#!/usr/bin/env python3
"""
debug_dedup_error.py — ตรวจสอบ schema ของ chv_table_bkey_v2 และ chv_matching_log_v2

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/investigate/debug_dedup_error.py
"""
import os, sys
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

for schema in ['control_fw', 'silver']:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM viriyah_cdqm_poc.{schema}.chv_table_bkey_v2").collect()[0][0]
        print(f"viriyah_cdqm_poc.{schema}.chv_table_bkey_v2: EXISTS ({cnt} rows)")
    except:
        print(f"viriyah_cdqm_poc.{schema}.chv_table_bkey_v2: NOT FOUND")

for schema in ['control_fw', 'silver']:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM viriyah_cdqm_poc.{schema}.chv_matching_log_v2").collect()[0][0]
        print(f"viriyah_cdqm_poc.{schema}.chv_matching_log_v2: EXISTS ({cnt} rows)")
    except:
        print(f"viriyah_cdqm_poc.{schema}.chv_matching_log_v2: NOT FOUND")
