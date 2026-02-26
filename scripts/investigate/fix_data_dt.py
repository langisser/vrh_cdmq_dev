#!/usr/bin/env python3
"""
fix_data_dt.py — ตรวจสอบ / แก้ DATA_DT format ใน devtest tables

Usage:
    source /home/khaw/ClaudeCode/databricks_dev_local/venv/bin/activate
    python3 scripts/investigate/fix_data_dt.py
"""
import os, sys
sys.path.insert(0, '/home/khaw/ClaudeCode/databricks_dev_local')
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

catalog = 'viriyah_cdqm_poc'
silver  = 'silver'
fw      = 'control_fw'
OLD_DT  = 'test-2026-02-26'
NEW_DT  = '2026-02-26'

print(f"Checking DATA_DT='{OLD_DT}' in devtest tables...")

for tbl in [f'{catalog}.{silver}.source_motor_devtest', f'{catalog}.{silver}.trust_source_devtest']:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl} WHERE DATA_DT = '{OLD_DT}'").collect()[0][0]
    print(f"  {tbl}: {cnt} rows with DATA_DT='{OLD_DT}'")

pv_cnt = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{fw}.CHV_PRE_VALIDATION_RESULT_V2 WHERE DATA_DT = '{OLD_DT}'").collect()[0][0]
print(f"  CHV_PRE_VALIDATION_RESULT_V2: {pv_cnt} rows with DATA_DT='{OLD_DT}'")
print(f"\nNote: re-load data with correct DATA_DT='{NEW_DT}' via data_prep_dedup notebook")
