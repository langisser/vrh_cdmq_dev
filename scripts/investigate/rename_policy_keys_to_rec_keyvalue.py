#!/usr/bin/env python3
"""
rename_policy_keys_to_rec_keyvalue.py — (one-time) rename column policy_keys → rec_keyvalue
ใน dedup tables ทั้ง 5 ตัว

Usage:
    source /home/khaw/ClaudeCode/databricks_dev_local/venv/bin/activate
    python3 scripts/investigate/rename_policy_keys_to_rec_keyvalue.py
"""
import os, sys
sys.path.insert(0, '/home/khaw/ClaudeCode/databricks_dev_local')
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

catalog = 'viriyah_cdqm_poc'
silver  = 'silver'

DEDUP_TABLES = [
    'dedup_customer_name',
    'dedup_province',
    'dedup_gender',
    'dedup_email',
    'dedup_phone',
]

for tbl in DEDUP_TABLES:
    full = f'{catalog}.{silver}.{tbl}'
    spark.sql(f"ALTER TABLE {full} SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name')")
    print(f'Enabled column mapping on {full}')
    spark.sql(f'ALTER TABLE {full} RENAME COLUMN policy_keys TO rec_keyvalue')
    print(f'Renamed policy_keys -> rec_keyvalue in {full}')

print('\nDone.')
