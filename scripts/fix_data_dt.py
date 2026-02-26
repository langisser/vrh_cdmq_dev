#!/usr/bin/env python3
"""Fix DATA_DT from 'test-2026-02-26' to '2026-02-26' in all devtest tables + pre-validation"""
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

catalog = 'viriyah_cdqm_poc'
silver  = 'silver'
fw      = 'control_fw'
OLD_DT  = 'test-2026-02-26'
NEW_DT  = '2026-02-26'

print(f"Will rename DATA_DT from '{OLD_DT}' → '{NEW_DT}' in devtest tables")

# Check counts before
for tbl in [f'{catalog}.{silver}.source_motor_devtest', f'{catalog}.{silver}.trust_source_devtest']:
    cnt = spark.sql(f"SELECT COUNT(*) FROM {tbl} WHERE DATA_DT = '{OLD_DT}'").collect()[0][0]
    print(f"  {tbl}: {cnt} rows with DATA_DT='{OLD_DT}'")

pv_cnt = spark.sql(f"SELECT COUNT(*) FROM {catalog}.{fw}.CHV_PRE_VALIDATION_RESULT_V2 WHERE DATA_DT = '{OLD_DT}'").collect()[0][0]
print(f"  CHV_PRE_VALIDATION_RESULT_V2: {pv_cnt} rows with DATA_DT='{OLD_DT}'")
print("Use data_prep_dedup.py with updated DATA_DT to re-load with correct date.")
