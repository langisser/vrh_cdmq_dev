#!/usr/bin/env python3
"""
debug_prereqs.py — ตรวจสอบ config tables และ pre-validation results

Usage:
    source /home/khaw/ClaudeCode/databricks_dev_local/venv/bin/activate
    python3 scripts/investigate/debug_prereqs.py
"""
import os, sys
sys.path.insert(0, '/home/khaw/ClaudeCode/databricks_dev_local')
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

catalog = 'viriyah_cdqm_poc'
fw = 'control_fw'

print("=== chv_config_check_pre_validation_v2 (all rows) ===")
spark.sql(f"SELECT * FROM {catalog}.{fw}.chv_config_check_pre_validation_v2 ORDER BY MATCHING_RULES").show(50, truncate=False)

print("\n=== chv_config_pre_validation_v2 (schema) ===")
spark.sql(f"DESCRIBE {catalog}.{fw}.chv_config_pre_validation_v2").show(50, truncate=False)

print("\n=== CHV_PRE_VALIDATION_RESULT_V2 (devtest data) ===")
spark.sql(f"""
    SELECT DISTINCT `TABLE`, DATA_DT, RESULT, COUNT(*) as cnt
    FROM {catalog}.{fw}.CHV_PRE_VALIDATION_RESULT_V2
    WHERE lower(`TABLE`) LIKE '%devtest%'
    GROUP BY `TABLE`, DATA_DT, RESULT
""").show(truncate=False)
