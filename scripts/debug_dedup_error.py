#!/usr/bin/env python3
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# Find which schema has chv_table_bkey_v2
for schema in ['control_fw', 'silver']:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM viriyah_cdqm_poc.{schema}.chv_table_bkey_v2").collect()[0][0]
        print(f"viriyah_cdqm_poc.{schema}.chv_table_bkey_v2: EXISTS ({cnt} rows)")
    except Exception as e:
        print(f"viriyah_cdqm_poc.{schema}.chv_table_bkey_v2: NOT FOUND")

# Also check chv_matching_log_v2
for schema in ['control_fw', 'silver']:
    try:
        cnt = spark.sql(f"SELECT COUNT(*) FROM viriyah_cdqm_poc.{schema}.chv_matching_log_v2").collect()[0][0]
        print(f"viriyah_cdqm_poc.{schema}.chv_matching_log_v2: EXISTS ({cnt} rows)")
    except:
        print(f"viriyah_cdqm_poc.{schema}.chv_matching_log_v2: NOT FOUND")
