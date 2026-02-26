# Databricks notebook source

# COMMAND ----------

catalog = 'viriyah_cdqm_poc'
fw = 'control_fw'
table = 'viriyah_cdqm_poc.silver.source_motor_devtest'

# Check PK config
print("=== chv_config_pk_v2 (devtest) ===")
pk_df = spark.sql(f"SELECT * FROM {catalog}.{fw}.chv_config_pk_v2 WHERE lower(`TABLE`) LIKE '%devtest%'")
pk_df.show(truncate=False)

# Check matching config
print("=== chv_config_matching_v2 (rules 31-39) ===")
m_df = spark.sql(f"SELECT MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, WEIGHT, TIER FROM {catalog}.{fw}.chv_config_matching_v2 WHERE MATCHING_RULES BETWEEN 31 AND 39 ORDER BY MATCHING_RULES")
m_df.show(truncate=False)

# Check filtered config as the notebook would see it
print(f"=== Config for MAIN_TABLE = {table} ===")
filtered = spark.sql(f"""
  SELECT MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, WEIGHT, TIER, ACT_F
  FROM {catalog}.{fw}.chv_config_matching_v2
  WHERE lower(MAIN_TABLE) = lower('{table}') AND ACT_F = 1
  ORDER BY TIER, MATCHING_RULES
""")
filtered.show(truncate=False)
print(f"Row count: {filtered.count()}")

# Check source_motor_devtest data
print("=== source_motor_devtest ===")
src = spark.sql(f"SELECT COUNT(*) as cnt, DATA_DT FROM viriyah_cdqm_poc.silver.source_motor_devtest GROUP BY DATA_DT")
src.show()

dbutils.notebook.exit("OK")
