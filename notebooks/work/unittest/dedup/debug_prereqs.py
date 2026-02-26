# Databricks notebook source
# Check pre-requisite tables for vrh_chv_match_v2

# COMMAND ----------

catalog = 'viriyah_cdqm_poc'
fw = 'control_fw'

# CHV_PARAM_GENERAL_V2 — DATE group
print("=== CHV_PARAM_GENERAL_V2 (PARAM_GROUP_NAME=DATE) ===")
spark.sql(f"SELECT * FROM {catalog}.{fw}.CHV_PARAM_GENERAL_V2 WHERE UPPER(PARAM_GROUP_NAME) = 'DATE'").show(truncate=False)

# CHV_PRE_VALIDATION_RESULT_V2 — distinct tables
print("=== CHV_PRE_VALIDATION_RESULT_V2 (distinct tables) ===")
spark.sql(f"SELECT DISTINCT `TABLE`, DATA_DT, RESULT FROM {catalog}.{fw}.CHV_PRE_VALIDATION_RESULT_V2 ORDER BY DATA_DT DESC LIMIT 20").show(truncate=False)

# Check if devtest has any pre-validation results
print("=== CHV_PRE_VALIDATION_RESULT_V2 (devtest) ===")
spark.sql(f"SELECT COUNT(*) as cnt FROM {catalog}.{fw}.CHV_PRE_VALIDATION_RESULT_V2 WHERE lower(`TABLE`) LIKE '%devtest%'").show()

dbutils.notebook.exit("OK")
