# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # cleanup_perf_data — Remove all perf test data for DATA_DT='2025-02-01'
# MAGIC
# MAGIC Deletes all rows inserted by the performance test from:
# MAGIC - `source_motor_devtest` (DATA_DT='2025-02-01')
# MAGIC - `trust_source_devtest` (DATA_DT='2025-02-01')
# MAGIC - All intermediate pipeline tables (DATA_DT='2025-02-01')
# MAGIC - Dedup output tables (rows whose BKEY came from this perf run)
# MAGIC
# MAGIC Safe to run multiple times (idempotent).

# COMMAND ----------

CATALOG      = "viriyah_cdqm_poc"
SILVER       = "silver"
CTRL         = "control_fw"
PERF_DATA_DT = "2025-02-01"

MOTOR_TABLE  = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE  = f"{CATALOG}.{SILVER}.trust_source_devtest"
BKEY_TABLE   = f"{CATALOG}.{SILVER}.chv_table_bkey_v2"

print(f"Cleaning up DATA_DT = '{PERF_DATA_DT}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Identify affected BKEYs from the perf run

# COMMAND ----------

perf_bkeys_df = spark.sql(f"""
  SELECT DISTINCT bkey
  FROM {BKEY_TABLE}
  WHERE data_dt = '{PERF_DATA_DT}'
    AND lower(`table`) = lower('{MOTOR_TABLE}')
""")
perf_bkeys_df.createOrReplaceTempView("perf_bkeys")

bkey_count = perf_bkeys_df.count()
print(f"BKEYs created in perf run: {bkey_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Delete from dedup output tables (by affected BKEYs)

# COMMAND ----------

DEDUP_TABLES = [
    f"{CATALOG}.{SILVER}.dedup_customer_name",
    f"{CATALOG}.{SILVER}.dedup_province",
    f"{CATALOG}.{SILVER}.dedup_gender",
    f"{CATALOG}.{SILVER}.dedup_email",
    f"{CATALOG}.{SILVER}.dedup_phone",
    f"{CATALOG}.{SILVER}.dedup_name_variant_report",
]

for tbl in DEDUP_TABLES:
    spark.sql(f"""
        DELETE FROM {tbl}
        WHERE bkey IN (SELECT bkey FROM perf_bkeys)
    """)
    print(f"  Cleaned: {tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Delete from intermediate/control tables

# COMMAND ----------

ctrl_tables = [
    (f"{CATALOG}.{CTRL}.chv_pre_validation_result_v2", "DATA_DT"),
    (f"{CATALOG}.{CTRL}.chv_matching_log_v2",          "DATA_DT"),
    (f"{CATALOG}.{CTRL}.chv_matching_result_v2",        "DATA_DT"),
    (f"{CATALOG}.{SILVER}.chv_table_bkey_v2",           "data_dt"),
]

for tbl, col in ctrl_tables:
    spark.sql(f"DELETE FROM {tbl} WHERE {col} = '{PERF_DATA_DT}'")
    print(f"  Cleaned: {tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Delete from source tables

# COMMAND ----------

spark.sql(f"DELETE FROM {MOTOR_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'")
print(f"  Cleaned: {MOTOR_TABLE}")

spark.sql(f"DELETE FROM {TRUST_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'")
print(f"  Cleaned: {TRUST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify

# COMMAND ----------

checks = [
    (MOTOR_TABLE,                                         f"DATA_DT = '{PERF_DATA_DT}'"),
    (TRUST_TABLE,                                         f"DATA_DT = '{PERF_DATA_DT}'"),
    (BKEY_TABLE,                                          f"data_dt = '{PERF_DATA_DT}'"),
    (f"{CATALOG}.{CTRL}.chv_pre_validation_result_v2",   f"DATA_DT = '{PERF_DATA_DT}'"),
    (f"{CATALOG}.{CTRL}.chv_matching_log_v2",             f"DATA_DT = '{PERF_DATA_DT}'"),
]

print("\n=== Verification ===")
all_ok = True
for tbl, cond in checks:
    n = spark.sql(f"SELECT COUNT(*) AS n FROM {tbl} WHERE {cond}").collect()[0]["n"]
    status = "✓" if n == 0 else "✗"
    if n != 0:
        all_ok = False
    print(f"  {status} {tbl.split('.')[-1]:40s} remaining rows: {n:,}")

if all_ok:
    print("\nAll perf data cleaned up successfully.")
    dbutils.notebook.exit("SUCCESS")
else:
    print("\nWARNING: Some rows were not cleaned. Check above.")
    dbutils.notebook.exit("PARTIAL_CLEANUP")
