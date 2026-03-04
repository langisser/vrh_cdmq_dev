# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Prefix Blocking — Step 1: Backup + Truncate bkey table

# COMMAND ----------

# Backup
spark.sql("DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix")
spark.sql("""
    CREATE TABLE viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix
    AS SELECT * FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
""")

cnt = spark.sql("SELECT COUNT(*) as cnt FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix").collect()[0]['cnt']
print(f"Backup created: {cnt} rows -> chv_table_bkey_v2_backup_prefixfix")

# COMMAND ----------

# Truncate
spark.sql("DELETE FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2")

cnt_after = spark.sql("SELECT COUNT(*) as cnt FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2").collect()[0]['cnt']
print(f"After truncate: {cnt_after} rows (expected 0)")
assert cnt_after == 0, f"Truncate failed — still {cnt_after} rows!"
print("OK — bkey table is empty, ready for rerun")
