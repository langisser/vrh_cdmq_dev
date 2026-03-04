# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Prefix Blocking — Backup → Truncate → Rerun → Compare
# MAGIC
# MAGIC Steps:
# MAGIC 1. Backup `chv_table_bkey_v2` → `chv_table_bkey_v2_backup_prefixfix`
# MAGIC 2. Truncate `chv_table_bkey_v2` (delete all rows)
# MAGIC 3. Rerun match pipeline for DATA_DT = 2025-01-01 and 2025-01-02
# MAGIC 4. Compare new results vs backup — must be identical

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Backup bkey table

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix")

spark.sql("""
    CREATE TABLE viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix
    AS SELECT * FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
""")

cnt = spark.sql("SELECT COUNT(*) as cnt FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix").collect()[0]['cnt']
print(f"Backup created: {cnt} rows in chv_table_bkey_v2_backup_prefixfix")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Truncate bkey table

# COMMAND ----------

spark.sql("DELETE FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2")

cnt = spark.sql("SELECT COUNT(*) as cnt FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2").collect()[0]['cnt']
print(f"After truncate: {cnt} rows (expected 0)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Compare new vs backup

# COMMAND ----------

# Row counts per date
print("=== Row counts: backup vs new ===")
spark.sql("""
    SELECT 'backup' as src, data_dt, COUNT(*) as cnt
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix
    GROUP BY data_dt
    UNION ALL
    SELECT 'new' as src, data_dt, COUNT(*) as cnt
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
    GROUP BY data_dt
    ORDER BY data_dt, src
""").show()

# COMMAND ----------

# Rows in backup but not in new (by bkey + policy_key combo)
print("=== Rows in BACKUP but missing from NEW ===")
missing = spark.sql("""
    SELECT b.data_dt, b.bkey, b.policy_key, b.source_table
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b
    LEFT ANTI JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2 n
      ON b.bkey = n.bkey AND b.policy_key = n.policy_key AND b.data_dt = n.data_dt
    ORDER BY b.data_dt, b.bkey
""")
missing_cnt = missing.count()
print(f"Missing rows: {missing_cnt}")
if missing_cnt > 0:
    missing.show(50, truncate=False)

# COMMAND ----------

# Rows in new but not in backup
print("=== Rows in NEW but not in BACKUP (unexpected extras) ===")
extra = spark.sql("""
    SELECT n.data_dt, n.bkey, n.policy_key, n.source_table
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 n
    LEFT ANTI JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b
      ON n.bkey = b.bkey AND n.policy_key = b.policy_key AND n.data_dt = b.data_dt
    ORDER BY n.data_dt, n.bkey
""")
extra_cnt = extra.count()
print(f"Extra rows: {extra_cnt}")
if extra_cnt > 0:
    extra.show(50, truncate=False)

# COMMAND ----------

# Final verdict
print("\n" + "=" * 50)
if missing_cnt == 0 and extra_cnt == 0:
    print("PASS — New results are identical to backup")
else:
    print(f"FAIL — missing={missing_cnt}, extra={extra_cnt}")
print("=" * 50)
