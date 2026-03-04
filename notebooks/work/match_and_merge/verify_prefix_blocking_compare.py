# Databricks notebook source
# MAGIC %md
# MAGIC # Verify Prefix Blocking — Step 2: Compare new bkey vs backup
# MAGIC
# MAGIC Compares **motor-trust pairings** (which motor KEY is grouped with which trust KEY),
# MAGIC ignoring BKEY integer values (which are non-deterministic across runs).

# COMMAND ----------

# Row counts per DATA_DT
print("=== Row counts: backup vs new ===")
spark.sql("""
    SELECT 'backup' as src, DATA_DT, COUNT(*) as cnt
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix
    GROUP BY DATA_DT
    UNION ALL
    SELECT 'new' as src, DATA_DT, COUNT(*) as cnt
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
    GROUP BY DATA_DT
    ORDER BY DATA_DT, src
""").show()

# COMMAND ----------

# Keys present in backup but missing from new (KEY+TABLE+DATA_DT)
print("=== Keys in backup missing from new ===")
key_missing = spark.sql("""
    SELECT b.DATA_DT, b.`KEY`, b.`TABLE`
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b
    LEFT ANTI JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2 n
      ON b.`KEY` = n.`KEY` AND b.`TABLE` = n.`TABLE` AND b.DATA_DT = n.DATA_DT
    ORDER BY b.DATA_DT, b.`KEY`
""")
key_missing_cnt = key_missing.count()
print(f"Keys in BACKUP missing from NEW: {key_missing_cnt}")
if key_missing_cnt > 0:
    key_missing.show(50, truncate=False)

# COMMAND ----------

# Compare motor-trust pairings (ignore BKEY integer — it re-numbers each run)
# A pairing = (motor KEY, trust KEY) that share the same BKEY within a run
missing = spark.sql("""
    WITH backup_pairs AS (
        SELECT b1.DATA_DT, b1.`KEY` AS motor_key, b2.`KEY` AS trust_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b1
        JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b2
          ON b1.BKEY = b2.BKEY AND b1.DATA_DT = b2.DATA_DT
        WHERE lower(b1.`TABLE`) LIKE '%source_motor%'
          AND lower(b2.`TABLE`) LIKE '%trust_source%'
    ),
    new_pairs AS (
        SELECT n1.DATA_DT, n1.`KEY` AS motor_key, n2.`KEY` AS trust_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 n1
        JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2 n2
          ON n1.BKEY = n2.BKEY AND n1.DATA_DT = n2.DATA_DT
        WHERE lower(n1.`TABLE`) LIKE '%source_motor%'
          AND lower(n2.`TABLE`) LIKE '%trust_source%'
    )
    SELECT bp.DATA_DT, bp.motor_key, bp.trust_key
    FROM backup_pairs bp
    LEFT ANTI JOIN new_pairs np
      ON bp.DATA_DT = np.DATA_DT AND bp.motor_key = np.motor_key AND bp.trust_key = np.trust_key
    ORDER BY DATA_DT, motor_key
""")
missing_cnt = missing.count()
print(f"Motor-trust pairings in BACKUP missing from NEW: {missing_cnt}")
if missing_cnt > 0:
    missing.show(50, truncate=False)

# COMMAND ----------

extra = spark.sql("""
    WITH backup_pairs AS (
        SELECT b1.DATA_DT, b1.`KEY` AS motor_key, b2.`KEY` AS trust_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b1
        JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2_backup_prefixfix b2
          ON b1.BKEY = b2.BKEY AND b1.DATA_DT = b2.DATA_DT
        WHERE lower(b1.`TABLE`) LIKE '%source_motor%'
          AND lower(b2.`TABLE`) LIKE '%trust_source%'
    ),
    new_pairs AS (
        SELECT n1.DATA_DT, n1.`KEY` AS motor_key, n2.`KEY` AS trust_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 n1
        JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2 n2
          ON n1.BKEY = n2.BKEY AND n1.DATA_DT = n2.DATA_DT
        WHERE lower(n1.`TABLE`) LIKE '%source_motor%'
          AND lower(n2.`TABLE`) LIKE '%trust_source%'
    )
    SELECT np.DATA_DT, np.motor_key, np.trust_key
    FROM new_pairs np
    LEFT ANTI JOIN backup_pairs bp
      ON np.DATA_DT = bp.DATA_DT AND np.motor_key = bp.motor_key AND np.trust_key = bp.trust_key
    ORDER BY DATA_DT, motor_key
""")
extra_cnt = extra.count()
print(f"Motor-trust pairings in NEW not in BACKUP: {extra_cnt}")
if extra_cnt > 0:
    extra.show(50, truncate=False)

# COMMAND ----------

# Final verdict
print("\n" + "=" * 50)
verdict = "PASS" if key_missing_cnt == 0 and missing_cnt == 0 and extra_cnt == 0 else "FAIL"
print(f"{verdict} — key_missing={key_missing_cnt}, pairing_missing={missing_cnt}, pairing_extra={extra_cnt}")
print("=" * 50)

assert key_missing_cnt == 0 and missing_cnt == 0 and extra_cnt == 0, \
    f"Mismatch: key_missing={key_missing_cnt}, pairing_missing={missing_cnt}, pairing_extra={extra_cnt}"
