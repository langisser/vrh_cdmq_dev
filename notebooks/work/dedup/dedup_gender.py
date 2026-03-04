# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_gender
# MAGIC รับ table + data_dt → หา affected BKEYs → MERGE ใหม่
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_GENDER^|1`

# COMMAND ----------

params_raw = dbutils.widgets.get("PARAMS")
parts = params_raw.split("^|")
source_table = parts[0].strip()
data_dt      = parts[1].strip()

print(f"source_table : {source_table}")
print(f"data_dt      : {data_dt}")

# COMMAND ----------

# Step 2: สร้าง temp view = existing (explode) UNION ALL source ใหม่ พร้อม normalize gender
# ถ้าใน bkey เจอ F หรือ M ให้ใช้แทน N
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW gender_staging AS
SELECT
    bkey,
    id_card,
    norm_gender              AS gender,
    birth_date,
    MAX(update_date)                      AS update_date,
    array_distinct(collect_list(rec_key)) AS rec_keyvalue
FROM (
    SELECT
        bkey, id_card, gender, birth_date, update_date, rec_key,
        COALESCE(
            MAX(CASE WHEN gender != 'N' AND gender IS NOT NULL THEN gender END)
              OVER (PARTITION BY bkey),
            gender
        ) AS norm_gender
    FROM (
        -- existing rows (explode rec_keyvalue กลับมาเป็น individual keys)
        SELECT bkey, id_card, gender, birth_date,
               update_date, explode(rec_keyvalue) AS rec_key
        FROM viriyah_cdqm_poc.silver.dedup_gender
        WHERE bkey IN (
            SELECT DISTINCT bkey
            FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
            WHERE lower(`table`) = lower('{source_table}')
              AND data_dt = '{data_dt}'
        )

        UNION ALL

        -- source ใหม่
        SELECT b.bkey, s.id_card, s.gender, s.birth_date,
               s.update_date, s.policy_id AS rec_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
        JOIN {source_table} s
          ON b.key = COALESCE(s.policy_id, 'null_val')
         AND lower(b.`table`) = lower('{source_table}')
         AND s.data_dt = '{data_dt}'
    )
)
GROUP BY bkey, id_card, norm_gender, birth_date
""")

# COMMAND ----------

# Step 3: MERGE INTO dedup_gender
# NOTE: birth_date ใช้ <=> (null-safe equal) เพราะ NULL = NULL คือ NULL ไม่ใช่ TRUE
spark.sql("""
MERGE INTO viriyah_cdqm_poc.silver.dedup_gender AS target
USING gender_staging AS source
ON  target.bkey       = source.bkey
AND target.id_card    = source.id_card
AND target.gender     = source.gender
AND target.birth_date <=> source.birth_date
WHEN MATCHED THEN UPDATE SET
    target.update_date  = source.update_date,
    target.rec_keyvalue = source.rec_keyvalue
WHEN NOT MATCHED THEN INSERT *
""")
