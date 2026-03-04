# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_email
# MAGIC รับ table + data_dt → หา affected BKEYs → MERGE ใหม่
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_EMAIL^|1`

# COMMAND ----------

params_raw = dbutils.widgets.get("PARAMS")
parts = params_raw.split("^|")
source_table = parts[0].strip()
data_dt      = parts[1].strip()

print(f"source_table : {source_table}")
print(f"data_dt      : {data_dt}")

# COMMAND ----------

# Step 2: สร้าง temp view = existing (explode) UNION ALL source ใหม่ (กรอง email IS NOT NULL)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW email_staging AS
SELECT
    bkey, id_card, email,
    MAX(update_date)                      AS update_date,
    array_distinct(collect_list(rec_key)) AS rec_keyvalue
FROM (
    -- existing rows (explode rec_keyvalue กลับมาเป็น individual keys)
    SELECT bkey, id_card, email,
           update_date, explode(rec_keyvalue) AS rec_key
    FROM viriyah_cdqm_poc.silver.dedup_email
    WHERE bkey IN (
        SELECT DISTINCT bkey
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
        WHERE lower(`table`) = lower('{source_table}')
          AND data_dt = '{data_dt}'
    )

    UNION ALL

    -- source ใหม่ (กรอง email IS NOT NULL)
    SELECT b.bkey, s.id_card, s.email,
           s.update_date, s.policy_id AS rec_key
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
    JOIN {source_table} s
      ON b.key = COALESCE(s.policy_id, 'null_val')
     AND lower(b.`table`) = lower('{source_table}')
     AND s.data_dt = '{data_dt}'
    WHERE s.email IS NOT NULL
)
GROUP BY bkey, id_card, email
""")

# COMMAND ----------

# Step 3: MERGE INTO dedup_email
spark.sql("""
MERGE INTO viriyah_cdqm_poc.silver.dedup_email AS target
USING email_staging AS source
ON  target.bkey    = source.bkey
AND target.id_card = source.id_card
AND target.email   = source.email
WHEN MATCHED THEN UPDATE SET
    target.update_date  = source.update_date,
    target.rec_keyvalue = source.rec_keyvalue
WHEN NOT MATCHED THEN INSERT *
""")
