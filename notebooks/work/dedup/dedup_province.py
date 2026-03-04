# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_province
# MAGIC รับ table + data_dt → หา affected BKEYs → MERGE ใหม่
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_PROVINCE^|1`

# COMMAND ----------

params_raw = dbutils.widgets.get("PARAMS")
parts = params_raw.split("^|")
source_table = parts[0].strip()
data_dt      = parts[1].strip()

print(f"source_table : {source_table}")
print(f"data_dt      : {data_dt}")

# COMMAND ----------

# Step 2: สร้าง temp view = existing (explode) UNION ALL source ใหม่
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW province_staging AS
SELECT
    bkey, id_card, area, district, postcode, province,
    MAX(update_date)                      AS update_date,
    array_distinct(collect_list(rec_key)) AS rec_keyvalue
FROM (
    -- existing rows (explode rec_keyvalue กลับมาเป็น individual keys)
    SELECT bkey, id_card, area, district, postcode, province,
           update_date, explode(rec_keyvalue) AS rec_key
    FROM viriyah_cdqm_poc.silver.dedup_province
    WHERE bkey IN (
        SELECT DISTINCT bkey
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
        WHERE lower(`table`) = lower('{source_table}')
          AND data_dt = '{data_dt}'
    )

    UNION ALL

    -- source ใหม่
    SELECT b.bkey, s.id_card, s.area, s.district, s.postcode, s.province,
           s.update_date, s.policy_id AS rec_key
    FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
    JOIN {source_table} s
      ON b.key = COALESCE(s.policy_id, 'null_val')
     AND lower(b.`table`) = lower('{source_table}')
     AND s.data_dt = '{data_dt}'
)
GROUP BY bkey, id_card, area, district, postcode, province
""")

# COMMAND ----------

# Step 3: MERGE INTO dedup_province
# NOTE: area/district/postcode/province ใช้ <=> (null-safe equal) เพราะ column เหล่านี้อาจ NULL
spark.sql("""
MERGE INTO viriyah_cdqm_poc.silver.dedup_province AS target
USING province_staging AS source
ON  target.bkey     = source.bkey
AND target.id_card  = source.id_card
AND target.area     <=> source.area
AND target.district <=> source.district
AND target.postcode <=> source.postcode
AND target.province <=> source.province
WHEN MATCHED THEN UPDATE SET
    target.update_date  = source.update_date,
    target.rec_keyvalue = source.rec_keyvalue
WHEN NOT MATCHED THEN INSERT *
""")
