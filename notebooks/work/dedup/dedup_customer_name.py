# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_customer_name
# MAGIC รับ table + data_dt → หา affected BKEYs → MERGE ใหม่
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_CUSTOMER_NAME^|1`

# COMMAND ----------
# MAGIC %sql
-- Step 1: หา affected BKEYs
SELECT DISTINCT bkey
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE lower(`table`) = lower('viriyah_cdqm_poc.silver.source_motor_devtest')
  AND data_dt = '2025-01-01'

# COMMAND ----------
# MAGIC %sql
-- Step 2: สร้าง temp view = existing (explode) UNION ALL source ใหม่ พร้อม normalize prefix
CREATE OR REPLACE TEMP VIEW customer_name_staging AS
SELECT
    bkey,
    id_card,
    fname,
    lname,
    norm_prefix              AS prefix,
    MAX(update_date)                      AS update_date,
    array_distinct(collect_list(rec_key)) AS rec_keyvalue
FROM (
    SELECT
        bkey, id_card, fname, lname, prefix, update_date, rec_key,
        COALESCE(
            MAX(CASE WHEN prefix != 'คุณ' AND prefix IS NOT NULL THEN prefix END)
              OVER (PARTITION BY bkey),
            'คุณ'
        ) AS norm_prefix
    FROM (
        -- existing rows (explode rec_keyvalue กลับมาเป็น individual keys)
        SELECT bkey, id_card, fname, lname, prefix,
               update_date, explode(rec_keyvalue) AS rec_key
        FROM viriyah_cdqm_poc.silver.dedup_customer_name
        WHERE bkey IN (
            SELECT DISTINCT bkey
            FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
            WHERE lower(`table`) = lower('viriyah_cdqm_poc.silver.source_motor_devtest')
              AND data_dt = '2025-01-01'
        )

        UNION ALL

        -- source ใหม่
        SELECT b.bkey, s.id_card, s.fname, s.lname, s.prefix,
               s.update_date, s.policy_id AS rec_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
        JOIN viriyah_cdqm_poc.silver.source_motor_devtest s
          ON b.key = COALESCE(s.policy_id, 'null_val')
         AND lower(b.`table`) = lower('viriyah_cdqm_poc.silver.source_motor_devtest')
         AND s.data_dt = '2025-01-01'
    )
)
GROUP BY bkey, id_card, fname, lname, norm_prefix

# COMMAND ----------
# MAGIC %sql
-- Step 3: MERGE INTO dedup_customer_name
-- NOTE: prefix ใช้ <=> (null-safe equal) เพราะ prefix อาจ NULL
MERGE INTO viriyah_cdqm_poc.silver.dedup_customer_name AS target
USING customer_name_staging AS source
ON  target.bkey    = source.bkey
AND target.id_card = source.id_card
AND target.fname   = source.fname
AND target.lname   = source.lname
AND target.prefix  <=> source.prefix
WHEN MATCHED THEN UPDATE SET
    target.update_date  = source.update_date,
    target.rec_keyvalue = source.rec_keyvalue
WHEN NOT MATCHED THEN INSERT *
