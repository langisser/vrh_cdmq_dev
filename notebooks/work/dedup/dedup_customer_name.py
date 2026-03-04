# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_customer_name
# MAGIC รับ table + data_dt → หา affected BKEYs → MERGE ใหม่
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_CUSTOMER_NAME^|1`

# COMMAND ----------

params_raw = dbutils.widgets.get("PARAMS")
parts = params_raw.split("^|")
source_table = parts[0].strip()
data_dt      = parts[1].strip()

print(f"source_table : {source_table}")
print(f"data_dt      : {data_dt}")

# COMMAND ----------

# Step 2: สร้าง temp view = existing (explode) UNION ALL source ใหม่ พร้อม normalize prefix
spark.sql(f"""
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
            WHERE lower(`table`) = lower('{source_table}')
              AND data_dt = '{data_dt}'
        )

        UNION ALL

        -- source ใหม่
        SELECT b.bkey, s.id_card, s.fname, s.lname, s.prefix,
               s.update_date, s.policy_id AS rec_key
        FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
        JOIN {source_table} s
          ON b.key = COALESCE(s.policy_id, 'null_val')
         AND lower(b.`table`) = lower('{source_table}')
         AND s.data_dt = '{data_dt}'
    )
)
GROUP BY bkey, id_card, fname, lname, norm_prefix
""")

# COMMAND ----------

# Step 3: MERGE INTO dedup_customer_name
# NOTE: prefix ใช้ <=> (null-safe equal) เพราะ prefix อาจ NULL
spark.sql("""
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
""")

# COMMAND ----------

import unicodedata
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Step 4: Thai unicode variant validation report
# หา rows ที่ fname หรือ lname มี combining-mark order ผิด (visually เหมือนแต่ byte ต่าง)

THAI_COMBINING_ORDER = {
    0x0E31:1, 0x0E34:2, 0x0E35:2, 0x0E36:2, 0x0E37:2,
    0x0E38:3, 0x0E39:3, 0x0E47:4, 0x0E48:5, 0x0E49:5,
    0x0E4A:5, 0x0E4B:5, 0x0E4C:6, 0x0E4D:7,
}

def normalize_thai(text):
    if text is None:
        return None
    text = unicodedata.normalize('NFC', text)
    cleaned = [ch for ch in text
               if ord(ch) not in (0x200B, 0x200C, 0x200D, 0xFEFF)
               and not (0xF700 <= ord(ch) <= 0xF7FF)]
    text = ''.join(cleaned)
    result, i = [], 0
    while i < len(text):
        base = text[i]; combining = []; i += 1
        while i < len(text) and 0x0E30 <= ord(text[i]) <= 0x0E4E:
            combining.append(text[i]); i += 1
        combining.sort(key=lambda c: THAI_COMBINING_ORDER.get(ord(c), 99))
        result.append(base + ''.join(combining))
    return ''.join(result)

normalize_udf = udf(normalize_thai, StringType())

df = spark.table("viriyah_cdqm_poc.silver.dedup_customer_name") \
    .withColumn("fname_norm", normalize_udf(col("fname"))) \
    .withColumn("lname_norm", normalize_udf(col("lname")))

variant_df = df.filter(
    (col("fname") != col("fname_norm")) | (col("lname") != col("lname_norm"))
)
variant_count = variant_df.count()
print(f"[Thai unicode variant check] found {variant_count} rows with combining-mark issue")
if variant_count > 0:
    variant_df.select(
        "bkey", "id_card", "fname", "fname_norm", "lname", "lname_norm", "rec_keyvalue"
    ).show(50, truncate=False)
