# Databricks notebook source
# MAGIC %md
# MAGIC # dedup_name_variant_report
# MAGIC
# MAGIC หา rows ใน `dedup_customer_name` ที่ fname หรือ lname ต่างกันเพราะ Unicode combining-mark order
# MAGIC แต่หลัง normalize แล้วเป็นค่าเดียวกัน — บ่งชี้ว่าน่าจะเป็นคนเดียวกัน
# MAGIC
# MAGIC **Output:** `viriyah_cdqm_poc.silver.dedup_name_variant_report`
# MAGIC
# MAGIC **PARAMS:** `table^|data_dt^|prcs_nm^|ld_id`
# MAGIC
# MAGIC ตัวอย่าง: `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_NAME_VARIANT_REPORT^|1`

# COMMAND ----------

import unicodedata
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Thai Normalization Function

# COMMAND ----------

THAI_COMBINING_ORDER = {
    0x0E31: 1,  # ั (mai han akat)
    0x0E34: 2,  # ิ (sara i)
    0x0E35: 2,  # ี (sara ii)
    0x0E36: 2,  # ึ (sara ue)
    0x0E37: 2,  # ื (sara uee)
    0x0E38: 3,  # ุ (sara u)
    0x0E39: 3,  # ู (sara uu)
    0x0E47: 4,  # ็ (maitaikhu)
    0x0E48: 5,  # ่ (mai ek)
    0x0E49: 5,  # ้ (mai tho)
    0x0E4A: 5,  # ๊ (mai tri)
    0x0E4B: 5,  # ๋ (mai jattawa)
    0x0E4C: 6,  # ์ (thanthakat)
    0x0E4D: 7,  # ํ (nikhahit)
}

def normalize_thai(text):
    """Normalize Thai text:
    1. NFC unicode normalization
    2. Strip hidden chars (zero-width, BOM, PUA U+F7xx)
    3. Reorder Thai combining chars (0x0E30-0x0E4E) by canonical order
    """
    if text is None:
        return None

    # Step 1: NFC
    text = unicodedata.normalize('NFC', text)

    # Step 2: strip hidden/PUA chars
    cleaned = []
    for ch in text:
        cp = ord(ch)
        if cp in (0x200B, 0x200C, 0x200D, 0xFEFF):  # zero-width / BOM
            continue
        if 0xF700 <= cp <= 0xF7FF:  # legacy TIS-620 PUA
            continue
        cleaned.append(ch)
    text = ''.join(cleaned)

    # Step 3: reorder Thai combining chars
    result = []
    i = 0
    while i < len(text):
        base = text[i]
        combining = []
        i += 1
        while i < len(text) and 0x0E30 <= ord(text[i]) <= 0x0E4E:
            combining.append(text[i])
            i += 1
        combining.sort(key=lambda c: THAI_COMBINING_ORDER.get(ord(c), 99))
        result.append(base + ''.join(combining))
    return ''.join(result)

normalize_thai_udf = udf(normalize_thai, StringType())
spark.udf.register("normalize_thai", normalize_thai)

# Quick sanity check
test_cases = [
    ('ณัฐพนธ์ุ',  'ณัฐพนธุ์'),   # combining order wrong → should normalize to same
    ('ณัฐพนธุ์',  'ณัฐพนธุ์'),   # already correct → unchanged
]
print("=== normalize_thai sanity check ===")
for raw, expected in test_cases:
    result = normalize_thai(raw)
    status = "OK" if result == expected else "FAIL"
    print(f"  [{status}] '{raw}' → '{result}' (expected '{expected}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Variant Report

# COMMAND ----------

catalog = "viriyah_cdqm_poc"
schema  = "silver"

# Load dedup_customer_name
dedup_df = spark.table(f"{catalog}.{schema}.dedup_customer_name")

# Add normalized columns
dedup_norm = dedup_df \
    .withColumn("fname_normalized", normalize_thai_udf(col("fname"))) \
    .withColumn("lname_normalized", normalize_thai_udf(col("lname")))

dedup_norm.createOrReplaceTempView("dedup_norm")

# Find rows where raw != normalized (has unicode issue)
variant_df = spark.sql("""
    SELECT
        a.bkey,
        a.id_card,
        a.fname                AS fname_raw,
        a.lname                AS lname_raw,
        a.fname_normalized,
        a.lname_normalized,
        b.fname                AS canonical_fname,
        b.lname                AS canonical_lname,
        b.rec_keyvalue         AS canonical_rec_keyvalue,
        a.rec_keyvalue,
        CASE
            WHEN a.fname != a.fname_normalized OR a.lname != a.lname_normalized
                THEN 'unicode_order'
            ELSE 'hidden_char'
        END                    AS reason_code
    FROM dedup_norm a
    JOIN dedup_norm b
      ON  a.bkey              = b.bkey
      AND a.id_card            = b.id_card
      AND a.fname_normalized   = b.fname_normalized
      AND a.lname_normalized   = b.lname_normalized
      AND (a.fname != b.fname OR a.lname != b.lname)  -- raw values differ (fname or lname)
    WHERE a.fname != a.fname_normalized     -- a has unicode issue in fname or lname
       OR a.lname != a.lname_normalized
""")

print(f"Variant rows found: {variant_df.count()}")
variant_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Report Table

# COMMAND ----------

# DDL
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.dedup_name_variant_report (
        bkey                    INT       COMMENT 'Business Key',
        id_card                 STRING    COMMENT 'ID card number',
        fname_raw               STRING    COMMENT 'Raw first name (as stored in dedup)',
        lname_raw               STRING    COMMENT 'Raw last name (as stored in dedup)',
        fname_normalized        STRING    COMMENT 'Normalized first name (Thai combining-mark reordered)',
        lname_normalized        STRING    COMMENT 'Normalized last name',
        canonical_fname         STRING    COMMENT 'Canonical row fname (the other row with same normalized value)',
        canonical_lname         STRING    COMMENT 'Canonical row lname',
        canonical_rec_keyvalue  ARRAY<STRING> COMMENT 'rec_keyvalue of canonical row',
        rec_keyvalue            ARRAY<STRING> COMMENT 'rec_keyvalue of this variant row',
        reason_code             STRING    COMMENT 'unicode_order / hidden_char / pua_mapping'
    )
    USING DELTA
    CLUSTER BY (bkey, id_card)
    COMMENT 'Rows in dedup_customer_name that appear to be the same person but differ due to Thai Unicode variants'
""")

# Overwrite (full refresh each run)
variant_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.{schema}.dedup_name_variant_report")

count = spark.sql(f"SELECT COUNT(*) AS n FROM {catalog}.{schema}.dedup_name_variant_report").collect()[0].n
print(f"dedup_name_variant_report: {count} rows written")

# COMMAND ----------

dbutils.notebook.exit(f"OK — {count} variant rows")
