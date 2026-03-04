# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # generate_perf_data — Insert 100K synthetic rows into source_motor_devtest
# MAGIC
# MAGIC **DATA_DT:** `2025-02-03` (new partition, separate from devtest data)
# MAGIC
# MAGIC | Target | Rows |
# MAGIC |---|---|
# MAGIC | `source_motor_devtest` | 100,000 |
# MAGIC | `trust_source_devtest` | copy from `DATA_DT='2025-01-01'` |
# MAGIC
# MAGIC **ID card distribution:**
# MAGIC - 70,000 rows: unique id_card (one person = one policy)
# MAGIC - 30,000 rows: shared id_card (10,000 distinct IDs × avg 3 policies each)
# MAGIC
# MAGIC **Idempotent:** Step 0 deletes existing perf partition before inserting.

# COMMAND ----------

CATALOG      = "viriyah_cdqm_poc"
SILVER       = "silver"
MOTOR_TABLE  = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE  = f"{CATALOG}.{SILVER}.trust_source_devtest"
PERF_DATA_DT = "2025-02-03"

TOTAL_ROWS       = 100_000
UNIQUE_ROWS      = 70_000   # unique id_card, one policy each
DUPLICATE_ROWS   = 30_000   # shared across 10,000 distinct id_cards
SHARED_ID_POOL   = 10_000   # → avg 3 policies per shared id_card

print(f"Motor table  : {MOTOR_TABLE}")
print(f"Trust table  : {TRUST_TABLE}")
print(f"DATA_DT      : {PERF_DATA_DT}")
print(f"Total rows   : {TOTAL_ROWS:,}")
print(f"  Unique IDs : {UNIQUE_ROWS:,}")
print(f"  Shared IDs : {DUPLICATE_ROWS:,}  ({SHARED_ID_POOL:,} distinct, avg {DUPLICATE_ROWS//SHARED_ID_POOL}x each)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Clear existing perf partition (idempotent)

# COMMAND ----------

spark.sql(f"DELETE FROM {MOTOR_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'")
spark.sql(f"DELETE FROM {TRUST_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'")
print(f"Cleared DATA_DT='{PERF_DATA_DT}' from both tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate 100K rows and insert into source_motor_devtest

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, TimestampType

# --- Reference pools (realistic Thai data) ---
FNAMES = [
    "มาโนชน์", "ปวีณา", "กิตติพงศ์", "ธวัชชัย", "วันดี", "กรรณิการ์", "จักรพงษ์",
    "ยุทธศาสตร์", "สุทิรัก", "มินตรา", "ฉัตรสุดา", "เรวัช", "สุภัสดี", "อุทุมพร",
    "ดอกเนียม", "ใหญ่", "สุรชัย", "อรอนงค์", "พิชัย", "นิภาพร", "วิชัย", "สมหญิง",
    "ประเสริฐ", "จิราพร", "สมศักดิ์", "รัตนา", "ชัยวัฒน์", "พรทิพย์", "อนุชา", "นงนุช",
    "สุวิทย์", "อรัญ", "ปิยะ", "วรรณา", "ธนกร", "กาญจนา", "สิทธิพล", "ลัดดา",
    "ณัฐพล", "สุดา", "ชาติชาย", "มาลี", "อดิศร", "พัชรี", "วีระ", "กนกวรรณ",
    "บุญเลิศ", "สุนีย์", "นิรันดร์", "ศิริพร",
]

LNAMES = [
    "บุญมา", "วิโรทศ", "ศิริวงษ์", "มีกฤษใหญ่", "พนม", "ทิพย์เสภา", "กุหลาบโชติ",
    "พิลา", "โอทอง", "คุ้มบุ่งคล้า", "จีระพันธุ", "ดีสุข", "วิชาชัย", "กลางถิ่น",
    "พุ่มพวง", "สุกโทน", "ชัยยา", "ทองดี", "สมบูรณ์", "รักษา", "ใจดี", "พงษ์ศิริ",
    "เจริญ", "สุขสม", "แก้วมณี", "ทองแดง", "ศรีทอง", "ดวงดาว", "มีสุข", "วงษ์ทอง",
    "สงคราม", "ชนะ", "แสงทอง", "จันทร์ดี", "บุญช่วย", "หมื่นไธสง", "สีดา", "โชติ",
    "อินทร์", "ทรัพย์", "รุ่งเรือง", "เพ็ชร", "ทอง", "ดาว", "ขาว", "นาค",
    "มังกร", "หงษ์", "ศรี", "แก้ว",
]

PREFIXES  = ["คุณ", "นาย", "นาง", "นางสาว"]
GENDERS   = ["M", "F", "N"]
TABLES    = ["applicants", "maspol", "update-applicants", "update_maspol", "pol_insured"]
PROVINCES = [
    ("กระทุ่มแบน", "ท่าไม้",     "สมุทรสาคร",  "74110"),
    ("กะทู้",      "กะทู้",       "ภูเก็ต",      "83120"),
    ("กันทรารมย์", "จาน",         "ศรีสะเกษ",   "33130"),
    ("กำแพงแสน",   "กำแพงแสน",   "นครปฐม",      "73140"),
    ("กมลาไสย",    "กมลาไสย",    "กาฬสินธุ์",   "46130"),
    ("เมือง",      "ในเมือง",     "อุดรธานี",    "41000"),
    ("เมือง",      "ในเมือง",     "เชียงใหม่",   "50000"),
    ("เมือง",      "ในเมือง",     "ขอนแก่น",     "40000"),
    ("บางรัก",     "มหาพฤฒาราม", "กรุงเทพมหานคร","10500"),
    ("ลาดพร้าว",   "จรเข้บัว",   "กรุงเทพมหานคร","10230"),
]

# COMMAND ----------

# Build the DataFrame using Spark SQL range for efficiency

# Create reference arrays as SQL literals
fname_arr  = ", ".join(f"'{n}'" for n in FNAMES)
lname_arr  = ", ".join(f"'{n}'" for n in LNAMES)
prefix_arr = ", ".join(f"'{p}'" for p in PREFIXES)
gender_arr = ", ".join(f"'{g}'" for g in GENDERS)
table_arr  = ", ".join(f"'{t}'" for t in TABLES)

# Province pool as arrays
prov_area     = ", ".join(f"'{p[0]}'" for p in PROVINCES)
prov_district = ", ".join(f"'{p[1]}'" for p in PROVINCES)
prov_province = ", ".join(f"'{p[2]}'" for p in PROVINCES)
prov_postcode = ", ".join(f"'{p[3]}'" for p in PROVINCES)

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW perf_raw AS
SELECT
    i,

    -- policy_id: unique per row
    CONCAT('perf', LPAD(CAST(i AS STRING), 6, '0')) AS policy_id,

    -- id_card:
    --   i < {UNIQUE_ROWS}  → unique pool: '9' + 12-digit i   ({UNIQUE_ROWS:,} distinct)
    --   i >= {UNIQUE_ROWS} → shared pool: '8' + 12-digit (i % {SHARED_ID_POOL})  ({SHARED_ID_POOL:,} distinct)
    CASE
      WHEN i < {UNIQUE_ROWS}
        THEN CONCAT('9', LPAD(CAST(i AS STRING), 12, '0'))
      ELSE
        CONCAT('8', LPAD(CAST(((i - {UNIQUE_ROWS}) % {SHARED_ID_POOL}) AS STRING), 12, '0'))
    END AS id_card,

    -- fname / lname from pool
    array({fname_arr})[i % {len(FNAMES)}]  AS fname,
    array({lname_arr})[i % {len(LNAMES)}]  AS lname,

    -- gender
    array({gender_arr})[i % {len(GENDERS)}] AS gender,

    -- source table
    array({table_arr})[i % {len(TABLES)}] AS `table`,

    -- birth_date: NULL for 40% rows, date string for others
    CASE
      WHEN i % 10 < 6
        THEN DATE_FORMAT(DATE_ADD(DATE('1960-01-01'), CAST((i * 137) % (40 * 365) AS INT)), 'yyyy-MM-dd')
      ELSE NULL
    END AS birth_date,

    -- prefix
    array({prefix_arr})[i % {len(PREFIXES)}] AS prefix,

    -- address from province pool
    array({prov_area})[i % {len(PROVINCES)}]     AS area,
    array({prov_district})[i % {len(PROVINCES)}] AS district,
    array({prov_province})[i % {len(PROVINCES)}] AS province,
    array({prov_postcode})[i % {len(PROVINCES)}] AS postcode,

    -- email: ~20% rows have email
    CASE WHEN i % 5 = 0 THEN CONCAT('user', CAST(i AS STRING), '@example.com') ELSE NULL END AS email,

    -- phone_no: ~30% rows have phone
    CASE
      WHEN i % 10 < 3
        THEN CONCAT('08', LPAD(CAST(i % 100000000 AS STRING), 8, '0'))
      ELSE NULL
    END AS phone_no,

    -- timestamps: spread across 2024-2025
    CAST(TIMESTAMP_SECONDS(1704067200 + (i * 997) % (365 * 24 * 3600)) AS TIMESTAMP) AS insert_date,
    CAST(TIMESTAMP_SECONDS(1704067200 + (i * 997 + 3600) % (365 * 24 * 3600)) AS TIMESTAMP) AS update_date,

    '{PERF_DATA_DT}' AS DATA_DT

FROM range({TOTAL_ROWS}) AS r(i)
""")

count = spark.sql("SELECT COUNT(*) AS n FROM perf_raw").collect()[0]["n"]
print(f"Generated {count:,} rows in temp view")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Insert into source_motor_devtest

# COMMAND ----------

spark.sql(f"""
INSERT INTO {MOTOR_TABLE}
  (policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
   area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT)
SELECT
  policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
  area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT
FROM perf_raw
""")

motor_count = spark.sql(f"SELECT COUNT(*) AS n FROM {MOTOR_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'").collect()[0]["n"]
print(f"source_motor_devtest [{PERF_DATA_DT}] row count: {motor_count:,}")

# Spot-check ID card distribution
unique_id_count = spark.sql(f"""
  SELECT COUNT(DISTINCT id_card) AS n
  FROM {MOTOR_TABLE}
  WHERE DATA_DT = '{PERF_DATA_DT}'
""").collect()[0]["n"]
print(f"  Distinct id_card values: {unique_id_count:,}")
print(f"  Expected unique (70K pool): {UNIQUE_ROWS:,}")
print(f"  Expected shared (10K pool): {SHARED_ID_POOL:,}")
print(f"  Expected total distinct   : {UNIQUE_ROWS + SHARED_ID_POOL:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Copy trust_source rows for DATA_DT='2025-02-01'
# MAGIC
# MAGIC Copies the existing 21 trust_source rows from `DATA_DT='2025-01-01'`
# MAGIC so the pipeline's trust source pre-validation step can run.

# COMMAND ----------

spark.sql(f"""
INSERT INTO {TRUST_TABLE}
  (id_card, fname, lname, gender, `table`, birth_date, prefix,
   area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT)
SELECT
  id_card, fname, lname, gender, `table`, birth_date, prefix,
  area, district, province, postcode, email, phone_no, insert_date, update_date,
  '{PERF_DATA_DT}' AS DATA_DT
FROM {TRUST_TABLE}
WHERE DATA_DT = '2025-01-01'
""")

trust_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TRUST_TABLE} WHERE DATA_DT = '{PERF_DATA_DT}'").collect()[0]["n"]
print(f"trust_source_devtest [{PERF_DATA_DT}] row count: {trust_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify

# COMMAND ----------

print("=== Verification ===")
print(f"source_motor_devtest [{PERF_DATA_DT}] : {motor_count:,} rows (expect {TOTAL_ROWS:,})")
print(f"trust_source_devtest  [{PERF_DATA_DT}] : {trust_count:,} rows (expect 21)")
print()

if motor_count == TOTAL_ROWS:
    print("✓ Row count correct")
else:
    print(f"✗ Row count mismatch! Got {motor_count:,}, expected {TOTAL_ROWS:,}")
    raise Exception(f"Row count mismatch: {motor_count}")

dbutils.notebook.exit("SUCCESS")
