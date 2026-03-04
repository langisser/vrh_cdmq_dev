# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # generate_levenshtein_test_data_10m — Insert ~10M rows into source_motor_devtest
# MAGIC
# MAGIC **DATA_DT:** `2025-02-04`
# MAGIC
# MAGIC | Target | Rows |
# MAGIC |---|---|
# MAGIC | `source_motor_devtest` | ~10,000,000 |
# MAGIC | `trust_source_devtest` | copy from `DATA_DT='2025-01-01'` |
# MAGIC
# MAGIC **ID card distribution:**
# MAGIC - 2,000,000 rows: unique id_card (prefix `'6'`, singletons)
# MAGIC - ~8,000,000 rows: 2,000,000 id_card groups (prefix `'7'`), 3–5 policies each
# MAGIC
# MAGIC **Name variants per group (simulate real dedup noise):**
# MAGIC - v1: exact fname + lname (base)
# MAGIC - v2: lname + repeat-mark suffix (`ๆ`)
# MAGIC - v3: fname truncated by 1 char
# MAGIC - v4: lname last char replaced with `า`
# MAGIC - v5: fname prepended with `ก`
# MAGIC
# MAGIC **Strategy:** Python batch loop of 50K rows → INSERT to avoid driver OOM.
# MAGIC
# MAGIC **Idempotent:** Step 0 deletes existing `2025-02-04` partition before inserting.

# COMMAND ----------

CATALOG      = "viriyah_cdqm_poc"
SILVER       = "silver"
MOTOR_TABLE  = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE  = f"{CATALOG}.{SILVER}.trust_source_devtest"
DATA_DT      = "2025-02-04"

N_UNIQUE     = 2_000_000   # singleton rows (unique id_card, prefix '6')
N_GROUPS     = 2_000_000   # shared id_card groups (prefix '7'), 3-5 policies each
BATCH_SIZE   = 50_000      # rows per insert batch

print(f"Motor table   : {MOTOR_TABLE}")
print(f"Trust table   : {TRUST_TABLE}")
print(f"DATA_DT       : {DATA_DT}")
print(f"Unique rows   : {N_UNIQUE:,}")
print(f"Groups        : {N_GROUPS:,}  (3–5 policies per group, avg 4)")
print(f"Expected total: ~{N_UNIQUE + N_GROUPS * 4:,}")
print(f"Batch size    : {BATCH_SIZE:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Clear existing partition (idempotent)

# COMMAND ----------

spark.sql(f"DELETE FROM {MOTOR_TABLE} WHERE DATA_DT = '{DATA_DT}'")
spark.sql(f"DELETE FROM {TRUST_TABLE} WHERE DATA_DT = '{DATA_DT}'")
print(f"Cleared DATA_DT='{DATA_DT}' from both tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define pools + helpers

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime

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
    ("กระทุ่มแบน", "ท่าไม้",      "สมุทรสาคร",    "74110"),
    ("กะทู้",      "กะทู้",        "ภูเก็ต",        "83120"),
    ("กันทรารมย์", "จาน",          "ศรีสะเกษ",     "33130"),
    ("กำแพงแสน",   "กำแพงแสน",    "นครปฐม",        "73140"),
    ("กมลาไสย",    "กมลาไสย",     "กาฬสินธุ์",     "46130"),
    ("เมือง",      "ในเมือง",      "อุดรธานี",      "41000"),
    ("เมือง",      "ในเมือง",      "เชียงใหม่",     "50000"),
    ("เมือง",      "ในเมือง",      "ขอนแก่น",       "40000"),
    ("บางรัก",     "มหาพฤฒาราม",  "กรุงเทพมหานคร", "10500"),
    ("ลาดพร้าว",   "จรเข้บัว",    "กรุงเทพมหานคร", "10230"),
]

schema = StructType([
    StructField("policy_id",   StringType(),    False),
    StructField("id_card",     StringType(),    True),
    StructField("fname",       StringType(),    True),
    StructField("lname",       StringType(),    True),
    StructField("gender",      StringType(),    True),
    StructField("table",       StringType(),    True),
    StructField("birth_date",  StringType(),    True),
    StructField("prefix",      StringType(),    True),
    StructField("area",        StringType(),    True),
    StructField("district",    StringType(),    True),
    StructField("province",    StringType(),    True),
    StructField("postcode",    StringType(),    True),
    StructField("email",       StringType(),    True),
    StructField("phone_no",    StringType(),    True),
    StructField("insert_date", TimestampType(), True),
    StructField("update_date", TimestampType(), True),
    StructField("DATA_DT",     StringType(),    False),
])

BASE_TS = int(datetime.datetime(2024, 1, 1).timestamp())

def make_ts(seed):
    offset = (seed * 997) % (365 * 24 * 3600)
    return datetime.datetime.fromtimestamp(BASE_TS + offset)

def make_variants(fname, lname, n):
    """
    Generate n name variants for the same id_card group.
    Variants simulate real-world data entry noise (typos, truncation, suffix).
    """
    variants = [
        (fname, lname),                                   # v1: exact
        (fname, lname + "ๆ"),                             # v2: lname + repeat mark
        (fname[:-1] if len(fname) > 1 else fname, lname), # v3: fname drop last char
        (fname, lname[:-1] + "า" if len(lname) > 1 else lname),  # v4: lname swap last → า
        ("ก" + fname, lname),                             # v5: fname prepend ก
    ]
    return variants[:n]

def make_row(policy_id, id_card, fname, lname, idx):
    prov = PROVINCES[idx % len(PROVINCES)]
    ts   = make_ts(idx)
    return (
        policy_id,
        id_card,
        fname,
        lname,
        GENDERS[idx % len(GENDERS)],
        TABLES[idx % len(TABLES)],
        None,
        PREFIXES[idx % len(PREFIXES)],
        prov[0], prov[1], prov[2], prov[3],
        f"user{idx}@example.com" if idx % 5 == 0 else None,
        f"08{str(idx % 100_000_000).zfill(8)}" if idx % 10 < 3 else None,
        ts, ts,
        DATA_DT,
    )

def insert_batch(batch, batch_num):
    df = spark.createDataFrame(batch, schema=schema)
    df.createOrReplaceTempView("batch_10m")
    spark.sql(f"""
        INSERT INTO {MOTOR_TABLE}
          (policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
           area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT)
        SELECT
          policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
          area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT
        FROM batch_10m
    """)
    print(f"  Batch {batch_num} inserted ({len(batch):,} rows)", flush=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Insert singleton rows in batches (2M rows)

# COMMAND ----------

print(f"=== Inserting {N_UNIQUE:,} singleton rows ===")
batch = []
batch_num = 0
for i in range(N_UNIQUE):
    batch.append(make_row(
        f"ten{str(i).zfill(8)}",
        "6" + str(i).zfill(12),
        FNAMES[i % len(FNAMES)],
        LNAMES[i % len(LNAMES)],
        i,
    ))
    if len(batch) == BATCH_SIZE:
        batch_num += 1
        insert_batch(batch, batch_num)
        batch = []

if batch:
    batch_num += 1
    insert_batch(batch, batch_num)
    batch = []

print(f"Singletons done — {batch_num} batches")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Insert group rows in batches (~8M rows)

# COMMAND ----------

print(f"=== Inserting group rows ({N_GROUPS:,} groups, 3–5 variants each) ===")
batch = []
batch_num = 0
policy_counter = N_UNIQUE

for g in range(N_GROUPS):
    if g % 3 == 0:
        n_variants = 3
    elif g % 2 == 0:
        n_variants = 4
    else:
        n_variants = 5

    id_card    = "7" + str(g).zfill(12)
    base_fname = FNAMES[g % len(FNAMES)]
    base_lname = LNAMES[g % len(LNAMES)]
    variants   = make_variants(base_fname, base_lname, n_variants)

    for fname, lname in variants:
        batch.append(make_row(
            f"ten{str(policy_counter).zfill(8)}",
            id_card,
            fname,
            lname,
            policy_counter,
        ))
        policy_counter += 1

        if len(batch) == BATCH_SIZE:
            batch_num += 1
            insert_batch(batch, batch_num)
            batch = []

if batch:
    batch_num += 1
    insert_batch(batch, batch_num)

print(f"Groups done — {batch_num} batches, {policy_counter - N_UNIQUE:,} group rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Copy trust_source rows

# COMMAND ----------

spark.sql(f"""
INSERT INTO {TRUST_TABLE}
  (id_card, fname, lname, gender, `table`, birth_date, prefix,
   area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT)
SELECT
  id_card, fname, lname, gender, `table`, birth_date, prefix,
  area, district, province, postcode, email, phone_no, insert_date, update_date,
  '{DATA_DT}' AS DATA_DT
FROM {TRUST_TABLE}
WHERE DATA_DT = '2025-01-01'
""")

trust_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TRUST_TABLE} WHERE DATA_DT = '{DATA_DT}'").collect()[0]["n"]
print(f"trust_source_devtest [{DATA_DT}] : {trust_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify

# COMMAND ----------

motor_count     = spark.sql(f"SELECT COUNT(*) AS n FROM {MOTOR_TABLE} WHERE DATA_DT = '{DATA_DT}'").collect()[0]["n"]
group_id_count  = spark.sql(f"SELECT COUNT(DISTINCT id_card) AS n FROM {MOTOR_TABLE} WHERE DATA_DT = '{DATA_DT}' AND id_card LIKE '7%'").collect()[0]["n"]
unique_id_count = spark.sql(f"SELECT COUNT(DISTINCT id_card) AS n FROM {MOTOR_TABLE} WHERE DATA_DT = '{DATA_DT}' AND id_card LIKE '6%'").collect()[0]["n"]

print("=== Verification ===")
print(f"source_motor_devtest [{DATA_DT}] : {motor_count:,} rows")
print(f"  Distinct shared id_card  (prefix '7') : {group_id_count:,}  (expect {N_GROUPS:,})")
print(f"  Distinct singleton id_card (prefix '6'): {unique_id_count:,}  (expect {N_UNIQUE:,})")
print(f"trust_source_devtest  [{DATA_DT}] : {trust_count:,} rows (expect 21)")

dbutils.notebook.exit("SUCCESS")
