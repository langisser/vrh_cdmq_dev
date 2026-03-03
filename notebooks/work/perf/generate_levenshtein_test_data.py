# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # generate_levenshtein_test_data — Insert ~20K levenshtein-test rows into source_motor_devtest
# MAGIC
# MAGIC **DATA_DT:** `2025-02-01` (separate partition for levenshtein matching validation)
# MAGIC
# MAGIC | Target | Rows |
# MAGIC |---|---|
# MAGIC | `source_motor_devtest` | ~20,000 |
# MAGIC | `trust_source_devtest` | copy from `DATA_DT='2025-01-01'` |
# MAGIC
# MAGIC **ID card distribution:**
# MAGIC - 8,000 rows: unique id_card (prefix `'6'`, singletons)
# MAGIC - ~12,000 rows: 3,000 id_card groups (prefix `'7'`), 3–5 policies each
# MAGIC   - Each group has a base fname/lname + levenshtein variants (distance 1)
# MAGIC   - Exercises matching rules 32/33/35/36/38/39
# MAGIC
# MAGIC **Idempotent:** Step 0 deletes existing `2025-02-01` partition before inserting.

# COMMAND ----------

CATALOG      = "viriyah_cdqm_poc"
SILVER       = "silver"
MOTOR_TABLE  = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE  = f"{CATALOG}.{SILVER}.trust_source_devtest"
LEV_DATA_DT  = "2025-02-01"

N_UNIQUE     = 8_000   # singleton rows (unique id_card, prefix '6')
N_GROUPS     = 3_000   # shared id_card groups (prefix '7')

print(f"Motor table  : {MOTOR_TABLE}")
print(f"Trust table  : {TRUST_TABLE}")
print(f"DATA_DT      : {LEV_DATA_DT}")
print(f"Unique rows  : {N_UNIQUE:,}")
print(f"Groups       : {N_GROUPS:,}  (3–5 policies per group)")
print(f"Expected total source rows: ~{N_UNIQUE + N_GROUPS*4:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Clear existing partition (idempotent)

# COMMAND ----------

spark.sql(f"DELETE FROM {MOTOR_TABLE} WHERE DATA_DT = '{LEV_DATA_DT}'")
spark.sql(f"DELETE FROM {TRUST_TABLE} WHERE DATA_DT = '{LEV_DATA_DT}'")
print(f"Cleared DATA_DT='{LEV_DATA_DT}' from both tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build rows in Python (driver), then create DataFrame
# MAGIC
# MAGIC Group rows need per-row name mutations (levenshtein variants) which are
# MAGIC easier to generate in Python than in Spark SQL.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType
import datetime

# --- Reference pools (same as generate_perf_data.py) ---
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

# --- Levenshtein variant generator ---

def make_variants(fname, lname, n):
    """Return list of (fname, lname) tuples — n variants including base (v=0).

    Each variant differs from base by levenshtein distance 1:
      v=0: base (no change)
      v=1: append 'ๆ' to lname
      v=2: drop last char from fname
      v=3: replace last char of lname with 'า'
      v=4: prepend 'ก' to fname
    """
    variants = [(fname, lname)]                              # v=0: base
    if n >= 2: variants.append((fname, lname + "ๆ"))        # v=1: add char to lname
    if n >= 3: variants.append((fname[:-1], lname))         # v=2: drop last char from fname
    if n >= 4: variants.append((fname, lname[:-1] + "า"))   # v=3: swap last char in lname
    if n >= 5: variants.append(("ก" + fname, lname))        # v=4: prepend char to fname
    return variants[:n]

# --- Helper for deterministic timestamps ---
BASE_TS = int(datetime.datetime(2024, 1, 1).timestamp())

def make_ts(seed):
    offset = (seed * 997) % (365 * 24 * 3600)
    return datetime.datetime.fromtimestamp(BASE_TS + offset)

# COMMAND ----------

# Build singleton rows (unique id_card, prefix '6')
singleton_rows = []
for i in range(N_UNIQUE):
    row_idx = i
    prov = PROVINCES[row_idx % len(PROVINCES)]
    ts   = make_ts(row_idx)
    singleton_rows.append((
        f"lev{str(i).zfill(6)}",                    # policy_id
        "6" + str(i).zfill(12),                     # id_card  (unique)
        FNAMES[row_idx % len(FNAMES)],               # fname
        LNAMES[row_idx % len(LNAMES)],               # lname
        GENDERS[row_idx % len(GENDERS)],             # gender
        TABLES[row_idx % len(TABLES)],               # table
        None,                                         # birth_date (all NULL for simplicity)
        PREFIXES[row_idx % len(PREFIXES)],           # prefix
        prov[0], prov[1], prov[2], prov[3],          # area, district, province, postcode
        f"user{i}@example.com" if i % 5 == 0 else None,  # email
        f"08{str(i % 100_000_000).zfill(8)}" if i % 10 < 3 else None,  # phone_no
        ts,                                           # insert_date
        ts,                                           # update_date
        LEV_DATA_DT,                                  # DATA_DT
    ))

print(f"Singleton rows built: {len(singleton_rows):,}")

# COMMAND ----------

# Build group rows (shared id_card, prefix '7'), with levenshtein variants
group_rows = []
policy_counter = N_UNIQUE  # continue numbering after singletons

for g in range(N_GROUPS):
    # Vary group size: 3 if g%3==0, 4 if g%2==0, else 5
    if g % 3 == 0:
        n_variants = 3
    elif g % 2 == 0:
        n_variants = 4
    else:
        n_variants = 5

    id_card   = "7" + str(g).zfill(12)
    base_fname = FNAMES[g % len(FNAMES)]
    base_lname = LNAMES[g % len(LNAMES)]
    variants   = make_variants(base_fname, base_lname, n_variants)

    for v, (fname, lname) in enumerate(variants):
        row_idx = policy_counter
        prov = PROVINCES[row_idx % len(PROVINCES)]
        ts   = make_ts(row_idx)
        group_rows.append((
            f"lev{str(policy_counter).zfill(6)}",    # policy_id
            id_card,                                  # id_card  (shared within group)
            fname,                                    # fname (variant)
            lname,                                    # lname (variant)
            GENDERS[row_idx % len(GENDERS)],          # gender
            TABLES[row_idx % len(TABLES)],            # table
            None,                                     # birth_date
            PREFIXES[row_idx % len(PREFIXES)],        # prefix
            prov[0], prov[1], prov[2], prov[3],       # area, district, province, postcode
            f"user{row_idx}@example.com" if row_idx % 5 == 0 else None,
            f"08{str(row_idx % 100_000_000).zfill(8)}" if row_idx % 10 < 3 else None,
            ts,                                       # insert_date
            ts,                                       # update_date
            LEV_DATA_DT,                              # DATA_DT
        ))
        policy_counter += 1

print(f"Group rows built   : {len(group_rows):,}  ({N_GROUPS:,} groups)")
print(f"Total rows to insert: {len(singleton_rows) + len(group_rows):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Insert into source_motor_devtest

# COMMAND ----------

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

all_rows = singleton_rows + group_rows
df = spark.createDataFrame(all_rows, schema=schema)
df.createOrReplaceTempView("lev_data")

spark.sql(f"""
INSERT INTO {MOTOR_TABLE}
  (policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
   area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT)
SELECT
  policy_id, id_card, fname, lname, gender, `table`, birth_date, prefix,
  area, district, province, postcode, email, phone_no, insert_date, update_date, DATA_DT
FROM lev_data
""")

motor_count = spark.sql(f"SELECT COUNT(*) AS n FROM {MOTOR_TABLE} WHERE DATA_DT = '{LEV_DATA_DT}'").collect()[0]["n"]
print(f"source_motor_devtest [{LEV_DATA_DT}] row count: {motor_count:,}")

# Spot-check id_card distribution
group_id_count = spark.sql(f"""
  SELECT COUNT(DISTINCT id_card) AS n
  FROM {MOTOR_TABLE}
  WHERE DATA_DT = '{LEV_DATA_DT}' AND id_card LIKE '7%'
""").collect()[0]["n"]
print(f"  Distinct shared id_card (prefix '7'): {group_id_count:,}  (expect {N_GROUPS:,})")

unique_id_count = spark.sql(f"""
  SELECT COUNT(DISTINCT id_card) AS n
  FROM {MOTOR_TABLE}
  WHERE DATA_DT = '{LEV_DATA_DT}' AND id_card LIKE '6%'
""").collect()[0]["n"]
print(f"  Distinct singleton id_card (prefix '6'): {unique_id_count:,}  (expect {N_UNIQUE:,})")

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
  '{LEV_DATA_DT}' AS DATA_DT
FROM {TRUST_TABLE}
WHERE DATA_DT = '2025-01-01'
""")

trust_count = spark.sql(f"SELECT COUNT(*) AS n FROM {TRUST_TABLE} WHERE DATA_DT = '{LEV_DATA_DT}'").collect()[0]["n"]
print(f"trust_source_devtest [{LEV_DATA_DT}] row count: {trust_count:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify

# COMMAND ----------

EXPECTED_MOTOR = len(all_rows)

print("=== Verification ===")
print(f"source_motor_devtest [{LEV_DATA_DT}] : {motor_count:,} rows (expect {EXPECTED_MOTOR:,})")
print(f"trust_source_devtest  [{LEV_DATA_DT}] : {trust_count:,} rows (expect 21)")
print()

if motor_count == EXPECTED_MOTOR:
    print("✓ Row count correct")
else:
    print(f"✗ Row count mismatch! Got {motor_count:,}, expected {EXPECTED_MOTOR:,}")
    raise Exception(f"Row count mismatch: {motor_count}")

print()
print("Spot-check levenshtein variants (sample 3 groups):")
spark.sql(f"""
  SELECT id_card, fname, lname
  FROM {MOTOR_TABLE}
  WHERE DATA_DT = '{LEV_DATA_DT}' AND id_card IN ('7000000000000', '7000000000001', '7000000000002')
  ORDER BY id_card, policy_id
""").show(15, truncate=False)

dbutils.notebook.exit("SUCCESS")
