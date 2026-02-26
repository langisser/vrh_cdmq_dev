# Databricks notebook source
# MAGIC %md
# MAGIC # Unit Test — vrh_chv_dedup_v2 (devtest pipeline)
# MAGIC
# MAGIC Asserts expected row counts in 5 dedup tables after running the full dedup pipeline
# MAGIC against `source_motor_devtest` + `trust_source_devtest` with `DATA_DT='2026-02-26'`.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC Run the following notebooks in order before this test:
# MAGIC 1. `ddl_source_devtest.py` — create devtest tables (once)
# MAGIC 2. `config_devtest.py` — insert PK + matching config (once)
# MAGIC 3. `data_prep_dedup.py` — load 9+1 test rows
# MAGIC 4. `vrh_chv_match_v2` with PARAMS:
# MAGIC    `viriyah_cdqm_poc.silver.source_motor_devtest^|2026-02-26^|TEST_MATCH_DEDUP^|1^|TEST_MATCH_DEDUP^|1`
# MAGIC 5. `vrh_chv_dedup_v2` with PARAMS:
# MAGIC    `viriyah_cdqm_poc.silver.source_motor_devtest^|2026-02-26^|TEST_DEDUP^|1^|TEST_DEDUP^|1`
# MAGIC    and widgets: `SOURCE_TABLE=viriyah_cdqm_poc.silver.source_motor_devtest`,
# MAGIC    `TRUST_TABLE=viriyah_cdqm_poc.silver.trust_source_devtest`
# MAGIC
# MAGIC ## Test Data (id_card = 5101299025871)
# MAGIC - 9 source rows in `source_motor_devtest`
# MAGIC - 1 trust row in `trust_source_devtest`
# MAGIC - All share same id_card → all match → all assigned **BKEY = 1**
# MAGIC
# MAGIC ## Expected Results
# MAGIC
# MAGIC ### dedup_customer_name (GROUP BY bkey, id_card, fname, lname, prefix)
# MAGIC | fname   | lname          | prefix | source   |
# MAGIC |---------|----------------|--------|----------|
# MAGIC | ศุภชัย  | ประทีปนาฏศิริ   | นาย    | 6 source |
# MAGIC | ศุภชัย  | ประทีปนาฏศิริ   | คุณ    | 2 source |
# MAGIC | ศุภชัย  | ประทีปนาภูศิริ  | คุณ    | 1 source |
# MAGIC | ศุภชัย  | ประทีปนาฏศิริ   | NULL   | 1 trust  |
# MAGIC → **4 rows**
# MAGIC
# MAGIC ### dedup_gender (GROUP BY bkey, id_card, gender, birth_date)
# MAGIC | gender | birth_date  | source   |
# MAGIC |--------|-------------|----------|
# MAGIC | M      | NULL        | 6 source |
# MAGIC | N      | 11/29/1969  | 1 source |
# MAGIC | N      | NULL        | 2 source |
# MAGIC | NULL   | NULL        | 1 trust  |
# MAGIC → **4 rows**
# MAGIC
# MAGIC ### dedup_province (GROUP BY bkey, id_card, area, district, postcode, province)
# MAGIC | area   | district      | postcode | province         | source   |
# MAGIC |--------|---------------|----------|------------------|----------|
# MAGIC | แสมดำ  | บางขุนเทียน   | 10150    | กรุงเทพมหานคร    | 9 source |
# MAGIC | NULL   | NULL          | NULL     | NULL             | 1 trust  |
# MAGIC → **2 rows**
# MAGIC
# MAGIC ### dedup_email (GROUP BY bkey, id_card, email)
# MAGIC All source + trust rows have NULL email → **1 row** (NULL bucket)
# MAGIC
# MAGIC ### dedup_phone (GROUP BY bkey, id_card, phone)
# MAGIC All source + trust rows have NULL phone → **1 row** (NULL bucket)

# COMMAND ----------

catalog = "viriyah_cdqm_poc"
silver  = "silver"
BKEY    = 1   # Expected BKEY for all test records (same id_card → 1 cluster)

SOURCE_TABLE = f"{catalog}.{silver}.source_motor_devtest"
TRUST_TABLE  = f"{catalog}.{silver}.trust_source_devtest"

print(f"catalog      : {catalog}")
print(f"silver       : {silver}")
print(f"BKEY         : {BKEY}")
print(f"SOURCE_TABLE : {SOURCE_TABLE}")
print(f"TRUST_TABLE  : {TRUST_TABLE}")

# COMMAND ----------

def assert_count(query, expected, label):
    """Run query, compare count to expected, print PASS/FAIL, and assert."""
    actual = spark.sql(query).collect()[0][0]
    status = "PASS" if actual == expected else "FAIL"
    print(f"{status} [{label}]: expected={expected}, actual={actual}")
    assert actual == expected, f"FAIL [{label}]: expected={expected}, actual={actual}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Assertions

# COMMAND ----------

print("=" * 60)
print("vrh_chv_dedup_v2 — Unit Test Assertions")
print("=" * 60)

# dedup_customer_name: 4 distinct (bkey, id_card, fname, lname, prefix) combinations
assert_count(
    f"SELECT COUNT(*) FROM {catalog}.{silver}.dedup_customer_name WHERE bkey = {BKEY}",
    4,
    "dedup_customer_name"
)

# dedup_gender: 4 distinct (bkey, id_card, gender, birth_date) combinations
assert_count(
    f"SELECT COUNT(*) FROM {catalog}.{silver}.dedup_gender WHERE bkey = {BKEY}",
    4,
    "dedup_gender"
)

# dedup_province: 2 distinct (bkey, id_card, area, district, postcode, province) combinations
# (9 source rows all same address, 1 trust row with all NULLs)
assert_count(
    f"SELECT COUNT(*) FROM {catalog}.{silver}.dedup_province WHERE bkey = {BKEY}",
    2,
    "dedup_province"
)

# dedup_email: 1 row (all NULL email — collapsed to single NULL bucket)
assert_count(
    f"SELECT COUNT(*) FROM {catalog}.{silver}.dedup_email WHERE bkey = {BKEY}",
    1,
    "dedup_email"
)

# dedup_phone: 1 row (all NULL phone — collapsed to single NULL bucket)
assert_count(
    f"SELECT COUNT(*) FROM {catalog}.{silver}.dedup_phone WHERE bkey = {BKEY}",
    1,
    "dedup_phone"
)

print("=" * 60)
print("ALL ASSERTIONS PASSED")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detailed Inspection (optional)

# COMMAND ----------

print("--- dedup_customer_name ---")
display(spark.sql(f"""
    SELECT bkey, id_card, fname, lname, prefix, update_date, policy_keys
    FROM {catalog}.{silver}.dedup_customer_name
    WHERE bkey = {BKEY}
    ORDER BY lname, prefix
"""))

# COMMAND ----------

print("--- dedup_gender ---")
display(spark.sql(f"""
    SELECT bkey, id_card, gender, birth_date, update_date, policy_keys
    FROM {catalog}.{silver}.dedup_gender
    WHERE bkey = {BKEY}
    ORDER BY gender, birth_date
"""))

# COMMAND ----------

print("--- dedup_province ---")
display(spark.sql(f"""
    SELECT bkey, id_card, area, district, postcode, province, update_date, policy_keys
    FROM {catalog}.{silver}.dedup_province
    WHERE bkey = {BKEY}
    ORDER BY area
"""))

# COMMAND ----------

print("--- dedup_email ---")
display(spark.sql(f"""
    SELECT bkey, id_card, email, update_date, policy_keys
    FROM {catalog}.{silver}.dedup_email
    WHERE bkey = {BKEY}
"""))

# COMMAND ----------

print("--- dedup_phone ---")
display(spark.sql(f"""
    SELECT bkey, id_card, phone, update_date, policy_keys
    FROM {catalog}.{silver}.dedup_phone
    WHERE bkey = {BKEY}
"""))

# COMMAND ----------

dbutils.notebook.exit("OK — all assertions passed")
