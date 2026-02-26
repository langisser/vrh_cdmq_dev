# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Drop & Create devtest source tables
# MAGIC
# MAGIC Drops and recreates 2 tables in `viriyah_cdqm_poc.silver` for devtest.
# MAGIC
# MAGIC | Table | PK | Notes |
# MAGIC |---|---|---|
# MAGIC | `source_motor_devtest` | `policy_id` | Motor source, includes `table` column |
# MAGIC | `trust_source_devtest` | `id_card`   | Trust source, includes `table` column |
# MAGIC
# MAGIC Run **once** before `insert_source_devtest.py`.

# COMMAND ----------

CATALOG = "viriyah_cdqm_poc"
SILVER  = "silver"

MOTOR_TABLE = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE = f"{CATALOG}.{SILVER}.trust_source_devtest"

print(f"Target schema : {CATALOG}.{SILVER}")
print(f"Motor table   : {MOTOR_TABLE}")
print(f"Trust table   : {TRUST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. source_motor_devtest

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {MOTOR_TABLE}")
print(f"Dropped (if existed): {MOTOR_TABLE}")

spark.sql(f"""
CREATE TABLE {MOTOR_TABLE} (
  policy_id   STRING    COMMENT 'Policy ID (PK — unique per row)',
  id_card     STRING    COMMENT 'ID card',
  fname       STRING    COMMENT 'First name',
  lname       STRING    COMMENT 'Last name',
  gender      STRING,
  `table`     STRING    COMMENT 'Source table name (e.g. applicants, maspol)',
  birth_date  STRING,
  prefix      STRING    COMMENT 'Name prefix',
  area        STRING,
  district    STRING,
  province    STRING,
  postcode    STRING,
  email       STRING,
  phone_no    STRING,
  insert_date TIMESTAMP,
  update_date TIMESTAMP
) USING DELTA
PARTITIONED BY (DATA_DT STRING)
COMMENT 'Dev-test motor source table'
""")
print(f"Created: {MOTOR_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. trust_source_devtest

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {TRUST_TABLE}")
print(f"Dropped (if existed): {TRUST_TABLE}")

spark.sql(f"""
CREATE TABLE {TRUST_TABLE} (
  id_card     STRING    COMMENT 'ID card (PK)',
  fname       STRING,
  lname       STRING,
  gender      STRING,
  `table`     STRING    COMMENT 'Source table name',
  birth_date  STRING,
  prefix      STRING,
  area        STRING,
  district    STRING,
  province    STRING,
  postcode    STRING,
  email       STRING,
  phone_no    STRING,
  insert_date TIMESTAMP,
  update_date TIMESTAMP
) USING DELTA
PARTITIONED BY (DATA_DT STRING)
COMMENT 'Dev-test trust source table'
""")
print(f"Created: {TRUST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

for tbl in [MOTOR_TABLE, TRUST_TABLE]:
    print(f"\n--- {tbl} ---")
    spark.sql(f"DESCRIBE TABLE {tbl}").show(30, truncate=False)
