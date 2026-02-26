# Databricks notebook source
# MAGIC %md
# MAGIC # CHV_DEDUP notebook — v2
# MAGIC ### Builds 5 dedup output tables from `chv_table_bkey_v2`
# MAGIC
# MAGIC #### Overview
# MAGIC After `vrh_chv_match_v2` assigns BKEYs, this notebook groups source records
# MAGIC by BKEY and writes deduplicated attribute rows to 5 silver tables:
# MAGIC - `dedup_customer_name`
# MAGIC - `dedup_province`
# MAGIC - `dedup_gender`
# MAGIC - `dedup_email`
# MAGIC - `dedup_phone`
# MAGIC
# MAGIC #### Write Mode
# MAGIC Recalculate-affected-BKEYs:
# MAGIC 1. Find distinct BKEYs linked to the given `table_name` + `data_date`
# MAGIC 2. Delete those BKEYs from all 5 dedup tables
# MAGIC 3. Recalculate dedup rows for those BKEYs (from ALL source tables, not just the input table)
# MAGIC 4. Append new rows
# MAGIC
# MAGIC #### PARAMS
# MAGIC - `table_name` — full table name (e.g. `viriyah_cdqm_poc.silver.SOURCE_MOTOR`)
# MAGIC - `data_date`  — data date (e.g. `2026-02-26`)
# MAGIC - `prcs_nm`    — process name
# MAGIC - `ld_id`      — load ID
# MAGIC - `updt_prcs_nm` — update process name
# MAGIC - `updt_ld_id`   — update load ID
# MAGIC
# MAGIC #### Sample PARAMS:
# MAGIC viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-02-26^|EDP_DEDUP_V2_SOURCE_MOTOR_DATE_2026-02-26^|1^|EDP_DEDUP_V2_SOURCE_MOTOR_DATE_2026-02-26^|1

# COMMAND ----------

# viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-02-26^|EDP_DEDUP_V2_SOURCE_MOTOR_DATE_2026-02-26^|1^|EDP_DEDUP_V2_SOURCE_MOTOR_DATE_2026-02-26^|1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Imports

# COMMAND ----------

from pyspark.sql.functions import (
    col, lit, max as spark_max, collect_list, concat_ws
)
from pyspark.sql.types import LongType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Widget + Parameter Parsing

# COMMAND ----------

dbutils.widgets.text("PARAMS", "")
params = dbutils.widgets.get("PARAMS")
dbutils.widgets.text("ENV", "")
ENV = dbutils.widgets.get("ENV")

# Optional: override source/trust table names (for devtest)
# Leave blank to use production tables (source_motor / trust_source)
dbutils.widgets.text("SOURCE_TABLE", "")
dbutils.widgets.text("TRUST_TABLE", "")

params = params.split('^|')

table     = params[0]      # full table name, e.g. viriyah_cdqm_poc.silver.SOURCE_MOTOR
data_dt   = params[1]      # data date, e.g. 2026-02-26
prcs_nm   = params[2]
ld_id     = params[3]
updt_prcs_nm = params[4]
updt_ld_id   = params[5]

_source_override = dbutils.widgets.get("SOURCE_TABLE").strip()
_trust_override  = dbutils.widgets.get("TRUST_TABLE").strip()

print(f"table      : {table}")
print(f"data_dt    : {data_dt}")
print(f"prcs_nm    : {prcs_nm}")
print(f"ld_id      : {ld_id}")
print(f"updt_prcs_nm : {updt_prcs_nm}")
print(f"updt_ld_id   : {updt_ld_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Constants

# COMMAND ----------

catalog     = "viriyah_cdqm_poc"
data_schema = "silver"

# 5 dedup tables to manage
DEDUP_TABLES = [
    "dedup_customer_name",
    "dedup_province",
    "dedup_gender",
    "dedup_email",
    "dedup_phone",
]

_default_source = f"{catalog}.{data_schema}.source_motor"
_default_trust  = f"{catalog}.{data_schema}.trust_source"

source_table = _source_override if _source_override else _default_source
trust_table  = _trust_override  if _trust_override  else _default_trust

print(f"catalog     : {catalog}")
print(f"data_schema : {data_schema}")
print(f"bkey_table  : {catalog}.{data_schema}.chv_table_bkey_v2")
print(f"source_table : {source_table}")
print(f"trust_table  : {trust_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Find Affected BKEYs
# MAGIC
# MAGIC Query `chv_table_bkey_v2` for distinct BKEYs linked to the input `table` + `data_date`.
# MAGIC These BKEYs may also have records in other source tables — we recalculate all of them.

# COMMAND ----------

affected_bkeys_df = spark.sql(f"""
    SELECT DISTINCT bkey
    FROM {catalog}.{data_schema}.chv_table_bkey_v2
    WHERE lower(table) = lower('{table}')
      AND data_dt = '{data_dt}'
""")

affected_bkeys = [row.bkey for row in affected_bkeys_df.collect()]
print(f"Affected BKEYs ({len(affected_bkeys)}): {affected_bkeys}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Delete Old Rows from 5 Dedup Tables
# MAGIC
# MAGIC Remove all rows for affected BKEYs so we can recalculate cleanly.
# MAGIC Skips delete if no affected BKEYs found (e.g. first run for a new date).

# COMMAND ----------

if len(affected_bkeys) == 0:
    print("No affected BKEYs found — skipping delete step.")
else:
    bkey_list_str = ", ".join(str(b) for b in affected_bkeys)
    for tbl in DEDUP_TABLES:
        full_tbl = f"{catalog}.{data_schema}.{tbl}"
        spark.sql(f"DELETE FROM {full_tbl} WHERE bkey IN ({bkey_list_str})")
        print(f"Deleted affected BKEYs from {full_tbl}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Build Unified Source DataFrame
# MAGIC
# MAGIC UNION ALL of source_motor and trust_source, joined to chv_table_bkey_v2.
# MAGIC Only rows whose BKEY is in `affected_bkeys` are included.
# MAGIC
# MAGIC **Join keys** follow CHV_CONFIG_PK_V2:
# MAGIC - source_motor: composite key = `CONCAT_WS(',', Ident_card, Prefix, Fname, Lname, Insert_Date)`
# MAGIC - trust_source: single key = `COALESCE(Id_card, '')`
# MAGIC
# MAGIC trust_source has no area/district/postcode/province/email/update_date columns — NULLs used.

# COMMAND ----------

if len(affected_bkeys) == 0:
    print("No affected BKEYs — skipping source build and dedup steps.")
    dbutils.notebook.exit("OK — no affected BKEYs to process.")

bkey_list_str = ", ".join(str(b) for b in affected_bkeys)

# Build UNION SQL based on whether we're using devtest or production tables.
# devtest tables use lowercase column names and a single-column PK (policy_id / id_card).
# production tables use PascalCase column names and a composite PK for source_motor.
if "_devtest" in source_table.lower():
    # --- devtest: lowercase columns, PK = policy_id (single key) ---
    source_join_key = "COALESCE(s.policy_id, 'null_val')"
    trust_join_key  = "COALESCE(t.id_card, 'null_val')"
    source_select = f"""
        b.bkey,
        s.id_card,
        s.fname,
        s.lname,
        s.prefix,
        s.gender,
        s.birth_date,
        s.area,
        s.district,
        s.postcode,
        s.province,
        s.email,
        s.phone_no                       AS phone,
        CAST(s.update_date AS timestamp) AS update_date,
        s.policy_id                      AS policy_key
    FROM {{bkey_table}} b
    JOIN {source_table} s
      ON b.key = {source_join_key}
     AND lower(b.table) = lower('{source_table}')
    WHERE b.bkey IN ({{bkeys}})"""
    trust_select = f"""
        b.bkey,
        t.id_card,
        t.fname,
        t.lname,
        t.prefix,
        t.gender,
        t.birth_date,
        t.area,
        t.district,
        t.postcode,
        t.province,
        t.email,
        t.phone_no                       AS phone,
        CAST(t.update_date AS timestamp) AS update_date,
        t.id_card                        AS policy_key
    FROM {{bkey_table}} b
    JOIN {trust_table} t
      ON b.key = {trust_join_key}
     AND lower(b.table) = lower('{trust_table}')
    WHERE b.bkey IN ({{bkeys}})"""
else:
    # --- production: PascalCase columns, composite PK for source_motor ---
    source_join_key = "CONCAT_WS(',', COALESCE(s.Ident_card,''), COALESCE(s.Prefix,''), COALESCE(s.Fname,''), COALESCE(s.Lname,''), COALESCE(CAST(s.Insert_Date AS STRING),''))"
    trust_join_key  = "COALESCE(t.Id_card, '')"
    source_select = f"""
        b.bkey,
        s.Ident_card                     AS id_card,
        s.Fname                          AS fname,
        s.Lname                          AS lname,
        s.Prefix                         AS prefix,
        s.gender,
        s.birth_date,
        s.area,
        s.district,
        s.postcode,
        s.province,
        s.email,
        s.phone,
        CAST(s.update_date AS timestamp) AS update_date,
        s.policy_no                      AS policy_key
    FROM {{bkey_table}} b
    JOIN {source_table} s
      ON b.key = {source_join_key}
     AND lower(b.table) = lower('{source_table}')
    WHERE b.bkey IN ({{bkeys}})"""
    trust_select = f"""
        b.bkey,
        t.Id_card                        AS id_card,
        t.Fname                          AS fname,
        t.Lname                          AS lname,
        t.Prefix                         AS prefix,
        t.gender,
        t.DoB                            AS birth_date,
        CAST(NULL AS STRING)             AS area,
        CAST(NULL AS STRING)             AS district,
        CAST(NULL AS STRING)             AS postcode,
        CAST(NULL AS STRING)             AS province,
        CAST(NULL AS STRING)             AS email,
        t.Tel                            AS phone,
        CAST(NULL AS timestamp)          AS update_date,
        t.Id_card                        AS policy_key
    FROM {{bkey_table}} b
    JOIN {trust_table} t
      ON b.key = {trust_join_key}
     AND lower(b.table) = lower('{trust_table}')
    WHERE b.bkey IN ({{bkeys}})"""

_bkey_table = f"{catalog}.{data_schema}.chv_table_bkey_v2"
_union_sql = f"""
    SELECT {source_select.format(bkey_table=_bkey_table, bkeys=bkey_list_str)}
    UNION ALL
    SELECT {trust_select.format(bkey_table=_bkey_table, bkeys=bkey_list_str)}
"""

source_df = spark.sql(_union_sql)
source_df.createOrReplaceTempView("base_df")
print(f"base_df row count: {source_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Build + Append dedup_customer_name

# COMMAND ----------

dedup_name = spark.sql("""
    SELECT
        bkey,
        id_card,
        fname,
        lname,
        prefix,
        MAX(update_date)         AS update_date,
        collect_list(policy_key) AS policy_keys
    FROM base_df
    GROUP BY bkey, id_card, fname, lname, prefix
""")

print(f"dedup_customer_name rows to append: {dedup_name.count()}")
dedup_name.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{data_schema}.dedup_customer_name"
)
print("Appended to dedup_customer_name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Build + Append dedup_province

# COMMAND ----------

dedup_province = spark.sql("""
    SELECT
        bkey,
        id_card,
        area,
        district,
        postcode,
        province,
        MAX(update_date)         AS update_date,
        collect_list(policy_key) AS policy_keys
    FROM base_df
    GROUP BY bkey, id_card, area, district, postcode, province
""")

print(f"dedup_province rows to append: {dedup_province.count()}")
dedup_province.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{data_schema}.dedup_province"
)
print("Appended to dedup_province")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Build + Append dedup_gender

# COMMAND ----------

dedup_gender = spark.sql("""
    SELECT
        bkey,
        id_card,
        gender,
        birth_date,
        MAX(update_date)         AS update_date,
        collect_list(policy_key) AS policy_keys
    FROM base_df
    GROUP BY bkey, id_card, gender, birth_date
""")

print(f"dedup_gender rows to append: {dedup_gender.count()}")
dedup_gender.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{data_schema}.dedup_gender"
)
print("Appended to dedup_gender")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Build + Append dedup_email

# COMMAND ----------

dedup_email = spark.sql("""
    SELECT
        bkey,
        id_card,
        email,
        MAX(update_date)         AS update_date,
        collect_list(policy_key) AS policy_keys
    FROM base_df
    GROUP BY bkey, id_card, email
""")

print(f"dedup_email rows to append: {dedup_email.count()}")
dedup_email.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{data_schema}.dedup_email"
)
print("Appended to dedup_email")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: Build + Append dedup_phone

# COMMAND ----------

dedup_phone = spark.sql("""
    SELECT
        bkey,
        id_card,
        phone,
        MAX(update_date)         AS update_date,
        collect_list(policy_key) AS policy_keys
    FROM base_df
    GROUP BY bkey, id_card, phone
""")

print(f"dedup_phone rows to append: {dedup_phone.count()}")
dedup_phone.write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{data_schema}.dedup_phone"
)
print("Appended to dedup_phone")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 13: Summary

# COMMAND ----------

print("=" * 60)
print("vrh_chv_dedup_v2 — Run Summary")
print("=" * 60)
print(f"  table    : {table}")
print(f"  data_dt  : {data_dt}")
print(f"  prcs_nm  : {prcs_nm}")
print(f"  Affected BKEYs ({len(affected_bkeys)}): {affected_bkeys}")
print()

for tbl in DEDUP_TABLES:
    full_tbl = f"{catalog}.{data_schema}.{tbl}"
    cnt = spark.sql(f"SELECT COUNT(*) AS n FROM {full_tbl} WHERE bkey IN ({bkey_list_str})").collect()[0].n
    print(f"  {tbl}: {cnt} rows for affected BKEYs")

print("=" * 60)
print("DONE")

# COMMAND ----------

dbutils.notebook.exit("OK")
