# Databricks notebook source
# MAGIC %md
# MAGIC # Config Insert — Match & Merge (Group 1, tc1 unit test)
# MAGIC Inserts Group 1 config for tc_case1_5101299025871 ↔ ts_case1_5101299025871.
# MAGIC Shows each table as a DataFrame BEFORE inserting.

# COMMAND ----------

ctrl  = 'viriyah_cdqm_poc.control_fw'
tc    = 'viriyah_cdqm_poc.silver.tc_case1_5101299025871'
ts    = 'viriyah_cdqm_poc.silver.ts_case1_5101299025871'

# COMMAND ----------

max_rule = spark.sql(f"""
    SELECT COALESCE(MAX(MATCHING_RULES), 0) AS max_rule
    FROM {ctrl}.chv_config_matching
""").collect()[0].max_rule
base = int(max_rule) + 1
print(f"MATCHING_RULES base: {base}  (rules {base} – {base+10})")

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType, DoubleType,
)

param_data = [
    Row(PARAM_GROUP_NAME='DATE', PARAM_NAME=tc, PARAM_DESC='Lag DATA_DT',
        PARAM_VAL_NUMBER=0, PARAM_VAL_DATE=None, PARAM_VAL_VARCHAR=None),
    Row(PARAM_GROUP_NAME='DATE', PARAM_NAME=ts, PARAM_DESC='Lag DATA_DT',
        PARAM_VAL_NUMBER=0, PARAM_VAL_DATE=None, PARAM_VAL_VARCHAR=None),
]
param_schema = StructType([
    StructField('PARAM_GROUP_NAME', StringType()),
    StructField('PARAM_NAME',       StringType()),
    StructField('PARAM_DESC',       StringType()),
    StructField('PARAM_VAL_NUMBER', IntegerType()),
    StructField('PARAM_VAL_DATE',   DateType()),
    StructField('PARAM_VAL_VARCHAR',StringType()),
])
param_df = spark.createDataFrame(param_data, schema=param_schema)
print(f"chv_param_general — {param_df.count()} rows to insert:")
display(param_df)

# COMMAND ----------

param_df.write.mode('append').insertInto(f'{ctrl}.chv_param_general')
print("Inserted chv_param_general")

# COMMAND ----------

pk_data = [
    Row(TABLE=tc, PK='id_card',    ORDER=1, ACT_F=1),
    Row(TABLE=tc, PK='fname',      ORDER=2, ACT_F=1),
    Row(TABLE=tc, PK='lname',      ORDER=3, ACT_F=1),
    Row(TABLE=tc, PK='phone',      ORDER=4, ACT_F=1),
    Row(TABLE=tc, PK='birth_date', ORDER=5, ACT_F=1),
    Row(TABLE=ts, PK='id_card',    ORDER=1, ACT_F=1),
]
pk_schema = StructType([
    StructField('TABLE', StringType()),
    StructField('PK',    StringType()),
    StructField('ORDER', IntegerType()),
    StructField('ACT_F', IntegerType()),
])
pk_df = spark.createDataFrame(pk_data, schema=pk_schema)
print(f"chv_config_pk — {pk_df.count()} rows to insert:")
display(pk_df)

# COMMAND ----------

pk_df.write.mode('append').insertInto(f'{ctrl}.chv_config_pk')
print("Inserted chv_config_pk")

# COMMAND ----------

pre_val_data = [
    # tc: 5 cols
    Row(TABLE=tc, RULES='CHECK_NULL', PARAMETER='id_card',    CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=tc, RULES='CHECK_NULL', PARAMETER='fname',      CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=tc, RULES='CHECK_NULL', PARAMETER='lname',      CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=tc, RULES='CHECK_NULL', PARAMETER='phone',      CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=tc, RULES='CHECK_NULL', PARAMETER='birth_date', CUSTOM_CONDITION=None, ACT_F=1),
    # ts: 3 cols
    Row(TABLE=ts, RULES='CHECK_NULL', PARAMETER='id_card',    CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=ts, RULES='CHECK_NULL', PARAMETER='fname',      CUSTOM_CONDITION=None, ACT_F=1),
    Row(TABLE=ts, RULES='CHECK_NULL', PARAMETER='lname',      CUSTOM_CONDITION=None, ACT_F=1),
]
pre_val_schema = StructType([
    StructField('TABLE',            StringType()),
    StructField('RULES',            StringType()),
    StructField('PARAMETER',        StringType()),
    StructField('CUSTOM_CONDITION', StringType()),
    StructField('ACT_F',            IntegerType()),
])
pre_val_df = spark.createDataFrame(pre_val_data, schema=pre_val_schema)
print(f"chv_config_pre_validation — {pre_val_df.count()} rows to insert:")
display(pre_val_df)

# COMMAND ----------

pre_val_df.write.mode('append').insertInto(f'{ctrl}.chv_config_pre_validation')
print("Inserted chv_config_pre_validation")

# COMMAND ----------

matching_data = [
    # tc → ts (3 rules: id_card, fname, lname — ts has no phone/birth_date)
    Row(MATCHING_RULES=base+0,  MAIN_TABLE=tc, MATCHING_TABLE=ts,
        MATCH_CONDITION='MAIN.id_card = MATCH.id_card',       GROUP=1, WEIGHT=1.0,  ACT_F=1),
    Row(MATCHING_RULES=base+1,  MAIN_TABLE=tc, MATCHING_TABLE=ts,
        MATCH_CONDITION='MAIN.fname = MATCH.fname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    Row(MATCHING_RULES=base+2,  MAIN_TABLE=tc, MATCHING_TABLE=ts,
        MATCH_CONDITION='MAIN.lname = MATCH.lname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    # tc → tc (5 rules: id_card, fname, lname, phone, birth_date)
    Row(MATCHING_RULES=base+3,  MAIN_TABLE=tc, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.id_card = MATCH.id_card',       GROUP=1, WEIGHT=1.0,  ACT_F=1),
    Row(MATCHING_RULES=base+4,  MAIN_TABLE=tc, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.fname = MATCH.fname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    Row(MATCHING_RULES=base+5,  MAIN_TABLE=tc, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.lname = MATCH.lname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    Row(MATCHING_RULES=base+6,  MAIN_TABLE=tc, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.phone = MATCH.phone',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    Row(MATCHING_RULES=base+7,  MAIN_TABLE=tc, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.birth_date = MATCH.birth_date', GROUP=1, WEIGHT=0.35, ACT_F=1),
    # ts → tc (3 rules: id_card, fname, lname — ts has no phone/birth_date)
    Row(MATCHING_RULES=base+8,  MAIN_TABLE=ts, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.id_card = MATCH.id_card',       GROUP=1, WEIGHT=1.0,  ACT_F=1),
    Row(MATCHING_RULES=base+9,  MAIN_TABLE=ts, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.fname = MATCH.fname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
    Row(MATCHING_RULES=base+10, MAIN_TABLE=ts, MATCHING_TABLE=tc,
        MATCH_CONDITION='MAIN.lname = MATCH.lname',           GROUP=1, WEIGHT=0.35, ACT_F=1),
]
matching_schema = StructType([
    StructField('MATCHING_RULES',  IntegerType()),
    StructField('MAIN_TABLE',      StringType()),
    StructField('MATCHING_TABLE',  StringType()),
    StructField('MATCH_CONDITION', StringType()),
    StructField('GROUP',           IntegerType()),
    StructField('WEIGHT',          DoubleType()),
    StructField('ACT_F',           IntegerType()),
])
matching_df = spark.createDataFrame(matching_data, schema=matching_schema)
print(f"chv_config_matching — {matching_df.count()} rows to insert:")
display(matching_df)

# COMMAND ----------

matching_df.write.mode('append').insertInto(f'{ctrl}.chv_config_matching')
print("Inserted chv_config_matching")

# COMMAND ----------

check_data = [
    # tc → ts rules (base+0 to base+2)
    Row(MATCHING_RULES=base+0,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+0,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+1,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+1,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+2,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
    Row(MATCHING_RULES=base+2,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
    # tc → tc rules (base+3 to base+7)
    Row(MATCHING_RULES=base+3,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+3,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+4,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+4,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+5,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
    Row(MATCHING_RULES=base+5,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
    Row(MATCHING_RULES=base+6,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='phone',      ACT_F=1),
    Row(MATCHING_RULES=base+6,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='phone',      ACT_F=1),
    Row(MATCHING_RULES=base+7,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='birth_date', ACT_F=1),
    Row(MATCHING_RULES=base+7,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='birth_date', ACT_F=1),
    # ts → tc rules (base+8 to base+10)
    Row(MATCHING_RULES=base+8,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+8,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='id_card',    ACT_F=1),
    Row(MATCHING_RULES=base+9,  TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+9,  TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='fname',      ACT_F=1),
    Row(MATCHING_RULES=base+10, TABLE='MAIN',  RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
    Row(MATCHING_RULES=base+10, TABLE='MATCH', RULES_CHECK='CHECK_NULL', COLUMN='lname',      ACT_F=1),
]
check_schema = StructType([
    StructField('MATCHING_RULES', IntegerType()),
    StructField('TABLE',          StringType()),
    StructField('RULES_CHECK',    StringType()),
    StructField('COLUMN',         StringType()),
    StructField('ACT_F',          IntegerType()),
])
check_df = spark.createDataFrame(check_data, schema=check_schema)
print(f"chv_config_check_pre_validation — {check_df.count()} rows to insert:")
display(check_df)

# COMMAND ----------

check_df.write.mode('append').insertInto(f'{ctrl}.chv_config_check_pre_validation')
print("Inserted chv_config_check_pre_validation")

# COMMAND ----------

for tbl in ['chv_param_general', 'chv_config_pk', 'chv_config_pre_validation',
            'chv_config_matching', 'chv_config_check_pre_validation']:
    cnt = spark.table(f'{ctrl}.{tbl}').count()
    print(f"{tbl}: {cnt} total rows")
