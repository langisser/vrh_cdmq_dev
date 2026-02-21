# Databricks notebook source
# MAGIC %md
# MAGIC # CHV_MATCHING notebook — v2 (TIER + SUBJECT)
# MAGIC ### Runs MATCHING, POST-VALIDATION and BKEY generation
# MAGIC
# MAGIC #### Changes from v1
# MAGIC - All config/result tables use `_v2` suffix
# MAGIC - **TIER loop**: config rows are processed tier-by-tier; records matched in
# MAGIC   Tier 1 are excluded from Tier 2, eliminating the dual-BKEY problem
# MAGIC - **SUBJECT column**: carried from `chv_config_matching_v2.SUBJECT` through
# MAGIC   matching log → matching result → bkey table
# MAGIC
# MAGIC #### PARAMS
# MAGIC - table_nm - name of main matching table
# MAGIC - data_dt - date of register
# MAGIC - prcs_nm
# MAGIC - ld_id
# MAGIC - updt_prcs_nm
# MAGIC - updt_ld_id
# MAGIC
# MAGIC #### Sample PARAMS:
# MAGIC viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-01-05^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05^|1^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05^|1

# COMMAND ----------

# viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-01-05^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05^|1^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05^|1

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Variables

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql import functions as F
import pandas as pd
import re
from functools import reduce
from pyspark.sql.window import Window
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("PARAMS", "")
params = dbutils.widgets.get("PARAMS")
dbutils.widgets.text("ENV", "")
ENV = dbutils.widgets.get("ENV")
params = params.split('^|')
catalog = 'viriyah_cdqm_poc'
fw_schema = 'control_fw'
data_schema = 'silver'

# COMMAND ----------

#Schema of table in sql-pool on production environment replace %ENV% with '_'
# Ex. replace EDP%ENV%DMT to EDP_DMT
if ENV.lower() == 'prd' or ENV.lower() == 'preprd':
  env = "_"
else:
  env = "_"+ENV.upper()+"_"

# COMMAND ----------

table = params[0]
data_dt = params[1]
prcs_nm = params[2]
ld_id = params[3]
updt_prcs_nm = params[4]
updt_ld_id = params[5]
print(ENV)
print(env)
print(params)

# COMMAND ----------

# This function concatenates 'PK' values based on 'ORDER' conditions in a group, using COALESCE. It returns the resulting string
def custom_aggregation(group):
    if any(group['ORDER'].astype(int) > 1):
        key = []
        for i in group['PK'] :
            key.append(f"COALESCE({i},'')")
        return ', '.join(key)
    else:
        return f"COALESCE({group['PK'].iloc[0]},'')"

# COMMAND ----------

# Get & Prepare config — all refs use _v2 tables
# ORDER BY TIER, MATCHING_RULES so tier 1 is always processed first
config_pk = spark.sql(f"select * from {catalog}.{fw_schema}.CHV_CONFIG_PK_V2 ORDER BY ORDER".replace('%ENV%',env))
config_pk = config_pk.toPandas()
config_pk = config_pk.groupby('TABLE').apply(custom_aggregation).reset_index(name='PK_CONCATENATED')

config_matching = spark.sql(f"select * from {catalog}.{fw_schema}.chv_config_matching_v2 where act_f = '1' ORDER BY TIER, MATCHING_RULES".replace("%ENV%",env))
config_check_pre_val = spark.sql(f"select * from {catalog}.{fw_schema}.chv_config_check_pre_validation_v2".replace("%ENV%",env))

config_matching_pd = config_matching.toPandas()
config_check_pre_val_pd = config_check_pre_val.toPandas()

config = config_matching_pd.merge(config_pk,left_on='MAIN_TABLE',right_on='TABLE',how='left')
config = config.merge(config_pk,left_on='MATCHING_TABLE',right_on='TABLE',how='left')

config = config[(config["ACT_F"] == 1 ) & (config["MAIN_TABLE"].str.lower() == table.lower())]

# COMMAND ----------

config

# COMMAND ----------

# MAGIC %md
# MAGIC # MATCHING
# MAGIC ## Two steps:
# MAGIC ## 1. TIER loop — compare based on config rules, tier-by-tier
# MAGIC ## 2. Verify weight threshold per (MAIN, MATCH) pair

# COMMAND ----------

# Get PK of main table
main_key = config.iloc[0]['PK_CONCATENATED_x']

# COMMAND ----------

# extracts unique column names used in the matching conditions
all_parameter = []
for idx,i in config.iterrows() :
  parameter = re.findall(r'MATCH\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)
  all_parameter += parameter

all_parameter = list(set(all_parameter))

# COMMAND ----------

#Get general param from _v2 table
general_param = spark.sql(f"select * from {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 ".replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. TIER loop — build and run SQL per tier
# MAGIC
# MAGIC - Tier 1 runs against ALL main-table records
# MAGIC - Tier 2+ runs only against records NOT matched in any previous tier
# MAGIC   (added as NOT IN filter on the main sub-query)
# MAGIC - SUBJECT from `chv_config_matching_v2` is carried into the log

# COMMAND ----------

# Track which main keys have been matched in any previous tier
matched_keys = set()
# Accumulate per-tier matching log DataFrames
all_log_dfs = []

tier_values = sorted(config['TIER'].unique())

for tier_idx, tier in enumerate(tier_values):
    tier_config = config[config['TIER'] == tier].copy()

    print(f"\n=== Processing Tier {tier} ({len(tier_config)} rules) ===")

    inner_sql = []
    join_sql = []
    from_sql_parts = []

    for idx, i in tier_config.iterrows():

        # --- MATCH side sub-query ---
        sub_sql = f"SELECT '{i.MATCHING_RULES}' AS RULES,"
        parameter = re.findall(r'MATCH\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)
        main_parameter = re.findall(r'MAIN\.([a-zA-Z0-9_]+)', i.MATCH_CONDITION)

        for param in all_parameter :
            if param in parameter :
                sub_sql += f"{param} AS {param},"
            else :
                sub_sql += f"NULL AS {param},"

        # --- MAIN side sub-query (with SUBJECT) ---
        sub_main_sql = f"SELECT CAST('{table.lower()}' AS STRING) AS MAIN_TABLE,CAST('{i.MATCHING_TABLE.lower()}' AS STRING) AS MATCHING_TABLE,CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) AS KEY,"
        for param in all_parameter :
            if param in main_parameter :
                sub_main_sql += f"{param} AS {param},"
            else :
                sub_main_sql += f"NULL AS {param},"

        # Carry SUBJECT and RULES from config row
        sub_main_sql += f"'{i.SUBJECT}' AS SUBJECT,'{i.MATCHING_RULES}' AS RULES\n"

        sub_main_sql += f"FROM {table} SRC\n"
        sub_main_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 PARM ON LOWER(PARM.PARAM_NAME) = '{table.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
        sub_main_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT_V2 VLD_RESULT\n"
        sub_main_sql += f"ON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MAIN_TABLE.lower()}' AND CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) = VLD_RESULT.KEY AND VLD_RESULT.RESULT = 'PASSED' AND VLD_RESULT.DATA_DT = '{data_dt}'\n"

        match_key = i.PK_CONCATENATED_y
        main_key = i.PK_CONCATENATED_x
        sub_sql += f"CONCAT_WS(',',{match_key}) AS KEY,'{i.MATCHING_TABLE.lower()}' AS TABLE \n"
        sub_sql += f"FROM {i.MATCHING_TABLE} SRC \n"
        sub_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 PARM ON LOWER(PARM.PARAM_NAME) = '{i.MATCHING_TABLE.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
        sub_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT_V2 VLD_RESULT\n"
        sub_sql += f"ON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MATCHING_TABLE.lower()}' AND CONCAT_WS(',',{match_key}) = VLD_RESULT.KEY\n AND VLD_RESULT.RESULT = 'PASSED'"

        vld_sql = 'AND ('
        main_vld_sql = 'AND ('
        _config_check_pre_val_pd = config_check_pre_val_pd.loc[(config_check_pre_val_pd['MATCHING_RULES'] == i.MATCHING_RULES)]
        sub_vld_sql = []
        sub_main_vld_sql = []
        for j in range(len(_config_check_pre_val_pd)) :
            if _config_check_pre_val_pd.iloc[j]['TABLE'] == 'MATCH' :
                rule = _config_check_pre_val_pd.iloc[j]['RULES_CHECK']
                column = _config_check_pre_val_pd.iloc[j]['COLUMN']
                sub_vld_sql.append(f"(VLD_RESULT.RULES = '{rule}' AND COLUMN = '{column}' AND RESULT = 'PASSED')")
            else :
                rule = _config_check_pre_val_pd.iloc[j]['RULES_CHECK']
                column = _config_check_pre_val_pd.iloc[j]['COLUMN']
                sub_main_vld_sql.append(f"(VLD_RESULT.RULES = '{rule}' AND COLUMN = '{column}' AND RESULT = 'PASSED')")

        vld_sql += 'or'.join(sub_vld_sql)
        vld_sql += ")\n"
        sub_sql += vld_sql
        sub_sql += f"WHERE SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('%DATA_DT%','yyyy-MM-dd'),{general_param.filter(lower(col('PARAM_NAME')) == i.MATCHING_TABLE.lower() ).collect()[0].PARAM_VAL_NUMBER}),'yyyy-MM-dd')"

        main_vld_sql += 'or'.join(sub_main_vld_sql)
        main_vld_sql += ")\n"
        sub_main_sql += main_vld_sql
        sub_main_sql += f"WHERE SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('%DATA_DT%','yyyy-MM-dd'),{general_param.filter(lower(col('PARAM_NAME')) == table.lower()).collect()[0].PARAM_VAL_NUMBER}),'yyyy-MM-dd')"

        # --- TIER exclusion filter ---
        # From Tier 2 onwards, exclude keys already matched in previous tiers
        if tier_idx > 0 and matched_keys:
            keys_str = "','".join(str(k) for k in matched_keys)
            sub_main_sql += f"\nAND CONCAT_WS(',',{main_key}) NOT IN ('{keys_str}')"
        elif tier_idx > 0 and not matched_keys:
            # No keys were matched in previous tiers; no filter needed
            pass

        from_sql_parts.append(sub_main_sql)
        inner_sql.append(sub_sql)

        # Join condition between main_df and match_df
        if i.MAIN_TABLE != i.MATCHING_TABLE:
            sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}')".replace('MAIN.%PK_MAIN%',f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%','KEY')
        else :
            sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}' AND MAIN.KEY <> MATCH.KEY)".replace('MAIN.%PK_MAIN%',f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%','KEY')
        join_sql.append(sub_sql2)

    # Build tier SQL — outer SELECT includes MAIN.SUBJECT
    tier_sql = f"SELECT MAIN.MAIN_TABLE AS MAIN_TABLE,MATCHING_TABLE AS MATCHING_TABLE,MAIN.RULES AS MATCHING_RULES,MAIN.KEY AS KEY_MAIN, MATCH.KEY AS KEY_MATCH,CASE WHEN MATCH.RULES IS NULL THEN 'FAILED' ELSE 'PASSED' END AS RESULT,'{ld_id}' AS LD_ID,'{updt_prcs_nm}' AS UPDT_PRCS_NM,'{updt_ld_id}' AS UPDT_LD_ID,MAIN.SUBJECT AS SUBJECT\n "
    from_clause = '\nUNION ALL\n'.join(from_sql_parts)
    tier_sql += f"FROM ({from_clause}) AS MAIN\n"
    tier_sql += f"LEFT JOIN ("
    tier_sql += '\n UNION ALL \n'.join(inner_sql) + ') MATCH\n'
    tier_sql += f"ON {'OR'.join(join_sql)}"

    tier_sql = tier_sql.replace('%DATA_DT%',data_dt).replace('%ENV%',env)
    print(tier_sql)

    tier_df = spark.sql(tier_sql)

    # Collect main keys that PASSED in this tier → exclude from next tier
    tier_passed = tier_df.filter("RESULT = 'PASSED'")
    matched_this_tier = set(
        row['KEY_MAIN'] for row in tier_passed.select('KEY_MAIN').distinct().collect()
    )
    print(f"Tier {tier}: {len(matched_this_tier)} keys matched")

    all_log_dfs.append(tier_df)
    matched_keys |= matched_this_tier

# COMMAND ----------

# Combine all tier logs into one DataFrame
full_df = reduce(lambda a, b: a.unionAll(b), all_log_dfs)

# COMMAND ----------

# Write combined log to chv_matching_log_v2
full_df.createOrReplaceTempView('mytempTable')
spark.sql(f"insert overwrite {catalog}.{fw_schema}.chv_matching_log_v2 partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from mytempTable".replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECK MATCHING CONDITION

# COMMAND ----------

# get config_matching for this table
config_matching = config_matching.filter(lower(config_matching.MAIN_TABLE) == table.lower())
config_matching.display()

# COMMAND ----------

# Read back from chv_matching_log_v2 (includes SUBJECT column)
df = spark.sql(f"select * from {catalog}.{fw_schema}.chv_matching_log_v2 where DATA_DT = '{data_dt}' and prcs_nm = '{prcs_nm}'".replace("%ENV%",env))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify weight threshold — accumulate SUBJECT through pivot

# COMMAND ----------

MAIN_TABLE_L = []
MATCHING_TABLE_L = []
KEY_MAIN_L = []
KEY_MATCH_L = []
SUBJECT_L = []

pair = []
for row in config_matching.collect() :
  if [row.MAIN_TABLE.lower(),row.MATCHING_TABLE.lower()] not in pair :
    pair.append([row.MAIN_TABLE.lower(),row.MATCHING_TABLE.lower()])

# PIVOT MATCHING RESULT FOR EACH RULE OF MAIN MATCH PAIR TO 1 RECORDS
for main,match in pair :
  _df = df.filter((df.MAIN_TABLE == main.replace('%ENV%',env)) & (df.MATCHING_TABLE == match.replace('%ENV%',env)))
  # Include SUBJECT in groupby so it survives the pivot
  pivot_df = _df.groupby("MAIN_TABLE", "MATCHING_TABLE", "KEY_MAIN", "KEY_MATCH", "SUBJECT")\
                .pivot("MATCHING_RULES")\
                .agg(first("RESULT"))
  pivot_df.createOrReplaceTempView('pivot_df')
  threshold_matching = config_matching.filter(lower(config_matching.MATCHING_TABLE) == match)
  threshold_dict = {}

  for t_row in threshold_matching.collect() :
    if t_row.GROUP not in threshold_dict.keys() :
      threshold_dict[t_row.GROUP] = {'mtch_rules' : {t_row.MATCHING_RULES:float(t_row.WEIGHT)}}
    else :
      threshold_dict[t_row.GROUP]['mtch_rules'][t_row.MATCHING_RULES] = float(t_row.WEIGHT)
  for p_row in pivot_df.collect() :
    for key in threshold_dict.keys() :
      c = 0
      for rule in threshold_dict[key]['mtch_rules'] :
        if str(rule) in pivot_df.columns:
          index = pivot_df.columns.index(str(rule))
          if p_row[index] == 'PASSED':
            c += threshold_dict[key]['mtch_rules'][rule]
      if c >= 1 :
        MAIN_TABLE_L.append(p_row.MAIN_TABLE)
        MATCHING_TABLE_L.append(p_row.MATCHING_TABLE)
        KEY_MAIN_L.append(p_row.KEY_MAIN)
        KEY_MATCH_L.append(p_row.KEY_MATCH)
        SUBJECT_L.append(p_row.SUBJECT)

# match_df now includes SUBJECT
match_df = pd.DataFrame({'MAIN_TABLE':MAIN_TABLE_L,
                         'MATCHING_TABLE' :MATCHING_TABLE_L,
                         'KEY_MAIN':KEY_MAIN_L,
                         'KEY_MATCH':KEY_MATCH_L,
                         'SUBJECT':SUBJECT_L})

schema = StructType([
    StructField("MAIN_TABLE", StringType(), True),
    StructField("MATCHING_TABLE", StringType(), True),
    StructField("KEY_MAIN", StringType(), True),
    StructField("KEY_MATCH", StringType(), True),
    StructField("SUBJECT", StringType(), True)
])

match_df = match_df.drop_duplicates()
match_df = spark.createDataFrame(match_df,schema=schema)

# COMMAND ----------

match_df = match_df.withColumn("LD_ID",lit(ld_id).cast(LongType()))\
            .withColumn("UPDT_PRCS_NM",lit(updt_prcs_nm))\
            .withColumn("UPDT_LD_ID",lit(updt_ld_id)\
            .cast(LongType()))
match_df.createOrReplaceTempView('match_df')

# COMMAND ----------

spark.sql(f"insert overwrite {catalog}.{fw_schema}.chv_matching_result_v2 partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select MAIN_TABLE, MATCHING_TABLE, KEY_MAIN, KEY_MATCH, LD_ID, UPDT_PRCS_NM, UPDT_LD_ID, SUBJECT from match_df".replace('%ENV%',env))

# COMMAND ----------

match_df.display()

# COMMAND ----------

post_df = match_df.dropDuplicates()

# COMMAND ----------

post_df.createOrReplaceTempView('temp_post')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate BKEY
# MAGIC Generate a BKEY after passing post-validation for customers identified as identical.

# COMMAND ----------

#Get max BKEY from chv_table_bkey_v2
bkey = int(spark.sql(f"select COALESCE(max(BKEY),0) as BKEY from {catalog}.{data_schema}.chv_table_bkey_v2".replace('%ENV%',env)).collect()[0].BKEY)
print(bkey)

# COMMAND ----------

key_main = config_pk[config_pk['TABLE'].str.lower() == table.lower()].reset_index().PK_CONCATENATED[0]

# COMMAND ----------

# Join temp_post (which has SUBJECT) with bkey_table_v2 for existing bkeys
df = spark.sql(f"""select temp_post.*
                        ,bkey_match.KEY as MATCH_KEY_BKEY
                        ,lower(bkey_match.table) as MATCH_TABLE_BKEY
                        ,bkey_match.BKEY as MATCH_BKEY
                        ,bkey_main.KEY as MAIN_KEY_BKEY
                        ,lower(bkey_main.table) as MAIN_TABLE_BKEY
                        ,bkey_main.BKEY as MAIN_BKEY
                         from temp_post
                left join {catalog}.{data_schema}.chv_table_bkey_v2 bkey_match
                on temp_post.key_match = bkey_match.key
                and lower(bkey_match.TABLE) = lower(temp_post.matching_table)

                left join {catalog}.{data_schema}.chv_table_bkey_v2 bkey_main
                on temp_post.key_main = bkey_main.key
                and lower(bkey_main.TABLE) = lower(temp_post.main_table)
                """.replace("%ENV%",env))

not_pass_post = spark.sql(f"""select concat_ws(',',{key_main}) as DATA_KEY from {table} as main

                              LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL_V2 PARM
                              ON LOWER(REPLACE(REPLACE(PARM.PARAM_NAME,'%','|'),'|ENV|','%ENV%')) = '{table.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'

                              left join temp_post
                              on concat_ws(',',{key_main}) = temp_post.key_main

                              left join {catalog}.{data_schema}.chv_table_bkey_v2 bkey
                              on concat_ws(',',{key_main}) = bkey.key
                              and lower(bkey.table) = '{table.lower()}'

                              where temp_post.key_match is null and bkey.key is null
                              and main.data_dt = DATE_FORMAT(DATE_ADD(TO_DATE('{data_dt}','yyyy-MM-dd'),CAST(PARM.PARAM_VAL_NUMBER AS INT)),'yyyy-MM-dd') """.replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate BKEY that exists in chv_table_bkey_v2

# COMMAND ----------

bkey_exists = df.filter(col("MATCH_BKEY").isNotNull() & col("MAIN_BKEY").isNull())
# Carry SUBJECT from match_df (available via temp_post → df join)
bkey_exists_rs = bkey_exists.select(
    col('KEY_MAIN').alias('KEY'),
    col('MAIN_TABLE').alias('TABLE'),
    col('MATCH_BKEY').alias('BKEY'),
    col('SUBJECT')
)
bkey_exists_rs = bkey_exists_rs.dropDuplicates()

# COMMAND ----------

bkey_exists_rs = bkey_exists_rs.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate new BKEY for pairs with no existing BKEY

# COMMAND ----------

gen_bkey_filter = df.filter((df["MATCH_BKEY"].isNull() & df["MAIN_BKEY"].isNull()))

# Build subject_map: (table, key) → SUBJECT
# Used later to attach SUBJECT when building bkey_df from bkey_dict
subject_map = {}
for row in gen_bkey_filter.collect():
    subject_map[(row.MAIN_TABLE, row.KEY_MAIN)] = row.SUBJECT
    subject_map[(row.MATCHING_TABLE, row.KEY_MATCH)] = row.SUBJECT

gen_bkey = gen_bkey_filter

# COMMAND ----------

table_s = set()
for i in config_matching.select('MAIN_TABLE').dropDuplicates().collect() :
  table_s.add(i.MAIN_TABLE.replace('%ENV%',env).lower())
for i in config_matching.select('MATCHING_TABLE').dropDuplicates().collect() :
  table_s.add(i.MATCHING_TABLE.replace('%ENV%',env).lower())

# COMMAND ----------

bkey_dict = {}
for i in table_s :
  bkey_dict[i] = {}

#generate relation of main_table,match_table,main_key,match_key in dict format
for i in gen_bkey.orderBy("KEY_MAIN", "KEY_MATCH").collect():
  if i.KEY_MAIN not in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH not in bkey_dict[i.MATCHING_TABLE].keys():
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN] = {}
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH] = {}
    for t in table_s :
      bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][t] = {}
      bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][t] = {}
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE] = set([i.KEY_MATCH])
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE] = set([i.KEY_MAIN])

  elif i.KEY_MAIN not in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH in bkey_dict[i.MATCHING_TABLE].keys() :
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN] = {}
    for t in table_s :
      bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][t] = {}
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE] = set([i.KEY_MATCH])
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE] = set(bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE]) | set([i.KEY_MAIN])

  elif i.KEY_MAIN in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH not in bkey_dict[i.MATCHING_TABLE].keys() :
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH] = {}
    for t in table_s :
      bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][t] = {}
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE] = set([i.KEY_MAIN])
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE] = set(bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE]) | set([i.KEY_MATCH])

  else :
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE] = set(bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH][i.MAIN_TABLE]) | set([i.KEY_MAIN])
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE] = set(bkey_dict[i.MAIN_TABLE][i.KEY_MAIN][i.MATCHING_TABLE]) | set([i.KEY_MATCH])

# COMMAND ----------

# recursive function to find all records that should share the same BKEY
def find_related_records(main_table, main_key, bkey_dict, visited=None):
    if visited is None:
        visited = set()

    records = set()

    if main_key not in visited:
        visited.add(main_key)
        records.add((main_table, main_key))

        if main_table in bkey_dict and main_key in bkey_dict[main_table]:
            match_info = bkey_dict[main_table][main_key]

            for match_table, match_keys in match_info.items():
                for match_key in match_keys:
                    records |= find_related_records(match_table, match_key, bkey_dict, visited)

    return records

data = []
# find all relations for all keys
for i in gen_bkey.orderBy("KEY_MAIN", "KEY_MATCH").collect():
  result = find_related_records(table.replace('%ENV%',env).lower(), i.KEY_MAIN,bkey_dict)
  for r in result :
    data.append((table.replace('%ENV%',env).lower(),r[0],i.KEY_MAIN,r[1]))

# Define the schema for the DataFrame
schema = StructType([
    StructField("MAIN_TABLE", StringType(), True),
    StructField("MATCHING_TABLE", StringType(), True),
    StructField("KEY_MAIN", StringType(), True),
    StructField("KEY_MATCH", StringType(), True)
])

# Create a DataFrame
gen_bkey = spark.createDataFrame(data, schema=schema)
gen_bkey = gen_bkey.dropDuplicates()

# COMMAND ----------

bkey_dict = {}
#generate bkey to PK in dict format
for i in gen_bkey.orderBy("KEY_MAIN", "KEY_MATCH").collect():
  if i.MAIN_TABLE not in bkey_dict.keys() :
    bkey += 1
    bkey_dict[i.MAIN_TABLE] = {i.KEY_MAIN : [str(bkey)] }
  if i.MATCHING_TABLE not in bkey_dict.keys() :
    if i.KEY_MAIN in bkey_dict[i.MAIN_TABLE].keys():
      bkey_dict[i.MATCHING_TABLE] = {i.KEY_MATCH : bkey_dict[i.MAIN_TABLE][i.KEY_MAIN]}
    else:
      bkey += 1
      bkey_dict[i.MATCHING_TABLE] = {i.KEY_MATCH : str(bkey) }


  if i.KEY_MAIN not in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH not in bkey_dict[i.MATCHING_TABLE].keys():
    bkey += 1
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN] = [str(bkey)]
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH] = [str(bkey)]
  elif i.KEY_MAIN not in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH in bkey_dict[i.MATCHING_TABLE].keys() :
    bkey_dict[i.MAIN_TABLE][i.KEY_MAIN] = bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH]
  elif i.KEY_MAIN in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH not in bkey_dict[i.MATCHING_TABLE].keys() :
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH] = bkey_dict[i.MAIN_TABLE][i.KEY_MAIN]
  else :
    if set(bkey_dict[i.MAIN_TABLE][i.KEY_MAIN]) != set(bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH]) :
      print(i.KEY_MAIN,bkey_dict[i.MAIN_TABLE][i.KEY_MAIN],i.KEY_MATCH,bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH])

# COMMAND ----------

TABLE = []
KEY = []
BKEY = []
SUBJECT_LIST = []
# Build bkey DataFrame from dict, attaching SUBJECT via subject_map lookup
for t in bkey_dict.keys() :
  for key in bkey_dict[t].keys() :
    for b_key in bkey_dict[t][key] :
      TABLE.append(t)
      KEY.append(key)
      BKEY.append(b_key)
      SUBJECT_LIST.append(subject_map.get((t, key), 'default'))

bkey_df = pd.DataFrame({'KEY':KEY,'TABLE':TABLE,'BKEY':BKEY,'SUBJECT':SUBJECT_LIST})

schema = StructType([
    StructField("KEY", StringType(), True),
    StructField("TABLE", StringType(), True),
    StructField("BKEY", StringType(), True),
    StructField("SUBJECT", StringType(), True)
])

gen_bkey_rs = spark.createDataFrame(bkey_df,schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate BKEY for keys that did not pass post-validation and have no existing BKEY

# COMMAND ----------

not_pass_post = not_pass_post.withColumn("BKEY", lit(None))
window_spec = Window.orderBy("BKEY")

not_pass_post = not_pass_post.withColumn("running_number", row_number().over(window_spec))
not_pass_post = not_pass_post.withColumn('BKEY',col('running_number')+bkey)\
    .withColumn('SUBJECT', lit('default'))\
    .select(col('DATA_KEY').alias('KEY'),lit(table.replace('%ENV%',env).lower()).alias("TABLE"),col('BKEY'),col('SUBJECT'))

# COMMAND ----------

def normalize_key(df):
    return df.withColumn("KEY", col("KEY").cast("string"))

final_df = (
    normalize_key(gen_bkey_rs)
    .unionAll(normalize_key(bkey_exists_rs))
    .unionAll(normalize_key(not_pass_post))
)

final_df = (
    final_df
    .withColumn("BKEY", col("BKEY").cast("int"))
    .withColumn("LD_ID", lit(ld_id).cast(LongType()))
    .withColumn("UPDT_PRCS_NM", lit(updt_prcs_nm))
    .withColumn("UPDT_LD_ID", lit(updt_ld_id).cast(LongType()))
    .select("KEY", "TABLE", "BKEY", "LD_ID", "UPDT_PRCS_NM", "UPDT_LD_ID", "SUBJECT")
)

# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceTempView('myTemptable')

# COMMAND ----------

spark.sql(f"insert overwrite {catalog}.{data_schema}.chv_table_bkey_v2 partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from myTemptable".replace('%ENV%',env))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT *
        FROM {catalog}.{data_schema}.chv_table_bkey_v2
        WHERE DATA_DT = '{data_dt}'
          AND PRCS_NM = '{prcs_nm}'
    """)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparison queries
# MAGIC Run these after executing both old and new pipelines on the same test data.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Old result
# MAGIC SELECT KEY, BKEY FROM viriyah_cdqm_poc.silver.chv_table_bkey
# MAGIC WHERE PRCS_NM LIKE '%TC_CASE2_SIMILARITY%'
# MAGIC ORDER BY KEY;
# MAGIC
# MAGIC -- New result (with SUBJECT)
# MAGIC SELECT KEY, BKEY, SUBJECT FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
# MAGIC WHERE PRCS_NM LIKE '%TC_CASE2_SIMILARITY%'
# MAGIC ORDER BY KEY;
# MAGIC
# MAGIC -- Side-by-side comparison
# MAGIC SELECT
# MAGIC   COALESCE(o.KEY, n.KEY) AS KEY,
# MAGIC   o.BKEY  AS OLD_BKEY,
# MAGIC   n.BKEY  AS NEW_BKEY,
# MAGIC   n.SUBJECT,
# MAGIC   CASE WHEN o.BKEY = n.BKEY THEN 'MATCH' ELSE 'DIFF' END AS STATUS
# MAGIC FROM viriyah_cdqm_poc.silver.chv_table_bkey o
# MAGIC FULL OUTER JOIN viriyah_cdqm_poc.silver.chv_table_bkey_v2 n
# MAGIC   ON o.KEY = n.KEY AND o.TABLE = n.TABLE
# MAGIC WHERE o.PRCS_NM LIKE '%TC_CASE2_SIMILARITY%'
# MAGIC    OR n.PRCS_NM LIKE '%TC_CASE2_SIMILARITY%'
# MAGIC ORDER BY KEY;
# MAGIC ```
