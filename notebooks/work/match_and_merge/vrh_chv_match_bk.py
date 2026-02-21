# Databricks notebook source
# MAGIC %md
# MAGIC # CHV_MATCHING notebook
# MAGIC ### This notebook runs a MATCHING,POST-VALIDATION AND GENERATE UNIQE KEY and inserts the results into a table
# MAGIC #### PARAMS
# MAGIC - table_nm - name of main matching table
# MAGIC - data_dt - date of register
# MAGIC - prcs_nm 
# MAGIC - ld_id 
# MAGIC - updt_prcs_nm
# MAGIC - updt_ld_id
# MAGIC
# MAGIC #### Sample PARAMS: 
# MAGIC viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-01-05^|X2_CHV_MATCHING_EDM_D_1^|14^|X2_CHV_MATCHING_EDM_D_1^|14

# COMMAND ----------

# viriyah_cdqm_poc.silver.SOURCE_MOTOR^|2026-01-05^|X2_CHV_MATCHING_EDM_D_1^|14^|X2_CHV_MATCHING_EDM_D_1^|14

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

# Get & Prepare config for matching
config_pk = spark.sql(f"select * from {catalog}.{fw_schema}.CHV_CONFIG_PK ORDER BY ORDER".replace('%ENV%',env))
config_pk = config_pk.toPandas()
config_pk = config_pk.groupby('TABLE').apply(custom_aggregation).reset_index(name='PK_CONCATENATED')


config_matching = spark.sql(f"select * from {catalog}.{fw_schema}.chv_config_matching where act_f = '1'".replace("%ENV%",env))
config_check_pre_val = spark.sql(f"select * from {catalog}.{fw_schema}.chv_config_check_pre_validation".replace("%ENV%",env))

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
# MAGIC ## In matching part is contain 2 step
# MAGIC ## 1. Compare based on specified rules in matching_config
# MAGIC ## 2. Verify each main key and match key pair, ensuring a cumulative weight surpasses a threshold of 1

# COMMAND ----------

# Get PK of main Table
main_key = config.iloc[0]['PK_CONCATENATED_x']

# COMMAND ----------

# extracts unique column names used in the matching conditions from the "MATCH_CONDITION" column
all_parameter = []
for idx,i in config.iterrows() :
  parameter = [element.split('.')[1] for element in i.MATCH_CONDITION.split(' ') if 'MATCH.' in element and '%' not in element]
  all_parameter += parameter

all_parameter = list(set(all_parameter))

# COMMAND ----------

#Get general param
general_param = spark.sql(f"select * from {catalog}.{fw_schema}.CHV_PARAM_GENERAL ".replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Compare based on specified rules in matching_config

# COMMAND ----------

# generate sql script for query matching dataframe
## select ...
## from (main_df)
## left join (match_df)
## Main DataFrame (main_df):
## - Queries for rules in main-table.
## - Inner joins with pre-validation_result.
## Match DataFrame (match_df):
## - Queries for rules in match-table.
## - Inner joins with pre-validation_result.
## Result: RULES, IC_NO, etc.
## Left Join:
## - main_df left-joined with match_df.
## - Conditions based on matching_rule.
inner_sql = []
join_sql = []
from_sql = []
for idx,i in config.iterrows() :

  #SUB-QUERY IN LEFT JOIN MATCH ()
  sub_sql = f"SELECT '{i.MATCHING_RULES}' AS RULES,"
  parameter = [element.split('.')[1] for element in i.MATCH_CONDITION.split(' ') if 'MATCH.' in element and '%' not in element]
  main_parameter = [element.split('.')[1] for element in i.MATCH_CONDITION.split(' ') if 'MAIN.' in element and '%' not in element]
      
  for param in all_parameter :
    if param in parameter :
      sub_sql += f"{param} AS {param},"
    else :
      sub_sql += f"NULL AS {param},"
#SUB-QUERY IN FROM ()
  sub_main_sql = f"SELECT CAST('{table.lower()}' AS STRING) AS MAIN_TABLE,CAST('{i.MATCHING_TABLE.lower()}' AS STRING) AS MATCHING_TABLE,CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) AS KEY,"
  for param in all_parameter :
    if param in main_parameter :
      sub_main_sql += f"{param} AS {param},"
    else :
      sub_main_sql += f"NULL AS {param},"
  
  sub_main_sql += f"'{i.MATCHING_RULES}' AS RULES\n"

  sub_main_sql += f"FROM {table} SRC\n"
  sub_main_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL PARM ON LOWER(PARM.PARAM_NAME) = '{table.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
  sub_main_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT VLD_RESULT\n"  
  sub_main_sql += f"ON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MAIN_TABLE.lower()}' AND CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']}) = VLD_RESULT.KEY AND VLD_RESULT.RESULT = 'PASSED' AND VLD_RESULT.DATA_DT = '{data_dt}'\n"


  match_key = i.PK_CONCATENATED_y
  main_key = i.PK_CONCATENATED_x
  sub_sql += f"CONCAT_WS(',',{match_key}) AS KEY,'{i.MATCHING_TABLE.lower()}' AS TABLE \n"
  sub_sql += f"FROM {i.MATCHING_TABLE} SRC \n"
  sub_sql += f"LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL PARM ON LOWER(PARM.PARAM_NAME) = '{i.MATCHING_TABLE.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'"
  sub_sql += f"INNER JOIN {catalog}.{fw_schema}.CHV_PRE_VALIDATION_RESULT VLD_RESULT\n"
  sub_sql += f"ON VLD_RESULT.DATA_DT = '{data_dt}' AND LOWER(VLD_RESULT.TABLE) = '{i.MATCHING_TABLE.lower()}' AND CONCAT_WS(',',{match_key}) = VLD_RESULT.KEY\n AND VLD_RESULT.RESULT = 'PASSED'"
  vld_sql = 'AND ('
  main_vld_sql = 'AND ('
  _config_check_pre_val_pd = config_check_pre_val_pd.loc[(config_check_pre_val_pd['MATCHING_RULES'] == i.MATCHING_RULES)]
  sub_vld_sql = []
  sub_main_vld_sql = []
  # On condition with pre-validation 
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
  from_sql.append(sub_main_sql)

  inner_sql.append(sub_sql)
  # On condition between main_df and match df
  if i.MAIN_TABLE != i.MATCHING_TABLE:
    sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}')".replace('MAIN.%PK_MAIN%',f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%','KEY')
  else : 
    sub_sql2 = f"({i.MATCH_CONDITION.replace('LEFT JOIN MATCH ON','').strip()} AND MAIN.RULES = '{i.MATCHING_RULES}' AND MATCH.RULES = '{i.MATCHING_RULES}' AND MAIN.KEY <> MATCH.KEY)".replace('MAIN.%PK_MAIN%',f"CONCAT_WS(',',{config.iloc[0]['PK_CONCATENATED_x']})").replace('%PK_MATCH%','KEY')
  join_sql.append(sub_sql2)

sql = f"SELECT MAIN.MAIN_TABLE AS MAIN_TABLE,MATCHING_TABLE AS MATCHING_TABLE,MAIN.RULES AS MATCHING_RULES,MAIN.KEY AS KEY_MAIN, MATCH.KEY AS KEY_MATCH,CASE WHEN MATCH.RULES IS NULL THEN 'FAILED' ELSE 'PASSED' END AS RESULT,'{ld_id}' AS LD_ID,'{updt_prcs_nm}' AS UPDT_PRCS_NM,'{updt_ld_id}' AS UPDT_LD_ID\n "
from_sql = '\nUNION ALL\n'.join(from_sql)
sql += f"FROM ({from_sql}) AS MAIN\n"
sql += f"LEFT JOIN ("
sql += '\n UNION ALL \n'.join(inner_sql) + ') MATCH\n'
sql += f"ON {'OR'.join(join_sql)}"

sql = sql.replace('%DATA_DT%',data_dt).replace('%ENV%',env)
print(sql)

# COMMAND ----------

df = spark.sql(sql)  # Ensure the SQL string in 'sql' variable has balanced parentheses.

# COMMAND ----------

# regis df as temp view
df.createOrReplaceTempView('mytempTable')
spark.sql(f"insert overwrite {catalog}.{fw_schema}.chv_matching_log partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from mytempTable".replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC # CHECK MATCHING CONDITION

# COMMAND ----------

# get config_matching
config_matching = config_matching.filter(lower(config_matching.MAIN_TABLE) == table.lower())
config_matching.display()

# COMMAND ----------

# Convert PySpark DataFrame to Pandas DataFrame
df = spark.sql(f"select * from {catalog}.{fw_schema}.chv_matching_log where DATA_DT = '{data_dt}' and prcs_nm = '{prcs_nm}'".replace("%ENV%",env))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Verify each main key and match key pair, ensuring a cumulative weight surpasses a threshold of 1

# COMMAND ----------

MAIN_TABLE_L = []
MATCHING_TABLE_L = []
KEY_MAIN_L = []
KEY_MATCH_L = []
# RESULT_L = []
# SUM_WEIGHT_L = []

pair = []
for row in config_matching.collect() :
  if [row.MAIN_TABLE.lower(),row.MATCHING_TABLE.lower()] not in pair :
    pair.append([row.MAIN_TABLE.lower(),row.MATCHING_TABLE.lower()])

# PIVOT MATCHING RESULT FOR EACH RULE OF MAIN MATCH PAIR TO 1 RECORDS
for main,match in pair :
  _df = df.filter((df.MAIN_TABLE == main.replace('%ENV%',env)) & (df.MATCHING_TABLE == match.replace('%ENV%',env)))
  pivot_df = _df.groupby("MAIN_TABLE", "MATCHING_TABLE", "KEY_MAIN", "KEY_MATCH")\
                .pivot("MATCHING_RULES")\
                .agg(first("RESULT"))
  pivot_df.createOrReplaceTempView('pivot_df')
  threshold_matching = config_matching.filter(lower(config_matching.MATCHING_TABLE) == match)
  threshold_dict = {}

# Sum the weights if more than 1 match is successful, and consider the result as 'PASS'
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
        # RESULT_L.append('PASSED')
        # SUM_WEIGHT_L.append(c)

      # else :
      #   MAIN_TABLE_L.append(p_row.MAIN_TABLE)
      #   MATCHING_TABLE_L.append(p_row.MATCHING_TABLE)
      #   KEY_MAIN_L.append(p_row.KEY_MAIN)
      #   KEY_MATCH_L.append(p_row.KEY_MATCH)
      #   RESULT_L.append('FAILED')
      #   SUM_WEIGHT_L.append(c)

# match_df = pd.DataFrame({'MAIN_TABLE':MAIN_TABLE_L,
#                          'MATCHING_TABLE' :MATCHING_TABLE_L,
#                          'KEY_MAIN':KEY_MAIN_L,
#                          'KEY_MATCH':KEY_MATCH_L,
#                          'RESULT':RESULT_L,
#                          'SUM_WEIGHT':SUM_WEIGHT_L})

match_df = pd.DataFrame({'MAIN_TABLE':MAIN_TABLE_L,
                         'MATCHING_TABLE' :MATCHING_TABLE_L,
                         'KEY_MAIN':KEY_MAIN_L,
                         'KEY_MATCH':KEY_MATCH_L,})

# schema = StructType([
#     StructField("MAIN_TABLE", StringType(), True),
#     StructField("MATCHING_TABLE", StringType(), True),
#     StructField("KEY_MAIN", StringType(), True),
#     StructField("KEY_MATCH", StringType(), True),
#     StructField("RESULT", StringType(), True),
#     StructField("SUM_WEIGHT", StringType(), True)
# ])

schema = StructType([
    StructField("MAIN_TABLE", StringType(), True),
    StructField("MATCHING_TABLE", StringType(), True),
    StructField("KEY_MAIN", StringType(), True),
    StructField("KEY_MATCH", StringType(), True)
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

spark.sql(f"insert overwrite {catalog}.{fw_schema}.chv_matching_result partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from match_df".replace('%ENV%',env))

# COMMAND ----------

match_df.display()

# COMMAND ----------

post_df = match_df.dropDuplicates()

# COMMAND ----------

# post_df = post_df.withColumn("LD_ID",lit(ld_id).cast(LongType()))\
#             .withColumn("UPDT_PRCS_NM",lit(updt_prcs_nm))\
#             .withColumn("UPDT_LD_ID",lit(updt_ld_id)\
#             .cast(LongType()))
post_df.createOrReplaceTempView('temp_post')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generate CHV_ID
# MAGIC Generate a CHV_ID after passing post-validation for customers who are identified as identical.
# MAGIC

# COMMAND ----------

#Get max CHV_ID from table viriyah_cdqm_poc.silver.chv_table_bkey
bkey = int(spark.sql(f"select COALESCE(max(CHV_ID),0) as BKEY from {catalog}.{data_schema}.chv_table_bkey".replace('%ENV%',env)).collect()[0].BKEY)
print(bkey)

# COMMAND ----------

key_main = config_pk[config_pk['TABLE'].str.lower() == table.lower()].reset_index().PK_CONCATENATED[0]

# COMMAND ----------

#Filter keys that have successfully passed post-validation to df dataframe and those that have not passed to not_pass_post dataframe.
df = spark.sql(f"""select temp_post.*
                        ,bkey_match.KEY as MATCH_KEY_BKEY
                        ,lower(bkey_match.table) as MATCH_TABLE_BKEY
                        ,bkey_match.CHV_ID as MATCH_CHV_ID
                        ,bkey_main.KEY as MAIN_KEY_BKEY
                        ,lower(bkey_main.table) as MAIN_TABLE_BKEY
                        ,bkey_main.CHV_ID as MAIN_CHV_ID
                         from temp_post
                left join {catalog}.{data_schema}.chv_table_bkey bkey_match 
                on temp_post.key_match = bkey_match.key 
                and lower(bkey_match.TABLE) = lower(temp_post.matching_table)

                left join {catalog}.{data_schema}.chv_table_bkey bkey_main 
                on temp_post.key_main = bkey_main.key 
                and lower(bkey_main.TABLE) = lower(temp_post.main_table)
                """.replace("%ENV%",env))


                              
not_pass_post = spark.sql(f"""select concat_ws(',',{key_main}) as DATA_KEY from {table} as main
                          
                              LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL PARM 
                              ON LOWER(REPLACE(REPLACE(PARM.PARAM_NAME,'%','|'),'|ENV|','%ENV%')) = '{table.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
                          
                              left join temp_post
                              on concat_ws(',',{key_main}) = temp_post.key_main

                              left join {catalog}.{data_schema}.chv_table_bkey bkey
                              on concat_ws(',',{key_main}) = bkey.key 
                              and lower(bkey.table) = '{table.lower()}' 
                              
                              where temp_post.key_match is null and bkey.key is null
                              and main.data_dt = DATE_FORMAT(DATE_ADD(TO_DATE('{data_dt}','yyyy-MM-dd'),CAST(PARM.PARAM_VAL_NUMBER AS INT)),'yyyy-MM-dd') """.replace("%ENV%",env))

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generate CHV_ID that exists in table bkey
# MAGIC
# MAGIC   

# COMMAND ----------

bkey_exists = df.filter(col("MATCH_CHV_ID").isNotNull() & col("MAIN_CHV_ID").isNull())
bkey_exists_rs = bkey_exists.select(col('KEY_MAIN').alias('KEY'),col('MAIN_TABLE').alias('TABLE'),col('MATCH_CHV_ID').alias('CHV_ID'))
bkey_exists_rs = bkey_exists_rs.dropDuplicates()

# COMMAND ----------

bkey_exists_rs = bkey_exists_rs.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generate  CHV_ID for a key that successfully passes post-validation and does not exists CHV_ID in the BKEY table.

# COMMAND ----------

gen_bkey = df.filter((df["MATCH_CHV_ID"].isNull() & df["MAIN_CHV_ID"].isNull()))

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
# {tableA : {key_main1 : {tableB : [key_match1,key_match2]
#                         tableC : [key_match3,key_match4]
#                        }
#            key_main2 : {tableB : [key_match5,key_match6]
#                         tableC : [key_match7,key_match8]
#               .
#               .
#               .
#            }
#  tableB : {key_main1 : {tableB : [key_match1,key_match2]
#                         tableC : [key_match3,key_match4]
#                        }
#            key_main2 : {tableB : [key_match5,key_match6]
#                         tableC : [key_match7,key_match8]
#               .
#               .
#               .
#            }
#     .
#     .
#     .
# }
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

# this function is a recursive function use to find record that have to generate same bkey 
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
# find all relation of all key
for i in gen_bkey.orderBy("KEY_MAIN", "KEY_MATCH").collect():
  result = find_related_records(table.replace('%ENV%',env), i.KEY_MAIN,bkey_dict)
  for r in result :
    data.append((table.replace('%ENV%',env),r[0],i.KEY_MAIN,r[1]))


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
# {tableA : {PK1 : [bkey1]
#            PK2 : [bkey2]
#            .
#            .
#            .
#            }
#  tableB : {PK3 : [bkey3]
#            PK4 : [bkey4]
#            .
#            .
#            .
#            }
#     .
#     .
#     .
# }
for i in gen_bkey.orderBy("KEY_MAIN", "KEY_MATCH").collect():
  # bkey += 1
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
    # bkey -= 1
  elif i.KEY_MAIN in bkey_dict[i.MAIN_TABLE].keys() and i.KEY_MATCH not in bkey_dict[i.MATCHING_TABLE].keys() :
    bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH] = bkey_dict[i.MAIN_TABLE][i.KEY_MAIN]
    # bkey -= 1
  else :
    if set(bkey_dict[i.MAIN_TABLE][i.KEY_MAIN]) != set(bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH]) :
      print(i.KEY_MAIN,bkey_dict[i.MAIN_TABLE][i.KEY_MAIN],i.KEY_MATCH,bkey_dict[i.MATCHING_TABLE][i.KEY_MATCH])

# COMMAND ----------

TABLE = []
KEY = []
BKEY = []
# use bkey dict to create bkey dataframe
for t in bkey_dict.keys() :
  for key in bkey_dict[t].keys() :
    for b_key in bkey_dict[t][key] :
      TABLE.append(t)
      KEY.append(key)
      BKEY.append(b_key)

bkey_df = pd.DataFrame({'KEY':KEY,'TABLE':TABLE,'CHV_ID':BKEY})

schema = StructType([
    StructField("KEY", StringType(), True),
    StructField("TABLE", StringType(), True),
    StructField("CHV_ID", StringType(), True)
])


gen_bkey_rs = spark.createDataFrame(bkey_df,schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Generate  CHV_ID for a key that not passes post-validation and does not exists CHV_ID in the BKEY table.

# COMMAND ----------

not_pass_post = not_pass_post.withColumn("CHV_ID", lit(None))
window_spec = Window.orderBy("CHV_ID")  # Replace "any_column" with the appropriate column for ordering

# Generate a running number
not_pass_post = not_pass_post.withColumn("running_number", row_number().over(window_spec))
not_pass_post = not_pass_post.withColumn('CHV_ID',col('running_number')+bkey).select(col('DATA_KEY').alias('KEY'),lit(table.replace('%ENV%',env)).alias("TABLE"),col('CHV_ID'))

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
    .withColumn("CHV_ID", col("CHV_ID").cast("int"))
    .withColumn("LD_ID", lit(ld_id).cast(LongType()))
    .withColumn("UPDT_PRCS_NM", lit(updt_prcs_nm))
    .withColumn("UPDT_LD_ID", lit(updt_ld_id).cast(LongType()))
)


# COMMAND ----------

display(final_df)

# COMMAND ----------

final_df.createOrReplaceTempView('myTemptable')

# COMMAND ----------

spark.sql(f"insert overwrite {catalog}.{data_schema}.chv_table_bkey partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from myTemptable".replace('%ENV%',env))

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * 
        FROM {catalog}.{data_schema}.chv_table_bkey
        WHERE DATA_DT = '{data_dt}'
          AND PRCS_NM = '{prcs_nm}'
    """)
)