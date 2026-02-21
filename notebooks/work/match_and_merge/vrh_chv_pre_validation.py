# Databricks notebook source
# MAGIC %md
# MAGIC # PRE-VALIDATION notebook
# MAGIC ### This notebook runs a PRE-VALIDATION and inserts the results into a table
# MAGIC #### PARAMS
# MAGIC - table_nm - checked table
# MAGIC - target_table - Validation result table
# MAGIC - data_dt - date of register
# MAGIC - prcs_nm 
# MAGIC - ld_id 
# MAGIC - updt_prcs_nm
# MAGIC - updt_ld_id
# MAGIC
# MAGIC #### Sample PARAMS: 
# MAGIC viriyah_cdqm_poc.silver.SOURCE_MOTOR^|viriyah_cdqm_poc.silver.CHV_PRE_VALIDATION_RESULT^|2026-01-07^|SOURCE_MOTOR^|2^|SOURCE_MOTOR^|2

# COMMAND ----------

# viriyah_cdqm_poc.silver.SOURCE_MOTOR^|viriyah_cdqm_poc.silver.CHV_PRE_VALIDATION_RESULT^|2026-01-07^|SOURCE_MOTOR^|2^|SOURCE_MOTOR^|2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Variables

# COMMAND ----------

import pandas as pd
import re
from pyspark.sql.types import BooleanType,DateType,LongType,StructType, StructField, StringType
from pyspark.sql.functions import col,concat_ws,coalesce,lit,lower,upper
from functools import reduce
from pyspark.sql import DataFrame,Row

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
print(ENV)
print(env)
print(params)

# COMMAND ----------

table_nm = params[0]
vld_table = params[1].replace('%ENV%',env)
data_dt = params[2]
prcs_nm = params[3]
prcs_id = params[4]
updt_prcs_nm = params[5]
updt_prcs_id = params[6]

# COMMAND ----------

# This function concatenates 'PK' values based on 'ORDER' conditions in a group, using COALESCE. It returns the resulting string
def custom_aggregation(group):
    if any(group['ORDER'].astype(int) > 1):
        return ', '.join(group['PK'])
    else:
        return group['PK'].iloc[0]

# This function regis funtion from string in config function
def convert_function_code(function_code,function_name):
    locals_dict = {}
    exec(function_code, globals(), locals_dict)
    print(locals_dict)
    return locals_dict[f'{function_name}']

# COMMAND ----------

# MAGIC %md
# MAGIC # Get & Prepare config for matching

# COMMAND ----------

config_pk = spark.sql(
    f"select * from {catalog}.{fw_schema}.CHV_CONFIG_PK WHERE ACT_F = '1' ORDER BY ORDER"
    .replace('%ENV%', env)
)
config_validation = spark.sql(f"""
    select config_val.*, config_function.FUNCTION, config_function.PROGRAMMING
    from {catalog}.{fw_schema}.CHV_CONFIG_PRE_VALIDATION config_val
    left join {catalog}.{fw_schema}.CHV_CONFIG_FUNCTION config_function
        on config_val.RULES = config_function.RULES
       and config_function.ACT_F = '1'
    where config_val.ACT_F = '1'
""".replace('%ENV%', env))
config_pk = config_pk.toPandas()
config_pk = config_pk.groupby('TABLE').apply(custom_aggregation).reset_index(name='PK_CONCATENATED')
config_validation = config_validation.toPandas().merge(config_pk,on='TABLE',how='left')

# COMMAND ----------

main = config_validation[config_validation['TABLE'].str.lower() == table_nm.lower()]

# COMMAND ----------

main.display()

# COMMAND ----------

#Get general param
general_param = spark.sql(f"select * from {catalog}.{fw_schema}.CHV_PARAM_GENERAL ".replace("%ENV%",env))

# COMMAND ----------

schema = StructType([
    StructField("TABLE", StringType(), True),
    StructField("KEY", StringType(), True),
    StructField("RULES", StringType(), True),
    StructField("COLUMN", StringType(), True),
    StructField("VALUE", StringType(), True),
    StructField("RESULT", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC # Get Pre-validation Result

# COMMAND ----------

df_list = list()
pk = config_pk[config_pk['TABLE'].str.lower() == table_nm.lower()]
pk = pk['PK_CONCATENATED'].iloc[0]
main_table = table_nm.replace('%ENV%',env).lower()
pk_columns = [column.strip() for column in pk.split(',')]
#check the table does it have pre-validation config. if it does not have config add log
if len(main) != 0:
  parameter = set()
  custom_condition = list()
  for idx,row in main.iterrows():
    parameter = parameter.union(set([i for i in row.PARAMETER.split(',')]))
    #if Pre-validation config have custom condition add case when to check condition
    if row.CUSTOM_CONDITION != None:
      custom_condition.append(row.CUSTOM_CONDITION.replace('WHERE', 'CASE WHEN') + f' THEN TRUE ELSE FALSE END AS CON_{row.PARAMETER.replace(",","_")}')
  parameter = parameter.union(set(custom_condition))
  #Get dataframe from sql query
  sql = f"""SELECT {",".join( [i for i in parameter])}, {pk} \n FROM {main_table} main \n LEFT JOIN {catalog}.{fw_schema}.CHV_PARAM_GENERAL PARM ON LOWER(PARM.PARAM_NAME) = '{table_nm.lower()}' AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE' WHERE main.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('%DATA_DT%','yyyy-MM-dd'),{general_param.filter(lower(col('PARAM_NAME')) == table_nm.lower()).collect()[0].PARAM_VAL_NUMBER}),'yyyy-MM-dd')"""
  sql = sql.replace('%ENV%',env).replace('%DATA_DT%',data_dt)
  df = spark.sql(sql.replace("%ENV%",env))

  for idx,row in main.iterrows():
    #if programming language is 'PYTHON' regis to udf function
    if row.PROGRAMMING == 'PYTHON':
      my_udf = convert_function_code(row.FUNCTION,row.RULES)
      regis_func = udf(my_udf,StringType())

      new_df = spark.createDataFrame([], schema=schema)
      custom_con_df = spark.createDataFrame([], schema=schema)

      #Pre-validation to table using udf function
      if row.CUSTOM_CONDITION != None:
        parameter_column_list = row.PARAMETER.split(',')
        print(row.PARAMETER)
        custom_con_df = df.filter(col(f"CON_{row.PARAMETER.replace(',','_')}") == 'TRUE') \
                    .withColumn('CHECK', eval("regis_func"+"("+",".join([f'col("{i}")'for i in parameter_column_list])+")"))\
                    .withColumn('VALUE',eval("concat_ws(',',"+",".join([f'coalesce(col("{i}").cast("string"),lit("N/A"))' for i in parameter_column_list]) + ')'))\
                    .withColumn('KEY', concat_ws(",", *[coalesce(col(c).cast("string"), lit("")) for c in pk_columns])) \
                    .select(lit(row.TABLE.replace('%ENV%',env)).alias('TABLE'),col('KEY'),lit(row.RULES),lit(row.PARAMETER).alias('COLUMN'),col('VALUE'),col('CHECK').alias('RESULT'))
                    # .withColumn('KEY', concat_ws(",", *[df[column] for column in pk_columns])) \
      else:
        parameter_column_list = row.PARAMETER.split(',')
        new_df = df.withColumn('CHECK', eval("regis_func"+"("+",".join([f'col("{i}")'for i in parameter_column_list])+")"))\
                    .withColumn('VALUE',eval("concat_ws(',',"+",".join([f'coalesce(col("{i}").cast("string"),lit("N/A"))' for i in parameter_column_list]) + ')'))\
                    .withColumn('KEY', concat_ws(",", *[coalesce(col(c).cast("string"), lit("")) for c in pk_columns])) \
                    .select(lit(row.TABLE.replace('%ENV%',env)).alias('TABLE'),col('KEY'),lit(row.RULES),lit(row.PARAMETER).alias('COLUMN'),col('VALUE'),col('CHECK').alias('RESULT'))
               
      df_union = new_df.union(custom_con_df)   
      df_list.append(df_union)


else:
  if len(config_pk[config_pk['TABLE'] == table_nm]) != 0:
    pk = config_pk[config_pk['TABLE'] == table_nm]
    pk = pk['PK_CONCATENATED'].iloc[0]
    log = spark.createDataFrame([Row(TABLE = table_nm.replace('%ENV%',env) ,KEY = pk , RULES = 'No Rules', COLUMN = '-',VALUE = '-999999999',CHECK = '-')])
    df_list.append(log)
  else:
    log = spark.createDataFrame([Row(TABLE = table_nm.replace('%ENV%',env) ,KEY = 'No table' , RULES = 'No Rules', COLUMN = '-',VALUE = '-999999999',CHECK = '-')])
    df_list.append(log)
if len(df_list) !=0 :
  final_df = reduce(DataFrame.unionAll, df_list)
  final_df = final_df.withColumn("LD_ID",lit(prcs_id).cast(LongType()))\
            .withColumn("UPDT_PRCS_NM",lit(updt_prcs_nm))\
            .withColumn("UPDT_LD_ID",lit(updt_prcs_id).cast(LongType()))


# COMMAND ----------

final_df.createOrReplaceTempView('mytempTable')
# display(spark.sql("SELECT * FROM mytempTable"))
vquery = "insert overwrite table "+ f"{vld_table}" + f" partition (DATA_DT = '{data_dt}', PRCS_NM = '{prcs_nm}') select * from mytempTable"
vquery = vquery.replace("%ENV%",env.upper())
print(vquery)
spark.sql(vquery)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT * 
        FROM {vld_table}
        WHERE DATA_DT = '{data_dt}'
          AND PRCS_NM = '{prcs_nm}'
    """)
)