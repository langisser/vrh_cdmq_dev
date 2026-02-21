# Databricks notebook source
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("data_date", "", "Data Date")

table_name = dbutils.widgets.get("table_name")
data_date = dbutils.widgets.get("data_date")
environment = 'dev'
catalog = 'viriyah_cdqm_poc'
fw_schema = 'control_fw'
data_schema = 'silver'
full_table_name = f"{catalog}.{data_schema}.{table_name}"

# v2 notebook paths
pre_vld_path = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge/vrh_chv_pre_validation_v2'
match_path = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge/vrh_chv_match_v2'

print(table_name)
print(full_table_name)
print(data_date)

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat, concat_ws, split, upper, lower

# COMMAND ----------

# Read pre-validation table list from chv_config_matching_v2
query = f"""
SELECT
  '{full_table_name.lower()}' AS PRE_VLD_TABLE

UNION

SELECT DISTINCT
  LOWER(MATCHING_TABLE) AS PRE_VLD_TABLE
FROM {catalog}.{fw_schema}.chv_config_matching_v2
WHERE
  LOWER(MAIN_TABLE) = '{full_table_name.lower()}'
"""

pre_vld_tbl_list = spark.sql(query)

result_df = pre_vld_tbl_list.withColumn(
    "pre_vld_param",
    concat_ws(
        "^|",
        lower(col("PRE_VLD_TABLE")),
        lit(f"{catalog}.{fw_schema}.chv_pre_validation_result_v2"),
        lit(data_date),
        concat(lit("EDP_PRE_VLD_V2_"), upper(split(col("PRE_VLD_TABLE"), "\\.")[2])),
        lit("1"),
        concat(lit("EDP_PRE_VLD_V2_"), upper(split(col("PRE_VLD_TABLE"), "\\.")[2])),
        lit("1")
    )
)

pre_vld_param_list = result_df.select("pre_vld_param").collect()

match_param = "^|".join([
    full_table_name,
    data_date,
    f"EDP_MATCHING_V2_{table_name.upper()}_DATE_{data_date}",
    "1",
    f"EDP_MATCHING_V2_{table_name.upper()}_DATE_{data_date}",
    '1'
  ])


# COMMAND ----------

for pre_vld_param in pre_vld_param_list:
    pre_parameters = {
            "ENV": environment,
            "PARAMS": pre_vld_param[0]
        }

    dbutils.notebook.run(
            pre_vld_path,
            timeout_seconds=600,
            arguments=pre_parameters
        )

match_parameters = {
        "ENV": environment,
        "PARAMS": match_param
    }

dbutils.notebook.run(
        match_path,
        timeout_seconds=1200,
        arguments=match_parameters
    )
