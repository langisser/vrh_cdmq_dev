# Databricks notebook source
# MAGIC %md
# MAGIC # Test Data Preparation - Case 1
# MAGIC
# MAGIC Prepares test data for unit test case 1 (ID: 5101299025871)
# MAGIC
# MAGIC ## Tables Created
# MAGIC ### Bronze (`viriyah_cdqm_poc.bronze`)
# MAGIC - `tc_case1_5101299025871` — source table (9 rows)
# MAGIC - `ts_case1_5101299025871` — trust source table (1 row)
# MAGIC
# MAGIC ### Silver (`viriyah_cdqm_poc.silver`)
# MAGIC - `tc_case1_5101299025871` — source table with DATA_DT = 2015-12-28
# MAGIC - `ts_case1_5101299025871` — trust source table with DATA_DT = 2015-12-28

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import Row
from pyspark.sql.functions import lit, to_date

catalog      = 'viriyah_cdqm_poc'
schema       = 'bronze'
silver       = 'silver'
DATA_DT      = '2015-12-28'

# COMMAND ----------

# DBTITLE 1,Source Table - tc_case1_5101299025871

source_schema = StructType([
    StructField("id_card",      StringType(), True),
    StructField("fname",        StringType(), True),
    StructField("lname",        StringType(), True),
    StructField("gender",       StringType(), True),
    StructField("table",        StringType(), True),
    StructField("insert_date",  StringType(), True),
    StructField("update_date",  StringType(), True),
    StructField("birth_date",   StringType(), True),
    StructField("prefix",       StringType(), True),
    StructField("area",         StringType(), True),
    StructField("district",     StringType(), True),
    StructField("postcode",     StringType(), True),
    StructField("province",     StringType(), True),
    StructField("email",        StringType(), True),
    StructField("phone",        StringType(), True),
])

source_data = [
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="maspol",            insert_date="9/18/2024 15:08",   update_date="1/17/2025 16:01",  birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="maspol",            insert_date="11/6/2025 15:10",   update_date="1/16/2026 16:48",  birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="maspol",            insert_date="1/22/2025 8:40",    update_date="4/25/2025 11:50",  birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="N", table="applicants",        insert_date="1/20/2025 15:03",   update_date="3/18/2025 13:52",  birth_date="11/29/1969", prefix="คุณ", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="N", table="update-applicants", insert_date="5/9/2024 14:43",    update_date="5/9/2024 14:43",   birth_date=None,         prefix="คุณ", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="applicants",        insert_date="10/30/2025 15:06",  update_date="11/12/2025 12:59", birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="maspol",            insert_date="1/13/2024 13:08",   update_date="4/25/2024 13:26",  birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาภูศิริ", gender="N", table="update_maspol",     insert_date="12/7/2024 15:58",   update_date="12/9/2024 20:49",  birth_date=None,         prefix="คุณ", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ",  gender="M", table="applicants",        insert_date="9/6/2024 14:33",    update_date="12/17/2024 10:39", birth_date=None,         prefix="นาย", area="แสมดำ", district="บางขุนเทียน", postcode="10150", province="กรุงเทพมหานคร", email=None, phone=None),
]

tc_df = spark.createDataFrame(source_data, schema=source_schema)
tc_df.createOrReplaceTempView("tc_case1_5101299025871")

# COMMAND ----------

# DBTITLE 1,Write Source Table to Bronze
tc_table = f"{catalog}.{schema}.tc_case1_5101299025871"

tc_df.write.format("delta").mode("overwrite").saveAsTable(tc_table)
print(f"Created: {tc_table} ({tc_df.count()} rows)")
display(spark.table(tc_table))

# COMMAND ----------

# DBTITLE 1,Write Source Table to Silver (with DATA_DT)
tc_silver_table = f"{catalog}.{silver}.tc_case1_5101299025871"

tc_silver_df = tc_df.withColumn("DATA_DT", to_date(lit(DATA_DT)))
tc_silver_df.write.format("delta").mode("overwrite") \
    .partitionBy("DATA_DT") \
    .saveAsTable(tc_silver_table)
print(f"Created: {tc_silver_table} ({tc_silver_df.count()} rows, DATA_DT={DATA_DT})")
display(spark.table(tc_silver_table))

# COMMAND ----------

# DBTITLE 1,Trust Source Table - ts_case1_5101299025871

trust_schema = StructType([
    StructField("id_card", StringType(), True),
    StructField("fname",   StringType(), True),
    StructField("lname",   StringType(), True),
    StructField("gender",  StringType(), True),
    StructField("table",   StringType(), True),
])

trust_data = [
    Row(id_card="5101299025871", fname="ศุภชัย", lname="ประทีปนาฏศิริ", gender=None, table="trust_source"),
]

ts_df = spark.createDataFrame(trust_data, schema=trust_schema)
ts_df.createOrReplaceTempView("ts_case1_5101299025871")

# COMMAND ----------

# DBTITLE 1,Write Trust Source Table to Bronze
ts_table = f"{catalog}.{schema}.ts_case1_5101299025871"

ts_df.write.format("delta").mode("overwrite").saveAsTable(ts_table)
print(f"Created: {ts_table} ({ts_df.count()} rows)")
display(spark.table(ts_table))

# COMMAND ----------

# DBTITLE 1,Write Trust Source Table to Silver (with DATA_DT)
ts_silver_table = f"{catalog}.{silver}.ts_case1_5101299025871"

ts_silver_df = ts_df.withColumn("DATA_DT", to_date(lit(DATA_DT)))
ts_silver_df.write.format("delta").mode("overwrite") \
    .partitionBy("DATA_DT") \
    .saveAsTable(ts_silver_table)
print(f"Created: {ts_silver_table} ({ts_silver_df.count()} rows, DATA_DT={DATA_DT})")
display(spark.table(ts_silver_table))
