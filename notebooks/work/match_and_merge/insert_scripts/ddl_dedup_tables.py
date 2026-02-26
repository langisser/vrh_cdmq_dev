# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Create 5 dedup output tables for vrh_chv_dedup_v2
# MAGIC
# MAGIC Creates 5 tables in `viriyah_cdqm_poc.silver` schema.
# MAGIC These tables store deduplicated records grouped by BKEY from `chv_table_bkey_v2`.
# MAGIC
# MAGIC | Table | Description |
# MAGIC |---|---|
# MAGIC | `dedup_customer_name` | Unique (bkey, id_card, fname, lname, prefix) combinations |
# MAGIC | `dedup_province` | Unique (bkey, id_card, area, district, postcode, province) combinations |
# MAGIC | `dedup_gender` | Unique (bkey, id_card, gender, birth_date) combinations |
# MAGIC | `dedup_email` | Unique (bkey, id_card, email) combinations |
# MAGIC | `dedup_phone` | Unique (bkey, id_card, phone) combinations |
# MAGIC
# MAGIC Run this notebook once before running `vrh_chv_dedup_v2` for the first time.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

CATALOG = "viriyah_cdqm_poc"
SILVER  = "silver"

print(f"Dedup tables : {CATALOG}.{SILVER}.dedup_*")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 1. dedup_customer_name

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.dedup_customer_name (
# MAGIC   bkey        INT           COMMENT 'Business Key from chv_table_bkey_v2',
# MAGIC   id_card     STRING        COMMENT 'ID card number',
# MAGIC   fname       STRING        COMMENT 'First name',
# MAGIC   lname       STRING        COMMENT 'Last name',
# MAGIC   prefix      STRING        COMMENT 'Name prefix / title',
# MAGIC   update_date TIMESTAMP     COMMENT 'MAX update_date from source records',
# MAGIC   policy_keys ARRAY<STRING> COMMENT 'List of policy_no or id_card keys from source records'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Deduplicated customer name records grouped by BKEY';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dedup_province

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.dedup_province (
# MAGIC   bkey        INT           COMMENT 'Business Key from chv_table_bkey_v2',
# MAGIC   id_card     STRING        COMMENT 'ID card number',
# MAGIC   area        STRING        COMMENT 'Area / sub-district',
# MAGIC   district    STRING        COMMENT 'District',
# MAGIC   postcode    STRING        COMMENT 'Postal code',
# MAGIC   province    STRING        COMMENT 'Province',
# MAGIC   update_date TIMESTAMP     COMMENT 'MAX update_date from source records',
# MAGIC   policy_keys ARRAY<STRING> COMMENT 'List of policy_no or id_card keys from source records'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Deduplicated province/address records grouped by BKEY';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dedup_gender

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.dedup_gender (
# MAGIC   bkey        INT           COMMENT 'Business Key from chv_table_bkey_v2',
# MAGIC   id_card     STRING        COMMENT 'ID card number',
# MAGIC   gender      STRING        COMMENT 'Gender',
# MAGIC   birth_date  STRING        COMMENT 'Date of birth',
# MAGIC   update_date TIMESTAMP     COMMENT 'MAX update_date from source records',
# MAGIC   policy_keys ARRAY<STRING> COMMENT 'List of policy_no or id_card keys from source records'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Deduplicated gender/birth_date records grouped by BKEY';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dedup_email

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.dedup_email (
# MAGIC   bkey        INT           COMMENT 'Business Key from chv_table_bkey_v2',
# MAGIC   id_card     STRING        COMMENT 'ID card number',
# MAGIC   email       STRING        COMMENT 'Email address (NULL rows are included)',
# MAGIC   update_date TIMESTAMP     COMMENT 'MAX update_date from source records',
# MAGIC   policy_keys ARRAY<STRING> COMMENT 'List of policy_no or id_card keys from source records'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Deduplicated email records grouped by BKEY (NULL email rows included)';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dedup_phone

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.dedup_phone (
# MAGIC   bkey        INT           COMMENT 'Business Key from chv_table_bkey_v2',
# MAGIC   id_card     STRING        COMMENT 'ID card number',
# MAGIC   phone       STRING        COMMENT 'Phone number (NULL rows are included)',
# MAGIC   update_date TIMESTAMP     COMMENT 'MAX update_date from source records',
# MAGIC   policy_keys ARRAY<STRING> COMMENT 'List of policy_no or id_card keys from source records'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Deduplicated phone records grouped by BKEY (NULL phone rows included)';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN viriyah_cdqm_poc.silver LIKE 'dedup_*';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Schema | Grouping Key |
# MAGIC |---|---|---|
# MAGIC | `dedup_customer_name` | silver | bkey, id_card, fname, lname, prefix |
# MAGIC | `dedup_province` | silver | bkey, id_card, area, district, postcode, province |
# MAGIC | `dedup_gender` | silver | bkey, id_card, gender, birth_date |
# MAGIC | `dedup_email` | silver | bkey, id_card, email |
# MAGIC | `dedup_phone` | silver | bkey, id_card, phone |
