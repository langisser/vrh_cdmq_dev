# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Create / Recreate dedup output tables
# MAGIC
# MAGIC Creates 5 tables in `viriyah_cdqm_poc.silver` schema.
# MAGIC Run this notebook to recreate tables (DROP + CREATE).
# MAGIC
# MAGIC | Table | Grouping Key |
# MAGIC |---|---|
# MAGIC | `dedup_customer_name` | bkey, id_card, fname, lname, prefix |
# MAGIC | `dedup_province` | bkey, id_card, area, district, postcode, province |
# MAGIC | `dedup_gender` | bkey, id_card, gender, birth_date |
# MAGIC | `dedup_email` | bkey, id_card, email |
# MAGIC | `dedup_phone` | bkey, id_card, phone_no |

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. dedup_customer_name

# COMMAND ----------
# MAGIC %sql
DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.dedup_customer_name;

CREATE TABLE viriyah_cdqm_poc.silver.dedup_customer_name (
  bkey        INT            COMMENT 'Business Key from chv_table_bkey_v2',
  id_card     STRING         COMMENT 'ID card number',
  fname       STRING         COMMENT 'First name',
  lname       STRING         COMMENT 'Last name',
  prefix      STRING         COMMENT 'Name prefix / title (normalized — คุณ replaced by specific prefix if found)',
  update_date TIMESTAMP      COMMENT 'MAX update_date from source records',
  rec_keyvalue ARRAY<STRING> COMMENT 'List of policy_id or id_card keys from source records'
)
USING DELTA
CLUSTER BY (bkey, id_card)
COMMENT 'Deduplicated customer name records grouped by BKEY';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. dedup_province

# COMMAND ----------
# MAGIC %sql
DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.dedup_province;

CREATE TABLE viriyah_cdqm_poc.silver.dedup_province (
  bkey        INT            COMMENT 'Business Key from chv_table_bkey_v2',
  id_card     STRING         COMMENT 'ID card number',
  area        STRING         COMMENT 'Area / sub-district',
  district    STRING         COMMENT 'District',
  postcode    STRING         COMMENT 'Postal code',
  province    STRING         COMMENT 'Province',
  update_date TIMESTAMP      COMMENT 'MAX update_date from source records',
  rec_keyvalue ARRAY<STRING> COMMENT 'List of policy_id or id_card keys from source records'
)
USING DELTA
CLUSTER BY (bkey, id_card)
COMMENT 'Deduplicated province/address records grouped by BKEY';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. dedup_gender

# COMMAND ----------
# MAGIC %sql
DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.dedup_gender;

CREATE TABLE viriyah_cdqm_poc.silver.dedup_gender (
  bkey        INT            COMMENT 'Business Key from chv_table_bkey_v2',
  id_card     STRING         COMMENT 'ID card number',
  gender      STRING         COMMENT 'Gender (normalized — N replaced by F or M if found in same bkey)',
  birth_date  STRING         COMMENT 'Date of birth',
  update_date TIMESTAMP      COMMENT 'MAX update_date from source records',
  rec_keyvalue ARRAY<STRING> COMMENT 'List of policy_id or id_card keys from source records'
)
USING DELTA
CLUSTER BY (bkey, id_card)
COMMENT 'Deduplicated gender/birth_date records grouped by BKEY';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 4. dedup_email

# COMMAND ----------
# MAGIC %sql
DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.dedup_email;

CREATE TABLE viriyah_cdqm_poc.silver.dedup_email (
  bkey        INT            COMMENT 'Business Key from chv_table_bkey_v2',
  id_card     STRING         COMMENT 'ID card number',
  email       STRING         COMMENT 'Email address (NULL rows excluded)',
  update_date TIMESTAMP      COMMENT 'MAX update_date from source records',
  rec_keyvalue ARRAY<STRING> COMMENT 'List of policy_id or id_card keys from source records'
)
USING DELTA
CLUSTER BY (bkey, id_card)
COMMENT 'Deduplicated email records grouped by BKEY (NULL email excluded)';

# COMMAND ----------
# MAGIC %md
# MAGIC ## 5. dedup_phone

# COMMAND ----------
# MAGIC %sql
DROP TABLE IF EXISTS viriyah_cdqm_poc.silver.dedup_phone;

CREATE TABLE viriyah_cdqm_poc.silver.dedup_phone (
  bkey        INT            COMMENT 'Business Key from chv_table_bkey_v2',
  id_card     STRING         COMMENT 'ID card number',
  phone_no    STRING         COMMENT 'Phone number (NULL rows excluded)',
  update_date TIMESTAMP      COMMENT 'MAX update_date from source records',
  rec_keyvalue ARRAY<STRING> COMMENT 'List of policy_id or id_card keys from source records'
)
USING DELTA
CLUSTER BY (bkey, id_card)
COMMENT 'Deduplicated phone records grouped by BKEY (NULL phone_no excluded)';

# COMMAND ----------
# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------
# MAGIC %sql
SHOW TABLES IN viriyah_cdqm_poc.silver LIKE 'dedup_*';
