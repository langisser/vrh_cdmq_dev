# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — Create all _v2 tables for TIER + SUBJECT POC
# MAGIC
# MAGIC Creates 10 `_v2` suffix tables in the same catalog/schema as originals.
# MAGIC No data copy here — use `chv_config_copy_to_v2.py` for config data,
# MAGIC and `chv_config_matching_v2.py` for the matching rules with TIER + SUBJECT.
# MAGIC
# MAGIC | Table | Schema | Change from original |
# MAGIC |---|---|---|
# MAGIC | `chv_config_pk_v2` | control_fw | same schema |
# MAGIC | `chv_config_function_v2` | control_fw | same schema |
# MAGIC | `chv_config_pre_validation_v2` | control_fw | same schema |
# MAGIC | `chv_config_check_pre_validation_v2` | control_fw | same schema |
# MAGIC | `chv_param_general_v2` | control_fw | same schema |
# MAGIC | `chv_pre_validation_result_v2` | control_fw | same schema + partition |
# MAGIC | `chv_config_matching_v2` | control_fw | **+ TIER INT, SUBJECT STRING** |
# MAGIC | `chv_matching_log_v2` | control_fw | **+ SUBJECT STRING** |
# MAGIC | `chv_matching_result_v2` | control_fw | **+ SUBJECT STRING** |
# MAGIC | `chv_table_bkey_v2` | silver | **+ SUBJECT STRING** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# Catalog and schema — same as originals, only table names get _v2 suffix
CATALOG   = "viriyah_cdqm_poc"
FW_SCHEMA = "control_fw"
SILVER    = "silver"

print(f"Control tables : {CATALOG}.{FW_SCHEMA}.*_v2")
print(f"Silver tables  : {CATALOG}.{SILVER}.*_v2")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART A — Config tables (same schema as originals)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1. chv_config_pk_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_config_pk_v2 (
# MAGIC   TABLE   STRING  COMMENT 'Full table name (catalog.schema.table)',
# MAGIC   PK      STRING  COMMENT 'Primary key column name',
# MAGIC   ORDER   INT     COMMENT 'Sequence for composite key concatenation',
# MAGIC   ACT_F   INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Primary key configuration per table — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2. chv_config_function_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_config_function_v2 (
# MAGIC   RULES       STRING  COMMENT 'Rule name (e.g. CHECK_NULL)',
# MAGIC   FUNCTION    STRING  COMMENT 'Function source code as string',
# MAGIC   PROGRAMMING STRING  COMMENT 'Programming language (PYTHON / SQL)',
# MAGIC   ACT_F       INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Validation function definitions — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## A3. chv_config_pre_validation_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2 (
# MAGIC   TABLE            STRING  COMMENT 'Full table name',
# MAGIC   RULES            STRING  COMMENT 'Validation rule name (ref chv_config_function)',
# MAGIC   PARAMETER        STRING  COMMENT 'Column(s) to validate (comma-separated)',
# MAGIC   CUSTOM_CONDITION STRING  COMMENT 'Optional WHERE condition override',
# MAGIC   ACT_F            INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pre-validation rules per table — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## A4. chv_config_check_pre_validation_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2 (
# MAGIC   MATCHING_RULES  INT     COMMENT 'Ref to chv_config_matching.MATCHING_RULES',
# MAGIC   TABLE           STRING  COMMENT 'MAIN or MATCH side',
# MAGIC   RULES_CHECK     STRING  COMMENT 'Validation rule to apply',
# MAGIC   COLUMN          STRING  COMMENT 'Column to validate',
# MAGIC   ACT_F           INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Per-matching-rule pre-validation checks — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## A5. chv_param_general_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_param_general_v2 (
# MAGIC   PARAM_GROUP_NAME  STRING  COMMENT 'Parameter group (e.g. DATE)',
# MAGIC   PARAM_NAME        STRING  COMMENT 'Full table name as param key',
# MAGIC   PARAM_DESC        STRING  COMMENT 'Description',
# MAGIC   PARAM_VAL_NUMBER  INT     COMMENT 'Numeric value (e.g. date lag days)',
# MAGIC   PARAM_VAL_DATE    DATE    COMMENT 'Date value',
# MAGIC   PARAM_VAL_VARCHAR STRING  COMMENT 'String value'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'General parameters per table (date lag, etc.) — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ## A6. chv_pre_validation_result_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2 (
# MAGIC   TABLE         STRING  COMMENT 'Full table name',
# MAGIC   KEY           STRING  COMMENT 'Concatenated primary key value',
# MAGIC   RULES         STRING  COMMENT 'Validation rule applied',
# MAGIC   COLUMN        STRING  COMMENT 'Column validated',
# MAGIC   VALUE         STRING  COMMENT 'Actual value at validation time',
# MAGIC   RESULT        STRING  COMMENT 'PASSED or FAILED',
# MAGIC   LD_ID         BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM  STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID    BIGINT  COMMENT 'Update load ID'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Pre-validation results per run — v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART B — Tables with new columns (TIER / SUBJECT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1. chv_config_matching_v2  ★ + TIER + SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_config_matching_v2 (
# MAGIC   MATCHING_RULES  INT     COMMENT 'Rule sequence ID (unique per rule row)',
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Full name of the main (source) table',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Full name of the table to match against',
# MAGIC   MATCH_CONDITION STRING  COMMENT 'Join condition (use MAIN. and MATCH. prefix)',
# MAGIC   `GROUP`         INT     COMMENT 'Weight group ID for threshold calculation',
# MAGIC   WEIGHT          DOUBLE  COMMENT 'Score weight for this rule (sum >= 1 = match)',
# MAGIC   ACT_F           INT     COMMENT '1=active, 0=inactive',
# MAGIC   TIER            INT     COMMENT 'Cascade priority (1=first, 2=second). Records matched in TIER 1 are excluded from TIER 2.',
# MAGIC   SUBJECT         STRING  COMMENT 'Business subject of this BKEY group (e.g. identity, address)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Matching rules config — v2 with TIER and SUBJECT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B2. chv_matching_log_v2  ★ + SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_matching_log_v2 (
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Main table name',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Matching table name',
# MAGIC   MATCHING_RULES  STRING  COMMENT 'Rule ID that produced this row',
# MAGIC   KEY_MAIN        STRING  COMMENT 'Concatenated PK from main table',
# MAGIC   KEY_MATCH       STRING  COMMENT 'Concatenated PK from match table (NULL if no match)',
# MAGIC   RESULT          STRING  COMMENT 'PASSED or FAILED',
# MAGIC   LD_ID           BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM    STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID      BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT         STRING  COMMENT 'Business subject (from chv_config_matching_v2.SUBJECT)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Raw matching log per rule per run — v2 with SUBJECT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B3. chv_matching_result_v2  ★ + SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.control_fw.chv_matching_result_v2 (
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Main table name',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Matching table name',
# MAGIC   KEY_MAIN        STRING  COMMENT 'Concatenated PK from main table',
# MAGIC   KEY_MATCH       STRING  COMMENT 'Concatenated PK from match table',
# MAGIC   LD_ID           BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM    STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID      BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT         STRING  COMMENT 'Business subject (from chv_config_matching_v2.SUBJECT)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Final matched pairs that passed weight threshold — v2 with SUBJECT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B4. chv_table_bkey_v2  ★ + SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS viriyah_cdqm_poc.silver.chv_table_bkey_v2 (
# MAGIC   KEY           STRING  COMMENT 'Concatenated PK value of the record',
# MAGIC   TABLE         STRING  COMMENT 'Full table name the KEY belongs to',
# MAGIC   BKEY          INT     COMMENT 'Business Key — unique per group per subject',
# MAGIC   LD_ID         BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM  STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID    BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT       STRING  COMMENT 'Business subject this BKEY belongs to (e.g. identity, address)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Business Key table — v2 with SUBJECT support';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART C — Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN viriyah_cdqm_poc.control_fw LIKE '*_v2';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN viriyah_cdqm_poc.silver LIKE '*_v2';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Schema | New Columns |
# MAGIC |---|---|---|
# MAGIC | `chv_config_pk_v2` | control_fw | — |
# MAGIC | `chv_config_function_v2` | control_fw | — |
# MAGIC | `chv_config_pre_validation_v2` | control_fw | — |
# MAGIC | `chv_config_check_pre_validation_v2` | control_fw | — |
# MAGIC | `chv_param_general_v2` | control_fw | — |
# MAGIC | `chv_pre_validation_result_v2` | control_fw | — |
# MAGIC | `chv_config_matching_v2` | control_fw | `TIER INT`, `SUBJECT STRING` |
# MAGIC | `chv_matching_log_v2` | control_fw | `SUBJECT STRING` |
# MAGIC | `chv_matching_result_v2` | control_fw | `SUBJECT STRING` |
# MAGIC | `chv_table_bkey_v2` | silver | `SUBJECT STRING` |
