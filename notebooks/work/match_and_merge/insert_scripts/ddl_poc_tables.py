# Databricks notebook source
# MAGIC %md
# MAGIC # DDL — POC Tables for TIER + SUBJECT Design
# MAGIC
# MAGIC ## Strategy
# MAGIC | Table | Action | เหตุผล |
# MAGIC |---|---|---|
# MAGIC | chv_config_matching | **CREATE NEW** | เพิ่ม TIER, SUBJECT columns |
# MAGIC | chv_table_bkey | **CREATE NEW** | เพิ่ม SUBJECT column |
# MAGIC | chv_matching_result | **CREATE NEW** | เพิ่ม SUBJECT column |
# MAGIC | chv_matching_log | **CREATE NEW** | เพิ่ม SUBJECT column |
# MAGIC | chv_config_pk | COPY AS-IS | ไม่เปลี่ยน schema |
# MAGIC | chv_config_pre_validation | COPY AS-IS | ไม่เปลี่ยน schema |
# MAGIC | chv_config_check_pre_validation | COPY AS-IS | ไม่เปลี่ยน schema |
# MAGIC | chv_config_function | COPY AS-IS | ไม่เปลี่ยน schema |
# MAGIC | chv_param_general | COPY AS-IS | ไม่เปลี่ยน schema |
# MAGIC | chv_pre_validation_result | COPY AS-IS | ไม่เปลี่ยน schema |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — กำหนด catalog / schema ของชุด POC

# COMMAND ----------

# MAGIC %python
# MAGIC # ปรับตรงนี้ก่อน run
# MAGIC SRC_CATALOG  = "viriyah_cdqm_poc"
# MAGIC SRC_CTRL     = "control_fw"
# MAGIC SRC_SILVER   = "silver"
# MAGIC
# MAGIC POC_CATALOG  = "viriyah_cdqm_poc"   # เปลี่ยนถ้าต้องการ catalog ใหม่
# MAGIC POC_CTRL     = "control_fw_v2"       # schema ใหม่สำหรับ control tables
# MAGIC POC_SILVER   = "silver_v2"           # schema ใหม่สำหรับ result tables
# MAGIC
# MAGIC print(f"Source  : {SRC_CATALOG}.{SRC_CTRL} / {SRC_CATALOG}.{SRC_SILVER}")
# MAGIC print(f"POC     : {POC_CATALOG}.{POC_CTRL} / {POC_CATALOG}.{POC_SILVER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — สร้าง Schema ใหม่ (ถ้ายังไม่มี)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL};
# MAGIC CREATE SCHEMA IF NOT EXISTS ${POC_CATALOG}.${POC_SILVER};

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART A — Tables ที่ COPY AS-IS (schema ไม่เปลี่ยน)

# COMMAND ----------

# MAGIC %md
# MAGIC ## A1. chv_config_pk

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_config_pk (
# MAGIC   TABLE   STRING  COMMENT 'Full table name (catalog.schema.table)',
# MAGIC   PK      STRING  COMMENT 'Primary key column name',
# MAGIC   ORDER   INT     COMMENT 'Sequence for composite key concatenation',
# MAGIC   ACT_F   INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Primary key configuration per table';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Copy data from original
# MAGIC INSERT INTO ${POC_CATALOG}.${POC_CTRL}.chv_config_pk
# MAGIC SELECT * FROM ${SRC_CATALOG}.${SRC_CTRL}.chv_config_pk;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A2. chv_config_function

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_config_function (
# MAGIC   RULES       STRING  COMMENT 'Rule name (e.g. CHECK_NULL)',
# MAGIC   FUNCTION    STRING  COMMENT 'Function source code as string',
# MAGIC   PROGRAMMING STRING  COMMENT 'Programming language (PYTHON / SQL)',
# MAGIC   ACT_F       INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Validation function definitions';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${POC_CATALOG}.${POC_CTRL}.chv_config_function
# MAGIC SELECT * FROM ${SRC_CATALOG}.${SRC_CTRL}.chv_config_function;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A3. chv_config_pre_validation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_config_pre_validation (
# MAGIC   TABLE            STRING  COMMENT 'Full table name',
# MAGIC   RULES            STRING  COMMENT 'Validation rule name (ref chv_config_function)',
# MAGIC   PARAMETER        STRING  COMMENT 'Column(s) to validate (comma-separated)',
# MAGIC   CUSTOM_CONDITION STRING  COMMENT 'Optional WHERE condition override',
# MAGIC   ACT_F            INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Pre-validation rules per table';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${POC_CATALOG}.${POC_CTRL}.chv_config_pre_validation
# MAGIC SELECT * FROM ${SRC_CATALOG}.${SRC_CTRL}.chv_config_pre_validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A4. chv_config_check_pre_validation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_config_check_pre_validation (
# MAGIC   MATCHING_RULES  INT     COMMENT 'Ref to chv_config_matching.MATCHING_RULES',
# MAGIC   TABLE           STRING  COMMENT 'MAIN or MATCH side',
# MAGIC   RULES_CHECK     STRING  COMMENT 'Validation rule to apply',
# MAGIC   COLUMN          STRING  COMMENT 'Column to validate',
# MAGIC   ACT_F           INT     COMMENT '1=active, 0=inactive'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Per-matching-rule pre-validation checks';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${POC_CATALOG}.${POC_CTRL}.chv_config_check_pre_validation
# MAGIC SELECT * FROM ${SRC_CATALOG}.${SRC_CTRL}.chv_config_check_pre_validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A5. chv_param_general

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_param_general (
# MAGIC   PARAM_GROUP_NAME  STRING  COMMENT 'Parameter group (e.g. DATE)',
# MAGIC   PARAM_NAME        STRING  COMMENT 'Full table name as param key',
# MAGIC   PARAM_DESC        STRING  COMMENT 'Description',
# MAGIC   PARAM_VAL_NUMBER  INT     COMMENT 'Numeric value (e.g. date lag days)',
# MAGIC   PARAM_VAL_DATE    DATE    COMMENT 'Date value',
# MAGIC   PARAM_VAL_VARCHAR STRING  COMMENT 'String value'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'General parameters per table (date lag, etc.)';

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${POC_CATALOG}.${POC_CTRL}.chv_param_general
# MAGIC SELECT * FROM ${SRC_CATALOG}.${SRC_CTRL}.chv_param_general;

# COMMAND ----------

# MAGIC %md
# MAGIC ## A6. chv_pre_validation_result

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_pre_validation_result (
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
# MAGIC COMMENT 'Pre-validation results per run';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART B — Tables ที่ CREATE NEW (schema เปลี่ยน)
# MAGIC > ⚠️ ไม่ copy data จาก original เพราะ schema ต่างกัน

# COMMAND ----------

# MAGIC %md
# MAGIC ## B1. chv_config_matching ★ เพิ่ม TIER + SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_config_matching (
# MAGIC   MATCHING_RULES  INT     COMMENT 'Rule sequence ID (unique per rule row)',
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Full name of the main (source) table',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Full name of the table to match against',
# MAGIC   MATCH_CONDITION STRING  COMMENT 'Join condition (use MAIN. and MATCH. prefix)',
# MAGIC   `GROUP`         INT     COMMENT 'Weight group ID for threshold calculation',
# MAGIC   WEIGHT          DOUBLE  COMMENT 'Score weight for this rule (sum >= 1 = match)',
# MAGIC   ACT_F           INT     COMMENT '1=active, 0=inactive',
# MAGIC   TIER            INT     DEFAULT 1 COMMENT 'Matching tier order (1=first, 2=next). Records matched in lower tier are excluded from higher tier',
# MAGIC   SUBJECT         STRING  DEFAULT 'default' COMMENT 'Business subject of this BKEY group (e.g. identity, address)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Matching rules config — v2 with TIER and SUBJECT support';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B2. chv_matching_log ★ เพิ่ม SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_matching_log (
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Main table name',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Matching table name',
# MAGIC   MATCHING_RULES  STRING  COMMENT 'Rule ID that produced this row',
# MAGIC   KEY_MAIN        STRING  COMMENT 'Concatenated PK from main table',
# MAGIC   KEY_MATCH       STRING  COMMENT 'Concatenated PK from match table (NULL if no match)',
# MAGIC   RESULT          STRING  COMMENT 'PASSED or FAILED',
# MAGIC   LD_ID           BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM    STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID      BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT         STRING  DEFAULT 'default' COMMENT 'Business subject (from chv_config_matching.SUBJECT)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Raw matching log per rule per run — v2 with SUBJECT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B3. chv_matching_result ★ เพิ่ม SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_CTRL}.chv_matching_result (
# MAGIC   MAIN_TABLE      STRING  COMMENT 'Main table name',
# MAGIC   MATCHING_TABLE  STRING  COMMENT 'Matching table name',
# MAGIC   KEY_MAIN        STRING  COMMENT 'Concatenated PK from main table',
# MAGIC   KEY_MATCH       STRING  COMMENT 'Concatenated PK from match table',
# MAGIC   LD_ID           BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM    STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID      BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT         STRING  DEFAULT 'default' COMMENT 'Business subject (from chv_config_matching.SUBJECT)'
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATA_DT STRING, PRCS_NM STRING)
# MAGIC COMMENT 'Final matched pairs that passed weight threshold — v2 with SUBJECT';

# COMMAND ----------

# MAGIC %md
# MAGIC ## B4. chv_table_bkey ★ เพิ่ม SUBJECT

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ${POC_CATALOG}.${POC_SILVER}.chv_table_bkey (
# MAGIC   KEY           STRING  COMMENT 'Concatenated PK value of the record',
# MAGIC   TABLE         STRING  COMMENT 'Full table name the KEY belongs to',
# MAGIC   BKEY          INT     COMMENT 'Business Key — unique per group per subject',
# MAGIC   LD_ID         BIGINT  COMMENT 'Load ID',
# MAGIC   UPDT_PRCS_NM  STRING  COMMENT 'Update process name',
# MAGIC   UPDT_LD_ID    BIGINT  COMMENT 'Update load ID',
# MAGIC   SUBJECT       STRING  DEFAULT 'default' COMMENT 'Business subject this BKEY belongs to (e.g. identity, address)'
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
# MAGIC -- ตรวจสอบ tables ที่สร้างใน POC schema
# MAGIC SHOW TABLES IN ${POC_CATALOG}.${POC_CTRL};

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN ${POC_CATALOG}.${POC_SILVER};

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Table | Schema | Action | New Columns |
# MAGIC |---|---|---|---|
# MAGIC | `chv_config_pk` | control_fw_v2 | Copied | — |
# MAGIC | `chv_config_function` | control_fw_v2 | Copied | — |
# MAGIC | `chv_config_pre_validation` | control_fw_v2 | Copied | — |
# MAGIC | `chv_config_check_pre_validation` | control_fw_v2 | Copied | — |
# MAGIC | `chv_param_general` | control_fw_v2 | Copied | — |
# MAGIC | `chv_pre_validation_result` | control_fw_v2 | Created | — |
# MAGIC | `chv_config_matching` | control_fw_v2 | **Created New** | `TIER`, `SUBJECT` |
# MAGIC | `chv_matching_log` | control_fw_v2 | **Created New** | `SUBJECT` |
# MAGIC | `chv_matching_result` | control_fw_v2 | **Created New** | `SUBJECT` |
# MAGIC | `chv_table_bkey` | silver_v2 | **Created New** | `SUBJECT` |
