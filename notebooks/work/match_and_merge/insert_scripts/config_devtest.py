# Databricks notebook source
# MAGIC %md
# MAGIC # Config — PK + Matching Rules for devtest tables
# MAGIC
# MAGIC Inserts config entries for `source_motor_devtest` and `trust_source_devtest`.
# MAGIC
# MAGIC ## PK Config
# MAGIC | Table | PK | Notes |
# MAGIC |---|---|---|
# MAGIC | `source_motor_devtest` | `policy_id` | Single key (not composite like production) |
# MAGIC | `trust_source_devtest` | `id_card` | Single key |
# MAGIC
# MAGIC ## Matching Rules (31–39)
# MAGIC Rule IDs 31–39 avoid collision with existing rules 1–21.
# MAGIC
# MAGIC | Rules | Direction | TIER | SUBJECT | Reason |
# MAGIC |---|---|---|---|---|
# MAGIC | 31–33 | source_motor_devtest → trust_source_devtest | 1 | identity | Cross-source, runs first |
# MAGIC | 34–36 | source_motor_devtest → source_motor_devtest | 2 | identity | Self-match, only records unmatched in Tier 1 |
# MAGIC | 37–39 | trust_source_devtest → source_motor_devtest | 1 | identity | Cross-source (trust as MAIN), runs first |
# MAGIC
# MAGIC **Weights:** id_card exact match = 0.5, fname levenshtein = 0.25, lname levenshtein = 0.25
# MAGIC Total weight = 1.0 → full match

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Clean existing devtest config (idempotent re-run)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove existing devtest entries so this script is safe to re-run
# MAGIC DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_pk_v2
# MAGIC WHERE lower(`TABLE`) LIKE '%devtest%';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
# MAGIC WHERE MATCHING_RULES BETWEEN 31 AND 39;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL_V2
# MAGIC WHERE lower(PARAM_NAME) LIKE '%devtest%';

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2
# MAGIC WHERE MATCHING_RULES BETWEEN 31 AND 39;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Insert PK Config

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_pk_v2 (TABLE, PK, ORDER, ACT_F) VALUES
# MAGIC   ('viriyah_cdqm_poc.silver.source_motor_devtest', 'policy_id', 1, 1),
# MAGIC   ('viriyah_cdqm_poc.silver.trust_source_devtest', 'id_card',   1, 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Insert Matching Config

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_matching_v2
# MAGIC   (MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, MATCH_CONDITION, `GROUP`, WEIGHT, ACT_F, TIER, SUBJECT)
# MAGIC VALUES
# MAGIC   -- TIER 1 (Dir A): source_motor_devtest → trust_source_devtest (cross-source, runs first)
# MAGIC   (31, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.trust_source_devtest', 'MAIN.id_card = MATCH.id_card',                1, 0.5,  1, 1, 'identity'),
# MAGIC   (32, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.trust_source_devtest', 'levenshtein(MAIN.fname, MATCH.fname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 1, 'identity'),
# MAGIC   (33, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.trust_source_devtest', 'levenshtein(MAIN.lname, MATCH.lname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 1, 'identity'),
# MAGIC   -- TIER 2 (Dir B): source_motor_devtest → source_motor_devtest (self-match, only records unmatched in Tier 1)
# MAGIC   (34, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'MAIN.id_card = MATCH.id_card',                1, 0.5,  1, 2, 'identity'),
# MAGIC   (35, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'levenshtein(MAIN.fname, MATCH.fname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 2, 'identity'),
# MAGIC   (36, 'viriyah_cdqm_poc.silver.source_motor_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'levenshtein(MAIN.lname, MATCH.lname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 2, 'identity'),
# MAGIC   -- TIER 1 (Dir C): trust_source_devtest → source_motor_devtest (cross-source, trust as MAIN)
# MAGIC   (37, 'viriyah_cdqm_poc.silver.trust_source_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'MAIN.id_card = MATCH.id_card',                1, 0.5,  1, 1, 'identity'),
# MAGIC   (38, 'viriyah_cdqm_poc.silver.trust_source_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'levenshtein(MAIN.fname, MATCH.fname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 1, 'identity'),
# MAGIC   (39, 'viriyah_cdqm_poc.silver.trust_source_devtest', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'levenshtein(MAIN.lname, MATCH.lname) <= 3 AND MAIN.id_card = MATCH.id_card', 1, 0.25, 1, 1, 'identity');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Insert CHV_PARAM_GENERAL_V2 (DATE offset = 0 for both devtest tables)

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL_V2
# MAGIC   (PARAM_GROUP_NAME, PARAM_NAME, PARAM_DESC, PARAM_VAL_NUMBER, PARAM_VAL_DATE, PARAM_VAL_VARCHAR)
# MAGIC VALUES
# MAGIC   ('DATE', 'viriyah_cdqm_poc.silver.source_motor_devtest', 'Lag DATA_DT', 0, NULL, NULL),
# MAGIC   ('DATE', 'viriyah_cdqm_poc.silver.trust_source_devtest', 'Lag DATA_DT', 0, NULL, NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Insert chv_config_check_pre_validation_v2 (CHECK_NULL for rules 31–39)
# MAGIC
# MAGIC Each matching rule needs MAIN + MATCH side pre-validation entries.
# MAGIC - Rules 31/34/37 (id_card exact match): validate id_card on both sides
# MAGIC - Rules 32/35/38 (fname levenshtein): validate fname on both sides
# MAGIC - Rules 33/36/39 (lname levenshtein): validate lname on both sides

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2
# MAGIC   (MATCHING_RULES, TABLE, RULES_CHECK, COLUMN, ACT_F)
# MAGIC VALUES
# MAGIC   -- Rule 31: id_card exact match (source → trust)
# MAGIC   (31, 'MAIN',  'CHECK_NULL', 'id_card', 1),
# MAGIC   (31, 'MATCH', 'CHECK_NULL', 'id_card', 1),
# MAGIC   -- Rule 32: fname levenshtein (source → trust)
# MAGIC   (32, 'MAIN',  'CHECK_NULL', 'fname', 1),
# MAGIC   (32, 'MATCH', 'CHECK_NULL', 'fname', 1),
# MAGIC   -- Rule 33: lname levenshtein (source → trust)
# MAGIC   (33, 'MAIN',  'CHECK_NULL', 'lname', 1),
# MAGIC   (33, 'MATCH', 'CHECK_NULL', 'lname', 1),
# MAGIC   -- Rule 34: id_card exact match (source self-match)
# MAGIC   (34, 'MAIN',  'CHECK_NULL', 'id_card', 1),
# MAGIC   (34, 'MATCH', 'CHECK_NULL', 'id_card', 1),
# MAGIC   -- Rule 35: fname levenshtein (source self-match)
# MAGIC   (35, 'MAIN',  'CHECK_NULL', 'fname', 1),
# MAGIC   (35, 'MATCH', 'CHECK_NULL', 'fname', 1),
# MAGIC   -- Rule 36: lname levenshtein (source self-match)
# MAGIC   (36, 'MAIN',  'CHECK_NULL', 'lname', 1),
# MAGIC   (36, 'MATCH', 'CHECK_NULL', 'lname', 1),
# MAGIC   -- Rule 37: id_card exact match (trust → source)
# MAGIC   (37, 'MAIN',  'CHECK_NULL', 'id_card', 1),
# MAGIC   (37, 'MATCH', 'CHECK_NULL', 'id_card', 1),
# MAGIC   -- Rule 38: fname levenshtein (trust → source)
# MAGIC   (38, 'MAIN',  'CHECK_NULL', 'fname', 1),
# MAGIC   (38, 'MATCH', 'CHECK_NULL', 'fname', 1),
# MAGIC   -- Rule 39: lname levenshtein (trust → source)
# MAGIC   (39, 'MAIN',  'CHECK_NULL', 'lname', 1),
# MAGIC   (39, 'MATCH', 'CHECK_NULL', 'lname', 1);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify PK config
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_config_pk_v2
# MAGIC WHERE lower(`TABLE`) LIKE '%devtest%'
# MAGIC ORDER BY `TABLE`, `ORDER`;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify matching config
# MAGIC SELECT MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, WEIGHT, ACT_F, TIER, SUBJECT
# MAGIC FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
# MAGIC WHERE MATCHING_RULES BETWEEN 31 AND 39
# MAGIC ORDER BY MATCHING_RULES;
