# Databricks notebook source
# MAGIC %md
# MAGIC # Copy config data from original tables → _v2 tables
# MAGIC
# MAGIC Copies data for the 5 config tables that have the same schema.
# MAGIC Run this AFTER `ddl_v2_tables.py` creates the tables.
# MAGIC
# MAGIC **Note:** `chv_config_matching_v2` is populated by `chv_config_matching_v2.py` (separate)
# MAGIC because it has new TIER + SUBJECT columns that need specific values.
# MAGIC
# MAGIC | Source | Target |
# MAGIC |---|---|
# MAGIC | chv_config_pk | chv_config_pk_v2 |
# MAGIC | chv_config_function | chv_config_function_v2 |
# MAGIC | chv_config_pre_validation | chv_config_pre_validation_v2 |
# MAGIC | chv_config_check_pre_validation | chv_config_check_pre_validation_v2 |
# MAGIC | chv_param_general | chv_param_general_v2 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. chv_config_pk → chv_config_pk_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_pk_v2
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_config_pk;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. chv_config_function → chv_config_function_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_function_v2
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_config_function;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. chv_config_pre_validation → chv_config_pre_validation_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_config_pre_validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. chv_config_check_pre_validation → chv_config_check_pre_validation_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. chv_param_general → chv_param_general_v2

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_param_general_v2
# MAGIC SELECT * FROM viriyah_cdqm_poc.control_fw.chv_param_general;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify — row counts should match originals

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'chv_config_pk'           AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_pk
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_pk_v2'        AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_pk_v2
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_function'     AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_function
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_function_v2'  AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_function_v2
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_pre_validation'     AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_pre_validation
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_pre_validation_v2'  AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_check_pre_validation'     AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation
# MAGIC UNION ALL
# MAGIC SELECT 'chv_config_check_pre_validation_v2'  AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2
# MAGIC UNION ALL
# MAGIC SELECT 'chv_param_general'     AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_param_general
# MAGIC UNION ALL
# MAGIC SELECT 'chv_param_general_v2'  AS tbl, COUNT(*) AS n FROM viriyah_cdqm_poc.control_fw.chv_param_general_v2
# MAGIC ORDER BY tbl;
