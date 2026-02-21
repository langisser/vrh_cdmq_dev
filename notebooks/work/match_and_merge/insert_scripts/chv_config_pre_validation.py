# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_pre_validation (TABLE, RULES, PARAMETER, CUSTOM_CONDITION, ACT_F)
# MAGIC VALUES
# MAGIC     ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'CHECK_NULL', 'Ident_card', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'CHECK_NULL', 'Fname', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'CHECK_NULL', 'Lname', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'CHECK_NULL', 'Tel', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'CHECK_NULL', 'DoB', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'CHECK_NULL', 'Id_card', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'CHECK_NULL', 'Fname', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'CHECK_NULL', 'Lname', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'CHECK_NULL', 'Tel', NULL, 1),
# MAGIC     ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'CHECK_NULL', 'DoB', NULL, 1);