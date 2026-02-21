# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_param_general (PARAM_GROUP_NAME, PARAM_NAME, PARAM_DESC, PARAM_VAL_NUMBER, PARAM_VAL_DATE, PARAM_VAL_VARCHAR) 
# MAGIC VALUES 
# MAGIC   ('DATE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Lag DATA_DT', 0, NULL, NULL),
# MAGIC   ('DATE', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'Lag DATA_DT', 0, NULL, NULL)