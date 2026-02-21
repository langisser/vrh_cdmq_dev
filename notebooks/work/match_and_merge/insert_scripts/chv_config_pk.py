# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_pk (TABLE, PK, ORDER, ACT_F) 
# MAGIC VALUES 
# MAGIC   ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Ident_card', 1, 1),
# MAGIC   ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Prefix', 2, 1),
# MAGIC   ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Fname', 3, 1),
# MAGIC   ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Lname', 4, 1),
# MAGIC   ('viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'Insert_Date', 5, 1), 
# MAGIC   ('viriyah_cdqm_poc.silver.TRUST_SOURCE', 'Id_card', 1, 1);