# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_matching (MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, MATCH_CONDITION, `GROUP`, WEIGHT, ACT_F)
# MAGIC VALUES
# MAGIC     (1, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Ident_card = MATCH.Id_card', 1, 1, 1),
# MAGIC     (2, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Fname = MATCH.Fname', 1, 0.35, 1),
# MAGIC     (3, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Lname = MATCH.Lname', 1, 0.35, 1),
# MAGIC     (4, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Tel = MATCH.Tel', 1, 0.35, 1),
# MAGIC     (5, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.DoB = MATCH.DoB', 1, 0.35, 1),
# MAGIC     (6, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Ident_card = MATCH.Ident_card', 1, 1, 1),
# MAGIC     (7, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Fname = MATCH.Fname', 1, 0.35, 1),
# MAGIC     (8, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Lname = MATCH.Lname', 1, 0.35, 1),
# MAGIC     (9, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Tel = MATCH.Tel', 1, 0.35, 1),
# MAGIC     (10, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.DoB = MATCH.DoB', 1, 0.35, 1),
# MAGIC     (11, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Id_card = MATCH.Ident_card', 1, 1, 1),
# MAGIC     (12, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Fname = MATCH.Fname', 1, 0.35, 1),
# MAGIC     (13, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Lname = MATCH.Lname', 1, 0.35, 1),
# MAGIC     (14, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Tel = MATCH.Tel', 1, 0.35, 1),
# MAGIC     (15, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.DoB = MATCH.DoB', 1, 0.35, 1);