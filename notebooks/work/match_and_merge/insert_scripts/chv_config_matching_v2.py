# Databricks notebook source
# MAGIC %md
# MAGIC # INSERT — chv_config_matching_v2
# MAGIC
# MAGIC 18 rules total = 15 SOURCE_MOTOR/TRUST_SOURCE rules + 3 tc_case2_similarity rules.
# MAGIC TIER and SUBJECT added to all rules.
# MAGIC
# MAGIC ## TIER assignment logic
# MAGIC | Rules | Direction | TIER | SUBJECT | Reason |
# MAGIC |---|---|---|---|---|
# MAGIC | 1–5 | source_motor → trust_source | 1 | identity | Cross-source match runs first |
# MAGIC | 6–10 | source_motor → source_motor | 2 | identity | Self-match only for records NOT matched in Tier 1 |
# MAGIC | 11–15 | trust_source → source_motor | 1 | identity | Cross-source (trust as MAIN) runs first |
# MAGIC | 19–21 | tc_case2_similarity → ts_case1_5101299025871 | 1 | identity | Single-direction levenshtein matching |
# MAGIC
# MAGIC **Key effect:** When source_motor is the main table, Tier 1 (cross-source) runs first.
# MAGIC Records that matched Tier 1 are excluded from Tier 2 (self-match),
# MAGIC preventing the dual-BKEY problem.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clear existing rows to allow idempotent re-run
# MAGIC DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_matching_v2
# MAGIC   (MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, MATCH_CONDITION, `GROUP`, WEIGHT, ACT_F, TIER, SUBJECT)
# MAGIC VALUES
# MAGIC   -- Tier 1: source_motor → trust_source  (cross-source, runs first)
# MAGIC   (1,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Ident_card = MATCH.Id_card',    1, 1,    1, 1, 'identity'),
# MAGIC   (2,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Fname = MATCH.Fname',          1, 0.35, 1, 1, 'identity'),
# MAGIC   (3,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Lname = MATCH.Lname',          1, 0.35, 1, 1, 'identity'),
# MAGIC   (4,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.Tel = MATCH.Tel',              1, 0.35, 1, 1, 'identity'),
# MAGIC   (5,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'MAIN.DoB = MATCH.DoB',              1, 0.35, 1, 1, 'identity'),
# MAGIC   -- Tier 2: source_motor → source_motor  (self-match, only for records unmatched in Tier 1)
# MAGIC   (6,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Ident_card = MATCH.Ident_card', 1, 1,    1, 2, 'identity'),
# MAGIC   (7,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Fname = MATCH.Fname',          1, 0.35, 1, 2, 'identity'),
# MAGIC   (8,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Lname = MATCH.Lname',          1, 0.35, 1, 2, 'identity'),
# MAGIC   (9,  'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Tel = MATCH.Tel',              1, 0.35, 1, 2, 'identity'),
# MAGIC   (10, 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.DoB = MATCH.DoB',              1, 0.35, 1, 2, 'identity'),
# MAGIC   -- Tier 1: trust_source → source_motor  (cross-source, runs first)
# MAGIC   (11, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Id_card = MATCH.Ident_card',   1, 1,    1, 1, 'identity'),
# MAGIC   (12, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Fname = MATCH.Fname',          1, 0.35, 1, 1, 'identity'),
# MAGIC   (13, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Lname = MATCH.Lname',          1, 0.35, 1, 1, 'identity'),
# MAGIC   (14, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.Tel = MATCH.Tel',              1, 0.35, 1, 1, 'identity'),
# MAGIC   (15, 'viriyah_cdqm_poc.silver.TRUST_SOURCE', 'viriyah_cdqm_poc.silver.SOURCE_MOTOR', 'MAIN.DoB = MATCH.DoB',              1, 0.35, 1, 1, 'identity'),
# MAGIC   -- Tier 1: tc_case2_similarity → ts_case1_5101299025871  (levenshtein similarity, single direction)
# MAGIC   (19, 'viriyah_cdqm_poc.silver.tc_case2_similarity', 'viriyah_cdqm_poc.silver.ts_case1_5101299025871', 'levenshtein(MAIN.id_card, MATCH.id_card)<=2', 1, 1.0, 1, 1, 'identity'),
# MAGIC   (20, 'viriyah_cdqm_poc.silver.tc_case2_similarity', 'viriyah_cdqm_poc.silver.ts_case1_5101299025871', 'levenshtein(MAIN.fname , MATCH.fname)<=3',    1, 0.5, 1, 1, 'identity'),
# MAGIC   (21, 'viriyah_cdqm_poc.silver.tc_case2_similarity', 'viriyah_cdqm_poc.silver.ts_case1_5101299025871', 'levenshtein(MAIN.lname , MATCH.lname)<=3',    1, 0.5, 1, 1, 'identity');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify
# MAGIC SELECT MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, WEIGHT, ACT_F, TIER, SUBJECT
# MAGIC FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
# MAGIC ORDER BY MAIN_TABLE, TIER, MATCHING_RULES;
