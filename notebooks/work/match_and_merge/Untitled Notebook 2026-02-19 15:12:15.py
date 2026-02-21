# Databricks notebook source
# MAGIC %sql
# MAGIC select from MAIN,MATCH
# MAGIC on MAIN.Fname = MATCH.Fname
# MAGIC -----------------------
# MAGIC select from MAIN,MATCH
# MAGIC left join
# MAGIC where MAIN.Fname = MATCH.Fname

# COMMAND ----------

# MAGIC %sql
# MAGIC select from MAIN,MATCH
# MAGIC on similality(MAIN.Fname,MATCH.Fname) >2

# COMMAND ----------

# MAGIC %sql
# MAGIC select from MAIN,MATCH
# MAGIC left join
# MAGIC where similality(MAIN.Fname,MATCH.Fname) >2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   MAIN.MAIN_TABLE AS MAIN_TABLE,
# MAGIC   MATCHING_TABLE AS MATCHING_TABLE,
# MAGIC   MAIN.RULES AS MATCHING_RULES,
# MAGIC   MAIN.KEY AS KEY_MAIN,
# MAGIC   MATCH.KEY AS KEY_MATCH,
# MAGIC   CASE
# MAGIC     WHEN MATCH.RULES IS NULL THEN 'FAILED'
# MAGIC     ELSE 'PASSED'
# MAGIC   END AS RESULT,
# MAGIC   '1' AS LD_ID,
# MAGIC   'EDM_SOURCE_MOTOR_DATE_2026-01-07' AS UPDT_PRCS_NM,
# MAGIC   '1' AS UPDT_LD_ID
# MAGIC FROM
# MAGIC   (
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.trust_source' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       Ident_card AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '1' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Ident_card'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC -----------------------------
# MAGIC
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.trust_source' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       Fname AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '2' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Fname'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.trust_source' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       Lname AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '3' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Lname'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.trust_source' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       Tel AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '4' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Tel'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.trust_source' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       DoB AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '5' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'DoB'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       Ident_card AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '6' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Ident_card'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       Fname AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '7' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Fname'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       Lname AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '8' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Lname'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       NULL AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       Tel AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '9' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'Tel'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     UNION ALL
# MAGIC     SELECT
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MAIN_TABLE,
# MAGIC       CAST('viriyah_cdqm_poc.silver.source_motor' AS STRING) AS MATCHING_TABLE,
# MAGIC       CONCAT_WS(
# MAGIC         ',',
# MAGIC         COALESCE(Ident_card, ''),
# MAGIC         COALESCE(Prefix, ''),
# MAGIC         COALESCE(Fname, ''),
# MAGIC         COALESCE(Lname, ''),
# MAGIC         COALESCE(Insert_Date, '')
# MAGIC       ) AS KEY,
# MAGIC       DoB AS DoB,
# MAGIC       NULL AS Ident_card,
# MAGIC       NULL AS Fname,
# MAGIC       NULL AS Tel,
# MAGIC       NULL AS Lname,
# MAGIC       NULL AS Id_card,
# MAGIC       '10' AS RULES
# MAGIC     FROM
# MAGIC       viriyah_cdqm_poc.silver.source_motor SRC
# MAGIC         LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC           ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC         INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC           ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC           AND CONCAT_WS(
# MAGIC             ',',
# MAGIC             COALESCE(Ident_card, ''),
# MAGIC             COALESCE(Prefix, ''),
# MAGIC             COALESCE(Fname, ''),
# MAGIC             COALESCE(Lname, ''),
# MAGIC             COALESCE(Insert_Date, '')
# MAGIC           ) = VLD_RESULT.KEY
# MAGIC           AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC           AND VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC           AND (
# MAGIC             (
# MAGIC               VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC               AND COLUMN = 'DoB'
# MAGIC               AND RESULT = 'PASSED'
# MAGIC             )
# MAGIC           )
# MAGIC     WHERE
# MAGIC       SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC   ) AS MAIN
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         '1' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         Id_card AS Id_card,
# MAGIC         CONCAT_WS(',', COALESCE(Id_card, '')) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.trust_source' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.TRUST_SOURCE SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND CONCAT_WS(',', COALESCE(Id_card, '')) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Id_card'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '2' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         Fname AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(',', COALESCE(Id_card, '')) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.trust_source' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.TRUST_SOURCE SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND CONCAT_WS(',', COALESCE(Id_card, '')) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Fname'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '3' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         Lname AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(',', COALESCE(Id_card, '')) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.trust_source' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.TRUST_SOURCE SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND CONCAT_WS(',', COALESCE(Id_card, '')) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Lname'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '4' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         Tel AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(',', COALESCE(Id_card, '')) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.trust_source' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.TRUST_SOURCE SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND CONCAT_WS(',', COALESCE(Id_card, '')) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Tel'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '5' AS RULES,
# MAGIC         DoB AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(',', COALESCE(Id_card, '')) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.trust_source' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.TRUST_SOURCE SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
# MAGIC             AND CONCAT_WS(',', COALESCE(Id_card, '')) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'DoB'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '6' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         Ident_card AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(
# MAGIC           ',',
# MAGIC           COALESCE(Ident_card, ''),
# MAGIC           COALESCE(Prefix, ''),
# MAGIC           COALESCE(Fname, ''),
# MAGIC           COALESCE(Lname, ''),
# MAGIC           COALESCE(Insert_Date, '')
# MAGIC         ) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.source_motor' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.SOURCE_MOTOR SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND CONCAT_WS(
# MAGIC               ',',
# MAGIC               COALESCE(Ident_card, ''),
# MAGIC               COALESCE(Prefix, ''),
# MAGIC               COALESCE(Fname, ''),
# MAGIC               COALESCE(Lname, ''),
# MAGIC               COALESCE(Insert_Date, '')
# MAGIC             ) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Ident_card'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '7' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         Fname AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(
# MAGIC           ',',
# MAGIC           COALESCE(Ident_card, ''),
# MAGIC           COALESCE(Prefix, ''),
# MAGIC           COALESCE(Fname, ''),
# MAGIC           COALESCE(Lname, ''),
# MAGIC           COALESCE(Insert_Date, '')
# MAGIC         ) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.source_motor' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.SOURCE_MOTOR SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND CONCAT_WS(
# MAGIC               ',',
# MAGIC               COALESCE(Ident_card, ''),
# MAGIC               COALESCE(Prefix, ''),
# MAGIC               COALESCE(Fname, ''),
# MAGIC               COALESCE(Lname, ''),
# MAGIC               COALESCE(Insert_Date, '')
# MAGIC             ) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Fname'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '8' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         Lname AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(
# MAGIC           ',',
# MAGIC           COALESCE(Ident_card, ''),
# MAGIC           COALESCE(Prefix, ''),
# MAGIC           COALESCE(Fname, ''),
# MAGIC           COALESCE(Lname, ''),
# MAGIC           COALESCE(Insert_Date, '')
# MAGIC         ) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.source_motor' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.SOURCE_MOTOR SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND CONCAT_WS(
# MAGIC               ',',
# MAGIC               COALESCE(Ident_card, ''),
# MAGIC               COALESCE(Prefix, ''),
# MAGIC               COALESCE(Fname, ''),
# MAGIC               COALESCE(Lname, ''),
# MAGIC               COALESCE(Insert_Date, '')
# MAGIC             ) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Lname'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '9' AS RULES,
# MAGIC         NULL AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         Tel AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(
# MAGIC           ',',
# MAGIC           COALESCE(Ident_card, ''),
# MAGIC           COALESCE(Prefix, ''),
# MAGIC           COALESCE(Fname, ''),
# MAGIC           COALESCE(Lname, ''),
# MAGIC           COALESCE(Insert_Date, '')
# MAGIC         ) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.source_motor' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.SOURCE_MOTOR SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND CONCAT_WS(
# MAGIC               ',',
# MAGIC               COALESCE(Ident_card, ''),
# MAGIC               COALESCE(Prefix, ''),
# MAGIC               COALESCE(Fname, ''),
# MAGIC               COALESCE(Lname, ''),
# MAGIC               COALESCE(Insert_Date, '')
# MAGIC             ) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'Tel'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC       UNION ALL
# MAGIC       SELECT
# MAGIC         '10' AS RULES,
# MAGIC         DoB AS DoB,
# MAGIC         NULL AS Ident_card,
# MAGIC         NULL AS Fname,
# MAGIC         NULL AS Tel,
# MAGIC         NULL AS Lname,
# MAGIC         NULL AS Id_card,
# MAGIC         CONCAT_WS(
# MAGIC           ',',
# MAGIC           COALESCE(Ident_card, ''),
# MAGIC           COALESCE(Prefix, ''),
# MAGIC           COALESCE(Fname, ''),
# MAGIC           COALESCE(Lname, ''),
# MAGIC           COALESCE(Insert_Date, '')
# MAGIC         ) AS KEY,
# MAGIC         'viriyah_cdqm_poc.silver.source_motor' AS TABLE
# MAGIC       FROM
# MAGIC         viriyah_cdqm_poc.silver.SOURCE_MOTOR SRC
# MAGIC           LEFT JOIN viriyah_cdqm_poc.control_fw.CHV_PARAM_GENERAL PARM
# MAGIC             ON LOWER(PARM.PARAM_NAME) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND UPPER(PARM.PARAM_GROUP_NAME) = 'DATE'
# MAGIC           INNER JOIN viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT VLD_RESULT
# MAGIC             ON VLD_RESULT.DATA_DT = '2026-01-07'
# MAGIC             AND LOWER(VLD_RESULT.TABLE) = 'viriyah_cdqm_poc.silver.source_motor'
# MAGIC             AND CONCAT_WS(
# MAGIC               ',',
# MAGIC               COALESCE(Ident_card, ''),
# MAGIC               COALESCE(Prefix, ''),
# MAGIC               COALESCE(Fname, ''),
# MAGIC               COALESCE(Lname, ''),
# MAGIC               COALESCE(Insert_Date, '')
# MAGIC             ) = VLD_RESULT.KEY
# MAGIC             AND VLD_RESULT.RESULT = 'PASSED'
# MAGIC             AND (
# MAGIC               (
# MAGIC                 VLD_RESULT.RULES = 'CHECK_NULL'
# MAGIC                 AND COLUMN = 'DoB'
# MAGIC                 AND RESULT = 'PASSED'
# MAGIC               )
# MAGIC             )
# MAGIC       WHERE
# MAGIC         SRC.DATA_DT = DATE_FORMAT(DATE_ADD(TO_DATE('2026-01-07', 'yyyy-MM-dd'), 0), 'yyyy-MM-dd')
# MAGIC     ) MATCH
# MAGIC       ON (
# MAGIC         MAIN.Ident_card = MATCH.Id_card
# MAGIC         AND MAIN.RULES = '1'
# MAGIC         AND MATCH.RULES = '1'
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Fname = MATCH.Fname
# MAGIC         AND MAIN.RULES = '2'
# MAGIC         AND MATCH.RULES = '2'
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Lname = MATCH.Lname
# MAGIC         AND MAIN.RULES = '3'
# MAGIC         AND MATCH.RULES = '3'
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Tel = MATCH.Tel
# MAGIC         AND MAIN.RULES = '4'
# MAGIC         AND MATCH.RULES = '4'
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.DoB = MATCH.DoB
# MAGIC         AND MAIN.RULES = '5'
# MAGIC         AND MATCH.RULES = '5'
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Ident_card = MATCH.Ident_card
# MAGIC         AND MAIN.RULES = '6'
# MAGIC         AND MATCH.RULES = '6'
# MAGIC         AND MAIN.KEY <> MATCH.KEY
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Fname = MATCH.Fname
# MAGIC         AND MAIN.RULES = '7'
# MAGIC         AND MATCH.RULES = '7'
# MAGIC         AND MAIN.KEY <> MATCH.KEY
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Lname = MATCH.Lname
# MAGIC         AND MAIN.RULES = '8'
# MAGIC         AND MATCH.RULES = '8'
# MAGIC         AND MAIN.KEY <> MATCH.KEY
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.Tel = MATCH.Tel
# MAGIC         AND MAIN.RULES = '9'
# MAGIC         AND MATCH.RULES = '9'
# MAGIC         AND MAIN.KEY <> MATCH.KEY
# MAGIC       )
# MAGIC       OR (
# MAGIC         MAIN.DoB = MATCH.DoB
# MAGIC         AND MAIN.RULES = '10'
# MAGIC         AND MATCH.RULES = '10'
# MAGIC         AND MAIN.KEY <> MATCH.KEY
# MAGIC       )