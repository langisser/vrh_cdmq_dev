-- ============================================================
-- Step 0: Clean result/log tables and non-devtest config rows
-- Run this in Databricks SQL editor BEFORE re-running the pipeline
-- ============================================================

-- Step 0a: TRUNCATE result/log tables (wipe all rows for fresh run)
TRUNCATE TABLE viriyah_cdqm_poc.silver.chv_table_bkey_v2;
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_matching_result_v2;
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_matching_log_v2;
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2;

-- ── Verify counts (expect all 0) ──────────────────────────────────────────
SELECT 'chv_table_bkey_v2'           AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
UNION ALL
SELECT 'chv_matching_result_v2'      AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2
UNION ALL
SELECT 'chv_matching_log_v2'         AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
UNION ALL
SELECT 'chv_pre_validation_result_v2' AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2;


-- Step 0b: DELETE non-devtest rows from config tables
-- (DO NOT touch CHV_PARAM_GENERAL_V2)

-- CHV_CONFIG_MATCHING_V2: keep only devtest rules 31–39
DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
WHERE MATCHING_RULES NOT IN (31, 32, 33, 34, 35, 36, 37, 38, 39);

-- CHV_CONFIG_PRE_VALIDATION_V2: keep only devtest tables
DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2
WHERE `TABLE` NOT LIKE '%devtest%';

-- CHV_CONFIG_CHECK_PRE_VALIDATION_V2: keep only devtest rules 31–39
DELETE FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2
WHERE MATCHING_RULES NOT IN (31, 32, 33, 34, 35, 36, 37, 38, 39);

-- ── Verify config counts (expect only devtest rows) ───────────────────────
SELECT 'chv_config_matching_v2'            AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
UNION ALL
SELECT 'chv_config_pre_validation_v2'      AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2
UNION ALL
SELECT 'chv_config_check_pre_validation_v2' AS tbl, COUNT(*) AS cnt FROM viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2;
