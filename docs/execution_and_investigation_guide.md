# Execution & Investigation Guide — Match and Merge Framework

**Project:** vrh
**Date:** 2026-02-21

---

## Overview — Execution Flow

```
vrh_chv_main
    │
    ├── [1] vrh_chv_pre_validation  ← run for SOURCE_MOTOR
    ├── [2] vrh_chv_pre_validation  ← run for TRUST_SOURCE
    │         (one run per unique MATCHING_TABLE in config)
    │
    └── [3] vrh_chv_match           ← run for SOURCE_MOTOR
```

---

## Step 1 — Prepare Parameters

| Parameter | ตัวอย่าง | หมายเหตุ |
|---|---|---|
| `table_name` | `SOURCE_MOTOR` | ชื่อตารางหลัก (ไม่ต้องใส่ catalog/schema) |
| `data_date` | `2026-01-05` | วันที่ของข้อมูล (yyyy-MM-dd) |

**PRCS_NM ที่จะถูก generate อัตโนมัติ:**
```
Pre-validation : EDP_PRE_VLD_SOURCE_MOTOR
Matching       : EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05
```

---

## Step 2 — Execution

### Option A: Run ผ่าน vrh_chv_main (แนะนำ)

```
Notebook  : /Workspace/Users/.../vrh/vrh_chv_main
Parameters:
  table_name = SOURCE_MOTOR
  data_date  = 2026-01-05
```

vrh_chv_main จะ:
1. Query `chv_config_matching` หา MATCHING_TABLE ทุกตัว
2. Run `vrh_chv_pre_validation` ให้ทุกตาราง (SOURCE_MOTOR + TRUST_SOURCE)
3. Run `vrh_chv_match` สำหรับ SOURCE_MOTOR

### Option B: Run ทีละ notebook (debug mode)

```
# Step 2.1 — Pre-validation: SOURCE_MOTOR
Notebook  : vrh_chv_pre_validation
PARAMS    : viriyah_cdqm_poc.silver.SOURCE_MOTOR
            ^|viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT
            ^|2026-01-05
            ^|EDP_PRE_VLD_SOURCE_MOTOR
            ^|1
            ^|EDP_PRE_VLD_SOURCE_MOTOR
            ^|1
ENV       : dev

# Step 2.2 — Pre-validation: TRUST_SOURCE
Notebook  : vrh_chv_pre_validation
PARAMS    : viriyah_cdqm_poc.silver.TRUST_SOURCE
            ^|viriyah_cdqm_poc.control_fw.CHV_PRE_VALIDATION_RESULT
            ^|2026-01-05
            ^|EDP_PRE_VLD_TRUST_SOURCE
            ^|1
            ^|EDP_PRE_VLD_TRUST_SOURCE
            ^|1
ENV       : dev

# Step 2.3 — Matching
Notebook  : vrh_chv_match
PARAMS    : viriyah_cdqm_poc.silver.SOURCE_MOTOR
            ^|2026-01-05
            ^|EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05
            ^|1
            ^|EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05
            ^|1
ENV       : dev
```

---

## Step 3 — Investigation Queries

### 3.1 Pre-validation Result

**ตรวจว่ามี record ผ่าน / ตกกี่ตัว**
```sql
SELECT
    TABLE,
    RESULT,
    COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result
WHERE DATA_DT  = '2026-01-05'
  AND PRCS_NM LIKE 'EDP_PRE_VLD_%'
GROUP BY TABLE, RESULT
ORDER BY TABLE, RESULT;
```

**ดู record ที่ FAILED พร้อมสาเหตุ**
```sql
SELECT
    TABLE,
    KEY,
    RULES,
    COLUMN,
    VALUE,
    RESULT
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result
WHERE DATA_DT = '2026-01-05'
  AND RESULT  = 'FAILED'
ORDER BY TABLE, KEY;
```

**คาดหวัง:** ทุก KEY ที่จะเข้า matching ต้อง PASSED ทุก rule

---

### 3.2 Matching Log — Raw Results per Rule

**สรุป PASSED/FAILED ต่อ rule**
```sql
SELECT
    MAIN_TABLE,
    MATCHING_TABLE,
    MATCHING_RULES,
    RESULT,
    COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_matching_log
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
GROUP BY MAIN_TABLE, MATCHING_TABLE, MATCHING_RULES, RESULT
ORDER BY MATCHING_RULES, RESULT;
```

**ดู record ที่ match ได้ในแต่ละ rule**
```sql
SELECT
    MATCHING_RULES,
    MAIN_TABLE,
    MATCHING_TABLE,
    KEY_MAIN,
    KEY_MATCH,
    RESULT
FROM viriyah_cdqm_poc.control_fw.chv_matching_log
WHERE DATA_DT  = '2026-01-05'
  AND PRCS_NM  = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND RESULT   = 'PASSED'
ORDER BY KEY_MAIN, MATCHING_RULES;
```

**ดู record ที่ไม่ match กับ rule ไหนเลย**
```sql
SELECT DISTINCT KEY_MAIN
FROM viriyah_cdqm_poc.control_fw.chv_matching_log
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND RESULT  = 'FAILED'

EXCEPT

SELECT DISTINCT KEY_MAIN
FROM viriyah_cdqm_poc.control_fw.chv_matching_log
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND RESULT  = 'PASSED';
```

---

### 3.3 Matching Result — Pairs ที่ผ่าน Weight Threshold

**ดู matched pairs ทั้งหมด**
```sql
SELECT
    MAIN_TABLE,
    MATCHING_TABLE,
    KEY_MAIN,
    KEY_MATCH
FROM viriyah_cdqm_poc.control_fw.chv_matching_result
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY KEY_MAIN;
```

**ตรวจ record ที่ match กับ TRUST_SOURCE (identity)**
```sql
SELECT KEY_MAIN, KEY_MATCH
FROM viriyah_cdqm_poc.control_fw.chv_matching_result
WHERE DATA_DT          = '2026-01-05'
  AND PRCS_NM          = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(MAIN_TABLE)     = 'viriyah_cdqm_poc.silver.source_motor'
  AND LOWER(MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.trust_source';
```

**ตรวจ record ที่ match กับ SOURCE_MOTOR เอง (self-dedup)**
```sql
SELECT KEY_MAIN, KEY_MATCH
FROM viriyah_cdqm_poc.control_fw.chv_matching_result
WHERE DATA_DT          = '2026-01-05'
  AND PRCS_NM          = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(MAIN_TABLE)     = 'viriyah_cdqm_poc.silver.source_motor'
  AND LOWER(MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.source_motor';
```

**⚠️ ตรวจ record ที่ match ทั้ง 2 ตาราง (dual-match) — ปัญหาที่รู้อยู่แล้ว**
```sql
SELECT
    a.KEY_MAIN,
    a.MATCHING_TABLE AS matched_trust,
    b.MATCHING_TABLE AS matched_self
FROM viriyah_cdqm_poc.control_fw.chv_matching_result a
JOIN viriyah_cdqm_poc.control_fw.chv_matching_result b
  ON a.KEY_MAIN = b.KEY_MAIN
  AND a.DATA_DT = b.DATA_DT
  AND a.PRCS_NM = b.PRCS_NM
WHERE a.DATA_DT               = '2026-01-05'
  AND a.PRCS_NM               = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(a.MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
  AND LOWER(b.MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.source_motor';
```

---

### 3.4 BKEY Table — Final Output

**ดู BKEY ทั้งหมดที่ generate**
```sql
SELECT
    TABLE,
    KEY,
    BKEY
FROM viriyah_cdqm_poc.silver.chv_table_bkey
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY BKEY, TABLE;
```

**ตรวจ record ที่ได้ BKEY มากกว่า 1 (dual-bkey problem)**
```sql
SELECT
    KEY,
    TABLE,
    COUNT(DISTINCT BKEY) AS bkey_count,
    COLLECT_LIST(BKEY)   AS bkeys
FROM viriyah_cdqm_poc.silver.chv_table_bkey
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
GROUP BY KEY, TABLE
HAVING COUNT(DISTINCT BKEY) > 1;
```

**ดู BKEY group — ใครอยู่ group เดียวกันบ้าง**
```sql
SELECT
    b.BKEY,
    b.TABLE,
    b.KEY
FROM viriyah_cdqm_poc.silver.chv_table_bkey b
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY BKEY, TABLE;
```

**ตรวจ SOURCE_MOTOR ที่ไม่มี BKEY จาก TRUST_SOURCE (ไม่ได้ match trust_source)**
```sql
SELECT b.KEY, b.BKEY
FROM viriyah_cdqm_poc.silver.chv_table_bkey b
WHERE b.DATA_DT         = '2026-01-05'
  AND b.PRCS_NM         = 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(b.TABLE)    = 'viriyah_cdqm_poc.silver.source_motor'
  AND NOT EXISTS (
      SELECT 1 FROM viriyah_cdqm_poc.silver.chv_table_bkey b2
      WHERE b2.BKEY     = b.BKEY
        AND b2.DATA_DT  = b.DATA_DT
        AND LOWER(b2.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
  );
```

---

## Step 4 — Full Investigation Dashboard (รัน ทีเดียวครบ)

```sql
-- ====================================================
-- INVESTIGATION DASHBOARD
-- กำหนดตัวแปรก่อน run
-- ====================================================
DECLARE OR REPLACE VARIABLE v_dt     STRING DEFAULT '2026-01-05';
DECLARE OR REPLACE VARIABLE v_prcs   STRING DEFAULT 'EDP_MATCHING_SOURCE_MOTOR_DATE_2026-01-05';
DECLARE OR REPLACE VARIABLE v_table  STRING DEFAULT 'viriyah_cdqm_poc.silver.source_motor';

-- [1] Pre-validation summary
SELECT 'PRE_VALIDATION' AS STAGE, TABLE, RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result
WHERE DATA_DT = v_dt GROUP BY TABLE, RESULT

UNION ALL

-- [2] Matching log summary
SELECT 'MATCHING_LOG' AS STAGE, MAIN_TABLE AS TABLE, RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_matching_log
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY MAIN_TABLE, RESULT

UNION ALL

-- [3] Matched pairs count
SELECT 'MATCHING_RESULT' AS STAGE, MATCHING_TABLE AS TABLE, 'MATCHED' AS RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_matching_result
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY MATCHING_TABLE

UNION ALL

-- [4] BKEY summary
SELECT 'BKEY' AS STAGE, TABLE, 'ASSIGNED' AS RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.silver.chv_table_bkey
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY TABLE

ORDER BY STAGE, TABLE;
```

---

## Summary — ตาราง Log ที่ต้องดู

```
STAGE               TABLE                          ดูอะไร
────────────────────────────────────────────────────────────────────
[1] Pre-validation  chv_pre_validation_result      record FAILED ก่อน matching
[2] Raw match       chv_matching_log               แต่ละ rule match ได้กี่คู่
[3] Passed pairs    chv_matching_result            คู่ที่ผ่าน weight threshold
[4] BKEY output     chv_table_bkey                 BKEY ที่ assign จริง + dual-bkey check
```

---

*Guide generated from design discussion — 2026-02-21*
