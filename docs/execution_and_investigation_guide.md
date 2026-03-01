# Execution & Investigation Guide — Match and Merge Framework

**Project:** vrh
**Date:** 2026-02-21

---

## Overview — Execution Flow

```
[1] vrh_chv_pre_validation_v2  ← run for SOURCE_MOTOR (MAIN table)
[2] vrh_chv_pre_validation_v2  ← run for TRUST_SOURCE (MATCHING table)
[3] vrh_chv_match_v2           ← BKEY assignment (Union-Find)
[4] dedup_customer_name  ┐
    dedup_province       │ ← individual dedup notebooks
    dedup_gender         │   run per-table, per data_dt
    dedup_email          │
    dedup_phone          ┘
```

> **Note:** `vrh_chv_main_v2` is the orchestrator notebook in production.
> For dev/debug, run each notebook individually (Option B below).

---

## Step 1 — Prepare Parameters

| Parameter | ตัวอย่าง | หมายเหตุ |
|---|---|---|
| `table_name` | `SOURCE_MOTOR` | ชื่อตารางหลัก (ไม่ต้องใส่ catalog/schema) |
| `data_date` | `2026-01-05` | วันที่ของข้อมูล (yyyy-MM-dd) |

**PRCS_NM ที่จะถูก generate อัตโนมัติ:**
```
Pre-validation : EDP_PRE_VLD_V2_{SOURCE_TABLE_NAME}   เช่น EDP_PRE_VLD_V2_SOURCE_MOTOR
Matching       : EDP_MATCHING_V2_{TABLE_NAME}_DATE_{data_date}   เช่น EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05
```

---

## Step 2 — Execution

### Option A: Run ผ่าน vrh_chv_main_v2 (production)

```
Notebook  : /Workspace/Users/.../vrh/match_and_merge/vrh_chv_main_v2
Parameters:
  table_name = SOURCE_MOTOR
  data_date  = 2026-01-05
```

vrh_chv_main_v2 จะ:
1. Query `chv_config_matching_v2` หา MATCHING_TABLE ทุกตัว
2. Run `vrh_chv_pre_validation_v2` ให้ทุกตาราง (SOURCE_MOTOR + TRUST_SOURCE)
3. Run `vrh_chv_match_v2` สำหรับ SOURCE_MOTOR

หลัง match เสร็จ — รัน dedup notebooks แยกต่างหาก (ดู Step 2.4)

### Option B: Run ทีละ notebook (dev/debug)

> **PARAMS format:** ใช้ `^|` เป็น separator, `ENV` ต้องเป็น `'dev'` เสมอ (ไม่ใช่ blank)
> **PRCS_NM:** ตั้งชื่อให้สอดคล้องกับ table + data_date เพื่อ query log ได้ง่าย

```
# Step 2.1 — Pre-validation: SOURCE_MOTOR_DEVTEST  (ตัวอย่าง devtest)
Notebook  : vrh_chv_pre_validation_v2
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest
            ^|viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
            ^|2025-01-01
            ^|EDP_PRE_VLD_V2_SOURCE_MOTOR_DEVTEST
            ^|1
            ^|EDP_PRE_VLD_V2_SOURCE_MOTOR_DEVTEST
            ^|1
ENV       : dev

# Step 2.2 — Pre-validation: TRUST_SOURCE_DEVTEST
Notebook  : vrh_chv_pre_validation_v2
PARAMS    : viriyah_cdqm_poc.silver.trust_source_devtest
            ^|viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
            ^|2025-01-01
            ^|EDP_PRE_VLD_V2_TRUST_SOURCE_DEVTEST
            ^|1
            ^|EDP_PRE_VLD_V2_TRUST_SOURCE_DEVTEST
            ^|1
ENV       : dev

# Step 2.3 — Matching: SOURCE_MOTOR_DEVTEST
Notebook  : vrh_chv_match_v2
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest
            ^|2025-01-01
            ^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01
            ^|1
            ^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01
            ^|1
ENV       : dev
```

> **เปลี่ยน data_date:** แก้ `2025-01-01` → วันที่ต้องการ แล้วแก้ PRCS_NM ให้ตรงด้วย เช่น `EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-02`

```
# Step 2.4 — Dedup (รันหลัง match เสร็จ)
# รัน 5 notebooks ด้านล่าง ทีละตัว (ลำดับไม่สำคัญ)

Notebook  : dedup/dedup_customer_name
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_CUSTOMER_NAME^|1

Notebook  : dedup/dedup_province
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_PROVINCE^|1

Notebook  : dedup/dedup_gender
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_GENDER^|1

Notebook  : dedup/dedup_email
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_EMAIL^|1

Notebook  : dedup/dedup_phone
PARAMS    : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_PHONE^|1
```

> แต่ละ dedup notebook ใช้ MERGE pattern — รันซ้ำได้ (idempotent)
> Script rebuild ทั้งหมด: `scripts/run_dedup_full_rebuild.py`

---

## Step 3 — Investigation Queries

### 3.1 Pre-validation Result

**ตรวจว่ามี record ผ่าน / ตกกี่ตัว**
```sql
SELECT
    TABLE,
    RESULT,
    COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
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
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
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
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
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
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT  = '2026-01-05'
  AND PRCS_NM  = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
  AND RESULT   = 'PASSED'
ORDER BY KEY_MAIN, MATCHING_RULES;
```

**ดู record ที่ไม่ match กับ rule ไหนเลย**
```sql
SELECT DISTINCT KEY_MAIN
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
  AND RESULT  = 'FAILED'

EXCEPT

SELECT DISTINCT KEY_MAIN
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
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
FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY KEY_MAIN;
```

**ตรวจ record ที่ match กับ TRUST_SOURCE (identity)**
```sql
SELECT KEY_MAIN, KEY_MATCH
FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2
WHERE DATA_DT          = '2026-01-05'
  AND PRCS_NM          = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(MAIN_TABLE)     = 'viriyah_cdqm_poc.silver.source_motor'
  AND LOWER(MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.trust_source';
```

**ตรวจ record ที่ match กับ SOURCE_MOTOR เอง (self-dedup)**
```sql
SELECT KEY_MAIN, KEY_MATCH
FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2
WHERE DATA_DT          = '2026-01-05'
  AND PRCS_NM          = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(MAIN_TABLE)     = 'viriyah_cdqm_poc.silver.source_motor'
  AND LOWER(MATCHING_TABLE) = 'viriyah_cdqm_poc.silver.source_motor';
```

**⚠️ ตรวจ record ที่ match ทั้ง 2 ตาราง (dual-match) — ปัญหาที่รู้อยู่แล้ว**
```sql
SELECT
    a.KEY_MAIN,
    a.MATCHING_TABLE AS matched_trust,
    b.MATCHING_TABLE AS matched_self
FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2 a
JOIN viriyah_cdqm_poc.control_fw.chv_matching_result_v2 b
  ON a.KEY_MAIN = b.KEY_MAIN
  AND a.DATA_DT = b.DATA_DT
  AND a.PRCS_NM = b.PRCS_NM
WHERE a.DATA_DT               = '2026-01-05'
  AND a.PRCS_NM               = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
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
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY BKEY, TABLE;
```

**ตรวจ record ที่ได้ BKEY มากกว่า 1 (expect 0 rows)**
```sql
SELECT
    KEY,
    TABLE,
    COUNT(DISTINCT BKEY) AS bkey_count,
    COLLECT_LIST(BKEY)   AS bkeys
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
GROUP BY KEY, TABLE
HAVING COUNT(DISTINCT BKEY) > 1;
```

**ดู BKEY group — ใครอยู่ group เดียวกันบ้าง**
```sql
SELECT
    b.BKEY,
    b.TABLE,
    b.KEY
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
WHERE DATA_DT = '2026-01-05'
  AND PRCS_NM = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
ORDER BY BKEY, TABLE;
```

**ตรวจ SOURCE_MOTOR ที่ไม่มี BKEY จาก TRUST_SOURCE (ไม่ได้ match trust_source)**
```sql
SELECT b.KEY, b.BKEY
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b
WHERE b.DATA_DT         = '2026-01-05'
  AND b.PRCS_NM         = 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05'
  AND LOWER(b.TABLE)    = 'viriyah_cdqm_poc.silver.source_motor'
  AND NOT EXISTS (
      SELECT 1 FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2 b2
      WHERE b2.BKEY     = b.BKEY
        AND b2.DATA_DT  = b.DATA_DT
        AND LOWER(b2.TABLE) = 'viriyah_cdqm_poc.silver.trust_source'
  );
```

---

### 3.5 Dedup Output Tables

**ตรวจสอบ duplicate groups (expect 0)**
```sql
SELECT 'dedup_customer_name' AS tbl, COUNT(*) AS dup_groups FROM (
    SELECT bkey, id_card, fname, lname, prefix, COUNT(*) AS n
    FROM viriyah_cdqm_poc.silver.dedup_customer_name
    GROUP BY bkey, id_card, fname, lname, prefix HAVING n > 1)
UNION ALL
SELECT 'dedup_gender', COUNT(*) FROM (
    SELECT bkey, id_card, gender, birth_date, COUNT(*) AS n
    FROM viriyah_cdqm_poc.silver.dedup_gender
    GROUP BY bkey, id_card, gender, birth_date HAVING n > 1)
UNION ALL
SELECT 'dedup_province', COUNT(*) FROM (
    SELECT bkey, id_card, area, district, postcode, province, COUNT(*) AS n
    FROM viriyah_cdqm_poc.silver.dedup_province
    GROUP BY bkey, id_card, area, district, postcode, province HAVING n > 1)
UNION ALL
SELECT 'dedup_email', COUNT(*) FROM (
    SELECT bkey, id_card, email, COUNT(*) AS n
    FROM viriyah_cdqm_poc.silver.dedup_email
    GROUP BY bkey, id_card, email HAVING n > 1)
UNION ALL
SELECT 'dedup_phone', COUNT(*) FROM (
    SELECT bkey, id_card, phone_no, COUNT(*) AS n
    FROM viriyah_cdqm_poc.silver.dedup_phone
    GROUP BY bkey, id_card, phone_no HAVING n > 1);
```

**ตรวจ Thai unicode name variants**
```sql
SELECT * FROM viriyah_cdqm_poc.silver.dedup_name_variant_report
ORDER BY bkey;
-- รัน dedup/dedup_name_variant_report notebook เพื่อ refresh
```

---

## Step 4 — Full Investigation Dashboard (รัน ทีเดียวครบ)

```sql
-- ====================================================
-- INVESTIGATION DASHBOARD
-- กำหนดตัวแปรก่อน run
-- ====================================================
DECLARE OR REPLACE VARIABLE v_dt     STRING DEFAULT '2026-01-05';
DECLARE OR REPLACE VARIABLE v_prcs   STRING DEFAULT 'EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2026-01-05';
DECLARE OR REPLACE VARIABLE v_table  STRING DEFAULT 'viriyah_cdqm_poc.silver.source_motor';

-- [1] Pre-validation summary
SELECT 'PRE_VALIDATION' AS STAGE, TABLE, RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
WHERE DATA_DT = v_dt GROUP BY TABLE, RESULT

UNION ALL

-- [2] Matching log summary
SELECT 'MATCHING_LOG' AS STAGE, MAIN_TABLE AS TABLE, RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY MAIN_TABLE, RESULT

UNION ALL

-- [3] Matched pairs count
SELECT 'MATCHING_RESULT' AS STAGE, MATCHING_TABLE AS TABLE, 'MATCHED' AS RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.control_fw.chv_matching_result_v2
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY MATCHING_TABLE

UNION ALL

-- [4] BKEY summary
SELECT 'BKEY' AS STAGE, TABLE, 'ASSIGNED' AS RESULT, COUNT(*) AS CNT
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = v_dt AND PRCS_NM = v_prcs GROUP BY TABLE

ORDER BY STAGE, TABLE;
```

---

## Summary — ตาราง Log ที่ต้องดู

```
STAGE               TABLE                              ดูอะไร
─────────────────────────────────────────────────────────────────────────
[1] Pre-validation  chv_pre_validation_result_v2        record FAILED ก่อน matching
[2] Raw match       chv_matching_log_v2                 แต่ละ rule match ได้กี่คู่
[3] Passed pairs    chv_matching_result_v2              คู่ที่ผ่าน weight threshold
[4] BKEY output     chv_table_bkey_v2                   BKEY ที่ assign จริง
[5] Dedup output    dedup_customer_name/province/        grouped dedup tables
                    gender/email/phone
[6] Name variants   dedup_name_variant_report            Thai unicode anomalies
```

---

*Guide generated from design discussion — 2026-02-21*

---

## Learning Cases — ปัญหาที่เจอระหว่าง setup devtest pipeline

### LC-001: CHV_CONFIG_PRE_VALIDATION_V2 ต้องมี entry ก่อน run pre_validation

**วันที่:** 2026-02-26
**Symptom:** `vrh_chv_pre_validation_v2` fail ด้วย `RunLifeCycleState.INTERNAL_ERROR` แต่ไม่มี error trace

**Root cause:** `CHV_CONFIG_PRE_VALIDATION_V2` ไม่มี row สำหรับ table ใหม่ → notebook ดึง config ออกมาเป็น empty DataFrame → fail ระหว่าง execute โดยไม่มี error message ชัดเจน

**Fix:** INSERT config ก่อน run pre_validation เสมอ
```sql
INSERT INTO viriyah_cdqm_poc.control_fw.CHV_CONFIG_PRE_VALIDATION_V2
  (TABLE, RULES, PARAMETER, CUSTOM_CONDITION, ACT_F)
VALUES
  ('viriyah_cdqm_poc.silver.<new_table>', 'CHECK_NULL', 'id_card', NULL, 1),
  ('viriyah_cdqm_poc.silver.<new_table>', 'CHECK_NULL', 'fname',   NULL, 1),
  ('viriyah_cdqm_poc.silver.<new_table>', 'CHECK_NULL', 'lname',   NULL, 1);
```

**Checklist สำหรับ table ใหม่ — ก่อน run pipeline:**
| Config table | ตรวจอะไร |
|---|---|
| `CHV_CONFIG_PK_V2` | มี PK entry สำหรับ table ใหม่ |
| `CHV_CONFIG_MATCHING_V2` | มี matching rules (MAIN_TABLE/MATCHING_TABLE/TIER/SUBJECT) |
| `CHV_CONFIG_PRE_VALIDATION_V2` | **มี CHECK_NULL entry สำหรับทุก column ที่ใช้ใน matching** |
| `CHV_CONFIG_CHECK_PRE_VALIDATION_V2` | มี CHECK_NULL entry ต่อ MATCHING_RULES (MAIN + MATCH side) |
| `CHV_PARAM_GENERAL_V2` | มี DATE lag = 0 สำหรับ table ใหม่ |

---

### LC-002: ลำดับการ run pipeline สำหรับ table ใหม่

**วันที่:** 2026-02-26
**Context:** รัน `vrh_chv_match_v2` โดยตรง → notebook SUCCESS แต่ไม่มีข้อมูลใน `chv_table_bkey_v2`

**Root cause:** `vrh_chv_match_v2` join กับ `CHV_PRE_VALIDATION_RESULT_V2` ด้วย INNER JOIN → ถ้าไม่มี pre_val result จะได้ empty result โดยไม่มี error

**ลำดับที่ถูกต้อง:**
```
1. vrh_chv_pre_validation_v2  ← run สำหรับ MAIN table
2. vrh_chv_pre_validation_v2  ← run สำหรับ MATCHING table (ถ้าต่างกัน)
3. vrh_chv_match_v2           ← run หลัง pre_val ครบทุก table
```

**PARAMS format:**
```
# pre_validation_v2 (7 params):
<table>^|<vld_result_table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# match_v2 (6 params):
<table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# ENV: ใช้ 'dev' เสมอ (ไม่ใช่ '' หรือ blank)
```

---

> **LC-003: วิธี run notebook บน Databricks** → ดูใน `CLAUDE.md` section "Development Workflow"
