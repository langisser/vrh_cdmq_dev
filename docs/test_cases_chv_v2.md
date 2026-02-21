# CHV Match & Merge v2 — Test Cases
**System:** VRH CHV Pipeline v2
**Environment:** Databricks — `viriyah_cdqm_poc` (dev)
**Date:** 2026-02-21

---

## Before Running Any Test

If re-running, truncate result tables first:
```sql
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2;
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_matching_log_v2;
TRUNCATE TABLE viriyah_cdqm_poc.control_fw.chv_matching_result_v2;
TRUNCATE TABLE viriyah_cdqm_poc.silver.chv_table_bkey_v2;
```

---

## TC-01: Similarity Matching

### Requirement
Records from the source table that share similar names and date of birth must be grouped under the same BKEY, even if there are minor spelling differences (fuzzy/similarity matching). Both the source records and their matched counterparts must appear in the bkey table with the same BKEY value.

### Input

**Command:**
```
python3 tests/run_chv_v2.py match tc_case2_similarity 2015-12-28
```

**Source table:** `viriyah_cdqm_poc.silver.tc_case2_similarity` — DATA_DT = `2015-12-28`

| id_card | Fname | Lname | DoB |
|---|---|---|---|
| 5101299025871 | วิริยะ | สุขุมพันธ์ | 1994-10-16 |
| 5101299025871 | วิริยะ | สุขุมพันธ์ | 1994-10-16 |
| 5101299025871 | วิริยะ | สุขุมพันธ | 1994-10-16 |
| 5101299025871 | วิริยะ | สุขุมพัน | 1994-10-16 |
| 5101299025871 | วิริยะ | สุขุมพันธุ | 1994-10-16 |

**Matching table:** `viriyah_cdqm_poc.silver.ts_case1_5101299025871`

| id_card | Fname | Lname | DoB |
|---|---|---|---|
| 5101299025871 | วิริยะ | สุขุมพันธ์ | 1994-10-16 |

### Expected Result

**Run result:** SUCCESS

**`chv_matching_log_v2`** — 15 rows, all RESULT = PASSED
```sql
SELECT MATCHING_RULES, RESULT, COUNT(*) AS cnt
FROM viriyah_cdqm_poc.control_fw.chv_matching_log_v2
WHERE DATA_DT = '2015-12-28'
GROUP BY MATCHING_RULES, RESULT
ORDER BY MATCHING_RULES
```
Expected: 3 rules × 5 records = 15 rows, RESULT = PASSED for all

**`chv_table_bkey_v2`** — 6 rows, all BKEY = 1, SUBJECT = identity
```sql
SELECT TABLE, KEY, BKEY, SUBJECT
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = '2015-12-28'
ORDER BY TABLE, KEY
```

| TABLE | BKEY | SUBJECT | Rows |
|---|---|---|---|
| silver.tc_case2_similarity | 1 | identity | 5 |
| silver.ts_case1_5101299025871 | 1 | identity | 1 |

### Pass / Fail Criteria

| # | Check | Query | Expected |
|---|---|---|---|
| 1 | Run succeeds | — | Result = SUCCESS |
| 2 | Matching log row count | `SELECT COUNT(*) FROM chv_matching_log_v2 WHERE DATA_DT='2015-12-28'` | 15 |
| 3 | All rules PASSED | `SELECT COUNT(*) FROM chv_matching_log_v2 WHERE DATA_DT='2015-12-28' AND RESULT<>'PASSED'` | 0 |
| 4 | BKEY table row count | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2015-12-28'` | 6 |
| 5 | All share BKEY=1 | `SELECT COUNT(DISTINCT BKEY) FROM chv_table_bkey_v2 WHERE DATA_DT='2015-12-28'` | 1 |
| 6 | SUBJECT correct | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2015-12-28' AND SUBJECT<>'identity'` | 0 |

---

## TC-02: TIER Cascade — Single BKEY per Record

### Requirement
When a record matches in Tier 1 (SOURCE_MOTOR → TRUST_SOURCE), it must not receive a second BKEY from Tier 2 (SOURCE_MOTOR → SOURCE_MOTOR). Each record must appear in the bkey table with exactly one BKEY, regardless of how many tiers it could potentially match in. Records with no match in any tier receive SUBJECT = `default`; all matched records receive SUBJECT = `identity`.

### Input

**Command:**
```
python3 tests/run_chv_v2.py match SOURCE_MOTOR 2026-01-05
```

**Source table:** `viriyah_cdqm_poc.silver.source_motor` — DATA_DT = `2026-01-05`

| Ident_card | Prefix | Fname | Lname | DoB |
|---|---|---|---|---|
| NULL | คุณ | กิตติภพ | รุ่งเรืองวัฒนะ | 2006-10-16 |
| 1234567880099 | คุณ | วิริยะ | สุขุมพันธ์ | 1994-10-16 |
| 1234567880099 | คุณ | วิริยะ | สุขุมพันธ์ | 1994-10-16 |
| 1234567880099 | นาวาอากาศ | วิริยะ | สุขุมพันธ์ | NULL |
| 1234567880100 | คุณ | วิริ | พันธุ์ | 1991-01-16 |
| 1234567880100 | คุณ | วิริ | พันธุ์ | 1991-01-16 |
| 1234567880100 | นาย | วิริ | พันธุ | 1991-10-16 |
| 1234567880100 | นาย | วิริ | พันธุ์ | 1994-10-16 |
| 1234567889999 | ดอกเตอร์ | นาตาชา | โรมา | 2025-01-16 |
| **3500900059524** | **นาย** | **วิริยะ** | **สุขุมพันธ์** | **1994-10-16** |

**Matching table:** `viriyah_cdqm_poc.silver.trust_source` — DATA_DT = `2026-01-05`

| id_card | Prefix | Fname | Lname | DoB |
|---|---|---|---|---|
| 44444444444 | คุณ | กิตติภพ | รุ่งเรืองวัฒนะ | 2006-10-16 |
| 1234567880099 | คุณ | วิริยะ | สุขุมพันธ์ | 1994-10-16 |
| 1234567880100 | คุณ | วิริ | พันธุ์ | 1991-10-16 |

### Expected Result

**Run result:** SUCCESS

**`chv_table_bkey_v2`** — 12 rows, 4 BKEYs
```sql
SELECT TABLE, KEY, BKEY, SUBJECT
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = '2026-01-05'
ORDER BY BKEY, TABLE, KEY
```

| BKEY | TABLE | SUBJECT | Rows |
|---|---|---|---|
| 1 | source_motor + trust_source | identity | 2 |
| 2 | source_motor + trust_source | identity | 5 |
| 3 | source_motor + trust_source | identity | 4 |
| 4 | source_motor only | **default** | 1 |

**Key check — `3500900059524` must have exactly 1 BKEY:**
```sql
SELECT KEY, BKEY, SUBJECT
FROM viriyah_cdqm_poc.silver.chv_table_bkey_v2
WHERE DATA_DT = '2026-01-05'
  AND TABLE = 'viriyah_cdqm_poc.silver.source_motor'
  AND KEY LIKE '3500900059524%'
```
Expected: 1 row, BKEY = 2, SUBJECT = identity

### Pass / Fail Criteria

| # | Check | Query | Expected |
|---|---|---|---|
| 1 | Run succeeds | — | Result = SUCCESS |
| 2 | BKEY table row count | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05'` | 12 |
| 3 | Number of distinct BKEYs | `SELECT COUNT(DISTINCT BKEY) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05'` | 4 |
| 4 | `3500900059524` has 1 BKEY only | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05' AND KEY LIKE '3500900059524%'` | 1 |
| 5 | No record has duplicate BKEY | `SELECT COUNT(*) FROM (SELECT KEY, TABLE, COUNT(DISTINCT BKEY) AS n FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05' GROUP BY KEY, TABLE HAVING n > 1)` | 0 |
| 6 | Unmatched record has SUBJECT=default | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05' AND KEY LIKE '1234567889999%' AND SUBJECT='default'` | 1 |
| 7 | Matched records have SUBJECT=identity | `SELECT COUNT(*) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05' AND SUBJECT<>'default' AND SUBJECT<>'identity'` | 0 |
| 8 | BKEY=2 contains both tables | `SELECT COUNT(DISTINCT TABLE) FROM chv_table_bkey_v2 WHERE DATA_DT='2026-01-05' AND BKEY=2` | 2 |

---

## TC-03: Full Pipeline (End-to-End)

### Requirement
The main orchestrator notebook must run pre-validation and matching end-to-end and produce correct bkey results identical to TC-01.

### Input

**Command:**
```
python3 tests/run_chv_v2.py main tc_case2_similarity 2015-12-28
python3 tests/run_chv_v2.py match tc_case2_similarity 2015-12-28
```

**Source table:** same as TC-01

### Expected Result

Same as TC-01 expected results. Pre-validation must have run first:
```sql
SELECT COUNT(*) FROM viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2
WHERE DATA_DT = '2015-12-28'
```
Expected: > 0 rows

### Pass / Fail Criteria

| # | Check | Expected |
|---|---|---|
| 1 | `main` step succeeds | Result = SUCCESS |
| 2 | `match` step succeeds | Result = SUCCESS |
| 3 | Pre-validation result exists | > 0 rows |
| 4–9 | Same as TC-01 checks 2–7 | Same as TC-01 |

---

## Test Execution Log

| TC | Test Case | Run Date | Tester | Result | Notes |
|---|---|---|---|---|---|
| TC-01 | Similarity Matching | | | PASS / FAIL | |
| TC-02 | TIER Cascade | | | PASS / FAIL | |
| TC-03 | Full Pipeline | | | PASS / FAIL | |

---

## Notes

- **Source data duplicate (TC-02):** `source_motor` has 1 duplicate row with the same key (`1234567880100,คุณ,วิริ,พันธุ์`). This is expected — bkey correctly stores 9 unique keys from 10 source rows. Not a bug.
- **Cluster:** Must be running before submitting. Contact tester lead if cluster is terminated.
