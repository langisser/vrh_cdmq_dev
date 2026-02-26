# Pending Decisions & Known Issues
**Project:** vrh — CHV Match & Merge Pipeline
**Last Updated:** 2026-02-22
**Status:** All Issues Resolved ✅

---

## How to Use This Document

| Tag | Meaning |
|---|---|
| `[PENDING]` | ต้องการ decision จาก business user ก่อนแก้ code |
| `[KNOWN ISSUE]` | รู้ปัญหาแล้ว รอ prioritize / fix |
| `[CLARIFY]` | ต้องการ clarify requirement เพิ่มเติม |

---

## Issue 1 — Pre-Validation Gate Behavior `[RESOLVED ✅]`

**ค้นพบจาก:** SCN03 Null Key Gate test
**Confirmed:** 2026-02-22

### Decision

> **ใช้ Option B — Score-Based (column-level)**
> Pre-validation fail ที่ column ใด column หนึ่ง **ไม่** reject ทั้ง row
> column ที่ fail คิด weight = 0, record ยังเข้า matching ได้จาก column อื่น

### ผลต่อ Test Cases

- SCN03 verdict `FAIL` ต้องปรับ expected ใหม่ให้ตรงกับ Option B behavior
- ไม่ต้องแก้ code ใดๆ — current implementation ถูกต้องแล้ว

### Dev Impact

ไม่มี — behavior ปัจจุบันตรงกับ business requirement แล้ว

---

## Issue 2 — Levenshtein Threshold สำหรับ ID Card `[RESOLVED ✅]`

**ค้นพบจาก:** SCN03, SCN04, SCN05
**SCN04/05 Result:** FAIL
**Confirmed:** 2026-02-26

### Decision

> **ใช้ Option A — Exact match**
> `MAIN.Ident_card = MATCH.Id_card`
> เลขบัตรประชาชนต่างกัน 1 หลัก = คนละคน ไม่ใช่ typo

### Dev Impact

- แก้ `chv_config_matching_v2` rule 1, 6, 11 — เปลี่ยน `MATCH_CONDITION` จาก `levenshtein(...) <= 2` เป็น `= `
- QA ปรับ expected: testcase ที่ ID ต่างกัน 1-2 หลักแล้ว expect PASS → เปลี่ยนเป็น FAIL

### ปัญหา

ปัจจุบัน matching rule สำหรับ ID card ใช้:
```
levenshtein(MAIN.Ident_card, MATCH.Id_card) <= 2   weight 1.0
```

**ผลที่เห็น — คนละคนแต่ ID ต่างกัน 1-2 ตัวอักษร match กัน:**

| MAIN ID | MATCH ID | Levenshtein | ผล | ถูกต้อง? |
|---|---|---|---|---|
| `7300000000001` | `7300000000001` | 0 | PASS | ✅ |
| `7310000000001` | `7300000000001` | 1 | PASS | ❓ |
| `7990000000001` | `7300000000001` | 2 | PASS | ❌ คนละคน |
| `8410000000001` | `7400000000001` | 2 | PASS | ❌ คนละคน |

### ตัวเลือก

| Option | Rule | ผลกระทบ |
|---|---|---|
| A — Exact match | `MAIN.Ident_card = MATCH.Id_card` | ปลอดภัยที่สุด, ไม่มี false positive จาก ID |
| B — Levenshtein <= 1 | ลด threshold จาก 2 → 1 | ลด false positive แต่ยังเหลือบางกรณี (เช่น 7310... vs 7300...) |
| C — Levenshtein <= 2 | คงเดิม | false positive สูง |

**Dev recommendation: Option A (exact match)**

เหตุผล:
- เลขบัตรประชาชน 13 หลักออกโดยรัฐ — แต่ละหลักมีความหมาย (วันเกิด, จังหวัด, check digit)
- ต่างกัน 1 หลัก = คนละคน ไม่ใช่ typo
- ถ้าไม่มี ID card → ระบบยังสามารถ match ได้ผ่าน Fname(0.5) + Lname(0.5) = 1.0

### คำถามสำหรับ Business User

> **"ข้อมูลจาก source system มีโอกาสที่เลข ID พิมพ์ผิด 1-2 ตัวจากการ keyin มั้ย?"**

- ถ้า "ใช่, พิมพ์ผิดบ่อย" → อาจยอมรับ levenshtein <= 1 สำหรับ ID
- ถ้า "ไม่, ข้อมูล ID มาจากระบบ (scan/API)" → ควรใช้ exact match

**Dev impact:** แก้ `chv_config_matching_v2` rule 1, 6, 11 (MATCH_CONDITION column)

---

## Issue 3 — Unmatched MATCHING table records `[RESOLVED ✅]`

**ค้นพบจาก:** design_chv_v2.md section 9 (Known Limitations)
**Confirmed:** 2026-02-26

### Decision

> **TRUST_SOURCE รันเป็น MAIN table ด้วย**
> ปัญหานี้ถูก handle โดย pipeline เอง — TRUST_SOURCE records ที่ไม่มีใคร match มาหา
> จะได้ BKEY เมื่อรัน pipeline โดยให้ TRUST_SOURCE เป็น MAIN table

### Dev Impact

- Config รอบ TRUST_SOURCE as MAIN: `chv_config_matching_v2` rules 11–15 (MAIN=TRUST_SOURCE → MATCHING=SOURCE_MOTOR, TIER=1) มีอยู่แล้ว
- ไม่ต้องเพิ่ม logic ใดๆ ใน notebook

---

## Issue 4 — Source-to-Source Match (8410 ↔ 8420) `[RESOLVED ✅]`

**ค้นพบจาก:** SCN04 Duplicate Source test
**Confirmed:** 2026-02-22

### Decision

> **ยอมรับได้** — `8410000000001` (แยก คน1) และ `8420000000001` (แยก คน2) ถูก match เป็นคนเดียวกัน
> เพราะ Fname + Lname เป็น matching factor ที่ถูก design ไว้แล้ว weight รวม >= 1 ถือว่า match ถูกต้องตาม business rule

### Dev Impact

ไม่มี — behavior ปัจจุบันถูกต้องแล้ว SCN04 = PASS WITH REMARK (acceptable)

---

## Issue 5 — Dedup Output Tables (Post-BKEY Merge Layer) `[CONFIRMED ✅]`

**ที่มา:** New requirement discussion — 2026-02-25
**Confirmed:** 2026-02-26
**Status:** Confirmed — พร้อม implement

### Requirement

หลังจาก pipeline สร้าง `chv_table_bkey_v2` แล้ว ต้องการ **output layer** ที่ group ข้อมูลจาก source_motor **และ** trust_source โดยใช้ BKEY เป็น grouping key

### Output Tables (5 ตาราง)

| Table | GROUP BY columns | Aggregate |
|---|---|---|
| `dedup_customer_name` | bkey, id_card, fname, lname, prefix | MAX(update_date), collect_list(policy_id) AS rec_keyvalue |
| `dedup_province` | bkey, id_card, area, district, postcode, province | MAX(update_date), collect_list(policy_id) AS rec_keyvalue |
| `dedup_gender` | bkey, id_card, gender, birth_date | MAX(update_date), collect_list(policy_id) AS rec_keyvalue |
| `dedup_email` | bkey, id_card, email | MAX(update_date), collect_list(policy_id) AS rec_keyvalue |
| `dedup_phone` | bkey, id_card, phone_no | MAX(update_date), collect_list(policy_id) AS rec_keyvalue |

### Design Decisions Confirmed

- **Scope:** ครอบคลุมทั้ง source_motor และ trust_source
- **Key columns:** source_motor → `policy_id`, trust_source → `id_card`
- **Dedup rule:** GROUP BY all non-date columns → MAX(update_date) per group
- **rec_keyvalue:** `array<string>` via `collect_list(policy_id)` — Spark/Databricks native
- **Null handling:** filter NULL สำหรับ email และ phone_no (ไม่เอา null rows)
- **Same BKEY can appear in multiple rows** if attribute values differ within the group (e.g. different prefix, different lname variant)
- **Output schema:** silver (ทั้ง bkey table และ dedup tables)
- **Trigger:** on-demand (ไม่ auto-run หลัง match pipeline)
- **Notebook:** สร้าง notebook ใหม่ `vrh_chv_dedup_v2` แยกจาก `vrh_chv_match_v2`

### Multi-Source Config (ที่ confirm แล้ว)

Pipeline รัน 2 รอบแยกกัน priority scope อยู่ใน MAIN table เดียวกัน:

```
MAIN=source_motor:
  priority 1: source_motor -> trust_source
  priority 2: source_motor -> source_motor

MAIN=trust_source:
  priority 1: trust_source -> source_motor
```

**Priority ไม่ขัดแย้งกัน** เพราะ priority ซ้ำกันได้ถ้า MAIN table คนละตัว

### SQL Pattern (source_motor)

```sql
SELECT
    b.bkey,
    s.id_card,
    s.fname,
    s.lname,
    s.prefix,
    MAX(s.update_date)           AS update_date,
    collect_list(s.policy_id)    AS rec_keyvalue
FROM chv_table_bkey_v2 b
JOIN source_motor s
  ON b.key = s.policy_id
 AND b.table = 'viriyah_cdqm_poc.silver.source_motor'
GROUP BY b.bkey, s.id_card, s.fname, s.lname, s.prefix
```

### Open Questions — Resolved

1. **trust_source join key:** ใช้ `config_pk` ของแต่ละ table — ไม่ hard-code `id_card` แต่ dynamic ตาม config (trust_source ปัจจุบันใช้ `id_card`)
2. **trust_source update_date:** trust_source ไม่มี `update_date` — ใช้ `CAST(NULL AS timestamp)` แทนเมื่อ UNION กับ source_motor เพื่อให้ schema ตรงกัน
3. **Null handling:** filter NULL สำหรับ email และ phone_no — ไม่เอา null rows เข้า dedup tables

---

## Summary — Decision Needed

| # | Issue | Owner | Priority | Status |
|---|---|---|---|---|
| 1 | Pre-validation gate: strict vs score-based | Business User | High | ✅ Resolved — Option B |
| 2 | ID card matching: exact vs levenshtein | Business User | High | ✅ Resolved — Option A (exact match) |
| 3 | Unmatched MATCHING table BKEY | Business User | Medium | ✅ Resolved — TRUST_SOURCE runs as MAIN |
| 4 | Source-to-source match (8410 ↔ 8420) | Business User | Medium | ✅ Resolved — ยอมรับได้ |
| 5 | Dedup output tables (post-BKEY merge layer) | Business User | Medium | ✅ Confirmed — all open questions resolved |

---

*Document created: 2026-02-22*
*Last updated: 2026-02-26 — All issues resolved. Issue 2: exact match, Issue 3: TRUST_SOURCE runs as MAIN*
*Based on analysis of SCN03, SCN04, SCN05 test results and code review of `vrh_chv_match_v2.py`*
