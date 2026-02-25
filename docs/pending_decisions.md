# Pending Decisions & Known Issues
**Project:** vrh — CHV Match & Merge Pipeline
**Last Updated:** 2026-02-22
**Status:** Issue 1 & 4 Resolved, Issue 2 & 3 Pending

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

## Issue 2 — Levenshtein Threshold สำหรับ ID Card `[PENDING]`

**ค้นพบจาก:** SCN03, SCN04, SCN05
**SCN04/05 Result:** FAIL

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

## Issue 3 — Unmatched MATCHING table records `[KNOWN ISSUE]`

**ค้นพบจาก:** design_chv_v2.md section 9 (Known Limitations)

### ปัญหา

`not_pass_post` query (records ที่ไม่ match ใคร) cover เฉพาะ **MAIN table** เท่านั้น
Records ใน MATCHING table ที่ไม่มีใคร match มาหา จะ **ไม่ได้รับ BKEY** ในรอบนี้

**ตัวอย่าง:**
ถ้า TRUST_SOURCE มี record `44444444444` ที่ไม่มีใครใน SOURCE_MOTOR match มาหาเลย
→ `44444444444` จะไม่มี BKEY จนกว่าจะรัน pipeline โดยให้ TRUST_SOURCE เป็น MAIN table

### คำถามสำหรับ Business User

> **"TRUST_SOURCE จะถูกรันเป็น MAIN table ด้วยมั้ย? หรือทำหน้าที่เป็น reference table เท่านั้น?"**

- ถ้า TRUST_SOURCE รันเป็น MAIN ด้วย → ปัญหานี้ถูก handle โดย pipeline เอง
- ถ้า TRUST_SOURCE เป็น reference เท่านั้น → ต้องเพิ่ม logic assign BKEY ให้ unmatched MATCHING records

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

## Issue 5 — Dedup Output Tables (Post-BKEY Merge Layer) `[DRAFT 📝]`

**ที่มา:** New requirement discussion — 2026-02-25
**Status:** Draft — ยังไม่ confirm, รอ business approval

### Requirement

หลังจาก pipeline สร้าง `chv_table_bkey_v2` แล้ว ต้องการ **output layer** ที่ group ข้อมูลจาก source_motor โดยใช้ BKEY เป็น grouping key

### Output Tables (5 ตาราง)

| Table | GROUP BY columns | Aggregate |
|---|---|---|
| `dedup_customer_name` | bkey, id_card, fname, lname, prefix | MAX(update_date), collect_list(policy_no) |
| `dedup_province` | bkey, id_card, area, district, postcode, province | MAX(update_date), collect_list(policy_no) |
| `dedup_gender` | bkey, id_card, gender, birth_date | MAX(update_date), collect_list(policy_no) |
| `dedup_email` | bkey, id_card, email | MAX(update_date), collect_list(policy_no) |
| `dedup_phone` | bkey, id_card, phone | MAX(update_date), collect_list(policy_no) |

### Design Decisions Confirmed

- **Key columns:** source_motor → `policy_no`, trust_source → `id_card`
- **Dedup rule:** GROUP BY all non-date columns → MAX(update_date) per group
- **policy_keys:** `array<string>` via `collect_list(policy_no)` — Spark/Databricks native
- **Null handling:** email/phone tables — include only non-null values
- **Same BKEY can appear in multiple rows** if attribute values differ within the group (e.g. different prefix, different lname variant)
- **trust_source rows** contribute BKEY linkage only — not included as source rows in dedup output (no update_date column)

### SQL Pattern

```sql
SELECT
    b.bkey,
    s.id_card,
    s.fname,
    s.lname,
    s.prefix,
    MAX(s.update_date)          AS update_date,
    collect_list(s.policy_no)   AS policy_keys
FROM chv_table_bkey_v2 b
JOIN source_motor s
  ON b.key = s.policy_no
 AND b.table = 'viriyah_cdqm_poc.silver.source_motor'
GROUP BY b.bkey, s.id_card, s.fname, s.lname, s.prefix
```

### Open Questions (รอ confirm)

1. **Scope:** ครอบคลุม source_motor เท่านั้น หรือรวม trust_source ด้วย?
2. **Null rows:** dedup_email / dedup_phone — include null email/phone rows หรือ exclude?
3. **Table location:** output tables อยู่ใน schema ไหน? (silver? gold?)
4. **Trigger:** รันหลัง `vrh_chv_match_v2` เสร็จอัตโนมัติ หรือ on-demand?
5. **Notebook:** สร้าง notebook ใหม่ `vrh_chv_dedup_v2` หรือ append ใน `vrh_chv_match_v2`?

---

## Summary — Decision Needed

| # | Issue | Owner | Priority | Status |
|---|---|---|---|---|
| 1 | Pre-validation gate: strict vs score-based | Business User | High | ✅ Resolved — Option B |
| 2 | ID card matching: exact vs levenshtein | Business User | High | ⏳ Pending |
| 3 | Unmatched MATCHING table BKEY | Business User | Medium | ⏳ Pending |
| 4 | Source-to-source match (8410 ↔ 8420) | Business User | Medium | ✅ Resolved — ยอมรับได้ |
| 5 | Dedup output tables (post-BKEY merge layer) | Business User | Medium | 📝 Draft — ยังไม่ confirm |

---

*Document created: 2026-02-22*
*Last updated: 2026-02-25 — added Issue 5 (dedup output tables)*
*Based on analysis of SCN03, SCN04, SCN05 test results and code review of `vrh_chv_match_v2.py`*
