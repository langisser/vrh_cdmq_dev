# Match & Merge — Design Analysis & Improvement Plan

**Project:** vrh (Viriyah CDQ Master Data)
**Date:** 2026-02-20
**Scope:** `chv_config_matching` / `vrh_chv_match.py`

---

## 1. สรุปปัญหาที่พบ (Current Issues)

### 1.1 ไม่มี Cascade — Rules ทุกตัวรันพร้อมกัน

ปัจจุบัน config ที่ตั้งไว้:

```
source_motor → trust_source       (rules 1–5)
source_motor → source_motor       (rules 6–10)
```

หลายคนเข้าใจว่าระบบจะ **ลองจับคู่กับ trust_source ก่อน แล้วถ้าไม่เจอค่อยไปจับคู่กับ source_motor** แต่ความจริงคือ:

> **Rules ทุกตัวถูก execute พร้อมกันใน UNION ALL SQL เดียว**
> ไม่มี cascade ไม่มีลำดับก่อนหลัง

```
vrh_chv_match.py : line 155–231
─────────────────────────────────────────────
for idx, i in config.iterrows():
    ...build SQL...

sql = UNION ALL ของทุก rule   ← รวมทุก tier ในคราวเดียว
spark.sql(sql)
```

---

### 1.2 Record ที่ Match ทั้งสองตาราง จะได้ 2 BKEY

เมื่อ `source_motor_A` match ทั้ง `trust_source_X` และ `source_motor_B`:

**ใน `df` (temp_post join bkey table) จะมี 2 rows:**

| KEY_MAIN | MATCHING_TABLE | MATCH_BKEY | MAIN_BKEY |
|---|---|---|---|
| sm_A | trust_source | 99 | NULL |
| sm_A | source_motor | NULL | NULL |

**Filter อิสระ 2 ตัว (line 447, 462) ไม่ได้ exclude กัน:**

```python
# line 447 — รับ row แรก
bkey_exists = df.filter(col("MATCH_BKEY").isNotNull() & col("MAIN_BKEY").isNull())
# → sm_A ได้ BKEY = 99

# line 462 — รับ row สอง (sm_A ยังอยู่ที่นี่ด้วย!)
gen_bkey = df.filter(df["MATCH_BKEY"].isNull() & df["MAIN_BKEY"].isNull())
# → sm_A ได้ BKEY = 150 (ใหม่)
```

**ผลลัพธ์ใน `chv_table_bkey`:**

| KEY | TABLE | BKEY |
|---|---|---|
| sm_A | source_motor | 99 |
| sm_A | source_motor | **150** ← duplicate! |

---

### 1.3 ไม่มี SUBJECT dimension — แยกไม่ออกว่า BKEY ไหนเป็นของ relationship ไหน

ถ้าต้องการเพิ่ม matching อีกชุด เช่น:
```
source_motor → trust_source_address   (address subject)
```

BKEY ที่ได้จะปะปนกันใน `chv_table_bkey` โดยไม่มีทางบอกได้ว่า BKEY ไหน = identity, BKEY ไหน = address

---

## 2. Workaround ปัจจุบัน (ยังไม่แก้ code)

ใช้ **TABLE column** เป็น implicit grouping:

```sql
-- หา identity BKEY ของ sm_A (linked กับ trust_source)
SELECT b.BKEY
FROM chv_table_bkey b
WHERE b.KEY = 'sm_A'
  AND b.TABLE = 'source_motor'
  AND EXISTS (
      SELECT 1 FROM chv_table_bkey b2
      WHERE b2.BKEY = b.BKEY
        AND b2.TABLE = 'trust_source'
  )
```

> ใช้ได้ในระยะสั้น แต่ query ซับซ้อน และเสี่ยงสับสนเมื่อ subject เพิ่มขึ้น

---

## 3. Objective ของการแก้ไข

| # | Objective | ปัญหาที่แก้ |
|---|---|---|
| 1 | **TIER support** — จัดลำดับการ match ก่อน-หลัง | Record ที่ match tier สูงกว่าจะไม่ถูกประมวลผลใน tier ต่ำกว่า → ไม่ได้ 2 BKEY |
| 2 | **SUBJECT support** — ระบุว่า BKEY ชุดนี้เป็นของ subject ไหน | แยก identity / address / อื่นๆ ออกจากกันได้ชัดเจน |
| 3 | **BKEY generate ครั้งเดียวหลังทุก TIER จบ** | ป้องกัน race condition ระหว่าง tier |

---

## 4. สิ่งที่ต้องเปลี่ยน (Change Details)

### 4.1 Schema Changes — SMALL

#### `chv_config_matching` — เพิ่ม 2 columns

```sql
ALTER TABLE viriyah_cdqm_poc.control_fw.chv_config_matching
  ADD COLUMN TIER    INT          DEFAULT 1,
  ADD COLUMN SUBJECT VARCHAR(100) DEFAULT 'default';
```

| Column | Type | หมายความว่า |
|---|---|---|
| `TIER` | INT | ลำดับการ match (1 = ก่อน, 2 = ถัดไป) |
| `SUBJECT` | VARCHAR | ชื่อ subject ของ BKEY เช่น `identity`, `address` |

#### `chv_table_bkey` — เพิ่ม 1 column

```sql
ALTER TABLE viriyah_cdqm_poc.silver.chv_table_bkey
  ADD COLUMN SUBJECT VARCHAR(100) DEFAULT 'default';
```

#### `chv_matching_result` — เพิ่ม 1 column

```sql
ALTER TABLE viriyah_cdqm_poc.control_fw.chv_matching_result
  ADD COLUMN SUBJECT VARCHAR(100) DEFAULT 'default';
```

---

### 4.2 Config Insert Scripts — SMALL

เพิ่มค่า TIER และ SUBJECT ทุกครั้งที่ insert config:

```sql
-- ก่อน
INSERT INTO chv_config_matching
  (MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, MATCH_CONDITION, GROUP, WEIGHT, ACT_F)
VALUES
  (1, 'source_motor', 'trust_source', 'MAIN.Ident_card = MATCH.Id_card', 1, 1, 1);

-- หลัง
INSERT INTO chv_config_matching
  (MATCHING_RULES, MAIN_TABLE, MATCHING_TABLE, MATCH_CONDITION, GROUP, WEIGHT, ACT_F, TIER, SUBJECT)
VALUES
  (1, 'source_motor', 'trust_source',         'MAIN.Ident_card = MATCH.Id_card', 1, 1, 1, 1, 'identity'),
  (6, 'source_motor', 'source_motor',          'MAIN.Ident_card = MATCH.Ident_card', 1, 1, 1, 2, 'identity'),
  (11, 'source_motor', 'trust_source_address', 'MAIN.addr_id = MATCH.addr_id',    1, 1, 1, 1, 'address');
  --                                                                                    ↑         ↑
  --                                                                                  TIER     SUBJECT
```

---

### 4.3 `vrh_chv_match.py` — MODERATE

#### จุดที่ต้องเปลี่ยน

| Line | การเปลี่ยนแปลง | ขนาด |
|---|---|---|
| 87 | เพิ่ม `ORDER BY TIER, MATCHING_RULES` | เล็กมาก |
| 152–231 | **Wrap matching loop ใน tier loop + filter unmatched pool** | ใหญ่สุด |
| 244 | เพิ่ม SUBJECT column ใน insert chv_matching_log | เล็ก |
| 366 | เพิ่ม SUBJECT column ใน insert chv_matching_result | เล็ก |
| 447–462 | เพิ่ม SUBJECT filter ใน bkey_exists / gen_bkey | เล็ก |
| 664–668 | เพิ่ม SUBJECT column ใน final_df union | เล็ก |

#### โครงสร้าง Logic ใหม่ (concept)

```
ปัจจุบัน:
──────────────────────────────────────
load config
↓
loop all rules → build one big SQL
↓
execute SQL (ทุก tier พร้อมกัน)
↓
assign bkeys

ใหม่:
──────────────────────────────────────
load config (ORDER BY TIER)
↓
unmatched_pool = all source keys

for each TIER:
  ├── filter config for this tier
  ├── build SQL (same logic, same structure)
  ├── execute SQL
  ├── collect matched pairs (ยังไม่ assign bkey)
  └── unmatched_pool = unmatched_pool - matched_this_tier
↓
assign bkeys from ALL tiers at once  ← moved here
↓
remaining unmatched_pool → singleton bkeys
```

> **Core SQL build logic ไม่เปลี่ยน** — แค่ย้ายเข้าไปอยู่ใน tier loop

---

## 5. สรุปขนาดงาน

```
┌─────────────────────────────┬──────────┬────────────────────────────────────┐
│ Component                   │ ขนาด     │ หมายเหตุ                           │
├─────────────────────────────┼──────────┼────────────────────────────────────┤
│ ALTER TABLE (4 tables)      │ เล็ก     │ เพิ่ม column ไม่กระทบ data เดิม   │
│ Config insert scripts       │ เล็ก     │ เพิ่มค่า TIER, SUBJECT ต่อ row    │
│ vrh_chv_match.py — SUBJECT  │ เล็ก     │ carry column ผ่านทุก step          │
│ vrh_chv_match.py — TIER     │ ปานกลาง  │ restructure matching loop          │
│ SUBJECT + TIER รวม          │ ปานกลาง  │ ไม่ใช่งานใหญ่ logic หลักไม่เปลี่ยน│
└─────────────────────────────┴──────────┴────────────────────────────────────┘
```

---

## 6. ประโยชน์ที่ได้รับ

| ก่อนแก้ | หลังแก้ |
|---|---|
| Record match 2 ตาราง → ได้ 2 BKEY | Record match 2 ตาราง → ได้ 1 BKEY (tier แรกที่ match) |
| ไม่รู้ว่า BKEY ไหนเป็น subject ไหน | SUBJECT column บอกชัดเจน |
| Query หา BKEY ซับซ้อน | `WHERE SUBJECT = 'identity'` |
| เพิ่ม source ใหม่ยาก | เพิ่ม TIER ใหม่ใน config ได้เลย |

---

*Document generated from design discussion — 2026-02-20*
