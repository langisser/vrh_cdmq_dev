# CHV Match & Merge v2 — Design Document
**Feature:** TIER + SUBJECT support
**Author:** VRH Dev Team
**Date:** 2026-02-21

---

## 1. Problem

### 1.1 Dual-BKEY Problem

In the original pipeline (v1), a record is matched against **all rules simultaneously** in a single SQL pass. When a record satisfies rules from multiple source-to-target pairs (e.g. SOURCE_MOTOR → TRUST_SOURCE **and** SOURCE_MOTOR → SOURCE_MOTOR), it produces two separate match pairs in the result, and the BKEY generation assigns it **two different BKEYs**.

**Example:**

Record `3500900059524` (SOURCE_MOTOR, Fname=วิริยะ, Lname=สุขุมพันธ์, DoB=1994-10-16):
- Matches TRUST_SOURCE `1234567880099` via name+DoB → BKEY = A
- Also matches SOURCE_MOTOR `1234567880099` rows via name+DoB → BKEY = B

Result: the same person ends up with 2 BKEYs in `chv_table_bkey_v2` — data inconsistency.

### 1.2 No Grouping Dimension (SUBJECT)

The original pipeline has no way to label *why* two records were grouped together. All matches produce a single undifferentiated BKEY with no context about the matching relationship (e.g. identity match vs. address match).

---

## 2. Solution

### 2.1 TIER — Cascade Priority

Rules in `chv_config_matching` are assigned a **TIER** (integer). The matching engine processes tiers in ascending order:

1. **Tier 1** runs against all eligible records.
2. **Tier 2** runs against all eligible records (same as Tier 1 — no SQL-level exclusion).
3. This continues for each subsequent tier.
4. After all tiers complete, **Union-Find** merges all matched pairs into connected components — ensuring each record receives exactly one BKEY regardless of how many tiers it appeared in.

**Effect:** A record may appear in multiple tiers, but Union-Find guarantees all transitively-connected keys share exactly one BKEY — eliminating the dual-BKEY problem at the BKEY assignment phase.

### 2.2 SUBJECT — Grouping Label

A `SUBJECT` column is added to `chv_config_matching_v2`. The value (e.g. `identity`) is carried through the entire pipeline:

```
chv_config_matching_v2.SUBJECT
    → matching SQL (CAST as literal string)
    → chv_matching_log_v2.SUBJECT
    → chv_matching_result_v2.SUBJECT
    → chv_table_bkey_v2.SUBJECT
```

Records that do not match in any tier (unmatched) receive `SUBJECT = 'default'`.

---

## 3. Config Rules (chv_config_matching_v2)

15 rules total, same conditions as v1, with TIER and SUBJECT added:

| Rules | MAIN_TABLE | MATCHING_TABLE | TIER | SUBJECT |
|---|---|---|---|---|
| 1–5 | SOURCE_MOTOR | TRUST_SOURCE | 1 | identity |
| 6–10 | SOURCE_MOTOR | SOURCE_MOTOR | 2 | identity |
| 11–15 | TRUST_SOURCE | SOURCE_MOTOR | 1 | identity |

**Key design decision:** SOURCE_MOTOR → TRUST_SOURCE is Tier 1 (higher priority). SOURCE_MOTOR self-match is Tier 2. All tiers run against the full eligible record set — records may appear in both tiers. Union-Find (Phase 3) merges all connected keys into one BKEY regardless of how many tiers they appeared in.

---

## 4. Architecture

### 4.1 New Tables (`_v2` suffix)

All tables run in parallel with originals — no existing data is modified.

| Table | Schema | Changes vs original |
|---|---|---|
| `chv_config_matching_v2` | control_fw | + TIER INT, SUBJECT STRING |
| `chv_config_pk_v2` | control_fw | same schema |
| `chv_config_pre_validation_v2` | control_fw | same schema |
| `chv_config_check_pre_validation_v2` | control_fw | same schema |
| `chv_config_function_v2` | control_fw | same schema |
| `chv_param_general_v2` | control_fw | same schema |
| `chv_pre_validation_result_v2` | control_fw | same schema |
| `chv_matching_log_v2` | control_fw | + SUBJECT STRING |
| `chv_matching_result_v2` | control_fw | + SUBJECT STRING |
| `chv_table_bkey_v2` | silver | + SUBJECT STRING |

### 4.2 New Notebooks

| Notebook | Based on | Changes |
|---|---|---|
| `vrh_chv_main_v2` | `vrh_chv_main` | All table/notebook refs → `_v2` |
| `vrh_chv_pre_validation_v2` | `vrh_chv_pre_validation` | All config table refs → `_v2` |
| `vrh_chv_match_v2` | `vrh_chv_match` | TIER loop + SUBJECT column + all table refs → `_v2` |
| `insert_scripts/ddl_v2_tables` | — | CREATE TABLE for all 10 `_v2` tables |
| `insert_scripts/chv_config_matching_v2` | — | INSERT 15 rules with TIER + SUBJECT |
| `insert_scripts/chv_config_copy_to_v2` | — | COPY data from original tables → `_v2` |

### 4.3 Pipeline Flow

```
vrh_chv_main_v2
    │
    ├─► vrh_chv_pre_validation_v2
    │       reads : chv_config_pk_v2
    │               chv_config_pre_validation_v2
    │               chv_config_check_pre_validation_v2
    │       writes: chv_pre_validation_result_v2
    │
    └─► vrh_chv_match_v2
            reads : chv_config_matching_v2  (ORDER BY TIER)
                    chv_config_pk_v2
                    chv_pre_validation_result_v2
                    chv_param_general_v2
            writes: chv_matching_log_v2
                    chv_matching_result_v2
                    chv_table_bkey_v2
```

---

## 5. TIER Loop — Implementation Detail

```
for each tier in [1, 2, ...]:
    tier_config = config rows where TIER = current tier

    build SQL:
        MAIN sub-query  → source table records (filtered by pre-validation PASSED)
        MATCH sub-query → matching table records (filtered by pre-validation PASSED)
        JOIN            → on matching condition (levenshtein, exact, etc.)

    run SQL → tier_df

accumulate all tier_df → write to chv_matching_log_v2

Phase 3 (BKEY assignment):
    collect all matched pairs from all tiers
    run Union-Find → one BKEY per connected component
    anti-join not_pass_post against gen_bkey_rs to prevent overlap
```

All tiers run independently against the full eligible record set. Deduplication is handled downstream by Union-Find — not by SQL-level exclusion between tiers.

---

## 6. BKEY Generation — Implementation Detail

### Phase Overview

BKEY generation in `vrh_chv_match_v2` has 4 phases:

| Phase | What it does |
|---|---|
| **Phase 1** | Look up existing BKEYs from previous runs (`bkey_exists_rs`) — keys already in `chv_table_bkey_v2` |
| **Phase 2** | Recursive graph traversal — follow matched pairs transitively to find all connected keys (`gen_bkey`) |
| **Phase 3** | Union-Find BKEY assignment — assign one BKEY integer per connected component (`gen_bkey_rs`) |
| **Phase 4** | Build final output — loop `bkey_dict`, attach SUBJECT, union all paths, write to `chv_table_bkey_v2` |

---

### Phase 3: Union-Find Algorithm

All matched pairs from all tiers are collected and unioned into **connected components**. One BKEY integer is assigned per component. All transitively-connected keys share exactly one BKEY.

```
for each matched pair (MAIN_TABLE, KEY_MAIN) ↔ (MATCHING_TABLE, KEY_MATCH):
    union(node_main, node_match)

for each node in parent:
    assign bkey = component_bkey[find(node)]
```

**Path compression** is used for O(α) lookup performance.

---

### Phase 4: Output Priority (Three-Way Dedup)

Three output paths are merged with explicit priority to ensure each KEY gets exactly one BKEY:

```
Priority (highest → lowest):
  1. bkey_exists_rs  — keys already assigned in previous runs
  2. gen_bkey_rs     — keys assigned in this run (via Union-Find)
  3. not_pass_post   — unmatched keys (self-BKEY)
```

Implementation uses left-anti joins:
```python
gen_bkey_rs_deduped   = gen_bkey_rs.join(bkey_exists_rs,   on=[KEY,TABLE], how="left_anti")
not_pass_post_deduped = not_pass_post.join(gen_bkey_rs,    on=[KEY,TABLE], how="left_anti")
                                     .join(bkey_exists_rs, on=[KEY,TABLE], how="left_anti")

final = gen_bkey_rs_deduped ∪ bkey_exists_rs ∪ not_pass_post_deduped
```

---

### SUBJECT Propagation

SUBJECT is carried from config → matching log → matching result → BKEY table via a `subject_map`:

```python
subject_map = {(MAIN_TABLE, KEY_MAIN): SUBJECT,
               (MATCHING_TABLE, KEY_MATCH): SUBJECT, ...}
```

Built from `gen_bkey_filter` (matched pairs). When attaching SUBJECT in Phase 4:
- Keys found in `subject_map` → use matched SUBJECT (e.g. `'identity'`)
- Keys not found (unmatched, `not_pass_post`) → `SUBJECT = 'default'`

Unmatched records with `SUBJECT = 'default'` are interim state — they will receive a proper SUBJECT when they eventually match another record in a future pipeline run (or when their table runs as MAIN).

---

### Case Sensitivity

All `table_s` set members and `bkey_dict` keys are normalised to `.lower()`. The `find_related_records()` call also uses `.lower()`. Without this, mixed-case table names (e.g. `SOURCE_MOTOR` vs `source_motor`) cause silent lookup failure and missing BKEY entries.

---

## 7. Comparison with v1

| Behaviour | v1 (original) | v2 (TIER + SUBJECT) |
|---|---|---|
| Matching | All rules in one SQL pass | Tier-by-tier (all tiers run independently, no SQL exclusion between tiers) |
| Dual-BKEY | Possible when record matches multiple rule groups | Eliminated — by Union-Find (all transitively-connected keys share one BKEY) |
| SUBJECT | Not present | Carried from config → bkey |
| Tables | Original | `_v2` suffix (parallel, non-destructive) |
| Config | `chv_config_matching` | `chv_config_matching_v2` + TIER + SUBJECT columns |

---

## 8. Pre-Validation Behavior — Design Discussion

### 8.1 Current Implementation

Pre-validation runs before matching and writes results to `chv_pre_validation_result_v2` (PASSED / FAILED per column per record). The matching notebook then joins against this table using **OR logic** across validated columns:

```sql
INNER JOIN CHV_PRE_VALIDATION_RESULT_V2 VLD_RESULT
  ON ... AND VLD_RESULT.RESULT = 'PASSED'
WHERE ...
AND (
  (VLD_RESULT.RULES = 'CHECK_NULL' AND COLUMN = 'Ident_card' AND RESULT = 'PASSED')
  OR
  (VLD_RESULT.RULES = 'CHECK_NULL' AND COLUMN = 'Fname'      AND RESULT = 'PASSED')
)
```

**Effect:** A record enters matching if **any one** validated column passes — even if another column (e.g. `Ident_card`) is NULL/FAILED.

---

### 8.2 Confirmed Design Decision — Score-Based (Option B) ✅

> **Business confirmed (2026-02-22):** Pre-validation operates at **column level**, not row level.
> A pre-validation failure on one column does NOT reject the entire record from matching.

**Rule:** Failed columns contribute **zero score** to the weight total. The record can still match if the cumulative score from other (passing) columns meets the threshold (`>= 1`).

**Example:**
| Record | Ident_card | Fname | Lname | ID weight | Name weight | Total | Result |
|---|---|---|---|---|---|---|---|
| NULL id, valid name | NULL → 0 | กิตติ → 0.5 | บุญดี → 0.5 | 0 | 1.0 | 1.0 | MATCH ✅ |
| Valid id, NULL fname | 7310... → 0.6 | NULL → 0 | บุญดี → 0.5 | 0.6 | 0.5 | 1.1 | MATCH ✅ |

This is **intended behavior** — records with partial data can still be identified if remaining fields provide sufficient evidence.

---

### 8.3 Impact on SCN03 (Null Key Gate)

SCN03 test verdict `FAIL` was based on a strict gate expectation (Option A). With the confirmed Option B behavior, the matching output of SCN03 is **correct by design**:
- Records with NULL fields but sufficient matching score → correctly grouped under same BKEY
- Pre-validation log still records the FAILED columns for data quality tracking
- SCN03 test case expectations should be revised to align with Option B

---

## 9. Known Limitations

- **Source duplicates:** If the source table has duplicate rows (same PK columns), bkey generation deduplicates them. The count of bkey rows may be less than the count of source rows. This is expected behaviour — it reflects data quality in the source, not a pipeline bug.
- **SUBJECT is single-value per record:** A record can only carry one SUBJECT (from the tier/rule it matched in). Multi-subject grouping is not supported in this design.
- **`not_pass_post` only covers MAIN table:** Unmatched records from the MATCHING table are not independently assigned a BKEY in the current run — they would receive a BKEY when that table is run as MAIN in its own pipeline execution.
