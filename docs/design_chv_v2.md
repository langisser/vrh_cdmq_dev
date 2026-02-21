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

Result: the same person ends up with 2 BKEYs in `chv_table_bkey` — data inconsistency.

### 1.2 No Grouping Dimension (SUBJECT)

The original pipeline has no way to label *why* two records were grouped together. All matches produce a single undifferentiated BKEY with no context about the matching relationship (e.g. identity match vs. address match).

---

## 2. Solution

### 2.1 TIER — Cascade Priority

Rules in `chv_config_matching` are assigned a **TIER** (integer). The matching engine processes tiers in ascending order:

1. **Tier 1** runs against all eligible records.
2. Records that produce a PASSED result in Tier 1 are collected into a `matched_keys` set.
3. **Tier 2** runs only against records **not in** `matched_keys` — they are excluded via a `NOT IN` filter on the main sub-query.
4. This continues for each subsequent tier.

**Effect:** A record can only be matched in **one tier**. It is impossible for the same record to appear in both Tier 1 and Tier 2 results, eliminating the dual-BKEY problem.

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

**Key design decision:** SOURCE_MOTOR → TRUST_SOURCE is Tier 1 (higher priority). SOURCE_MOTOR self-match is Tier 2. Any SOURCE_MOTOR record that finds a match in TRUST_SOURCE will never be re-matched against other SOURCE_MOTOR records.

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
matched_keys = {}   # keys matched in previous tiers

for each tier in [1, 2, ...]:
    tier_config = config rows where TIER = current tier

    build SQL:
        MAIN sub-query  → source table records (filtered by pre-validation PASSED)
                        → if tier > 1: add WHERE KEY NOT IN (matched_keys)
        MATCH sub-query → matching table records (filtered by pre-validation PASSED)
        JOIN            → on matching condition (levenshtein, exact, etc.)

    run SQL → tier_df

    collect PASSED keys from tier_df → add to matched_keys

accumulate all tier_df → write to chv_matching_log_v2
```

The `NOT IN (matched_keys)` filter is injected per rule inside the MAIN sub-query, ensuring records matched in Tier 1 cannot appear as MAIN in Tier 2.

---

## 6. BKEY Generation — Key Changes

### SUBJECT propagation

`gen_bkey_filter` (pairs with no existing BKEY) carries the SUBJECT column from `chv_matching_result_v2`. A `subject_map` dict `(table, key) → SUBJECT` is built from this data and used when constructing the final `bkey_df`.

Unmatched records (`not_pass_post`) receive `SUBJECT = 'default'` via `lit('default')`.

### Case sensitivity fix

`table_s` and `bkey_dict` keys are normalised to lowercase (`.lower()`). The `find_related_records` call and `not_pass_post` TABLE literal also use `.lower()` to ensure consistent lookup. Without this, tables with mixed-case names (e.g. `SOURCE_MOTOR`) would fail graph traversal silently, producing an empty bkey table.

---

## 7. Comparison with v1

| Behaviour | v1 (original) | v2 (TIER + SUBJECT) |
|---|---|---|
| Matching | All rules in one SQL pass | Tier-by-tier, cascading exclusion |
| Dual-BKEY | Possible when record matches multiple rule groups | Eliminated — each record matched in at most 1 tier |
| SUBJECT | Not present | Carried from config → bkey |
| Tables | Original | `_v2` suffix (parallel, non-destructive) |
| Config | `chv_config_matching` | `chv_config_matching_v2` + TIER + SUBJECT columns |

---

## 8. Known Limitations

- **Source duplicates:** If the source table has duplicate rows (same PK columns), bkey generation deduplicates them. The count of bkey rows may be less than the count of source rows. This is expected behaviour — it reflects data quality in the source, not a pipeline bug.
- **SUBJECT is single-value per record:** A record can only carry one SUBJECT (from the tier/rule it matched in). Multi-subject grouping is not supported in this design.
- **`not_pass_post` only covers MAIN table:** Unmatched records from the MATCHING table are not independently assigned a BKEY in the current run — they would receive a BKEY when that table is run as MAIN in its own pipeline execution.
