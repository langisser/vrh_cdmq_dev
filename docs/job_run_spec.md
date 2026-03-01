# Job Run Specification — CHV Match & Merge Pipeline (v2)

**Project:** vrh
**Pipeline:** Customer Hub (CHV) Match & Merge v2
**Updated:** 2026-02-27

---

## 0. Environment

| Item | Value |
|---|---|
| **Workspace URL** | `https://adb-7405612978007880.0.azuredatabricks.net/` |
| **Cluster ID** | `0130-031624-0nmpnh8g` |
| **Match & Merge notebook path** | `/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge` |
| **Dedup notebook path** | `/Workspace/Users/khachornpop@inteltion.com/vrh/dedup` |
| **Dedup scripts (local)** | `scripts/upload_dedup_pipeline.py`, `scripts/run_dedup_full_rebuild.py` |
| **Config file (local)** | `<repo-root>/.databrickscfg` |
| **Source table** | `viriyah_cdqm_poc.silver.source_motor_devtest` |
| **Trust source table** | `viriyah_cdqm_poc.silver.trust_source_devtest` |

---

## 1. Job Overview

| # | Job Name | Notebook | Purpose |
|---|---|---|---|
| 1 | `PRE_VAL_MOTOR` | `vrh_chv_pre_validation_v2` | Pre-validate SOURCE_MOTOR records before matching |
| 2 | `PRE_VAL_TRUST` | `vrh_chv_pre_validation_v2` | Pre-validate TRUST_SOURCE records before matching |
| 3 | `MATCH_MOTOR` | `vrh_chv_match_v2` | Run matching + BKEY assignment for SOURCE_MOTOR |
| 4a | `DEDUP_NAME` | `dedup_customer_name` | Dedup name+prefix per BKEY — path: `/vrh/dedup` |
| 4b | `DEDUP_PROVINCE` | `dedup_province` | Dedup address per BKEY |
| 4c | `DEDUP_GENDER` | `dedup_gender` | Dedup gender+birth_date per BKEY |
| 4d | `DEDUP_EMAIL` | `dedup_email` | Dedup email per BKEY |
| 4e | `DEDUP_PHONE` | `dedup_phone` | Dedup phone_no per BKEY |

> **Run order:** Job 1 → Job 2 → Job 3 → Jobs 4a–4e (4a–4e can run in any order after Job 3)

---

## 2. Job Parameters

### Notebook Parameters (passed as `base_parameters`)

| Parameter | Type | Description | Example |
|---|---|---|---|
| `PARAMS` | STRING | Pipe-delimited run parameters (see PARAMS format below) | `viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01^|1^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01^|1` |
| `ENV` | STRING | Environment flag — always `dev` | `dev` |
| `SOURCE_TABLE` | STRING | **(DEDUP job only)** Full qualified motor source table name | `viriyah_cdqm_poc.silver.source_motor_devtest` |
| `TRUST_TABLE` | STRING | **(DEDUP job only)** Full qualified trust source table name | `viriyah_cdqm_poc.silver.trust_source_devtest` |

### PARAMS Field Format

Delimiter: `^|` (caret-pipe)

#### Pre-validation (7 fields)
```
<source_table>^|<vld_result_table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>
```

| Position | Field | Type | Example |
|---|---|---|---|
| 1 | `source_table` | STRING | `viriyah_cdqm_poc.silver.source_motor_devtest` |
| 2 | `vld_result_table` | STRING | `viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2` |
| 3 | `data_dt` | DATE (yyyy-MM-dd) | `2025-01-01` |
| 4 | `prcs_nm` | STRING | `EDP_PRE_VLD_MOTOR_DEVTEST` |
| 5 | `ld_id` | INT | `1` |
| 6 | `updt_prcs_nm` | STRING | `EDP_PRE_VLD_MOTOR_DEVTEST` |
| 7 | `updt_ld_id` | INT | `1` |

#### Match v2 (6 fields)
```
<source_table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>
```

| Position | Field | Type | Example |
|---|---|---|---|
| 1 | `source_table` | STRING | `viriyah_cdqm_poc.silver.source_motor_devtest` |
| 2 | `data_dt` | DATE (yyyy-MM-dd) | `2025-01-01` |
| 3 | `prcs_nm` | STRING | `EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01` |
| 4 | `ld_id` | INT | `1` |
| 5 | `updt_prcs_nm` | STRING | `EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01` |
| 6 | `updt_ld_id` | INT | `1` |

#### Dedup notebooks (4 fields)
```
<source_table>^|<data_dt>^|<prcs_nm>^|<ld_id>
```

| Position | Field | Type | Example |
|---|---|---|---|
| 1 | `source_table` | STRING | `viriyah_cdqm_poc.silver.source_motor_devtest` |
| 2 | `data_dt` | DATE (yyyy-MM-dd) | `2025-01-01` |
| 3 | `prcs_nm` | STRING | `EDP_DEDUP_CUSTOMER_NAME` |
| 4 | `ld_id` | INT | `1` |

> No `SOURCE_TABLE` / `TRUST_TABLE` widget params needed — individual dedup notebooks
> read directly from `chv_table_bkey_v2` using the `source_table` param.

### PRCS_NM Naming Convention

| Job | Pattern | Example |
|---|---|---|
| Pre-val motor | `EDP_PRE_VLD_V2_<TABLE_SUFFIX>` | `EDP_PRE_VLD_V2_SOURCE_MOTOR_DEVTEST` |
| Pre-val trust | `EDP_PRE_VLD_V2_<TABLE_SUFFIX>` | `EDP_PRE_VLD_V2_TRUST_SOURCE_DEVTEST` |
| Match | `EDP_MATCHING_V2_<TABLE_SUFFIX>_DATE_<yyyy-MM-dd>` | `EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01` |
| Dedup | `EDP_DEDUP_<NOTEBOOK_SUFFIX>` | `EDP_DEDUP_CUSTOMER_NAME`, `EDP_DEDUP_PHONE` |

---

## 3. Source Tables

### 3.1 source_motor_devtest
**Full name:** `viriyah_cdqm_poc.silver.source_motor_devtest`
**Schema:** `silver`
**PK:** `policy_id`
**Partition:** `DATA_DT` (STRING, yyyy-MM-dd)
**Rows (devtest):** ~1,442

| Column | Type | Description | Used in Matching |
|---|---|---|---|
| `policy_id` | STRING | Policy ID — PK | PK (key) |
| `id_card` | STRING | National ID card number | Yes — exact match (Rule 1,2,4,5) |
| `fname` | STRING | First name | Yes — Levenshtein similarity |
| `lname` | STRING | Last name | Yes — Levenshtein similarity |
| `gender` | STRING | Gender | No |
| `table` | STRING | Source sub-table name (e.g. `applicants`) | No |
| `birth_date` | STRING | Date of birth | Yes — exact match |
| `prefix` | STRING | Name prefix / title | No |
| `area` | STRING | Sub-district | No |
| `district` | STRING | District | No |
| `province` | STRING | Province | No |
| `postcode` | STRING | Postal code | No |
| `email` | STRING | Email address | No |
| `phone_no` | STRING | Phone number | No |
| `insert_date` | TIMESTAMP | Record insert timestamp | No |
| `update_date` | TIMESTAMP | Record update timestamp | No |
| `DATA_DT` | STRING | Partition date (yyyy-MM-dd) | Filter — must match run date |

### 3.2 trust_source_devtest
**Full name:** `viriyah_cdqm_poc.silver.trust_source_devtest`
**Schema:** `silver`
**PK:** `id_card`
**Partition:** `DATA_DT` (STRING, yyyy-MM-dd)
**Rows (devtest):** ~21

| Column | Type | Description | Used in Matching |
|---|---|---|---|
| `id_card` | STRING | National ID card — PK | Yes — exact match |
| `fname` | STRING | First name | Yes — Levenshtein similarity |
| `lname` | STRING | Last name | Yes — Levenshtein similarity |
| `gender` | STRING | Gender | No |
| `table` | STRING | Source sub-table name | No |
| `birth_date` | STRING | Date of birth | Yes — exact match |
| `prefix` | STRING | Name prefix / title | No |
| `area` | STRING | Sub-district | No |
| `district` | STRING | District | No |
| `province` | STRING | Province | No |
| `postcode` | STRING | Postal code | No |
| `email` | STRING | Email address | No |
| `phone_no` | STRING | Phone number | No |
| `insert_date` | TIMESTAMP | Record insert timestamp | No |
| `update_date` | TIMESTAMP | Record update timestamp | No |
| `DATA_DT` | STRING | Partition date (yyyy-MM-dd) | Filter — must match run date |

---

## 4. Output Tables

### 4.1 Intermediate / Log Tables (control_fw schema)

| Table | Full Name | Key Columns | Written By |
|---|---|---|---|
| Pre-val result | `viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2` | `TABLE, KEY, RULES, COLUMN, VALUE, RESULT, DATA_DT, PRCS_NM` | `vrh_chv_pre_validation_v2` |
| Matching log | `viriyah_cdqm_poc.control_fw.chv_matching_log_v2` | `MAIN_TABLE, MATCHING_TABLE, MATCHING_RULES, KEY_MAIN, KEY_MATCH, RESULT, SUBJECT, DATA_DT, PRCS_NM` | `vrh_chv_match_v2` |
| Matching result | `viriyah_cdqm_poc.control_fw.chv_matching_result_v2` | `MAIN_TABLE, MATCHING_TABLE, KEY_MAIN, KEY_MATCH, SUBJECT, DATA_DT, PRCS_NM` | `vrh_chv_match_v2` |

### 4.2 BKEY Table (silver schema)

| Table | Full Name | Key Columns |
|---|---|---|
| BKEY | `viriyah_cdqm_poc.silver.chv_table_bkey_v2` | `TABLE, KEY, BKEY (INT), SUBJECT, DATA_DT, PRCS_NM` |

### 4.3 Dedup Output Tables (silver schema)

| Table | Full Name | Key Columns |
|---|---|---|
| Customer name | `viriyah_cdqm_poc.silver.dedup_customer_name` | `bkey, id_card, fname, lname, prefix, update_date, rec_keyvalue` |
| Province/address | `viriyah_cdqm_poc.silver.dedup_province` | `bkey, id_card, area, district, postcode, province, update_date, rec_keyvalue` |
| Gender/DOB | `viriyah_cdqm_poc.silver.dedup_gender` | `bkey, id_card, gender, birth_date, update_date, rec_keyvalue` |
| Email | `viriyah_cdqm_poc.silver.dedup_email` | `bkey, id_card, email, update_date, rec_keyvalue` |
| Phone | `viriyah_cdqm_poc.silver.dedup_phone` | `bkey, id_card, phone_no, update_date, rec_keyvalue` |
| Name variant report | `viriyah_cdqm_poc.silver.dedup_name_variant_report` | `bkey, id_card, fname_raw, lname_raw, fname_normalized, lname_normalized, reason_code` |

---

## 5. Config Tables (Required Before First Run)

These must be populated before running the pipeline for any new table.

| Config Table | Full Name | Purpose |
|---|---|---|
| PK config | `viriyah_cdqm_poc.control_fw.chv_config_pk_v2` | Defines PK column per source table |
| Matching rules | `viriyah_cdqm_poc.control_fw.chv_config_matching_v2` | Rules with TIER + SUBJECT (rules 1–15 prod, 31–39 devtest) |
| Pre-val config | `viriyah_cdqm_poc.control_fw.chv_config_pre_validation_v2` | CHECK_NULL rules per column per table |
| Pre-val check | `viriyah_cdqm_poc.control_fw.chv_config_check_pre_validation_v2` | Column-level check per MATCHING_RULES |
| General params | `viriyah_cdqm_poc.control_fw.chv_param_general_v2` | DATE lag setting per table |

---

## 6. Full Run Example (devtest, DATA_DT = 2025-01-01)

### Job 1 — Pre-validation: SOURCE_MOTOR
```
Notebook : vrh_chv_pre_validation_v2
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2^|2025-01-01^|EDP_PRE_VLD_V2_SOURCE_MOTOR_DEVTEST^|1^|EDP_PRE_VLD_V2_SOURCE_MOTOR_DEVTEST^|1
ENV      : dev
```

### Job 2 — Pre-validation: TRUST_SOURCE
```
Notebook : vrh_chv_pre_validation_v2
PARAMS   : viriyah_cdqm_poc.silver.trust_source_devtest^|viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2^|2025-01-01^|EDP_PRE_VLD_V2_TRUST_SOURCE_DEVTEST^|1^|EDP_PRE_VLD_V2_TRUST_SOURCE_DEVTEST^|1
ENV      : dev
```

### Job 3 — Match: SOURCE_MOTOR
```
Notebook : vrh_chv_match_v2
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01^|1^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_2025-01-01^|1
ENV      : dev
```

### Jobs 4a–4e — Dedup (run each notebook separately, any order)
```
Notebook : dedup/dedup_customer_name
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_CUSTOMER_NAME^|1
ENV      : dev

Notebook : dedup/dedup_province
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_PROVINCE^|1
ENV      : dev

Notebook : dedup/dedup_gender
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_GENDER^|1
ENV      : dev

Notebook : dedup/dedup_email
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_EMAIL^|1
ENV      : dev

Notebook : dedup/dedup_phone
PARAMS   : viriyah_cdqm_poc.silver.source_motor_devtest^|2025-01-01^|EDP_DEDUP_PHONE^|1
ENV      : dev
```

> Each notebook uses MERGE pattern — safe to re-run (idempotent).
> For bulk rebuild: `python3 scripts/run_dedup_full_rebuild.py`

---

*Generated from design docs and scripts — 2026-02-27*
*Updated 2026-03-02 — dedup layer changed from vrh_chv_dedup_v2 to individual notebooks; rec_keyvalue column name corrected; PARAMS format updated to 4 fields*
