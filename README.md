# vrh_cdmq_dev

Databricks development project for VRH Customer Data Match & Merge (CDMQ) pipeline.

## Workspace

`/Workspace/Users/khachornpop@inteltion.com/vrh`

## Quick Start

### 1. Configure Credentials

Copy `.databrickscfg.example` → `.databrickscfg` (DO NOT COMMIT) and fill in:

```ini
[DEFAULT]
host       = https://adb-7405612978007880.0.azuredatabricks.net
azure_client_id     = ...
azure_tenant_id     = ...
azure_client_secret = ...
cluster_id = 0130-031624-0nmpnh8g
```

> Auth uses Azure CLI — no PAT token. Run `az login` before using.

### 2. Set Environment

```bash
export DATABRICKS_CONFIG_FILE=<repo-root>/.databrickscfg
source <repo-root>/venv/bin/activate
```

### 3. Test Connection

```bash
databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh
```

### 4. Development Cycle

```bash
# 1. Edit notebook locally
#    notebooks/work/match_and_merge/vrh_chv_match_v2.py

# 2. Upload all dedup notebooks (preferred)
python3 scripts/upload_dedup_pipeline.py

# 3. Or upload single notebook
databricks workspace import --file notebooks/work/<path>.py \
  --language PYTHON --format SOURCE --overwrite \
  /Workspace/Users/khachornpop@inteltion.com/vrh/<path>
```

### 5. Run SQL on Cluster

```python
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '<repo-root>/.databrickscfg'
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql("SELECT ...").show()
```

> Requires `databricks-connect==17.3.*` (must match cluster DBR 17.3)

## Pipeline Overview

```
[1] vrh_chv_pre_validation_v2   ← run for MAIN table (source_motor)
[2] vrh_chv_pre_validation_v2   ← run for MATCHING table (trust_source)
[3] vrh_chv_match_v2            ← BKEY assignment (Union-Find)
[4] dedup_customer_name  ┐
    dedup_province       │ ← individual dedup notebooks (per-table, per data_dt)
    dedup_gender         │
    dedup_email          │
    dedup_phone          ┘
```

**PARAMS format:**
```
# pre_validation_v2 (7 params):
<table>^|<vld_result_table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# match_v2 (5 params):
<table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# dedup notebooks (4 params):
<table>^|<data_dt>^|<prcs_nm>^|<ld_id>

# ENV: ใช้ 'dev' เสมอ
```

## Key Tables

| Table | Schema | Description |
|---|---|---|
| `source_motor_devtest` | silver | Devtest motor source data (1442 rows) |
| `trust_source_devtest` | silver | Devtest trust source data (21 rows) |
| `chv_table_bkey_v2` | silver | BKEY assignment output |
| `chv_matching_result_v2` | control_fw | Matched pairs |
| `chv_matching_log_v2` | control_fw | Per-rule match log |
| `chv_pre_validation_result_v2` | control_fw | Pre-validation results |
| `dedup_customer_name` | silver | Dedup: name + prefix per BKEY |
| `dedup_province` | silver | Dedup: address per BKEY |
| `dedup_gender` | silver | Dedup: gender + birth_date per BKEY |
| `dedup_email` | silver | Dedup: email per BKEY |
| `dedup_phone` | silver | Dedup: phone_no per BKEY |
| `dedup_name_variant_report` | silver | Thai unicode name variant report |

## Project Structure

```
vrh_cdmq_dev/
├── .databrickscfg                    # Credentials (DO NOT COMMIT)
├── CLAUDE.md                         # Claude Code guidance
├── README.md                         # This file
├── requirements.txt
├── notebooks/
│   └── work/
│       ├── match_and_merge/          # Main pipeline notebooks
│       │   ├── vrh_chv_main_v2.py
│       │   ├── vrh_chv_pre_validation_v2.py
│       │   ├── vrh_chv_match_v2.py
│       │   └── insert_scripts/
│       ├── dedup/                    # Dedup output notebooks
│       │   ├── ddl_dedup_tables.py
│       │   ├── dedup_customer_name.py
│       │   ├── dedup_province.py
│       │   ├── dedup_gender.py
│       │   ├── dedup_email.py
│       │   ├── dedup_phone.py
│       │   └── dedup_name_variant_report.py
│       └── unittest/
├── scripts/                          # Pipeline + utility scripts
│   ├── upload_dedup_pipeline.py      # Upload all dedup notebooks to workspace
│   └── run_dedup_full_rebuild.py     # DROP + full rebuild of dedup tables
├── docs/                             # Design docs
│   ├── design_chv_v2.md
│   ├── execution_and_investigation_guide.md
│   ├── job_run_spec.md
│   ├── technical_practices.md
│   └── pending_decisions.md
├── source/                           # Source data files
│   └── Sample_Data_PoC_Match_Merge.xlsx
├── tests/                            # Test runners
└── venv/                             # Python venv (do not use for notebook runs)
```

## Documentation

- **CLAUDE.md**: Framework guidance, gotchas, and project-specific workflows
- **docs/design_chv_v2.md**: Match & Merge design doc (TIER+SUBJECT, BKEY, Union-Find)
- **docs/execution_and_investigation_guide.md**: Step-by-step execution + investigation queries
- **docs/job_run_spec.md**: Full PARAMS format and devtest examples
- **docs/technical_practices.md**: Technical practices (TP-001/002/003)
- **docs/pending_decisions.md**: Business decisions log (all resolved)
