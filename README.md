# vrh_cdmq_dev

Databricks development project for VRH Customer Data Match & Merge (CDMQ) pipeline.

## Workspace

`/Workspace/Users/khachornpop@inteltion.com/vrh`

## Quick Start

### 1. Configure Credentials

Edit `.databrickscfg` (DO NOT COMMIT):

```ini
[DEFAULT]
host = https://adb-7405612978007880.0.azuredatabricks.net
token = dapi_your_token_here
cluster_id = your-cluster-id
```

### 2. Set Environment

```bash
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
```

### 3. Test Connection

```bash
databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh
```

### 4. Development Cycle

```bash
# 1. Edit notebook locally
#    notebooks/work/match_and_merge/vrh_chv_match_v2.py

# 2. Upload to workspace
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
databricks workspace import --file notebooks/work/match_and_merge/<notebook>.py \
  --language PYTHON --format SOURCE --overwrite \
  /Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge/<notebook>

# 3. Run on cluster via CLI
databricks jobs submit --json '{
  "run_name": "my_run",
  "tasks": [{"task_key": "main", "existing_cluster_id": "<cluster_id>",
    "notebook_task": {"notebook_path": "/Workspace/.../notebook",
      "base_parameters": {"PARAMS": "...", "ENV": "dev"}}}],
  "timeout_seconds": 600
}'
```

### 5. Run SQL on cluster (DatabricksSession)

```python
source /home/khaw/ClaudeCode/databricks_dev_local/venv/bin/activate

python3 - <<'EOF'
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql("SELECT ...").show()
EOF
```

## Pipeline Overview

```
vrh_chv_pre_validation_v2   ← run for MAIN table (source_motor)
vrh_chv_pre_validation_v2   ← run for MATCHING table (trust_source)
vrh_chv_match_v2            ← BKEY assignment (Union-Find)
vrh_chv_dedup_v2            ← deduplication output tables
```

**PARAMS format:**
```
# pre_validation_v2 (7 params):
<table>^|<vld_result_table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# match_v2 (6 params):
<table>^|<data_dt>^|<prcs_nm>^|<ld_id>^|<updt_prcs_nm>^|<updt_ld_id>

# ENV: ใช้ 'dev' เสมอ
```

## Key Tables

| Table | Schema | Description |
|---|---|---|
| `source_motor_devtest` | silver | Devtest motor source data |
| `trust_source_devtest` | silver | Devtest trust source data |
| `chv_table_bkey_v2` | silver | BKEY assignment output |
| `chv_matching_result_v2` | control_fw | Matched pairs |
| `chv_matching_log_v2` | control_fw | Per-rule match log |
| `chv_pre_validation_result_v2` | control_fw | Pre-validation results |

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
│       │   ├── vrh_chv_dedup_v2.py
│       │   └── insert_scripts/
│       └── unittest/
├── scripts/                          # Pipeline + utility scripts
│   ├── run_dedup_pipeline.sh
│   ├── step0_cleanup_devtest.sql
│   └── ...
├── docs/                             # Design docs
│   ├── design_chv_v2.md
│   ├── execution_and_investigation_guide.md
│   ├── technical_practices.md
│   └── pending_decisions.md
├── source/                           # Source data files
│   └── Sample_Data_PoC_Match_Merge.xlsx
├── tests/                            # Test runners
└── venv/                             # Python venv (not used for pipeline runs)
```

## Documentation

- **CLAUDE.md**: Framework guidance and project-specific workflows
- **docs/design_chv_v2.md**: Match & Merge design doc
- **docs/execution_and_investigation_guide.md**: Step-by-step execution + investigation queries
- **docs/technical_practices.md**: Technical practices (TP-001/002/003)
- **docs/pending_decisions.md**: Open design decisions log
