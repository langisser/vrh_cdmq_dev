# CLAUDE.md

This file provides guidance to Claude Code when working with this Databricks development project.

## Project Overview

**Project:** vrh
**Workspace:** `/Workspace/Users/khachornpop@inteltion.com/vrh`
**Framework:** Based on the Databricks local development framework from `databricks_dev_local`

## Databricks Configuration

### Connection Settings
- **Config File:** `.databrickscfg` (in project root — never commit this file)
- **Workspace Path:** `/Workspace/Users/khachornpop@inteltion.com/vrh`

Always set the config file path before Databricks operations:
```bash
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
```

> **Note:** `setup_env.sh` มีอยู่ในโปรเจคแต่ยังชี้ path เก่า (`vrh/`) — ใช้คำสั่ง export ตรงๆ ข้างบนแทนครับ

## Project Structure

```
vrh_cdmq_dev/
├── .databrickscfg                    # Databricks connection config (DO NOT COMMIT)
├── .gitignore
├── setup_env.sh                      # ⚠️ path ยังชี้ vrh/ เก่า — ใช้ export ตรงๆ แทน
├── CLAUDE.md                         # This file
├── README.md
├── requirements.txt
├── notebooks/
│   └── work/                         # Active notebooks for development
│       ├── match_and_merge/          # Main pipeline notebooks
│       │   ├── vrh_chv_main_v2.py
│       │   ├── vrh_chv_pre_validation_v2.py
│       │   ├── vrh_chv_match_v2.py   # ← main notebook (BKEY assignment)
│       │   ├── vrh_chv_dedup_v2.py
│       │   └── insert_scripts/       # DDL + config insert notebooks
│       └── unittest/                 # Unit test notebooks
│           ├── dedup/
│           └── tc1/
├── scripts/                          # Pipeline runner scripts
│   ├── run_dedup_pipeline.sh         # Full dedup pipeline runner
│   ├── step0_cleanup_devtest.sql     # Pre-run cleanup SQL (TRUNCATE + DELETE config)
│   ├── run_ddl_source_devtest.py
│   ├── run_insert_source_devtest.py
│   └── ...
├── docs/                             # Design docs
│   ├── design_chv_v2.md
│   ├── execution_and_investigation_guide.md
│   ├── technical_practices.md
│   ├── pending_decisions.md
│   └── ...
├── source/                           # Source data files
│   └── Sample_Data_PoC_Match_Merge.xlsx
├── tests/                            # Local test runners
│   └── run_chv_v2.py
└── venv/                             # Python virtual environment
```

## Development Workflow

### 1. Download notebook from workspace
```bash
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
databricks workspace export /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook> \
  --format SOURCE > notebooks/work/<notebook>.py
```

### 2. Edit locally
Edit files in `notebooks/work/` using your IDE.

### 3. Run on cluster (actual workflow)
ไม่ได้ test locally ผ่าน venv — รันบน cluster โดยตรงผ่าน DatabricksSession หรือ jobs submit

**วิธีที่ 1 — รัน notebook via WorkspaceClient (แนะนำสำหรับ pipeline run):**
เขียน parameters ลงไฟล์ Python แล้วรันไฟล์นั้น เพื่อไม่ต้อง approve command ยาวๆ ทุกครั้ง:
```python
# scripts/run_<notebook>_<params>.py
import sys
sys.path.insert(0, '/home/khaw/ClaudeCode/databricks_dev_local')
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, RunTask

CLUSTER_ID   = '<cluster_id>'
NOTEBOOK     = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge/<notebook>'
PARAMS_STR   = '<table>^|<data_dt>^|<prcs_nm>^|1^|<prcs_nm>^|1'

w = WorkspaceClient()
run = w.jobs.submit(run_name='run', tasks=[RunTask(
    task_key='t1',
    existing_cluster_id=CLUSTER_ID,
    notebook_task=NotebookTask(notebook_path=NOTEBOOK,
                               base_parameters={'PARAMS': PARAMS_STR, 'ENV': 'dev'})
)]).result()
print(run.state.result_state)
```
```bash
source /home/khaw/ClaudeCode/databricks_dev_local/venv/bin/activate
python3 scripts/run_<notebook>_<params>.py
```

**วิธีที่ 2 — รัน SQL โดยตรงบน cluster (สำหรับ investigation/debug):**
```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql("SELECT ...")
```

### 4. Upload back to workspace
```bash
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
databricks workspace import --file notebooks/work/<notebook>.py \
  --language PYTHON --format SOURCE --overwrite \
  /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook>
```

## Using Templates

### Creating a new notebook
```bash
cp notebooks/work/TEMPLATE_main_notebook.py notebooks/work/my_notebook.py
cp tests/TEMPLATE_run_notebook.py tests/run_my_notebook.py
# Then customize both files for your use case
```

### Key template customizations
- **Main notebook**: Update parameters, business logic, and helper imports
- **Common functions**: Add project-specific helper functions
- **Test runner**: Update `NOTEBOOK_NAME`, `HELPER_NOTEBOOK`, and `NOTEBOOK_PARAMS`

## Important Notes

- **Magic commands** (`# MAGIC`, `# COMMAND ----------`, `# DBTITLE`) are stripped during local execution
- **`safe_notebook_exit()`** works in both workspace and local environments
- **Version control**: Keep previous notebook versions (e.g., `notebook_v1.py`, `v2.py`)
- **venv สำหรับ DatabricksSession / WorkspaceClient:** `/home/khaw/ClaudeCode/databricks_dev_local/venv`
- **Run scripts:** เขียน parameters ลงไฟล์ใน `scripts/` แล้วรัน Python file แทนการพิมพ์ command ยาวๆ โดยตรง

## Common Commands

```bash
# Test Databricks connection
source setup_env.sh
databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh

# Download all notebooks from workspace
databricks workspace export_dir /Workspace/Users/khachornpop@inteltion.com/vrh \
  notebooks/mirror --format SOURCE

# List Databricks jobs
databricks jobs list
```

## Troubleshooting

1. **Auth errors**: Run `source setup_env.sh` to export `DATABRICKS_CONFIG_FILE`
2. **Import errors**: Run `source venv/bin/activate` before test scripts
3. **Cluster errors**: Verify `cluster_id` is set in `.databrickscfg`
4. **Parameter errors**: Check parameter names match job definition (case-sensitive)

