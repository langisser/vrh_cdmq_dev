# CLAUDE.md

## Critical Rules

- **NEVER commit `.databrickscfg`** — it contains credentials
- **No local test execution** — all notebooks run on Databricks cluster only
- **Auth method:** always `azure_cli` — config uses Azure CLI, no PAT token
- **Never modify `databricks_dev_local`** — raise issues on GitHub instead

## Environment Setup

```bash
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
```

## Key Paths

| What | Path |
|---|---|
| Workspace root | `/Workspace/Users/khachornpop@inteltion.com/vrh` |
| Match & Merge notebooks | `.../vrh/match_and_merge/` |
| Dedup notebooks | `.../vrh/dedup/` |
| Cluster ID | `0130-031624-0nmpnh8g` |
| Local notebooks | `notebooks/work/` |

## Tools Library (databricks_dev_local)

Always use these instead of raw SDK:

```python
from tools import run_notebook, get_run_cell_error
from tools import upload_to_workspace, download_from_workspace
from tools import get_run_cells
```

## Run Notebook Script Template

```python
# scripts/run_<name>.py
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error

result = run_notebook(
    notebook_path='/Workspace/Users/khachornpop@inteltion.com/vrh/<path>',
    params={'PARAMS': '<params>', 'ENV': 'dev'},
    cluster_id='0130-031624-0nmpnh8g',
    auth_method='azure_cli'
)
print(result['status'], result.get('error'))

if result['status'] == 'FAILED':
    for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
        print(e['summary'])
        print(e['error_detail'])
```

```bash
source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
python3 scripts/run_<name>.py
```

## Workspace CLI Commands

```bash
# List workspace
databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh

# Download notebook
databricks workspace export /Workspace/Users/khachornpop@inteltion.com/vrh/<path> \
  --format SOURCE > notebooks/work/<file>.py

# Upload notebook (prefer upload_dedup_pipeline.py instead)
databricks workspace import --file notebooks/work/<file>.py \
  --language PYTHON --format SOURCE --overwrite \
  /Workspace/Users/khachornpop@inteltion.com/vrh/<path>
```

## Run SQL on Cluster (debug)

```python
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()
spark.sql("SELECT ...")
```

## PARAMS Format

Delimiter: `^|` (caret-pipe). See `docs/job_run_spec.md` for full field reference.

| Notebook | PARAMS fields |
|---|---|
| `vrh_chv_pre_validation_v2` | `<source_table>^|<vld_result_table>^|<data_dt>^|<prcs_nm>^|1^|<prcs_nm>^|1` |
| `vrh_chv_match_v2` | `<source_table>^|<data_dt>^|<prcs_nm>^|1^|<prcs_nm>^|1` |
| `vrh_chv_dedup_v2` | `<source_table>^|<data_dt>^|<prcs_nm>^|1^|<prcs_nm>^|1` + `SOURCE_TABLE`, `TRUST_TABLE` params |

**PRCS_NM naming:**
- Pre-val: `EDP_PRE_VLD_V2_{TABLE_SUFFIX}`
- Match: `EDP_MATCHING_V2_{TABLE}_DATE_{yyyy-MM-dd}`
- Dedup: `EDP_DEDUP_{TABLE}_DATE_{yyyy-MM-dd}`

## Gotchas

- **Run order is mandatory:** pre_val (SOURCE_MOTOR) → pre_val (TRUST_SOURCE) → match → dedup
- **Pre-val config must exist before running:** check `CHV_CONFIG_PRE_VALIDATION_V2` has entries
- **`ENV` must be `'dev'`** — never blank or empty string
- **Dedup tables** use Liquid Clustering on `(bkey, id_card)` — do not add ZORDER or PARTITION BY
- **Column `rec_keyvalue`** is `ARRAY<STRING>` — not `policy_keys`, not `policy_no`

## Key Docs

| Doc | What's in it |
|---|---|
| `docs/job_run_spec.md` | Full run parameters, PARAMS format, devtest examples |
| `docs/design_chv_v2.md` | TIER+SUBJECT design, BKEY phases, Union-Find |
| `docs/execution_and_investigation_guide.md` | Investigation SQL queries, pipeline order |
| `docs/pending_decisions.md` | All resolved business decisions |
