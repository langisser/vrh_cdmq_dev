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
export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh/.databrickscfg
# Or simply:
source setup_env.sh
```

## Project Structure

```
vrh/
├── .databrickscfg                    # Databricks connection config (DO NOT COMMIT)
├── .gitignore                        # Git ignore rules
├── .claudeignore                     # Claude Code ignore rules
├── setup_env.sh                      # Quick environment setup script
├── CLAUDE.md                         # This file
├── README.md                         # Project documentation
├── notebooks/
│   ├── work/                         # Active notebooks for development
│   │   ├── TEMPLATE_main_notebook.py
│   │   └── TEMPLATE_common_functions.py
│   └── mirror/                       # Original workspace downloads (reference)
├── tests/                            # Local test runners
│   └── TEMPLATE_run_notebook.py
└── venv/                             # Python virtual environment
```

## Development Workflow

### 1. Download notebook from workspace
```bash
source setup_env.sh
databricks workspace export /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook> \
  --format SOURCE > notebooks/work/<notebook>.py
```

### 2. Edit locally
Edit files in `notebooks/work/` using your IDE.

### 3. Test locally
```bash
source venv/bin/activate
python3 tests/run_<notebook>.py
```

### 4. Upload back to workspace
```bash
source setup_env.sh
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
- **Always test locally** before uploading to workspace

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

## Setup Checklist

- [ ] Fill in `.databrickscfg` with host, token, cluster_id
- [ ] Run `source setup_env.sh` to configure environment
- [ ] Test connection: `databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh`
- [ ] Activate venv: `source venv/bin/activate`
- [ ] Download notebooks: `databricks workspace export_dir ...`
- [ ] Create test runners for each notebook
- [ ] Update this CLAUDE.md with project-specific workflows
