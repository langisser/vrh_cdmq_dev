# vrh

Databricks project for local development and testing.

## Workspace

`/Workspace/Users/khachornpop@inteltion.com/vrh`

## Quick Start

### 1. Configure Credentials

Edit `.databrickscfg` with your Databricks workspace details:

```ini
[DEFAULT]
host = https://your-workspace.cloud.databricks.com
token = dapi_your_token_here
cluster_id = your-cluster-id
```

### 2. Set Up Environment

```bash
# Set up Databricks environment
source setup_env.sh

# Activate Python virtual environment
source venv/bin/activate
```

### 3. Test Connection

```bash
databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh
```

### 4. Download Notebooks

```bash
databricks workspace export_dir /Workspace/Users/khachornpop@inteltion.com/vrh \
  notebooks/mirror --format SOURCE
```

### 5. Development Cycle

```bash
# Download → Edit → Test → Upload

# Download
databricks workspace export /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook> \
  --format SOURCE > notebooks/work/<notebook>.py

# Test locally
python3 tests/run_<notebook>.py

# Upload
databricks workspace import --file notebooks/work/<notebook>.py \
  --language PYTHON --format SOURCE --overwrite \
  /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook>
```

## Project Structure

```
vrh/
├── notebooks/
│   ├── work/        # Your development notebooks
│   └── mirror/      # Workspace downloads (reference)
├── tests/           # Local test runners
├── venv/            # Python virtual environment
├── .databrickscfg   # Credentials (DO NOT COMMIT)
└── setup_env.sh     # Environment setup script
```

## Documentation

- **CLAUDE.md**: Framework guidance and project-specific workflows
- **SETUP_NEW_PROJECT.md**: Step-by-step setup reference (from framework template)
