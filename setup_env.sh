#!/bin/bash
# Source this file to set up Databricks environment for vrh project
# Usage: source setup_env.sh

export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh/.databrickscfg

echo "Databricks environment configured for vrh!"
echo "Config file: $DATABRICKS_CONFIG_FILE"
echo ""
echo "Quick commands:"
echo "  # Test connection"
echo "  databricks workspace list /Workspace/Users/khachornpop@inteltion.com/vrh"
echo ""
echo "  # Download all notebooks from workspace"
echo "  databricks workspace export_dir /Workspace/Users/khachornpop@inteltion.com/vrh notebooks/mirror --format SOURCE"
echo ""
echo "  # Upload a notebook to workspace"
echo "  databricks workspace import --file notebooks/work/<notebook>.py --language PYTHON --format SOURCE --overwrite /Workspace/Users/khachornpop@inteltion.com/vrh/<notebook>"
echo ""
echo "  # Run local test"
echo "  source venv/bin/activate && python3 tests/run_<notebook>.py"
