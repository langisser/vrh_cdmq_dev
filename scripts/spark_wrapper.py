#!/usr/bin/env python3
"""
spark_wrapper.py — Execute a Python script file against Databricks using DatabricksSession.

Usage:
    python3 /home/khaw/ClaudeCode/vrh_cdmq_dev/scripts/spark_wrapper.py -file <script.py>

The script file should contain plain Python code. DatabricksSession is automatically
created and injected as `spark` before the script runs. The DATABRICKS_CONFIG_FILE
environment variable is also set automatically.

Example:
    python3 /home/khaw/ClaudeCode/vrh_cdmq_dev/scripts/spark_wrapper.py -file /tmp/my_query.py
"""

import argparse
import os
import sys

# Always point to the correct Databricks config
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

def main():
    parser = argparse.ArgumentParser(
        description='Execute a Python script with a pre-configured DatabricksSession as `spark`.'
    )
    parser.add_argument('-file', required=True, help='Path to the Python script to execute')
    args = parser.parse_args()

    script_path = args.file
    if not os.path.isfile(script_path):
        print(f"ERROR: File not found: {script_path}", file=sys.stderr)
        sys.exit(1)

    # Build the Databricks session
    try:
        from databricks.connect import DatabricksSession
        spark = DatabricksSession.builder.getOrCreate()
    except Exception as e:
        print(f"ERROR: Failed to create DatabricksSession: {e}", file=sys.stderr)
        sys.exit(1)

    # Read the script
    with open(script_path, 'r') as f:
        script_source = f.read()

    # Execute the script with `spark` available in its namespace
    script_globals = {
        '__file__': script_path,
        '__name__': '__main__',
        'spark': spark,
    }
    try:
        exec(compile(script_source, script_path, 'exec'), script_globals)
    except SystemExit as e:
        sys.exit(e.code)
    except Exception as e:
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
