#!/usr/bin/env python3
"""
Template Test Runner for Databricks Notebooks

This script executes a Databricks notebook locally using Databricks Connect.

Usage:
    python3 tests/TEMPLATE_run_notebook.py

Customize:
    1. Update NOTEBOOK_NAME to your notebook filename (without .py extension)
    2. Update HELPER_NOTEBOOK to your common functions filename
    3. Adjust NOTEBOOK_PARAMS with your notebook's parameters
    4. Modify any other configuration as needed
"""

import sys
import os
from datetime import datetime
import re

# Add the notebooks/work directory to the Python path
notebook_dir = os.path.join(os.path.dirname(__file__), '..', 'notebooks', 'work')
sys.path.insert(0, os.path.abspath(notebook_dir))

# Configuration
NOTEBOOK_NAME = "TEMPLATE_main_notebook"  # Change this to your notebook name
HELPER_NOTEBOOK = "TEMPLATE_common_functions"  # Change this to your helper functions name

# Notebook parameters (customize these based on your notebook's requirements)
NOTEBOOK_PARAMS = {
    "param1": "default_value1",
    "param2": "default_value2",
    "workflow_name": ""  # Optional: specify a workflow to run
}

# Log file configuration
LOG_DIR = os.path.dirname(__file__)
LOG_FILENAME = f"{NOTEBOOK_NAME}_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
LOG_PATH = os.path.join(LOG_DIR, LOG_FILENAME)


class MockDbutils:
    """
    Mock dbutils for local execution
    This allows notebooks to run locally without modification
    """
    class Widgets:
        def __init__(self, params):
            self.params = params

        def text(self, name, default, label):
            """Mock widget creation"""
            if name not in self.params:
                self.params[name] = default

        def get(self, name):
            """Get parameter value"""
            return self.params.get(name, "")

    class Notebook:
        @staticmethod
        def exit(value):
            """Mock notebook exit"""
            print(f"Notebook exit called with value: {value}")
            sys.exit(0)

    def __init__(self, params):
        self.widgets = self.Widgets(params)
        self.notebook = self.Notebook()


def strip_notebook_magic(content):
    """
    Remove Databricks magic commands and comments from notebook content

    Args:
        content (str): Notebook source code

    Returns:
        str: Cleaned source code
    """
    lines = content.split('\n')
    cleaned_lines = []

    for line in lines:
        # Skip magic commands
        if line.strip().startswith('# MAGIC'):
            continue

        # Skip command separators
        if line.strip().startswith('# COMMAND ----------'):
            continue

        # Skip DBTITLE comments
        if line.strip().startswith('# DBTITLE'):
            continue

        # Keep all other lines
        cleaned_lines.append(line)

    return '\n'.join(cleaned_lines)


def load_notebook(notebook_name):
    """
    Load and prepare a notebook for execution

    Args:
        notebook_name (str): Name of the notebook file (without .py extension)

    Returns:
        str: Cleaned notebook source code
    """
    notebook_path = os.path.join(notebook_dir, f"{notebook_name}.py")

    if not os.path.exists(notebook_path):
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    print(f"Loading notebook: {notebook_path}")

    with open(notebook_path, 'r') as f:
        content = f.read()

    # Strip Databricks-specific magic commands
    cleaned_content = strip_notebook_magic(content)

    return cleaned_content


def main():
    """
    Main execution function
    """
    print("=" * 80)
    print(f"Databricks Notebook Local Execution")
    print(f"Notebook: {NOTEBOOK_NAME}")
    print(f"Started: {datetime.now()}")
    print("=" * 80)
    print()

    # Create log file
    print(f"Logging to: {LOG_PATH}")
    print()

    try:
        # Load helper functions first
        print(f"Loading helper functions: {HELPER_NOTEBOOK}")
        helper_code = load_notebook(HELPER_NOTEBOOK)

        # Create mock dbutils with parameters
        mock_dbutils = MockDbutils(NOTEBOOK_PARAMS)

        # Prepare execution environment
        exec_globals = {
            'dbutils': mock_dbutils,
            '__name__': '__main__',
            'display': print  # Mock display function
        }

        # Execute helper functions
        exec(helper_code, exec_globals)
        print(f"Helper functions loaded successfully\n")

        # Load main notebook
        print(f"Loading main notebook: {NOTEBOOK_NAME}")
        notebook_code = load_notebook(NOTEBOOK_NAME)

        # Execute main notebook
        print(f"Executing notebook...\n")
        print("-" * 80)

        # Redirect output to both console and log file
        original_stdout = sys.stdout
        original_stderr = sys.stderr

        with open(LOG_PATH, 'w') as log_file:
            class TeeOutput:
                def __init__(self, *files):
                    self.files = files

                def write(self, data):
                    for f in self.files:
                        f.write(data)
                        f.flush()

                def flush(self):
                    for f in self.files:
                        f.flush()

            tee = TeeOutput(original_stdout, log_file)
            sys.stdout = tee
            sys.stderr = tee

            try:
                # Execute the notebook
                exec(notebook_code, exec_globals)

                print("-" * 80)
                print(f"\nNotebook execution completed successfully")
                print(f"Completed: {datetime.now()}")

            finally:
                # Restore original stdout/stderr
                sys.stdout = original_stdout
                sys.stderr = original_stderr

        print(f"\nLog saved to: {LOG_PATH}")
        print("=" * 80)

    except SystemExit as e:
        # Handle safe_notebook_exit()
        print(f"\nNotebook exited with code: {e.code}")
        if e.code == 0:
            print("Execution completed successfully")
        else:
            print("Execution completed with errors")
        return e.code

    except Exception as e:
        print(f"\nERROR: {str(e)}")
        print("=" * 80)
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
