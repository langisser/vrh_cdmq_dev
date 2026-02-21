#!/usr/bin/env python3
"""
Test runner for helloworld notebook
"""

import sys
import os
from datetime import datetime

notebook_dir = os.path.join(os.path.dirname(__file__), '..', 'notebooks', 'work')
sys.path.insert(0, os.path.abspath(notebook_dir))

NOTEBOOK_NAME = "helloworld"

LOG_DIR = os.path.dirname(__file__)
LOG_PATH = os.path.join(LOG_DIR, f"{NOTEBOOK_NAME}_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")


class MockDbutils:
    class Notebook:
        @staticmethod
        def exit(value):
            print(f"Notebook exit: {value}")
            sys.exit(0)

    def __init__(self):
        self.notebook = self.Notebook()


def strip_magic(content):
    lines = content.split('\n')
    return '\n'.join(
        line for line in lines
        if not line.strip().startswith(('# MAGIC', '# COMMAND ----------', '# DBTITLE'))
    )


def main():
    print("=" * 60)
    print(f"Running notebook: {NOTEBOOK_NAME}")
    print(f"Started: {datetime.now()}")
    print("=" * 60)

    notebook_path = os.path.join(notebook_dir, f"{NOTEBOOK_NAME}.py")
    with open(notebook_path, 'r') as f:
        code = strip_magic(f.read())

    exec_globals = {
        'dbutils': MockDbutils(),
        '__name__': '__main__',
    }

    original_stdout = sys.stdout
    with open(LOG_PATH, 'w') as log_file:
        class Tee:
            def write(self, data):
                original_stdout.write(data)
                log_file.write(data)
            def flush(self):
                original_stdout.flush()
                log_file.flush()

        sys.stdout = Tee()
        try:
            exec(code, exec_globals)
            print(f"\nCompleted: {datetime.now()}")
        except SystemExit as e:
            print(f"\nExited with code: {e.code}")
        except Exception as e:
            import traceback
            print(f"\nERROR: {e}")
            traceback.print_exc()
        finally:
            sys.stdout = original_stdout

    print(f"Log saved: {LOG_PATH}")
    print("=" * 60)


if __name__ == "__main__":
    main()
