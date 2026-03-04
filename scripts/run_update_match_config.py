#!/usr/bin/env python3
"""
run_update_match_config.py — Apply prefix blocking to Levenshtein matching rules

Updates MATCH_CONDITION for rules 32,33,35,36,38,39 in chv_config_matching_v2
to prepend LEFT(fname/lname,2) equi-filter before levenshtein(), reducing
self-join pairs from 10B → ~200K (~500x speedup).

Usage:
    source /home/khaw/ClaudeCode/vrh_cdmq_dev/venv/bin/activate
    python3 scripts/run_update_match_config.py
"""
import os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from tools import run_notebook, get_run_cell_error

NOTEBOOK = '/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge/insert_scripts/update_match_config_prefix_blocking'
CLUSTER_ID = '0130-031624-0nmpnh8g'

result = run_notebook(
    notebook_path=NOTEBOOK,
    params={'ENV': 'dev'},
    cluster_id=CLUSTER_ID,
    auth_method='azure_cli',
)
print(result['status'], result.get('error', ''))

if result['status'] == 'FAILED':
    for e in get_run_cell_error(result['run_id'], auth_method='azure_cli'):
        print(e['summary'])
        print(e['error_detail'])
