#!/usr/bin/env python3
"""
Run full match & merge v2 pipeline for DATA_DT = 2025-01-01
Steps:
  1. pre_validation — source_motor_devtest
  2. pre_validation — trust_source_devtest
  3. match_v2       — source_motor_devtest
"""
import os, sys
os.environ["DATABRICKS_CONFIG_FILE"] = "/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg"
sys.path.insert(0, "/home/khaw/ClaudeCode/databricks_dev_local")

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, RunTask

CLUSTER_ID = "0130-031624-0nmpnh8g"
WS         = "/Workspace/Users/khachornpop@inteltion.com/vrh/match_and_merge"
DATA_DT    = "2025-01-01"
MOTOR      = "viriyah_cdqm_poc.silver.source_motor_devtest"
TRUST      = "viriyah_cdqm_poc.silver.trust_source_devtest"
VLD_TABLE  = "viriyah_cdqm_poc.control_fw.chv_pre_validation_result_v2"

JOBS = [
    {
        "label": "1_pre_val_motor",
        "notebook": f"{WS}/vrh_chv_pre_validation_v2",
        "params": {
            "PARAMS": f"{MOTOR}^|{VLD_TABLE}^|{DATA_DT}^|EDP_PRE_VLD_MOTOR_DEVTEST^|1^|EDP_PRE_VLD_MOTOR_DEVTEST^|1",
            "ENV": "dev",
        },
    },
    {
        "label": "2_pre_val_trust",
        "notebook": f"{WS}/vrh_chv_pre_validation_v2",
        "params": {
            "PARAMS": f"{TRUST}^|{VLD_TABLE}^|{DATA_DT}^|EDP_PRE_VLD_TRUST_DEVTEST^|1^|EDP_PRE_VLD_TRUST_DEVTEST^|1",
            "ENV": "dev",
        },
    },
    {
        "label": "3_match_v2",
        "notebook": f"{WS}/vrh_chv_match_v2",
        "params": {
            "PARAMS": f"{MOTOR}^|{DATA_DT}^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_{DATA_DT}^|1^|EDP_MATCHING_V2_SOURCE_MOTOR_DATE_{DATA_DT}^|1",
            "ENV": "dev",
        },
    },
]

w = WorkspaceClient()
for job in JOBS:
    print(f"\n>>> [{job['label']}] submitting...")
    run = w.jobs.submit(
        run_name=f"match_merge_{DATA_DT}_{job['label']}",
        tasks=[RunTask(
            task_key="t1",
            existing_cluster_id=CLUSTER_ID,
            notebook_task=NotebookTask(
                notebook_path=job["notebook"],
                base_parameters=job["params"],
            ),
        )],
    ).result()
    state = run.state.result_state
    print(f"    result: {state}")
    if str(state) != "RunResultState.SUCCESS":
        print(f"FAILED at [{job['label']}] — stopping.")
        sys.exit(1)

print(f"\nAll steps PASSED for DATA_DT={DATA_DT}")
