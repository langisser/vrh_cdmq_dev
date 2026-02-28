import sys, os
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.sdk import WorkspaceClient

RUN_ID = 669945570294922

w = WorkspaceClient()

# get-output ของ task run_id (ไม่ใช่ job run_id)
job_run = w.jobs.get_run(run_id=RUN_ID)
for task in job_run.tasks:
    print(f"task_key : {task.task_key}")
    print(f"run_id   : {task.run_id}")
    print(f"state    : {task.state.result_state if task.state else 'N/A'}")
    out = w.jobs.get_run_output(run_id=task.run_id)
    print(f"error    : {out.error}")
    print(f"error_trace (500 chars):\n{(out.error_trace or '')[:500]}")
    if out.notebook_output:
        print(f"notebook result: {out.notebook_output.result}")
    print()
