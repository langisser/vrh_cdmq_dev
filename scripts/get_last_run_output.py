import sys, os
sys.path.insert(0, '/home/khaw/ClaudeCode/databricks_dev_local')
os.environ['DATABRICKS_CONFIG_FILE'] = '/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg'

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

runs = [r for r in w.jobs.list_runs(limit=10) if r.run_name and 'dedup_customer_name' in r.run_name]
if not runs:
    print("No runs found")
    exit()

run_id = runs[0].run_id
print(f"run_id: {run_id}")

run = w.jobs.get_run(run_id=run_id)
for task in run.tasks:
    print(f"\n=== task: {task.task_key} | state: {task.state.result_state} ===")
    out = w.jobs.get_run_output(run_id=task.run_id)
    # notebook_output.result = last cell value (dbutils.notebook.exit value)
    if out.notebook_output:
        print(f"exit value : {out.notebook_output.result}")
        print(f"truncated  : {out.notebook_output.truncated}")
    if out.error:
        print(f"ERROR      : {out.error}")
    if out.error_trace:
        print(f"TRACE      :\n{out.error_trace}")
    # logs URL
    if task.run_page_url:
        print(f"run URL    : {task.run_page_url}")
