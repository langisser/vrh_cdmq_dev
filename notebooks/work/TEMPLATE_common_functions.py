# Databricks notebook source
# MAGIC %md
# MAGIC # Common Functions Library
# MAGIC
# MAGIC This notebook contains reusable helper functions for Databricks notebooks.
# MAGIC
# MAGIC ## Functions
# MAGIC - `run_workflow()`: Execute a Databricks workflow
# MAGIC - `run_and_wait_workflow_v2()`: Execute and monitor workflow completion
# MAGIC - `safe_notebook_exit()`: Exit notebook compatible with both workspace and local execution
# MAGIC - `display_compatible()`: Display function that works in both environments
# MAGIC
# MAGIC ## Usage
# MAGIC In Databricks workspace: Use `%run /path/to/this/notebook`
# MAGIC In local testing: Import functions directly

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
import time
import sys
import os

# COMMAND ----------

# DBTITLE 1,Initialize Databricks Client

def get_workspace_client():
    """
    Initialize and return a Databricks WorkspaceClient

    Returns:
        WorkspaceClient: Authenticated Databricks client
    """
    try:
        # Databricks Connect will use config from .databrickscfg or environment variables
        w = WorkspaceClient()
        return w
    except Exception as e:
        print(f"Error initializing Databricks client: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Workflow Execution Functions

def run_workflow(workflow_name, job_parameters=None):
    """
    Execute a Databricks workflow by name

    Args:
        workflow_name (str): Name of the workflow to execute
        job_parameters (dict, optional): Parameters to pass to the job

    Returns:
        tuple: (run_id, workflow_url)

    Example:
        run_id, url = run_workflow("my_workflow", {"param1": "value1"})
    """
    try:
        w = get_workspace_client()

        # Find job by name
        jobs_list = w.jobs.list(name=workflow_name)
        matching_jobs = [job for job in jobs_list if job.settings.name == workflow_name]

        if not matching_jobs:
            raise ValueError(f"Workflow '{workflow_name}' not found")

        job = matching_jobs[0]
        job_id = job.job_id

        print(f"Found workflow '{workflow_name}' with ID: {job_id}")

        # Prepare job parameters
        if job_parameters:
            # Convert parameters to the format expected by Databricks
            notebook_params = {k: str(v) for k, v in job_parameters.items()}
            run_response = w.jobs.run_now(
                job_id=job_id,
                notebook_params=notebook_params
            )
        else:
            run_response = w.jobs.run_now(job_id=job_id)

        run_id = run_response.run_id
        workflow_url = f"{w.config.host}/#job/{job_id}/run/{run_id}"

        print(f"Started workflow run: {run_id}")
        print(f"URL: {workflow_url}")

        return run_id, workflow_url

    except Exception as e:
        print(f"Error running workflow '{workflow_name}': {str(e)}")
        raise


def run_and_wait_workflow_v2(workflow_name, interval=5, job_parameters=None):
    """
    Execute a Databricks workflow and wait for completion

    Args:
        workflow_name (str): Name of the workflow to execute
        interval (int): Polling interval in seconds (default: 5)
        job_parameters (dict, optional): Parameters to pass to the job

    Returns:
        tuple: (result_state, workflow_url, run_id)

    Example:
        result, url, run_id = run_and_wait_workflow_v2(
            "my_workflow",
            interval=10,
            job_parameters={"date": "2024-01-01"}
        )
    """
    try:
        # Start the workflow
        run_id, workflow_url = run_workflow(workflow_name, job_parameters)

        # Get workspace client for monitoring
        w = get_workspace_client()

        # Monitor workflow execution
        print(f"Monitoring workflow execution (polling every {interval} seconds)...")

        while True:
            run_info = w.jobs.get_run(run_id=run_id)
            state = run_info.state.life_cycle_state

            print(f"Workflow state: {state}")

            # Check if workflow is still running
            if state in ['PENDING', 'RUNNING', 'TERMINATING']:
                time.sleep(interval)
                continue

            # Workflow completed
            result_state = run_info.state.result_state
            print(f"Workflow completed with result: {result_state}")

            if result_state == jobs.RunResultState.SUCCESS:
                print(f"Workflow '{workflow_name}' completed successfully")
            else:
                print(f"Workflow '{workflow_name}' failed with state: {result_state}")
                if run_info.state.state_message:
                    print(f"Error message: {run_info.state.state_message}")

            return result_state, workflow_url, run_id

    except Exception as e:
        print(f"Error monitoring workflow '{workflow_name}': {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Environment Compatibility Functions

def safe_notebook_exit(payload):
    """
    Exit notebook in a way that works in both Databricks workspace and local execution

    Args:
        payload: Data to return (dict, string, or any JSON-serializable object)

    In Databricks workspace:
        Calls dbutils.notebook.exit() with the payload

    In local execution:
        Raises SystemExit with appropriate exit code
    """
    try:
        # Try to use dbutils (available in Databricks workspace)
        import json
        payload_str = json.dumps(payload) if isinstance(payload, dict) else str(payload)
        dbutils.notebook.exit(payload_str)
    except NameError:
        # dbutils not available (local execution)
        print(f"Notebook exit (local): {payload}")

        # Determine exit code based on payload
        if isinstance(payload, dict):
            status = payload.get("status", "unknown")
            exit_code = 0 if status == "success" else 1
        else:
            exit_code = 0

        raise SystemExit(exit_code)


def display_compatible(data, message=None):
    """
    Display function that works in both Databricks workspace and local execution

    Args:
        data: Data to display (DataFrame, dict, list, etc.)
        message (str, optional): Message to print before displaying data
    """
    if message:
        print(message)

    try:
        # Try to use Databricks display()
        display(data)
    except NameError:
        # display() not available (local execution)
        if hasattr(data, 'show'):
            # PySpark DataFrame
            data.show()
        elif hasattr(data, 'head'):
            # Pandas DataFrame
            print(data.head(20))
        else:
            # Other data types
            print(data)

# COMMAND ----------

# DBTITLE 1,Logging Utilities

def log_message(level, message):
    """
    Simple logging function

    Args:
        level (str): Log level (INFO, WARNING, ERROR, DEBUG)
        message (str): Message to log
    """
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")


def log_info(message):
    """Log INFO level message"""
    log_message("INFO", message)


def log_warning(message):
    """Log WARNING level message"""
    log_message("WARNING", message)


def log_error(message):
    """Log ERROR level message"""
    log_message("ERROR", message)


def log_debug(message):
    """Log DEBUG level message"""
    log_message("DEBUG", message)

# COMMAND ----------

# DBTITLE 1,Add Your Custom Helper Functions Below

# TODO: Add your project-specific helper functions here
#
# Example:
# def process_data(df, config):
#     """
#     Process data according to business logic
#
#     Args:
#         df: Input DataFrame
#         config: Configuration dictionary
#
#     Returns:
#         Processed DataFrame
#     """
#     # Your logic here
#     return df

# COMMAND ----------

print("Common functions library loaded successfully")
