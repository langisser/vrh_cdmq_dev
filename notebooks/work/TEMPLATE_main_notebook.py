# Databricks notebook source
# MAGIC %md
# MAGIC # [YOUR PROJECT NAME] - Main Notebook
# MAGIC
# MAGIC ## Description
# MAGIC [Describe what this notebook does]
# MAGIC
# MAGIC ## Parameters
# MAGIC - `param1`: [Description of parameter 1]
# MAGIC - `param2`: [Description of parameter 2]
# MAGIC
# MAGIC ## Prerequisites
# MAGIC - [List any prerequisites]

# COMMAND ----------

# DBTITLE 1,Import Required Libraries
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
import sys
import os

# Import custom helper functions (adjust path as needed)
# In workspace: Use %run to import helper notebook
# In local: Import will be handled by test runner
try:
    # Try to import common functions (for local execution)
    from TEMPLATE_common_functions import (
        run_workflow,
        run_and_wait_workflow_v2,
        safe_notebook_exit
    )
except ImportError:
    # In workspace, functions will be available via %run
    pass

# COMMAND ----------

# DBTITLE 1,Define Parameters
# Create widgets for notebook parameters
dbutils.widgets.text("param1", "default_value1", "Parameter 1 Description")
dbutils.widgets.text("param2", "default_value2", "Parameter 2 Description")
dbutils.widgets.text("workflow_name", "", "Databricks Workflow Name (optional)")

# Get parameter values
param1 = dbutils.widgets.get("param1")
param2 = dbutils.widgets.get("param2")
workflow_name = dbutils.widgets.get("workflow_name")

print(f"Parameters:")
print(f"  param1: {param1}")
print(f"  param2: {param2}")
print(f"  workflow_name: {workflow_name}")

# COMMAND ----------

# DBTITLE 1,Main Logic

def main():
    """
    Main execution logic for this notebook

    CUSTOMIZE THIS SECTION WITH YOUR BUSINESS LOGIC
    """
    try:
        print(f"Starting execution at {datetime.now()}")

        # Example: Read data from a table
        # df = spark.table("your_catalog.your_schema.your_table")

        # Example: Apply transformations
        # result_df = df.filter(F.col("status") == param1)

        # Example: Write results
        # result_df.write.mode("overwrite").saveAsTable("your_output_table")

        # Example: Execute a Databricks workflow (if workflow_name provided)
        if workflow_name and workflow_name.strip():
            print(f"Executing workflow: {workflow_name}")

            # Optionally pass parameters to the workflow
            job_parameters = {
                "param1": param1,
                "param2": param2
            }

            result, workflow_url, run_id = run_and_wait_workflow_v2(
                workflow_name,
                interval=5,
                job_parameters=job_parameters
            )

            print(f"Workflow completed with result: {result}")
            print(f"Workflow URL: {workflow_url}")
            print(f"Run ID: {run_id}")
        else:
            print("No workflow specified, executing notebook logic only")
            # Add your notebook-specific logic here
            pass

        print(f"Execution completed successfully at {datetime.now()}")

        # Return success status
        return {
            "status": "success",
            "message": "Execution completed successfully",
            "timestamp": str(datetime.now())
        }

    except Exception as e:
        error_msg = f"Error during execution: {str(e)}"
        print(error_msg)

        # Return error status
        return {
            "status": "error",
            "message": error_msg,
            "timestamp": str(datetime.now())
        }

# COMMAND ----------

# DBTITLE 1,Execute and Exit

# Run main logic
result = main()

# Exit with result (compatible with both workspace and local execution)
safe_notebook_exit(result)
