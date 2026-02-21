# Databricks notebook source
# MAGIC %md
# MAGIC # Hello World
# MAGIC
# MAGIC A simple test notebook to verify local development and workspace upload workflow.

# COMMAND ----------

# DBTITLE 1,Hello World
print("Hello, World!")
print("Databricks local development is working!")

# COMMAND ----------

# DBTITLE 1,Show Parameters
import sys
from datetime import datetime

print(f"Python version: {sys.version}")
print(f"Execution time: {datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Exit
try:
    dbutils.notebook.exit("Hello World completed successfully")
except NameError:
    print("Notebook exit (local): Hello World completed successfully")
