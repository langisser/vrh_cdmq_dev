# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO viriyah_cdqm_poc.control_fw.chv_config_function (RULES, FUNCTION, PROGRAMMING, ACT_F)
# MAGIC VALUES (
# MAGIC   'CHECK_NULL',
# MAGIC   'def CHECK_NULL(col):\n  if col != None:\n    return "PASSED"\n  else:\n    return "FAILED"',
# MAGIC   'PYTHON',
# MAGIC   1
# MAGIC );