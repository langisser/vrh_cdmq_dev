# Databricks notebook source
# MAGIC %sql
# MAGIC select slimirality(a,b) < 2

# COMMAND ----------

select * from MAIN,MATCH
'MAIN.Fname = MATCH.Fname'