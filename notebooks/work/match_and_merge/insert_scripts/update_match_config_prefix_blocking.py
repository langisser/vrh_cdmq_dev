# Databricks notebook source
# MAGIC %md
# MAGIC # Update Matching Config — Add Prefix Blocking to Levenshtein Rules
# MAGIC
# MAGIC Adds `LEFT(fname/lname,2) = LEFT(fname/lname,2)` equi-filter before levenshtein()
# MAGIC in rules 32, 33, 35, 36, 38, 39.
# MAGIC
# MAGIC This enables Spark HashJoin on the prefix (fast), then evaluates Levenshtein
# MAGIC only on candidates sharing the same 2-character prefix (~500x fewer pairs).
# MAGIC
# MAGIC **Rules updated:**
# MAGIC | Rule | Direction | Field |
# MAGIC |---|---|---|
# MAGIC | 32 | motor→trust | fname |
# MAGIC | 33 | motor→trust | lname |
# MAGIC | 35 | motor→motor | fname |
# MAGIC | 36 | motor→motor | lname |
# MAGIC | 38 | trust→motor | fname |
# MAGIC | 39 | trust→motor | lname |

# COMMAND ----------

# Verify current state before update
current = spark.sql("""
    SELECT MATCHING_RULES, MATCH_CONDITION
    FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
    WHERE MATCHING_RULES IN (32, 33, 35, 36, 38, 39)
    ORDER BY MATCHING_RULES
""")
print("=== Current MATCH_CONDITION (before update) ===")
current.show(truncate=False)

# COMMAND ----------

# Update fname rules (32, 35, 38)
spark.sql("""
    UPDATE viriyah_cdqm_poc.control_fw.chv_config_matching_v2
    SET MATCH_CONDITION = 'LEFT(MAIN.fname,2) = LEFT(MATCH.fname,2) AND levenshtein(MAIN.fname, MATCH.fname) <= 3'
    WHERE MATCHING_RULES IN (32, 35, 38)
""")
print("Updated fname rules 32, 35, 38")

# COMMAND ----------

# Update lname rules (33, 36, 39)
spark.sql("""
    UPDATE viriyah_cdqm_poc.control_fw.chv_config_matching_v2
    SET MATCH_CONDITION = 'LEFT(MAIN.lname,2) = LEFT(MATCH.lname,2) AND levenshtein(MAIN.lname, MATCH.lname) <= 3'
    WHERE MATCHING_RULES IN (33, 36, 39)
""")
print("Updated lname rules 33, 36, 39")

# COMMAND ----------

# Verify updated state
updated = spark.sql("""
    SELECT MATCHING_RULES, MATCH_CONDITION
    FROM viriyah_cdqm_poc.control_fw.chv_config_matching_v2
    WHERE MATCHING_RULES IN (32, 33, 35, 36, 38, 39)
    ORDER BY MATCHING_RULES
""")
print("=== Updated MATCH_CONDITION (after update) ===")
updated.show(truncate=False)
