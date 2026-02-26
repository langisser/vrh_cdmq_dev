#!/usr/bin/env python3
"""
run_ddl_source_devtest.py

Drop & recreate devtest source tables on Databricks using DatabricksSession.

Usage:
    source venv/bin/activate  (from databricks_dev_local)
    export DATABRICKS_CONFIG_FILE=/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg
    python3 scripts/run_ddl_source_devtest.py
"""

import os
import sys

os.environ["DATABRICKS_CONFIG_FILE"] = "/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg"

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
print(f"Spark connected: {spark.version}")

CATALOG = "viriyah_cdqm_poc"
SILVER  = "silver"
MOTOR_TABLE = f"{CATALOG}.{SILVER}.source_motor_devtest"
TRUST_TABLE = f"{CATALOG}.{SILVER}.trust_source_devtest"

# ── source_motor_devtest ─────────────────────────────────────────────────────
print(f"\nDrop & create: {MOTOR_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {MOTOR_TABLE}")

spark.sql(f"""
CREATE TABLE {MOTOR_TABLE} (
  policy_id   STRING    COMMENT 'Policy ID (PK)',
  id_card     STRING    COMMENT 'ID card',
  fname       STRING    COMMENT 'First name',
  lname       STRING    COMMENT 'Last name',
  gender      STRING,
  `table`     STRING    COMMENT 'Source table name (e.g. applicants, maspol)',
  birth_date  STRING,
  prefix      STRING,
  area        STRING,
  district    STRING,
  province    STRING,
  postcode    STRING,
  email       STRING,
  phone_no    STRING,
  insert_date TIMESTAMP,
  update_date TIMESTAMP
) USING DELTA
PARTITIONED BY (DATA_DT STRING)
COMMENT 'Dev-test motor source table'
""")
print(f"  Created: {MOTOR_TABLE}")

# ── trust_source_devtest ─────────────────────────────────────────────────────
print(f"\nDrop & create: {TRUST_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {TRUST_TABLE}")

spark.sql(f"""
CREATE TABLE {TRUST_TABLE} (
  id_card     STRING    COMMENT 'ID card (PK)',
  fname       STRING,
  lname       STRING,
  gender      STRING,
  `table`     STRING    COMMENT 'Source table name',
  birth_date  STRING,
  prefix      STRING,
  area        STRING,
  district    STRING,
  province    STRING,
  postcode    STRING,
  email       STRING,
  phone_no    STRING,
  insert_date TIMESTAMP,
  update_date TIMESTAMP
) USING DELTA
PARTITIONED BY (DATA_DT STRING)
COMMENT 'Dev-test trust source table'
""")
print(f"  Created: {TRUST_TABLE}")

# ── Verify ───────────────────────────────────────────────────────────────────
print("\n── Verify ──")
for tbl in [MOTOR_TABLE, TRUST_TABLE]:
    print(f"\nDESCRIBE {tbl}")
    spark.sql(f"DESCRIBE TABLE {tbl}").show(30, truncate=False)

print("\nDone.")
