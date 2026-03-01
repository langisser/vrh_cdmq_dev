# Databricks notebook source
# MAGIC %md
# MAGIC # Validate dedup tables — no duplicate rows

# COMMAND ----------

import json

catalog = "viriyah_cdqm_poc"
schema  = "silver"

checks = {}
all_ok = True

DEDUP_KEYS = {
    "dedup_gender":        "bkey, id_card, gender, birth_date",
    "dedup_province":      "bkey, id_card, area, district, postcode, province",
    "dedup_customer_name": "bkey, id_card, fname, lname, prefix",
    "dedup_email":         "bkey, id_card, email",
    "dedup_phone":         "bkey, id_card, phone_no",
}

for tbl, keys in DEDUP_KEYS.items():
    full_tbl = f"{catalog}.{schema}.{tbl}"

    dup_count = spark.sql(f"""
        SELECT COUNT(*) AS n FROM (
            SELECT {keys}, COUNT(*) AS cnt
            FROM {full_tbl}
            GROUP BY {keys}
            HAVING cnt > 1
        )
    """).collect()[0].n

    total = spark.sql(f"SELECT COUNT(*) AS n FROM {full_tbl}").collect()[0].n

    status = "OK" if dup_count == 0 else "FAIL"
    if dup_count != 0:
        all_ok = False

    checks[tbl] = {"status": status, "dup_groups": dup_count, "total_rows": total}
    print(f"[{status}] {tbl}: dup_groups={dup_count}, total_rows={total}")

print()
summary = "PASSED" if all_ok else "FAILED"
print(f"=== {summary} ===")

dbutils.notebook.exit(json.dumps({"result": summary, "checks": checks}))
