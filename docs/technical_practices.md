# Technical Practices — vrh_cdmq_dev

รวม programming practices ที่ใช้ในโปรเจกต์นี้

---

## TP-001: Generate INSERT statements จาก Excel

**Use case:** Load source data จาก Excel เข้า Databricks table สำหรับ devtest / unit test

**Pattern:**
- ใช้ `openpyxl` อ่าน Excel
- Generate SQL แบบ multi-row `INSERT INTO ... VALUES (...)` แบ่งเป็น batches
- Output เป็น `.py` file (Databricks notebook format) เพื่อ review ก่อน run

**ตัวอย่าง script:** `scripts/gen_insert_source_devtest.py`

```python
import openpyxl

BATCH_SIZE = 100

def escape(val) -> str:
    if val is None:
        return "NULL"
    s = str(val).strip()
    if s == "" or s.lower() == "none":
        return "NULL"
    return "'" + s.replace("'", "''") + "'"

wb = openpyxl.load_workbook("source/data.xlsx")
ws = wb["Sheet1"]
headers = [ws.cell(1, c).value for c in range(1, ws.max_column + 1)]

rows = []
for r in range(2, ws.max_row + 1):
    row = {headers[c]: ws.cell(r, c + 1).value for c in range(len(headers))}
    if not all(v is None for v in row.values()):
        rows.append(row)

# batch insert
for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i + BATCH_SIZE]
    values = ",\n".join(
        "  (" + ", ".join(escape(row.get(col)) for col in headers) + ")"
        for row in batch
    )
    sql = f"INSERT INTO my_table ({', '.join(headers)}) VALUES\n{values};"
    print(sql)
```

**หมายเหตุ:**
- Column ชื่อ reserved word (เช่น `table`) ต้องใส่ backtick: `` `table` ``
- Partition column (`DATA_DT`) ให้ใส่เป็น fixed value แยกต่างหาก

---

## TP-002: Run DDL/DML บน Databricks จาก local ผ่าน DatabricksSession

**Use case:** One-time tasks เช่น DROP/CREATE table, INSERT data, investigate data — โดยไม่ต้อง upload notebook ขึ้น workspace

**Pattern:**
```python
import os
os.environ["DATABRICKS_CONFIG_FILE"] = "/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg"

from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()

# DDL
spark.sql("DROP TABLE IF EXISTS my_catalog.my_schema.my_table")
spark.sql("""
    CREATE TABLE my_catalog.my_schema.my_table (
        id STRING,
        name STRING
    ) USING DELTA PARTITIONED BY (DATA_DT STRING)
""")

# DML
spark.sql("INSERT INTO my_catalog.my_schema.my_table VALUES ('1', 'test', '2025-01-01')")

# Query
spark.sql("SELECT COUNT(*) FROM my_catalog.my_schema.my_table").show()
```

**venv:** `/home/khaw/ClaudeCode/databricks_dev_local/venv`
**config:** `/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg`

**ข้อควรระวัง:** เหมาะสำหรับ one-time / setup / investigate เท่านั้น — core jobs และ production pipeline ต้องรันเป็น notebook บน Databricks workspace ตามปกติ

---

## TP-003: Submit Databricks notebook run จาก local ผ่าน WorkspaceClient

**Use case:** One-time tasks ที่ต้องรัน notebook บน workspace เช่น setup pipeline, load data, smoke test — โดยไม่ต้องเปิด Databricks UI

**Pattern:**
```python
import os
os.environ["DATABRICKS_CONFIG_FILE"] = "/home/khaw/ClaudeCode/vrh_cdmq_dev/.databrickscfg"

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import NotebookTask, RunTask

w = WorkspaceClient()

run = w.jobs.submit(
    run_name="my_one_time_run",
    tasks=[RunTask(
        task_key="task1",
        existing_cluster_id="<cluster_id>",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/Users/.../my_notebook",
            base_parameters={
                "PARAMS": "param1^|param2^|param3",
                "ENV": "dev"
            }
        )
    )]
).result()  # blocks จน job เสร็จ

print(run.state.result_state)  # SUCCESS / FAILED
```

**ข้อควรระวัง:** เหมาะสำหรับ one-time / setup / investigate เท่านั้น — core jobs และ production pipeline ต้องรันเป็น notebook บน Databricks workspace ตามปกติ

---

*Updated: 2026-02-26*
