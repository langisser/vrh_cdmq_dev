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

> TP-002 (DatabricksSession) และ TP-003 (WorkspaceClient submit) อยู่ใน `CLAUDE.md` — Development Workflow แล้ว

*Updated: 2026-02-26*
