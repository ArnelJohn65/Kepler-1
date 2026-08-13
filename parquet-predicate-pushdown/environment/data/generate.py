"""Generate synthetic Parquet data and queries for the predicate-pushdown task."""

import json
import os
import pyarrow as pa
import pyarrow.parquet as pq
import random

random.seed(42)

OUT_DIR = "/app/data"
os.makedirs(OUT_DIR, exist_ok=True)

# Build a table with 3 row groups, ~100 rows each
# Column: id (int), amount (float), category (str), nullable_col (int, with nulls)

def make_row_group(id_start, id_end, amount_min, amount_max, category, null_fraction=0.1):
    n = id_end - id_start
    ids = list(range(id_start, id_end))
    amounts = [round(amount_min + random.random() * (amount_max - amount_min), 2) for _ in range(n)]
    categories = [category] * n
    nullables = [None if random.random() < null_fraction else random.randint(1, 100) for _ in range(n)]
    return pa.table({
        "id": pa.array(ids, type=pa.int64()),
        "amount": pa.array(amounts, type=pa.float64()),
        "category": pa.array(categories, type=pa.string()),
        "nullable_col": pa.array(nullables, type=pa.int64()),
    })

rg0 = make_row_group(0,   100, 0.0,   100.0, "A")
rg1 = make_row_group(100, 200, 100.0, 200.0, "B")
rg2 = make_row_group(200, 300, 200.0, 300.0, "C")

# Write as a single Parquet file with 3 row groups
writer = pq.ParquetWriter(
    os.path.join(OUT_DIR, "sales.parquet"),
    rg0.schema,
    write_statistics=True,
)
for rg in [rg0, rg1, rg2]:
    writer.write_table(rg)
writer.close()

# Queries
queries = [
    # q1: amount in row group 0 only (should skip rg1, rg2)
    {"id": "q1", "file": "sales.parquet", "predicate": {"column": "amount", "op": "lt", "value": 50.0}, "columns": ["id", "amount"]},
    # q2: amount in row group 2 only (should skip rg0, rg1)
    {"id": "q2", "file": "sales.parquet", "predicate": {"column": "amount", "op": "ge", "value": 250.0}, "columns": ["id", "amount"]},
    # q3: category = "B" (should skip rg0, rg2)
    {"id": "q3", "file": "sales.parquet", "predicate": {"column": "category", "op": "eq", "value": "B"}, "columns": ["id", "category"]},
    # q4: IS NULL on nullable_col — must NOT prune incorrectly
    {"id": "q4", "file": "sales.parquet", "predicate": {"column": "nullable_col", "op": "is_null"}, "columns": ["id", "nullable_col"]},
    # q5: boundary value — exact max of rg0 (off-by-one bug manifests here)
    {"id": "q5", "file": "sales.parquet", "predicate": {"column": "id", "op": "eq", "value": 99}, "columns": ["id"]},
    # q6: no predicate — reads everything
    {"id": "q6", "file": "sales.parquet", "predicate": None, "columns": ["id", "amount", "category"]},
]

with open(os.path.join(OUT_DIR, "queries.json"), "w") as f:
    json.dump(queries, f, indent=2)

print("Data generation complete.")
