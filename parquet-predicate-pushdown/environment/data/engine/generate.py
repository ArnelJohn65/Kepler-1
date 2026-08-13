"""
Dataset generator.

Creates a deterministic Parquet dataset under /app/data/ and writes
the queries to run to /app/queries.json.

The dataset is a single table called `sensors` with columns:
    id              INTEGER   -- row id, 1-based (1..10000)
    sensor_id       INTEGER   -- equals id; strictly increasing per row group
    reading         FLOAT     -- sensor reading
    category        TEXT      -- category label A/B/C/D/E
    flagged         BOOLEAN   -- random flag
    value_nullable  INTEGER   -- nullable; NULL for every 10th row

Row group size: 1000 rows. Total rows: 10000 (10 row groups).

Because sensor_id = id, each row group k covers sensor_id range
[(k*1000)+1 .. (k+1)*1000] with no overlap. The MAXIMUM of each
row group is always the very last element -- so the off-by-one bug in
the stats accumulator (range(n-1) instead of range(n)) causes the
recorded maximum to be one less than the true maximum, breaking
boundary-value predicate pushdown.
"""
import json
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

from engine.stats import compute_stats

SEED = 42
DATA_DIR = "/app/data"
NUM_ROWS = 10_000
ROW_GROUP_SIZE = 1_000
CATEGORIES = ["A", "B", "C", "D", "E"]


def generate():
    random.seed(SEED)
    os.makedirs(DATA_DIR, exist_ok=True)

    ids = list(range(1, NUM_ROWS + 1))
    # sensor_id equals the row id: strictly increasing within each row group,
    # so the last element is always the maximum.
    sensor_ids = ids[:]

    readings = [round(random.uniform(0.0, 100.0), 4) for _ in range(NUM_ROWS)]
    categories = [random.choice(CATEGORIES) for _ in range(NUM_ROWS)]
    flagged = [random.random() > 0.5 for _ in range(NUM_ROWS)]
    # NULL for every 10th row (0-indexed)
    value_nullable = [None if i % 10 == 0 else random.randint(0, 999) for i in range(NUM_ROWS)]

    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("sensor_id", pa.int64()),
        pa.field("reading", pa.float64()),
        pa.field("category", pa.string()),
        pa.field("flagged", pa.bool_()),
        pa.field("value_nullable", pa.int64()),
    ])

    filepath = os.path.join(DATA_DIR, "sensors.parquet")
    writer = pq.ParquetWriter(filepath, schema)

    stats_index = {"sensors.parquet": []}

    for rg in range(NUM_ROWS // ROW_GROUP_SIZE):
        start = rg * ROW_GROUP_SIZE
        end = start + ROW_GROUP_SIZE

        rg_ids = ids[start:end]
        rg_sensor_ids = sensor_ids[start:end]
        rg_readings = readings[start:end]
        rg_categories = categories[start:end]
        rg_flagged = flagged[start:end]
        rg_nullable = value_nullable[start:end]

        arrays = [
            pa.array(rg_ids, type=pa.int64()),
            pa.array(rg_sensor_ids, type=pa.int64()),
            pa.array(rg_readings, type=pa.float64()),
            pa.array(rg_categories, type=pa.string()),
            pa.array(rg_flagged, type=pa.bool_()),
            pa.array(rg_nullable, type=pa.int64()),
        ]
        batch = pa.RecordBatch.from_arrays(arrays, schema=schema)
        writer.write_batch(batch)

        rg_stats = {}
        for col, vals in [
            ("id", rg_ids),
            ("sensor_id", rg_sensor_ids),
            ("reading", rg_readings),
            ("category", rg_categories),
            ("value_nullable", rg_nullable),
        ]:
            rg_stats[col] = compute_stats(vals)

        stats_index["sensors.parquet"].append(rg_stats)

    writer.close()

    index_path = os.path.join(DATA_DIR, "stats_index.json")
    with open(index_path, "w") as f:
        json.dump(stats_index, f, indent=2)

    queries = [
        # q1: sensor_id = 5 -- only in row group 0 (sensor_ids 1..1000)
        {
            "id": "q1",
            "table": "sensors",
            "predicate": {"op": "=", "col": "sensor_id", "val": 5},
            "columns": ["id", "sensor_id"],
        },
        # q2: sensor_id > 9000 -- only in row group 9 (9001..10000)
        {
            "id": "q2",
            "table": "sensors",
            "predicate": {"op": ">", "col": "sensor_id", "val": 9000},
            "columns": ["id", "sensor_id"],
        },
        # q3: sensor_id <= 1000 -- only row group 0
        {
            "id": "q3",
            "table": "sensors",
            "predicate": {"op": "<=", "col": "sensor_id", "val": 1000},
            "columns": ["id", "sensor_id"],
        },
        # q4: IS NULL on value_nullable -- cannot prune any row group
        {
            "id": "q4",
            "table": "sensors",
            "predicate": {"op": "IS NULL", "col": "value_nullable"},
            "columns": ["id", "value_nullable"],
        },
        # q5: sensor_id = 10000 -- the very last row (boundary bug test).
        # With the off-by-one bug the max of RG 9 is recorded as 9999,
        # so the planner incorrectly prunes RG 9 and returns 0 rows.
        {
            "id": "q5",
            "table": "sensors",
            "predicate": {"op": "=", "col": "sensor_id", "val": 10000},
            "columns": ["id", "sensor_id"],
        },
        # q6: sensor_id >= 4000 AND sensor_id <= 5000 -- spans RGs 3 and 4
        {
            "id": "q6",
            "table": "sensors",
            "predicate": {
                "op": "AND",
                "children": [
                    {"op": ">=", "col": "sensor_id", "val": 4000},
                    {"op": "<=", "col": "sensor_id", "val": 5000},
                ],
            },
            "columns": ["id", "sensor_id"],
        },
        # q7: sensor_id != 5 -- cannot prune safely (negated predicate)
        {
            "id": "q7",
            "table": "sensors",
            "predicate": {"op": "!=", "col": "sensor_id", "val": 5},
            "columns": ["id", "sensor_id"],
        },
    ]

    with open("/app/queries.json", "w") as f:
        json.dump(queries, f, indent=2)

    print(f"Generated {NUM_ROWS} rows in {NUM_ROWS // ROW_GROUP_SIZE} row groups.")
    print(f"Data: {filepath}")
    print(f"Stats index: {index_path}")
    print("Queries: /app/queries.json")


if __name__ == "__main__":
    generate()
