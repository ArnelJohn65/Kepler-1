import json
import math
import os
import random

import pyarrow as pa
import pyarrow.parquet as pq

TESTS_ROOT = os.environ.get("TESTS_ROOT", "/tests")
OUT_DIR = os.path.join(TESTS_ROOT, "data")
os.makedirs(OUT_DIR, exist_ok=True)

RANDOM_SEED = 20260813
ROW_GROUPS = 32
ROWS_PER_GROUP = 500

regions = ["APAC", "EMEA", "AMER"]
segments = ["retail", "enterprise", "public", "startup", "midmarket", "gov"]
statuses = ["open", "closed", "pending", "hold", "fraud"]
skus = [f"SKU{i:03d}" for i in range(1, 41)]


def make_row_group(group_idx: int) -> tuple[pa.Table, dict]:
    rng = random.Random(RANDOM_SEED + group_idx)
    n = ROWS_PER_GROUP

    amount_band = group_idx % 6
    amount_base = amount_band * 200.0

    ids = []
    amount_values = []
    region_values = []
    segment_values = []
    status_values = []
    priority_values = []
    sku_values = []
    event_day_values = []
    score_values = []

    region = regions[group_idx % len(regions)]
    segment_primary = segments[group_idx % len(segments)]
    segment_secondary = segments[(group_idx + 2) % len(segments)]
    status_primary = statuses[group_idx % len(statuses)]
    status_secondary = statuses[(group_idx + 1) % len(statuses)]

    window = len(skus)
    sku_slice_start = (group_idx * 3) % window
    group_skus = [skus[(sku_slice_start + offset) % len(skus)] for offset in range(4)]

    for row_offset in range(n):
        global_id = group_idx * ROWS_PER_GROUP + row_offset
        ids.append(global_id)

        base = amount_base + rng.uniform(0.0, 180.0)
        trend = (row_offset % 25) * 0.35
        amount_values.append(round(base + trend, 2))

        region_values.append(region)

        if row_offset % 7 == 0:
            segment_values.append(segment_secondary)
        else:
            segment_values.append(segment_primary)

        if row_offset % 11 == 0:
            status_values.append(status_secondary)
        else:
            status_values.append(status_primary)

        if group_idx % 5 == 0:
            priority_values.append(None)
        else:
            if row_offset % 9 == 0:
                priority_values.append(None)
            else:
                priority_values.append((group_idx + row_offset) % 5 + 1)

        sku_values.append(group_skus[row_offset % len(group_skus)])
        event_day_values.append(19000 + (group_idx % 12))

        if row_offset % 17 == 0:
            score_values.append(None)
        elif row_offset % 19 == 0:
            score_values.append(float("nan"))
        else:
            score_values.append(round((amount_values[-1] / 10.0) + rng.uniform(-1.0, 1.0), 3))

    table = pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "amount": pa.array(amount_values, type=pa.float64()),
            "region": pa.array(region_values, type=pa.string()),
            "segment": pa.array(segment_values, type=pa.string()),
            "status": pa.array(status_values, type=pa.string()),
            "priority": pa.array(priority_values, type=pa.int64()),
            "sku": pa.array(sku_values, type=pa.string()),
            "event_day": pa.array(event_day_values, type=pa.int32()),
            "score": pa.array(score_values, type=pa.float64()),
        }
    )

    index_entry = {
        "row_group": group_idx,
        "num_rows": n,
        "values": {
            "region": sorted(set(region_values)),
            "segment": sorted(set(segment_values)),
            "status": sorted(set(status_values)),
            "sku": sorted(set(sku_values)),
            "event_day": sorted(set(event_day_values)),
        },
        "has_null": {
            "priority": any(v is None for v in priority_values),
            "score": any(v is None for v in score_values),
        },
        "has_nan": {
            "score": any(isinstance(v, float) and math.isnan(v) for v in score_values),
        },
    }

    return table, index_entry


def build_queries() -> list[dict]:
    return [
        {
            "id": "q1",
            "file": "sales.parquet",
            "columns": ["id", "amount", "region", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 40.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 150.0},
                    {"type": "in", "column": "region", "values": ["APAC"]},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "open"},
                ],
            },
            "max_row_groups_read": 4,
            "min_result_count": 1,
        },
        {
            "id": "q2",
            "file": "sales.parquet",
            "columns": ["id", "amount", "segment", "sku"],
            "predicate": {
                "type": "and",
                "children": [
                    {
                        "type": "or",
                        "children": [
                            {"type": "in", "column": "segment", "values": ["enterprise", "gov"]},
                            {"type": "in", "column": "sku", "values": ["SKU003", "SKU017"]},
                        ],
                    },
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 780.0},
                ],
            },
            "max_row_groups_read": 10,
            "min_result_count": 1,
        },
        {
            "id": "q3",
            "file": "sales.parquet",
            "columns": ["id", "region", "status", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {
                        "type": "not",
                        "child": {"type": "cmp", "column": "status", "op": "eq", "value": "fraud"},
                    },
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 260.0},
                ],
            },
            "max_row_groups_read": 6,
            "min_result_count": 1,
        },
        {
            "id": "q4",
            "file": "sales.parquet",
            "columns": ["id", "priority", "region", "segment"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "priority"},
                    {
                        "type": "or",
                        "children": [
                            {"type": "cmp", "column": "region", "op": "eq", "value": "AMER"},
                            {"type": "cmp", "column": "segment", "op": "eq", "value": "public"},
                        ],
                    },
                ],
            },
            "max_row_groups_read": 16,
            "min_result_count": 1,
        },
        {
            "id": "q5",
            "file": "sales.parquet",
            "columns": ["id", "amount", "score", "sku"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_not_null", "column": "score"},
                    {
                        "type": "not",
                        "child": {
                            "type": "or",
                            "children": [
                                {"type": "cmp", "column": "amount", "op": "lt", "value": 400.0},
                                {"type": "cmp", "column": "amount", "op": "gt", "value": 620.0},
                            ],
                        },
                    },
                    {"type": "in", "column": "sku", "values": ["SKU010", "SKU011", "SKU012"]},
                ],
            },
            "max_row_groups_read": 2,
            "min_result_count": 1,
        },
        {
            "id": "q6",
            "file": "sales.parquet",
            "columns": ["id", "event_day", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "event_day", "op": "ge", "value": 19010},
                    {"type": "cmp", "column": "event_day", "op": "le", "value": 19011},
                    {"type": "in", "column": "status", "values": ["hold", "pending"]},
                ],
            },
            "max_row_groups_read": 3,
            "min_result_count": 1,
        },
        {
            "id": "q7",
            "file": "sales.parquet",
            "columns": ["id", "region", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "not", "child": {"type": "in", "column": "region", "values": ["APAC"]}},
                    {
                        "type": "or",
                        "children": [
                            {"type": "in", "column": "priority", "values": [1, 2]},
                            {"type": "is_null", "column": "priority"},
                        ],
                    },
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 900.0},
                ],
            },
            "max_row_groups_read": 10,
            "min_result_count": 1,
        },
        {
            "id": "q8",
            "file": "sales.parquet",
            "columns": ["id", "segment", "sku", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "sku", "op": "eq", "value": "SKU031"},
                    {"type": "cmp", "column": "segment", "op": "ne", "value": "startup"},
                    {"type": "cmp", "column": "amount", "op": "le", "value": 1080.0},
                ],
            },
            "max_row_groups_read": 4,
            "min_result_count": 1,
        },
    ]


def main() -> None:
    parquet_path = os.path.join(OUT_DIR, "sales.parquet")
    index_path = os.path.join(OUT_DIR, "row_group_index.json")
    queries_path = os.path.join(OUT_DIR, "queries.json")

    writer = None
    index_entries = []
    try:
        for rg in range(ROW_GROUPS):
            table, entry = make_row_group(rg)
            index_entries.append(entry)
            if writer is None:
                writer = pq.ParquetWriter(parquet_path, table.schema, write_statistics=True)
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index_entries, f, indent=2)

    queries = build_queries()
    with open(queries_path, "w", encoding="utf-8") as f:
        json.dump(queries, f, indent=2)

    print(f"Generated {parquet_path} with {ROW_GROUPS} row groups")


if __name__ == "__main__":
    main()
