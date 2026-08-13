import json
import os
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
OUT_DIR = os.path.join(APP_ROOT, "data")
os.makedirs(OUT_DIR, exist_ok=True)

RANDOM_SEED = 20260813
ROW_GROUPS = 192
ROWS_PER_GROUP = 640

regions = ["APAC", "EMEA", "AMER"]
segments = ["retail", "enterprise", "public", "startup", "midmarket", "gov"]
statuses = ["open", "closed", "pending", "hold", "fraud"]
skus = [f"SKU{i:03d}" for i in range(1, 81)]
channels = ["web", "mobile", "partner"]


def make_row_group(group_idx: int) -> pa.Table:
    rng = random.Random(RANDOM_SEED + group_idx)

    amount_band = group_idx % 12
    amount_base = amount_band * 110.0

    region = regions[group_idx % len(regions)]
    segment_primary = segments[group_idx % len(segments)]
    segment_secondary = segments[(group_idx + 2) % len(segments)]
    status_primary = statuses[group_idx % len(statuses)]
    status_secondary = statuses[(group_idx + 3) % len(statuses)]

    sku_slice_start = (group_idx * 5) % len(skus)
    group_skus = [skus[(sku_slice_start + offset) % len(skus)] for offset in range(6)]

    ids: list[int] = []
    amount_values: list[float] = []
    region_values: list[str] = []
    segment_values: list[str] = []
    status_values: list[str] = []
    priority_values: list[int | None] = []
    sku_values: list[str] = []
    event_day_values: list[int] = []
    score_values: list[float | None] = []
    channel_values: list[str | None] = []
    event_ts_values: list[datetime] = []
    amount_dec_values: list[Decimal] = []

    for row_offset in range(ROWS_PER_GROUP):
        global_id = group_idx * ROWS_PER_GROUP + row_offset
        ids.append(global_id)

        base = amount_base + rng.uniform(0.0, 95.0)
        trend = (row_offset % 31) * 0.27
        amount = round(base + trend, 2)
        amount_values.append(amount)

        region_values.append(region)

        if row_offset % 2 == 0:
            segment = segment_primary
            status = status_primary
        else:
            segment = segment_secondary
            status = status_secondary
        segment_values.append(segment)
        status_values.append(status)

        if group_idx % 7 == 0:
            priority_values.append(None)
        elif row_offset % 13 == 0:
            priority_values.append(None)
        else:
            priority_values.append((group_idx + row_offset) % 5 + 1)

        sku_values.append(group_skus[row_offset % len(group_skus)])

        event_day = 19500 + (group_idx % 24)
        event_day_values.append(event_day)

        if row_offset % 17 == 0:
            score_values.append(None)
        elif row_offset % 19 == 0:
            score_values.append(float("nan"))
        else:
            score_values.append(round((amount / 9.0) + rng.uniform(-1.0, 1.0), 3))

        if group_idx % 9 == 0:
            channel_values.append(None)
        elif row_offset % 10 == 0:
            channel_values.append(None)
        else:
            channel_values.append(channels[(group_idx + row_offset) % len(channels)])

        ts = datetime(2025, 1, 1, tzinfo=UTC) + timedelta(days=(group_idx % 30), minutes=row_offset)
        event_ts_values.append(ts)

        amount_dec_values.append(Decimal(f"{amount:.2f}"))

    return pa.table(
        {
            "id": pa.array(ids, type=pa.int64()),
            "amount": pa.array(amount_values, type=pa.float64()),
            "amount_dec": pa.array(amount_dec_values, type=pa.decimal128(12, 2)),
            "region": pa.array(region_values, type=pa.string()),
            "segment": pa.array(segment_values, type=pa.string()),
            "status": pa.array(status_values, type=pa.string()),
            "priority": pa.array(priority_values, type=pa.int64()),
            "sku": pa.array(sku_values, type=pa.string()),
            "event_day": pa.array(event_day_values, type=pa.int32()),
            "event_ts": pa.array(event_ts_values, type=pa.timestamp("ms", tz="UTC")),
            "score": pa.array(score_values, type=pa.float64()),
            "channel": pa.array(channel_values, type=pa.string()),
        }
    )


def build_queries() -> list[dict]:
    return [
        {
            "id": "q1",
            "file": "sales.parquet",
            "columns": ["id", "amount", "region", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 20.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 90.0},
                    {"type": "in", "column": "region", "values": ["APAC"]},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "hold"},
                ],
            },
            "max_row_groups_read": 9,
            "min_result_count": 1,
        },
        {
            "id": "q2",
            "file": "sales.parquet",
            "columns": ["id", "amount", "segment", "status", "sku"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 760.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 940.0},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "enterprise"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "pending"},
                ],
            },
            "max_row_groups_read": 8,
            "min_result_count": 1,
        },
        {
            "id": "q3",
            "file": "sales.parquet",
            "columns": ["id", "channel", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {
                        "type": "not",
                        "child": {
                            "type": "or",
                            "children": [
                                {"type": "in", "column": "channel", "values": ["web", "partner"]},
                                {"type": "in", "column": "priority", "values": [1, 2]},
                            ],
                        },
                    },
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 500.0},
                ],
            },
            "max_row_groups_read": 128,
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
            "max_row_groups_read": 96,
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
            "max_row_groups_read": 6,
            "min_result_count": 1,
        },
        {
            "id": "q6",
            "file": "sales.parquet",
            "columns": ["id", "event_day", "status", "event_ts"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "event_day", "op": "ge", "value": 19518},
                    {"type": "cmp", "column": "event_day", "op": "le", "value": 19519},
                    {"type": "in", "column": "status", "values": ["hold", "pending"]},
                ],
            },
            "max_row_groups_read": 14,
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
            "max_row_groups_read": 48,
            "min_result_count": 1,
        },
        {
            "id": "q8",
            "file": "sales.parquet",
            "columns": ["id", "segment", "sku", "amount", "amount_dec"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "sku", "op": "eq", "value": "SKU031"},
                    {"type": "cmp", "column": "segment", "op": "ne", "value": "startup"},
                    {"type": "cmp", "column": "amount_dec", "op": "le", "value": "1080.00"},
                ],
            },
            "max_row_groups_read": 22,
            "min_result_count": 1,
        },
        {
            "id": "q9",
            "file": "sales.parquet",
            "columns": ["id", "segment", "status", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "startup"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "pending"},
                    {"type": "is_null", "column": "priority"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 200.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 360.0},
                ],
            },
            "max_row_groups_read": 12,
            "min_result_count": 1,
        },
        {
            "id": "q10",
            "file": "sales.parquet",
            "columns": ["id", "region", "channel", "priority", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                    {
                        "type": "not",
                        "child": {
                            "type": "or",
                            "children": [
                                {"type": "in", "column": "channel", "values": ["mobile"]},
                                {"type": "in", "column": "status", "values": ["fraud"]},
                                {"type": "in", "column": "priority", "values": [3, 4]},
                            ],
                        },
                    },
                ],
            },
            "max_row_groups_read": 64,
            "min_result_count": 1,
        },
        {
            "id": "q11",
            "file": "sales.parquet",
            "columns": ["id", "amount", "region"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 0.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 2000.0},
                ],
            },
            "max_row_groups_read": ROW_GROUPS,
            "min_result_count": 1,
        },
        {
            "id": "q12",
            "file": "sales.parquet",
            "columns": ["id", "score", "amount"],
            "predicate": {
                "type": "not",
                "child": {
                    "type": "or",
                    "children": [
                        {"type": "cmp", "column": "score", "op": "lt", "value": 0.0},
                        {"type": "cmp", "column": "score", "op": "ge", "value": 0.0},
                    ],
                },
            },
            "max_row_groups_read": ROW_GROUPS,
            "min_result_count": 1,
        },
    ]


def main() -> None:
    parquet_path = os.path.join(OUT_DIR, "sales.parquet")
    queries_path = os.path.join(OUT_DIR, "queries.json")

    writer = None
    try:
        for rg in range(ROW_GROUPS):
            table = make_row_group(rg)
            if writer is None:
                writer = pq.ParquetWriter(
                    parquet_path,
                    table.schema,
                    write_statistics=[
                        "id",
                        "amount",
                        "amount_dec",
                        "region",
                        "segment",
                        "status",
                        "sku",
                        "event_day",
                        "event_ts",
                    ],
                    use_dictionary=True,
                )
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    queries = build_queries()
    with open(queries_path, "w", encoding="utf-8") as f:
        json.dump(queries, f, indent=2)

    print(f"Generated {parquet_path} with {ROW_GROUPS} row groups")


if __name__ == "__main__":
    main()
