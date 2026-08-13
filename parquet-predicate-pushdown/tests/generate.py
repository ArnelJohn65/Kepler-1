import json
import os
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq

from verifier_helpers import (
    apply_predicate,
    build_reference_index,
    columns_in_predicate,
    index_json_bytes,
    may_index_row_group_match,
)

TESTS_ROOT = os.environ.get("TESTS_ROOT", "/tests")
OUT_DIR = os.path.join(TESTS_ROOT, "data")
os.makedirs(OUT_DIR, exist_ok=True)

RANDOM_SEED = 20260813
ROW_GROUPS = 192
ROWS_PER_GROUP = 1024

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


def build_visible_queries() -> list[dict]:
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
        },
        {
            "id": "q3",
            "file": "sales.parquet",
            "columns": ["id", "event_day", "region", "status", "event_ts"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "event_day", "op": "eq", "value": 19507},
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "closed"},
                ],
            },
        },
        {
            "id": "q4",
            "file": "sales.parquet",
            "columns": ["id", "priority", "region", "segment", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "priority"},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "gov"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 330.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 470.0},
                ],
            },
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
        },
        {
            "id": "q6",
            "file": "sales.parquet",
            "columns": ["id", "event_day", "status", "region", "event_ts"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "event_day", "op": "ge", "value": 19518},
                    {"type": "cmp", "column": "event_day", "op": "le", "value": 19519},
                    {"type": "in", "column": "status", "values": ["hold", "pending"]},
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                ],
            },
        },
        {
            "id": "q7",
            "file": "sales.parquet",
            "columns": ["id", "region", "segment", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "not", "child": {"type": "in", "column": "region", "values": ["APAC"]}},
                    {"type": "in", "column": "priority", "values": [1, 2]},
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 900.0},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "enterprise"},
                ],
            },
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
        },
        {
            "id": "q10",
            "file": "sales.parquet",
            "columns": ["id", "region", "segment", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "fraud"},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "enterprise"},
                ],
            },
        },
        {
            "id": "q11",
            "file": "sales.parquet",
            "columns": ["id", "amount", "region", "sku", "event_day"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "sku", "op": "eq", "value": "SKU001"},
                    {"type": "cmp", "column": "event_day", "op": "eq", "value": 19500},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 0.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 150.0},
                ],
            },
        },
        {
            "id": "q12",
            "file": "sales.parquet",
            "columns": ["id", "score", "amount", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "score"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "hold"},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 100.0},
                ],
            },
        },
    ]


def build_hidden_queries() -> list[dict]:
    return [
        {
            "id": "h1",
            "columns": ["id", "segment", "status", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "enterprise"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "pending"},
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 600.0},
                ],
            },
        },
        {
            "id": "h2",
            "columns": ["id", "segment", "status", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "retail"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "pending"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 400.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 520.0},
                ],
            },
        },
        {
            "id": "h3",
            "columns": ["id", "priority", "segment", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "priority"},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "midmarket"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 200.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 260.0},
                ],
            },
        },
        {
            "id": "h4",
            "columns": ["id", "score", "status", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "score"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "hold"},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 100.0},
                ],
            },
        },
        {
            "id": "h5",
            "columns": ["id", "segment", "status", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "not", "child": {"type": "in", "column": "region", "values": ["APAC"]}},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "startup"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "fraud"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 100.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 200.0},
                ],
            },
        },
        {
            "id": "h6",
            "columns": ["id", "priority", "region", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "in", "column": "priority", "values": [4, 5]},
                    {"type": "cmp", "column": "region", "op": "eq", "value": "EMEA"},
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 900.0},
                ],
            },
        },
        {
            "id": "h7",
            "columns": ["id", "channel", "region", "event_day"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "is_null", "column": "channel"},
                    {"type": "cmp", "column": "region", "op": "eq", "value": "APAC"},
                    {"type": "cmp", "column": "event_day", "op": "eq", "value": 19509},
                ],
            },
        },
        {
            "id": "h8",
            "columns": ["id", "segment", "status", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "gov"},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "closed"},
                    {"type": "is_null", "column": "priority"},
                    {"type": "cmp", "column": "amount", "op": "ge", "value": 300.0},
                    {"type": "cmp", "column": "amount", "op": "lt", "value": 420.0},
                ],
            },
        },
        {
            "id": "h9",
            "columns": ["id", "sku", "event_day", "status"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "sku", "op": "eq", "value": "SKU001"},
                    {"type": "cmp", "column": "event_day", "op": "eq", "value": 19516},
                    {"type": "cmp", "column": "status", "op": "eq", "value": "closed"},
                ],
            },
        },
        {
            "id": "h10",
            "columns": ["id", "amount_dec", "sku", "segment"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "cmp", "column": "sku", "op": "eq", "value": "SKU006"},
                    {"type": "cmp", "column": "event_day", "op": "eq", "value": 19501},
                    {"type": "cmp", "column": "amount_dec", "op": "lt", "value": "180.00"},
                ],
            },
        },
        {
            "id": "h11",
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
                                {"type": "cmp", "column": "amount", "op": "lt", "value": 510.0},
                                {"type": "cmp", "column": "amount", "op": "gt", "value": 630.0},
                            ],
                        },
                    },
                    {"type": "in", "column": "sku", "values": ["SKU039", "SKU040", "SKU041"]},
                ],
            },
        },
        {
            "id": "h12",
            "columns": ["id", "region", "segment", "priority", "amount"],
            "predicate": {
                "type": "and",
                "children": [
                    {"type": "not", "child": {"type": "in", "column": "region", "values": ["APAC"]}},
                    {"type": "in", "column": "priority", "values": [1, 2]},
                    {"type": "cmp", "column": "amount", "op": "gt", "value": 900.0},
                    {"type": "cmp", "column": "segment", "op": "eq", "value": "enterprise"},
                ],
            },
        },
    ]


def add_precision_ceilings(parquet_path: str, hidden_queries: list[dict]) -> list[dict]:
    parquet_file = pq.ParquetFile(parquet_path)
    schema = {field.name: field.type for field in parquet_file.schema_arrow}
    index_payload = build_reference_index(parquet_file)

    for query in hidden_queries:
        predicate = query.get("predicate")
        matching_groups = 0
        surviving_groups = 0
        read_columns = set(query["columns"])
        columns_in_predicate(predicate, read_columns)
        for row_group_entry in index_payload["row_groups"]:
            if may_index_row_group_match(row_group_entry, schema, predicate):
                surviving_groups += 1
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(row_group_index, columns=sorted(read_columns))
            if apply_predicate(decoded, predicate).num_rows > 0:
                matching_groups += 1

        assert matching_groups > 0, f"{query['id']} must match at least one row group"
        assert surviving_groups >= matching_groups, f"{query['id']} reference index must be sound"
        query["max_surviving_row_groups"] = surviving_groups

    return hidden_queries


def main() -> None:
    parquet_path = os.path.join(OUT_DIR, "sales.parquet")
    visible_queries_path = os.path.join(OUT_DIR, "queries.json")
    hidden_queries_path = os.path.join(OUT_DIR, "hidden_queries.json")

    writer = None
    try:
        for row_group_index in range(ROW_GROUPS):
            table = make_row_group(row_group_index)
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

    visible_queries = build_visible_queries()
    hidden_queries = add_precision_ceilings(parquet_path, build_hidden_queries())

    with open(visible_queries_path, "w", encoding="utf-8") as f:
        json.dump(visible_queries, f, indent=2)
    with open(hidden_queries_path, "w", encoding="utf-8") as f:
        json.dump(hidden_queries, f, indent=2)

    reference_index_size = len(index_json_bytes(build_reference_index(pq.ParquetFile(parquet_path))))
    print(f"Generated {parquet_path} with {ROW_GROUPS} row groups")
    print(f"Reference index size: {reference_index_size} bytes")


if __name__ == "__main__":
    main()
