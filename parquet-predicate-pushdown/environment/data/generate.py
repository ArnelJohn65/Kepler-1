import json
import os
import random
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
OUT_DIR = os.path.join(APP_ROOT, "data")
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


def _coerce_scalar(value: Any, dtype: pa.DataType) -> Any:
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_timestamp(dtype) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        value = _coerce_scalar(node["value"], col.type)
        if op == "eq":
            return pc.equal(col, value)
        if op == "ne":
            return pc.not_equal(col, value)
        if op == "lt":
            return pc.less(col, value)
        if op == "le":
            return pc.less_equal(col, value)
        if op == "gt":
            return pc.greater(col, value)
        if op == "ge":
            return pc.greater_equal(col, value)
        raise AssertionError(f"unsupported cmp op in query spec: {op}")

    if t == "in":
        col = table.column(node["column"])
        coerced = [_coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(coerced, type=col.type))

    if t == "is_null":
        return pc.is_null(table.column(node["column"]))

    if t == "is_not_null":
        return pc.is_valid(table.column(node["column"]))

    if t == "and":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.and_(out, mask)
        return out

    if t == "or":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.or_(out, mask)
        return out

    if t == "not":
        return pc.invert(_build_mask(table, node["child"]))

    raise AssertionError(f"unsupported node type in query spec: {t}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if node is None:
        return
    node_type = node["type"]
    if node_type in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if node_type in {"and", "or"}:
        for child in node["children"]:
            _columns_in_predicate(child, output)
        return
    if node_type == "not":
        _columns_in_predicate(node["child"], output)
        return
    raise AssertionError(f"unknown predicate node type in query spec: {node_type}")


def build_queries() -> list[dict[str, Any]]:
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
            "budget_slack": 2,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 3,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 3,
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
            "budget_slack": 4,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 2,
            "min_result_count": 1,
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
            "budget_slack": 2,
            "min_result_count": 1,
        },
    ]


def finalize_query_budgets(parquet_path: str, queries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    pf = pq.ParquetFile(parquet_path)
    total_row_groups = pf.metadata.num_row_groups

    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]

        required = set(projection)
        _columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        matching_groups = 0
        max_group_bytes = 0

        for rg_idx in range(total_row_groups):
            decoded = pf.read_row_group(rg_idx, columns=read_columns)
            max_group_bytes = max(max_group_bytes, decoded.nbytes)
            if _apply_predicate(decoded, predicate).num_rows > 0:
                matching_groups += 1

        assert matching_groups > 0, f"{query['id']} must have at least one matching row group"

        max_reads = matching_groups + int(query.pop("budget_slack"))
        assert max_reads < total_row_groups, f"{query['id']} max_row_groups_read must be less than total row groups"
        assert max_reads <= max(1, total_row_groups // 5), f"{query['id']} max_row_groups_read is too loose"

        query["max_row_groups_read"] = max_reads
        query["max_decoded_bytes"] = max_reads * max_group_bytes

    return queries


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

    queries = finalize_query_budgets(parquet_path, build_queries())
    with open(queries_path, "w", encoding="utf-8") as f:
        json.dump(queries, f, indent=2)

    print(f"Generated {parquet_path} with {ROW_GROUPS} row groups")


if __name__ == "__main__":
    main()
