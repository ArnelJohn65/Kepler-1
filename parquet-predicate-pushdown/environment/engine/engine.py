"""
Baseline engine scaffold with two-pass execution.

build: scan every row group once and persist a plain-data JSON index.
query: load that index, scan visible queries, and write /app/results.json.

This baseline intentionally does no pruning in query mode.
"""

import json
import math
import os
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
INDEX_PATH = os.path.join(APP_ROOT, "row_group_index.json")
PAIR_INDEX_COLUMNS = [("segment", "status"), ("region", "channel"), ("sku", "event_day")]


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return v


def _coerce_scalar(value: Any, dtype: pa.DataType) -> Any:
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_timestamp(dtype) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


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
    raise ValueError(f"unsupported predicate node type: {node_type}")


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    node_type = node["type"]
    if node_type == "cmp":
        col = table.column(node["column"])
        value = _coerce_scalar(node["value"], col.type)
        op = node["op"]
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
        raise ValueError(f"unsupported cmp op: {op}")
    if node_type == "in":
        col = table.column(node["column"])
        values = [_coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(values, type=col.type))
    if node_type == "is_null":
        return pc.is_null(table.column(node["column"]))
    if node_type == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if node_type == "and":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.and_(out, mask)
        return out
    if node_type == "or":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.or_(out, mask)
        return out
    if node_type == "not":
        return pc.invert(_build_mask(table, node["child"]))
    raise ValueError(f"unsupported predicate node type: {node_type}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def build_index() -> None:
    parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, "sales.parquet"))
    row_groups: list[dict[str, Any]] = []
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        rg_meta = parquet_file.metadata.row_group(row_group_index)
        table = parquet_file.read_row_group(row_group_index)
        columns: dict[str, Any] = {}
        for field in parquet_file.schema_arrow:
            column = table.column(field.name)
            col_meta = None
            for col_idx in range(rg_meta.num_columns):
                probe = rg_meta.column(col_idx)
                if probe.path_in_schema == field.name:
                    col_meta = probe
                    break
            if col_meta is None:
                raise RuntimeError(f"missing column metadata for {field.name}")
            stats = col_meta.statistics
            distinct_values = None
            if pa.types.is_string(field.type) or pa.types.is_integer(field.type):
                distinct_values = sorted({_normalize(v) for v in column.drop_null().to_pylist()}, key=repr)
            columns[field.name] = {
                "min": _normalize(stats.min) if stats is not None and stats.has_min_max else None,
                "max": _normalize(stats.max) if stats is not None and stats.has_min_max else None,
                "null_count": int(column.null_count),
                "distinct_values": distinct_values,
                "has_nan": pa.types.is_floating(field.type)
                and any(isinstance(v, float) and math.isnan(v) for v in column.to_pylist() if v is not None),
            }
        pair_distinct_values: dict[str, list[list[Any]]] = {}
        for left, right in PAIR_INDEX_COLUMNS:
            pairs = sorted(
                {(_normalize(a), _normalize(b)) for a, b in zip(table.column(left).to_pylist(), table.column(right).to_pylist())},
                key=repr,
            )
            pair_distinct_values[f"{left}|{right}"] = [[left_value, right_value] for left_value, right_value in pairs]
        row_groups.append(
            {
                "row_group": row_group_index,
                "num_rows": rg_meta.num_rows,
                "columns": columns,
                "pair_distinct_values": pair_distinct_values,
            }
        )

    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump({"format": "row-group-index-v1", "parquet_file": "sales.parquet", "row_groups": row_groups}, f)
    print(f"Wrote {INDEX_PATH}")


def run_queries() -> None:
    queries = _load_queries()
    if not queries:
        raise RuntimeError("No visible queries available")
    parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, "sales.parquet"))

    all_results: list[dict[str, Any]] = []
    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]
        read_columns = set(projection)
        _columns_in_predicate(predicate, read_columns)

        rows: list[dict[str, Any]] = []
        for row_group_index in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(row_group_index, columns=sorted(read_columns))
            filtered = _apply_predicate(decoded, predicate)
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in filtered.select(projection).to_pylist())
        all_results.append({"query_id": query["id"], "rows": rows})

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    print(f"Wrote {RESULTS_PATH}")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "query"
    if mode == "build":
        build_index()
    elif mode == "query":
        run_queries()
    elif mode == "all":
        build_index()
        run_queries()
    else:
        raise SystemExit("Usage: python engine.py [build|query|all]")


if __name__ == "__main__":
    main()
