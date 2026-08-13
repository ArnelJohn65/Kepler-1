import json
import math
import os
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


def normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return v


def coerce_scalar(value: Any, dtype: pa.DataType) -> Any:
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_timestamp(dtype) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if node is None:
        return
    node_type = node["type"]
    if node_type in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if node_type in {"and", "or"}:
        for child in node["children"]:
            columns_in_predicate(child, output)
        return
    if node_type == "not":
        columns_in_predicate(node["child"], output)
        return
    raise ValueError(f"unsupported predicate node type: {node_type}")


def build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    node_type = node["type"]
    if node_type == "cmp":
        col = table.column(node["column"])
        value = coerce_scalar(node["value"], col.type)
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
        values = [coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(values, type=col.type))
    if node_type == "is_null":
        return pc.is_null(table.column(node["column"]))
    if node_type == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if node_type == "and":
        masks = [build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.and_(out, mask)
        return out
    if node_type == "or":
        masks = [build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.or_(out, mask)
        return out
    if node_type == "not":
        return pc.invert(build_mask(table, node["child"]))
    raise ValueError(f"unsupported predicate node type: {node_type}")


def apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(build_mask(table, predicate))


def load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def parquet_file() -> pq.ParquetFile:
    return pq.ParquetFile(os.path.join(DATA_DIR, "sales.parquet"))


def write_json(path: str, payload: Any, *, pretty: bool = False) -> None:
    with open(path, "w", encoding="utf-8") as f:
        if pretty:
            json.dump(payload, f, indent=2)
        else:
            json.dump(payload, f, sort_keys=True, separators=(",", ":"))


def weak_index_payload(parquet: pq.ParquetFile) -> dict[str, Any]:
    row_groups = []
    for row_group_index in range(parquet.metadata.num_row_groups):
        rg_meta = parquet.metadata.row_group(row_group_index)
        table = parquet.read_row_group(row_group_index)
        columns = {
            field.name: {
                "min": None,
                "max": None,
                "null_count": int(table.column(field.name).null_count),
                "distinct_values": None,
                "has_nan": False,
            }
            for field in parquet.schema_arrow
        }
        row_groups.append(
            {
                "row_group": row_group_index,
                "num_rows": rg_meta.num_rows,
                "columns": columns,
                "pair_distinct_values": {},
            }
        )
    return {"format": "row-group-index-v1", "parquet_file": "sales.parquet", "row_groups": row_groups}


def index_from_data(
    parquet: pq.ParquetFile,
    *,
    distinct_columns: set[str] | None,
    pair_columns: list[tuple[str, str]],
) -> dict[str, Any]:
    row_groups = []
    for row_group_index in range(parquet.metadata.num_row_groups):
        rg_meta = parquet.metadata.row_group(row_group_index)
        table = parquet.read_row_group(row_group_index)
        columns: dict[str, Any] = {}
        for field in parquet.schema_arrow:
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
            if distinct_columns and field.name in distinct_columns:
                distinct_values = sorted({normalize(v) for v in column.drop_null().to_pylist()}, key=repr)
            columns[field.name] = {
                "min": normalize(stats.min) if stats is not None and stats.has_min_max else None,
                "max": normalize(stats.max) if stats is not None and stats.has_min_max else None,
                "null_count": int(column.null_count),
                "distinct_values": distinct_values,
                "has_nan": pa.types.is_floating(field.type)
                and any(isinstance(v, float) and math.isnan(v) for v in column.to_pylist() if v is not None),
            }
        pair_distinct_values: dict[str, list[list[Any]]] = {}
        for left, right in pair_columns:
            pairs = sorted(
                {(
                    normalize(a),
                    normalize(b),
                ) for a, b in zip(table.column(left).to_pylist(), table.column(right).to_pylist())},
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
    return {"format": "row-group-index-v1", "parquet_file": "sales.parquet", "row_groups": row_groups}


def write_results(
    parquet: pq.ParquetFile,
    queries: list[dict[str, Any]],
    *,
    row_group_plans: dict[str, list[int]] | None = None,
) -> None:
    all_results: list[dict[str, Any]] = []
    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]
        read_columns = set(projection)
        columns_in_predicate(predicate, read_columns)

        rows: list[dict[str, Any]] = []
        row_group_indexes = row_group_plans.get(query["id"]) if row_group_plans is not None else None
        if row_group_indexes is None:
            row_group_indexes = list(range(parquet.metadata.num_row_groups))
        for row_group_index in row_group_indexes:
            decoded = parquet.read_row_group(row_group_index, columns=sorted(read_columns))
            filtered = apply_predicate(decoded, predicate)
            rows.extend({k: normalize(v) for k, v in row.items()} for row in filtered.select(projection).to_pylist())
        all_results.append({"query_id": query["id"], "rows": rows})
    write_json(RESULTS_PATH, all_results, pretty=True)


def exact_visible_plans(parquet: pq.ParquetFile, queries: list[dict[str, Any]]) -> dict[str, list[int]]:
    plans: dict[str, list[int]] = {}
    for query in queries:
        predicate = query.get("predicate")
        read_columns = set(query["columns"])
        columns_in_predicate(predicate, read_columns)
        matching_groups: list[int] = []
        for row_group_index in range(parquet.metadata.num_row_groups):
            decoded = parquet.read_row_group(row_group_index, columns=sorted(read_columns))
            if apply_predicate(decoded, predicate).num_rows > 0:
                matching_groups.append(row_group_index)
        plans[query["id"]] = matching_groups
    return plans
