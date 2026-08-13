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


def _build_index(parquet_file: pq.ParquetFile) -> dict[str, Any]:
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

    return {
        "format": "row-group-index-v1",
        "parquet_file": "sales.parquet",
        "row_groups": row_groups,
    }


def _decode_bound(value: Any, dtype: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_timestamp(dtype):
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return value


def _nonnull_only(summary: dict[str, Any]) -> bool:
    return summary["null_count"] == 0


def _all_nulls(summary: dict[str, Any], num_rows: int) -> bool:
    return summary["null_count"] >= num_rows > 0


def _flatten_and(node: dict[str, Any]) -> list[dict[str, Any]]:
    if node["type"] != "and":
        return [node]
    out: list[dict[str, Any]] = []
    for child in node["children"]:
        out.extend(_flatten_and(child))
    return out


def _allowed_sets_for_and(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], node: dict[str, Any]) -> dict[str, set[Any]]:
    del row_group_entry
    allowed: dict[str, set[Any]] = {}
    for leaf in _flatten_and(node):
        leaf_type = leaf["type"]
        column = leaf.get("column")
        if column is None:
            continue
        dtype = schema[column]
        if leaf_type == "cmp" and leaf["op"] == "eq":
            values = {_normalize(_coerce_scalar(leaf["value"], dtype))}
        elif leaf_type == "in":
            values = {_normalize(_coerce_scalar(value, dtype)) for value in leaf["values"]}
        elif leaf_type == "is_null":
            values = {None}
        else:
            continue
        if column in allowed:
            allowed[column] &= values
        else:
            allowed[column] = set(values)
    return allowed


def _pair_feasible(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], node: dict[str, Any]) -> bool:
    if node["type"] != "and":
        return True
    allowed = _allowed_sets_for_and(row_group_entry, schema, node)
    for pair_key, pairs in row_group_entry["pair_distinct_values"].items():
        left, right = pair_key.split("|", 1)
        if left not in allowed or right not in allowed:
            continue
        exact_pairs = {tuple(pair) for pair in pairs}
        if not any((left_value, right_value) in exact_pairs for left_value in allowed[left] for right_value in allowed[right]):
            return False
    return True


def _leaf_always_true(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], node: dict[str, Any]) -> bool:
    column = node["column"]
    summary = row_group_entry["columns"][column]
    dtype = schema[column]
    num_rows = row_group_entry["num_rows"]
    distinct_values = summary["distinct_values"]
    has_nan = summary["has_nan"]
    node_type = node["type"]

    if node_type == "is_null":
        return summary["null_count"] == num_rows
    if node_type == "is_not_null":
        return summary["null_count"] == 0
    if node_type == "in":
        if not _nonnull_only(summary) or distinct_values is None:
            return False
        allowed = {_normalize(_coerce_scalar(value, dtype)) for value in node["values"]}
        return set(distinct_values).issubset(allowed) and len(distinct_values) > 0
    if node_type != "cmp":
        return False

    exact_value = _normalize(_coerce_scalar(node["value"], dtype))
    typed_value = _coerce_scalar(node["value"], dtype)
    min_value = _decode_bound(summary["min"], dtype)
    max_value = _decode_bound(summary["max"], dtype)
    op = node["op"]

    if not _nonnull_only(summary):
        return False
    if op == "eq":
        if has_nan:
            return False
        return distinct_values == [exact_value] if distinct_values is not None else min_value == max_value == typed_value
    if op == "ne":
        if has_nan:
            return True
        return exact_value not in distinct_values if distinct_values is not None else (
            min_value is not None and max_value is not None and (typed_value < min_value or typed_value > max_value)
        )
    if min_value is None or max_value is None or has_nan:
        return False
    if op == "lt":
        return max_value < typed_value
    if op == "le":
        return max_value <= typed_value
    if op == "gt":
        return min_value > typed_value
    if op == "ge":
        return min_value >= typed_value
    return False


def _cmp_may_true(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], column: str, op: str, value: Any) -> bool:
    summary = row_group_entry["columns"][column]
    dtype = schema[column]
    num_rows = row_group_entry["num_rows"]
    exact_value = _normalize(_coerce_scalar(value, dtype))
    typed_value = _coerce_scalar(value, dtype)
    min_value = _decode_bound(summary["min"], dtype)
    max_value = _decode_bound(summary["max"], dtype)
    distinct_values = summary["distinct_values"]
    has_nan = summary["has_nan"]

    if _all_nulls(summary, num_rows):
        return False
    if op == "eq" and distinct_values is not None and exact_value not in distinct_values:
        return False
    if op == "ne":
        if has_nan:
            return True
        if distinct_values is not None and distinct_values == [exact_value] and _nonnull_only(summary):
            return False
    if min_value is None or max_value is None:
        return True
    if op == "eq":
        return not (typed_value < min_value or typed_value > max_value)
    if op == "ne":
        return not (min_value == max_value == typed_value and _nonnull_only(summary) and not has_nan)
    if op == "lt":
        return min_value < typed_value
    if op == "le":
        return min_value <= typed_value
    if op == "gt":
        return max_value > typed_value
    if op == "ge":
        return max_value >= typed_value
    return True


def _in_may_true(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], column: str, values: list[Any]) -> bool:
    summary = row_group_entry["columns"][column]
    dtype = schema[column]
    num_rows = row_group_entry["num_rows"]
    canonical_values = {_normalize(_coerce_scalar(value, dtype)) for value in values}
    typed_values = [_coerce_scalar(value, dtype) for value in values]
    distinct_values = summary["distinct_values"]
    min_value = _decode_bound(summary["min"], dtype)
    max_value = _decode_bound(summary["max"], dtype)

    if _all_nulls(summary, num_rows):
        return False
    if distinct_values is not None and set(distinct_values).isdisjoint(canonical_values):
        return False
    if min_value is not None and max_value is not None and all(value < min_value or value > max_value for value in typed_values):
        return False
    return True


def _may_be_false(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], node: dict[str, Any]) -> bool:
    node_type = node["type"]
    if node_type == "and":
        return any(_may_be_false(row_group_entry, schema, child) for child in node["children"])
    if node_type == "or":
        return all(_may_be_false(row_group_entry, schema, child) for child in node["children"])
    if node_type == "not":
        return _may_be_true(row_group_entry, schema, node["child"])
    return not _leaf_always_true(row_group_entry, schema, node)


def _may_be_true(row_group_entry: dict[str, Any], schema: dict[str, pa.DataType], predicate: dict[str, Any] | None) -> bool:
    if predicate is None:
        return True
    node_type = predicate["type"]
    if node_type == "and":
        if not all(_may_be_true(row_group_entry, schema, child) for child in predicate["children"]):
            return False
        return _pair_feasible(row_group_entry, schema, predicate)
    if node_type == "or":
        return any(_may_be_true(row_group_entry, schema, child) for child in predicate["children"])
    if node_type == "not":
        return _may_be_false(row_group_entry, schema, predicate["child"])
    if node_type == "cmp":
        return _cmp_may_true(row_group_entry, schema, predicate["column"], predicate["op"], predicate["value"])
    if node_type == "in":
        return _in_may_true(row_group_entry, schema, predicate["column"], predicate["values"])
    if node_type == "is_null":
        return row_group_entry["columns"][predicate["column"]]["null_count"] > 0
    if node_type == "is_not_null":
        return row_group_entry["columns"][predicate["column"]]["null_count"] < row_group_entry["num_rows"]
    raise ValueError(f"unsupported predicate node type: {node_type}")


def build_index() -> None:
    parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, "sales.parquet"))
    payload = _build_index(parquet_file)
    with open(INDEX_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, sort_keys=True, separators=(",", ":"))
    print(f"Wrote {INDEX_PATH}")


def run_query_pass() -> None:
    if not os.path.exists(INDEX_PATH):
        raise RuntimeError(f"Missing build-pass index: {INDEX_PATH}")

    queries = _load_queries()
    if not queries:
        raise RuntimeError("No visible queries available")

    parquet_file = pq.ParquetFile(os.path.join(DATA_DIR, "sales.parquet"))
    schema = {field.name: field.type for field in parquet_file.schema_arrow}
    with open(INDEX_PATH, encoding="utf-8") as f:
        index_payload = json.load(f)

    all_results: list[dict[str, Any]] = []
    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]
        read_columns = set(projection)
        _columns_in_predicate(predicate, read_columns)

        rows: list[dict[str, Any]] = []
        for entry in index_payload["row_groups"]:
            if not _may_be_true(entry, schema, predicate):
                continue
            decoded = parquet_file.read_row_group(entry["row_group"], columns=sorted(read_columns))
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
        return
    if mode == "query":
        run_query_pass()
        return
    if mode == "all":
        build_index()
        run_query_pass()
        return
    raise SystemExit("Usage: python solve.py [build|query|all]")


if __name__ == "__main__":
    main()
