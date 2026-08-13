import json
import math
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

INDEX_FORMAT = "row-group-index-v1"
PAIR_INDEX_COLUMNS = [("segment", "status"), ("region", "channel"), ("sku", "event_day")]
PAIR_KEY_RE = re.compile(r"^[^|]+\|[^|]+$")


def normalize_for_receipt(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return v


def normalize_for_index(v: Any) -> Any:
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v


def index_json_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


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
    raise AssertionError(f"unknown predicate node type in query spec: {node_type}")


def build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    node_type = node["type"]

    if node_type == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        value = coerce_scalar(node["value"], col.type)
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

    if node_type == "in":
        col = table.column(node["column"])
        coerced = [coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(coerced, type=col.type))

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

    raise AssertionError(f"unsupported node type in query spec: {node_type}")


def apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(build_mask(table, predicate))


def receipt_for_table(table: pa.Table) -> str:
    import hashlib

    h = hashlib.blake2b(digest_size=16)
    h.update(f"rows={table.num_rows}".encode("utf-8"))
    for row in table.to_pylist():
        payload = json.dumps({k: normalize_for_receipt(v) for k, v in row.items()}, sort_keys=True, separators=(",", ":"))
        h.update(payload.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def canonicalize_value(value: Any, dtype: pa.DataType) -> Any:
    if value is None:
        return None

    if pa.types.is_timestamp(dtype):
        if isinstance(value, datetime):
            dt = value
        elif isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            raise AssertionError(f"timestamp value has unsupported type: {type(value)}")
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")

    if pa.types.is_decimal(dtype):
        if isinstance(value, Decimal):
            dec = value
        else:
            dec = Decimal(str(value))
        return format(dec, "f")

    if pa.types.is_floating(dtype):
        if value == "NaN":
            return "NaN"
        fv = float(value)
        if math.isnan(fv):
            return "NaN"
        return fv

    if pa.types.is_integer(dtype):
        if isinstance(value, bool):
            raise AssertionError("boolean where integer expected")
        return int(value)

    return value


def canonicalize_row(row: dict[str, Any], schema: dict[str, pa.DataType]) -> dict[str, Any]:
    return {k: canonicalize_value(row[k], schema[k]) for k in row}


def build_reference_index(parquet_file: pq.ParquetFile) -> dict[str, Any]:
    row_groups: list[dict[str, Any]] = []

    for row_group_index in range(parquet_file.metadata.num_row_groups):
        rg_meta = parquet_file.metadata.row_group(row_group_index)
        table = parquet_file.read_row_group(row_group_index)
        column_summaries: dict[str, Any] = {}

        for field in parquet_file.schema_arrow:
            column = table.column(field.name)
            col_meta = None
            for col_idx in range(rg_meta.num_columns):
                probe = rg_meta.column(col_idx)
                if probe.path_in_schema == field.name:
                    col_meta = probe
                    break
            assert col_meta is not None, f"missing column metadata for {field.name}"
            stats = col_meta.statistics
            distinct_values = None
            if pa.types.is_string(field.type) or pa.types.is_integer(field.type):
                distinct_values = sorted({normalize_for_index(v) for v in column.drop_null().to_pylist()}, key=repr)
            column_summaries[field.name] = {
                "min": normalize_for_index(stats.min) if stats is not None and stats.has_min_max else None,
                "max": normalize_for_index(stats.max) if stats is not None and stats.has_min_max else None,
                "null_count": int(column.null_count),
                "distinct_values": distinct_values,
                "has_nan": pa.types.is_floating(field.type)
                and any(isinstance(v, float) and math.isnan(v) for v in column.to_pylist() if v is not None),
            }

        pair_distinct_values: dict[str, list[list[Any]]] = {}
        for left, right in PAIR_INDEX_COLUMNS:
            left_values = table.column(left).to_pylist()
            right_values = table.column(right).to_pylist()
            pair_distinct_values[f"{left}|{right}"] = [
                [a, b]
                for a, b in sorted(
                    {(normalize_for_index(a), normalize_for_index(b)) for a, b in zip(left_values, right_values)},
                    key=repr,
                )
            ]

        row_groups.append(
            {
                "row_group": row_group_index,
                "num_rows": rg_meta.num_rows,
                "columns": column_summaries,
                "pair_distinct_values": pair_distinct_values,
            }
        )

    return {
        "format": INDEX_FORMAT,
        "parquet_file": "sales.parquet",
        "row_groups": row_groups,
    }


def validate_index_payload(
    payload: dict[str, Any],
    schema: dict[str, pa.DataType],
    expected_row_groups: int,
) -> None:
    assert isinstance(payload, dict), "row_group_index.json must be a JSON object"
    assert set(payload.keys()) == {"format", "parquet_file", "row_groups"}, (
        "row_group_index.json keys must be exactly format, parquet_file, row_groups"
    )
    assert payload["format"] == INDEX_FORMAT, f"unsupported index format: {payload['format']}"
    assert payload["parquet_file"] == "sales.parquet", f"unexpected parquet_file: {payload['parquet_file']}"
    assert isinstance(payload["row_groups"], list), "row_groups must be a list"
    assert len(payload["row_groups"]) == expected_row_groups, (
        f"row_groups must contain exactly {expected_row_groups} entries"
    )

    schema_columns = set(schema)
    for expected_rg, entry in enumerate(payload["row_groups"]):
        assert isinstance(entry, dict), "each row_groups entry must be an object"
        assert set(entry.keys()) == {"row_group", "num_rows", "columns", "pair_distinct_values"}, (
            "each row_groups entry must have exactly row_group, num_rows, columns, pair_distinct_values"
        )
        assert entry["row_group"] == expected_rg, f"row_group entry {expected_rg} must have row_group={expected_rg}"
        assert isinstance(entry["num_rows"], int) and not isinstance(entry["num_rows"], bool) and entry["num_rows"] >= 0, (
            f"invalid num_rows for row group {expected_rg}"
        )
        columns = entry["columns"]
        assert isinstance(columns, dict), f"columns for row group {expected_rg} must be an object"
        assert set(columns.keys()) == schema_columns, f"columns for row group {expected_rg} must cover the full schema"

        for column_name, summary in columns.items():
            assert isinstance(summary, dict), f"column summary for {column_name} must be an object"
            assert set(summary.keys()) == {"min", "max", "null_count", "distinct_values", "has_nan"}, (
                f"column summary keys for {column_name} are invalid"
            )
            null_count = summary["null_count"]
            assert isinstance(null_count, int) and not isinstance(null_count, bool), (
                f"null_count for {column_name} must be an integer"
            )
            assert 0 <= null_count <= entry["num_rows"], f"null_count out of range for {column_name}"
            assert isinstance(summary["has_nan"], bool), f"has_nan for {column_name} must be a boolean"
            if not pa.types.is_floating(schema[column_name]):
                assert summary["has_nan"] is False, f"non-floating column {column_name} must not report has_nan"
            distinct_values = summary["distinct_values"]
            if distinct_values is not None:
                assert isinstance(distinct_values, list), f"distinct_values for {column_name} must be null or a list"
                assert len({json.dumps(v, sort_keys=True) for v in distinct_values}) == len(distinct_values), (
                    f"distinct_values for {column_name} must be unique"
                )

        pair_values = entry["pair_distinct_values"]
        assert isinstance(pair_values, dict), f"pair_distinct_values for row group {expected_rg} must be an object"
        for pair_key, pairs in pair_values.items():
            assert isinstance(pair_key, str) and PAIR_KEY_RE.match(pair_key), f"invalid pair key: {pair_key}"
            left, right = pair_key.split("|", 1)
            assert left in schema_columns and right in schema_columns and left != right, f"unknown pair columns: {pair_key}"
            assert isinstance(pairs, list), f"pair_distinct_values[{pair_key}] must be a list"
            for pair in pairs:
                assert isinstance(pair, list) and len(pair) == 2, f"pair value for {pair_key} must be a 2-element list"


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


def _allowed_sets_for_and(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    node: dict[str, Any],
) -> dict[str, set[Any]]:
    allowed: dict[str, set[Any]] = {}
    for leaf in _flatten_and(node):
        leaf_type = leaf["type"]
        column = leaf.get("column")
        if column is None:
            continue
        dtype = schema[column]
        if leaf_type == "cmp" and leaf["op"] == "eq":
            values = {canonicalize_value(leaf["value"], dtype)}
        elif leaf_type == "in":
            values = {canonicalize_value(v, dtype) for v in leaf["values"]}
        elif leaf_type == "is_null":
            values = {None}
        else:
            continue

        if column in allowed:
            allowed[column] &= values
        else:
            allowed[column] = set(values)
    return allowed


def _pair_feasible(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    node: dict[str, Any],
) -> bool:
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


def _leaf_always_true(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    node: dict[str, Any],
) -> bool:
    column = node["column"]
    summary = row_group_entry["columns"][column]
    num_rows = row_group_entry["num_rows"]
    dtype = schema[column]
    node_type = node["type"]
    distinct_values = summary["distinct_values"]
    has_nan = summary["has_nan"]

    if node_type == "is_null":
        return summary["null_count"] == num_rows

    if node_type == "is_not_null":
        return summary["null_count"] == 0

    if node_type == "in":
        if not _nonnull_only(summary) or distinct_values is None:
            return False
        allowed = {canonicalize_value(v, dtype) for v in node["values"]}
        return set(distinct_values).issubset(allowed) and len(distinct_values) > 0

    if node_type != "cmp":
        return False

    if not _nonnull_only(summary):
        return False

    min_value = _decode_bound(summary["min"], dtype)
    max_value = _decode_bound(summary["max"], dtype)
    exact_value = canonicalize_value(node["value"], dtype)
    typed_value = coerce_scalar(node["value"], dtype)
    op = node["op"]

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


def _cmp_may_true(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    column: str,
    op: str,
    value: Any,
) -> bool:
    summary = row_group_entry["columns"][column]
    num_rows = row_group_entry["num_rows"]
    dtype = schema[column]
    exact_value = canonicalize_value(value, dtype)
    typed_value = coerce_scalar(value, dtype)
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


def _in_may_true(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    column: str,
    values: list[Any],
) -> bool:
    summary = row_group_entry["columns"][column]
    num_rows = row_group_entry["num_rows"]
    dtype = schema[column]
    allowed = {canonicalize_value(v, dtype) for v in values}
    typed_allowed = [coerce_scalar(v, dtype) for v in values]
    distinct_values = summary["distinct_values"]
    min_value = _decode_bound(summary["min"], dtype)
    max_value = _decode_bound(summary["max"], dtype)

    if _all_nulls(summary, num_rows):
        return False
    if distinct_values is not None and set(distinct_values).isdisjoint(allowed):
        return False
    if min_value is not None and max_value is not None and all(v < min_value or v > max_value for v in typed_allowed):
        return False
    return True


def _may_be_false(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    node: dict[str, Any],
) -> bool:
    node_type = node["type"]
    if node_type == "and":
        return any(_may_be_false(row_group_entry, schema, child) for child in node["children"])
    if node_type == "or":
        return all(_may_be_false(row_group_entry, schema, child) for child in node["children"])
    if node_type == "not":
        return may_index_row_group_match(row_group_entry, schema, node["child"])
    return not _leaf_always_true(row_group_entry, schema, node)


def may_index_row_group_match(
    row_group_entry: dict[str, Any],
    schema: dict[str, pa.DataType],
    predicate: dict[str, Any] | None,
) -> bool:
    if predicate is None:
        return True

    node_type = predicate["type"]
    if node_type == "and":
        if not all(may_index_row_group_match(row_group_entry, schema, child) for child in predicate["children"]):
            return False
        return _pair_feasible(row_group_entry, schema, predicate)
    if node_type == "or":
        return any(may_index_row_group_match(row_group_entry, schema, child) for child in predicate["children"])
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
    raise AssertionError(f"unsupported predicate node type in query spec: {node_type}")
