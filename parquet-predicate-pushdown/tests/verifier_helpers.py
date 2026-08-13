import hashlib
import json
import math
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc


def normalize_for_receipt(v: Any) -> Any:
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
    h = hashlib.blake2b(digest_size=16)
    h.update(f"rows={table.num_rows}".encode("utf-8"))
    for row in table.to_pylist():
        payload = json.dumps({k: normalize_for_receipt(v) for k, v in row.items()}, sort_keys=True, separators=(",", ":"))
        h.update(payload.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def query_receipt(query_id: str, read_row_groups: list[dict[str, Any]]) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(query_id.encode("utf-8"))
    h.update(b"|")
    for entry in read_row_groups:
        h.update(f"{entry['row_group']}:{entry['decoded_rows']}:{entry['receipt']}".encode("utf-8"))
        h.update(b"|")
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
