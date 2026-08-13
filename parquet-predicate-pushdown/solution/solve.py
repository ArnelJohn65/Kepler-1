import hashlib
import json
import math
import os
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")


@dataclass
class ColumnStats:
    min: Any
    max: Any
    null_count: int | None
    num_rows: int


@dataclass
class RowGroupContext:
    stats: dict[str, ColumnStats]
    values: dict[str, set[Any]]
    has_null: dict[str, bool]


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def _load_index() -> dict[int, dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "row_group_index.json"), encoding="utf-8") as f:
        entries = json.load(f)
    return {int(entry["row_group"]): entry for entry in entries}


def _extract_stats(rg_meta: pq.RowGroupMetaData) -> dict[str, ColumnStats]:
    out: dict[str, ColumnStats] = {}
    for col_idx in range(rg_meta.num_columns):
        cmeta = rg_meta.column(col_idx)
        stats = cmeta.statistics
        if stats is None:
            out[cmeta.path_in_schema] = ColumnStats(None, None, None, rg_meta.num_rows)
            continue
        out[cmeta.path_in_schema] = ColumnStats(
            stats.min if stats.has_min_max else None,
            stats.max if stats.has_min_max else None,
            stats.null_count,
            rg_meta.num_rows,
        )
    return out


def _rg_context(rg_meta: pq.RowGroupMetaData, index_entry: dict[str, Any]) -> RowGroupContext:
    values_raw = index_entry.get("values", {})
    values = {k: set(v) for k, v in values_raw.items()}
    has_null = {k: bool(v) for k, v in index_entry.get("has_null", {}).items()}
    return RowGroupContext(stats=_extract_stats(rg_meta), values=values, has_null=has_null)


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    node_type = node["type"]
    if node_type == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        value = node["value"]
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
        raise ValueError(f"Unsupported cmp op: {op}")
    if node_type == "in":
        col = table.column(node["column"])
        return pc.is_in(col, value_set=pa.array(node["values"], type=col.type))
    if node_type == "is_null":
        return pc.is_null(table.column(node["column"]))
    if node_type == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if node_type == "and":
        masks = [_build_mask(table, c) for c in node["children"]]
        out = masks[0]
        for m in masks[1:]:
            out = pc.and_(out, m)
        return out
    if node_type == "or":
        masks = [_build_mask(table, c) for c in node["children"]]
        out = masks[0]
        for m in masks[1:]:
            out = pc.or_(out, m)
        return out
    if node_type == "not":
        return pc.invert(_build_mask(table, node["child"]))
    raise ValueError(f"Unsupported predicate node type: {node_type}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def _column_stats(ctx: RowGroupContext, column: str) -> ColumnStats:
    return ctx.stats.get(column, ColumnStats(None, None, None, 0))


def _all_nulls(stats: ColumnStats, ctx: RowGroupContext, column: str) -> bool:
    if stats.num_rows == 0:
        return True
    if stats.null_count is not None and stats.null_count >= stats.num_rows:
        return True
    if ctx.has_null.get(column) and stats.null_count is None:
        return False
    return False


def _nonnull_only(stats: ColumnStats) -> bool:
    return stats.null_count == 0


def _cmp_may_true(ctx: RowGroupContext, column: str, op: str, value: Any) -> bool:
    stats = _column_stats(ctx, column)
    if _all_nulls(stats, ctx, column):
        return False

    values = ctx.values.get(column)
    if op == "eq" and values is not None and value not in values:
        return False

    if op == "ne" and values is not None and len(values) == 1 and value in values and _nonnull_only(stats):
        return False

    rg_min = stats.min
    rg_max = stats.max
    if rg_min is None or rg_max is None:
        return True

    if op == "eq":
        return not (value < rg_min or value > rg_max)
    if op == "ne":
        if rg_min == rg_max == value and _nonnull_only(stats):
            return False
        return True
    if op == "lt":
        return rg_min < value
    if op == "le":
        return rg_min <= value
    if op == "gt":
        return rg_max > value
    if op == "ge":
        return rg_max >= value
    return True


def _in_may_true(ctx: RowGroupContext, column: str, values: list[Any]) -> bool:
    stats = _column_stats(ctx, column)
    if _all_nulls(stats, ctx, column):
        return False

    allowed = set(values)
    index_values = ctx.values.get(column)
    if index_values is not None and index_values.isdisjoint(allowed):
        return False

    if stats.min is not None and stats.max is not None and all((v < stats.min or v > stats.max) for v in allowed):
        return False
    return True


def _leaf_always_true(ctx: RowGroupContext, node: dict[str, Any]) -> bool:
    node_type = node["type"]

    if node_type == "is_null":
        stats = _column_stats(ctx, node["column"])
        return stats.null_count is not None and stats.null_count == stats.num_rows

    if node_type == "is_not_null":
        stats = _column_stats(ctx, node["column"])
        return stats.null_count == 0

    if node_type == "in":
        stats = _column_stats(ctx, node["column"])
        if not _nonnull_only(stats):
            return False
        index_values = ctx.values.get(node["column"])
        if index_values is None:
            return False
        return index_values.issubset(set(node["values"])) and len(index_values) > 0

    if node_type == "cmp":
        column = node["column"]
        op = node["op"]
        value = node["value"]
        stats = _column_stats(ctx, column)
        if not _nonnull_only(stats):
            return False
        rg_min = stats.min
        rg_max = stats.max
        if rg_min is None or rg_max is None:
            return False
        index_values = ctx.values.get(column)

        if op == "eq":
            if index_values is not None:
                return index_values == {value}
            return rg_min == rg_max == value
        if op == "ne":
            if index_values is not None:
                return value not in index_values
            return value < rg_min or value > rg_max
        if op == "lt":
            return rg_max < value
        if op == "le":
            return rg_max <= value
        if op == "gt":
            return rg_min > value
        if op == "ge":
            return rg_min >= value

    return False


def _may_be_true(ctx: RowGroupContext, node: dict[str, Any]) -> bool:
    node_type = node["type"]
    if node_type == "and":
        return all(_may_be_true(ctx, child) for child in node["children"])
    if node_type == "or":
        return any(_may_be_true(ctx, child) for child in node["children"])
    if node_type == "not":
        return _may_be_false(ctx, node["child"])
    if node_type == "cmp":
        return _cmp_may_true(ctx, node["column"], node["op"], node["value"])
    if node_type == "in":
        return _in_may_true(ctx, node["column"], node["values"])
    if node_type == "is_null":
        stats = _column_stats(ctx, node["column"])
        if stats.null_count is not None:
            return stats.null_count > 0
        return ctx.has_null.get(node["column"], True)
    if node_type == "is_not_null":
        stats = _column_stats(ctx, node["column"])
        if stats.null_count is not None:
            return stats.null_count < stats.num_rows
        return True
    return True


def _may_be_false(ctx: RowGroupContext, node: dict[str, Any]) -> bool:
    node_type = node["type"]
    if node_type == "and":
        return any(_may_be_false(ctx, child) for child in node["children"])
    if node_type == "or":
        return all(_may_be_false(ctx, child) for child in node["children"])
    if node_type == "not":
        return _may_be_true(ctx, node["child"])
    return not _leaf_always_true(ctx, node)


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if not node:
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
    raise ValueError(f"Unsupported node type: {node_type}")


def _receipt_for_table(table: pa.Table) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(f"rows={table.num_rows}".encode("utf-8"))
    for row in table.to_pylist():
        payload = json.dumps({k: _normalize(v) for k, v in row.items()}, sort_keys=True, separators=(",", ":"))
        h.update(payload.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _query_receipt(query_id: str, read_row_groups: list[dict[str, Any]]) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(query_id.encode("utf-8"))
    h.update(b"|")
    for entry in read_row_groups:
        h.update(f"{entry['row_group']}:{entry['decoded_rows']}:{entry['receipt']}".encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def main() -> None:
    queries = _load_queries()
    index_by_rg = _load_index()
    if not queries:
        raise RuntimeError("No queries available")

    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))

    results_payload: list[dict[str, Any]] = []
    trace_records: list[dict[str, Any]] = []

    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]

        required_columns = set(projection)
        _columns_in_predicate(predicate, required_columns)
        read_columns = sorted(required_columns)

        rows: list[dict[str, Any]] = []
        read_trace: list[dict[str, Any]] = []

        for rg_idx in range(pf.metadata.num_row_groups):
            rg_meta = pf.metadata.row_group(rg_idx)
            ctx = _rg_context(rg_meta, index_by_rg.get(rg_idx, {}))

            if predicate is not None and not _may_be_true(ctx, predicate):
                continue

            decoded = pf.read_row_group(rg_idx, columns=read_columns)
            receipt = _receipt_for_table(decoded)

            filtered = _apply_predicate(decoded, predicate)
            projected = filtered.select(projection)
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in projected.to_pylist())

            read_trace.append(
                {
                    "row_group": rg_idx,
                    "decoded_rows": decoded.num_rows,
                    "receipt": receipt,
                }
            )

        trace_record = {
            "query_id": query["id"],
            "read_row_groups": read_trace,
            "query_receipt": _query_receipt(query["id"], read_trace),
            "result_count": len(rows),
        }

        results_payload.append({"query_id": query["id"], "rows": rows})
        trace_records.append(trace_record)

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results_payload, f, indent=2)

    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for record in trace_records:
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {RESULTS_PATH} and {TRACE_PATH}")


if __name__ == "__main__":
    main()
