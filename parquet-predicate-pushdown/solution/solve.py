import hashlib
import json
import math
import os
import pickle
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from time import perf_counter
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")
INDEX_PATH = os.path.join(DATA_DIR, "row_group_index.pkl")
METRICS_PATH = os.path.join(APP_ROOT, "query_metrics.json")

PAIR_INDEX_COLUMNS = [("segment", "status"), ("region", "channel"), ("sku", "event_day")]


@dataclass
class ColumnStats:
    min: Any
    max: Any
    null_count: int | None
    num_rows: int


@dataclass
class RowGroupIndex:
    stats: dict[str, ColumnStats]
    values: dict[str, set[Any]]
    has_null: dict[str, bool]
    has_nan: dict[str, bool]
    pair_values: dict[str, set[tuple[Any, Any]]]


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


def _extract_stats(rg_meta: pq.RowGroupMetaData) -> dict[str, ColumnStats]:
    out: dict[str, ColumnStats] = {}
    for ci in range(rg_meta.num_columns):
        cmeta = rg_meta.column(ci)
        s = cmeta.statistics
        if s is None:
            out[cmeta.path_in_schema] = ColumnStats(None, None, None, rg_meta.num_rows)
        else:
            out[cmeta.path_in_schema] = ColumnStats(
                s.min if s.has_min_max else None,
                s.max if s.has_min_max else None,
                s.null_count,
                rg_meta.num_rows,
            )
    return out


def _build_row_group_index(pf: pq.ParquetFile) -> list[RowGroupIndex]:
    index: list[RowGroupIndex] = []
    for rg_idx in range(pf.metadata.num_row_groups):
        rg_meta = pf.metadata.row_group(rg_idx)
        table = pf.read_row_group(rg_idx)

        values: dict[str, set[Any]] = {}
        has_null: dict[str, bool] = {}
        has_nan: dict[str, bool] = {}

        for col_name in table.column_names:
            col = table.column(col_name)
            has_null[col_name] = col.null_count > 0

            if pa.types.is_floating(col.type):
                py_values = col.to_pylist()
                has_nan[col_name] = any(isinstance(v, float) and math.isnan(v) for v in py_values if v is not None)

            if pa.types.is_string(col.type) or pa.types.is_integer(col.type):
                values[col_name] = set(col.drop_null().to_pylist())

        pair_values: dict[str, set[tuple[Any, Any]]] = {}
        for left, right in PAIR_INDEX_COLUMNS:
            if left in table.column_names and right in table.column_names:
                left_vals = table.column(left).to_pylist()
                right_vals = table.column(right).to_pylist()
                pair_values[f"{left}|{right}"] = {(a, b) for a, b in zip(left_vals, right_vals)}

        index.append(
            RowGroupIndex(
                stats=_extract_stats(rg_meta),
                values=values,
                has_null=has_null,
                has_nan=has_nan,
                pair_values=pair_values,
            )
        )
    return index


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
        raise ValueError(f"Unsupported cmp op: {op}")
    if t == "in":
        col = table.column(node["column"])
        values = [_coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(values, type=col.type))
    if t == "is_null":
        return pc.is_null(table.column(node["column"]))
    if t == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if t == "and":
        masks = [_build_mask(table, c) for c in node["children"]]
        out = masks[0]
        for m in masks[1:]:
            out = pc.and_(out, m)
        return out
    if t == "or":
        masks = [_build_mask(table, c) for c in node["children"]]
        out = masks[0]
        for m in masks[1:]:
            out = pc.or_(out, m)
        return out
    if t == "not":
        return pc.invert(_build_mask(table, node["child"]))
    raise ValueError(f"Unsupported predicate node type: {t}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if not node:
        return
    t = node["type"]
    if t in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if t in {"and", "or"}:
        for child in node["children"]:
            _columns_in_predicate(child, output)
        return
    if t == "not":
        _columns_in_predicate(node["child"], output)
        return
    raise ValueError(f"Unsupported node type: {t}")


def _nonnull_only(st: ColumnStats) -> bool:
    return st.null_count == 0


def _all_nulls(st: ColumnStats) -> bool:
    return st.null_count is not None and st.null_count >= st.num_rows > 0


def _coerce_for_stats_value(st: ColumnStats, value: Any) -> Any:
    probe = st.min if st.min is not None else st.max
    if isinstance(probe, Decimal):
        return Decimal(str(value))
    if isinstance(probe, datetime) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _cmp_may_true(rg: RowGroupIndex, column: str, op: str, value: Any) -> bool:
    st = rg.stats.get(column, ColumnStats(None, None, None, 0))
    value = _coerce_for_stats_value(st, value)
    if _all_nulls(st):
        return False

    cv = rg.values.get(column)
    has_nan = rg.has_nan.get(column, False)

    if op == "eq" and cv is not None and value not in cv:
        return False

    if op == "ne":
        if has_nan:
            return True
        if cv is not None and len(cv) == 1 and value in cv and _nonnull_only(st):
            return False

    rg_min, rg_max = st.min, st.max
    if rg_min is None or rg_max is None:
        return True

    if op == "eq":
        return not (value < rg_min or value > rg_max)
    if op == "ne":
        if rg_min == rg_max == value and _nonnull_only(st) and not has_nan:
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


def _in_may_true(rg: RowGroupIndex, column: str, values: list[Any]) -> bool:
    st = rg.stats.get(column, ColumnStats(None, None, None, 0))
    allowed_values = [_coerce_for_stats_value(st, v) for v in values]
    if _all_nulls(st):
        return False

    allowed = set(allowed_values)
    cv = rg.values.get(column)
    if cv is not None and cv.isdisjoint(allowed):
        return False

    rg_min, rg_max = st.min, st.max
    if rg_min is not None and rg_max is not None and all(v < rg_min or v > rg_max for v in allowed):
        return False
    return True


def _leaf_always_true(rg: RowGroupIndex, node: dict[str, Any]) -> bool:
    col = node["column"]
    st = rg.stats.get(col, ColumnStats(None, None, None, 0))
    t = node["type"]
    has_nan = rg.has_nan.get(col, False)

    if t == "is_null":
        return st.null_count is not None and st.null_count == st.num_rows

    if t == "is_not_null":
        return st.null_count == 0

    if t == "in":
        allowed_values = [_coerce_for_stats_value(st, v) for v in node["values"]]
        if not _nonnull_only(st):
            return False
        cv = rg.values.get(col)
        return cv is not None and cv.issubset(set(allowed_values)) and len(cv) > 0

    if t == "cmp":
        if not _nonnull_only(st):
            return False
        op, value = node["op"], _coerce_for_stats_value(st, node["value"])
        rg_min, rg_max = st.min, st.max
        if rg_min is None or rg_max is None:
            return False

        cv = rg.values.get(col)
        if op == "eq":
            if has_nan:
                return False
            return (cv == {value}) if cv is not None else (rg_min == rg_max == value)
        if op == "ne":
            if has_nan:
                return True
            return (value not in cv) if cv is not None else (value < rg_min or value > rg_max)
        if op in {"lt", "le", "gt", "ge"} and has_nan:
            return False
        if op == "lt":
            return rg_max < value
        if op == "le":
            return rg_max <= value
        if op == "gt":
            return rg_min > value
        if op == "ge":
            return rg_min >= value

    return False


def _flatten_and(node: dict[str, Any]) -> list[dict[str, Any]]:
    if node["type"] != "and":
        return [node]
    out: list[dict[str, Any]] = []
    for child in node["children"]:
        out.extend(_flatten_and(child))
    return out


def _allowed_sets_for_and(node: dict[str, Any]) -> dict[str, set[Any]]:
    allowed: dict[str, set[Any]] = {}
    for leaf in _flatten_and(node):
        t = leaf["type"]
        if t == "cmp" and leaf["op"] == "eq":
            vals = {leaf["value"]}
        elif t == "in":
            vals = set(leaf["values"])
        elif t == "is_null":
            vals = {None}
        else:
            continue

        col = leaf["column"]
        if col in allowed:
            allowed[col] &= vals
        else:
            allowed[col] = set(vals)
    return allowed


def _pair_feasible(rg: RowGroupIndex, node: dict[str, Any]) -> bool:
    if node["type"] != "and":
        return True
    allowed = _allowed_sets_for_and(node)

    for left, right in PAIR_INDEX_COLUMNS:
        if left not in allowed or right not in allowed:
            continue
        pair_set = rg.pair_values.get(f"{left}|{right}")
        if pair_set is None:
            continue
        if not any((a, b) in pair_set for a in allowed[left] for b in allowed[right]):
            return False

    return True


def _may_be_false(rg: RowGroupIndex, node: dict[str, Any]) -> bool:
    t = node["type"]
    if t == "and":
        return any(_may_be_false(rg, c) for c in node["children"])
    if t == "or":
        return all(_may_be_false(rg, c) for c in node["children"])
    if t == "not":
        return _may_be_true(rg, node["child"])
    return not _leaf_always_true(rg, node)


def _may_be_true(rg: RowGroupIndex, node: dict[str, Any]) -> bool:
    t = node["type"]
    if t == "and":
        if not all(_may_be_true(rg, c) for c in node["children"]):
            return False
        return _pair_feasible(rg, node)
    if t == "or":
        return any(_may_be_true(rg, c) for c in node["children"])
    if t == "not":
        return _may_be_false(rg, node["child"])
    if t == "cmp":
        return _cmp_may_true(rg, node["column"], node["op"], node["value"])
    if t == "in":
        return _in_may_true(rg, node["column"], node["values"])
    if t == "is_null":
        st = rg.stats.get(node["column"], ColumnStats(None, None, None, 0))
        if st.null_count is not None:
            return st.null_count > 0
        return rg.has_null.get(node["column"], True)
    if t == "is_not_null":
        st = rg.stats.get(node["column"], ColumnStats(None, None, None, 0))
        if st.null_count is not None:
            return st.null_count < st.num_rows
        return True
    return True


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
        h.update(
            f"{entry['row_group']}:{entry['decoded_rows']}:{entry['decoded_bytes']}:{entry['receipt']}".encode("utf-8")
        )
        h.update(b"|")
    return h.hexdigest()


def build_index() -> None:
    queries = _load_queries()
    if not queries:
        raise RuntimeError("No queries available")

    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))
    rg_index = _build_row_group_index(pf)

    with open(INDEX_PATH, "wb") as f:
        pickle.dump({"num_row_groups": pf.metadata.num_row_groups, "index": rg_index}, f)

    print(f"Wrote {INDEX_PATH}")


def run_query_pass() -> None:
    if not os.path.exists(INDEX_PATH):
        raise RuntimeError(f"Missing build-pass index: {INDEX_PATH}")

    queries = _load_queries()
    if not queries:
        raise RuntimeError("No queries available")

    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))
    with open(INDEX_PATH, "rb") as f:
        payload = pickle.load(f)
    rg_index: list[RowGroupIndex] = payload["index"]

    results_payload: list[dict[str, Any]] = []
    trace_records: list[dict[str, Any]] = []

    query_phase_start = perf_counter()
    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]

        required_columns = set(projection)
        _columns_in_predicate(predicate, required_columns)
        read_columns = sorted(required_columns)

        rows: list[dict[str, Any]] = []
        read_trace: list[dict[str, Any]] = []

        for rg_idx in range(pf.metadata.num_row_groups):
            if predicate is not None and not _may_be_true(rg_index[rg_idx], predicate):
                continue

            decoded = pf.read_row_group(rg_idx, columns=read_columns)
            receipt = _receipt_for_table(decoded)
            decoded_bytes = decoded.nbytes

            filtered = _apply_predicate(decoded, predicate)
            projected = filtered.select(projection)
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in projected.to_pylist())

            read_trace.append(
                {
                    "row_group": rg_idx,
                    "decoded_rows": decoded.num_rows,
                    "decoded_bytes": decoded_bytes,
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

    elapsed_ms = int((perf_counter() - query_phase_start) * 1000)

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(results_payload, f, indent=2)

    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for record in trace_records:
            f.write(json.dumps(record) + "\n")

    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump({"query_pass_elapsed_ms": elapsed_ms}, f, indent=2)

    print(f"Wrote {RESULTS_PATH} and {TRACE_PATH}")
    print(f"Query pass elapsed: {elapsed_ms} ms")


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
