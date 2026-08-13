import hashlib
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
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")


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


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if node is None:
        return
    t = node["type"]
    if t in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if t in {"and", "or"}:
        for c in node["children"]:
            _columns_in_predicate(c, output)
        return
    if t == "not":
        _columns_in_predicate(node["child"], output)


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        val = _coerce_scalar(node["value"], col.type)
        return {
            "eq": pc.equal,
            "ne": pc.not_equal,
            "lt": pc.less,
            "le": pc.less_equal,
            "gt": pc.greater,
            "ge": pc.greater_equal,
        }[op](col, val)
    if t == "in":
        col = table.column(node["column"])
        values = [_coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(values, type=col.type))
    if t == "is_null":
        return pc.is_null(table.column(node["column"]))
    if t == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if t == "and":
        parts = [_build_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.and_(out, p)
        return out
    if t == "or":
        parts = [_build_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.or_(out, p)
        return out
    if t == "not":
        return pc.invert(_build_mask(table, node["child"]))
    raise ValueError(t)


def _apply(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def _receipt(table: pa.Table) -> str:
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
    for e in read_row_groups:
        h.update(f"{e['row_group']}:{e['decoded_rows']}:{e['decoded_bytes']}:{e['receipt']}".encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def _may_true(meta: pq.RowGroupMetaData, node: dict[str, Any]) -> bool:
    t = node["type"]
    if t == "and":
        return all(_may_true(meta, c) for c in node["children"])
    if t == "or":
        return any(_may_true(meta, c) for c in node["children"])
    if t == "not":
        return True

    col = node["column"]
    cmeta = None
    for i in range(meta.num_columns):
        cm = meta.column(i)
        if cm.path_in_schema == col:
            cmeta = cm
            break
    if cmeta is None:
        return True
    stats = cmeta.statistics
    if stats is None or not stats.has_min_max:
        return True

    if t == "is_null":
        return stats.null_count is None or stats.null_count > 0
    if t == "is_not_null":
        return stats.null_count is None or stats.null_count < meta.num_rows

    mn, mx = stats.min, stats.max

    def _coerce(v: Any) -> Any:
        probe = mn if mn is not None else mx
        if isinstance(probe, Decimal):
            return Decimal(str(v))
        if isinstance(probe, datetime) and isinstance(v, str):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        return v

    if t == "cmp":
        value = _coerce(node["value"])
        op = node["op"]
        if op == "eq":
            return not (value < mn or value > mx)
        if op == "ne":
            return True
        if op == "lt":
            return mn < value
        if op == "le":
            return mn <= value
        if op == "gt":
            return mx > value
        if op == "ge":
            return mx >= value
        return True

    if t == "in":
        return any(not (_coerce(v) < mn or _coerce(v) > mx) for v in node["values"])

    return True


def main() -> None:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        queries = json.load(f)
    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))

    all_results = []
    all_traces = []
    for q in queries:
        predicate = q.get("predicate")
        proj = q["columns"]
        required = set(proj)
        _columns_in_predicate(predicate, required)
        read_cols = sorted(required)

        rows = []
        read_trace = []
        for rg in range(pf.metadata.num_row_groups):
            if predicate is not None and not _may_true(pf.metadata.row_group(rg), predicate):
                continue
            decoded = pf.read_row_group(rg, columns=read_cols)
            filtered = _apply(decoded, predicate)
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in filtered.select(proj).to_pylist())
            read_trace.append(
                {
                    "row_group": rg,
                    "decoded_rows": decoded.num_rows,
                    "decoded_bytes": decoded.nbytes,
                    "receipt": _receipt(decoded),
                }
            )

        all_results.append({"query_id": q["id"], "rows": rows})
        all_traces.append(
            {
                "query_id": q["id"],
                "read_row_groups": read_trace,
                "query_receipt": _query_receipt(q["id"], read_trace),
                "result_count": len(rows),
            }
        )

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for rec in all_traces:
            f.write(json.dumps(rec) + "\n")


if __name__ == "__main__":
    main()
