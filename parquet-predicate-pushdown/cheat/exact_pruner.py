import hashlib
import json
import math
import os
import pickle
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
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")
INDEX_PATH = os.path.join(DATA_DIR, "exact_read_index.pkl")


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


def _mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
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
        parts = [_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.and_(out, p)
        return out
    if t == "or":
        parts = [_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.or_(out, p)
        return out
    if t == "not":
        return pc.invert(_mask(table, node["child"]))
    raise ValueError(t)


def _apply(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_mask(table, predicate))


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


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def build() -> None:
    queries = _load_queries()
    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))

    read_plan: dict[str, dict[str, Any]] = {}
    for q in queries:
        predicate = q.get("predicate")
        proj = q["columns"]
        required = set(proj)
        _columns_in_predicate(predicate, required)
        read_cols = sorted(required)

        matching_groups: list[int] = []
        for rg in range(pf.metadata.num_row_groups):
            decoded = pf.read_row_group(rg, columns=read_cols)
            if _apply(decoded, predicate).num_rows > 0:
                matching_groups.append(rg)

        read_plan[q["id"]] = {"read_columns": read_cols, "matching_groups": matching_groups}

    with open(INDEX_PATH, "wb") as f:
        pickle.dump(read_plan, f)


def query() -> None:
    queries = _load_queries()
    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))
    with open(INDEX_PATH, "rb") as f:
        read_plan = pickle.load(f)

    all_results = []
    all_traces = []
    for q in queries:
        predicate = q.get("predicate")
        proj = q["columns"]
        plan = read_plan[q["id"]]
        read_cols = plan["read_columns"]

        rows = []
        read_trace = []
        for rg in plan["matching_groups"]:
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


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "build":
        build()
    elif mode == "query":
        query()
    elif mode == "all":
        build()
        query()
    else:
        raise SystemExit("Usage: python exact_pruner.py [build|query|all]")


if __name__ == "__main__":
    main()
