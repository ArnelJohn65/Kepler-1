"""
Baseline engine scaffold with two-pass execution.

build: may scan all row groups and persist any index files.
query: must load persisted index and produce /app/results.json and /app/trace.jsonl.

This baseline intentionally does no pruning in query mode.
"""

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
INDEX_PATH = os.path.join(APP_ROOT, "row_group_index.pkl")


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


def _columns_in_predicate(node: dict[str, Any] | None, out: set[str]) -> None:
    if node is None:
        return
    t = node["type"]
    if t in {"cmp", "in", "is_null", "is_not_null"}:
        out.add(node["column"])
    elif t in {"and", "or"}:
        for c in node["children"]:
            _columns_in_predicate(c, out)
    elif t == "not":
        _columns_in_predicate(node["child"], out)


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        val = _coerce_scalar(node["value"], col.type)
        ops = {
            "eq": pc.equal,
            "ne": pc.not_equal,
            "lt": pc.less,
            "le": pc.less_equal,
            "gt": pc.greater,
            "ge": pc.greater_equal,
        }
        return ops[op](col, val)
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
    raise ValueError(f"unknown predicate type: {t}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
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


def build_index() -> None:
    import glob as _glob
    matches = _glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    if not matches:
        raise RuntimeError(f"No parquet file found in {DATA_DIR}")
    parquet_path = sorted(matches)[0]
    pf = pq.ParquetFile(parquet_path)

    # Baseline placeholder index: only row counts and stat presence.
    stats = []
    for rg_idx in range(pf.metadata.num_row_groups):
        rg = pf.metadata.row_group(rg_idx)
        stats.append({"row_group": rg_idx, "num_rows": rg.num_rows, "num_columns": rg.num_columns})

    with open(INDEX_PATH, "wb") as f:
        pickle.dump({"parquet": os.path.basename(parquet_path), "row_groups": stats}, f)

    print(f"Wrote {INDEX_PATH}")


def run_queries() -> None:
    if not os.path.exists(INDEX_PATH):
        raise RuntimeError(f"Missing build-pass index: {INDEX_PATH}")

    queries = _load_queries()
    if not queries:
        raise RuntimeError("No queries available")
    with open(INDEX_PATH, "rb") as f:
        payload = pickle.load(f)
    pf = pq.ParquetFile(os.path.join(DATA_DIR, payload["parquet"]))

    all_results: list[dict[str, Any]] = []
    all_traces: list[dict[str, Any]] = []

    for q in queries:
        predicate = q.get("predicate")
        proj = q["columns"]
        required = set(proj)
        _columns_in_predicate(predicate, required)
        read_cols = sorted(required)

        rows = []
        read_trace = []
        for rg in range(pf.metadata.num_row_groups):
            decoded = pf.read_row_group(rg, columns=read_cols)
            filtered = _apply_predicate(decoded, predicate)
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

    print(f"Wrote {RESULTS_PATH} and {TRACE_PATH}")


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
