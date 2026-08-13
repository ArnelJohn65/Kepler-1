"""
Baseline query engine — full scan only.

Load queries from /app/data/queries.json, scan every row group in
the referenced Parquet file, apply the predicate, and write results
to /app/results.json.

You need to:
  1. Write /app/trace.jsonl (one JSON object per line, one per query).
  2. Add row-group pruning so that each query reads at most
     max_row_groups_read row groups.
  3. Emit correct receipts in the trace so the verifier can validate them.
"""

import json
import os
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        val = node["value"]
        ops = {
            "eq": pc.equal, "ne": pc.not_equal,
            "lt": pc.less, "le": pc.less_equal,
            "gt": pc.greater, "ge": pc.greater_equal,
        }
        return ops[op](col, val)
    if t == "in":
        col = table.column(node["column"])
        return pc.is_in(col, value_set=pa.array(node["values"], type=col.type))
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


def run_query(pf: pq.ParquetFile, query: dict[str, Any]) -> list[dict[str, Any]]:
    predicate = query.get("predicate")
    projection = query["columns"]
    required = set(projection)
    _columns_in_predicate(predicate, required)
    read_columns = sorted(required)

    rows: list[dict[str, Any]] = []
    for rg_idx in range(pf.metadata.num_row_groups):
        decoded = pf.read_row_group(rg_idx, columns=read_columns)
        filtered = _apply_predicate(decoded, predicate)
        rows.extend(filtered.select(projection).to_pylist())
    return rows


def main() -> None:
    queries = _load_queries()
    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))

    all_results: list[dict[str, Any]] = []
    for query in queries:
        rows = run_query(pf, query)
        all_results.append({"query_id": query["id"], "rows": rows})

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"Wrote {RESULTS_PATH}")
    print("WARNING: trace.jsonl not written — you must implement it")


if __name__ == "__main__":
    main()
