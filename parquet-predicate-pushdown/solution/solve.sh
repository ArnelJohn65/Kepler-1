#!/usr/bin/env bash
set -euo pipefail

cat > /tmp/solve.py << 'PYEOF'
"""
Reference solution: fixes the off-by-one bug and implements row-group pruning.
"""

import json
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
import os

DATA_DIR = "/app/data"
RESULTS_PATH = "/app/results.json"
TRACE_PATH = "/app/trace.jsonl"


def _apply_predicate(table: pa.Table, predicate: dict) -> pa.Table:
    col = predicate["column"]
    op = predicate["op"]
    val = predicate.get("value")
    column = table.column(col)

    if op == "eq":
        mask = pc.equal(column, val)
    elif op == "ne":
        mask = pc.not_equal(column, val)
    elif op == "lt":
        mask = pc.less(column, val)
    elif op == "le":
        mask = pc.less_equal(column, val)
    elif op == "gt":
        mask = pc.greater(column, val)
    elif op == "ge":
        mask = pc.greater_equal(column, val)
    elif op == "is_null":
        mask = pc.is_null(column)
    elif op == "is_not_null":
        mask = pc.is_valid(column)
    else:
        raise ValueError(f"Unknown op: {op}")

    return table.filter(mask)


def _can_prune(rg_stats: dict, predicate: dict) -> bool:
    """
    Return True if the row group definitely cannot contain any matching row.
    Only prune for comparison ops where nulls don't matter.
    """
    op = predicate["op"]
    val = predicate.get("value")

    # Never prune null-related predicates — nulls are absent from min/max.
    if op in ("is_null", "is_not_null", "ne"):
        return False

    rg_min = rg_stats.get("min")
    rg_max = rg_stats.get("max")

    if rg_min is None or rg_max is None:
        return False  # No stats — can't prune

    if op == "eq":
        return val < rg_min or val > rg_max
    elif op == "lt":
        return rg_min >= val
    elif op == "le":
        return rg_min > val
    elif op == "gt":
        return rg_max <= val
    elif op == "ge":
        return rg_max < val

    return False


def run_query(query: dict) -> tuple:
    parquet_file = os.path.join(DATA_DIR, query["file"])
    predicate = query.get("predicate")
    columns = query.get("columns")

    pf = pq.ParquetFile(parquet_file)
    num_rgs = pf.metadata.num_row_groups

    results = []
    row_groups_read = []

    for rg_idx in range(num_rgs):
        rg_meta = pf.metadata.row_group(rg_idx)
        rg_stats = {}
        if predicate and "column" in predicate:
            col = predicate["column"]
            for col_idx in range(rg_meta.num_columns):
                cmeta = rg_meta.column(col_idx)
                if cmeta.path_in_schema == col and cmeta.statistics:
                    stats = cmeta.statistics
                    if stats.has_min_max:
                        rg_stats = {
                            "min": stats.min,
                            "max": stats.max,
                            "null_count": stats.null_count,
                        }
                    break

        if predicate and _can_prune(rg_stats, predicate):
            continue

        row_groups_read.append(rg_idx)
        batch = pf.read_row_group(rg_idx, columns=columns)
        if predicate:
            batch = _apply_predicate(batch, predicate)

        for row in batch.to_pylist():
            results.append(row)

    return results, row_groups_read


def main():
    queries_path = os.path.join(DATA_DIR, "queries.json")
    with open(queries_path) as f:
        queries = json.load(f)

    all_results = []
    with open(TRACE_PATH, "w") as trace_f:
        for q in queries:
            rows, rgs_read = run_query(q)
            all_results.append({"query_id": q["id"], "rows": rows})
            trace_f.write(json.dumps({"query_id": q["id"], "row_groups_read": rgs_read}) + "\n")

    with open(RESULTS_PATH, "w") as f:
        json.dump(all_results, f, indent=2, default=str)

    print(f"Done. Results: {RESULTS_PATH}, Trace: {TRACE_PATH}")


if __name__ == "__main__":
    main()
PYEOF

python /tmp/solve.py
