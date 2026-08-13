"""
Parquet query engine with predicate pushdown (incomplete).

Problems to fix:
1. Off-by-one bug in _compute_stats: boundary predicates get wrong statistics.
2. The planner never prunes row groups — _should_skip always returns False.
"""

import json
import pyarrow.parquet as pq
import pyarrow as pa
import os


DATA_DIR = "/app/data"
RESULTS_PATH = "/app/results.json"
TRACE_PATH = "/app/trace.jsonl"


# ---------------------------------------------------------------------------
# Statistics collection — BUG: off-by-one on boundary values
# ---------------------------------------------------------------------------

def _compute_stats(table: pa.Table, col: str):
    """Return (min_val, max_val) for a column, excluding nulls.

    BUG: uses len(arr) instead of len(arr) - 1 when finding the last
    non-null value, so the reported max is sometimes one element too low.
    This causes boundary predicates to be incorrectly pruned or not pruned.
    """
    arr = table.column(col).drop_null()
    if len(arr) == 0:
        return None, None
    sorted_arr = pa.compute.sort_indices(arr)
    min_val = arr[sorted_arr[0].as_py()].as_py()
    # BUG: uses index len(sorted_arr) - 2 instead of len(sorted_arr) - 1, so the
    # true maximum value is never reported. Boundary predicates that land on the
    # real max will be incorrectly pruned.
    max_val = arr[sorted_arr[len(sorted_arr) - 2].as_py()].as_py()
    return min_val, max_val


# ---------------------------------------------------------------------------
# Row-group pruning — NOT IMPLEMENTED
# ---------------------------------------------------------------------------

def _should_skip(rg_stats: dict, predicate: dict) -> bool:
    """Return True if the row group can be skipped for this predicate.

    Currently always returns False (no pruning).
    """
    return False


# ---------------------------------------------------------------------------
# Query runner
# ---------------------------------------------------------------------------

def _apply_predicate(table: pa.Table, predicate: dict) -> pa.Table:
    """Filter a table in memory using the predicate."""
    import pyarrow.compute as pc

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


def run_query(query: dict) -> tuple[list, list[int]]:
    """Run a single query. Returns (result_rows, row_groups_read)."""
    parquet_file = os.path.join(DATA_DIR, query["file"])
    predicate = query.get("predicate")
    columns = query.get("columns")

    pf = pq.ParquetFile(parquet_file)
    num_rgs = pf.metadata.num_row_groups

    results = []
    row_groups_read = []

    for rg_idx in range(num_rgs):
        # Compute stats for pruning decision
        rg_meta = pf.metadata.row_group(rg_idx)
        rg_stats = {}
        if predicate and "column" in predicate:
            col = predicate["column"]
            for col_idx in range(rg_meta.num_columns):
                cmeta = rg_meta.column(col_idx)
                if cmeta.path_in_schema == col and cmeta.statistics:
                    stats = cmeta.statistics
                    rg_stats = {
                        "min": stats.min if stats.has_min_max else None,
                        "max": stats.max if stats.has_min_max else None,
                        "null_count": stats.null_count,
                    }
                    break

        # Pruning decision
        if predicate and _should_skip(rg_stats, predicate):
            continue

        row_groups_read.append(rg_idx)

        # Read row group
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

    print(f"Wrote {RESULTS_PATH} and {TRACE_PATH}")


if __name__ == "__main__":
    main()
