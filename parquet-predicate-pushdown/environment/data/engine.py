"""Columnar query engine with row-group statistics.

The engine reads a JSON dataset (list of row groups). It builds per-column
min/max statistics for each row group and uses them to prune row groups during
query execution.

There is a latent off-by-one bug in the statistics writer (see RowGroupStats)
and the predicate pushdown planner (_can_prune) is not yet implemented.
The agent must fix both.
"""

import json
import os
import sys


class RowGroupStats:
    """Tracks min/max for one column in one row group. Nulls are excluded."""

    def __init__(self):
        self.min_val = None
        self.max_val = None

    def update(self, value):
        if value is None:
            return
        if self.min_val is None or value < self.min_val:
            self.min_val = value
        # BUG: off-by-one in max initialisation.  When the first non-null
        # value is seen, max_val is set to (value - 1) instead of value.
        # This causes the stored max to be one less than the true max,
        # which makes boundary predicates such as  x >= <true max>  prune
        # the row group incorrectly.
        if self.max_val is None:
            # BUG: subtracts 1 from numeric values on first init.
            self.max_val = (value - 1) if isinstance(value, (int, float)) else value
        elif value > self.max_val:
            self.max_val = value

    def to_dict(self):
        return {"min": self.min_val, "max": self.max_val}


class RowGroup:
    def __init__(self, rows, columns):
        self.rows = rows
        self.columns = columns
        self.stats = {}
        self._build_stats()

    def _build_stats(self):
        for col in self.columns:
            s = RowGroupStats()
            for row in self.rows:
                s.update(row.get(col))
            self.stats[col] = s


class Engine:
    def __init__(self, dataset_path):
        with open(dataset_path) as f:
            data = json.load(f)
        self.columns = data["columns"]
        self.row_groups = []
        for rg_data in data["row_groups"]:
            rg = RowGroup(rg_data["rows"], self.columns)
            self.row_groups.append(rg)
        self.trace = []

    # ------------------------------------------------------------------
    # Pushdown planner
    # ------------------------------------------------------------------

    def _can_prune(self, rg, predicate):
        """Return True if statistics prove this row group has no matching rows.

        Not yet implemented — always returns False (no pruning).
        The agent must implement this method to achieve measurable row-group
        pruning and also fix the off-by-one bug in RowGroupStats.update above.
        """
        return False

    # ------------------------------------------------------------------
    # Query execution
    # ------------------------------------------------------------------

    def execute(self, query):
        predicate = query.get("predicate")
        select_cols = query.get("select", self.columns)
        agg = query.get("aggregate")
        query_id = query.get("id", "unknown")

        read_groups = []
        for i, rg in enumerate(self.row_groups):
            if predicate is not None and self._can_prune(rg, predicate):
                continue
            read_groups.append(i)

        rows_out = []
        for i in read_groups:
            rg = self.row_groups[i]
            for row in rg.rows:
                if predicate is None or self._eval_predicate(row, predicate):
                    rows_out.append({c: row.get(c) for c in select_cols})

        self.trace.append({
            "query_id": query_id,
            "row_groups_read": read_groups,
            "row_groups_total": len(self.row_groups),
        })

        if agg == "count":
            return {"count": len(rows_out)}
        if agg == "sum":
            col = query["agg_col"]
            return {"sum": round(sum(r[col] for r in rows_out if r[col] is not None), 4)}
        if agg == "min":
            col = query["agg_col"]
            vals = [r[col] for r in rows_out if r[col] is not None]
            return {"min": min(vals) if vals else None}
        if agg == "max":
            col = query["agg_col"]
            vals = [r[col] for r in rows_out if r[col] is not None]
            return {"max": max(vals) if vals else None}
        return rows_out

    def _eval_predicate(self, row, pred):
        op = pred["op"]
        col = pred.get("col")
        val = pred.get("val")
        v = row.get(col) if col else None

        if op == "eq":
            return v == val
        if op == "neq":
            return v != val
        if op == "lt":
            return v is not None and v < val
        if op == "lte":
            return v is not None and v <= val
        if op == "gt":
            return v is not None and v > val
        if op == "gte":
            return v is not None and v >= val
        if op == "is_null":
            return v is None
        if op == "is_not_null":
            return v is not None
        if op == "and":
            return all(self._eval_predicate(row, p) for p in pred["operands"])
        if op == "or":
            return any(self._eval_predicate(row, p) for p in pred["operands"])
        if op == "not":
            return not self._eval_predicate(row, pred["operand"])
        raise ValueError(f"Unknown op: {op}")

    # ------------------------------------------------------------------
    # Output
    # ------------------------------------------------------------------

    def flush_trace(self, path):
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w") as f:
            for entry in self.trace:
                f.write(json.dumps(entry, sort_keys=True) + "\n")


def main():
    dataset_path = sys.argv[1] if len(sys.argv) > 1 else "/app/data/dataset.json"
    queries_path = sys.argv[2] if len(sys.argv) > 2 else "/app/data/queries.json"
    results_path = sys.argv[3] if len(sys.argv) > 3 else "/app/results.json"
    trace_path   = sys.argv[4] if len(sys.argv) > 4 else "/app/trace.jsonl"

    engine = Engine(dataset_path)

    with open(queries_path) as f:
        queries = json.load(f)

    results = {}
    for q in queries:
        result = engine.execute(q)
        results[q["id"]] = result

    os.makedirs(os.path.dirname(os.path.abspath(results_path)), exist_ok=True)
    with open(results_path, "w") as f:
        json.dump(results, f, sort_keys=True, indent=2)

    engine.flush_trace(trace_path)
    print(f"Wrote {results_path} and {trace_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
