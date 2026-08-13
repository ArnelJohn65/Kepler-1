#!/usr/bin/env bash
# Reference solution for parquet-predicate-pushdown.
# 1. Fix the off-by-one bug in engine/stats.py.
# 2. Implement predicate pushdown in engine/planner.py.
# 3. Generate the dataset.
# 4. Run the engine to produce /app/results.json and /app/trace.jsonl.

set -euo pipefail

###############################################################################
# Step 1: Fix the off-by-one bug in stats.py
###############################################################################
cat > /app/engine/stats.py << 'PYEOF'
"""Statistics writer for row groups."""
from typing import Any, Dict, List, Optional


def compute_stats(values: List[Any]) -> Optional[Dict[str, Any]]:
    """Return min/max statistics for a list of values, excluding NULLs."""
    non_null = [v for v in values if v is not None]
    if not non_null:
        return None

    min_val = non_null[0]
    max_val = non_null[0]

    # Fixed: iterate over all elements (was range(len(non_null) - 1))
    for i in range(1, len(non_null)):
        v = non_null[i]
        if v < min_val:
            min_val = v
        if v > max_val:
            max_val = v

    return {"min": min_val, "max": max_val, "null_count": len(values) - len(non_null)}
PYEOF

###############################################################################
# Step 2: Implement predicate pushdown in planner.py
###############################################################################
cat > /app/engine/planner.py << 'PYEOF'
"""Query planner with predicate pushdown."""
import json
import os
from typing import Any, Dict, List, Optional


def _can_prune(stats: Optional[Dict], pred: Dict) -> bool:
    """Return True if the row group can be definitively skipped.

    Conservative: any uncertainty returns False (do not prune).
    """
    if stats is None:
        return False

    op = pred.get("op")

    # Compound operators: only prune if ALL children say prune (AND),
    # or ANY child says prune (for OR the whole group must be prunable).
    if op == "AND":
        return any(_can_prune(stats, c) for c in pred["children"])
    if op == "OR":
        return all(_can_prune(stats, c) for c in pred["children"])
    # NOT: never prune — flipping comparison makes pruning unsound.
    if op in ("NOT", "IS NULL", "IS NOT NULL", "!="):
        return False

    col = pred.get("col")
    val = pred.get("val")

    col_stats = stats.get(col)
    if col_stats is None:
        return False

    rg_min = col_stats.get("min")
    rg_max = col_stats.get("max")
    if rg_min is None or rg_max is None:
        return False

    if op == "=":
        return val < rg_min or val > rg_max
    if op == "<":
        return rg_min >= val
    if op == "<=":
        return rg_min > val
    if op == ">":
        return rg_max <= val
    if op == ">=":
        return rg_max < val

    return False


class Planner:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        index_path = os.path.join(data_dir, "stats_index.json")
        with open(index_path) as f:
            self.index = json.load(f)

    def plan(self, query: Dict[str, Any]) -> Dict[str, Any]:
        table = query["table"]
        filename = f"{table}.parquet"

        if filename not in self.index:
            raise KeyError(f"Unknown table: {table}")

        predicate = query.get("predicate")
        rg_stats_list = self.index[filename]
        row_groups = []

        for rg_idx, rg_stats in enumerate(rg_stats_list):
            if predicate and _can_prune(rg_stats, predicate):
                continue
            row_groups.append(rg_idx)

        skipped = [i for i in range(len(rg_stats_list)) if i not in row_groups]

        return {
            "query": query,
            "scans": [
                {
                    "file": filename,
                    "row_groups": row_groups,
                    "skipped_row_groups": skipped,
                }
            ],
        }
PYEOF

###############################################################################
# Step 3: Generate dataset
###############################################################################
cd /app
python -m engine.generate

###############################################################################
# Step 4: Run the engine
###############################################################################
python -m engine.run \
    --queries /app/queries.json \
    --data-dir /app/data \
    --results /app/results.json \
    --trace /app/trace.jsonl

echo "Done. Results: /app/results.json  Trace: /app/trace.jsonl"
