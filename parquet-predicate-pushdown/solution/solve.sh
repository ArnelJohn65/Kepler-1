#!/usr/bin/env bash
# Reference solution: fix the off-by-one bug in RowGroupStats and implement
# predicate pushdown in _can_prune.
set -euo pipefail

ENGINE=/app/data/engine.py

# ---- 1. Fix the off-by-one bug in RowGroupStats.update ----
# The buggy line is:
#   self.max_val = (value - 1) if isinstance(value, (int, float)) else value
# Replace it with the correct version:
#   self.max_val = value
python3 - "$ENGINE" << 'PYEOF'
import re, sys

path = sys.argv[1]
with open(path) as f:
    src = f.read()

old = '            self.max_val = (value - 1) if isinstance(value, (int, float)) else value'
new = '            self.max_val = value'
assert old in src, "Bug line not found — engine may have changed"
src = src.replace(old, new, 1)

with open(path, "w") as f:
    f.write(src)

print("Bug fixed.", file=sys.stderr)
PYEOF

# ---- 2. Implement _can_prune ----
python3 - "$ENGINE" << 'PYEOF'
import sys

path = sys.argv[1]
with open(path) as f:
    src = f.read()

OLD = '''    def _can_prune(self, rg, predicate):
        """Return True if statistics prove this row group has no matching rows.

        Not yet implemented — always returns False (no pruning).
        The agent must implement this method to achieve measurable row-group
        pruning and also fix the off-by-one bug in RowGroupStats.update above.
        """
        return False'''

NEW = '''    def _can_prune(self, rg, predicate):
        """Return True if statistics prove this row group has no matching rows."""
        op = predicate["op"]
        col = predicate.get("col")

        if op == "and":
            return any(self._can_prune(rg, p) for p in predicate["operands"])
        if op == "or":
            return all(self._can_prune(rg, p) for p in predicate["operands"])
        # For NOT, IS NULL, IS NOT NULL, NEQ we conservatively keep the row group.
        if op in ("not", "is_null", "is_not_null", "neq"):
            return False

        if col is None or col not in rg.stats:
            return False

        s = rg.stats[col]
        val = predicate.get("val")

        # If the column has no stats (all nulls), we cannot prune.
        if s.min_val is None or s.max_val is None:
            return False

        if op == "eq":
            return s.max_val < val or s.min_val > val
        if op == "gt":
            return s.max_val <= val
        if op == "gte":
            return s.max_val < val
        if op == "lt":
            return s.min_val >= val
        if op == "lte":
            return s.min_val > val

        return False'''

assert OLD in src, "Placeholder _can_prune not found"
src = src.replace(OLD, NEW, 1)

with open(path, "w") as f:
    f.write(src)

print("_can_prune implemented.", file=sys.stderr)
PYEOF

# ---- 3. Run the query suite ----
python3 /app/data/generate_dataset.py /app/data
python3 /app/data/engine.py \
    /app/data/dataset.json \
    /app/data/queries.json \
    /app/results.json \
    /app/trace.jsonl

echo "solve.sh complete" >&2
