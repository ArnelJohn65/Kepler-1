A columnar query engine lives in `/app/data/engine.py`. It scans every row group for every query. Your job is to fix a bug and add row-group pruning so it stops doing that.

**The engine**

The engine source is `/app/data/engine.py`. It reads a JSON dataset split into row groups. Each row group has per-column min/max statistics. The engine runs a fixed query suite from `/app/data/queries.json` and writes results to `/app/results.json` and an execution trace to `/app/trace.jsonl`.

**What you must do**

1. Fix the off-by-one bug in `RowGroupStats.update` in `/app/data/engine.py`. The bug is in how `max_val` is initialised on the first non-null value in a row group. It causes wrong answers on boundary predicates such as `x >= <exact max of a row group>`.

2. Implement `_can_prune` in the `Engine` class. The method currently always returns `False`. It must return `True` when per-column min/max statistics prove that a row group contains no rows matching the predicate. Handle NULL semantics correctly: nulls are excluded from min/max, so `IS NULL` and `IS NOT NULL` predicates must not cause incorrect pruning.

3. Generate the dataset and run the engine:

```
python /app/data/generate_dataset.py /app/data
python /app/data/engine.py \
    /app/data/dataset.json \
    /app/data/queries.json \
    /app/results.json \
    /app/trace.jsonl
```

**Output files**

- `/app/results.json` — query results keyed by query id, stable JSON
- `/app/trace.jsonl` — one JSON line per query with fields `query_id`, `row_groups_read` (list of indices), and `row_groups_total`

**Verification**

Results are checked against expected values for all 10 queries. The trace is checked for pruning evidence: the total number of row groups read across all queries must be at most 130. A full-scan engine reads 200 (20 row groups × 10 queries), so 130 is only reachable with working pushdown. Two queries explicitly test the boundary bug: if `max_val` is still off by one, those queries return wrong counts and fail.

You have 3600 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
