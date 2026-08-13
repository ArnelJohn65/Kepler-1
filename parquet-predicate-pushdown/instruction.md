There is a columnar query engine in `/app/engine/`. It reads Parquet files, but it currently reads every row group for every query regardless of the query predicate. Your job is to make it smarter.

The engine already writes per-row-group min/max statistics when it generates data. There is a bug in that statistics writer. Find and fix the bug. The bug affects predicates on boundary values — the maximum value of a row group is recorded incorrectly because of an off-by-one in the accumulator loop.

After fixing the bug, implement predicate pushdown in the query planner. When the planner sees a simple comparison predicate on a column, it should use the row-group statistics to skip row groups that cannot contain matching rows. If a row group's max is less than the lower bound of the predicate, skip it. If a row group's min is greater than the upper bound, skip it.

Pay attention to NULL semantics. The min/max statistics exclude NULL values. This means that a row group containing only NULLs will have no statistics. Do not skip a row group when statistics are absent. Do not prune row groups for IS NULL predicates or for NOT operators that flip the meaning of the comparison.

The engine must emit an execution trace to `/app/trace.jsonl`. Each line is a JSON object. When the engine reads a row group, emit a record like:

```
{"event": "row_group_read", "file": "...", "row_group": 0, "rows": 1000}
```

When it skips a row group due to predicate pushdown, emit:

```
{"event": "row_group_skipped", "file": "...", "row_group": 1, "reason": "predicate_pushdown"}
```

After the engine finishes running all queries, write the query results to `/app/results.json`. The format is a JSON object whose keys are query IDs and values are lists of row dicts, for example:

```json
{"q1": [{"id": 1, "val": 42}], "q2": []}
```

The queries to run are defined in `/app/queries.json`. Each query has an `id`, a `table`, a `predicate`, and a list of `columns` to return.

To generate the dataset and set up the engine workspace, run:

```
cd /app && python -m engine.generate
```

This creates the Parquet files under `/app/data/` and writes the queries to `/app/queries.json`.

You have 14400 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
