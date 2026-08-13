The query engine at `/app/engine/` has a predicate-pushdown system that's not finished. Right now it reads every row group for every query, no matter what the predicate says. Your job is to make it actually prune row groups using the per-row-group min/max statistics that are already stored in the Parquet footers.

There are two problems you need to fix.

**Off-by-one in the statistics writer.** The code that writes min/max values for each row group has a boundary bug. Predicates that land exactly on the min or max value of a row group get wrong statistics, which causes incorrect pruning. Find it and fix it before you implement pruning.

**No pruning in the planner.** The planner currently ignores statistics entirely. Add logic so that before reading a row group, the engine checks whether the predicate can possibly match anything in that row group. If the statistics prove no row in the group can match, skip it.

**Null handling.** Nulls are excluded from min/max statistics. That means you cannot use min/max to prune `IS NULL` predicates or negated predicates — doing so would give wrong results. Make sure your pruning logic leaves those cases alone.

## What to produce

Run the engine against the provided queries and write two output files.

**`/app/results.json`** — query results. One JSON object per line (or a JSON array — check the engine's existing output format and match it). Each entry must include the query identifier and the result rows.

**`/app/trace.jsonl`** — execution trace. One JSON object per line, one per query. Each object must record which row groups were actually read for that query. For example:

```json
{"query_id": "q1", "row_groups_read": [0, 2]}
```

Both files are required. Use those exact absolute paths.

## Done means

- Results in `/app/results.json` match the correct answers for every query.
- The trace in `/app/trace.jsonl` shows that at least some queries with equality or range predicates on non-null columns read fewer row groups than the total. If every query reads every row group, the solution is wrong even if the result rows are correct.
- Null-related queries return correct results and are not pruned incorrectly.
- The off-by-one fix is in place so boundary-value predicates work.

You have 1800 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
