The query engine in /app/engine/ works, but it's slow for a dumb reason. Every query reads every row group in the file, even when the statistics already tell us the row group can't contain a matching row. Nobody ever wired the pruning up.

There's a second problem underneath that one. The code that writes the per row group min/max stats has an off by one in it. You won't notice it on most queries. It shows up on boundary predicates, where the constant in the predicate is exactly equal to the min or the max of a row group. Fix that first. If you build pruning on top of bad stats you'll start dropping rows that should have come back, and the results will be quietly wrong instead of loudly broken.

Then add row group pruning to the planner so it skips row groups the stats rule out.

Watch the nulls. Nulls aren't counted in min/max, so a row group full of nulls has stats that look empty or misleading. IS NULL has to keep working, and so do negated predicates. Don't prune a row group just because its stats don't overlap the predicate range if nulls could still satisfy it. This is where I'd expect this to go wrong.

Run the full query suite and write the answers to /app/results.json.

Also write an execution trace to /app/trace.jsonl recording which row groups were actually read for each query. One JSON object per line. This is how the pruning gets measured, so it has to reflect real reads, not what you intended to read.

You're done when both files exist, the results are correct, and the trace shows the engine is genuinely skipping row groups.

You have 14400 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
