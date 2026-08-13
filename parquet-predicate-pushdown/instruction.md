I need you to improve the query execution path in `/app/engine/` and then run the full query set.

The dataset and query specs are under `/app/data/`.

Each query in `/app/data/queries.json` uses a predicate tree. Nodes can be `and`, `or`, or `not`. Leaf predicates include comparison (`eq`, `ne`, `lt`, `le`, `gt`, `ge`), `in`, `is_null`, and `is_not_null`.

Your output files must be:
- `/app/results.json`
- `/app/trace.jsonl`

`/app/results.json` must be a JSON array. Each item must be an object with:
- `query_id` (string)
- `rows` (array of row objects)

`/app/trace.jsonl` must contain one JSON object per line, exactly one line per query. Each object must contain:
- `query_id` (string)
- `read_row_groups` (array of objects)
  - each read-row-group object must contain:
    - `row_group` (integer)
    - `decoded_rows` (integer)
    - `receipt` (string)
- `query_receipt` (string)
- `result_count` (integer)

The verifier checks two things at the same time:
1. Correct query answers for every query.
2. Real row-group pruning based on `max_row_groups_read` in each query spec.

The verifier also recomputes row-group receipts from decoded Parquet data and checks them against your trace.

Do not change file names or key names in either artifact.

You have 14400 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
