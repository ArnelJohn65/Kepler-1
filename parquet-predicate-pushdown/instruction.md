The dataset is at /app/data/sales.parquet and /app/data/queries.json. Each query includes a predicate tree, a column projection, a max_row_groups_read budget, and a min_result_count floor.

Predicates use these node types: and, or, not, cmp, in, is_null, is_not_null. cmp supports eq, ne, lt, le, gt, ge.

I need two artifacts:

1) /app/results.json
- JSON array
- exactly one object per query
- each object must have exactly: query_id (string), rows (array)
- each row object must have exactly the keys listed in that query's columns field
- rows must be in sequential row-group scan order

2) /app/trace.jsonl
- one JSON object per line
- exactly one line per query
- lines must follow the same query order as queries.json
- each line object must have exactly: query_id (string), read_row_groups (array), query_receipt (string), result_count (integer)

Each read_row_groups element must have exactly: row_group (non-negative integer), decoded_rows (integer), receipt (string).

Row groups in each query trace must be strictly increasing by row_group index.

receipt format:
- start blake2b with digest_size=16
- hash UTF-8 bytes of "rows=N" where N is decoded row count
- for each decoded row in order, hash JSON(row) with sorted keys and no spaces, where NaN is encoded as the string "NaN", then hash a newline byte
- lowercase hex digest is the receipt

query_receipt format:
- start blake2b with digest_size=16
- hash UTF-8 bytes of query_id, then a pipe byte
- for each read row group in order, hash UTF-8 bytes of "row_group:decoded_rows:receipt", then a pipe byte
- lowercase hex digest is the query_receipt

result_count must equal the number of rows for that query in results.json.

All query budgets must be satisfied. For each query, read_row_groups length must be <= max_row_groups_read. Returned rows must exactly match the reference rows from the predicate and projection.


You have 18000 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
