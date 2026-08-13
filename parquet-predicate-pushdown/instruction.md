The dataset is at /app/data/sales.parquet and /app/data/queries.json.

Each query has:
- id
- predicate tree
- columns projection
- max_row_groups_read
- max_decoded_bytes
- min_result_count

Predicate nodes: and, or, not, cmp, in, is_null, is_not_null. cmp ops: eq, ne, lt, le, gt, ge.

Run in two passes:
1) Build pass: construct and persist any row-group index you need. This pass may scan all row groups.
2) Query pass: read the persisted index and answer all queries, then write the artifacts.

The query pass is time-budgeted by the runner. A full-scan query pass is expected to miss budget.

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

Each read_row_groups element must have exactly:
- row_group (non-negative integer)
- decoded_rows (integer)
- decoded_bytes (integer)
- receipt (string)

Row groups in each query trace must be strictly increasing by row_group index.

Value encoding requirements for both results.json rows and receipt row JSON payloads:
- timestamps: ISO 8601 UTC with Z suffix
- decimals: format(Decimal, "f")
- regular floats: JSON number
- NaN: the string "NaN"
- null: JSON null

Receipt requirements:
- For each query, define receipt columns as sorted(unique(projection columns ∪ predicate columns)).
- For each reported row group, decode exactly those receipt columns for every row in that row group.
- start blake2b with digest_size=16
- hash UTF-8 bytes of "rows=N" where N is decoded row count
- for each decoded row in order, hash JSON(row) with sorted keys and no spaces, then hash a newline byte
- lowercase hex digest is the receipt

query_receipt format:
- start blake2b with digest_size=16
- hash UTF-8 bytes of query_id, then a pipe byte
- for each read row group in order, hash UTF-8 bytes of "row_group:decoded_rows:decoded_bytes:receipt", then a pipe byte
- lowercase hex digest is the query_receipt

result_count must equal the number of rows for that query in results.json.

Per query, both budgets must hold:
- len(read_row_groups) <= max_row_groups_read
- sum(decoded_bytes for read_row_groups) <= max_decoded_bytes

Returned rows must exactly match reference predicate evaluation and projection.


You have 18000 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
