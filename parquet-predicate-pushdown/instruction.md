The dataset is at /app/data/sales.parquet. A query specification is at /app/data/queries.json.

The build pass runs before the query pass. The build pass may read the parquet file and write any index or summary to /app/. The build pass must not read /app/data/queries.json. It should produce an index that works for any predicate over the schema, without knowing the specific queries in advance.

The query pass runs after the build pass. It reads /app/data/queries.json and the index from /app/. For each query, it reads only the row groups that the index cannot prove are empty, decodes those groups, filters them, and writes the results.

Each query in queries.json has:
- id
- predicate tree
- columns projection
- max_row_groups_read
- max_decoded_bytes
- min_result_count

Predicate nodes: and, or, not, cmp, in, is_null, is_not_null. cmp ops: eq, ne, lt, le, gt, ge.

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
- For each query, define receipt columns as sorted(unique(projection columns union predicate columns)).
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

Returned rows must exactly match reference predicate evaluation and projection. Every row group that contains at least one matching row must appear in read_row_groups.

You have 18000 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
