The dataset is at /app/data/sales.parquet and /app/data/queries.json. Each query has a predicate tree, a column projection, a max_row_groups_read limit, and a min_result_count. Your engine must satisfy all of them.

Predicates are trees. Nodes can be and, or, or not. Leaf types are cmp (with op eq/ne/lt/le/gt/ge), in (a list of values), is_null, and is_not_null. The engine in /app/engine/ already evaluates predicates correctly against decoded row data. What it does not do is skip row groups.

Add row-group pruning. A row group can be skipped only when you can prove the predicate cannot match any row in it. Every row group your pruner does not skip must appear in the trace for that query, including row groups where the predicate matched zero rows after decoding.

Write the results to /app/results.json. It must be a JSON array. Each element must be an object with exactly two keys: query_id (string) and rows (array of row objects). Row objects must have exactly the keys named in the query's columns list and no others. Rows must appear in sequential row-group scan order, not sorted or reordered.

Write the trace to /app/trace.jsonl. One JSON object per line, exactly one line per query, in the same order as the queries in queries.json. Each object must have exactly four keys: query_id (string), read_row_groups (array), query_receipt (string), result_count (integer).

Each element of read_row_groups must have exactly three keys: row_group (non-negative integer), decoded_rows (integer), receipt (string). Row groups must appear in strictly increasing order by row_group index. The receipt for a row group is computed as follows: take the decoded rows for that row group (the full batch you read, before filtering), hash them with blake2b(digest_size=16) starting with the UTF-8 bytes of "rows=N" where N is the number of rows, then for each row in order append the JSON encoding of the row (keys sorted, no spaces, NaN float values encoded as the string "NaN") followed by a newline byte. The hex digest is the receipt string.

The query_receipt chains the row-group receipts: start a blake2b(digest_size=16) hash, feed it the UTF-8 bytes of query_id followed by a pipe byte, then for each read row group in order feed it the UTF-8 bytes of "row_group_index:decoded_rows:receipt" followed by a pipe byte. The hex digest is the query_receipt string.

result_count must equal the number of rows in the corresponding results.json entry.

You have 14400 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
