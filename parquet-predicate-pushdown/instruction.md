The query engine in /app/engine/ can run queries but it's slow because it reads every row group in the file even when the data tells you the row group can't possibly match. Your job is to fix that and add the execution trace.

The dataset is at /app/data/sales.parquet and /app/data/queries.json. Each query in queries.json has a predicate tree, a column projection, a max_row_groups_read limit, and a min_result_count. Your engine must meet all of them.

Predicates are trees. Nodes can be and, or, or not. Leaf types are cmp (with op eq/ne/lt/le/gt/ge), in (a list of values), is_null, and is_not_null. The engine already evaluates predicates correctly. What it doesn't do is skip row groups.

The Parquet file has column-level min/max statistics in each row group. That's a start, but it won't get you far enough on its own. Some predicates use value sets (in lists against string columns) or null checks that plain range statistics can't answer. You'll need to build a secondary index over the row groups yourself. generate.py in /app/data/ is a starting point for understanding the data layout.

Add row-group pruning to the engine. A row group can be skipped only when you can prove the predicate cannot match any row in it. Nulls complicate this: if a column has null values in a row group, a predicate on that column can only be proven impossible if the column is all nulls and you're looking for a non-null value, or the null count is zero and you need a null. Don't prune on weak evidence.

Write the results to /app/results.json. It must be a JSON array. Each element must be an object with exactly two keys: query_id (string) and rows (array of row objects). Row objects must have exactly the keys named in the query's columns list and no others. Rows must appear in sequential row-group scan order, not sorted or reordered.

Write the trace to /app/trace.jsonl. One JSON object per line, exactly one line per query, in the same order as the queries in queries.json. Each object must have exactly four keys: query_id (string), read_row_groups (array), query_receipt (string), result_count (integer).

Each element of read_row_groups must have exactly three keys: row_group (non-negative integer), decoded_rows (integer), receipt (string). Row groups must appear in strictly increasing order by row_group index. The receipt for a row group is computed as follows: take the decoded rows for that row group (the full batch you read, before filtering), hash them with blake2b(digest_size=16) starting with the UTF-8 bytes of "rows=N" where N is the number of rows, then for each row in order append the JSON encoding of the row (keys sorted, no spaces, NaN float values encoded as the string "NaN") followed by a newline byte. The hex digest is the receipt string.

The query_receipt chains the row-group receipts: start a blake2b(digest_size=16) hash, feed it the UTF-8 bytes of query_id followed by a pipe byte, then for each read row group in order feed it the UTF-8 bytes of "row_group_index:decoded_rows:receipt" followed by a pipe byte. The hex digest is the query_receipt string.

result_count must equal the number of rows in the corresponding results.json entry.

You have 14400 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
