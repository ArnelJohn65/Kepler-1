The dataset is at /app/data/sales.parquet. The visible query list is at /app/data/queries.json. The verifier also has a hidden query set that is not present anywhere in the agent image.

I need two artifacts.

1) /app/results.json
- JSON array
- exactly one object per visible query
- each object must have exactly: query_id (string), rows (array)
- each row object must have exactly the keys listed in that query's columns field
- rows must be in sequential row-group scan order

2) /app/row_group_index.json
- JSON object
- exact top-level keys: format, parquet_file, row_groups
- format must be "row-group-index-v1"
- parquet_file must be "sales.parquet"
- row_groups must be a JSON array with exactly one entry for each parquet row group, in row-group index order

Each row_groups entry must be an object with exactly:
- row_group (non-negative integer)
- num_rows (non-negative integer)
- columns (object)
- pair_distinct_values (object)

The columns object must contain exactly one key for every parquet column name. Each value must be an object with exactly:
- min
- max
- null_count
- distinct_values
- has_nan

Rules for those column summaries:
- min and max are either JSON null or canonical scalar values for that column
- null_count is the exact number of nulls in that row group for that column
- distinct_values is either JSON null or the exact set of unique non-null values present in that row group for that column, encoded as a JSON array with no duplicates
- has_nan is true only if that row group contains at least one NaN in that column

The pair_distinct_values object maps "left|right" to the exact set of observed row-group value pairs for those two columns. Each value must be a JSON array of two-element JSON arrays [left_value, right_value]. You may include any distinct-column pair keys you want. The verifier only uses facts that are actually present in your index.

Canonical scalar encoding rules for both artifacts:
- timestamps: ISO 8601 UTC with Z suffix
- decimals: format(Decimal, "f")
- regular floats: JSON number
- NaN: the string "NaN"
- null: JSON null

Each visible query in queries.json has:
- id
- predicate tree
- columns projection

Predicate nodes: and, or, not, cmp, in, is_null, is_not_null. cmp ops: eq, ne, lt, le, gt, ge.

The build pass runs before the query pass. The build pass may read /app/data/sales.parquet and must write /app/row_group_index.json. The query pass must load that same persisted index, answer the visible queries from /app/data/queries.json, and write /app/results.json.

The verifier checks /app/results.json for exact visible-query correctness. It checks /app/row_group_index.json against hidden queries by using the index to decide which row groups are still possible matches. Any hidden query where your index excludes a row group that really contains a matching row is an immediate failure. Hidden-query precision is graded from how many row groups remain possible after applying your index. There is no self-reported read trace.

/app/row_group_index.json must be at most 2097152 bytes.

You have 18000 seconds to complete this task. Do not cheat by using online solutions or hints specific to this task.
