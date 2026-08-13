"""
Positive control: a valid index that includes the reference facts plus a few extra exact pair maps.
"""

import pyarrow as pa

from common import INDEX_PATH, index_from_data, load_queries, parquet_file, write_json, write_results

REFERENCE_PAIRS = [("segment", "status"), ("region", "channel"), ("sku", "event_day")]
EXTRA_PAIRS = [("region", "status"), ("segment", "priority"), ("status", "priority")]


def main() -> None:
    parquet = parquet_file()
    queries = load_queries()
    distinct_columns = {
        field.name
        for field in parquet.schema_arrow
        if pa.types.is_string(field.type) or pa.types.is_integer(field.type)
    }
    write_json(INDEX_PATH, index_from_data(parquet, distinct_columns=distinct_columns, pair_columns=REFERENCE_PAIRS + EXTRA_PAIRS))
    write_results(parquet, queries)


if __name__ == "__main__":
    main()
