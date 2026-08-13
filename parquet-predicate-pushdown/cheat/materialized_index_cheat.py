"""
Cheat attempt: store far too much exact column data in the index.

The visible answers are correct, but the persisted JSON artifact blows the size cap.
"""

import sys

from common import INDEX_PATH, index_from_data, load_queries, parquet_file, write_json, write_results


def build() -> None:
    parquet = parquet_file()
    all_columns = {field.name for field in parquet.schema_arrow}
    all_pairs = []
    names = list(all_columns)
    for idx, left in enumerate(names):
        for right in names[idx + 1 :]:
            all_pairs.append((left, right))
    write_json(INDEX_PATH, index_from_data(parquet, distinct_columns=all_columns, pair_columns=all_pairs))


def query() -> None:
    parquet = parquet_file()
    write_results(parquet, load_queries())


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "build":
        build()
        return
    if mode == "query":
        query()
        return
    if mode == "all":
        build()
        query()
        return
    raise SystemExit("Usage: python materialized_index_cheat.py [build|query|all]")


if __name__ == "__main__":
    main()
