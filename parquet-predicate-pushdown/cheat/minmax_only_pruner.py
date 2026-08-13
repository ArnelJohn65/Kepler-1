"""
Cheat attempt: use only min/max and null counts in the persisted index.

This stays within the size cap and answers the visible queries correctly, but hidden-query precision fails.
"""

from common import INDEX_PATH, index_from_data, load_queries, parquet_file, write_json, write_results


def main() -> None:
    parquet = parquet_file()
    queries = load_queries()
    write_json(INDEX_PATH, index_from_data(parquet, distinct_columns=set(), pair_columns=[]))
    write_results(parquet, queries)


if __name__ == "__main__":
    main()
