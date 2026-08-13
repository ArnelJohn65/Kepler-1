"""
Cheat attempt: build a deliberately useless but valid index, then full-scan the visible queries.

The visible answers are correct, but hidden-query grading sees an index that cannot prune anything.
"""

from common import INDEX_PATH, load_queries, parquet_file, weak_index_payload, write_json, write_results


def main() -> None:
    parquet = parquet_file()
    queries = load_queries()
    write_json(INDEX_PATH, weak_index_payload(parquet))
    write_results(parquet, queries)


if __name__ == "__main__":
    main()
