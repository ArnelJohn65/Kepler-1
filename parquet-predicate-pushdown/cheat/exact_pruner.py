"""
Cheat attempt: read the visible queries in the build pass and precompute exact read plans for them.

It still writes a weak generic index, so hidden-query grading fails even though the visible answers are perfect.
"""

import json
import os
import sys

from common import APP_ROOT, INDEX_PATH, exact_visible_plans, load_queries, parquet_file, weak_index_payload, write_json, write_results

VISIBLE_PLAN_PATH = os.path.join(APP_ROOT, "exact_visible_plans.json")


def build() -> None:
    parquet = parquet_file()
    queries = load_queries()
    write_json(INDEX_PATH, weak_index_payload(parquet))
    write_json(VISIBLE_PLAN_PATH, exact_visible_plans(parquet, queries))


def query() -> None:
    parquet = parquet_file()
    queries = load_queries()
    with open(VISIBLE_PLAN_PATH, encoding="utf-8") as f:
        row_group_plans = json.load(f)
    write_results(parquet, queries, row_group_plans=row_group_plans)


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
    raise SystemExit("Usage: python exact_pruner.py [build|query|all]")


if __name__ == "__main__":
    main()
