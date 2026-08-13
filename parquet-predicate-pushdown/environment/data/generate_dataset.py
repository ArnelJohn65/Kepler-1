"""Generate a deterministic dataset and query suite for the predicate-pushdown task."""

import json
import os
import random
import sys

SEED = 42
ROW_GROUP_SIZE = 500
NUM_ROW_GROUPS = 20       # 10 000 rows total
NULL_RATE = 0.05
OUTPUT_DIR = sys.argv[1] if len(sys.argv) > 1 else "/app/data"


def make_dataset():
    rng = random.Random(SEED)
    columns = ["id", "value", "category", "score"]
    row_groups = []
    row_id = 0
    for rg_idx in range(NUM_ROW_GROUPS):
        base = rg_idx * 1000
        rows = []
        for _ in range(ROW_GROUP_SIZE):
            value = base + rng.randint(0, 999)
            score = round(rng.uniform(0.0, 100.0), 4)
            category = rng.choice(["A", "B", "C", "D"])
            if rng.random() < NULL_RATE:
                value = None
            if rng.random() < NULL_RATE:
                score = None
            rows.append({
                "id": row_id,
                "value": value,
                "category": category,
                "score": score,
            })
            row_id += 1
        row_groups.append({"rows": rows})
    return {"columns": columns, "row_groups": row_groups}


def make_queries(dataset):
    rgs = dataset["row_groups"]

    # Exact boundary values for row group 5 (value range ~5000-5999)
    rg5_values = [r["value"] for r in rgs[5]["rows"] if r["value"] is not None]
    rg5_min = min(rg5_values)   # e.g. 5003
    rg5_max = max(rg5_values)   # e.g. 5999

    queries = [
        # Q1: narrow range inside one row group — prunes ~19/20
        {
            "id": "q1_range_narrow",
            "predicate": {"op": "and", "operands": [
                {"op": "gte", "col": "value", "val": 5000},
                {"op": "lt",  "col": "value", "val": 6000},
            ]},
            "aggregate": "count",
        },
        # Q2: boundary predicate — x >= exact max of RG5.
        # Without the bug fix, the stored max is (rg5_max - 1), so a pruner
        # would incorrectly prune RG5 for this query and return a wrong count.
        {
            "id": "q2_boundary_gte_max",
            "predicate": {"op": "gte", "col": "value", "val": rg5_max},
            "aggregate": "count",
        },
        # Q3: boundary predicate — x <= exact min of RG5.
        {
            "id": "q3_boundary_lte_min",
            "predicate": {"op": "lte", "col": "value", "val": rg5_min},
            "aggregate": "count",
        },
        # Q4: IS NULL — must NOT prune any row group (nulls excluded from stats)
        {
            "id": "q4_is_null",
            "predicate": {"op": "is_null", "col": "value"},
            "aggregate": "count",
        },
        # Q5: IS NOT NULL
        {
            "id": "q5_is_not_null",
            "predicate": {"op": "is_not_null", "col": "value"},
            "aggregate": "count",
        },
        # Q6: narrow window in RG10 — prunes ~19/20
        {
            "id": "q6_range_rg10",
            "predicate": {"op": "and", "operands": [
                {"op": "gte", "col": "value", "val": 10000},
                {"op": "lt",  "col": "value", "val": 11000},
            ]},
            "aggregate": "count",
        },
        # Q7: no predicate — full scan, reads all
        {
            "id": "q7_full_scan",
            "aggregate": "count",
        },
        # Q8: sum of score for RG3 range
        {
            "id": "q8_sum_score",
            "predicate": {"op": "and", "operands": [
                {"op": "gte", "col": "value", "val": 3000},
                {"op": "lt",  "col": "value", "val": 4000},
            ]},
            "aggregate": "sum",
            "agg_col": "score",
        },
        # Q9: category filter only — no value range, cannot prune via value stats
        {
            "id": "q9_category",
            "predicate": {"op": "eq", "col": "category", "val": "A"},
            "aggregate": "count",
        },
        # Q10: above all data — result must be 0, prunes all row groups
        {
            "id": "q10_above_all",
            "predicate": {"op": "gt", "col": "value", "val": 999999},
            "aggregate": "count",
        },
    ]
    return queries


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dataset = make_dataset()
    queries = make_queries(dataset)

    dataset_path = os.path.join(OUTPUT_DIR, "dataset.json")
    queries_path = os.path.join(OUTPUT_DIR, "queries.json")

    with open(dataset_path, "w") as f:
        json.dump(dataset, f)
    with open(queries_path, "w") as f:
        json.dump(queries, f, indent=2)

    print(f"Generated {dataset_path} and {queries_path}", file=sys.stderr)
    print(f"  {len(dataset['row_groups'])} row groups x {len(dataset['row_groups'][0]['rows'])} rows", file=sys.stderr)


if __name__ == "__main__":
    main()
