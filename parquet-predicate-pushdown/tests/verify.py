"""
Verifier for the parquet-predicate-pushdown task.

Checks:
1. /app/results.json exists and contains correct query results.
2. /app/trace.jsonl exists and shows actual pruning for selective queries.
3. Null queries are not incorrectly pruned.
"""

import json
import sys
import os

RESULTS_PATH = "/app/results.json"
TRACE_PATH = "/app/trace.jsonl"

# Ground truth is computed by the verifier itself using a correct implementation.
# We bake the expected row counts here (sealed from agent).

# Query expectations (sealed ground truth)
# These must match what the correct engine produces on the generated data.
EXPECTED = {
    "q1": {"predicate_col": "amount", "op": "lt", "value": 50.0, "pruned_rgs": {1, 2}},
    "q2": {"predicate_col": "amount", "op": "ge", "value": 250.0, "pruned_rgs": {0, 1}},
    "q3": {"predicate_col": "category", "op": "eq", "value": "B", "pruned_rgs": {0, 2}},
    "q4": {"predicate_col": "nullable_col", "op": "is_null", "pruned_rgs": set()},  # must NOT prune
    "q5": {"predicate_col": "id", "op": "eq", "value": 99, "pruned_rgs": {1, 2}},  # boundary
    "q6": {"predicate_col": None, "op": None, "pruned_rgs": set()},  # no predicate
}

TOTAL_ROW_GROUPS = 3


def load_results():
    with open(RESULTS_PATH) as f:
        return {r["query_id"]: r["rows"] for r in json.load(f)}


def load_trace():
    trace = {}
    with open(TRACE_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                obj = json.loads(line)
                trace[obj["query_id"]] = set(obj["row_groups_read"])
    return trace


def verify_results(results: dict) -> list[str]:
    errors = []
    # q5 must return exactly one row (id=99)
    q5_rows = results.get("q5", [])
    ids = [r.get("id") for r in q5_rows]
    if 99 not in ids:
        errors.append(f"q5: expected row with id=99, got ids={ids}")
    if any(i != 99 for i in ids):
        errors.append(f"q5: got extra rows beyond id=99: {ids}")

    # q4 must not be empty (there are nulls in the data)
    q4_rows = results.get("q4", [])
    if len(q4_rows) == 0:
        errors.append("q4: expected at least some null rows, got none")

    # q6 must return 300 rows total
    q6_rows = results.get("q6", [])
    if len(q6_rows) != 300:
        errors.append(f"q6: expected 300 rows (no filter), got {len(q6_rows)}")

    # q1 rows must all have amount < 50
    for r in results.get("q1", []):
        if r.get("amount") is not None and r["amount"] >= 50.0:
            errors.append(f"q1: row with amount={r['amount']} should not appear (>=50)")
            break

    # q2 rows must all have amount >= 250
    for r in results.get("q2", []):
        if r.get("amount") is not None and r["amount"] < 250.0:
            errors.append(f"q2: row with amount={r['amount']} should not appear (<250)")
            break

    return errors


def verify_pruning(trace: dict) -> list[str]:
    errors = []
    for qid, exp in EXPECTED.items():
        if qid not in trace:
            errors.append(f"{qid}: missing from trace")
            continue
        rgs_read = trace[qid]
        pruned = set(range(TOTAL_ROW_GROUPS)) - rgs_read

        if exp["pruned_rgs"] and not (exp["pruned_rgs"] & pruned):
            errors.append(
                f"{qid}: expected row groups {exp['pruned_rgs']} to be pruned, "
                f"but read all of: {rgs_read}"
            )

        # q4 IS NULL — must read all row groups
        if qid == "q4" and len(rgs_read) != TOTAL_ROW_GROUPS:
            errors.append(
                f"q4 (IS NULL): must read all {TOTAL_ROW_GROUPS} row groups to find nulls, "
                f"but only read {rgs_read}"
            )

    return errors


def main():
    errors = []

    if not os.path.exists(RESULTS_PATH):
        print(f"FAIL: {RESULTS_PATH} does not exist")
        sys.exit(1)

    if not os.path.exists(TRACE_PATH):
        print(f"FAIL: {TRACE_PATH} does not exist")
        sys.exit(1)

    try:
        results = load_results()
    except Exception as e:
        print(f"FAIL: Could not parse {RESULTS_PATH}: {e}")
        sys.exit(1)

    try:
        trace = load_trace()
    except Exception as e:
        print(f"FAIL: Could not parse {TRACE_PATH}: {e}")
        sys.exit(1)

    errors += verify_results(results)
    errors += verify_pruning(trace)

    if errors:
        for e in errors:
            print(f"FAIL: {e}")
        sys.exit(1)

    print("PASS: all checks passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
