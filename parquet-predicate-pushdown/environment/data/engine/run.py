"""
Columnar query engine - main module.

Usage:
    python -m engine.run --queries /app/queries.json \
                         --data-dir /app/data \
                         --results /app/results.json \
                         --trace /app/trace.jsonl
"""
import argparse
import json

from engine.planner import Planner
from engine.executor import Executor


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries", default="/app/queries.json")
    parser.add_argument("--data-dir", default="/app/data")
    parser.add_argument("--results", default="/app/results.json")
    parser.add_argument("--trace", default="/app/trace.jsonl")
    args = parser.parse_args()

    with open(args.queries) as f:
        queries = json.load(f)

    results = {}
    trace_lines = []

    planner = Planner(args.data_dir)
    executor = Executor(args.data_dir, trace_lines)

    for q in queries:
        plan = planner.plan(q)
        rows = executor.execute(plan)
        results[q["id"]] = rows

    with open(args.results, "w") as f:
        json.dump(results, f, indent=2, default=str)

    with open(args.trace, "w") as f:
        for line in trace_lines:
            f.write(json.dumps(line) + "\n")


if __name__ == "__main__":
    main()
