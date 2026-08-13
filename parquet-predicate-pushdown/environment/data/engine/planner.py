"""
Query planner.

Reads per-row-group statistics from the metadata index and produces a plan
that lists which row groups to read for each file.

Currently the planner does NOT implement predicate pushdown — it schedules
every row group for every query. The agent must add that logic.
"""
import json
import os
from typing import Any, Dict, List, Optional


class Planner:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        index_path = os.path.join(data_dir, "stats_index.json")
        with open(index_path) as f:
            # index: { "filename": [ {col: {min, max, null_count}, ...}, ... ] }
            self.index = json.load(f)

    def plan(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Produce an execution plan for a query.

        A plan is:
        {
            "query": <original query dict>,
            "scans": [
                {
                    "file": "sensors.parquet",
                    "row_groups": [0, 1, 2, ...]   # which row groups to read
                },
                ...
            ]
        }
        """
        table = query["table"]
        filename = f"{table}.parquet"

        if filename not in self.index:
            raise KeyError(f"Unknown table: {table}")

        num_rgs = len(self.index[filename])
        # No predicate pushdown: read everything.
        row_groups = list(range(num_rgs))

        skipped = [i for i in range(num_rgs) if i not in row_groups]

        return {
            "query": query,
            "scans": [
                {
                    "file": filename,
                    "row_groups": row_groups,
                    "skipped_row_groups": skipped,
                }
            ],
        }
