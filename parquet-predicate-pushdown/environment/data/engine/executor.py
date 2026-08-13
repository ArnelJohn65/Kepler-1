"""
Query executor.

Reads the specified row groups from Parquet files, applies in-memory
filtering, and returns result rows.
"""
import json
import os
from typing import Any, Dict, List

import pyarrow.parquet as pq


class Executor:
    def __init__(self, data_dir: str, trace: List[Dict]):
        self.data_dir = data_dir
        self.trace = trace

    def execute(self, plan: Dict[str, Any]) -> List[Dict]:
        query = plan["query"]
        predicate = query.get("predicate")
        columns = query.get("columns")

        result_rows = []

        for scan in plan["scans"]:
            filepath = os.path.join(self.data_dir, scan["file"])
            pf = pq.ParquetFile(filepath)

            # Emit skip events for pruned row groups
            for skip_rg in scan.get("skipped_row_groups", []):
                self.trace.append({
                    "event": "row_group_skipped",
                    "file": scan["file"],
                    "row_group": skip_rg,
                    "reason": "predicate_pushdown",
                })

            for rg_idx in scan["row_groups"]:
                table = pf.read_row_group(rg_idx, columns=columns)
                self.trace.append({
                    "event": "row_group_read",
                    "file": scan["file"],
                    "row_group": rg_idx,
                    "rows": table.num_rows,
                })
                df = table.to_pydict()
                num_rows = table.num_rows

                for i in range(num_rows):
                    row = {col: df[col][i] for col in df}
                    if predicate is None or _eval_predicate(row, predicate):
                        result_rows.append(row)

        # Stable ordering for deterministic results
        if result_rows:
            sort_key = list(result_rows[0].keys())[0]
            result_rows.sort(key=lambda r: (r[sort_key] is None, r[sort_key]))

        return result_rows


def _eval_predicate(row: Dict, pred: Dict) -> bool:
    op = pred["op"]

    if op == "AND":
        return all(_eval_predicate(row, c) for c in pred["children"])
    if op == "OR":
        return any(_eval_predicate(row, c) for c in pred["children"])
    if op == "NOT":
        return not _eval_predicate(row, pred["child"])

    col = pred["col"]
    val = pred.get("val")
    row_val = row.get(col)

    if op == "IS NULL":
        return row_val is None
    if op == "IS NOT NULL":
        return row_val is not None

    if row_val is None:
        return False

    if op == "=":
        return row_val == val
    if op == "!=":
        return row_val != val
    if op == "<":
        return row_val < val
    if op == "<=":
        return row_val <= val
    if op == ">":
        return row_val > val
    if op == ">=":
        return row_val >= val

    raise ValueError(f"Unknown operator: {op}")
