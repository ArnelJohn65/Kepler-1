import hashlib
import json
import math
import os
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")
INDEX_PATH = os.path.join(DATA_DIR, "row_group_index.json")


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def _load_index() -> dict[int, dict[str, Any]]:
    with open(INDEX_PATH, encoding="utf-8") as f:
        entries = json.load(f)
    return {int(entry["row_group"]): entry for entry in entries}


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if not node:
        return
    node_type = node["type"]
    if node_type in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if node_type in {"and", "or"}:
        for child in node["children"]:
            _columns_in_predicate(child, output)
        return
    if node_type == "not":
        _columns_in_predicate(node["child"], output)
        return
    raise ValueError(f"Unsupported predicate node type: {node_type}")


def _build_mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    node_type = node["type"]

    if node_type == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        value = node["value"]
        if op == "eq":
            return pc.equal(col, value)
        if op == "ne":
            return pc.not_equal(col, value)
        if op == "lt":
            return pc.less(col, value)
        if op == "le":
            return pc.less_equal(col, value)
        if op == "gt":
            return pc.greater(col, value)
        if op == "ge":
            return pc.greater_equal(col, value)
        raise ValueError(f"Unsupported cmp op: {op}")

    if node_type == "in":
        col = table.column(node["column"])
        values = pa.array(node["values"], type=col.type)
        return pc.is_in(col, value_set=values)

    if node_type == "is_null":
        return pc.is_null(table.column(node["column"]))

    if node_type == "is_not_null":
        return pc.is_valid(table.column(node["column"]))

    if node_type == "and":
        masks = [_build_mask(table, child) for child in node["children"]]
        mask = masks[0]
        for other in masks[1:]:
            mask = pc.and_(mask, other)
        return mask

    if node_type == "or":
        masks = [_build_mask(table, child) for child in node["children"]]
        mask = masks[0]
        for other in masks[1:]:
            mask = pc.or_(mask, other)
        return mask

    if node_type == "not":
        return pc.invert(_build_mask(table, node["child"]))

    raise ValueError(f"Unsupported predicate node type: {node_type}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    mask = _build_mask(table, predicate)
    return table.filter(mask)


def _extract_stats(rg_meta: pq.RowGroupMetaData, column_name: str) -> dict[str, Any]:
    for col_idx in range(rg_meta.num_columns):
        cmeta = rg_meta.column(col_idx)
        if cmeta.path_in_schema != column_name:
            continue
        stats = cmeta.statistics
        if stats is None:
            return {"min": None, "max": None, "null_count": None, "num_rows": rg_meta.num_rows}
        return {
            "min": stats.min if stats.has_min_max else None,
            "max": stats.max if stats.has_min_max else None,
            "null_count": stats.null_count,
            "num_rows": rg_meta.num_rows,
        }
    return {"min": None, "max": None, "null_count": None, "num_rows": rg_meta.num_rows}


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v


def _receipt_for_table(table: pa.Table) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(f"rows={table.num_rows}".encode("utf-8"))
    for row in table.to_pylist():
        payload = json.dumps({k: _normalize(v) for k, v in row.items()}, sort_keys=True, separators=(",", ":"))
        h.update(payload.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _cmp_may_match(stats: dict[str, Any], op: str, value: Any) -> bool:
    rg_min = stats["min"]
    rg_max = stats["max"]
    if rg_min is None or rg_max is None:
        return True
    if op == "eq":
        return not (value < rg_min or value > rg_max)
    if op == "ne":
        return True
    if op == "lt":
        return rg_min < value
    if op == "le":
        return rg_min <= value
    if op == "gt":
        return rg_max > value
    if op == "ge":
        return rg_max >= value
    return True


def _may_match(node: dict[str, Any], rg_meta: pq.RowGroupMetaData, index_entry: dict[str, Any]) -> bool:
    node_type = node["type"]

    if node_type == "cmp":
        stats = _extract_stats(rg_meta, node["column"])
        return _cmp_may_match(stats, node["op"], node["value"])

    if node_type == "in":
        return True

    if node_type == "is_null":
        stats = _extract_stats(rg_meta, node["column"])
        null_count = stats.get("null_count")
        if null_count == 0:
            return False
        return True

    if node_type == "is_not_null":
        stats = _extract_stats(rg_meta, node["column"])
        null_count = stats.get("null_count")
        num_rows = stats.get("num_rows")
        if null_count is not None and num_rows is not None and null_count >= num_rows:
            return False
        return True

    if node_type == "and":
        for child in node["children"]:
            if not _may_match(child, rg_meta, index_entry):
                return False
        return True

    if node_type in {"or", "not"}:
        return True

    return True


def _query_receipt(query_id: str, read_row_groups: list[dict[str, Any]]) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(query_id.encode("utf-8"))
    h.update(b"|")
    for entry in read_row_groups:
        h.update(f"{entry['row_group']}:{entry['decoded_rows']}:{entry['receipt']}".encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


def run_query(pf: pq.ParquetFile, query: dict[str, Any], index_by_rg: dict[int, dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    predicate = query.get("predicate")
    projection = query.get("columns", [])

    required_columns = set(projection)
    _columns_in_predicate(predicate, required_columns)
    read_columns = sorted(required_columns)

    results: list[dict[str, Any]] = []
    read_trace: list[dict[str, Any]] = []

    for rg_idx in range(pf.metadata.num_row_groups):
        rg_meta = pf.metadata.row_group(rg_idx)
        index_entry = index_by_rg.get(rg_idx, {})

        if predicate is not None and not _may_match(predicate, rg_meta, index_entry):
            continue

        decoded = pf.read_row_group(rg_idx, columns=read_columns)
        receipt = _receipt_for_table(decoded)

        filtered = _apply_predicate(decoded, predicate)
        if projection:
            filtered = filtered.select(projection)

        read_trace.append(
            {
                "row_group": rg_idx,
                "decoded_rows": decoded.num_rows,
                "receipt": receipt,
            }
        )

        for row in filtered.to_pylist():
            results.append({k: _normalize(v) for k, v in row.items()})

    trace_record = {
        "query_id": query["id"],
        "read_row_groups": read_trace,
        "query_receipt": _query_receipt(query["id"], read_trace),
        "result_count": len(results),
    }
    return results, trace_record


def main() -> None:
    queries = _load_queries()
    index_by_rg = _load_index()

    if not queries:
        raise RuntimeError("No queries found")

    parquet_file = os.path.join(DATA_DIR, queries[0]["file"])
    pf = pq.ParquetFile(parquet_file)

    all_results: list[dict[str, Any]] = []
    trace_records: list[dict[str, Any]] = []

    for query in queries:
        rows, trace = run_query(pf, query, index_by_rg)
        all_results.append({"query_id": query["id"], "rows": rows})
        trace_records.append(trace)

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for record in trace_records:
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {RESULTS_PATH} and {TRACE_PATH}")


if __name__ == "__main__":
    main()
