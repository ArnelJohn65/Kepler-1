import hashlib
import json
import math
import os
from functools import lru_cache
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")


def _query_ids() -> list[str]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        payload = json.load(f)
    return [q["id"] for q in payload]


@pytest.fixture(scope="session")
def queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        payload = json.load(f)
    assert isinstance(payload, list)
    assert payload, "queries.json is empty"
    return payload


@pytest.fixture(scope="session")
def queries_by_id(queries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {q["id"]: q for q in queries}
    assert len(out) == len(queries), "duplicate query IDs in queries.json"
    return out


@pytest.fixture(scope="session")
def parquet_file(queries: list[dict[str, Any]]) -> pq.ParquetFile:
    file_name = queries[0]["file"]
    return pq.ParquetFile(os.path.join(DATA_DIR, file_name))


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if node is None:
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
    raise AssertionError(f"unknown predicate node type in query spec: {node_type}")


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
        raise AssertionError(f"unsupported cmp op in query spec: {op}")

    if node_type == "in":
        col = table.column(node["column"])
        return pc.is_in(col, value_set=pa.array(node["values"], type=col.type))

    if node_type == "is_null":
        return pc.is_null(table.column(node["column"]))

    if node_type == "is_not_null":
        return pc.is_valid(table.column(node["column"]))

    if node_type == "and":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.and_(out, mask)
        return out

    if node_type == "or":
        masks = [_build_mask(table, child) for child in node["children"]]
        out = masks[0]
        for mask in masks[1:]:
            out = pc.or_(out, mask)
        return out

    if node_type == "not":
        return pc.invert(_build_mask(table, node["child"]))

    raise AssertionError(f"unsupported node type in query spec: {node_type}")


def _apply_predicate(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_build_mask(table, predicate))


def _receipt_for_table(table: pa.Table) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(f"rows={table.num_rows}".encode("utf-8"))
    for row in table.to_pylist():
        payload = json.dumps({k: _normalize(v) for k, v in row.items()}, sort_keys=True, separators=(",", ":"))
        h.update(payload.encode("utf-8"))
        h.update(b"\n")
    return h.hexdigest()


def _query_receipt(query_id: str, read_row_groups: list[dict[str, Any]]) -> str:
    h = hashlib.blake2b(digest_size=16)
    h.update(query_id.encode("utf-8"))
    h.update(b"|")
    for entry in read_row_groups:
        h.update(f"{entry['row_group']}:{entry['decoded_rows']}:{entry['receipt']}".encode("utf-8"))
        h.update(b"|")
    return h.hexdigest()


@lru_cache(maxsize=1)
def _results_payload() -> list[dict[str, Any]]:
    assert os.path.exists(RESULTS_PATH), f"missing {RESULTS_PATH}"
    with open(RESULTS_PATH, encoding="utf-8") as f:
        payload = json.load(f)
    assert isinstance(payload, list), "results.json must be a JSON array"
    return payload


@lru_cache(maxsize=1)
def _trace_payload() -> list[dict[str, Any]]:
    assert os.path.exists(TRACE_PATH), f"missing {TRACE_PATH}"
    records: list[dict[str, Any]] = []
    with open(TRACE_PATH, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


@pytest.fixture(scope="session")
def results_by_id(queries_by_id: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    payload = _results_payload()
    out: dict[str, list[dict[str, Any]]] = {}

    for item in payload:
        assert isinstance(item, dict), "each results entry must be an object"
        assert set(item.keys()) == {"query_id", "rows"}, "results entry keys must be query_id and rows"
        query_id = item["query_id"]
        rows = item["rows"]
        assert isinstance(query_id, str), "query_id must be a string"
        assert query_id in queries_by_id, f"unknown query_id in results: {query_id}"
        assert isinstance(rows, list), f"rows for {query_id} must be a list"
        for row in rows:
            assert isinstance(row, dict), f"rows for {query_id} must be row objects"
        out[query_id] = rows

    assert set(out.keys()) == set(queries_by_id.keys()), "results must include every query exactly once"
    return out


@pytest.fixture(scope="session")
def trace_by_id(queries_by_id: dict[str, dict[str, Any]], results_by_id: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, Any]]:
    payload = _trace_payload()
    out: dict[str, dict[str, Any]] = {}

    for item in payload:
        assert isinstance(item, dict), "each trace record must be an object"
        assert set(item.keys()) == {"query_id", "read_row_groups", "query_receipt", "result_count"}, (
            "trace record keys must be query_id, read_row_groups, query_receipt, result_count"
        )

        query_id = item["query_id"]
        assert isinstance(query_id, str), "trace query_id must be a string"
        assert query_id in queries_by_id, f"unknown query_id in trace: {query_id}"

        read_row_groups = item["read_row_groups"]
        assert isinstance(read_row_groups, list), f"read_row_groups for {query_id} must be a list"

        seen = set()
        last_rg = -1
        for entry in read_row_groups:
            assert isinstance(entry, dict), f"trace read row group entry for {query_id} must be an object"
            assert set(entry.keys()) == {"row_group", "decoded_rows", "receipt"}
            rg = entry["row_group"]
            decoded_rows = entry["decoded_rows"]
            receipt = entry["receipt"]
            assert isinstance(rg, int) and rg >= 0, f"row_group must be non-negative integer for {query_id}"
            assert rg not in seen, f"duplicate row_group {rg} in trace for {query_id}"
            assert rg > last_rg, f"row_group entries must be strictly increasing for {query_id}"
            assert isinstance(decoded_rows, int) and decoded_rows >= 0, f"decoded_rows must be integer for {query_id}"
            assert isinstance(receipt, str) and len(receipt) == 32, f"invalid receipt format for {query_id} rg {rg}"
            seen.add(rg)
            last_rg = rg

        assert isinstance(item["query_receipt"], str) and len(item["query_receipt"]) == 32, (
            f"invalid query_receipt for {query_id}"
        )
        assert isinstance(item["result_count"], int), f"result_count must be integer for {query_id}"
        assert item["result_count"] == len(results_by_id[query_id]), f"result_count mismatch for {query_id}"

        out[query_id] = item

    assert set(out.keys()) == set(queries_by_id.keys()), "trace must include every query exactly once"
    return out


@pytest.fixture(scope="session")
def reference_rows_by_query(queries: list[dict[str, Any]], parquet_file: pq.ParquetFile) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}

    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]

        required = set(projection)
        _columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        rows: list[dict[str, Any]] = []
        for rg_idx in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(rg_idx, columns=read_columns)
            filtered = _apply_predicate(decoded, predicate)
            projected = filtered.select(projection)
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in projected.to_pylist())

        out[query["id"]] = rows

    return out


def test_results_and_trace_files_exist() -> None:
    assert os.path.exists(RESULTS_PATH), f"missing {RESULTS_PATH}"
    assert os.path.exists(TRACE_PATH), f"missing {TRACE_PATH}"


@pytest.mark.parametrize("query_id", _query_ids())
def test_query_results_match_reference(
    query_id: str,
    results_by_id: dict[str, list[dict[str, Any]]],
    reference_rows_by_query: dict[str, list[dict[str, Any]]],
) -> None:
    actual = results_by_id[query_id]
    expected = reference_rows_by_query[query_id]
    assert len(actual) == len(expected), f"row count mismatch for {query_id}"
    assert len(actual) > 0, f"{query_id} result rows must be non-empty"
    assert actual == expected, f"row content mismatch for {query_id}"


@pytest.mark.parametrize("query_id", _query_ids())
def test_query_pruning_targets(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    trace_by_id: dict[str, dict[str, Any]],
    parquet_file: pq.ParquetFile,
) -> None:
    record = trace_by_id[query_id]
    reads = record["read_row_groups"]
    max_reads = queries_by_id[query_id]["max_row_groups_read"]
    assert len(reads) <= max_reads, f"{query_id} read {len(reads)} row groups but max is {max_reads}"
    assert len(reads) < parquet_file.metadata.num_row_groups, f"{query_id} must prune at least one row group"


@pytest.mark.parametrize("query_id", _query_ids())
def test_trace_receipts_match_decoded_bytes(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    trace_by_id: dict[str, dict[str, Any]],
    parquet_file: pq.ParquetFile,
) -> None:
    query = queries_by_id[query_id]
    predicate = query.get("predicate")
    projection = query["columns"]

    required = set(projection)
    _columns_in_predicate(predicate, required)
    read_columns = sorted(required)

    record = trace_by_id[query_id]
    reads = record["read_row_groups"]

    for entry in reads:
        rg = entry["row_group"]
        assert rg < parquet_file.metadata.num_row_groups, f"row_group {rg} out of range for {query_id}"
        decoded = parquet_file.read_row_group(rg, columns=read_columns)
        expected_receipt = _receipt_for_table(decoded)
        assert entry["decoded_rows"] == decoded.num_rows, f"decoded_rows mismatch for {query_id} rg {rg}"
        assert entry["receipt"] == expected_receipt, f"receipt mismatch for {query_id} rg {rg}"

    expected_query_receipt = _query_receipt(query_id, reads)
    assert record["query_receipt"] == expected_query_receipt, f"query_receipt mismatch for {query_id}"
