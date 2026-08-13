import json
import os
from functools import lru_cache
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from verifier_helpers import (
    apply_predicate,
    canonicalize_row,
    columns_in_predicate,
    query_receipt,
    receipt_for_table,
)

TESTS_ROOT = os.environ.get("TESTS_ROOT", "/tests")
DATA_DIR = os.path.join(TESTS_ROOT, "data")
QUERIES_PATH = os.path.join(DATA_DIR, "queries.json")

APP_ROOT = os.environ.get("APP_ROOT", "/app")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")
INDEX_PATH = os.path.join(APP_ROOT, "row_group_index.pkl")
INDEX_SIZE_CAP_BYTES = 2 * 1024 * 1024


def _artifact_size_bytes(path: str) -> int:
    if os.path.isfile(path):
        return os.path.getsize(path)
    if os.path.isdir(path):
        total = 0
        for root, _, files in os.walk(path):
            for name in files:
                total += os.path.getsize(os.path.join(root, name))
        return total
    pytest.fail(f"missing agent artifact: {path}", pytrace=False)


def _read_query_specs() -> list[dict[str, Any]]:
    with open(QUERIES_PATH, encoding="utf-8") as f:
        return json.load(f)


def _query_params() -> list[Any]:
    if not os.path.exists(QUERIES_PATH):
        pytest.fail(f"verifier query spec missing: {QUERIES_PATH} — verifier image was not built correctly", pytrace=False)
    return [pytest.param(q["id"], id=q["id"]) for q in _read_query_specs()]


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "query_id" in metafunc.fixturenames:
        metafunc.parametrize("query_id", _query_params())


@pytest.fixture(scope="session")
def queries() -> list[dict[str, Any]]:
    assert os.path.exists(QUERIES_PATH), f"missing verifier query spec: {QUERIES_PATH}"
    payload = _read_query_specs()
    assert isinstance(payload, list), "queries.json must be a JSON array"
    assert payload, "queries.json is empty"
    return payload


@pytest.fixture(scope="session")
def queries_by_id(queries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {q["id"]: q for q in queries}
    assert len(out) == len(queries), "duplicate query IDs in queries.json"
    return out


@pytest.fixture(scope="session")
def parquet_file(queries: list[dict[str, Any]]) -> pq.ParquetFile:
    path = os.path.join(DATA_DIR, queries[0]["file"])
    assert os.path.exists(path), f"missing verifier dataset: {path}"
    return pq.ParquetFile(path)


@pytest.fixture(scope="session")
def schema_types(parquet_file: pq.ParquetFile) -> dict[str, pa.DataType]:
    return {field.name: field.type for field in parquet_file.schema_arrow}


@lru_cache(maxsize=1)
def _results_payload() -> list[dict[str, Any]]:
    assert os.path.exists(RESULTS_PATH), f"missing agent artifact: {RESULTS_PATH}"
    with open(RESULTS_PATH, encoding="utf-8") as f:
        payload = json.load(f)
    assert isinstance(payload, list), "results.json must be a JSON array"
    return payload


@lru_cache(maxsize=1)
def _trace_payload() -> list[dict[str, Any]]:
    assert os.path.exists(TRACE_PATH), f"missing agent artifact: {TRACE_PATH}"
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
        assert set(item.keys()) == {"query_id", "rows"}, "results entry keys must be exactly query_id and rows"
        query_id = item["query_id"]
        rows = item["rows"]
        assert isinstance(query_id, str), "query_id must be a string"
        assert query_id in queries_by_id, f"unknown query_id in results: {query_id}"
        assert query_id not in out, f"duplicate query_id in results: {query_id}"
        assert isinstance(rows, list), f"rows for {query_id} must be a list"
        projection = set(queries_by_id[query_id]["columns"])
        for row in rows:
            assert isinstance(row, dict), f"rows for {query_id} must be row objects"
            assert set(row.keys()) == projection, f"row keys for {query_id} must be exactly {sorted(projection)}"
        out[query_id] = rows

    assert set(out.keys()) == set(queries_by_id.keys()), "results must include every query exactly once"
    return out


@pytest.fixture(scope="session")
def trace_by_id(
    queries: list[dict[str, Any]],
    queries_by_id: dict[str, dict[str, Any]],
    results_by_id: dict[str, list[dict[str, Any]]],
) -> dict[str, dict[str, Any]]:
    payload = _trace_payload()
    assert len(payload) == len(queries), "trace.jsonl must contain exactly one line per query"

    out: dict[str, dict[str, Any]] = {}

    for item, query in zip(payload, queries):
        assert isinstance(item, dict), "each trace record must be an object"
        assert set(item.keys()) == {"query_id", "read_row_groups", "query_receipt", "result_count"}, (
            "trace record keys must be exactly query_id, read_row_groups, query_receipt, result_count"
        )

        query_id = item["query_id"]
        assert isinstance(query_id, str), "trace query_id must be a string"
        assert query_id in queries_by_id, f"unknown query_id in trace: {query_id}"
        assert query_id == query["id"], "trace records must follow queries.json order"
        assert query_id not in out, f"duplicate query_id in trace: {query_id}"

        read_row_groups = item["read_row_groups"]
        assert isinstance(read_row_groups, list), f"read_row_groups for {query_id} must be a list"

        last_rg = -1
        for entry in read_row_groups:
            assert isinstance(entry, dict), f"trace read row group entry for {query_id} must be an object"
            assert set(entry.keys()) == {"row_group", "decoded_rows", "decoded_bytes", "receipt"}, (
                f"read_row_groups entry keys for {query_id} must be exactly row_group, decoded_rows, decoded_bytes, receipt"
            )
            rg = entry["row_group"]
            decoded_rows = entry["decoded_rows"]
            decoded_bytes = entry["decoded_bytes"]
            receipt = entry["receipt"]
            assert isinstance(rg, int) and not isinstance(rg, bool) and rg >= 0, (
                f"row_group must be a non-negative integer for {query_id}"
            )
            assert rg > last_rg, f"row_group entries must be strictly increasing for {query_id}"
            assert isinstance(decoded_rows, int) and not isinstance(decoded_rows, bool) and decoded_rows >= 0, (
                f"decoded_rows must be a non-negative integer for {query_id}"
            )
            assert isinstance(decoded_bytes, int) and not isinstance(decoded_bytes, bool) and decoded_bytes >= 0, (
                f"decoded_bytes must be a non-negative integer for {query_id}"
            )
            assert isinstance(receipt, str) and len(receipt) == 32, f"invalid receipt format for {query_id} rg {rg}"
            last_rg = rg

        assert isinstance(item["query_receipt"], str) and len(item["query_receipt"]) == 32, (
            f"invalid query_receipt for {query_id}"
        )
        assert isinstance(item["result_count"], int) and not isinstance(item["result_count"], bool), (
            f"result_count must be an integer for {query_id}"
        )
        assert item["result_count"] == len(results_by_id[query_id]), f"result_count mismatch for {query_id}"

        out[query_id] = item

    assert set(out.keys()) == set(queries_by_id.keys()), "trace must include every query exactly once"
    return out


@pytest.fixture(scope="session")
def reference_rows_by_query(
    queries: list[dict[str, Any]], parquet_file: pq.ParquetFile
) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}

    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]

        required = set(projection)
        columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        rows: list[dict[str, Any]] = []
        for rg_idx in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(rg_idx, columns=read_columns)
            filtered = apply_predicate(decoded, predicate)
            projected = filtered.select(projection)
            rows.extend(projected.to_pylist())

        out[query["id"]] = rows

    return out


@pytest.fixture(scope="session")
def reference_matching_row_groups_by_query(
    queries: list[dict[str, Any]], parquet_file: pq.ParquetFile
) -> dict[str, set[int]]:
    out: dict[str, set[int]] = {}
    for query in queries:
        predicate = query.get("predicate")
        projection = query["columns"]
        required = set(projection)
        columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        groups: set[int] = set()
        for rg_idx in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(rg_idx, columns=read_columns)
            filtered = apply_predicate(decoded, predicate)
            if filtered.num_rows > 0:
                groups.add(rg_idx)
        out[query["id"]] = groups
    return out


def test_results_and_trace_files_exist() -> None:
    assert os.path.exists(RESULTS_PATH), f"missing agent artifact: {RESULTS_PATH}"
    assert os.path.exists(TRACE_PATH), f"missing agent artifact: {TRACE_PATH}"


def test_index_size_cap() -> None:
    assert os.path.exists(INDEX_PATH), f"missing agent artifact: {INDEX_PATH}"
    size_bytes = _artifact_size_bytes(INDEX_PATH)
    assert size_bytes <= INDEX_SIZE_CAP_BYTES, (
        f"persisted index artifact is too large: {size_bytes} bytes exceeds {INDEX_SIZE_CAP_BYTES}"
    )


def test_query_results_match_reference(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    results_by_id: dict[str, list[dict[str, Any]]],
    reference_rows_by_query: dict[str, list[dict[str, Any]]],
    schema_types: dict[str, pa.DataType],
) -> None:
    assert query_id in queries_by_id, f"unknown query id: {query_id}"
    actual = [canonicalize_row(row, schema_types) for row in results_by_id[query_id]]
    expected = [canonicalize_row(row, schema_types) for row in reference_rows_by_query[query_id]]
    min_result_count = int(queries_by_id[query_id]["min_result_count"])
    assert len(actual) == len(expected), f"row count mismatch for {query_id}"
    assert len(actual) >= min_result_count, f"{query_id} must return at least {min_result_count} rows"
    assert actual == expected, f"row content mismatch for {query_id}"


def test_query_pruning_targets(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    trace_by_id: dict[str, dict[str, Any]],
) -> None:
    assert query_id in queries_by_id, f"unknown query id: {query_id}"
    record = trace_by_id[query_id]
    reads = record["read_row_groups"]
    max_reads = int(queries_by_id[query_id]["max_row_groups_read"])
    assert len(reads) <= max_reads, f"{query_id} read {len(reads)} row groups but max is {max_reads}"


def test_trace_receipts_match_decoded_bytes(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    trace_by_id: dict[str, dict[str, Any]],
    parquet_file: pq.ParquetFile,
) -> None:
    assert query_id in queries_by_id, f"unknown query id: {query_id}"
    query = queries_by_id[query_id]
    predicate = query.get("predicate")
    projection = query["columns"]

    required = set(projection)
    columns_in_predicate(predicate, required)
    read_columns = sorted(required)

    record = trace_by_id[query_id]
    reads = record["read_row_groups"]

    total_decoded_bytes = 0
    for entry in reads:
        rg = entry["row_group"]
        assert rg < parquet_file.metadata.num_row_groups, f"row_group {rg} out of range for {query_id}"
        decoded = parquet_file.read_row_group(rg, columns=read_columns)
        assert entry["decoded_rows"] == decoded.num_rows, f"decoded_rows mismatch for {query_id} rg {rg}"
        assert entry["decoded_bytes"] == decoded.nbytes, f"decoded_bytes mismatch for {query_id} rg {rg}"
        assert entry["receipt"] == receipt_for_table(decoded), f"receipt mismatch for {query_id} rg {rg}"
        total_decoded_bytes += decoded.nbytes

    max_decoded_bytes = int(query["max_decoded_bytes"])
    assert total_decoded_bytes <= max_decoded_bytes, (
        f"{query_id} decoded {total_decoded_bytes} bytes in query phase but max is {max_decoded_bytes}"
    )

    expected_query_receipt = query_receipt(query_id, reads)
    assert record["query_receipt"] == expected_query_receipt, f"query_receipt mismatch for {query_id}"


def test_query_read_set_covers_all_matching_groups(
    query_id: str,
    trace_by_id: dict[str, dict[str, Any]],
    reference_matching_row_groups_by_query: dict[str, set[int]],
) -> None:
    required_matching = reference_matching_row_groups_by_query[query_id]
    agent_set = {e["row_group"] for e in trace_by_id[query_id]["read_row_groups"]}
    missing = required_matching - agent_set
    assert not missing, (
        f"{query_id}: missing row groups with matching rows: {sorted(missing)}. "
        f"Reported set was {sorted(agent_set)}"
    )
