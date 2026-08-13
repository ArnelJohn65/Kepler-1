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
    may_index_row_group_match,
    validate_index_payload,
)

TESTS_ROOT = os.environ.get("TESTS_ROOT", "/tests")
DATA_DIR = os.path.join(TESTS_ROOT, "data")
VISIBLE_QUERIES_PATH = os.path.join(DATA_DIR, "queries.json")
HIDDEN_QUERIES_PATH = os.path.join(DATA_DIR, "hidden_queries.json")

APP_ROOT = os.environ.get("APP_ROOT", "/app")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
INDEX_PATH = os.path.join(APP_ROOT, "row_group_index.json")
INDEX_SIZE_CAP_BYTES = 2 * 1024 * 1024


def _read_json(path: str) -> Any:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _query_params(path: str, prefix: str) -> list[Any]:
    if not os.path.exists(path):
        pytest.fail(f"verifier query spec missing: {path} — verifier image was not built correctly", pytrace=False)
    return [pytest.param(query["id"], id=f"{prefix}:{query['id']}") for query in _read_json(path)]


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "visible_query_id" in metafunc.fixturenames:
        metafunc.parametrize("visible_query_id", _query_params(VISIBLE_QUERIES_PATH, "visible"))
    if "hidden_query_id" in metafunc.fixturenames:
        metafunc.parametrize("hidden_query_id", _query_params(HIDDEN_QUERIES_PATH, "hidden"))


@pytest.fixture(scope="session")
def visible_queries() -> list[dict[str, Any]]:
    payload = _read_json(VISIBLE_QUERIES_PATH)
    assert isinstance(payload, list) and payload, "visible queries must be a non-empty JSON array"
    return payload


@pytest.fixture(scope="session")
def hidden_queries() -> list[dict[str, Any]]:
    payload = _read_json(HIDDEN_QUERIES_PATH)
    assert isinstance(payload, list) and payload, "hidden queries must be a non-empty JSON array"
    return payload


@pytest.fixture(scope="session")
def visible_queries_by_id(visible_queries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {query["id"]: query for query in visible_queries}
    assert len(out) == len(visible_queries), "visible queries contain duplicate ids"
    return out


@pytest.fixture(scope="session")
def hidden_queries_by_id(hidden_queries: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    out = {query["id"]: query for query in hidden_queries}
    assert len(out) == len(hidden_queries), "hidden queries contain duplicate ids"
    return out


@pytest.fixture(scope="session")
def parquet_file() -> pq.ParquetFile:
    path = os.path.join(DATA_DIR, "sales.parquet")
    assert os.path.exists(path), f"missing verifier dataset: {path}"
    return pq.ParquetFile(path)


@pytest.fixture(scope="session")
def schema_types(parquet_file: pq.ParquetFile) -> dict[str, pa.DataType]:
    return {field.name: field.type for field in parquet_file.schema_arrow}


@lru_cache(maxsize=1)
def _results_payload() -> list[dict[str, Any]]:
    assert os.path.exists(RESULTS_PATH), f"missing agent artifact: {RESULTS_PATH}"
    payload = _read_json(RESULTS_PATH)
    assert isinstance(payload, list), "results.json must be a JSON array"
    return payload


@lru_cache(maxsize=1)
def _index_payload() -> dict[str, Any]:
    assert os.path.exists(INDEX_PATH), f"missing agent artifact: {INDEX_PATH}"
    return _read_json(INDEX_PATH)


@pytest.fixture(scope="session")
def results_by_id(visible_queries_by_id: dict[str, dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    for item in _results_payload():
        assert isinstance(item, dict), "each results entry must be an object"
        assert set(item.keys()) == {"query_id", "rows"}, "results entry keys must be exactly query_id and rows"
        query_id = item["query_id"]
        assert isinstance(query_id, str), "query_id must be a string"
        assert query_id in visible_queries_by_id, f"unknown query_id in results: {query_id}"
        assert query_id not in out, f"duplicate query_id in results: {query_id}"
        rows = item["rows"]
        assert isinstance(rows, list), f"rows for {query_id} must be a list"
        projection = set(visible_queries_by_id[query_id]["columns"])
        for row in rows:
            assert isinstance(row, dict), f"rows for {query_id} must be row objects"
            assert set(row.keys()) == projection, f"row keys for {query_id} must be exactly {sorted(projection)}"
        out[query_id] = rows
    assert set(out) == set(visible_queries_by_id), "results.json must include every visible query exactly once"
    return out


@pytest.fixture(scope="session")
def index_payload(parquet_file: pq.ParquetFile, schema_types: dict[str, pa.DataType]) -> dict[str, Any]:
    payload = _index_payload()
    validate_index_payload(payload, schema_types, parquet_file.metadata.num_row_groups)
    return payload


def test_generated_specs_exist() -> None:
    assert os.path.exists(VISIBLE_QUERIES_PATH), f"missing verifier query spec: {VISIBLE_QUERIES_PATH}"
    assert os.path.exists(HIDDEN_QUERIES_PATH), f"missing verifier hidden query spec: {HIDDEN_QUERIES_PATH}"


def test_agent_artifacts_exist() -> None:
    assert os.path.exists(RESULTS_PATH), f"missing agent artifact: {RESULTS_PATH}"
    assert os.path.exists(INDEX_PATH), f"missing agent artifact: {INDEX_PATH}"


def test_index_size_cap(index_payload: dict[str, Any]) -> None:
    del index_payload
    size_bytes = os.path.getsize(INDEX_PATH)
    assert size_bytes <= INDEX_SIZE_CAP_BYTES, (
        f"row_group_index.json is too large: {size_bytes} bytes exceeds {INDEX_SIZE_CAP_BYTES}"
    )


def test_visible_query_results_match_reference(
    visible_query_id: str,
    visible_queries_by_id: dict[str, dict[str, Any]],
    results_by_id: dict[str, list[dict[str, Any]]],
    parquet_file: pq.ParquetFile,
    schema_types: dict[str, pa.DataType],
) -> None:
    query = visible_queries_by_id[visible_query_id]
    predicate = query.get("predicate")
    read_columns = set(query["columns"])
    columns_in_predicate(predicate, read_columns)

    expected_rows: list[dict[str, Any]] = []
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        decoded = parquet_file.read_row_group(row_group_index, columns=sorted(read_columns))
        expected_rows.extend(apply_predicate(decoded, predicate).select(query["columns"]).to_pylist())

    actual = [canonicalize_row(row, schema_types) for row in results_by_id[visible_query_id]]
    expected = [canonicalize_row(row, schema_types) for row in expected_rows]
    assert actual == expected, f"visible query {visible_query_id} returned incorrect rows"


def test_hidden_query_index_is_sound_and_precise(
    hidden_query_id: str,
    hidden_queries_by_id: dict[str, dict[str, Any]],
    index_payload: dict[str, Any],
    parquet_file: pq.ParquetFile,
    schema_types: dict[str, pa.DataType],
) -> None:
    query = hidden_queries_by_id[hidden_query_id]
    predicate = query.get("predicate")
    surviving_row_groups = {
        entry["row_group"]
        for entry in index_payload["row_groups"]
        if may_index_row_group_match(entry, schema_types, predicate)
    }

    read_columns = set(query["columns"])
    columns_in_predicate(predicate, read_columns)
    matching_row_groups: set[int] = set()
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        decoded = parquet_file.read_row_group(row_group_index, columns=sorted(read_columns))
        if apply_predicate(decoded, predicate).num_rows > 0:
            matching_row_groups.add(row_group_index)

    missing = matching_row_groups - surviving_row_groups
    assert not missing, f"{hidden_query_id} unsoundly excludes matching row groups: {sorted(missing)}"

    max_survivors = int(query["max_surviving_row_groups"])
    assert len(surviving_row_groups) <= max_survivors, (
        f"{hidden_query_id} leaves {len(surviving_row_groups)} row groups alive but the ceiling is {max_survivors}"
    )
