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

TESTS_ROOT = os.environ.get("TESTS_ROOT", "/tests")
DATA_DIR = os.path.join(TESTS_ROOT, "data")
QUERIES_PATH = os.path.join(DATA_DIR, "queries.json")

APP_ROOT = os.environ.get("APP_ROOT", "/app")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")

MIN_EXPECTED_QUERIES = 9


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


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    return v


def _normalize_row(row: dict[str, Any]) -> dict[str, Any]:
    return {k: _normalize(v) for k, v in row.items()}


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


def _ref_extract_stats(rg_meta: pq.RowGroupMetaData) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for ci in range(rg_meta.num_columns):
        cmeta = rg_meta.column(ci)
        s = cmeta.statistics
        if s is not None:
            out[cmeta.path_in_schema] = {
                "null_count": s.null_count,
                "num_rows": rg_meta.num_rows,
                "min": s.min if s.has_min_max else None,
                "max": s.max if s.has_min_max else None,
            }
        else:
            out[cmeta.path_in_schema] = {
                "null_count": None,
                "num_rows": rg_meta.num_rows,
                "min": None,
                "max": None,
            }
    return out


def _ref_build_entry(table: pa.Table, rg_meta: pq.RowGroupMetaData) -> dict[str, Any]:
    values: dict[str, set] = {}
    has_null: dict[str, bool] = {}
    for col_name in table.column_names:
        col = table.column(col_name)
        has_null[col_name] = col.null_count > 0
        if pa.types.is_string(col.type) or pa.types.is_integer(col.type):
            values[col_name] = set(col.drop_null().to_pylist())
    return {"values": values, "has_null": has_null, "stats": _ref_extract_stats(rg_meta)}


def _ref_cmp_may(entry: dict, column: str, op: str, value: Any) -> bool:
    st = entry["stats"].get(column, {})
    nc, nr = st.get("null_count"), st.get("num_rows", 0)
    if nc is not None and nc >= nr > 0:
        return False
    cv = entry["values"].get(column)
    rg_min, rg_max = st.get("min"), st.get("max")
    if op == "eq":
        if cv is not None and value not in cv:
            return False
        if rg_min is not None and rg_max is not None:
            return not (value < rg_min or value > rg_max)
        return True
    if op == "ne":
        if cv is not None and len(cv) == 1 and value in cv and nc == 0:
            return False
        if rg_min is not None and rg_max is not None and rg_min == rg_max == value and nc == 0:
            return False
        return True
    if rg_min is None or rg_max is None:
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


def _ref_in_may(entry: dict, column: str, values: list) -> bool:
    st = entry["stats"].get(column, {})
    nc, nr = st.get("null_count"), st.get("num_rows", 0)
    if nc is not None and nc >= nr > 0:
        return False
    allowed = set(values)
    cv = entry["values"].get(column)
    if cv is not None and cv.isdisjoint(allowed):
        return False
    rg_min, rg_max = st.get("min"), st.get("max")
    if rg_min is not None and rg_max is not None and all(v < rg_min or v > rg_max for v in allowed):
        return False
    return True


def _ref_leaf_always_true(entry: dict, node: dict) -> bool:
    col = node["column"]
    st = entry["stats"].get(col, {})
    nc, nr = st.get("null_count"), st.get("num_rows", 0)
    t = node["type"]
    if t == "is_null":
        return nc is not None and nc == nr
    if t == "is_not_null":
        return nc == 0
    if t == "in":
        if nc != 0:
            return False
        cv = entry["values"].get(col)
        return cv is not None and cv.issubset(set(node["values"])) and len(cv) > 0
    if t == "cmp":
        if nc != 0:
            return False
        rg_min, rg_max = st.get("min"), st.get("max")
        if rg_min is None or rg_max is None:
            return False
        op, value = node["op"], node["value"]
        cv = entry["values"].get(col)
        if op == "eq":
            return (cv == {value}) if cv is not None else (rg_min == rg_max == value)
        if op == "ne":
            return (value not in cv) if cv is not None else (value < rg_min or value > rg_max)
        if op == "lt":
            return rg_max < value
        if op == "le":
            return rg_max <= value
        if op == "gt":
            return rg_min > value
        if op == "ge":
            return rg_min >= value
    return False


def _ref_may_be_false(entry: dict, node: dict) -> bool:
    t = node["type"]
    if t == "and":
        return any(_ref_may_be_false(entry, c) for c in node["children"])
    if t == "or":
        return all(_ref_may_be_false(entry, c) for c in node["children"])
    if t == "not":
        return _ref_may_be_true(entry, node["child"])
    return not _ref_leaf_always_true(entry, node)


def _ref_may_be_true(entry: dict, node: dict) -> bool:
    t = node["type"]
    if t == "and":
        return all(_ref_may_be_true(entry, c) for c in node["children"])
    if t == "or":
        return any(_ref_may_be_true(entry, c) for c in node["children"])
    if t == "not":
        return _ref_may_be_false(entry, node["child"])
    if t == "cmp":
        return _ref_cmp_may(entry, node["column"], node["op"], node["value"])
    if t == "in":
        return _ref_in_may(entry, node["column"], node["values"])
    if t == "is_null":
        st = entry["stats"].get(node["column"], {})
        nc = st.get("null_count")
        if nc is not None:
            return nc > 0
        return entry["has_null"].get(node["column"], True)
    if t == "is_not_null":
        st = entry["stats"].get(node["column"], {})
        nc, nr = st.get("null_count"), st.get("num_rows", 0)
        if nc is not None:
            return nc < nr
        return True
    return True


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
        out[query_id] = [_normalize_row(row) for row in rows]

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
            assert set(entry.keys()) == {"row_group", "decoded_rows", "receipt"}, (
                f"read_row_groups entry keys for {query_id} must be exactly row_group, decoded_rows, receipt"
            )
            rg = entry["row_group"]
            decoded_rows = entry["decoded_rows"]
            receipt = entry["receipt"]
            assert isinstance(rg, int) and not isinstance(rg, bool) and rg >= 0, (
                f"row_group must be a non-negative integer for {query_id}"
            )
            assert rg > last_rg, f"row_group entries must be strictly increasing for {query_id}"
            assert isinstance(decoded_rows, int) and not isinstance(decoded_rows, bool) and decoded_rows >= 0, (
                f"decoded_rows must be a non-negative integer for {query_id}"
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
        _columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        rows: list[dict[str, Any]] = []
        for rg_idx in range(parquet_file.metadata.num_row_groups):
            decoded = parquet_file.read_row_group(rg_idx, columns=read_columns)
            filtered = _apply_predicate(decoded, predicate)
            projected = filtered.select(projection)
            rows.extend(_normalize_row(row) for row in projected.to_pylist())

        out[query["id"]] = rows

    return out


def test_results_and_trace_files_exist() -> None:
    assert os.path.exists(RESULTS_PATH), f"missing agent artifact: {RESULTS_PATH}"
    assert os.path.exists(TRACE_PATH), f"missing agent artifact: {TRACE_PATH}"


def test_expected_query_count(queries: list[dict[str, Any]]) -> None:
    assert len(queries) >= MIN_EXPECTED_QUERIES, (
        f"verifier query spec must define at least {MIN_EXPECTED_QUERIES} queries, found {len(queries)}"
    )


def test_query_results_match_reference(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    results_by_id: dict[str, list[dict[str, Any]]],
    reference_rows_by_query: dict[str, list[dict[str, Any]]],
) -> None:
    assert query_id in queries_by_id, f"unknown query id: {query_id}"
    actual = results_by_id[query_id]
    expected = reference_rows_by_query[query_id]
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
    _columns_in_predicate(predicate, required)
    read_columns = sorted(required)

    record = trace_by_id[query_id]
    reads = record["read_row_groups"]

    for entry in reads:
        rg = entry["row_group"]
        assert rg < parquet_file.metadata.num_row_groups, f"row_group {rg} out of range for {query_id}"
        decoded = parquet_file.read_row_group(rg, columns=read_columns)
        assert entry["decoded_rows"] == decoded.num_rows, f"decoded_rows mismatch for {query_id} rg {rg}"
        assert entry["receipt"] == _receipt_for_table(decoded), f"receipt mismatch for {query_id} rg {rg}"

    expected_query_receipt = _query_receipt(query_id, reads)
    assert record["query_receipt"] == expected_query_receipt, f"query_receipt mismatch for {query_id}"


@pytest.fixture(scope="session")
def rg_index(parquet_file: pq.ParquetFile) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for rg_idx in range(parquet_file.metadata.num_row_groups):
        rg_meta = parquet_file.metadata.row_group(rg_idx)
        table = parquet_file.read_row_group(rg_idx)
        result.append(_ref_build_entry(table, rg_meta))
    return result


def test_query_read_set_matches_reference(
    query_id: str,
    queries_by_id: dict[str, dict[str, Any]],
    trace_by_id: dict[str, dict[str, Any]],
    rg_index: list[dict[str, Any]],
) -> None:
    predicate = queries_by_id[query_id].get("predicate")
    reference_set = {
        rg_idx
        for rg_idx, entry in enumerate(rg_index)
        if predicate is None or _ref_may_be_true(entry, predicate)
    }
    agent_set = {e["row_group"] for e in trace_by_id[query_id]["read_row_groups"]}
    assert agent_set == reference_set, (
        f"{query_id}: reported row-group set does not match the sound-pruner reference. "
        f"Agent: {sorted(agent_set)}, reference: {sorted(reference_set)}. "
        f"Missing (must appear even when empty): {sorted(reference_set - agent_set)}. "
        f"Extra (provably prunable): {sorted(agent_set - reference_set)}."
    )
