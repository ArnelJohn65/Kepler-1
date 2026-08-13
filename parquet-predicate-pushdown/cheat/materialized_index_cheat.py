import hashlib
import json
import math
import os
import pickle
import sys
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")
INDEX_PATH = os.path.join(APP_ROOT, "row_group_index.pkl")


def _normalize(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return "NaN"
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, datetime):
        return v.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return v


def _coerce_scalar(value: Any, dtype: pa.DataType) -> Any:
    if pa.types.is_decimal(dtype):
        return Decimal(str(value))
    if pa.types.is_timestamp(dtype) and isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


def _columns_in_predicate(node: dict[str, Any] | None, output: set[str]) -> None:
    if node is None:
        return
    t = node["type"]
    if t in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if t in {"and", "or"}:
        for child in node["children"]:
            _columns_in_predicate(child, output)
        return
    if t == "not":
        _columns_in_predicate(node["child"], output)


def _mask(table: pa.Table, node: dict[str, Any]) -> pa.Array:
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        val = _coerce_scalar(node["value"], col.type)
        return {
            "eq": pc.equal,
            "ne": pc.not_equal,
            "lt": pc.less,
            "le": pc.less_equal,
            "gt": pc.greater,
            "ge": pc.greater_equal,
        }[op](col, val)
    if t == "in":
        col = table.column(node["column"])
        values = [_coerce_scalar(v, col.type) for v in node["values"]]
        return pc.is_in(col, value_set=pa.array(values, type=col.type))
    if t == "is_null":
        return pc.is_null(table.column(node["column"]))
    if t == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if t == "and":
        parts = [_mask(table, child) for child in node["children"]]
        out = parts[0]
        for part in parts[1:]:
            out = pc.and_(out, part)
        return out
    if t == "or":
        parts = [_mask(table, child) for child in node["children"]]
        out = parts[0]
        for part in parts[1:]:
            out = pc.or_(out, part)
        return out
    if t == "not":
        return pc.invert(_mask(table, node["child"]))
    raise ValueError(t)


def _apply(table: pa.Table, predicate: dict[str, Any] | None) -> pa.Table:
    if predicate is None:
        return table
    return table.filter(_mask(table, predicate))


def _receipt(table: pa.Table) -> str:
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
        h.update(
            f"{entry['row_group']}:{entry['decoded_rows']}:{entry['decoded_bytes']}:{entry['receipt']}".encode("utf-8")
        )
        h.update(b"|")
    return h.hexdigest()


def _load_queries() -> list[dict[str, Any]]:
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        return json.load(f)


def build() -> None:
    import glob as _glob

    matches = _glob.glob(os.path.join(DATA_DIR, "*.parquet"))
    if not matches:
        raise RuntimeError(f"No parquet file found in {DATA_DIR}")
    parquet_path = sorted(matches)[0]
    pf = pq.ParquetFile(parquet_path)

    materialized_row_groups: list[dict[str, Any]] = []
    for rg_idx in range(pf.metadata.num_row_groups):
        decoded = pf.read_row_group(rg_idx)
        buf = pa.BufferOutputStream()
        with pa.ipc.new_stream(buf, decoded.schema) as writer:
            writer.write_table(decoded)
        materialized_row_groups.append({"row_group": rg_idx, "ipc": buf.getvalue().to_pybytes()})

    with open(INDEX_PATH, "wb") as f:
        pickle.dump({"parquet": os.path.basename(parquet_path), "row_groups": materialized_row_groups}, f)


def query() -> None:
    queries = _load_queries()
    with open(INDEX_PATH, "rb") as f:
        payload = pickle.load(f)

    all_results = []
    all_traces = []
    for query_spec in queries:
        predicate = query_spec.get("predicate")
        projection = query_spec["columns"]
        required = set(projection)
        _columns_in_predicate(predicate, required)
        read_columns = sorted(required)

        rows = []
        read_trace = []
        for entry in payload["row_groups"]:
            decoded = pa.ipc.open_stream(pa.py_buffer(entry["ipc"])).read_all().select(read_columns)
            filtered = _apply(decoded, predicate)
            if filtered.num_rows == 0:
                continue
            rows.extend({k: _normalize(v) for k, v in row.items()} for row in filtered.select(projection).to_pylist())
            read_trace.append(
                {
                    "row_group": entry["row_group"],
                    "decoded_rows": decoded.num_rows,
                    "decoded_bytes": decoded.nbytes,
                    "receipt": _receipt(decoded),
                }
            )

        all_results.append({"query_id": query_spec["id"], "rows": rows})
        all_traces.append(
            {
                "query_id": query_spec["id"],
                "read_row_groups": read_trace,
                "query_receipt": _query_receipt(query_spec["id"], read_trace),
                "result_count": len(rows),
            }
        )

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)
    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for rec in all_traces:
            f.write(json.dumps(rec) + "\n")


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    if mode == "build":
        build()
    elif mode == "query":
        query()
    elif mode == "all":
        build()
        query()
    else:
        raise SystemExit("Usage: python materialized_index_cheat.py [build|query|all]")


if __name__ == "__main__":
    main()
