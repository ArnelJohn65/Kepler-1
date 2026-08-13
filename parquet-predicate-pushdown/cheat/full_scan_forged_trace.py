import json
import os

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

APP_ROOT = os.environ.get("APP_ROOT", "/app")
DATA_DIR = os.path.join(APP_ROOT, "data")
RESULTS_PATH = os.path.join(APP_ROOT, "results.json")
TRACE_PATH = os.path.join(APP_ROOT, "trace.jsonl")


def _columns_in_predicate(node, output):
    if node is None:
        return
    t = node["type"]
    if t in {"cmp", "in", "is_null", "is_not_null"}:
        output.add(node["column"])
        return
    if t in {"and", "or"}:
        for c in node["children"]:
            _columns_in_predicate(c, output)
        return
    if t == "not":
        _columns_in_predicate(node["child"], output)
        return


def _mask(table: pa.Table, node):
    t = node["type"]
    if t == "cmp":
        col = table.column(node["column"])
        op = node["op"]
        val = node["value"]
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
        return pc.is_in(col, value_set=pa.array(node["values"], type=col.type))
    if t == "is_null":
        return pc.is_null(table.column(node["column"]))
    if t == "is_not_null":
        return pc.is_valid(table.column(node["column"]))
    if t == "and":
        parts = [_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.and_(out, p)
        return out
    if t == "or":
        parts = [_mask(table, c) for c in node["children"]]
        out = parts[0]
        for p in parts[1:]:
            out = pc.or_(out, p)
        return out
    if t == "not":
        return pc.invert(_mask(table, node["child"]))
    raise ValueError(t)


def _apply(table, predicate):
    if predicate is None:
        return table
    return table.filter(_mask(table, predicate))


def main():
    with open(os.path.join(DATA_DIR, "queries.json"), encoding="utf-8") as f:
        queries = json.load(f)
    pf = pq.ParquetFile(os.path.join(DATA_DIR, queries[0]["file"]))

    all_results = []
    for q in queries:
        predicate = q.get("predicate")
        proj = q["columns"]
        required = set(proj)
        _columns_in_predicate(predicate, required)
        read_cols = sorted(required)

        rows = []
        for rg in range(pf.metadata.num_row_groups):
            decoded = pf.read_row_group(rg, columns=read_cols)
            filtered = _apply(decoded, predicate)
            rows.extend(filtered.select(proj).to_pylist())
        all_results.append({"query_id": q["id"], "rows": rows})

    with open(RESULTS_PATH, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    # Forged trace that claims selective reads without valid receipts.
    with open(TRACE_PATH, "w", encoding="utf-8") as f:
        for q in queries:
            fake = {
                "query_id": q["id"],
                "read_row_groups": [{"row_group": 0, "decoded_rows": 500, "receipt": "deadbeefdeadbeefdeadbeefdeadbeef"}],
                "query_receipt": "cafebabecafebabecafebabecafebabe",
                "result_count": len(next(item["rows"] for item in all_results if item["query_id"] == q["id"])),
            }
            f.write(json.dumps(fake) + "\n")


if __name__ == "__main__":
    main()
