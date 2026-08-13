"""
Verifier tests for parquet-predicate-pushdown.

Checks:
1. /app/results.json exists and contains correct query results.
2. /app/trace.jsonl exists and shows that predicate pushdown actually pruned
   row groups for selective queries (below a threshold only achievable with
   working pushdown).
3. NULL semantics are respected: IS NULL query reads all row groups.
4. Boundary predicate (q5, sensor_id=10000) returns correct results,
   verifying the off-by-one bug is fixed.
"""
import json
import os
import pytest

RESULTS_PATH = "/app/results.json"
TRACE_PATH = "/app/trace.jsonl"
GROUND_TRUTH_PATH = "/tests/ground_truth.json"

# Maximum row groups that may be read for selective queries.
# Total row groups in the dataset: 10.
# With correct pushdown:
#   q1 (sensor_id=5): 1 rg read
#   q2 (sensor_id>9000): 1 rg read
#   q3 (sensor_id<=1000): 1 rg read
#   q5 (sensor_id=10000): 1 rg read (requires off-by-one fix)
#   q6 (sensor_id 4000-5000): 2 rgs read
PRUNING_THRESHOLDS = {
    "q1": 2,
    "q2": 2,
    "q3": 2,
    "q5": 2,
    "q6": 3,
}
# Queries that must NOT be pruned (all 10 rgs must be read)
NO_PRUNE_QUERIES = {"q4", "q7"}


@pytest.fixture(scope="session")
def results():
    assert os.path.exists(RESULTS_PATH), f"Missing {RESULTS_PATH}"
    with open(RESULTS_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def ground_truth():
    with open(GROUND_TRUTH_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def trace():
    assert os.path.exists(TRACE_PATH), f"Missing {TRACE_PATH}"
    lines = []
    with open(TRACE_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                lines.append(json.loads(line))
    return lines


def _sort_rows(rows, key=None):
    if not rows:
        return rows
    if key is None:
        key = list(rows[0].keys())[0]
    return sorted(rows, key=lambda r: (r.get(key) is None, r.get(key)))


def _count_reads_by_query(trace):
    """Split trace into per-query read counts using the sequential segment approach.

    Each query targets exactly one Parquet file with 10 row groups, so each
    query generates exactly 10 trace events (row_group_read or row_group_skipped).
    We iterate the flat trace in order, collecting 10 events per query.
    """
    query_order = ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]
    counts = {}
    event_iter = iter(trace)
    for qid in query_order:
        read_count = 0
        seen = 0
        for ev in event_iter:
            if ev.get("event") in ("row_group_read", "row_group_skipped"):
                if ev.get("event") == "row_group_read":
                    read_count += 1
                seen += 1
                if seen == 10:
                    break
        counts[qid] = read_count
    return counts


class TestResults:
    def test_results_file_exists(self, results):
        assert isinstance(results, dict)

    def test_all_query_ids_present(self, results):
        for qid in ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]:
            assert qid in results, f"Missing query {qid} in results"

    def test_q1_correct(self, results, ground_truth):
        got = _sort_rows(results["q1"], "id")
        exp = _sort_rows(ground_truth["q1"], "id")
        assert got == exp, f"q1 mismatch: got {len(got)} rows, expected {len(exp)}"

    def test_q2_correct(self, results, ground_truth):
        got = _sort_rows(results["q2"], "id")
        exp = _sort_rows(ground_truth["q2"], "id")
        assert got == exp, f"q2 mismatch"

    def test_q3_correct(self, results, ground_truth):
        got = _sort_rows(results["q3"], "id")
        exp = _sort_rows(ground_truth["q3"], "id")
        assert got == exp, f"q3 mismatch"

    def test_q4_null_semantics(self, results, ground_truth):
        got = _sort_rows(results["q4"], "id")
        exp = _sort_rows(ground_truth["q4"], "id")
        assert got == exp, f"q4 (IS NULL) mismatch"

    def test_q5_boundary_correct(self, results, ground_truth):
        """sensor_id=10000 is the last element in the last row group.
        With the off-by-one bug in stats, that row group's max is recorded as
        9999 instead of 10000, causing the planner to incorrectly prune it and
        return 0 rows instead of 1.
        """
        got = _sort_rows(results["q5"], "id")
        exp = _sort_rows(ground_truth["q5"], "id")
        assert len(got) == len(exp), (
            f"q5 boundary: got {len(got)} rows, expected {len(exp)}. "
            "This indicates the off-by-one bug in stats.py is not fixed."
        )
        assert got == exp, f"q5 boundary row data mismatch"

    def test_q6_correct(self, results, ground_truth):
        got = _sort_rows(results["q6"], "id")
        exp = _sort_rows(ground_truth["q6"], "id")
        assert got == exp, f"q6 mismatch"

    def test_q7_correct(self, results, ground_truth):
        got = _sort_rows(results["q7"], "id")
        exp = _sort_rows(ground_truth["q7"], "id")
        assert got == exp, f"q7 mismatch"


class TestPruning:
    def test_trace_file_exists(self, trace):
        assert len(trace) > 0

    def test_trace_has_read_events(self, trace):
        reads = [e for e in trace if e.get("event") == "row_group_read"]
        assert len(reads) > 0

    def test_pruning_reduces_row_groups(self, trace):
        """With correct predicate pushdown, the total number of row_group_read
        events must be well below 70 (7 queries * 10 row groups each = 70 without
        pushdown). With pushdown the expected total is 26. We allow up to 35 to
        give some flexibility while still requiring real pushdown.
        """
        total_reads = sum(1 for e in trace if e.get("event") == "row_group_read")
        assert total_reads <= 35, (
            f"Total row_group_read events={total_reads}. Expected <= 35 with pushdown. "
            "Without pushdown the total would be 70."
        )

    def test_per_query_pruning(self, trace):
        """Verify per-query read counts using the sequential segment approach."""
        counts = _count_reads_by_query(trace)
        for qid, threshold in PRUNING_THRESHOLDS.items():
            reads = counts.get(qid, 10)
            assert reads <= threshold, (
                f"Query {qid}: read {reads} row groups, expected <= {threshold} with pushdown"
            )

    def test_no_prune_queries_read_all(self, trace):
        """IS NULL and != queries must not be pruned (10 row groups each)."""
        counts = _count_reads_by_query(trace)
        for qid in NO_PRUNE_QUERIES:
            reads = counts.get(qid, 0)
            assert reads >= 10, (
                f"Query {qid} should read all 10 row groups, but only read {reads}"
            )

    def test_trace_has_skip_events(self, trace):
        skips = [e for e in trace if e.get("event") == "row_group_skipped"]
        assert len(skips) > 0, "No row_group_skipped events — pushdown not implemented"

    def test_skip_events_have_reason(self, trace):
        for e in trace:
            if e.get("event") == "row_group_skipped":
                assert e.get("reason") == "predicate_pushdown", (
                    f"Skip event missing correct reason: {e}"
                )
