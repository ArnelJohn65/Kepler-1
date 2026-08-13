"""Verifier tests for the parquet-predicate-pushdown task."""

import json
import math
import os

import pytest

RESULTS_PATH = os.environ.get("RESULTS_PATH", "/app/results.json")
TRACE_PATH = os.environ.get("TRACE_PATH", "/app/trace.jsonl")
GROUND_TRUTH_PATH = os.environ.get("GROUND_TRUTH_PATH", "/tests/ground_truth.json")

# Total row-groups-read across all queries with genuine pushdown.
# Full scan (no pushdown) reads 20 * 10 = 200.
# A correct pushdown implementation reads at most 104.
# We allow 130 to give some headroom while staying well below 200.
PRUNING_THRESHOLD = 130


@pytest.fixture(scope="session")
def results():
    assert os.path.exists(RESULTS_PATH), f"Missing {RESULTS_PATH}"
    with open(RESULTS_PATH) as f:
        return json.load(f)


@pytest.fixture(scope="session")
def trace():
    assert os.path.exists(TRACE_PATH), f"Missing {TRACE_PATH}"
    entries = []
    with open(TRACE_PATH) as f:
        for line in f:
            line = line.strip()
            if line:
                entries.append(json.loads(line))
    return entries


@pytest.fixture(scope="session")
def ground_truth():
    with open(GROUND_TRUTH_PATH) as f:
        return json.load(f)


class TestQueryResults:
    """All query results must match ground truth exactly (or within float tolerance)."""

    def _check(self, results, ground_truth, qid):
        assert qid in results, f"Query {qid} missing from results"
        got = results[qid]
        exp = ground_truth[qid]
        assert set(got.keys()) == set(exp.keys()), f"{qid}: key mismatch {got} vs {exp}"
        for k in exp:
            g, e = got[k], exp[k]
            if isinstance(e, float) or isinstance(g, float):
                assert math.isclose(g, e, rel_tol=1e-4), f"{qid}[{k}]: got {g}, expected {e}"
            else:
                assert g == e, f"{qid}[{k}]: got {g}, expected {e}"

    def test_q1_range_narrow(self, results, ground_truth):
        self._check(results, ground_truth, "q1_range_narrow")

    def test_q2_boundary_gte_max(self, results, ground_truth):
        """Boundary predicate — off-by-one bug must be fixed."""
        self._check(results, ground_truth, "q2_boundary_gte_max")

    def test_q3_boundary_lte_min(self, results, ground_truth):
        """Boundary predicate — off-by-one bug must be fixed."""
        self._check(results, ground_truth, "q3_boundary_lte_min")

    def test_q4_is_null(self, results, ground_truth):
        self._check(results, ground_truth, "q4_is_null")

    def test_q5_is_not_null(self, results, ground_truth):
        self._check(results, ground_truth, "q5_is_not_null")

    def test_q6_range_rg10(self, results, ground_truth):
        self._check(results, ground_truth, "q6_range_rg10")

    def test_q7_full_scan(self, results, ground_truth):
        self._check(results, ground_truth, "q7_full_scan")

    def test_q8_sum_score(self, results, ground_truth):
        self._check(results, ground_truth, "q8_sum_score")

    def test_q9_category(self, results, ground_truth):
        self._check(results, ground_truth, "q9_category")

    def test_q10_above_all(self, results, ground_truth):
        self._check(results, ground_truth, "q10_above_all")


class TestTraceFormat:
    """trace.jsonl must exist and have well-formed entries."""

    def test_trace_exists(self, trace):
        assert len(trace) > 0, "trace.jsonl is empty"

    def test_trace_fields(self, trace):
        for entry in trace:
            assert "query_id" in entry
            assert "row_groups_read" in entry
            assert "row_groups_total" in entry
            assert isinstance(entry["row_groups_read"], list)

    def test_trace_has_all_queries(self, trace):
        ids = {e["query_id"] for e in trace}
        expected = {
            "q1_range_narrow", "q2_boundary_gte_max", "q3_boundary_lte_min",
            "q4_is_null", "q5_is_not_null", "q6_range_rg10", "q7_full_scan",
            "q8_sum_score", "q9_category", "q10_above_all",
        }
        assert expected.issubset(ids), f"Missing trace entries: {expected - ids}"


class TestPruningEvidence:
    """Total row groups read must be below the threshold, proving real pushdown."""

    def test_total_row_groups_read_below_threshold(self, trace):
        total_read = sum(len(e["row_groups_read"]) for e in trace)
        assert total_read <= PRUNING_THRESHOLD, (
            f"Total row groups read ({total_read}) exceeds pruning threshold "
            f"({PRUNING_THRESHOLD}). Pushdown is not working. "
            f"A full scan would read 200 row groups across 10 queries."
        )

    def test_full_scan_query_reads_all(self, trace):
        """q7 has no predicate and must read all 20 row groups."""
        q7 = next((e for e in trace if e["query_id"] == "q7_full_scan"), None)
        assert q7 is not None
        assert len(q7["row_groups_read"]) == q7["row_groups_total"], (
            "Full-scan query must read every row group"
        )

    def test_above_all_query_prunes_everything(self, trace):
        """q10 predicate is above all data — all row groups should be pruned."""
        q10 = next((e for e in trace if e["query_id"] == "q10_above_all"), None)
        assert q10 is not None
        assert len(q10["row_groups_read"]) == 0, (
            f"q10 (value > 999999) should prune all row groups, "
            f"but read {len(q10['row_groups_read'])}"
        )

    def test_narrow_range_prunes_most(self, trace):
        """q1 (value in [5000,6000)) should read at most 2 row groups."""
        q1 = next((e for e in trace if e["query_id"] == "q1_range_narrow"), None)
        assert q1 is not None
        assert len(q1["row_groups_read"]) <= 2, (
            f"q1 narrow range should read <=2 row groups, "
            f"but read {len(q1['row_groups_read'])}"
        )

    def test_is_null_reads_all(self, trace):
        """IS NULL cannot be pruned via min/max stats; must read all row groups."""
        q4 = next((e for e in trace if e["query_id"] == "q4_is_null"), None)
        assert q4 is not None
        assert len(q4["row_groups_read"]) == q4["row_groups_total"], (
            "IS NULL query must read all row groups (cannot prune via value stats)"
        )
