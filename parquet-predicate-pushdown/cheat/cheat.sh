#!/usr/bin/env bash
# Cheating attempt: fabricate results.json and a forged trace.jsonl.
# This is NEVER executed by the pipeline. It documents which verifier
# checks catch this kind of attack.
#
# Cheat strategy:
#   1. Hardcode the expected counts from ground_truth.json into results.json.
#   2. Forge a trace with small row_groups_read lists to pass the pruning check.
#
# Why this fails:
#
# CHECK 1 — Query results (TestQueryResults):
#   The verifier compares every field of every query result against its sealed
#   ground truth. If the cheat hardcodes the correct values, this check passes.
#   (The cheat below does hardcode them, so this check alone does NOT catch it.)
#
# CHECK 2 — Pruning threshold (test_total_row_groups_read_below_threshold):
#   We forge a trace that reads 0 row groups for each query. Total = 0 <= 130.
#   This check passes with the forged trace — not caught here either.
#
# CHECK 3 — Full-scan query reads all (test_full_scan_query_reads_all):
#   q7 has no predicate. The test asserts len(row_groups_read) == row_groups_total.
#   Our forged trace gives row_groups_read = [] for q7, so len([]) = 0 != 20.
#   THIS CHECK REJECTS THE CHEAT.
#
# CHECK 4 — IS NULL reads all (test_is_null_reads_all):
#   q4 is an IS NULL query. The test asserts all row groups are read.
#   Our forged trace gives row_groups_read = [] for q4, so len([]) = 0 != 20.
#   THIS CHECK ALSO REJECTS THE CHEAT.
#
# To pass ALL checks, a cheat would need to know the exact row-group counts for
# every query, which requires actually running the engine or having its output.
# At that point the cheat is indistinguishable from the real solution.

set -euo pipefail

RESULTS=/app/results.json
TRACE=/app/trace.jsonl

# Fabricate results (using correct values so result checks pass)
cat > "$RESULTS" << 'JSON'
{
  "q10_above_all": {"count": 0},
  "q1_range_narrow": {"count": 478},
  "q2_boundary_gte_max": {"count": 6643},
  "q3_boundary_lte_min": {"count": 2383},
  "q4_is_null": {"count": 498},
  "q5_is_not_null": {"count": 9502},
  "q6_range_rg10": {"count": 464},
  "q7_full_scan": {"count": 10000},
  "q8_sum_score": {"sum": 23060.8075},
  "q9_category": {"count": 2549}
}
JSON

# Forge a trace claiming 0 row groups were read for every query.
# This makes the pruning threshold check pass, but fails the full-scan
# and IS NULL assertions.
for qid in q1_range_narrow q2_boundary_gte_max q3_boundary_lte_min \
            q4_is_null q5_is_not_null q6_range_rg10 q7_full_scan \
            q8_sum_score q9_category q10_above_all; do
    echo "{\"query_id\": \"$qid\", \"row_groups_read\": [], \"row_groups_total\": 20}" >> "$TRACE"
done

echo "Forged results and trace written (pipeline will reject this)" >&2
