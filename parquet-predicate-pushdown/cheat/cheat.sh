#!/usr/bin/env bash
# Plausible cheating attempt: write fabricated results and a fake trace.
#
# Why the verifier rejects this:
#   1. The result counts (e.g. q1: 50 rows) happen to be right, but the actual
#      row data (id, sensor_id values) must match the ground truth exactly.
#      A cheat that doesn't run the engine cannot know the correct id values
#      without running the generator with the exact same seed.
#   2. The trace must show <= 35 row_group_read events total. A fake trace can
#      achieve this, BUT the per-query segment test counts reads+skips per query
#      and requires exactly 10 events per query (one per row group). A cheat
#      that doesn't understand this will produce the wrong segment lengths,
#      causing _count_reads_by_query to misattribute events to wrong queries
#      and fail the per-query threshold assertions.
#   3. test_q5_boundary_correct checks that sensor_id=200 returns 50 rows with
#      the correct id values. Getting these right without running the engine
#      requires exact knowledge of the deterministic generator output.
#
# A determined cheater who reads the ground_truth.json from the tests image
# still cannot access it at solve time — it is in a separate sealed image.

set -euo pipefail

# Write plausible-looking but wrong results
cat > /app/results.json << 'EOF'
{
  "q1": [],
  "q2": [],
  "q3": [],
  "q4": [],
  "q5": [],
  "q6": [],
  "q7": []
}
EOF

# Write a fake trace claiming pushdown happened
python3 - << 'PYEOF'
import json

trace = []
queries = ["q1", "q2", "q3", "q4", "q5", "q6", "q7"]
for q in queries:
    trace.append({"event": "row_group_read", "file": "sensors.parquet", "row_group": 0, "rows": 1000})
    for rg in range(1, 10):
        trace.append({"event": "row_group_skipped", "file": "sensors.parquet", "row_group": rg, "reason": "predicate_pushdown"})

with open("/app/trace.jsonl", "w") as f:
    for line in trace:
        f.write(json.dumps(line) + "\n")
PYEOF

echo "Cheat complete (will fail verification due to wrong result data)"
