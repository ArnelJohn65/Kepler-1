#!/usr/bin/env bash
# Verifier entry point. Writes /logs/verifier/reward.txt (0 or 1) and
# /logs/verifier/ctrf.json on every code path, including crashes.
set -uo pipefail

REWARD_DIR=/logs/verifier
mkdir -p "$REWARD_DIR"

# Default to failure
echo "0" > "$REWARD_DIR/reward.txt"

RESULTS=/app/results.json
TRACE=/app/trace.jsonl

# Check artifacts exist
if [ ! -f "$RESULTS" ]; then
    echo "FAIL: $RESULTS not found" >&2
    pytest /tests/test_queries.py \
        --ctrf "$REWARD_DIR/ctrf.json" \
        --tb=no -q 2>/dev/null || true
    exit 0
fi

if [ ! -f "$TRACE" ]; then
    echo "FAIL: $TRACE not found" >&2
    pytest /tests/test_queries.py \
        --ctrf "$REWARD_DIR/ctrf.json" \
        --tb=no -q 2>/dev/null || true
    exit 0
fi

# Run pytest
if pytest /tests/test_queries.py \
        --ctrf "$REWARD_DIR/ctrf.json" \
        -v 2>&1; then
    echo "1" > "$REWARD_DIR/reward.txt"
else
    echo "0" > "$REWARD_DIR/reward.txt"
fi

exit 0
