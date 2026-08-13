#!/usr/bin/env bash
# Verifier entry point.
# Always writes exactly 0 or 1 to /logs/verifier/reward.txt.
# Exits 0 on every code path.

set -uo pipefail

mkdir -p /logs/verifier

REWARD=0

run_tests() {
    pytest /tests/test_queries.py \
        --ctrf /logs/verifier/ctrf.json \
        -v \
        2>&1 | tee /logs/verifier/pytest.log
    return ${PIPESTATUS[0]}
}

if run_tests; then
    REWARD=1
fi

echo "$REWARD" > /logs/verifier/reward.txt
echo "Reward: $REWARD"
exit 0
