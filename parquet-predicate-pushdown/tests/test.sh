#!/usr/bin/env bash
set -euo pipefail

EXPECTED_TESTS=26
REWARD=0
mkdir -p /logs/verifier

set +e
pytest -q /tests/test_verify.py --ctrf=/logs/verifier/ctrf.json 2>&1 | tee /logs/verifier/pytest.log
PYTEST_EXIT=$?
set -e

echo "pytest exit code: $PYTEST_EXIT"

if [ ! -f /logs/verifier/ctrf.json ]; then
  echo "CTRF report not written — reward 0"
  echo "$REWARD" > /reward
  echo "$REWARD" > /logs/verifier/reward.txt
  exit 0
fi

PASSED=$(python3 -c "
import json, sys
with open('/logs/verifier/ctrf.json') as f:
    d = json.load(f)
s = d['results']['summary']
passed = s.get('passed', 0)
failed = s.get('failed', 0)
skipped = s.get('skipped', 0)
errors = s.get('errors', 0)
pending = s.get('pending', 0)
print(passed, failed, skipped, errors, pending)
" 2>/dev/null || echo "0 1 0 0 0")

read -r PASS FAIL SKIP ERR PEND <<< "$PASSED"

echo "passed=$PASS failed=$FAIL skipped=$SKIP errors=$ERR pending=$PEND expected=$EXPECTED_TESTS"

if [ "$PASS" = "$EXPECTED_TESTS" ] && [ "$FAIL" = "0" ] && [ "$SKIP" = "0" ] && [ "$ERR" = "0" ] && [ "$PEND" = "0" ]; then
  REWARD=1
else
  REWARD=0
fi

echo "$REWARD" > /reward
echo "$REWARD" > /logs/verifier/reward.txt
echo "Reward: $REWARD"
exit 0
