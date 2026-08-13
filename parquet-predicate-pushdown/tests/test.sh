#!/usr/bin/env bash
set -euo pipefail

EXPECTED_TESTS=26  # test_agent_artifacts_exist + test_index_size_cap + 12 visible queries + 12 hidden queries
REWARD=0
LOGS_ROOT="${LOGS_ROOT:-/logs}"
REWARD_PATH="${REWARD_PATH:-/reward}"
TESTS_ROOT="${TESTS_ROOT:-/tests}"
mkdir -p "${LOGS_ROOT}/verifier"

set +e
pytest -q "${TESTS_ROOT}/test_verify.py" --ctrf="${LOGS_ROOT}/verifier/ctrf.json" 2>&1 | tee "${LOGS_ROOT}/verifier/pytest.log"
PYTEST_EXIT=$?
set -e

echo "pytest exit code: $PYTEST_EXIT"

if [ ! -f "${LOGS_ROOT}/verifier/ctrf.json" ]; then
  echo "CTRF report not written — reward 0"
  echo "$REWARD" > "${REWARD_PATH}"
  echo "$REWARD" > "${LOGS_ROOT}/verifier/reward.txt"
  exit 0
fi

PASSED=$(python3 -c "
import json, sys
with open('${LOGS_ROOT}/verifier/ctrf.json') as f:
    d = json.load(f)
s = d['results']['summary']
passed = s.get('passed', 0)
failed = s.get('failed', 0)
skipped = s.get('skipped', 0)
errors = s.get('errors', 0)
pending = s.get('pending', 0)
print(passed, failed, skipped, errors, pending)
" 2>>"${LOGS_ROOT}/verifier/pytest.log" || echo "0 1 0 0 0")

read -r PASS FAIL SKIP ERR PEND <<< "$PASSED"

echo "passed=$PASS failed=$FAIL skipped=$SKIP errors=$ERR pending=$PEND expected=$EXPECTED_TESTS"

if [ "$PASS" = "$EXPECTED_TESTS" ] && [ "$FAIL" = "0" ] && [ "$SKIP" = "0" ] && [ "$ERR" = "0" ] && [ "$PEND" = "0" ]; then
  REWARD=1
else
  REWARD=0
fi

echo "$REWARD" > "${REWARD_PATH}"
echo "$REWARD" > "${LOGS_ROOT}/verifier/reward.txt"
echo "Reward: $REWARD"
exit 0
