#!/usr/bin/env bash
set -euo pipefail

REWARD=0
mkdir -p /logs/verifier

if pytest -q /tests/test_verify.py --ctrf=/logs/verifier/ctrf.json; then
  REWARD=1
else
  REWARD=0
fi

echo "$REWARD" > /reward
echo "$REWARD" > /logs/verifier/reward.txt

echo "Reward: $REWARD"
exit 0
