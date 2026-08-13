#!/usr/bin/env bash
# Verifier entry point. Writes reward to /reward (0 or 1).
set -euo pipefail

REWARD=0

python /tests/verify.py "$@"
EXIT=$?

if [ $EXIT -eq 0 ]; then
    REWARD=1
fi

echo "$REWARD" > /reward
echo "Reward: $REWARD"
exit 0
