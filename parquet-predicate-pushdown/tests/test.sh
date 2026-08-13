#!/usr/bin/env bash
# Verifier entry point. Writes reward to /reward (0 or 1).
set -euo pipefail

REWARD=0
mkdir -p /logs/verifier

set +e
python /tests/verify.py "$@"
EXIT=$?
set -e

if [ $EXIT -eq 0 ]; then
    REWARD=1
fi

echo "$REWARD" > /reward
echo "$REWARD" > /logs/verifier/reward.txt

python - <<PYEOF
import json

reward = $REWARD
report = {
    "results": {
        "tool": {"name": "verifier", "version": "1.0.0"},
        "summary": {
            "tests": 1,
            "passed": 1 if reward == 1 else 0,
            "failed": 0 if reward == 1 else 1,
            "skipped": 0,
            "pending": 0,
            "other": 0,
            "start": 0,
            "stop": 0,
        },
        "tests": [
            {
                "name": "verify.py",
                "status": "passed" if reward == 1 else "failed",
            }
        ],
    }
}
with open("/logs/verifier/ctrf.json", "w") as f:
    json.dump(report, f)
PYEOF

echo "Reward: $REWARD"
exit 0
