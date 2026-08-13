#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${APP_ROOT:-/app}/data"
QUERIES_FILE="${DATA_DIR}/queries.json"
QUERIES_HIDDEN="${QUERIES_FILE}.hidden"

restore_queries() {
  if [ -f "${QUERIES_HIDDEN}" ]; then
    mv "${QUERIES_HIDDEN}" "${QUERIES_FILE}"
  fi
}

trap restore_queries EXIT

mv "${QUERIES_FILE}" "${QUERIES_HIDDEN}"
python "${SCRIPT_DIR}/engine.py" build
restore_queries
python "${SCRIPT_DIR}/engine.py" query

trap - EXIT
