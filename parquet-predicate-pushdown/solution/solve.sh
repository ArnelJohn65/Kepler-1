#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${APP_ROOT:-}" ]; then
  export APP_ROOT=/app
fi

BUILD_TIMEOUT_SEC="${BUILD_TIMEOUT_SEC:-120}"
QUERY_TIMEOUT_SEC="${QUERY_TIMEOUT_SEC:-1.6}"

timeout "${BUILD_TIMEOUT_SEC}" python "${SCRIPT_DIR}/solve.py" build
timeout "${QUERY_TIMEOUT_SEC}" python "${SCRIPT_DIR}/solve.py" query
