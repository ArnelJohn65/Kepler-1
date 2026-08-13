#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BUILD_TIMEOUT_SEC="${BUILD_TIMEOUT_SEC:-120}"
QUERY_TIMEOUT_SEC="${QUERY_TIMEOUT_SEC:-1.6}"

timeout "${BUILD_TIMEOUT_SEC}" python "${SCRIPT_DIR}/engine.py" build
timeout "${QUERY_TIMEOUT_SEC}" python "${SCRIPT_DIR}/engine.py" query
