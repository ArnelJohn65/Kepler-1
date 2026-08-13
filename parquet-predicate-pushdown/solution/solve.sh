#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [ -z "${APP_ROOT:-}" ]; then
  export APP_ROOT=/app
fi

python "${SCRIPT_DIR}/solve.py"
