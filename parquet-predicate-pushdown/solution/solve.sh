#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export APP_ROOT="${APP_ROOT:-/app}"

python "${SCRIPT_DIR}/solve.py" build
python "${SCRIPT_DIR}/solve.py" query
