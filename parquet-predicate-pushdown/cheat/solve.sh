#!/usr/bin/env bash
# Cheat oracle: full scan that reports only matching row groups.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python "$SCRIPT_DIR/full_scan_omit_empty.py"
