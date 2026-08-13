#!/usr/bin/env bash
# Cheat oracle: full scan with forged trace — scores 0 because receipts are wrong.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python "$SCRIPT_DIR/full_scan_forged_trace.py"
