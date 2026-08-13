#!/usr/bin/env bash
# Cheat oracle: full scan with forged trace — scores 0 because receipts are wrong.
set -euo pipefail
exec python /app/cheat/full_scan_forged_trace.py
