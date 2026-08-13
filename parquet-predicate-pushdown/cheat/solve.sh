#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_ROOT="${APP_ROOT:-/app}"
DATA_DIR="${APP_ROOT}/data"
QUERIES_FILE="${DATA_DIR}/queries.json"
QUERIES_HIDDEN="${QUERIES_FILE}.hidden"
VERIFY_SCRIPT="${SCRIPT_DIR}/../tests/test.sh"
CHEAT_FAILED=0

restore_queries() {
  if [ -f "${QUERIES_HIDDEN}" ]; then
    mv "${QUERIES_HIDDEN}" "${QUERIES_FILE}"
  fi
}

reset_state() {
  restore_queries
  rm -f \
    "${APP_ROOT}/results.json" \
    "${APP_ROOT}/trace.jsonl" \
    "${APP_ROOT}/row_group_index.pkl" \
    "${APP_ROOT}/query_metrics.json" \
    "${DATA_DIR}/exact_read_index.pkl"
}

run_verifier() {
  bash "${VERIFY_SCRIPT}" >/dev/null 2>&1 || true
  if [ -f /reward ]; then
    cat /reward
  else
    echo 0
  fi
}

run_one_phase_cheat() {
  local name="$1"
  local script="$2"

  echo "== ${name} =="
  reset_state
  if ! python "${script}" >/dev/null 2>&1; then
    echo "${name}: cheat crashed as expected"
  fi

  local reward
  reward="$(run_verifier)"
  if [ "${reward}" = "1" ]; then
    echo "${name}: verifier accepted cheat"
    CHEAT_FAILED=1
  else
    echo "${name}: verifier rejected cheat"
  fi
}

run_two_phase_cheat() {
  local name="$1"
  local script="$2"

  echo "== ${name} =="
  reset_state
  trap restore_queries EXIT
  mv "${QUERIES_FILE}" "${QUERIES_HIDDEN}"
  if python "${script}" build >/dev/null 2>&1; then
    restore_queries
    if ! python "${script}" query >/dev/null 2>&1; then
      echo "${name}: query phase failed as expected"
    fi
  else
    echo "${name}: build phase failed as expected"
    restore_queries
  fi
  trap - EXIT

  local reward
  reward="$(run_verifier)"
  if [ "${reward}" = "1" ]; then
    echo "${name}: verifier accepted cheat"
    CHEAT_FAILED=1
  else
    echo "${name}: verifier rejected cheat"
  fi
}

run_two_phase_cheat "exact-pruner" "${SCRIPT_DIR}/exact_pruner.py"
run_one_phase_cheat "full-scan-omit-empty" "${SCRIPT_DIR}/full_scan_omit_empty.py"
run_one_phase_cheat "minmax-only-pruner" "${SCRIPT_DIR}/minmax_only_pruner.py"
run_one_phase_cheat "full-scan-forged-trace" "${SCRIPT_DIR}/full_scan_forged_trace.py"
run_two_phase_cheat "materialized-index-cheat" "${SCRIPT_DIR}/materialized_index_cheat.py"

reset_state
if [ "${CHEAT_FAILED}" -ne 0 ]; then
  exit 1
fi
