#!/usr/bin/env bash
set -euo pipefail

BIN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$BIN_DIR/env.sh"

wait_until() {
  local timeout_secs="$1"
  local desc="$2"
  shift 2

  local deadline=$((SECONDS + timeout_secs))
  while ((SECONDS < deadline)); do
    if "$@"; then
      return 0
    fi
    sleep 1
  done

  echo "[FAIL] timeout: $desc" >&2
  return 1
}

subscription_has_pid() {
  local subname="$1"
  local cnt
  cnt="$(sub_psql -At -c "SELECT count(*) FROM pg_stat_subscription WHERE subname='${subname}' AND pid IS NOT NULL;")"
  [[ "$cnt" == "1" ]]
}

table_row_count_is() {
  local table_name="$1"
  local expected="$2"
  local cnt
  cnt="$(sub_psql -At -c "SELECT count(*) FROM public.${table_name};")"
  [[ "$cnt" == "$expected" ]]
}

log_contains() {
  local logfile="$1"
  local pattern="$2"
  grep -q "$pattern" "$logfile"
}

