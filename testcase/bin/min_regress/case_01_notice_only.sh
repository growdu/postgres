#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

"$BIN_DIR/full_init.sh" >/dev/null

set +e
out_all="$(pub_psql -c "CREATE PUBLICATION mr_notice_all FOR ALL TABLES WITH (ddl='schema');" 2>&1)"
rc_all=$?
set -e
if ((rc_all != 0)); then
  echo "[FAIL] create publication mr_notice_all failed" >&2
  echo "$out_all" >&2
  exit 1
fi

if ! grep -q 'enables ddl without "table" in FOR ALL TABLES scope' <<<"$out_all"; then
  echo "[FAIL] missing notice for FOR ALL TABLES + ddl='schema'" >&2
  echo "$out_all" >&2
  exit 1
fi

set +e
out_schema="$(pub_psql -c "CREATE PUBLICATION mr_notice_schema FOR TABLES IN SCHEMA public WITH (ddl='schema');" 2>&1)"
rc_schema=$?
set -e
if ((rc_schema != 0)); then
  echo "[FAIL] create publication mr_notice_schema failed" >&2
  echo "$out_schema" >&2
  exit 1
fi

if ! grep -Eq 'enables ddl without "table" in FOR TABLES IN SCHEMA scope|unsupported ddl options for current FOR scope' <<<"$out_schema"; then
  echo "[FAIL] missing scope warning for FOR TABLES IN SCHEMA + ddl='schema'" >&2
  echo "$out_schema" >&2
  exit 1
fi

echo "[PASS] case_01_notice_only"
