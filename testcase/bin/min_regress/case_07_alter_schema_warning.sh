#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE SCHEMA mr_alter_s;"
pub_psql -c "CREATE TABLE mr_alter_s.t1(id int primary key);"
pub_psql -c "CREATE PUBLICATION mr_schema_pub FOR TABLES IN SCHEMA mr_alter_s WITH (ddl='table');"

set +e
alter_out="$(pub_psql -c "ALTER SCHEMA mr_alter_s RENAME TO mr_alter_s2;" 2>&1)"
alter_rc=$?
set -e

if ((alter_rc != 0)); then
  echo "[FAIL] ALTER SCHEMA should succeed" >&2
  echo "$alter_out" >&2
  exit 1
fi

if ! grep -q 'ALTER SCHEMA on "mr_alter_s" is not synchronized by publication "mr_schema_pub" in current FOR scope' <<<"$alter_out"; then
  echo "[FAIL] missing ALTER SCHEMA scope warning for FOR TABLES IN SCHEMA publication" >&2
  echo "$alter_out" >&2
  exit 1
fi

echo "[PASS] case_07_alter_schema_warning"
