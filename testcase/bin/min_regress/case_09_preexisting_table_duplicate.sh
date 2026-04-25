#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE SCHEMA mr_preexist;"
pub_psql -c "CREATE PUBLICATION mr_preexist_pub FOR ALL TABLES WITH (ddl='table');"

sub_psql -c "CREATE SCHEMA mr_preexist;"
sub_psql -c "CREATE TABLE mr_preexist.t_dup(id int primary key);"

sub_psql -c "CREATE SUBSCRIPTION mr_preexist_sub CONNECTION '$PUB_CONNINFO' PUBLICATION mr_preexist_pub WITH (copy_data=false, create_slot=true, slot_name='mr_preexist_slot', ddl='table');"

wait_until 40 "subscription worker mr_preexist_sub start" subscription_has_pid "mr_preexist_sub"

pub_psql -c "CREATE TABLE mr_preexist.t_dup(id int primary key);"

wait_until 20 "subscription worker mr_preexist_sub alive after duplicate table ddl" subscription_has_pid "mr_preexist_sub"

if grep -q 'ERROR:  relation "t_dup" already exists' "$SUB_DATA/server.log"; then
  echo "[FAIL] duplicate table error should be ignored when subscriber pre-creates table" >&2
  exit 1
fi

echo "[PASS] case_09_preexisting_table_duplicate"
