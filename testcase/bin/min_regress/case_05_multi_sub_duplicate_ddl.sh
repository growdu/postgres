#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

schema_exists() {
  local schema_name="$1"
  local cnt
  cnt="$(sub_psql -At -c "SELECT count(*) FROM pg_namespace WHERE nspname='${schema_name}';")"
  [[ "$cnt" == "1" ]]
}

table_exists() {
  local fq_name="$1"
  local reg
  reg="$(sub_psql -At -c "SELECT to_regclass('${fq_name}')::text;")"
  [[ "$reg" == "$fq_name" ]]
}

index_exists() {
  local fq_name="$1"
  local reg
  reg="$(sub_psql -At -c "SELECT to_regclass('${fq_name}')::text;")"
  [[ "$reg" == "$fq_name" ]]
}

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE PUBLICATION mr_dup_pub FOR ALL TABLES WITH (ddl='schema,table,index');"

sub_psql -c "CREATE SUBSCRIPTION mr_dup_sub1 CONNECTION '$PUB_CONNINFO' PUBLICATION mr_dup_pub WITH (copy_data=false, create_slot=true, slot_name='mr_dup_slot1', ddl='schema,table,index');"
sub_psql -c "CREATE SUBSCRIPTION mr_dup_sub2 CONNECTION '$PUB_CONNINFO' PUBLICATION mr_dup_pub WITH (copy_data=false, create_slot=true, slot_name='mr_dup_slot2', ddl='schema,table,index');"

wait_until 40 "subscription worker mr_dup_sub1 start" subscription_has_pid "mr_dup_sub1"
wait_until 40 "subscription worker mr_dup_sub2 start" subscription_has_pid "mr_dup_sub2"

pub_psql -c "CREATE SCHEMA mr_dup;"
pub_psql -c "CREATE TABLE mr_dup.t1(id int primary key, v int);"
pub_psql -c "CREATE INDEX mr_dup_t1_v_idx ON mr_dup.t1(v);"

wait_until 40 "schema replicated" schema_exists "mr_dup"
wait_until 40 "table replicated" table_exists "mr_dup.t1"
wait_until 40 "index replicated" index_exists "mr_dup.mr_dup_t1_v_idx"
wait_until 30 "subscription worker mr_dup_sub1 alive after duplicate ddl" subscription_has_pid "mr_dup_sub1"
wait_until 30 "subscription worker mr_dup_sub2 alive after duplicate ddl" subscription_has_pid "mr_dup_sub2"

if grep -q 'ERROR:  schema "mr_dup" already exists' "$SUB_DATA/server.log"; then
  echo "[FAIL] duplicate schema error should be ignored for follower subscription worker" >&2
  exit 1
fi
if grep -q 'ERROR:  relation "t1" already exists' "$SUB_DATA/server.log"; then
  echo "[FAIL] duplicate table error should be ignored for follower subscription worker" >&2
  exit 1
fi
if grep -q 'ERROR:  relation "mr_dup_t1_v_idx" already exists' "$SUB_DATA/server.log"; then
  echo "[FAIL] duplicate index error should be ignored for follower subscription worker" >&2
  exit 1
fi

echo "[PASS] case_05_multi_sub_duplicate_ddl"
