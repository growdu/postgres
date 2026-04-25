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

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE PUBLICATION mr_dup_schema_pub FOR ALL TABLES WITH (ddl='schema');"

sub_psql -c "CREATE SUBSCRIPTION mr_dup_schema_sub1 CONNECTION '$PUB_CONNINFO' PUBLICATION mr_dup_schema_pub WITH (copy_data=false, create_slot=true, slot_name='mr_dup_schema_slot1', ddl='schema');"
sub_psql -c "CREATE SUBSCRIPTION mr_dup_schema_sub2 CONNECTION '$PUB_CONNINFO' PUBLICATION mr_dup_schema_pub WITH (copy_data=false, create_slot=true, slot_name='mr_dup_schema_slot2', ddl='schema');"

wait_until 40 "subscription worker mr_dup_schema_sub1 start" subscription_has_pid "mr_dup_schema_sub1"
wait_until 40 "subscription worker mr_dup_schema_sub2 start" subscription_has_pid "mr_dup_schema_sub2"

pub_psql -c "CREATE SCHEMA mr_dup_schema_s;"

wait_until 40 "schema replicated to subscriber" schema_exists "mr_dup_schema_s"
wait_until 20 "subscription worker mr_dup_schema_sub1 alive after duplicate schema ddl" subscription_has_pid "mr_dup_schema_sub1"
wait_until 20 "subscription worker mr_dup_schema_sub2 alive after duplicate schema ddl" subscription_has_pid "mr_dup_schema_sub2"

if grep -q 'ERROR:  schema "mr_dup_schema_s" already exists' "$SUB_DATA/server.log"; then
  echo "[FAIL] duplicate schema error should be ignored for follower subscription worker" >&2
  exit 1
fi

echo "[PASS] case_08_multi_sub_schema_duplicate"
