#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE TABLE public.ok_t(id int primary key);"
pub_psql -c "CREATE TABLE public.miss_t(id int primary key);"
pub_psql -c "CREATE PUBLICATION mr_pub FOR ALL TABLES WITH (publish='insert', ddl='schema');"

sub_psql -c "CREATE TABLE public.ok_t(id int primary key);"

sub_psql -c "CREATE SUBSCRIPTION mr_sub CONNECTION '$PUB_CONNINFO' PUBLICATION mr_pub WITH (copy_data=false, create_slot=true, slot_name='mr_sub_slot', ddl='schema');"

wait_until 40 "subscription worker start" subscription_has_pid "mr_sub"

pub_psql -c "INSERT INTO public.miss_t VALUES (1);"
pub_psql -c "INSERT INTO public.ok_t VALUES (10);"

wait_until 40 "ok_t replicated while miss_t missing" table_row_count_is "ok_t" "1"
wait_until 20 "missing-table pause warning emitted" log_contains "$SUB_DATA/server.log" "pausing replicated apply for missing target relation"
wait_until 20 "subscription worker still alive" subscription_has_pid "mr_sub"

echo "[PASS] case_02_missing_table_isolation"

