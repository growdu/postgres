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

# Trigger missing-table pause first.
pub_psql -c "INSERT INTO public.miss_t VALUES (1);"
wait_until 20 "missing-table pause warning emitted" log_contains "$SUB_DATA/server.log" "pausing replicated apply for missing target relation"

# Baseline: other table still replicates.
pub_psql -c "INSERT INTO public.ok_t VALUES (10);"
wait_until 40 "ok_t replicated" table_row_count_is "ok_t" "1"

# Create missing table on subscriber, then trigger RELATION refresh from publisher.
sub_psql -c "CREATE TABLE public.miss_t(id int primary key);"
pub_psql -c "ALTER PUBLICATION mr_pub SET (publish='insert,update,delete');"

# Next DML on miss_t should apply after relation metadata refresh.
pub_psql -c "INSERT INTO public.miss_t VALUES (2);"
wait_until 40 "miss_t resumed after relation refresh" table_row_count_is "miss_t" "1"

echo "[PASS] case_03_resume_on_relation_refresh"

