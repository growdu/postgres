#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

table_exists() {
  local fq_name="$1"
  local reg
  reg="$(sub_psql -At -c "SELECT to_regclass('${fq_name}')::text;")"
  [[ "$reg" == "$fq_name" ]]
}

schema_table_row_count_is() {
  local schema_name="$1"
  local table_name="$2"
  local expected="$3"
  local cnt
  cnt="$(sub_psql -At -c "SELECT count(*) FROM ${schema_name}.${table_name};")"
  [[ "$cnt" == "$expected" ]]
}

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE SCHEMA s1;"
pub_psql -c "CREATE TABLE s1.t_pause(id int primary key);"
pub_psql -c "CREATE PUBLICATION mr_q_pub FOR ALL TABLES WITH (publish='insert', ddl='schema');"

sub_psql -c "CREATE SUBSCRIPTION mr_q_sub CONNECTION '$PUB_CONNINFO' PUBLICATION mr_q_pub WITH (copy_data=false, create_slot=true, slot_name='mr_q_slot', ddl='schema');"

wait_until 40 "subscription worker start" subscription_has_pid "mr_q_sub"

# Table DDL is not synchronized in ddl='schema' mode, so this relation pauses.
pub_psql -c "INSERT INTO s1.t_pause VALUES (1);"
wait_until 20 "missing-table pause warning emitted" log_contains "$SUB_DATA/server.log" "pausing replicated apply for missing target relation"
wait_until 20 "subscription worker still alive after pause" subscription_has_pid "mr_q_sub"

pub_psql -c "ALTER PUBLICATION mr_q_pub SET (ddl='table');"
sub_psql -c "ALTER SUBSCRIPTION mr_q_sub SET (ddl='table');"

# Prepare subscriber prerequisites and force a fresh CREATE TABLE Q message.
sub_psql -c "CREATE SCHEMA IF NOT EXISTS s1;"
pub_psql -c "DROP TABLE s1.t_pause;"
pub_psql -c "CREATE TABLE s1.t_pause(id int primary key);"

wait_until 40 "Q create table replicated to subscriber" table_exists "s1.t_pause"

pub_psql -c "INSERT INTO s1.t_pause VALUES (2);"
wait_until 40 "dml resumes after Q create table" schema_table_row_count_is "s1" "t_pause" "1"

echo "[PASS] case_06_resume_on_q_create"
