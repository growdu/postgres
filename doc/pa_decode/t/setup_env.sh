#!/bin/bash
# Functional test harness for the parallel logical decoding feature.
# Sets up a publisher cluster and a subscriber cluster under /tmp/pdtest.
set -euo pipefail

PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}

export PATH="$PGBIN:$PATH"

stop_all() {
  for d in "$ROOT/pub" "$ROOT/sub"; do
    if [ -f "$d/postmaster.pid" ]; then
      "$PGBIN/pg_ctl" -D "$d" -m immediate stop >/dev/null 2>&1 || true
    fi
  done
}

init_one() {
  local d=$1 port=$2
  rm -rf "$d"
  "$PGBIN/initdb" -D "$d" -U postgres --no-sync -E UTF8 >/dev/null
  cat >> "$d/postgresql.conf" <<EOF
port = $port
listen_addresses = 'localhost'
unix_socket_directories = '$ROOT'
wal_level = logical
max_worker_processes = 32
max_replication_slots = 16
max_wal_senders = 16
max_logical_replication_workers = 16
max_parallel_apply_workers_per_subscription = 4
max_parallel_workers = 16
max_parallel_decode_workers = ${MAX_PARALLEL_DECODE_WORKERS:-0}
fsync = off
synchronous_commit = off
full_page_writes = off
EOF
}

start_one() {
  local d=$1
  "$PGBIN/pg_ctl" -D "$d" -l "$d/server.log" -w start >/dev/null
}

case "${1:-all}" in
  init)
    mkdir -p "$ROOT"
    stop_all
    init_one "$ROOT/pub" "$PORT_PUB"
    init_one "$ROOT/sub" "$PORT_SUB"
    ;;
  start)
    start_one "$ROOT/pub"
    start_one "$ROOT/sub"
    ;;
  stop)
    stop_all
    ;;
  psql)
    shift
    "$PGBIN/psql" -h "$ROOT" -p "$PORT_PUB" -U postgres "$@"
    ;;
  all)
    "$0" init
    "$0" start
    ;;
esac
