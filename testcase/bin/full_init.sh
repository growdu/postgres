#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

"$PG_BIN/pg_ctl" -D "$PUB_DATA" -m immediate stop >/dev/null 2>&1 || true
"$PG_BIN/pg_ctl" -D "$SUB_DATA" -m immediate stop >/dev/null 2>&1 || true
rm -rf "$PUB_DATA" "$SUB_DATA"

"$PG_BIN/initdb" -D "$PUB_DATA" -U "$PG_SUPERUSER" -A trust >/dev/null
"$PG_BIN/initdb" -D "$SUB_DATA" -U "$PG_SUPERUSER" -A trust >/dev/null

cat >>"$PUB_DATA/postgresql.conf" <<CFG
listen_addresses='*'
port=$PUB_PORT
wal_level=logical
max_replication_slots=64
max_wal_senders=64
max_logical_replication_workers=64
max_worker_processes=128
CFG

cat >>"$SUB_DATA/postgresql.conf" <<CFG
listen_addresses='*'
port=$SUB_PORT
max_logical_replication_workers=64
max_worker_processes=128
CFG

"$PG_BIN/pg_ctl" -D "$PUB_DATA" -l "$PUB_DATA/server.log" start >/dev/null
"$PG_BIN/pg_ctl" -D "$SUB_DATA" -l "$SUB_DATA/server.log" start >/dev/null

pub_admin -c "DROP DATABASE IF EXISTS $PUB_DB;"
pub_admin -c "CREATE DATABASE $PUB_DB;"
sub_admin -c "DROP DATABASE IF EXISTS $SUB_DB;"
sub_admin -c "CREATE DATABASE $SUB_DB;"

echo "[OK] full init done: pub=$PUB_PORT sub=$SUB_PORT"

