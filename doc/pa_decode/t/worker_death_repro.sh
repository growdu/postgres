#!/bin/bash
# Try to reproduce "the parallel decode worker died unexpectedly".
#
# The scenario: the subscription is dropped while the publisher is still
# streaming.  The apply worker is killed, the replication connection is
# closed, and the walsender of the publisher has to wind the session down.
# The workers are told to stop by the postmaster, because the walsender that
# started them is going away.  The walsender has to notice that the workers
# are leaving and end the session in an orderly fashion, instead of
# reporting the death of a worker that was asked to stop.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
#
# The harness has to use the same ports and the same root, so they have to be
# in the environment of the harness.
export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB

WORKERS=${WORKERS:-4}

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init >/dev/null
"$SCRIPT_DIR/setup_env.sh" start >/dev/null

pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
pub -c "select slot_name from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput')" >/dev/null
sub -c "create table pd_t (id int primary key, v text)"

cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
EOF
"$PGBIN/pg_ctl" -D "$ROOT/pub" reload >/dev/null

echo "== starting the parallel subscription"
sub -c "create subscription pd_slot_a connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"

sleep 2

echo "== dropping the subscription while the publisher is streaming"
sub -c "drop subscription pd_slot_a" || true

sleep 3

echo "== the publisher log"
grep -n "错误\|致命\|parallel decode" "$ROOT/pub/server.log" || true

echo "== the subscriber log"
grep -n "错误\|致命\|警告" "$ROOT/sub/server.log" || true

"$SCRIPT_DIR/setup_env.sh" stop >/dev/null
