#!/bin/bash
# The teardown of a parallel decoding session must be quiet.
#
# The scenario: the walsender of the publisher goes away while the decode
# workers are streaming.  The walsender takes the queues it reads from with
# it, so a worker that hands over a frame after that runs into a detached
# queue.  That is the normal way for a worker to learn that the session is
# over, so it must not be reported as an error.
#
# The check is on the log of the publisher: there must be no error about a
# detached queue, no other error at all, and the participants must have
# stopped.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB

WORKERS=${WORKERS:-4}
export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init >/dev/null
"$SCRIPT_DIR/setup_env.sh" start >/dev/null

pub -c "create table pd_t (id int primary key, v text)"
pub -c "create table pd_load (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
pub -c "select slot_name from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput')" >/dev/null
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create table pd_load (id int primary key, v text)"

cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
EOF
"$PGBIN/pg_ctl" -D "$ROOT/pub" reload >/dev/null

echo "== starting the parallel subscription"
sub -c "create subscription pd_slot_a connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'parallel', copy_data = false)"

sleep 2

echo "== loading the publisher"
(
  while true; do
    pub -c "insert into pd_load select g, repeat('x', 500) from generate_series(1, 200) g on conflict (id) do update set v = excluded.v" >/dev/null 2>&1 || true
  done
) &
LOAD_PID=$!
trap 'kill $LOAD_PID 2>/dev/null || true' EXIT

sleep 3

echo "== the participants of the session"
grep -c "parallel decode worker" "$ROOT/pub/server.log" || true

WALSENDER_PID=$(pub -t -A -c "select pid from pg_stat_activity where backend_type = 'walsender' limit 1")
echo "== stopping the walsender $WALSENDER_PID"
kill -STOP "$WALSENDER_PID"

# The walsender is not reading the frames any more, so the workers fill the
# queues and wait for room.  That is the state in which a worker runs into the
# end of the queue: it is in the middle of a transfer when the walsender goes
# away.
sleep 3

echo "== killing the walsender"
kill -CONT "$WALSENDER_PID"
kill -TERM "$WALSENDER_PID"

sleep 3

kill $LOAD_PID 2>/dev/null || true

echo "== the publisher log"
grep -n "错误\|致命\|detached\|parallel decode" "$ROOT/pub/server.log" || true

fail=0

if grep -q "could not send a parallel decode" "$ROOT/pub/server.log"; then
	echo "FAIL: the publisher reported a failed send while the session was ending"
	fail=1
fi

if grep -q "detached" "$ROOT/pub/server.log"; then
	echo "FAIL: the publisher reported a detached queue"
	fail=1
fi

if grep "错误" "$ROOT/pub/server.log" | grep -qi "parallel decode\|detached"; then
	echo "FAIL: the publisher logged an error about the parallel decoding"
	fail=1
fi

if grep "退出代码" "$ROOT/pub/server.log" | grep -qv "退出代码 0"; then
	echo "FAIL: a participant of the session exited with a code that is not zero"
	fail=1
fi

if ! grep -q "parallel decode walreader stopped" "$ROOT/pub/server.log"; then
	echo "FAIL: the walreader did not stop"
	fail=1
fi

stopped=$(grep -c "parallel decode worker .* stopped" "$ROOT/pub/server.log" || true)
if [ "$stopped" -lt 1 ]; then
	echo "FAIL: the decode workers did not stop"
	fail=1
fi

"$SCRIPT_DIR/setup_env.sh" stop >/dev/null

if [ "$fail" -eq 0 ]; then
	echo "PASS: the teardown of the session was quiet"
	exit 0
fi

exit 1
