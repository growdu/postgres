#!/bin/bash
# Verify what the publisher does with a transaction that takes long to decode.
#
# The frames of a transaction are held back until the transaction has its turn,
# so that the transactions reach the subscriber in the order they were
# committed in.  Two things have to hold while a big transaction waits:
#
#   1. the publisher counts the bytes of the frames that wait, and warns when
#      they take more than parallel_decode_pending_max_bytes.  The session
#      keeps running: the warning is about the memory the publisher uses, not
#      about a failure.
#
#   2. the stall detection must not end the session.  The worker that owns the
#      transaction at the head of the queue is still making progress, so the
#      session is not stuck, even when the decoding of the transaction takes
#      longer than parallel_decode_barrier_timeout and the publisher has
#      nothing more to send in the meantime.
#
# The subscriber does not ask for streaming, so the publisher sends the
# transaction as one block, and the block waits for its turn in the pending
# area of the walsender.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
#
# The number of decode workers of the publisher.
WORKERS=${WORKERS:-4}
#
# The size of the transaction, in rows and in bytes of payload per row.
ROWS=${ROWS:-200000}
PAYLOAD=${PAYLOAD:-200}
#
# The number of seconds the publisher waits for a worker to make progress
# before it calls the session stuck.  The test uses a small number, so that
# the check is exercised without a test that runs for a minute.
BARRIER_TIMEOUT=${BARRIER_TIMEOUT:-5}
#
# The number of bytes the frames that wait for their turn may take before the
# publisher warns about them.
PENDING_MAX_BYTES=${PENDING_MAX_BYTES:-1048576}
#
# How long the test waits for the transaction to arrive.
WAIT=${WAIT:-300}

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	dump_state
	exit 1
}

rowcount() {
	sub -At -c "select count(*) from pd_t"
}

checksum() {
	psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -c \
		"select coalesce(sum(id),0) || ':' || count(*) || ':' || coalesce(sum(length(v)),0)
		 from pd_t"
}

dump_state() {
	echo "--- the log of the publisher" >&2
	tail -40 "$ROOT/pub/server.log" >&2 || true
	echo "--- the state of the subscriber" >&2
	sub -c "select pid, backend_type, state, wait_event_type, wait_event
			from pg_stat_activity
			where backend_type like 'logical replication%'
			   or backend_type = 'walsender'
			order by backend_type, pid" >&2 || true
	echo "--- the state of the publisher" >&2
	pub -c "select pid, backend_type, state, wait_event_type, wait_event
			from pg_stat_activity
			where backend_type like '%logical decoding%'
			   or backend_type = 'walsender'
			   or application_name like 'pg_%'
			order by backend_type, pid" >&2 || true
	echo "--- the replication state on the publisher" >&2
	pub -c "select application_name, state, sent_lsn, write_lsn, flush_lsn
			from pg_stat_replication" >&2 || true
	echo "--- the background workers of the publisher" >&2
	ps -eo pid,stat,args | grep -E '[p]arallel decode (walreader|worker)' >&2 || true
}

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
cat >> "$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_barrier_timeout = $BARRIER_TIMEOUT
parallel_decode_pending_max_bytes = $PENDING_MAX_BYTES
EOF
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating the subscription (streaming = off, copy_data = false)"
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = 'off', copy_data = false)"

echo "== writing a transaction of $ROWS rows"
start=$(date +%s)
pub -c "insert into pd_t select g, repeat('x', $PAYLOAD) from generate_series(1, $ROWS) g"
echo "   the transaction is written"

echo "== waiting for the transaction to arrive"
i=0
n=""
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	n=$(rowcount 2>/dev/null || echo "")
	[ "$n" = "$ROWS" ] && break
done
elapsed=$(($(date +%s) - start))

if [ "$n" != "$ROWS" ]; then
	echo "   after ${i}s the subscriber has $n rows instead of $ROWS" >&2
	fail "the transaction did not arrive in ${WAIT}s"
fi
echo "   the transaction arrived after ${elapsed}s"

pub_sum=$(pub -At -c "select coalesce(sum(id),0) || ':' || count(*) || ':' || coalesce(sum(length(v)),0) from pd_t")
sub_sum=$(checksum)
[ "$pub_sum" = "$sub_sum" ] ||
	fail "the rows differ (the publisher has $pub_sum, the subscriber has $sub_sum)"

#
# The warning about the pending area.
#
if ! grep -q "the parallel decoding session holds" "$ROOT/pub/server.log"; then
	fail "the publisher did not warn about the frames that wait for their turn"
fi
echo "   the publisher warned about the frames that wait for their turn"

#
# The stall detection must not end the session while the worker that owns the
# transaction at the head of the queue is making progress.
#
if grep -q "the parallel decode session stalled" "$ROOT/pub/server.log"; then
	fail "the stall detection ended the session while a worker was making progress"
fi
echo "   the stall detection did not end the session"

#
# The check above is only meaningful when the transaction really took longer
# than the timeout: otherwise the stall detection had no reason to fire.
#
if [ "$elapsed" -le "$BARRIER_TIMEOUT" ]; then
	fail "the transaction arrived after ${elapsed}s, which is not more than the \
barrier timeout of ${BARRIER_TIMEOUT}s: the workload is too small to exercise \
the stall detection"
fi
echo "   the transaction took longer than the barrier timeout of ${BARRIER_TIMEOUT}s"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
