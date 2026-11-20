#!/bin/bash
# Verify what the publisher does when the frames that wait for their turn pile
# up in its memory.
#
# The publisher holds the frames of a transaction back until the transaction has
# its turn, so that the transactions reach the subscriber in the order they were
# committed in.  The frames of the transactions that follow a transaction that
# has not been released yet wait in the memory of the publisher.  Two things
# have to hold:
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
# The workload is two transactions that overlap:
#
#   1. a big one, which takes long to decode.  Its worker holds the release
#      frontier back: the transaction has ended, but the worker has not decoded
#      the end of it yet, so it is still waiting for its turn.
#
#   2. a smaller one, which starts while the big one is still being written,
#      so that it is decoded by another worker, and which writes its rows
#      after the big one has ended.  The frames of a streamed transaction have
#      no commit sequence number, so they wait for the release frontier: they
#      pile up in the pending area of the walsender while the big transaction
#      is waiting for its turn.
#
# The frames of the big transaction itself do not pile up.  They are before the
# end event of their own transaction, so they may go as soon as they arrive.
# That is why the size of the second transaction is what decides whether the
# publisher warns: it has to be bigger than parallel_decode_pending_max_bytes.
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
# The size of the big transaction, in rows and in bytes of payload per row.
# The transaction has to take longer than the barrier timeout below, otherwise
# the stall detection has no reason to fire and the test proves nothing.  The
# size is chosen so that the decoding of the transaction takes a few seconds.
ROWS=${ROWS:-1000000}
PAYLOAD=${PAYLOAD:-200}
#
# The size of the second transaction, in rows and in bytes of payload per row.
# Its frames are the ones that pile up in the pending area, so it has to be
# bigger than the limit of the pending area.
ROWS2=${ROWS2:-20000}
PAYLOAD2=${PAYLOAD2:-200}
#
# The number of seconds the publisher waits for a worker to make progress
# before it calls the session stuck.  The test uses a small number, so that the
# check is exercised without a test that runs for a minute.
BARRIER_TIMEOUT=${BARRIER_TIMEOUT:-2}
#
# The number of bytes the frames that wait for their turn may take before the
# publisher warns about them.
PENDING_MAX_BYTES=${PENDING_MAX_BYTES:-65536}
#
# The number of bytes the stream segments that wait for their turn may take
# before the publisher warns about them.
HELD_MAX_BYTES=${HELD_MAX_BYTES:-65536}
#
# How long the test waits for the transactions to arrive.
WAIT=${WAIT:-300}
#
# How long the second transaction waits for the big one to end.  The wait is
# done in the session of the second transaction, and every statement of it
# takes a new snapshot, so the transaction sees the rows of the big one as
# soon as they are committed.
WAIT_STEPS=${WAIT_STEPS:-600}
WAIT_STEP=${WAIT_STEP:-0.1}

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
	echo "--- the workers of the publisher" >&2
	pub -c "select worker_index, status, in_flight_txns, decoded_txns,
				   in_flight_bytes, queue_depth
			from pg_parallel_decode_workers order by worker_index" >&2 || true
}

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
cat >> "$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_barrier_timeout = $BARRIER_TIMEOUT
parallel_decode_pending_max_bytes = $PENDING_MAX_BYTES
parallel_decode_held_max_bytes = $HELD_MAX_BYTES
EOF
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating the subscription (streaming = parallel, copy_data = false)"
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = 'parallel', copy_data = false)"

#
# The script of the second transaction.  It waits for the rows of the big
# transaction to show up, which means that the big transaction has ended, and
# writes its own rows after that.
#
b_script=$ROOT/b.sql
{
	echo "begin;"
	echo "insert into pd_t select g + $ROWS, repeat('z', 10)"
	echo "	from generate_series(1, 10) g;"
	i=0
	while [ "$i" -lt "$WAIT_STEPS" ]; do
		i=$((i + 1))
		echo "select pg_sleep($WAIT_STEP) from (select 1) s"
		echo "	where not exists (select 1 from pd_t where id = $ROWS);"
	done
	echo "insert into pd_t select g + $ROWS + 10, repeat('y', $PAYLOAD2)"
	echo "	from generate_series(1, $ROWS2) g;"
	echo "commit;"
} > "$b_script"

echo "== writing the big transaction of $ROWS rows"
start=$(date +%s)
(
	pub -c "begin" \
		-c "insert into pd_t select g, repeat('x', $PAYLOAD)
			 from generate_series(1, $ROWS) g" \
		-c "commit"
) &
big_pid=$!

#
# The second transaction starts while the big one is still being written, so
# that it goes to another worker: a transaction goes to the worker that has the
# fewest bytes in flight, and the big transaction keeps its worker busy.
#
sleep 1
echo "== writing the second transaction of $ROWS2 rows"
(
	pub -f "$b_script"
) &
small_pid=$!

wait "$big_pid" || fail "the big transaction failed"
echo "   the big transaction is written"

wait "$small_pid" || fail "the second transaction failed"
echo "   the second transaction is written"

echo "== waiting for the transactions to arrive"
i=0
n=""
total=$((ROWS + 10 + ROWS2))
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	n=$(rowcount 2>/dev/null || echo "")
	[ "$n" = "$total" ] && break
done
elapsed=$(($(date +%s) - start))

if [ "$n" != "$total" ]; then
	echo "   after ${i}s the subscriber has $n rows instead of $total" >&2
	fail "the transactions did not arrive in ${WAIT}s"
fi
echo "   the transactions arrived after ${elapsed}s"

pub_sum=$(pub -At -c "select coalesce(sum(id),0) || ':' || count(*) || ':' || coalesce(sum(length(v)),0) from pd_t")
sub_sum=$(checksum)
[ "$pub_sum" = "$sub_sum" ] ||
	fail "the rows differ (the publisher has $pub_sum, the subscriber has $sub_sum)"

#
# The warning about the pending area.
#
if ! grep -q "bytes of frames that wait for their turn" "$ROOT/pub/server.log"; then
	fail "the publisher did not warn about the frames that wait for their turn"
fi
echo "   the publisher warned about the frames that wait for their turn"

#
# The frames that wait are the stream segments of the second transaction, so the
# publisher warns about them as well.
#
if ! grep -q "bytes of stream segments that wait for their turn" "$ROOT/pub/server.log"; then
	fail "the publisher did not warn about the stream segments that wait for their turn"
fi
echo "   the publisher warned about the stream segments that wait for their turn"

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
	fail "the transactions arrived after ${elapsed}s, which is not more than the \
barrier timeout of ${BARRIER_TIMEOUT}s: the workload is too small to exercise \
the stall detection"
fi
echo "   the transactions took longer than the barrier timeout of ${BARRIER_TIMEOUT}s"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
