#!/bin/bash
# Compare the wire bytes of the serial and of the parallel decoding path when
# the workload arrives while the subscription is already streaming.
#
# The other tests write the whole workload before the subscription starts.
# That is the easy case: the decoder only has to catch up with the WAL that
# is already there.  This test writes the workload while the subscription is
# live, which is the case that matters: the decoder has to build its
# snapshot, find its starting point and decode the changes that arrive while
# it is doing so, and it must not lose any of them.
#
# The parallel run is the live one, because the parallel path has no
# equivalent of DecodingContextFindStartpoint(): the workers build their
# snapshot from the records the walreader hands over.  The serial run
# decodes the same transactions afterwards, from the other slot, so that the
# two traces hold the same messages.
#
# The comparison is done the same way as in frame_diff_smoke.sh: two slots are
# created in one transaction, before the workload is written, so that both of
# them decode exactly the same transactions.
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

# The number of concurrent sessions and the number of transactions per session.
SESSIONS=${SESSIONS:-6}
TXNS=${TXNS:-8}

#
# The number of decode workers of the parallel run.  More than one worker is
# the interesting case: the transactions are then decoded by different
# workers, and the commit barrier has to put them back in order.
WORKERS=${WORKERS:-4}

TRACE_SERIAL=$ROOT/serial.trace
TRACE_PARALLEL=$ROOT/parallel.trace

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for_rows() {
	# wait_for_rows <expected>
	local expected=$1 i n
	for i in $(seq 1 120); do
		n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
		if [ "$n" = "$expected" ]; then
			return 0
		fi
		sleep 1
	done
	fail "the subscriber did not catch up (expected $expected rows)"
}

set_trace() {
	# set_trace <file> <max_parallel_decode_workers>
	cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$1'
max_parallel_decode_workers = $2
EOF
	pg_ctl -D "$ROOT/pub" reload >/dev/null
}

# The workload: several sessions that run transactions at the same time.  The
# transactions of a session sleep between their statements, so that the
# transactions of the sessions interleave.
workload() {
	local session=$1 i
	for i in $(seq 1 "$TXNS"); do
		psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q <<EOF
begin;
insert into pd_t values ($session * 1000 + $i, 'v$session-$i');
select pg_sleep(0.05);
insert into pd_t values ($session * 1000 + $i + 500, 'v$session-$i-b');
commit;
EOF
	done
}

run_workload() {
	local pids=() i

	for i in $(seq 1 "$SESSIONS"); do
		workload "$i" &
		pids+=($!)
	done

	for i in "${pids[@]}"; do
		wait "$i"
	done
}

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating two slots at the same position"
pub <<'EOF'
begin;
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput');
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_b', 'pgoutput');
commit;
EOF

ROWS=$((SESSIONS * TXNS * 2))
echo "   $ROWS rows written"

sub -c "create table pd_t (id int primary key, v text)"

# ---------------------------------------------------------------------------
# Run 1: the parallel decoding path, with the workload arriving while the
# subscription is streaming.
# ---------------------------------------------------------------------------
echo "== run 1: parallel decoding path ($WORKERS worker(s)), live workload"
rm -f "$TRACE_PARALLEL"
set_trace "$TRACE_PARALLEL" "$WORKERS"
sub -c "create subscription pd_slot_b connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"

# Give the walsender the time to start the session and to build the
# snapshot, so that the workload really arrives while it is streaming.
sleep 3

echo "== writing the workload"
run_workload

wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_b"
sleep 2
[ -s "$TRACE_PARALLEL" ] || fail "the parallel run produced no trace file"

parallel_sum=$(sub -At -c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from pd_t")
pub_sum=$(pub -At -c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from pd_t")
if [ "$parallel_sum" != "$pub_sum" ]; then
	echo "   publisher: $pub_sum"
	echo "   subscriber: $parallel_sum"
	fail "the subscriber does not have the same data as the publisher"
fi
echo "   the data of the subscriber matches: $parallel_sum"

# ---------------------------------------------------------------------------
# Run 2: the serial decoding path, which decodes the same transactions from
# the other slot.
# ---------------------------------------------------------------------------
echo "== run 2: serial decoding path, same transactions"
rm -f "$TRACE_SERIAL"
sub -c "truncate pd_t"
set_trace "$TRACE_SERIAL" 0
sub -c "create subscription pd_slot_a connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"
wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_a"
sleep 2
[ -s "$TRACE_SERIAL" ] || fail "the serial run produced no trace file"

# ---------------------------------------------------------------------------
# Compare the two traces.
# ---------------------------------------------------------------------------
echo "== comparing the traces"
python3 "$SCRIPT_DIR/pd_frame_tool.py" diff "$TRACE_SERIAL" "$TRACE_PARALLEL" --messages \
	|| fail "the parallel path does not produce the same bytes as the serial path"

serial_sum=$(sub -At -c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from pd_t")
if [ "$serial_sum" != "$pub_sum" ]; then
	echo "   publisher: $pub_sum"
	echo "   subscriber: $serial_sum"
	fail "the subscriber does not have the same data as the publisher"
fi
echo "   the data of the subscriber matches: $serial_sum"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
