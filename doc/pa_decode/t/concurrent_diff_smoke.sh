#!/bin/bash
# Compare the wire bytes of the serial and of the parallel decoding path under
# a workload of concurrent transactions.
#
# The workload makes several transactions run at the same time, so that the
# transactions are decoded by different workers and their commits interleave.
# The commit barrier has to restore the order in which the transactions were
# committed, so the frames of the parallel path have to be the same as the
# frames of the serial path.
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
		n=$(psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -q \
			-c "select count(*) from pd_t" 2>/dev/null || echo 0)
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

echo "== writing the concurrent workload"
run_workload

ROWS=$((SESSIONS * TXNS * 2))
echo "   $ROWS rows written"

sub -c "create table pd_t (id int primary key, v text)"

# ---------------------------------------------------------------------------
# Run 1: the serial decoding path.
# ---------------------------------------------------------------------------
echo "== run 1: serial decoding path"
rm -f "$TRACE_SERIAL"
set_trace "$TRACE_SERIAL" 0
sub -c "create subscription pd_slot_a connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"
wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_a"
sleep 2
[ -s "$TRACE_SERIAL" ] || fail "the serial run produced no trace file"

# ---------------------------------------------------------------------------
# Run 2: the publisher-side parallel decoding path.
# ---------------------------------------------------------------------------
echo "== run 2: parallel decoding path ($WORKERS worker(s))"
rm -f "$TRACE_PARALLEL"
sub -c "truncate pd_t"
set_trace "$TRACE_PARALLEL" "$WORKERS"
sub -c "create subscription pd_slot_b connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"
wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_b"
sleep 2
[ -s "$TRACE_PARALLEL" ] || fail "the parallel run produced no trace file"

# ---------------------------------------------------------------------------
# Compare the two traces.
# ---------------------------------------------------------------------------
echo "== comparing the traces"
python3 "$SCRIPT_DIR/pd_frame_tool.py" diff "$TRACE_SERIAL" "$TRACE_PARALLEL" --messages \
	|| fail "the parallel path does not produce the same bytes as the serial path"

# The data of the subscriber has to be the same as the data of the publisher.
pub_sum=$(pub -At -c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from pd_t")
sub_sum=$(sub -At -c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from pd_t")
if [ "$pub_sum" != "$sub_sum" ]; then
	echo "   publisher: $pub_sum"
	echo "   subscriber: $sub_sum"
	fail "the subscriber does not have the same data as the publisher"
fi
echo "   the data of the subscriber matches: $sub_sum"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
