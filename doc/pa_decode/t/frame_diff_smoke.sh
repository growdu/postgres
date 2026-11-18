#!/bin/bash
# Compare the wire bytes produced by the serial decoding path against the
# bytes produced by the publisher-side parallel decoding path.
#
# The two runs have to decode exactly the same WAL, otherwise the comparison
# says nothing.  Two replication slots are therefore created in one
# transaction, before the workload is written, so that they share the same
# restart_lsn and both start before the workload.  Each run consumes one of
# the two slots, so both runs decode the same transactions.  The publisher
# records every replication frame it sends into a trace file (see the
# parallel_decode_trace_file GUC), and the frame level tool compares the two
# trace files.
#
# The comparison is allowed to differ in two ways, both intentional:
#
#   * the order of the frames, because the parallel path may send a stream
#     segment before the commit message of an older transaction;
#   * the frame header LSNs, because the parallel path clamps them to the
#     release frontier.
#
# Any other difference is a defect.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
ROWS=${ROWS:-200}

export PATH="$PGBIN:$PATH"

TRACE_SERIAL=$ROOT/serial.trace
TRACE_PARALLEL=$ROOT/parallel.trace
#
# The number of decode workers of the parallel run.  More than one worker is
# the interesting case: the transactions are then decoded by different
# workers, and the commit barrier has to put them back in order.
WORKERS=${WORKERS:-4}

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for_rows() {
	# wait_for_rows <expected>
	local expected=$1 i n
	for i in $(seq 1 60); do
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

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

# Both slots are created in one transaction, before the workload is written,
# so that they share the same restart_lsn and both start before the
# workload.  The two runs then decode exactly the same transactions.
echo "== creating two slots at the same position"
pub <<'EOF'
begin;
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput');
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_b', 'pgoutput');
commit;
EOF

lsn_a=$(pub -At -c "select restart_lsn from pg_replication_slots where slot_name = 'pd_slot_a'")
lsn_b=$(pub -At -c "select restart_lsn from pg_replication_slots where slot_name = 'pd_slot_b'")
echo "   pd_slot_a starts at $lsn_a, pd_slot_b starts at $lsn_b"
if [ "$lsn_a" != "$lsn_b" ]; then
	# Creating a slot writes a WAL record of its own, so the two slots do
	# not start at exactly the same position.  That is harmless as long as
	# no transaction is committed between the two creations, which is the
	# case here: both slots are created in one transaction, and the
	# workload is written afterwards.
	echo "   (the slots differ only by the slot creation records)"
fi

echo "== writing the workload"
pub -c "insert into pd_t select g, 'v' || g from generate_series(1, $ROWS) g"

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
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE_SERIAL" >"$ROOT/serial.dump" || true
head -12 "$ROOT/serial.dump"
echo
python3 "$SCRIPT_DIR/pd_frame_tool.py" diff "$TRACE_SERIAL" "$TRACE_PARALLEL" --messages \
	|| fail "the parallel path does not produce the same bytes as the serial path"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
