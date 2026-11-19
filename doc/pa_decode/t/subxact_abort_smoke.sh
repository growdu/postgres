#!/bin/bash
# Smoke test for the abort of a subtransaction (AC-14) in the publisher-side
# parallel decoding path.
#
# A transaction that is rolled back to a savepoint has its subtransaction
# aborted.  When the changes of the subtransaction have already been streamed
# to the subscriber, the publisher has to tell the subscriber that they are
# void, so that the spool of the subscriber is truncated and the changes that
# came after them are not discarded with them.
#
# The test runs the same workload twice, once with the parallel path and once
# with the serial path, and compares the result on the subscriber.  The two
# runs have to produce the same rows.
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

# The size of a stream segment.  It is small on purpose, so that the changes
# of the subtransaction are streamed before it is aborted.
SEGMENT_SIZE=${SEGMENT_SIZE:-65536}

# The number of the rows of the three parts of the transaction.
ROWS_BEFORE=${ROWS_BEFORE:-2000}
ROWS_ABORTED=${ROWS_ABORTED:-800}
ROWS_AFTER=${ROWS_AFTER:-500}

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/subxact.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

# run_workload <workers> <output file>
#
# Sets the clusters up, writes the workload and copies the rows of the
# subscriber into the given file.
run_workload() {
	local workers=$1 out=$2

	echo "-- setting up the clusters ($workers worker(s))"
	MAX_PARALLEL_DECODE_WORKERS=$workers "$SCRIPT_DIR/setup_env.sh" init
	"$SCRIPT_DIR/setup_env.sh" start

	cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $workers
parallel_decode_trace_file = '$TRACE'
parallel_decode_stream_segment_size = $SEGMENT_SIZE
parallel_decode_stream_min_size = 0
parallel_decode_streaming_mode = 'auto'
EOF
	pg_ctl -D "$ROOT/pub" reload >/dev/null
	rm -f "$TRACE"

	echo "-- creating the schema"
	pub -c "create table pd_t (id int primary key, v text)"
	pub -c "create publication pd_pub for all tables"
	sub -c "create table pd_t (id int primary key, v text)"

	echo "-- creating the subscription"
	sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

	echo "-- writing the workload"
	#
	# The first part is streamed out before the savepoint, the second part is
	# the subtransaction that is rolled back, the third part comes after it
	# and has to survive the rollback.
	pub <<EOF
begin;
insert into pd_t select g, repeat('a', 200) from generate_series(1, $ROWS_BEFORE) g;
savepoint sp1;
insert into pd_t select g, repeat('b', 200) from generate_series($((ROWS_BEFORE + 1)), $((ROWS_BEFORE + ROWS_ABORTED))) g;
rollback to savepoint sp1;
insert into pd_t select g, repeat('c', 200) from generate_series($((ROWS_BEFORE + ROWS_ABORTED + 1)), $((ROWS_BEFORE + ROWS_ABORTED + ROWS_AFTER))) g;
commit;
EOF

	# wait for the subscriber to catch up
	local i n expected=$((ROWS_BEFORE + ROWS_AFTER))
	for i in $(seq 1 120); do
		n=$(psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -q \
			-c "select count(*) from pd_t" 2>/dev/null || echo 0)
		if [ "$n" = "$expected" ]; then
			break
		fi
		sleep 1
	done
	[ "$n" = "$expected" ] ||
		fail "the subscriber has $n rows instead of $expected"

	psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -q \
		-c "select id, v from pd_t order by id" >"$out"

	# keep the trace of this run, the next run overwrites it
	if [ -s "$TRACE" ]; then
		python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages \
			>"$out.trace"
	fi

	"$SCRIPT_DIR/setup_env.sh" stop
}

echo "== the parallel path"
run_workload "$WORKERS" "$ROOT/parallel.rows"

echo "== the serial path"
run_workload 0 "$ROOT/serial.rows"

echo "== comparing the results"
if ! diff -u "$ROOT/serial.rows" "$ROOT/parallel.rows" >"$ROOT/subxact.diff"; then
	cat "$ROOT/subxact.diff" >&2
	fail "the parallel path and the serial path produced different rows"
fi
echo "   the parallel path produced the same rows as the serial path"

echo "== checking that the rows of the aborted subtransaction are not there"
bad=$(grep -c 'b$' "$ROOT/parallel.rows" || true)
[ "$bad" = "0" ] || fail "$bad row(s) of the aborted subtransaction are on the subscriber"

echo "== checking the trace"
[ -s "$ROOT/parallel.rows.trace" ] || fail "the publisher produced no trace file"
nabort=$(grep -c ' A ' "$ROOT/parallel.rows.trace" || true)
echo "   the publisher sent $nabort stream abort message(s)"

echo "== OK"
