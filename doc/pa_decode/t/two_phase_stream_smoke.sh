#!/bin/bash
# Smoke test for the two-phase commit on the publisher-side parallel decoding
# path, for the prepared transactions that the parallel path streams.
#
# The parallel decoding path streams every transaction while it is in progress,
# so a prepared transaction with enough changes is streamed before its prepare
# record arrives.  The subscriber then sees the stream segments of the
# transaction and a stream prepare message, where the serial path sends the
# whole transaction with a begin prepare and a prepare message.
#
# The test runs the same workload twice, once with the parallel path and once
# with the serial path, and checks that
#
#   * the subscriber applies the same rows in both runs;
#   * the publisher sends a stream prepare message for the prepared
#     transaction that it streamed, and a begin prepare and a prepare message
#     for the one that it did not;
#   * the commit prepared and the rollback prepared arrive in both runs;
#   * the changes of the transactions that were aborted do not reach the
#     subscriber.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest_2p_stream}
PORT_PUB=${PORT_PUB:-39401}
PORT_SUB=${PORT_SUB:-55401}
WORKERS=${WORKERS:-4}

#
# The harness has to use the same ports and the same root, so they have to be
# in the environment of the harness.
#
export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/two_phase_stream.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

# The number of changes of the transactions of the workload.  The first one is
# bigger than the number of changes of a stream segment (the default of
# parallel_decode_stream_segment_changes is 8), so that the parallel path
# streams it, and the second one is smaller, so that it does not.
ROWS=${ROWS:-20}
ROWS_SMALL=${ROWS_SMALL:-3}

#
# The workload of one run: the same transactions, in the same order, in both
# runs.
#
workload() {
	pub -c "begin" \
		-c "insert into pd_t select g, repeat('a', 200) from generate_series(1, $ROWS) g" \
		-c "commit"

	# A prepared transaction that the parallel path streams.
	pub -c "begin" \
		-c "insert into pd_t select g + 1000, repeat('b', 200) from generate_series(1, $ROWS) g" \
		-c "prepare transaction 'pd_gid_streamed'"

	# A prepared transaction that the parallel path does not stream.
	pub -c "begin" \
		-c "insert into pd_t select g + 2000, repeat('c', 200) from generate_series(1, $ROWS_SMALL) g" \
		-c "prepare transaction 'pd_gid_plain'"

	# A transaction that is rolled back: its changes never reach the
	# subscriber.
	pub -c "begin" \
		-c "insert into pd_t select g + 3000, repeat('d', 200) from generate_series(1, $ROWS) g" \
		-c "rollback"

	# A prepared transaction that is rolled back as well.
	pub -c "begin" \
		-c "insert into pd_t select g + 4000, repeat('e', 200) from generate_series(1, $ROWS) g" \
		-c "prepare transaction 'pd_gid_rolled_back'"

	pub -c "commit prepared 'pd_gid_streamed'"
	pub -c "commit prepared 'pd_gid_plain'"
	pub -c "rollback prepared 'pd_gid_rolled_back'"
}

# run_workload <workers> <output file> <trace file>
#
# Sets the clusters up, writes the workload and copies the rows of the
# subscriber into the given file.  The trace of the run is copied into the
# given trace file, because the next run overwrites the trace of the
# publisher.
run_workload() {
	local workers=$1 out=$2 rawtrace=$3

	echo "-- setting up the clusters ($workers worker(s))"
	MAX_PARALLEL_DECODE_WORKERS=$workers "$SCRIPT_DIR/setup_env.sh" init

	cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $workers
max_prepared_transactions = 10
parallel_decode_trace_file = '$TRACE'
EOF
	#
	# The subscriber holds the prepared transaction while it waits for the
	# commit, so it needs the prepared transactions as well.
	#
	cat >>"$ROOT/sub/postgresql.conf" <<EOF
max_prepared_transactions = 10
EOF

	"$SCRIPT_DIR/setup_env.sh" start
	rm -f "$TRACE"

	echo "-- creating the schema"
	pub -c "create table pd_t (id int primary key, v text)"
	pub -c "create publication pd_pub for all tables"
	sub -c "create table pd_t (id int primary key, v text)"

	echo "-- creating the subscription"
	sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = 'parallel', copy_data = false, two_phase = true)"

	echo "-- writing the workload"
	workload

	# wait for the subscriber to catch up
	#
	# The rows of the transactions that were rolled back never reach the
	# subscriber: the changes of an aborted transaction are discarded when the
	# abort arrives.
	#
	local i n expected=$((2 * ROWS + ROWS_SMALL))
	for i in $(seq 1 120); do
		n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
		if [ "$n" = "$expected" ]; then
			break
		fi
		sleep 1
	done
	[ "$n" = "$expected" ] ||
		fail "the subscriber has $n rows instead of $expected"

	sub -At -c "select id, v from pd_t order by id" >"$out"

	# keep the trace of this run, the next run overwrites it
	[ -s "$TRACE" ] || fail "the publisher produced no trace file"
	cp "$TRACE" "$rawtrace"
	python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$rawtrace" --messages \
		>"$out.trace"

	"$SCRIPT_DIR/setup_env.sh" stop
}

echo "== the parallel path"
run_workload "$WORKERS" "$ROOT/parallel.rows" "$ROOT/parallel.trace"

echo "== the serial path"
run_workload 0 "$ROOT/serial.rows" "$ROOT/serial.trace"

echo "== comparing the rows"
if ! diff -u "$ROOT/serial.rows" "$ROOT/parallel.rows" >"$ROOT/two_phase.diff"; then
	cat "$ROOT/two_phase.diff" >&2
	fail "the parallel path and the serial path produced different rows"
fi
echo "   the parallel path produced the same rows as the serial path"

#
# The rows of the transactions that were rolled back are not on the subscriber.
# The payloads of the committed transactions are a, b and c, the payloads of
# the rolled back ones are d and e.
#
for c in d e; do
	bad=$(grep -c "$c\$" "$ROOT/parallel.rows" || true)
	[ "$bad" = "0" ] || fail "$bad row(s) of a rolled back transaction are on the subscriber"
done
echo "   the rows of the rolled back transactions are not on the subscriber"

echo "== checking the tags of the parallel path"
nstreamprep=$(grep -c '^ *p ' "$ROOT/parallel.rows.trace" || true)
nprep=$(grep -c '^ *P ' "$ROOT/parallel.rows.trace" || true)
nbegprep=$(grep -c '^ *b ' "$ROOT/parallel.rows.trace" || true)
ncommitprep=$(grep -c '^ *K ' "$ROOT/parallel.rows.trace" || true)
nrollbackprep=$(grep -c '^ *r ' "$ROOT/parallel.rows.trace" || true)
nabort=$(grep -c '^ *A ' "$ROOT/parallel.rows.trace" || true)
echo "   stream prepares: $nstreamprep, prepares: $nprep, begin prepares: $nbegprep"
echo "   commit prepareds: $ncommitprep, rollback prepareds: $nrollbackprep, stream aborts: $nabort"

[ "$nstreamprep" = "2" ] ||
	fail "the parallel path sent $nstreamprep stream prepare message(s) instead of two"
[ "$nprep" = "1" ] ||
	fail "the parallel path sent $nprep prepare message(s) instead of one"
[ "$nbegprep" = "1" ] ||
	fail "the parallel path sent $nbegprep begin prepare message(s) instead of one"
[ "$ncommitprep" = "2" ] ||
	fail "the parallel path sent $ncommitprep commit prepared message(s) instead of two"
[ "$nrollbackprep" = "1" ] ||
	fail "the parallel path sent $nrollbackprep rollback prepared message(s) instead of one"
[ "$nabort" = "1" ] ||
	fail "the parallel path sent $nabort stream abort message(s) instead of one"

echo "== comparing the traces"
if ! python3 "$SCRIPT_DIR/pd_frame_tool.py" diff \
	"$ROOT/serial.trace" "$ROOT/parallel.trace" >"$ROOT/two_phase.tracediff"; then
	cat "$ROOT/two_phase.tracediff" >&2
	fail "the traces of the two paths differ"
fi
cat "$ROOT/two_phase.tracediff"
echo "   the traces of the two paths match"

echo "== OK"
