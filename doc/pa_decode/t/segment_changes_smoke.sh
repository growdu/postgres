#!/bin/bash
# Smoke test for the cutting of the stream segments by the number of changes.
#
# The publisher cuts the changes of a transaction into stream segments by the
# number of changes the transaction has collected (the
# parallel_decode_stream_segment_changes parameter).  The test checks that
#
#   * a transaction that collects more changes than the threshold is cut into
#     as many segments as the threshold says, and that every segment is a
#     proper stream segment (a STREAM START and a STREAM STOP each);
#   * a transaction that stays below the threshold is not cut: it is streamed
#     as a single segment when it commits;
#   * the subscriber receives all the rows of both transactions.
#
# The workload is two transactions: one that is bigger than the threshold and
# one that is smaller.
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
#
# The number of changes a transaction has to collect before it is cut into a
# segment of its own.
SEGMENT_CHANGES=${SEGMENT_CHANGES:-4}
#
# The number of rows of the big transaction.  It is a multiple of the
# threshold, so that the number of the segments is exact.
ROWS=${ROWS:-20}
#
# The number of rows of the small transaction.  It stays below the threshold,
# so the transaction is not cut.
ROWS_SMALL=${ROWS_SMALL:-2}

WORKERS=${WORKERS:-4}

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/segment_changes.trace

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

echo "== setting up the clusters"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
parallel_decode_trace_file = '$TRACE'
parallel_decode_stream_segment_changes = $SEGMENT_CHANGES
EOF
pg_ctl -D "$ROOT/pub" reload >/dev/null
rm -f "$TRACE"

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating a subscription that asks for the parallel streaming mode"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

echo "== writing a transaction of $ROWS rows (the threshold is $SEGMENT_CHANGES)"
pub -c "insert into pd_t select g, 'a' || g from generate_series(1, $ROWS) g"

echo "== writing a transaction of $ROWS_SMALL rows (below the threshold)"
pub -c "insert into pd_t select g, 'b' || g from generate_series($((ROWS + 1)), $((ROWS + ROWS_SMALL))) g"

wait_for_rows "$((ROWS + ROWS_SMALL))"

echo "== checking the content"
bad=$(sub -At -c "select count(*) from pd_t where v <> (case when id <= $ROWS then 'a' || id else 'b' || id end)")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking the trace"
[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages >"$ROOT/segment_changes.dump"

nstart=$(grep -c '^ *S ' "$ROOT/segment_changes.dump" || true)
nstop=$(grep -c '^ *E ' "$ROOT/segment_changes.dump" || true)
ncommit=$(grep -c '^ *c ' "$ROOT/segment_changes.dump" || true)
echo "   stream segments: $nstart start(s), $nstop stop(s), $ncommit commit(s)"

#
# The big transaction is cut into one segment per threshold, the small one is
# a segment of its own, so there are one more segment than the big
# transaction has thresholds.
#
expected_segments=$((ROWS / SEGMENT_CHANGES + 1))
[ "$nstart" = "$expected_segments" ] ||
	fail "the publisher produced $nstart stream segment(s) instead of $expected_segments"
[ "$nstop" = "$nstart" ] ||
	fail "the publisher produced $nstart stream start(s) and $nstop stop(s)"
#
# Both transactions are committed with a stream commit each.
#
[ "$ncommit" = "2" ] ||
	fail "the publisher produced $ncommit stream commit(s) instead of two"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
