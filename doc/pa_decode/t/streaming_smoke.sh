#!/bin/bash
# Smoke test for the early streaming of the publisher-side parallel decoding
# path (P6).
#
# The subscriber asks for the parallel streaming mode, so the publisher
# streams the changes of a transaction before the transaction is complete.
# The test checks that
#
#   * the changes of a large transaction reach the subscriber (the rows are
#     all there and they have the right content);
#   * the publisher really produced stream segments (the trace file holds
#     STREAM START and STREAM STOP messages);
#   * the frames of a segment reach the subscriber without the frames of
#     another transaction in between (the subscriber accepts only one stream
#     segment at a time, so an interleaved segment makes it fail);
#   * the session survives a restart of the publisher without losing
#     anything: the flush position of the slot may never pass the record
#     that ends a transaction that has not been delivered yet.
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

# The number of rows of the large transaction.  Every row is a few hundred
# bytes, so the transaction is a few megabytes, which is enough to make the
# publisher split it into several stream segments.
ROWS=${ROWS:-4000}

# The size of a stream segment.  It is small on purpose, so that the test
# does not have to write a huge transaction to see several segments.
SEGMENT_SIZE=${SEGMENT_SIZE:-65536}

WORKERS=${WORKERS:-4}

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/streaming.trace

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
parallel_decode_stream_segment_size = $SEGMENT_SIZE
parallel_decode_stream_min_size = 0
parallel_decode_streaming_mode = 'auto'
EOF
pg_ctl -D "$ROOT/pub" reload >/dev/null
rm -f "$TRACE"

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating a subscription that asks for the parallel streaming mode"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

echo "== writing a large transaction"
pub -c "insert into pd_t select g, repeat('x', 200) from generate_series(1, $ROWS) g"

wait_for_rows "$ROWS"

echo "== checking the content"
bad=$(sub -At -c "select count(*) from pd_t where v <> repeat('x', 200) or id not between 1 and $ROWS")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking the trace"
[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages >"$ROOT/streaming.dump"

nstart=$(grep -c '^ *S ' "$ROOT/streaming.dump" || true)
nstop=$(grep -c '^ *E ' "$ROOT/streaming.dump" || true)
echo "   stream segments: $nstart start(s), $nstop stop(s)"
[ "$nstart" -gt 0 ] || fail "the publisher produced no stream segment"
[ "$nstart" = "$nstop" ] || fail "the publisher produced $nstart stream start(s) and $nstop stop(s)"

# ---------------------------------------------------------------------------
# The session must survive a restart of the publisher without losing
# anything.  The flush position of the slot may never pass the record that
# ends a transaction that has not been delivered yet, or the transaction is
# skipped when the session comes back.
# ---------------------------------------------------------------------------
echo "== writing a second transaction and restarting the publisher"
pub -c "insert into pd_t select g, repeat('y', 200) from generate_series($((ROWS + 1)), $((ROWS * 2))) g"

# The publisher is stopped the hard way below, so the commit of the
# transaction has to be in the WAL file before that: with
# synchronous_commit = off the commit record may still sit in the WAL
# buffer when the process dies, and the transaction would look like it never
# committed.
pub -c "select pg_switch_wal()" >/dev/null

pg_ctl -D "$ROOT/pub" -m immediate stop >/dev/null 2>&1 || true
pg_ctl -D "$ROOT/pub" -l "$ROOT/pub/server.log" -w start >/dev/null

wait_for_rows "$((ROWS * 2))"

bad=$(sub -At -c "select count(*) from pd_t where (id <= $ROWS and v <> repeat('x', 200)) or (id > $ROWS and v <> repeat('y', 200))")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content after the restart"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
