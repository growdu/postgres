#!/bin/bash
# Smoke test for the streaming of every transaction in the parallel streaming
# mode.
#
# A transaction used to be streamed only when it had reached the size of a
# stream segment, so a workload of small transactions produced no stream
# segment at all: the subscriber received the transactions as whole blocks
# and its apply workers had nothing to do.
#
# The test checks that
#
#   * every committed transaction produces a stream segment in the parallel
#     streaming mode (one STREAM START and one STREAM STOP each), even when
#     the transaction is far below the size of a stream segment;
#   * the subscriber receives all the rows of those transactions;
#   * a subscriber that does not ask for the parallel streaming mode still
#     receives the transactions as whole blocks (a BEGIN and a COMMIT each,
#     and no stream segment at all).
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

# The number of transactions of the parallel streaming mode.
NTXN=${NTXN:-5}

# The number of transactions of the mode that does not stream.
NTXN_PLAIN=${NTXN_PLAIN:-3}

# The size of a stream segment.  It is large on purpose: the transactions of
# the test stay far below it, so every stream segment of the trace comes from
# a transaction that was streamed when it committed.
SEGMENT_SIZE=${SEGMENT_SIZE:-1048576}

WORKERS=${WORKERS:-4}

export PATH="$PGBIN:$PATH"

TRACE_PARALLEL=$ROOT/stream_all_parallel.trace
TRACE_PLAIN=$ROOT/stream_all_plain.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for_rows() {
	# wait_for_rows <table> <expected>
	local table=$1 expected=$2 i n
	for i in $(seq 1 120); do
		n=$(psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -q \
			-c "select count(*) from $table" 2>/dev/null || echo 0)
		if [ "$n" = "$expected" ]; then
			return 0
		fi
		sleep 1
	done
	fail "the subscriber did not catch up (expected $expected rows in $table)"
}

count_messages() {
	# count_messages <dump file> <tag>
	grep -c "^ *$2 " "$1" || true
}

echo "== setting up the clusters"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
parallel_decode_trace_file = '$TRACE_PARALLEL'
parallel_decode_stream_segment_size = $SEGMENT_SIZE
parallel_decode_stream_min_size = 0
parallel_decode_streaming_mode = 'auto'
EOF
pg_ctl -D "$ROOT/pub" reload >/dev/null
rm -f "$TRACE_PARALLEL"

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for table pd_t"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating a subscription that asks for the parallel streaming mode"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

echo "== writing $NTXN small transactions"
i=1
while [ "$i" -le "$NTXN" ]; do
	pub -c "insert into pd_t values ($i, 'a$i')"
	i=$((i + 1))
done

wait_for_rows pd_t "$NTXN"

bad=$(sub -At -c "select count(*) from pd_t where v <> ('a' || id)")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking the trace of the parallel streaming mode"
[ -s "$TRACE_PARALLEL" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE_PARALLEL" --messages >"$ROOT/stream_all_parallel.dump"

nstart=$(count_messages "$ROOT/stream_all_parallel.dump" S)
nstop=$(count_messages "$ROOT/stream_all_parallel.dump" E)
ncommit=$(count_messages "$ROOT/stream_all_parallel.dump" c)
echo "   $nstart stream start(s), $nstop stream stop(s), $ncommit stream commit(s)"
[ "$nstart" = "$NTXN" ] || fail "the publisher produced $nstart stream segment(s) for $NTXN transaction(s)"
[ "$nstop" = "$NTXN" ] || fail "the publisher produced $nstop stream stop(s) for $NTXN transaction(s)"
[ "$ncommit" = "$NTXN" ] || fail "the publisher produced $ncommit stream commit(s) for $NTXN transaction(s)"

# ---------------------------------------------------------------------------
# A subscriber that does not ask for the parallel streaming mode must still
# receive the transactions as whole blocks: the behavior of the serial path
# does not change.
# ---------------------------------------------------------------------------
echo "== switching the trace file"
sub -c "drop subscription pd_sub"
sed -i "s|^parallel_decode_trace_file = .*|parallel_decode_trace_file = '$TRACE_PLAIN'|" \
	"$ROOT/pub/postgresql.conf"
pg_ctl -D "$ROOT/pub" reload >/dev/null
rm -f "$TRACE_PLAIN"

echo "== creating a subscription that does not ask for the parallel streaming mode"
pub -c "create table pd_t2 (id int primary key, v text)"
pub -c "create publication pd_pub2 for table pd_t2"
sub -c "create table pd_t2 (id int primary key, v text)"
sub -c "create subscription pd_sub2 connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub2 with (streaming = 'on', copy_data = false)"

echo "== writing $NTXN_PLAIN small transactions"
i=1
while [ "$i" -le "$NTXN_PLAIN" ]; do
	pub -c "insert into pd_t2 values ($i, 'b$i')"
	i=$((i + 1))
done

wait_for_rows pd_t2 "$NTXN_PLAIN"

bad=$(sub -At -c "select count(*) from pd_t2 where v <> ('b' || id)")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking the trace of the mode that does not stream"
[ -s "$TRACE_PLAIN" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE_PLAIN" --messages >"$ROOT/stream_all_plain.dump"

nstart=$(count_messages "$ROOT/stream_all_plain.dump" S)
nstop=$(count_messages "$ROOT/stream_all_plain.dump" E)
nbegin=$(count_messages "$ROOT/stream_all_plain.dump" B)
ncommit=$(count_messages "$ROOT/stream_all_plain.dump" C)
echo "   $nstart stream start(s), $nstop stream stop(s), $nbegin begin(s), $ncommit commit(s)"
[ "$nstart" = "0" ] || fail "the publisher produced $nstart stream segment(s) for a subscriber that did not ask for them"
[ "$nstop" = "0" ] || fail "the publisher produced $nstop stream stop(s) for a subscriber that did not ask for them"
[ "$nbegin" = "$NTXN_PLAIN" ] || fail "the publisher produced $nbegin begin(s) for $NTXN_PLAIN transaction(s)"
[ "$ncommit" = "$NTXN_PLAIN" ] || fail "the publisher produced $ncommit commit(s) for $NTXN_PLAIN transaction(s)"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
