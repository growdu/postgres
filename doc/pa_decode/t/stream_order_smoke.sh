#!/bin/bash
# Verify that the publisher does not send a stream segment before the end
# event of a transaction that ended before it.
#
# A stream segment has no commit sequence number, so it cannot be ordered by
# the sequence of the commits.  It still may not overtake the end event of a
# transaction that ended before it: the subscriber applies the transactions at
# the same time when it streams in parallel, and two transactions that the
# publisher serialized with row locks must not be applied at the same time.
# Otherwise the subscriber reports a deadlock that the publisher does not have.
#
# The test makes the case happen:
#
#   * the two transactions run at the same time, so that the publisher decodes
#     them with two workers.  Two transactions that do not run at the same time
#     are decoded by the same worker, and one worker sends the end event of a
#     transaction before the segments of the next one.
#
#   * the first transaction is big, so the worker that owns it takes a while
#     to decode it, and its end event reaches the subscriber late.
#
#   * the second transaction goes on writing after the first one has committed,
#     so it produces stream segments that are positioned after the end event of
#     the first transaction.  Those are the segments that may not overtake it.
#
# The publisher checks the order itself, so the test looks at what it reports:
#
#   * the publisher holds the stream segments that wait for their turn, which
#     is what the release frontier is for;
#
#   * the publisher does not report that it sent a segment before the commit
#     of an older transaction.
#
# The rows have to arrive at the subscriber with the right content, and the
# workload really has to use the streaming path: a workload that produces no
# stream segment would not exercise anything.
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
# The number of decode workers of the publisher.
WORKERS=${WORKERS:-4}
#
# The number of rows of the first transaction.  It has to be big enough that
# its worker is still busy when the second transaction produces the stream
# segment that waits for the end event of the first transaction.
ROWS=${ROWS:-200000}
#
# The size of the rows of the first transaction.
VAL1=${VAL1:-10}
#
# The number of rows of the second transaction.
ROWS2=${ROWS2:-40000}
#
# The size of the rows of the second transaction.  The rows are big on purpose:
# the second transaction fills a stream segment with a few rows, so it
# produces the segment that waits for its turn before the first transaction
# has ended.
VAL2=${VAL2:-1000}
#
# The size of a stream segment.
SEGMENT_SIZE=${SEGMENT_SIZE:-8192}
#
# The time between the start of the first transaction and the start of the
# second one.  The second transaction has to be running when the first one
# commits, so that it produces stream segments that are positioned after the
# end event of the first transaction.
GAP=${GAP:-0.2}
#
# How long the test waits for the rows to arrive.
WAIT=${WAIT:-300}

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/stream_order.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	echo "--- the log of the publisher" >&2
	tail -40 "$ROOT/pub/server.log" >&2 || true
	exit 1
}

rowcount() {
	sub -At -c "select count(*) from pd_t" 2>/dev/null || echo ""
}

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$TRACE'
parallel_decode_stream_segment_size = $SEGMENT_SIZE
parallel_decode_stream_min_size = 0
parallel_decode_streaming_mode = 'auto'
parallel_decode_held_max_bytes = 1
EOF
pg_ctl -D "$ROOT/pub" reload >/dev/null
rm -f "$TRACE"

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating the subscription (streaming = parallel, copy_data = false)"
sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = 'parallel', copy_data = false)"

#
# The two transactions run at the same time, so that the publisher gives them
# to two workers.  The first one is big: its worker takes a while to decode
# it, and the end event of the transaction reaches the subscriber late.
#
echo "== writing the first transaction ($ROWS rows)"
pub -c "insert into pd_t select g, repeat('x', $VAL1) from generate_series(1, $ROWS) g" &
pid1=$!

sleep "$GAP"

#
# The second transaction is written while the first one is still running, and
# it goes on writing after the first one has committed: the stream segments it
# produces then are positioned after the end event of the first transaction.
#
echo "== writing the second transaction ($ROWS2 rows)"
pub -c "insert into pd_t select g, repeat('y', $VAL2) from generate_series($((ROWS + 1)), $((ROWS + ROWS2))) g" &
pid2=$!

wait "$pid1" || fail "the first transaction failed"
wait "$pid2" || fail "the second transaction failed"

echo "== waiting for the rows to arrive"
i=0
n=""
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	n=$(rowcount)
	[ "$n" = "$((ROWS + ROWS2))" ] && break
done
[ "$n" = "$((ROWS + ROWS2))" ] ||
	fail "the subscriber has $n rows instead of $((ROWS + ROWS2))"

echo "== checking the content"
bad=$(sub -At -c "select count(*) from pd_t
		where (id <= $ROWS and v <> repeat('x', $VAL1))
		   or (id > $ROWS and v <> repeat('y', $VAL2))")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking that the publisher really streamed"
[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages >"$ROOT/stream_order.dump"
nstart=$(grep -c '^ *S ' "$ROOT/stream_order.dump" || true)
nstop=$(grep -c '^ *E ' "$ROOT/stream_order.dump" || true)
[ "$nstart" -gt 0 ] || fail "the publisher produced no stream segment"
[ "$nstart" = "$nstop" ] ||
	fail "the publisher produced $nstart stream start(s) and $nstop stop(s)"
echo "   the publisher produced $nstart stream segment(s)"

#
# Both transactions have to be streamed: the test is about the segments of two
# transactions that are decoded at the same time.
#
nsecond=$(grep '^ *S ' "$ROOT/stream_order.dump" | grep -c 'xid=' || true)
[ "$nsecond" -gt 0 ] || fail "the publisher produced no stream segment at all"

#
# The end events of the transactions have to reach the subscriber in the order
# in which the transactions ended.
#
echo "== checking the order of the end events"
order=$(awk '
	$1 == "c" {
		if (!match($0, /xid=[0-9]+/))
			next
		xid = substr($0, RSTART + 4, RLENGTH - 4)
		if (!match($0, /commit_lsn=[0-9]+/))
			next
		lsn = substr($0, RSTART + 10, RLENGTH - 10) + 0
		if (n == 0)
			first = lsn
		else if (lsn < first) {
			print "the end events are not in the order of the commits"
			exit
		}
		n++
	}
	END {
		if (n < 2)
			print "the publisher produced fewer than two end events"
		else
			print "ok"
	}
' "$ROOT/stream_order.dump")
[ "$order" = "ok" ] || fail "$order"

#
# The publisher holds the stream segments that wait for their turn: they are
# held until the release frontier has moved past their position.
#
echo "== checking that the publisher held the stream segments"
if ! grep -q "stream segments that wait for their turn" "$ROOT/pub/server.log"; then
	fail "the publisher held no stream segment: the segments did not wait for \
the transactions that ended before them"
fi
echo "   the publisher held the stream segments that wait for their turn"

#
# The publisher checks the order of the end events itself: a stream segment
# that was sent before the commit of a transaction that ended before it is
# reported.
#
echo "== checking the order of the stream segments"
if grep -q "sent a stream segment before the commit of an older transaction" \
	"$ROOT/pub/server.log"; then
	fail "the publisher sent a stream segment before the commit of an older \
transaction"
fi
echo "   the publisher sent no stream segment before an older commit"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
