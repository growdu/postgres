#!/bin/bash
# Verify the order of the stream segments and of the end events of the
# publisher side parallel decoding path.
#
# A stream segment belongs to a transaction that is still running, so it has
# no phase number of its own.  The commit_sync barrier of the design document
# orders the end events of the transactions, so that the subscriber learns
# that the transactions are over in the order in which they were committed.
# The data of a transaction is not held back: a stream segment may reach the
# subscriber before the end event of a transaction that ended before it.  The
# subscriber applies the transactions at the same time then, and the two
# transactions that the publisher serialized with row locks have to be applied
# one after the other by the subscriber.
#
# The test makes the case happen:
#
#   * the two transactions run at the same time, so that the publisher decodes
#     them with two workers.  Two transactions that do not run at the same time
#     are decoded by the same worker, and one worker sends the end event of a
#     transaction before the segments of the next one.
#
#   * the first transaction is big, so the worker that owns it takes a while to
#     decode it, and its end event reaches the subscriber late.
#
#   * the second transaction goes on writing after the first one has committed,
#     so it produces stream segments that are positioned after the end event of
#     the first transaction.  Those are the segments that may pass the end
#     event of the first transaction.
#
# The order of the two transactions on the wire is a race: it depends on how
# fast the two workers decode their transactions.  The test therefore runs the
# case several times, and it asks for one case that shows the segments of the
# transaction that committed later passing the end event of the transaction
# that committed earlier.  The end events are checked in every case, because
# their order does not depend on the race.
#
# The test asserts:
#
#   * the end events of the transactions reach the subscriber in the order in
#     which the transactions ended: the barrier holds the end events back;
#
#   * in at least one case the stream segments of the transaction that
#     committed later reach the subscriber before the end event of the
#     transaction that committed earlier: the release frontier is gone, and
#     the data of a transaction is not held back;
#
#   * the publisher does not hold the stream segments back and does not report
#     that it sent a segment before the commit of an older transaction;
#
#   * the subscriber does not report a deadlock;
#
#   * the rows arrive at the subscriber with the right content.
#
# The second part of the test makes the case of two transactions that touch the
# same row: the publisher serializes them with a row lock, so the
# subscriber has to apply them one after the other.  The test asserts that the
# subscriber does not report a deadlock and that the row ends up with the
# value of the transaction that committed last.
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
# segment that passes the end event of the first transaction.
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
# produces the segment that passes the end event of the first transaction.
VAL2=${VAL2:-1000}
#
# The number of changes a transaction has to collect before it is cut into a
# stream segment of its own.
SEGMENT_CHANGES=${SEGMENT_CHANGES:-64}
#
# The time between the start of the first transaction and the start of the
# second one.  The second transaction has to be running when the first one
# commits, so that it produces stream segments that are positioned after the
# end event of the first transaction.
GAP=${GAP:-0.2}
#
# The number of times the case is run.  The order of the two transactions on
# the wire is a race, so the test needs more than one case to see the
# segments of a transaction pass the end event of an older one.
ATTEMPTS=${ATTEMPTS:-8}
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
	echo "--- the log of the subscriber" >&2
	tail -40 "$ROOT/sub/server.log" >&2 || true
	exit 1
}

rowcount() {
	sub -At -c "select count(*) from pd_t" 2>/dev/null || echo ""
}

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$TRACE'
parallel_decode_stream_segment_changes = $SEGMENT_CHANGES
lc_messages = 'C'
EOF
cat >>"$ROOT/sub/postgresql.conf" <<EOF
lc_messages = 'C'
EOF
"$SCRIPT_DIR/setup_env.sh" start

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
# The number of rows of one case.  Every case uses its own range of ids, so
# that the cases do not touch each other.
#
SPAN=$((ROWS + ROWS2 + 1000))

#
# The two transactions run at the same time, so that the publisher gives them
# to two workers.  The first one is big: its worker takes a while to decode
# it, and the end event of the transaction reaches the subscriber late.
#
inverted=0
attempt=0
while [ "$attempt" -lt "$ATTEMPTS" ]; do
	attempt=$((attempt + 1))
	base=$(((attempt - 1) * SPAN))
	first=$((base + 1))
	second=$((base + ROWS + 1))
	last=$((base + ROWS + ROWS2))

	echo "== case $attempt: writing the first transaction ($ROWS rows)"
	pub -c "insert into pd_t select g, repeat('x', $VAL1)
			from generate_series($first, $((base + ROWS))) g" &
	pid1=$!

	sleep "$GAP"

	#
	# The second transaction is written while the first one is still running,
	# and it goes on writing after the first one has committed: the stream
	# segments it produces then are positioned after the end event of the
	# first transaction.
	#
	echo "== case $attempt: writing the second transaction ($ROWS2 rows)"
	pub -c "insert into pd_t select g, repeat('y', $VAL2)
			from generate_series($second, $last) g" &
	pid2=$!

	wait "$pid1" || fail "the first transaction of case $attempt failed"
	wait "$pid2" || fail "the second transaction of case $attempt failed"

	#
	# The cases use their own ranges of ids, and the ranges have gaps between
	# them, so the number of the rows is not the last id.
	#
	expected=$((attempt * (ROWS + ROWS2)))

	echo "== case $attempt: waiting for the rows to arrive"
	i=0
	n=""
	while [ "$i" -lt "$WAIT" ]; do
		sleep 1
		i=$((i + 1))
		n=$(rowcount)
		[ "$n" = "$expected" ] && break
	done
	[ "$n" = "$expected" ] ||
		fail "the subscriber has $n rows instead of $expected"

	echo "== case $attempt: checking the content"
	bad=$(sub -At -c "select count(*) from pd_t
			where id between $first and $((base + ROWS))
			  and v <> repeat('x', $VAL1)")
	[ "$bad" = "0" ] || fail "$bad row(s) of case $attempt have the wrong content"
	bad=$(sub -At -c "select count(*) from pd_t
			where id between $second and $last
			  and v <> repeat('y', $VAL2)")
	[ "$bad" = "0" ] || fail "$bad row(s) of case $attempt have the wrong content"

	echo "== case $attempt: checking that the subscriber did not report a deadlock"
	if grep -qi "deadlock" "$ROOT/sub/server.log"; then
		fail "the subscriber reported a deadlock"
	fi
	echo "   the subscriber reported no deadlock"
done

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
# The ids of the transactions of every case, so that the messages of the trace
# can be told apart.
#
xids=""
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
	base=$(((attempt - 1) * SPAN))
	xid1=$(pub -At -c "select distinct xmin from pd_t
			where id between $((base + 1)) and $((base + ROWS))")
	xid2=$(pub -At -c "select distinct xmin from pd_t
			where id between $((base + ROWS + 1)) and $((base + ROWS + ROWS2))")
	[ -n "$xid1" ] && [ -n "$xid2" ] ||
		fail "the publisher did not report the ids of the transactions of case $attempt"
	echo "   the transactions of case $attempt have the ids $xid1 and $xid2"
	xids="$xids $xid1 $xid2"
done

#
# The order of the messages on the wire.  The two transactions of a case are
# told apart by their ids, and the one that committed first is the one whose
# commit record has the smaller commit lsn.
#
#   * the end events have to be in the order of the commits: the barrier lets
#     the phases pass in the order of their phase numbers;
#
#   * the segments of the transaction that committed later may pass the end
#     event of the transaction that committed earlier: the release frontier is
#     gone, and the data of a transaction is not held back.
#
echo "== checking the order of the messages"
for ((attempt = 1; attempt <= ATTEMPTS; attempt++)); do
	read -r xid1 xid2 <<<"$(echo $xids | cut -d' ' -f$((2 * attempt - 1)),$((2 * attempt)))"
	order=$(awk -v xid1="$xid1" -v xid2="$xid2" '
		function xid_of(line,   m) {
			if (match(line, /xid=[0-9]+/))
				return substr(line, RSTART + 4, RLENGTH - 4) + 0
			return -1
		}
		/^ *S .*xid=/ {
			xid = xid_of($0)
			if (xid == xid1 && !data1)
				data1 = NR
			if (xid == xid2 && !data2)
				data2 = NR
		}
		/^ *[cC] .*xid=/ {
			xid = xid_of($0)
			if (xid == xid1 && !end1) {
				end1 = NR
				if (match($0, /commit_lsn=/)) {
					v = substr($0, RSTART + 11)
					gsub(/[^0-9]/, "", v)
					lsn1 = v + 0
				}
			}
			if (xid == xid2 && !end2) {
				end2 = NR
				if (match($0, /commit_lsn=/)) {
					v = substr($0, RSTART + 11)
					gsub(/[^0-9]/, "", v)
					lsn2 = v + 0
				}
			}
		}
		END {
			if (!data1 || !data2 || !end1 || !end2) {
				print "the trace does not hold the messages of the two transactions"
				exit
			}
			if (lsn1 < lsn2) {
				print end1 " " end2 " " data1 " " data2
				exit
			}
			print end2 " " end1 " " data2 " " data1
		}
	' "$ROOT/stream_order.dump")
	set -- $order
	case "$1" in
	"the"*)
		fail "$order"
		;;
	esac
	end_first=$1
	end_second=$2
	data_first=$3
	data_second=$4
	[ "$end_first" -lt "$end_second" ] ||
		fail "the end events of case $attempt are not in the order of the commits"
	echo "   case $attempt: the end events are in the order of the commits"
	if [ "$data_second" -lt "$end_first" ]; then
		inverted=$((inverted + 1))
		echo "   case $attempt: the segments of the transaction that committed later passed the end event of the other one"
	else
		echo "   case $attempt: the segments of the transaction that committed later did not pass the end event of the other one"
	fi
done

[ "$inverted" -gt 0 ] ||
	fail "in none of the $ATTEMPTS cases did the data of a transaction pass the end event of an older one: the publisher may still be holding the data back"
echo "   the data of a transaction passed the end event of an older one in $inverted case(s)"

#
# The publisher does not hold the stream segments back any more: the release
# frontier is gone.
#
echo "== checking that the publisher did not hold the stream segments"
if grep -q "bytes of stream segments that wait for their turn" "$ROOT/pub/server.log"; then
	fail "the publisher held the stream segments back: the release frontier \
should be gone"
fi
echo "   the publisher did not hold the stream segments back"

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

#
# The case of two transactions that touch the same row.  The publisher
# serializes them with a row lock: the second one waits for the first one to
# commit.  The subscriber has to apply them one after the other, and it must
# not report a deadlock that the publisher does not have.
#
echo "== writing two transactions that touch the same row"
#
# The first transaction is big and it keeps the row locked until it commits, so
# the second one waits for the row lock at the publisher: the publisher
# serializes the two transactions, and the second one commits last.  The
# second one is decoded by another worker, and that worker is much faster: its
# segment reaches the subscriber before the segment of the first transaction,
# so the subscriber applies the second transaction first and the two
# transactions meet on the subscriber.
#
pair_base=$((ATTEMPTS * SPAN))
{
	echo "begin;"
	echo "update pd_t set v = 'first' where id = 1;"
	echo "insert into pd_t select g, repeat('z', 10)"
	echo "	from generate_series($((pair_base + 1)), $((pair_base + 100000))) g;"
	echo "\\! sleep 3"
	echo "commit;"
} | pub &
pid3=$!

#
# The second transaction starts a little later, so that it is the one that
# waits for the row lock.
#
sleep 1

{
	echo "begin;"
	echo "update pd_t set v = 'second' where id = 1;"
	echo "commit;"
} | pub &
pid4=$!

wait "$pid3" || fail "the first transaction of the pair failed"
wait "$pid4" || fail "the second transaction of the pair failed"

echo "== waiting for the row to arrive"
i=0
v=""
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	v=$(sub -At -c "select v from pd_t where id = 1" 2>/dev/null || echo "")
	[ "$v" = "second" ] && break
done
[ "$v" = "second" ] ||
	fail "the subscriber has the value '$v' of the row instead of 'second'"

echo "== checking that the subscriber did not report a deadlock"
if grep -qi "deadlock" "$ROOT/sub/server.log"; then
	fail "the subscriber reported a deadlock"
fi
echo "   the subscriber reported no deadlock"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
