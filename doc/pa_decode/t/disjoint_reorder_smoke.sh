#!/bin/bash
# Verify that the data of a transaction is sent as soon as the transaction has
# settled, even when an older transaction has not ended yet.
#
# The commit_sync barrier of the design document holds back the end events of
# the transactions, so that the subscriber learns that the transactions are
# over in the order in which they were committed.  The data of a transaction is
# not held back: it is sent as soon as the worker that owns the transaction
# has produced it.  Two transactions that do not touch the same rows may
# therefore be applied by the subscriber at the same time.
#
# The test makes the case happen:
#
#   * the two transactions work on two different tables, so they are disjoint:
#     the subscriber may apply them at the same time, and neither of them can
#     block the other;
#
#   * the second transaction writes its rows first, so they are at the head of
#     the WAL, and the worker that owns it sends them as soon as it has
#     decoded them;
#
#   * the first transaction is big and comes second, so the worker that owns it
#     takes a while to decode it, and its end event reaches the subscriber
#     late.  The second transaction commits after the first one, so the end
#     event of the first transaction has the lower phase number.
#
# The order on the wire is therefore
#
#	the data of the second transaction,
#	the end event of the first transaction,
#	the end event of the second transaction,
#
# and the test asserts exactly that.
#
# The rows have to arrive at the subscriber with the right content, and the
# subscriber must not report a deadlock.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest_disjoint_reorder}
PORT_PUB=${PORT_PUB:-39431}
PORT_SUB=${PORT_SUB:-55431}
#
# The number of decode workers of the publisher.
WORKERS=${WORKERS:-4}
#
# The number of rows of the first transaction.  It has to be big enough that
# its worker is still busy when the second transaction produces its data.
ROWS=${ROWS:-100000}
#
# The size of the rows of the first transaction.
VAL1=${VAL1:-100}
#
# The number of rows of the second transaction.
ROWS2=${ROWS2:-2000}
#
# The size of the rows of the second transaction.  The rows are big on purpose:
# the second transaction has to fill the WAL buffers of the publisher while it
# is running, so that its rows reach the WAL before the first transaction
# commits.
VAL2=${VAL2:-4000}
#
# The number of changes a transaction has to collect before it is cut into a
# stream segment of its own.
SEGMENT_CHANGES=${SEGMENT_CHANGES:-8}
#
# How long the test waits for the rows to arrive.
WAIT=${WAIT:-300}

export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB
export PATH="$PGBIN:$PATH"

TRACE=$ROOT/disjoint_reorder.trace

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

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$TRACE'
parallel_decode_stream_segment_changes = $SEGMENT_CHANGES
wal_buffers = 1MB
lc_messages = 'C'
EOF
cat >>"$ROOT/sub/postgresql.conf" <<EOF
lc_messages = 'C'
EOF
"$SCRIPT_DIR/setup_env.sh" start
#
# The trace is written by appending, so a file that an earlier run left behind
# would still be in it.  The handshake files of the two transactions live
# directly in $ROOT, and setup_env.sh init only wipes $ROOT/pub and $ROOT/sub,
# so a stale one would let the second transaction commit before the first one.
# See marker_order_smoke.sh for the details.
#
rm -f "$TRACE" "$ROOT/first.done" "$ROOT/second.done"

echo "== creating the schema"
pub -c "create table pd_a (id int primary key, v text)"
pub -c "create table pd_b (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_a (id int primary key, v text)"
sub -c "create table pd_b (id int primary key, v text)"

echo "== creating the subscription (streaming = parallel, copy_data = false)"
sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = 'parallel', copy_data = false)"

#
# The second transaction writes its rows first: they are then at the head of
# the WAL, and the worker that owns the transaction sends them as soon as it
# has decoded them.  The transaction waits for the first one to commit before
# it commits itself, so the end event of the first transaction has to be the
# one with the lower phase number.
#
echo "== writing the second transaction ($ROWS2 rows)"
(
	{
		echo "begin;"
		echo "insert into pd_b select g, repeat('y', $VAL2)"
		echo "	from generate_series(1, $ROWS2) g;"
		echo "\\! touch $ROOT/second.done"
		while [ ! -f "$ROOT/first.done" ]; do sleep 0.2; done
		echo "commit;"
	} | pub
) &
pid2=$!

#
# The first transaction waits for the rows of the second one to be in the WAL,
# and then it writes its own rows.  It is big, so its worker takes a while to
# decode it, and its end event reaches the subscriber late.
#
echo "== writing the first transaction ($ROWS rows)"
(
	i=0
	while [ ! -f "$ROOT/second.done" ]; do
		sleep 0.2
		i=$((i + 1))
		[ "$i" -gt 300 ] && break
	done
	pub -c "begin" \
		-c "insert into pd_a select g, repeat('x', $VAL1) from generate_series(1, $ROWS) g" \
		-c "commit"
	touch "$ROOT/first.done"
) &
pid1=$!

wait "$pid1" || fail "the first transaction failed"
wait "$pid2" || fail "the second transaction failed"

echo "== waiting for the rows to arrive"
i=0
n=""
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	n=$(sub -At -c "select count(*) from pd_a" 2>/dev/null || echo "")
	m=$(sub -At -c "select count(*) from pd_b" 2>/dev/null || echo "")
	[ "$n" = "$ROWS" ] && [ "$m" = "$ROWS2" ] && break
done
[ "$n" = "$ROWS" ] || fail "the subscriber has $n rows in pd_a instead of $ROWS"
[ "$m" = "$ROWS2" ] || fail "the subscriber has $m rows in pd_b instead of $ROWS2"

echo "== checking the content"
bad=$(sub -At -c "select count(*) from pd_a where v <> repeat('x', $VAL1)")
[ "$bad" = "0" ] || fail "$bad row(s) of pd_a have the wrong content"
bad=$(sub -At -c "select count(*) from pd_b where v <> repeat('y', $VAL2)")
[ "$bad" = "0" ] || fail "$bad row(s) of pd_b have the wrong content"

echo "== checking that the subscriber did not report a deadlock"
if grep -qiE "deadlock|死锁" "$ROOT/sub/server.log"; then
	fail "the subscriber reported a deadlock"
fi
echo "   the subscriber reported no deadlock"

#
# The ids of the transactions, so that the messages of the trace can be told
# apart.
#
xid1=$(pub -At -c "select distinct xmin from pd_a")
xid2=$(pub -At -c "select distinct xmin from pd_b")
[ -n "$xid1" ] && [ -n "$xid2" ] || fail "the publisher did not report the ids of the transactions"
echo "   the transactions have the ids $xid1 and $xid2"

[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages \
	>"$ROOT/disjoint_reorder.dump"

#
# The order of the messages on the wire:
#
#   * the data of the second transaction comes before the end event of the
#     first one: the data of a transaction is not held back;
#
#   * the end event of the first transaction comes before the end event of the
#     second one: the barrier lets the phases pass in the order of their
#     phase numbers.
#
lines=$(awk -v xid1="$xid1" -v xid2="$xid2" '
	/^ *S .*xid=/ {
		if (match($0, /xid=[0-9]+/)) {
			xid = substr($0, RSTART + 4, RLENGTH - 4) + 0
			if (xid == xid2 && !data2)
				data2 = NR
		}
	}
	/^ *[cC] .*xid=/ {
		if (match($0, /xid=[0-9]+/)) {
			xid = substr($0, RSTART + 4, RLENGTH - 4) + 0
			if (xid == xid1 && !end1)
				end1 = NR
			if (xid == xid2 && !end2)
				end2 = NR
		}
	}
	END {
		if (!data2 || !end1 || !end2) {
			print "the trace does not hold the messages of the two transactions"
			exit
		}
		if (data2 > end1) {
			print "the data of the second transaction did not pass the end event of the first one"
			exit
		}
		if (end1 > end2) {
			print "the end events are not in the order of the commits"
			exit
		}
		print "ok"
	}
' "$ROOT/disjoint_reorder.dump")
[ "$lines" = "ok" ] || fail "$lines"
echo "   the data of the second transaction passed the end event of the first one"
echo "   the end events are in the order of the commits"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
