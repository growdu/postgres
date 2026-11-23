#!/bin/bash
# Verify the commit_sync barrier of the publisher side parallel decoding path.
#
# The barrier lets the phases of the transactions pass in the order of their
# phase numbers.  A phase is the end event of a transaction: the commit, the
# abort or the prepare.  The worker tags the frame that carries the end event,
# and the walsender keeps the tag apart from the data frames of the
# transaction, so that the end event is the last frame of the phase.
#
# The test runs a workload with two transactions:
#
#   1. a transaction that commits.  It produces a stream commit message, so
#      its phase has an end event: the phase record says so.
#
#   2. a transaction that is rolled back.  Its changes never reach the
#      subscriber, and it did not collect enough changes to be streamed, so it
#      has no end event at all: the phase record says that too.  The phase
#      exists nevertheless: the transaction ended, so it has a phase number.
#
# The publisher records what the barrier did with every phase in its trace
# file, so the test can look at the phases from the outside:
#
#   * the phases are let pass in the order of their phase numbers, and the
#     phase numbers are the ones the walreader handed out;
#
#   * the phase of the committed transaction has an end event, and the phase of
#     the rolled back transaction has none.
#
# The rows have to arrive at the subscriber with the right content, and the
# subscriber must not report a deadlock.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest_marker_tag}
PORT_PUB=${PORT_PUB:-39411}
PORT_SUB=${PORT_SUB:-55411}
#
# The number of decode workers of the publisher.
WORKERS=${WORKERS:-4}
#
# The number of rows of the transactions of the workload.  It has to be below
# the number of the changes that cut a transaction into a stream segment (the
# default of parallel_decode_stream_segment_changes is 8): a transaction that
# was streamed before it was rolled back produces a stream abort message, and
# that message is an end event as well.
ROWS=${ROWS:-3}

export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB
export PATH="$PGBIN:$PATH"

TRACE=$ROOT/marker_tag.trace

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
lc_messages = 'C'
EOF
cat >>"$ROOT/sub/postgresql.conf" <<EOF
lc_messages = 'C'
EOF
"$SCRIPT_DIR/setup_env.sh" start
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
# The first transaction commits, the second one is rolled back.
#
echo "== writing the workload"
pub -c "begin" \
	-c "insert into pd_t select g, repeat('a', 100) from generate_series(1, $ROWS) g" \
	-c "commit"
pub -c "begin" \
	-c "insert into pd_t select g + 1000, repeat('b', 100) from generate_series(1, $ROWS) g" \
	-c "rollback"

echo "== waiting for the rows to arrive"
i=0
n=""
while [ "$i" -lt 120 ]; do
	sleep 1
	i=$((i + 1))
	n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo "")
	[ "$n" = "$ROWS" ] && break
done
[ "$n" = "$ROWS" ] || fail "the subscriber has $n rows instead of $ROWS"

bad=$(sub -At -c "select count(*) from pd_t where v <> repeat('a', 100)")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== checking that the subscriber did not report a deadlock"
if grep -qiE "deadlock|死锁" "$ROOT/sub/server.log"; then
	fail "the subscriber reported a deadlock"
fi
echo "   the subscriber reported no deadlock"

echo "== checking the phases of the barrier"
[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages --phases \
	>"$ROOT/marker_tag.dump"

#
# The phases are let pass in the order of their phase numbers, and the phase
# numbers are the ones the walreader handed out: they are consecutive.
#
phases=$(grep -c 'phase cseq=' "$ROOT/marker_tag.dump" || true)
[ "$phases" = "2" ] || fail "the publisher let $phases phases pass instead of two"
echo "   the publisher let $phases phases pass"

order=$(awk '
	/phase cseq=/ {
		if (!match($0, /cseq=[0-9]+/))
			next
		cseq = substr($0, RSTART + 5, RLENGTH - 5) + 0
		if (n > 0 && cseq != first + n) {
			print "the phases are not in the order of their phase numbers"
			exit
		}
		if (n == 0)
			first = cseq
		n++
	}
	END {
		if (n < 2)
			print "the publisher let fewer than two phases pass"
		else
			print "ok"
	}
' "$ROOT/marker_tag.dump")
[ "$order" = "ok" ] || fail "$order"
echo "   the phases were let pass in the order of their phase numbers"

#
# The phase of the committed transaction has an end event, the phase of the
# rolled back transaction has none.
#
markers=$(grep 'phase cseq=' "$ROOT/marker_tag.dump" | grep -c 'marker=yes' || true)
[ "$markers" = "1" ] ||
	fail "$markers phase(s) have an end event instead of one"
echo "   one phase has an end event"

#
# The end event of the committed transaction is the stream commit message, and
# the rolled back transaction produced no message at all.
#
ncommit=$(grep -c '^ *c ' "$ROOT/marker_tag.dump" || true)
[ "$ncommit" = "1" ] || fail "the publisher sent $ncommit stream commit message(s) instead of one"
nabort=$(grep -c '^ *A ' "$ROOT/marker_tag.dump" || true)
[ "$nabort" = "0" ] || fail "the publisher sent $nabort stream abort message(s) instead of none"
echo "   the publisher sent one end event"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
