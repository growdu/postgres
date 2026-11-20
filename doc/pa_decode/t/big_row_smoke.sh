#!/bin/bash
# Smoke test for a change that does not fit into one queue entry of the
# publisher.
#
# The publisher hands the changes of a transaction to the walsender over a
# shared memory queue.  A queue entry holds 256 KB, so a change that is
# bigger than that is split into chunks that the walsender has to put back
# together.  The walsender may not have all the chunks when it looks for a
# frame, so the transfer of a big change is spread over several rounds, and
# the walsender has to remember what it is putting together.
#
# The test writes a row that is much bigger than a queue entry and checks
# that it reaches the subscriber with its content intact.  A row that is
# dropped on the way does not make the session fail: the subscriber simply
# does not have the row, and the publisher does not know that it is gone.
# The pending area test covers the case in which the transfer of the big
# change is interrupted and picked up again.
#
# The row is written together with other rows, so that the publisher has a
# batch of frames to write out when the big row reaches it.  The frames of
# the batch are taken without waiting, which is the case in which a transfer
# that is not complete yet has to be picked up again.
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
# The size of the big row, in bytes.  It has to be bigger than the entry of
# the queue that carries the changes of a transaction (256 KB), or the change
# is not split into chunks at all.  It also has to stay below the size of
# the queue that the parallel apply worker of the subscriber uses to receive
# the changes (16 MB).
BIGSIZE=${BIGSIZE:-8388608}
#
# The number of rows that are written around the big row.  They make the
# publisher write a batch of frames out.
ROWS=${ROWS:-200}
#
# Whether the big row is the first change of its transaction or the last one.
BIGFIRST=${BIGFIRST:-0}

WORKERS=${WORKERS:-4}

export PATH="$PGBIN:$PATH"

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
EOF
pg_ctl -D "$ROOT/pub" reload >/dev/null

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating a subscription that asks for the parallel streaming mode"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

echo "== writing a row of $BIGSIZE bytes"
if [ "$BIGFIRST" = "1" ]; then
	pub -c "begin" \
		-c "insert into pd_t values (0, repeat('h', $BIGSIZE))" \
		-c "insert into pd_t select g, repeat('x', 200) from generate_series(1, $ROWS) g" \
		-c "commit"
else
	pub -c "begin" \
		-c "insert into pd_t select g, repeat('x', 200) from generate_series(1, $ROWS) g" \
		-c "insert into pd_t values (0, repeat('h', $BIGSIZE))" \
		-c "commit"
fi
pub -c "insert into pd_t select g, repeat('y', 200) from generate_series($((ROWS + 1)), $((ROWS * 2))) g"

wait_for_rows "$((ROWS * 2 + 1))"

echo "== checking the content"
bad=$(sub -At -c "select count(*) from pd_t where id = 0 and v <> repeat('h', $BIGSIZE)")
[ "$bad" = "0" ] || fail "the big row does not have the right content"

bad=$(sub -At -c "select count(*) from pd_t where id <> 0 and (v <> repeat('x', 200) and v <> repeat('y', 200))")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

size=$(sub -At -c "select coalesce(max(length(v)), 0) from pd_t where id = 0")
[ "$size" = "$BIGSIZE" ] || fail "the big row has $size bytes instead of $BIGSIZE"

echo "   the big row of $size bytes reached the subscriber"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
