#!/bin/bash
# Smoke test for the two-phase commit on the publisher-side parallel decoding
# path.
#
# The parallel decoding path streams every transaction, so that the subscriber
# can apply the transactions in parallel.  A prepared transaction is the
# exception: the two-phase commit has its own protocol (BEGIN PREPARE,
# PREPARE, COMMIT PREPARED), and the subscriber expects that protocol.  A
# prepared transaction that was streamed would arrive as a stream block
# instead, and the subscriber would not see the prepare at all.
#
# The test checks that
#
#   * the prepared transaction reaches the subscriber as a prepare: the row is
#     not applied before the commit, and the subscriber holds a prepared
#     transaction while it waits;
#   * the commit prepared applies the row;
#   * the publisher produced no stream segment for the transaction.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
WORKERS=${WORKERS:-4}
#
# The harness has to use the same ports and the same root, so they have to be
# in the environment of the harness.
export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/two_phase.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

echo "== setting up the clusters"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init

#
# max_prepared_transactions needs a restart, so it has to be in the config
# before the clusters are started.  The subscriber needs it as well: it holds
# the prepared transaction while it waits for the commit.
cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
max_prepared_transactions = 10
parallel_decode_trace_file = '$TRACE'
EOF
cat >>"$ROOT/sub/postgresql.conf" <<EOF
max_prepared_transactions = 10
EOF

"$SCRIPT_DIR/setup_env.sh" start
rm -f "$TRACE"

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

echo "== creating a subscription that asks for the parallel streaming mode"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false, two_phase = true)"

echo "== preparing a transaction"
pub -c "begin" -c "insert into pd_t values (1, 'prepared')" \
	-c "prepare transaction 'pd_two_phase_gid'"

#
# The transaction is not applied before it is committed: the subscriber holds
# it as a prepared transaction.
sleep 3
n=$(sub -At -c "select count(*) from pd_t")
[ "$n" = "0" ] || fail "the subscriber applied the prepared transaction before the commit"
p=$(sub -At -c "select count(*) from pg_prepared_xacts")
[ "$p" = "1" ] || fail "the subscriber holds $p prepared transaction(s) instead of one"
echo "   the subscriber holds the prepared transaction"

echo "== committing the prepared transaction"
pub -c "commit prepared 'pd_two_phase_gid'"

n=0
for i in $(seq 1 60); do
	n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
	[ "$n" = "1" ] && break
	sleep 1
done
[ "$n" = "1" ] || fail "the subscriber did not apply the prepared transaction"
echo "   the subscriber has the row"

echo "== checking the trace"
[ -s "$TRACE" ] || fail "the publisher produced no trace file"
python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" --messages >"$ROOT/two_phase.dump"

nprep=$(grep -c '^ *P ' "$ROOT/two_phase.dump" || true)
ncommit=$(grep -c '^ *K ' "$ROOT/two_phase.dump" || true)
nstart=$(grep -c '^ *S ' "$ROOT/two_phase.dump" || true)
echo "   prepares: $nprep, commit prepareds: $ncommit, stream segments: $nstart"

[ "$nprep" -gt 0 ] || fail "the publisher did not send a prepare"
[ "$ncommit" -gt 0 ] || fail "the publisher did not send a commit prepared"
#
# The prepared transaction is not streamed: it reaches the subscriber as a
# whole transaction, with the messages of the two-phase commit.
[ "$nstart" = "0" ] || fail "the prepared transaction was streamed"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
