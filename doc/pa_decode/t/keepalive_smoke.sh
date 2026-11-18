#!/bin/bash
# Check that the publisher-side parallel decoding path sends keepalives while
# it is idle.
#
# The walsender spends most of its time waiting for the decode worker to hand
# over the next frame.  The replication connection has to be serviced during
# that wait, otherwise the subscriber would not see any keepalive and the
# session would run into the replication timeout.
#
# The test lowers wal_sender_timeout so that the keepalive interval is short,
# then leaves the subscription idle and counts the keepalive frames the
# publisher wrote into its trace file.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
IDLE=${IDLE:-20}
WORKERS=${WORKERS:-1}

export PATH="$PGBIN:$PATH"

TRACE=$ROOT/parallel.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

cat >>"$ROOT/pub/postgresql.conf" <<EOF
wal_sender_timeout = 10s
parallel_decode_trace_file = '$TRACE'
max_parallel_decode_workers = $WORKERS
EOF
"$PGBIN/pg_ctl" -D "$ROOT/pub" reload >/dev/null

pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id int primary key, v text)"

rm -f "$TRACE"

echo "== subscribing with the parallel decoding path"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'on', copy_data = false)"

# The parallel path has to be in use, otherwise the test proves nothing.
for i in $(seq 1 60); do
	if grep -q "parallel decode walreader started" "$ROOT/pub/server.log"; then
		break
	fi
	sleep 1
done
grep -q "parallel decode walreader started" "$ROOT/pub/server.log" ||
	fail "the parallel decoding path was not used"

pub -c "insert into pd_t values (1, 'a')"

n=0
for i in $(seq 1 60); do
	n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
	if [ "$n" = "1" ]; then
		break
	fi
	sleep 1
done
[ "$n" = "1" ] || fail "the subscriber did not receive the row"

echo "== idling for $IDLE seconds"
sleep "$IDLE"

keepalives=$(python3 "$SCRIPT_DIR/pd_frame_tool.py" dump "$TRACE" 2>/dev/null |
	grep -cE '^#[0-9]+ k ' || true)

echo "   keepalive frame(s) in the trace: $keepalives"
if [ "$keepalives" -lt 1 ]; then
	fail "the parallel path did not send any keepalive while it was idle"
fi

echo "== OK"
sub -c "drop subscription pd_sub" || true
"$SCRIPT_DIR/setup_env.sh" stop
