#!/bin/bash
# Verify the monitoring views of the publisher-side parallel decoding.
#
# The feature has to publish what the session is doing, so that the
# behaviour of the publisher can be observed while it runs (design document,
# 11.3).  The views are:
#
#   pg_parallel_decode_workers		the counters of the decode workers
#   pg_parallel_decode_commit_lag	the backlog of the commit ordering
#   pg_parallel_decode_txn_latency	the time the decoding of the transactions took
#   pg_parallel_decode_pending_commit_age	the age of the longest pending commit
#   pg_parallel_decode_walsender_frames	the output of the session
#   pg_parallel_decode_wakeups	the signals between the participants
#
# The test writes a workload, checks that the views exist and that they
# report the activity of the session, and checks that the counters of the
# workers add up to the counters of the session.
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
TXNS=${TXNS:-40}
CONC=${CONC:-8}

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

echo "== setting up the clusters"
#
# The number of the workers has to be set before the clusters are started:
# the change of the GUC takes effect in the next replication session, so a
# session that is started before the reload would use the old value.
export MAX_PARALLEL_DECODE_WORKERS=$WORKERS
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id bigserial primary key, v text)"
pub -c "create publication pd_pub for all tables"
sub -c "create table pd_t (id bigserial primary key, v text)"

echo "== starting the parallel decoding session ($WORKERS workers)"
#
# The slot has to carry the name of the subscription, because that is the
# name the apply worker asks the publisher for.
pub -c "select pg_create_logical_replication_slot('pd_sub', 'pgoutput')" >/dev/null
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"

echo "== writing the workload"
workload_client() {
	local client=$1 i
	for i in $(seq 1 "$TXNS"); do
		pub -c "begin; insert into pd_t (v) values ('v$client'); select pg_sleep(0.05); commit;" >/dev/null
	done
}
pids=()
for client in $(seq 1 "$CONC"); do
	workload_client "$client" &
	pids+=($!)
done
for p in "${pids[@]}"; do
	wait "$p"
done

echo "== checking that the views exist"
for view in pg_parallel_decode_workers \
	pg_parallel_decode_commit_lag \
	pg_parallel_decode_txn_latency \
	pg_parallel_decode_pending_commit_age \
	pg_parallel_decode_walsender_frames \
	pg_parallel_decode_wakeups; do
	n=$(pub -At -c "select count(*) from pg_class where relname = '$view'")
	[ "$n" = "1" ] || fail "the view $view does not exist"
done

echo "== checking the counters of the session"
#
# The session is still running, so it has to be in the views.  The counters
# are published from time to time, so the test waits for them.
wait_for() {
	# wait_for <query> <expected-not-empty>
	local query=$1 i value
	for i in $(seq 1 30); do
		value=$(pub -At -c "$query" 2>/dev/null || true)
		[ -n "$value" ] && [ "$value" != "0" ] && break
		sleep 1
	done
	echo "$value"
}

frames=$(wait_for "select sum(frames) from pg_parallel_decode_walsender_frames")
[ -n "$frames" ] && [ "$frames" != "0" ] || fail "the session reported no frames"
echo "   the session wrote $frames frame(s)"

workers=$(pub -At -c "select count(*) from pg_parallel_decode_workers")
[ "$workers" = "$WORKERS" ] || fail "the view reports $workers workers instead of $WORKERS"
echo "   the view reports $workers worker(s)"

txns=$(wait_for "select sum(txn_count) from pg_parallel_decode_txn_latency")
[ -n "$txns" ] && [ "$txns" != "0" ] || fail "the session reported no transactions"
echo "   the session decoded $txns transaction(s)"

down=$(wait_for "select sum(wakeups_down) from pg_parallel_decode_wakeups")
[ -n "$down" ] && [ "$down" != "0" ] || fail "the session reported no wakeups"
echo "   the walreader sent $down signal(s) to the workers"

up=$(wait_for "select sum(wakeups_up) from pg_parallel_decode_wakeups")
[ -n "$up" ] && [ "$up" != "0" ] || fail "the session reported no wakeups"
echo "   the workers sent $up signal(s) to the walsender"

echo "== checking that the counters of the workers add up"
sum_up=$(pub -At -c "select sum(wakeups) from pg_parallel_decode_workers")
[ "$sum_up" = "$up" ] || fail "the workers report $sum_up wakeups, the session reports $up"
echo "   the wakeups of the workers add up to the wakeups of the session"

echo "== checking the ordering backlog"
lag=$(pub -At -c "select sum(commit_lag) from pg_parallel_decode_commit_lag")
[ -n "$lag" ] || fail "the commit lag is not reported"
echo "   the commit lag is $lag"

echo "== checking that a session without the parallel path is not reported"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
sleep 2
n=$(pub -At -c "select count(*) from pg_parallel_decode_walsender_frames")
[ "$n" = "0" ] || fail "a session that does not use the parallel path is reported"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
