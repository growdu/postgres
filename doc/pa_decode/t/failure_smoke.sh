#!/bin/bash
# Smoke test for the failure handling of the publisher-side parallel decoding
# path (P7).
#
# A participant of the session that fails has to
#
#   * report the error to the walsender, so that the session ends with a
#     message that says what went wrong instead of the generic "the
#     participant died unexpectedly";
#   * make the walsender end the session, instead of leaving it stalled;
#   * let the subscriber recover: the replication reconnects and the data
#     that was written in the meantime still arrives.
#
# The failures are produced with the fault injection knob of the feature, so
# that the failure paths can be verified without having to break the machine.
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

setup() {
	# setup <fault>
	echo "== setting up the clusters (fault $1)"
	MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init \
		>/dev/null 2>&1
	"$SCRIPT_DIR/setup_env.sh" start >/dev/null 2>&1

	cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
parallel_decode_fault_inject = $1
EOF
	pg_ctl -D "$ROOT/pub" reload >/dev/null

	pub -c "create table pd_t (id int primary key, v text)"
	pub -c "create publication pd_pub for all tables"
	sub -c "create table pd_t (id int primary key, v text)"
	sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"
}

# ---------------------------------------------------------------------------
# A decode worker that fails.
# ---------------------------------------------------------------------------
setup 1

echo "== the publisher must report the failure of the worker"
for i in $(seq 1 60); do
	if grep -q "the parallel decode worker failed" "$ROOT/pub/server.log"; then
		break
	fi
	sleep 1
done
grep -q "the parallel decode worker failed" "$ROOT/pub/server.log" ||
	fail "the publisher did not report the failure of the decode worker"

# The message must name the reason, not just say that something went wrong.
grep -q "failed on purpose" "$ROOT/pub/server.log" ||
	fail "the publisher did not report the reason of the failure"

# ---------------------------------------------------------------------------
# A walreader that fails.
# ---------------------------------------------------------------------------
setup 2

echo "== the publisher must report the failure of the walreader"
for i in $(seq 1 60); do
	if grep -q "the parallel decode walreader failed" "$ROOT/pub/server.log"; then
		break
	fi
	sleep 1
done
grep -q "the parallel decode walreader failed" "$ROOT/pub/server.log" ||
	fail "the publisher did not report the failure of the walreader"

grep -q "failed on purpose" "$ROOT/pub/server.log" ||
	fail "the publisher did not report the reason of the failure"

# ---------------------------------------------------------------------------
# A healthy session after the failures: the replication has to work again, and
# the data has to arrive.
# ---------------------------------------------------------------------------
setup 0

echo "== the replication works again after the failures"
pub -c "insert into pd_t select g, repeat('z', 100) from generate_series(1, 100) g"
wait_for_rows 100

bad=$(sub -At -c "select count(*) from pd_t where v <> repeat('z', 100)")
[ "$bad" = "0" ] || fail "$bad row(s) have the wrong content"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop >/dev/null
