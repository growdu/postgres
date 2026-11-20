#!/bin/bash
# Verify that the watermarks of the replication slot advance in the
# publisher-side parallel decoding path, like they do in the serial path.
#
# The watermarks of the slot are the xmin of the catalog and the position the
# decoding may restart from.  Both of them are computed from the transactions
# that are in flight, and in the parallel path the transactions live in the
# decode workers.  The walreader therefore computes the values and hands them
# over to the walsender, which is the process that owns the slot.
#
# The test creates a subscription, writes some data, and then waits for the
# restart position of the slot to move forward.  The publisher writes a
# running transactions record every wal_writer_delay seconds (10 by default),
# and the record is what makes the watermarks advance.
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
ROWS=${ROWS:-100}
#
# How long the test waits for the watermarks to move forward.  The publisher
# writes a running transactions record every wal_writer_delay seconds, and the
# subscriber has to confirm the position of the record before the watermarks
# are applied, so the wait has to cover a few of those periods.
WAIT=${WAIT:-90}

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

slot_state() {
	# slot_state <slot>
	pub -At -F' ' -c "select restart_lsn, catalog_xmin from pg_replication_slots where slot_name = '$1'"
}

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
cat >>"$ROOT/pub/postgresql.conf" <<EOF
max_parallel_decode_workers = $WORKERS
EOF
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating the subscription ($WORKERS decode worker(s))"
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create subscription pd_sub connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (streaming = 'parallel', copy_data = false)"

echo "== writing the workload"
pub -c "insert into pd_t select g, 'v' || g from generate_series(1, $ROWS) g"

for i in $(seq 1 60); do
	n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
	[ "$n" = "$ROWS" ] && break
	sleep 1
done
[ "$n" = "$ROWS" ] || fail "the subscriber did not catch up (expected $ROWS rows, got $n)"

before=$(slot_state pd_sub)
echo "   the slot starts at $before"
restart_before=${before%% *}
xmin_before=${before##* }

echo "== waiting for the watermarks to advance (up to ${WAIT}s)"
advanced=no
i=0
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	now=$(slot_state pd_sub)
	restart_now=${now%% *}
	xmin_now=${now##* }
	if [ "$restart_now" != "$restart_before" ]; then
		advanced=yes
		echo "   after ${i}s the slot is at $now"
		break
	fi
done

if [ "$advanced" != "yes" ]; then
	fail "the restart position of the slot did not move (still $restart_before)"
fi

# The xmin of the catalog has to be valid: the slot protects the system
# tables from being vacuumed away while it decodes.
if [ "$xmin_now" = "0" ] || [ -z "$xmin_now" ]; then
	fail "the slot does not protect the catalog (catalog_xmin is $xmin_now)"
fi
echo "   the catalog xmin of the slot is $xmin_now (was $xmin_before)"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
