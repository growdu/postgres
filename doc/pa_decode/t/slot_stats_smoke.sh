#!/bin/bash
# The statistics of a replication slot have to be reported for the sessions
# that use the parallel decoding path as well.
#
# The decode workers of the parallel decoding path do not own the slot: they
# work on a copy of it.  The index of the slot in the statistics of the slots
# is computed from the address of the slot, so it cannot be computed from such
# a copy: the workers have to be told the index of the slot of the session.
#
# If the workers report the statistics under a wrong index, two things go
# wrong: the slot of the session shows no data at all, and the statistics of
# the publisher are held under an index that belongs to no slot, which makes
# the publisher read out of bounds when it writes the statistics at shutdown.
#
# The test therefore checks three things:
#
#   1. the slot of a parallel session reports the transactions it decoded,
#   2. the slot of a serial session reports them as well,
#   3. the publisher survives a clean shutdown after a parallel session.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
export PDTEST_ROOT=${PDTEST_ROOT:-/tmp/pdtest}
export PORT_PUB=${PORT_PUB:-5442}
export PORT_SUB=${PORT_SUB:-5443}
#
# The publisher has to have the decode workers, otherwise the session of the
# parallel subscription would not take the parallel decoding path.
export MAX_PARALLEL_DECODE_WORKERS=${MAX_PARALLEL_DECODE_WORKERS:-4}
export PATH="$PGBIN:$PATH"

pub() { "$PGBIN/psql" -h "$PDTEST_ROOT" -p "$PORT_PUB" -U postgres -X -At -v ON_ERROR_STOP=1 "$@"; }
sub() { "$PGBIN/psql" -h "$PDTEST_ROOT" -p "$PORT_SUB" -U postgres -X -At -v ON_ERROR_STOP=1 "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start
trap '"$SCRIPT_DIR/setup_env.sh" stop' EXIT

pub -d postgres -c "CREATE TABLE t(id int primary key, v text);"
pub -d postgres -c "CREATE PUBLICATION p FOR TABLE t;"
sub -d postgres -c "CREATE TABLE t(id int primary key, v text);"

echo "== the serial subscription"
sub -d postgres -c "CREATE SUBSCRIPTION s_serial CONNECTION 'host=$PDTEST_ROOT port=$PORT_PUB dbname=postgres user=postgres' PUBLICATION p WITH (streaming = off, copy_data = false);"

echo "== the parallel subscription"
sub -d postgres -c "CREATE SUBSCRIPTION s_par CONNECTION 'host=$PDTEST_ROOT port=$PORT_PUB dbname=postgres user=postgres' PUBLICATION p WITH (streaming = parallel, copy_data = false);"

sleep 3
pub -d postgres -c "INSERT INTO t VALUES (1,'a'),(2,'b');"

echo "== waiting for the rows on the subscriber"
for i in $(seq 1 60); do
	n=$(sub -d postgres -c "SELECT count(*) FROM t;" || true)
	[ "$n" = "2" ] && break
	sleep 0.5
done
[ "$n" = "2" ] || fail "the subscriber has $n rows, expected 2"

#
# The counters of the slots are reported by the process that decodes the
# transactions: the walsender for the serial session, the decode workers for
# the parallel one.  They are read from the shared memory, so they are there
# as soon as the transaction has been decoded.
echo "== waiting for the statistics of the slots"
for i in $(seq 1 60); do
	serial=$(pub -d postgres -c "SELECT total_txns FROM pg_stat_replication_slots WHERE slot_name = 's_serial';")
	parallel=$(pub -d postgres -c "SELECT total_txns FROM pg_stat_replication_slots WHERE slot_name = 's_par';")
	[ "${serial:-0}" != "0" ] && [ "${parallel:-0}" != "0" ] && break
	sleep 0.5
done

echo "== the statistics of the slots"
pub -d postgres -c "SELECT slot_name, total_txns, total_bytes FROM pg_stat_replication_slots ORDER BY slot_name;"

[ "${serial:-0}" != "0" ] || fail "the serial slot reports no transactions"
[ "${parallel:-0}" != "0" ] || fail "the parallel slot reports no transactions"

for slot in s_serial s_par; do
	bytes=$(pub -d postgres -c "SELECT total_bytes FROM pg_stat_replication_slots WHERE slot_name = '$slot';")
	[ "${bytes:-0}" != "0" ] || fail "the slot $slot reports no bytes"
done

echo "== a clean shutdown of the publisher"
"$PGBIN/pg_ctl" -D "$PDTEST_ROOT/pub" -m fast -w stop >/dev/null || fail "the publisher did not stop"

#
# The statistics of the publisher are written at shutdown, and the writer
# looks up the name of every slot of the statistics.  An index that belongs
# to no slot makes it read out of bounds, and the publisher dies with it.
if grep -qE "信号 11|段错误|Segmentation" "$PDTEST_ROOT/pub/server.log"; then
	echo "--- the log of the publisher ---" >&2
	tail -20 "$PDTEST_ROOT/pub/server.log" >&2
	fail "the publisher crashed at shutdown"
fi

echo "PASS"
