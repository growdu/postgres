#!/bin/bash
# Verify that the publisher-side parallel decoding path sends the definition
# of a relation once, like the serial decoding path does.
#
# Every decode worker of the parallel path has its own relation cache, so
# without a shared table of the relations that the session has sent already,
# every worker that touches a relation sends the definition of that relation
# and the subscriber receives it as many times as there are workers.
#
# The workload of this test is a number of separate transactions that all
# change the same table, so that the transactions are spread over the
# workers: the transaction goes to the worker that has the fewest bytes in
# flight, and the transactions of the workload are of the same size.
#
# The test runs the workload twice, once with the serial decoding path and
# once with the parallel one, and compares the number of the relation
# messages in the two trace files.  Both numbers have to be one.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
#
# The number of transactions of the workload.  It has to be larger than the
# number of the workers, so that every worker gets at least one transaction.
TXNS=${TXNS:-40}
WORKERS=${WORKERS:-4}
#
# The number of the sessions that write the workload at the same time.  The
# transaction goes to the worker that has the fewest bytes in flight, and the
# bytes of a transaction are only released when it ends, so the transactions
# have to be in flight at the same time to be spread over the workers.
CONC=${CONC:-8}
#
# The number of the rows of the workload.
EXPECTED=$((CONC * TXNS))

export PATH="$PGBIN:$PATH"

TRACE_SERIAL=$ROOT/rel_serial.trace
TRACE_PARALLEL=$ROOT/rel_parallel.trace

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

set_trace() {
	# set_trace <file> <max_parallel_decode_workers>
	cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$1'
max_parallel_decode_workers = $2
EOF
	pg_ctl -D "$ROOT/pub" reload >/dev/null
}

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id bigserial primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating two slots at the same position"
pub <<'EOF'
begin;
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput');
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_b', 'pgoutput');
commit;
EOF

echo "== writing the workload ($TXNS transactions)"
sub -c "create table pd_t (id bigserial primary key, v text)"
#
# The workload is written before the two runs, so that both slots decode the
# same transactions.  The sessions write at the same time and keep their
# transactions open for a moment, so that several transactions are in flight
# at the same time and the workers get a transaction each.
workload_client() {
	local client=$1 i
	for i in $(seq 1 "$TXNS"); do
		pub -c "begin; insert into pd_t (v) values ('v$client'); select pg_sleep(0.2); commit;" >/dev/null
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

run() {
	# run <slot> <tracefile> <workers>
	local slot=$1 trace=$2 workers=$3
	local i

	sub -c "truncate pd_t"
	rm -f "$trace"
	set_trace "$trace" "$workers"
	sub -c "create subscription $slot connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"
	for i in $(seq 1 60); do
		n=$(sub -At -c "select count(*) from pd_t" 2>/dev/null || echo 0)
		[ "$n" = "$EXPECTED" ] && break
		sleep 1
	done
	[ "$n" = "$EXPECTED" ] || fail "the subscriber did not catch up (expected $EXPECTED rows, got $n)"
	sub -c "drop subscription $slot" >/dev/null 2>&1
	sleep 2
	[ -s "$trace" ] || fail "the run produced no trace file"
}

count_relations() {
	# count_relations <tracefile>
	python3 - "$1" "$SCRIPT_DIR/pd_frame_tool.py" <<-'EOF'
	import importlib.util, sys
	sys.dont_write_bytecode = True
	spec = importlib.util.spec_from_file_location("pdft", sys.argv[2])
	mod = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(mod)
	frames = mod.read_trace(sys.argv[1])
	streaming = False
	count = 0
	for frame in frames:
	    if frame.type != "w":
	        continue
	    messages, streaming = mod.parse_messages(frame.payload, streaming)
	    for tag, raw, desc in messages:
	        if tag == mod.MSG_RELATION:
	            count += 1
	print(count)
	EOF
}

echo "== run 1: serial decoding path"
run pd_slot_a "$TRACE_SERIAL" 0
serial_count=$(count_relations "$TRACE_SERIAL")
echo "   the serial path sent $serial_count relation message(s)"

echo "== run 2: parallel decoding path ($WORKERS workers)"
run pd_slot_b "$TRACE_PARALLEL" "$WORKERS"
parallel_count=$(count_relations "$TRACE_PARALLEL")
echo "   the parallel path sent $parallel_count relation message(s)"

if [ "$serial_count" != "1" ]; then
	fail "the serial path sent $serial_count relation messages instead of one"
fi
if [ "$parallel_count" != "1" ]; then
	fail "the parallel path sent $parallel_count relation messages instead of one"
fi

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
