#!/bin/bash
# AC-5: a workload that mixes DDL and DML.
#
# The parallel decoding path gives every decode worker its own catalog
# caches, so a DDL that happens while the session is running has to reach
# all of them.  A worker that keeps a stale relation would decode the
# changes of the new shape with the old metadata, and the bytes it would
# produce would differ from the ones the serial path produces.
#
# PostgreSQL does not replicate DDL ("The database schema and DDL commands
# are not replicated"), so the tables of the subscriber have to exist
# before the subscription starts.  The subscriber gets them with the shape
# the workload ends up with, the publisher starts with the old shape: the
# replicated tuples are matched by column name, so the columns the
# subscriber has and the publisher does not yet have stay empty.
#
# The workload is written while the first subscription is streaming, so
# that the DDL is decoded while the session is running.  The second run
# decodes the same WAL afterwards, from the other slot, so the two traces
# hold the same messages.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}

WORKERS=${WORKERS:-4}

TRACE_SERIAL=$ROOT/serial.trace
TRACE_PARALLEL=$ROOT/parallel.trace

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	exit 1
}

wait_for_rows() {
	# wait_for_rows <expected>
	# The count covers all the tables of the workload, so that the test does
	# not measure a subscriber that is still applying the tail of the
	# workload.
	local expected=$1 i n
	for i in $(seq 1 120); do
		n=$(sub -At -c "select (select count(*) from pd_t) + (select count(*) from pd_t2) + (select count(*) from pd_t3)" 2>/dev/null || echo 0)
		if [ "$n" = "$expected" ]; then
			return 0
		fi
		sleep 1
	done
	fail "the subscriber did not catch up (expected $expected rows)"
}

set_trace() {
	# set_trace <file> <max_parallel_decode_workers>
	cat >>"$ROOT/pub/postgresql.conf" <<EOF
parallel_decode_trace_file = '$1'
max_parallel_decode_workers = $2
EOF
	pg_ctl -D "$ROOT/pub" reload >/dev/null
}

# The workload.  It changes the shape of a table while the session is
# running, creates a table, and creates and drops an index.
#
# The name of the subscription is needed, because the subscriber only
# replicates the tables that are in its subscription, and the list of those
# tables is fixed when the subscription is created.  A table that appears on
# the publisher afterwards is only picked up by a refresh of the
# publication, so the workload has to ask for one.  Without it the
# subscriber silently drops the changes of the new table, which is the
# behaviour of PostgreSQL and not of the parallel decoding path.
workload() {
	# workload <subscription>
	local subname=$1

	pub -c "insert into pd_t select g, 'v' || g from generate_series(1, 10) g"

	# A shape change: the column is added while the session is running.
	pub -c "insert into pd_t3 values (1, 'v1')"
	pub -c "alter table pd_t3 add column c1 int"
	pub -c "insert into pd_t3 values (2, 'v2', 42)"

	# A table that is created and filled while the session is running.
	pub -c "create table pd_t2 (id int primary key, v text)"
	sub -c "alter subscription $subname refresh publication with (copy_data = false)"
	pub -c "insert into pd_t2 values (1, 'a'), (2, 'b')"

	# An index that is created and dropped again.
	pub -c "create index pd_t_v_idx on pd_t (v)"
	pub -c "insert into pd_t values (11, 'v11')"
	pub -c "drop index pd_t_v_idx"
}

# The data of a table, in a form that does not depend on the column order.
table_sum() {
	# table_sum <relation>
	local rel=$1
	psql -h "$ROOT" -p "$PORT_SUB" -U postgres -At -q \
		-c "select count(*), sum(id), md5(string_agg(v, ',' order by id)) from $rel" \
		2>/dev/null || echo "missing"
}

echo "== setting up the clusters"
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema on the publisher"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "create table pd_t3 (id int primary key, v text)"
pub -c "create publication pd_pub for all tables"

echo "== creating the schema on the subscriber"
# The subscriber gets the shape the workload ends up with.  The publisher
# only adds the column while the session is running.
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create table pd_t2 (id int primary key, v text)"
sub -c "create table pd_t3 (id int primary key, v text, c1 int)"

echo "== creating two slots at the same position"
pub <<'EOF'
begin;
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_a', 'pgoutput');
select slot_name, lsn from pg_create_logical_replication_slot('pd_slot_b', 'pgoutput');
commit;
EOF

# ---------------------------------------------------------------------------
# Run 1: the serial decoding path.  The workload is written while the
# subscription is streaming.
# ---------------------------------------------------------------------------
echo "== run 1: serial decoding path"
rm -f "$TRACE_SERIAL"
set_trace "$TRACE_SERIAL" 0
sub -c "create subscription pd_slot_a connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"

sleep 3

echo "== writing the workload"
workload pd_slot_a

ROWS=$(pub -At -c "select (select count(*) from pd_t) + (select count(*) from pd_t2) + (select count(*) from pd_t3)")
echo "   $ROWS rows in the tables of the workload"

wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_a"
sleep 2
[ -s "$TRACE_SERIAL" ] || fail "the serial run produced no trace file"

serial_t=$(table_sum pd_t)
serial_t2=$(table_sum pd_t2)
serial_t3=$(table_sum pd_t3)
echo "   the subscriber holds pd_t=$serial_t pd_t2=$serial_t2 pd_t3=$serial_t3"

# ---------------------------------------------------------------------------
# Run 2: the parallel decoding path.  It decodes the same WAL, including the
# DDL, from the other slot.
# ---------------------------------------------------------------------------
echo "== run 2: parallel decoding path ($WORKERS worker(s))"
rm -f "$TRACE_PARALLEL"
sub -c "truncate pd_t, pd_t2, pd_t3"
set_trace "$TRACE_PARALLEL" "$WORKERS"
sub -c "create subscription pd_slot_b connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres' publication pd_pub with (create_slot = false, streaming = 'on', copy_data = false)"

wait_for_rows "$ROWS"
sub -c "drop subscription pd_slot_b"
sleep 2
[ -s "$TRACE_PARALLEL" ] || fail "the parallel run produced no trace file"

# ---------------------------------------------------------------------------
# Compare the two traces.
# ---------------------------------------------------------------------------
echo "== comparing the traces"
python3 "$SCRIPT_DIR/pd_frame_tool.py" diff "$TRACE_SERIAL" "$TRACE_PARALLEL" --messages \
	|| fail "the parallel path does not produce the same bytes as the serial path"

# The data has to match as well: the tuples have to be decoded with the
# shape the publisher had when it wrote them.
check_table() {
	# check_table <relation> <serial value>
	local rel=$1 serial=$2 parallel
	parallel=$(table_sum "$rel")
	if [ "$serial" != "$parallel" ]; then
		echo "   serial run: $rel = $serial"
		echo "   parallel run: $rel = $parallel"
		fail "the parallel run did not replicate the same data as the serial run"
	fi
	echo "   $rel matches: $parallel"
}

check_table pd_t "$serial_t"
check_table pd_t2 "$serial_t2"
check_table pd_t3 "$serial_t3"

echo "== OK"
"$SCRIPT_DIR/setup_env.sh" stop
