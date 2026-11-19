#!/bin/bash
# Verify that the publisher-side parallel decoding path works together with
# the initial table synchronization of a subscription.
#
# The initial synchronization has two phases on the subscriber side:
#
#   1. the tablesync worker copies the rows of the table.  It does that with
#      a plain "COPY ... TO STDOUT" on the publisher, over a connection that
#      is not a replication connection at all, so no decoding happens for it;
#   2. the tablesync worker then starts a streaming replication connection to
#      catch up with the apply worker.  That connection is a logical
#      replication connection, so the publisher-side parallel decoding path
#      runs for it as well: one walreader and N decode workers, in addition to
#      the ones the apply connection has.
#
# The test therefore checks three things:
#
#   1. the rows of the table arrive at the subscriber,
#   2. the tablesync worker reaches the "ready" state, and its process ends,
#   3. the changes that are written after the synchronization arrive too.
#
# When the synchronization does not finish in time, the test dumps the state
# of both clusters: the activity of the processes, the replication slots, and
# the background workers of the publisher.  That is the information that is
# needed to tell the phases apart.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
export PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
WORKERS=${WORKERS:-4}
#
# The number of rows that are written before the subscription is created.
ROWS=${ROWS:-1000}
#
# The streaming mode the subscription asks for.  "parallel" is the mode the
# feature is built for; "on" is the mode that is used by the other smoke
# scripts.
STREAMING=${STREAMING:-parallel}
#
# How long the test waits for the initial synchronization to finish.
WAIT=${WAIT:-60}

export PATH="$PGBIN:$PATH"

pub() { psql -h "$ROOT" -p "$PORT_PUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }
sub() { psql -h "$ROOT" -p "$PORT_SUB" -U postgres -v ON_ERROR_STOP=1 -q "$@"; }

fail() {
	echo "FAIL: $*" >&2
	dump_state
	exit 1
}

# The state of the synchronization of the tables of the subscription.
relstate() {
	sub -At -F' ' -c \
		"select srsubstate from pg_subscription_rel sr
		 where sr.srsubid = (select oid from pg_subscription where subname = 'pd_sub')
		   and sr.srrelid = 'pd_t'::regclass"
}

# The number of the rows the subscriber has.
rowcount() {
	sub -At -c "select count(*) from pd_t"
}

# The state of the tablesync worker of the subscriber.
tablesync_state() {
	sub -At -F'|' -c \
		"select pid, state, coalesce(wait_event_type,''), coalesce(wait_event,''),
		        coalesce(left(query,60),'')
		 from pg_stat_activity
		 where backend_type = 'logical replication tablesync worker'"
}

# Dump everything that is needed to tell where the synchronization is.
dump_state() {
	echo "--- the state of the subscriber" >&2
	sub -c "select pid, backend_type, state, wait_event_type, wait_event,
				   left(coalesce(query,''),80) as query
			from pg_stat_activity
			where backend_type like 'logical replication%'
			   or backend_type = 'walsender'
			order by backend_type, pid" >&2 || true
	echo "--- the synchronization state of the table" >&2
	sub -c "select srrelid::regclass, srsubstate, srsublsn
			from pg_subscription_rel" >&2 || true
	echo "--- the state of the publisher" >&2
	pub -c "select pid, backend_type, state, wait_event_type, wait_event,
				   left(coalesce(query,''),80) as query
			from pg_stat_activity
			where backend_type like '%logical decoding%'
			   or backend_type = 'walsender'
			   or application_name like 'pg_%'
			order by backend_type, pid" >&2 || true
	echo "--- the replication slots of the publisher" >&2
	pub -c "select slot_name, plugin, active, restart_lsn, confirmed_flush_lsn,
				   catalog_xmin from pg_replication_slots" >&2 || true
	echo "--- the replication state on the publisher" >&2
	pub -c "select application_name, state, sent_lsn, write_lsn, flush_lsn,
				   replay_lsn from pg_stat_replication" >&2 || true
	echo "--- the background workers of the publisher" >&2
	ps -eo pid,stat,args | grep -E '[p]arallel decode (walreader|worker)' >&2 || true
	echo "--- the tablesync worker of the subscriber" >&2
	ps -eo pid,stat,args | grep -E '[l]ogical replication tablesync' >&2 || true
}

echo "== setting up the clusters ($WORKERS decode worker(s))"
MAX_PARALLEL_DECODE_WORKERS=$WORKERS "$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start

echo "== creating the schema"
pub -c "create table pd_t (id int primary key, v text)"
pub -c "insert into pd_t select g, 'v' || g from generate_series(1, $ROWS) g"
pub -c "create publication pd_pub for all tables"

echo "== creating the subscription (copy_data = true, streaming = '$STREAMING')"
sub -c "create table pd_t (id int primary key, v text)"
sub -c "create subscription pd_sub
		connection 'host=$ROOT port=$PORT_PUB user=postgres dbname=postgres'
		publication pd_pub
		with (streaming = '$STREAMING', copy_data = true)"

echo "== waiting for the initial synchronization"
i=0
state=""
while [ "$i" -lt "$WAIT" ]; do
	sleep 1
	i=$((i + 1))
	state=$(relstate 2>/dev/null || echo "")
	[ "$state" = "r" ] && break
done

if [ "$state" != "r" ]; then
	echo "   after ${i}s the synchronization is in the state '$state'" >&2
	echo "   the tablesync worker: $(tablesync_state 2>/dev/null || echo 'gone')" >&2
	fail "the initial synchronization did not finish in ${WAIT}s"
fi
echo "   the synchronization is ready after ${i}s"

n=$(rowcount)
[ "$n" = "$ROWS" ] || fail "the subscriber has $n rows instead of $ROWS"

# The rows have to be the ones the publisher has, not just the right number.
pub_sum=$(pub -At -c "select coalesce(sum(id),0) || ':' || count(*) from pd_t")
sub_sum=$(sub -At -c "select coalesce(sum(id),0) || ':' || count(*) from pd_t")
[ "$pub_sum" = "$sub_sum" ] ||
	fail "the rows differ (the publisher has $pub_sum, the subscriber has $sub_sum)"

echo "== writing a change after the synchronization"
pub -c "insert into pd_t values ($((ROWS + 1)), 'after')"
i=0
while [ "$i" -lt 30 ]; do
	sleep 1
	i=$((i + 1))
	[ "$(rowcount)" = "$((ROWS + 1))" ] && break
done
[ "$(rowcount)" = "$((ROWS + 1))" ] ||
	fail "the change after the synchronization did not arrive"

echo "== OK"
sub -c "drop subscription pd_sub" >/dev/null 2>&1 || true
"$SCRIPT_DIR/setup_env.sh" stop
