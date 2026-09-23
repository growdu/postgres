#!/usr/bin/env bash
#---------------------------------------------------------------------
# logical_rep_test.sh — build a publisher/subscriber cluster with
# pg_lrstat preloaded on both ends, drive a pgbench load against the
# publisher, and continuously observe the replication rates while the
# load runs.  Verifies that the statistics machinery behaves sanely
# (views populate, rates non-zero under load, backlogs consistent).
#
# Usage:
#   ./logical_rep_test.sh [duration_seconds] [sample_every_seconds]
#
# Environment overrides:
#   BINDIR       postgres/bin directory          (default /usr/local/pgsql/bin)
#   SOCKROOT     socket dir base                  (default /tmp)
#   PORT_BASE    base port                        (default 55470)
#   SCALE        pgbench scale factor             (default 5)
#   CLIENTS      pgbench clients                  (default 4)
#   KEEP         =1 keep the cluster on failure   (default 0)
#
# The script needs no root and no installed cluster; it creates both
# nodes under a temp directory with trust auth on a unix socket.
#---------------------------------------------------------------------
set -euo pipefail

DURATION=${1:-60}
SAMPLE_EVERY=${2:-5}
: "${BINDIR:=/usr/local/pgsql/bin}"
: "${SOCKROOT:=/tmp}"
: "${PORT_BASE:=55470}"
: "${SCALE:=5}"
: "${CLIENTS:=4}"
: "${KEEP:=0}"

P_PORT=$PORT_BASE
S_PORT=$((PORT_BASE + 1))
TS=$(date +%H%M%S)
T=$(mktemp -d "$SOCKROOT/lrbench.$TS.XXXX")
P_DATA=$T/pub
S_DATA=$T/sub
P_SOCK=$T
S_SOCK=$T
LOG=$T/observe.log

PSQL() { # PSQL <port> <db> <sql...>
	"$BINDIR/psql" -h "$T" -p "$1" -U postgres -d "$2" -X -qAt -c "$3"
}

cleanup() {
	rc=$?
	if [ $rc -ne 0 ] && [ "$KEEP" = 1 ]; then
		echo "KEEP=1: cluster kept at $T (pub port $P_PORT, sub port $S_PORT)"
		exit $rc
	fi
	"$BINDIR/pg_ctl" -D "$P_DATA" stop -m immediate >/dev/null 2>&1 || true
	"$BINDIR/pg_ctl" -D "$S_DATA" stop -m immediate >/dev/null 2>&1 || true
	rm -rf "$T"
	exit $rc
}
trap cleanup EXIT INT TERM

echo "== init publisher (port $P_PORT) and subscriber (port $S_PORT) under $T"
"$BINDIR/initdb" -D "$P_DATA" -U postgres -A trust >/dev/null
"$BINDIR/initdb" -D "$S_DATA" -U postgres -A trust >/dev/null

cat >>"$P_DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_lrstat'
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
pg_lrstat.sample_interval = '2s'
pg_lrstat.rate_window = '10s'
pg_lrstat.remote_poll_budget = '2s'
listen_addresses = ''
unix_socket_directories = '$P_SOCK'
port = $P_PORT
EOF
cat >>"$S_DATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.sample_interval = '2s'
pg_lrstat.rate_window = '10s'
pg_lrstat.remote_poll_budget = '2s'
listen_addresses = ''
unix_socket_directories = '$S_SOCK'
port = $S_PORT
EOF

"$BINDIR/pg_ctl" -D "$P_DATA" -l "$T/pub.log" -w start >/dev/null
"$BINDIR/pg_ctl" -D "$S_DATA" -l "$T/sub.log" -w start >/dev/null

echo "== create extension / schema / publication / subscription"
PSQL $P_PORT postgres "CREATE EXTENSION pg_lrstat;"
PSQL $S_PORT postgres "CREATE EXTENSION pg_lrstat;"

# pgbench schema on both ends; the subscriber's copy is emptied so the
# initial table sync does not collide with pre-existing primary keys.
"$BINDIR/pgbench" -h "$T" -p $P_PORT -U postgres -i -s $SCALE -q postgres >/dev/null
"$BINDIR/pgbench" -h "$T" -p $S_PORT -U postgres -i -s 1 -q postgres >/dev/null
PSQL $S_PORT postgres "TRUNCATE pgbench_accounts, pgbench_branches, pgbench_tellers, pgbench_history;"

PSQL $P_PORT postgres "CREATE PUBLICATION lrbench_pub FOR ALL TABLES;"
PSQL $S_PORT postgres \
	"CREATE SUBSCRIPTION lrbench_sub CONNECTION 'host=$T port=$P_PORT dbname=postgres user=postgres' PUBLICATION lrbench_pub;"

echo "== wait for initial sync"
for i in $(seq 1 120); do
	n=$(PSQL $S_PORT postgres \
		"SELECT count(*) FROM pg_stat_subscription WHERE subname='lrbench_sub' AND pid IS NOT NULL AND received_lsn IS NOT NULL;")
	synced=$(PSQL $P_PORT postgres \
		"SELECT count(*) FROM pg_replication_slots WHERE slot_name='lrbench_sub' AND active;")
	[ "$n" -ge 1 ] && [ "$synced" -ge 1 ] && break
	sleep 1
done
# let the apply worker make some progress before the load
sleep 5

echo "== run pgbench -T $DURATION and observe every ${SAMPLE_EVERY}s (log: $LOG)"
"$BINDIR/pgbench" -h "$T" -p $P_PORT -U postgres -c $CLIENTS -j 2 -T $DURATION postgres \
	>"$T/pgbench.out" 2>&1 &
BENCH=$!

observe() {
	local ts=$1
	# publisher: generation / send rates and slot facts
	local pub
	pub=$(PSQL $P_PORT postgres \
		"SELECT coalesce(round(max(gen_rate)::numeric,2),0)||'/'||coalesce(round(max(send_rate)::numeric,2),0),
		        coalesce(max(backlog_unsent),0), coalesce(max(retained_wal),0)
		 FROM pg_lrstat_pub_rate r JOIN pg_lrstat_pub_sample s USING (slot_name)
		 WHERE slot_name='lrbench_sub';" 2>/dev/null || echo '?')
	# subscriber: both ends composed
	local sub
	sub=$(PSQL $S_PORT postgres \
		"SELECT remote_state||'|'||
		        coalesce(round(gen_rate::numeric,2),0)||'/'||
		        coalesce(round(send_rate::numeric,2),0)||'/'||
		        coalesce(round(recv_rate::numeric,2),0)||'/'||
		        coalesce(round(apply_rate::numeric,2),0),
		        coalesce(backlog_unsent,0)+backlog_inflight+backlog_unapplied,
		        coalesce(backlog_total,-1),
		        coalesce(round(eta_total::numeric),-1)
		 FROM pg_lrstat_overall WHERE sub_name='lrbench_sub';" 2>/dev/null || echo '?')
	echo "$ts pub[gen/send MB/s=$pub] sub[$sub]" | tee -a "$LOG"
}

t_end=$((SECONDS + DURATION))
while kill -0 $BENCH 2>/dev/null && [ $SECONDS -lt $t_end ]; do
	observe "$(date +%T)"
	sleep $SAMPLE_EVERY
done
wait $BENCH || true

# a few more rounds so the drain phase is captured too
for i in 1 2 3 4; do
	observe "$(date +%T) (drain)"
	sleep $SAMPLE_EVERY
done

echo "== health checks"
info_p=$(PSQL $P_PORT postgres "SELECT loaded||'/'||last_round_ok||'/'||dropped_samples FROM pg_lrstat_info;")
info_s=$(PSQL $S_PORT postgres "SELECT loaded||'/'||last_round_ok||'/'||dropped_samples FROM pg_lrstat_info;")
echo "pg_lrstat_info  publisher[loaded/last_round_ok/dropped = $info_p]  subscriber[$info_s]"

rows_pub=$(PSQL $P_PORT postgres "SELECT count(*) FROM pg_lrstat_pub_sample WHERE slot_name='lrbench_sub';")
rows_sub=$(PSQL $S_PORT postgres "SELECT count(*) FROM pg_lrstat_sub_sample WHERE sub_name='lrbench_sub';")
lag=$(PSQL $S_PORT postgres \
	"SELECT pg_size_pretty(coalesce(max(backlog_total),0)) FROM pg_lrstat_overall WHERE sub_name='lrbench_sub';")
echo "views: pub_sample=$rows_pub row(s), sub_sample=$rows_sub row(s), final backlog_total=$lag"

echo "== assertions"
fail=0
# 1) both sides loaded, sampler healthy, nothing dropped
[ "$(echo "$info_p" | cut -d/ -f1)" = true ] || { echo "FAIL: publisher not loaded"; fail=1; }
[ "$(echo "$info_s" | cut -d/ -f1)" = true ] || { echo "FAIL: subscriber not loaded"; fail=1; }
[ "$(echo "$info_s" | cut -d/ -f2)" = true ] || { echo "FAIL: subscriber sampler round failed"; fail=1; }
[ "$(echo "$info_p" | cut -d/ -f3)" = 0 ] || { echo "FAIL: publisher dropped samples"; fail=1; }
[ "$(echo "$info_s" | cut -d/ -f3)" = 0 ] || { echo "FAIL: subscriber dropped samples"; fail=1; }
# 2) during load the subscriber observed remote publisher + non-zero rates
ok_rows=$(grep -c "sub\[ok|" "$LOG" || true)
[ "${ok_rows:-0}" -ge 1 ] || { echo "FAIL: no observation with remote_state=ok"; fail=1; }
grep -qE "sub\[ok\|[0-9.]+/[0-9.]+/[0-9.]+" "$LOG" || true
gen_ok=$(awk -F'[=/]' '/sub\[ok\|/{for(i=1;i<=NF;i++) if($i+0>0){print "y";exit}}' "$LOG" || true)
[ "${gen_ok:-n}" = y ] || { echo "FAIL: gen_rate never observed non-zero"; fail=1; }
# 3) backlog composition invariant on the final row (sum == total)
last=$(grep "sub\[ok|" "$LOG" | tail -1)
sum=$(echo "$last" | sed -E 's/.*,[[:space:]]*([0-9]+),[[:space:]]*([0-9]+|-[0-9]+),.*/\1/')
tot=$(echo "$last" | sed -E 's/.*,[[:space:]]*([0-9]+|-[0-9]+)\]$/\1/')
if [ "$sum" != "$tot" ]; then
	echo "FAIL: stage sum ($sum) != backlog_total ($tot) in: $last"
	fail=1
fi

if [ $fail -eq 0 ]; then
	echo "PASS: pg_lrstat statistics behaved correctly under load"
else
	echo "RESULT: FAILURES DETECTED (cluster kept: $([ "$KEEP" = 1 ] && echo yes || echo no))"
	exit 1
fi
