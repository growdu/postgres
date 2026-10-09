# Copyright (c) 2026, PostgreSQL Global Development Group

# Test that pg_lrstat handles table sync workers: the subscriber must not
# crash while a "table synchronization" worker runs (it did in early
# versions), the worker must be visible in recv_stat with its target
# table, history must record samples during the sync, and everything
# must settle back to the plain apply worker afterwards.
#
# The first table synchronizes normally.  A second table is added to the
# publication afterwards; its sync worker is kept alive deterministically
# by an open exclusive-lock transaction on the subscriber-side target
# table (the sync's COPY cannot start), so the worker can be inspected
# while it exists.

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf(
	'postgresql.conf', qq(
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.sample_interval = '1s'
));
$node_publisher->start;
$node_publisher->safe_psql('postgres', 'CREATE EXTENSION pg_lrstat');

my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf(
	'postgresql.conf', qq(
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.sample_interval = '1s'
pg_lrstat.stale_target_ttl = '5s'
));
$node_subscriber->start;
$node_subscriber->safe_psql('postgres', 'CREATE EXTENSION pg_lrstat');

# ---- schema verification -------------------------------------------
# Every view's column list (order, name, type) is pinned here; any
# signature drift in pg_lrstat--2.1.sql fails these assertions.
my %expected_cols = (
	pg_lrstat_info => [
		qw(loaded:boolean session_name:text session_state:text
		   session_truncated:boolean session_degraded:boolean
		   session_start_ts:timestamptz session_stop_ts:timestamptz
		   sample_interval_ms:bigint last_round_ts:timestamptz
		   last_round_ok:boolean last_round_error:text nrounds:bigint
		   dropped_samples:bigint remote_poll:boolean
		   exported_report_names:text[])],
	pg_lrstat_send_stat => [
		qw(slot_name:text ts:timestamptz plugin:text temporary:boolean
		   active:boolean sender_pid:integer application_name:text
		   client_addr:text state:text sync_state:text wal_status:text
		   current_lsn:pg_lsn sent_lsn:pg_lsn
		   confirmed_flush_lsn:pg_lsn backlog_unsent:double precision
		   backlog_inflight:double precision
		   backlog_peer_unapplied:double precision
		   backlog_total:double precision retained_wal:double precision
		   gen_mbps:double precision send_mbps:double precision
		   apply_mbps:double precision spill_mb:double precision
		   write_lag:interval flush_lag:interval replay_lag:interval
		   send_blocked:boolean)],
	pg_lrstat_recv_stat => [
		qw(recv_name:text ts:timestamptz worker_type:text
		   worker_pid:integer leader_pid:integer relid:oid
		   received_lsn:pg_lsn applied_lsn:pg_lsn
		   last_msg_send_time:timestamptz
		   last_msg_receipt_time:timestamptz
		   backlog_apply:double precision recv_mbps:double precision
		   apply_mbps:double precision local_wal_mbps:double precision
		   apply_error_count:bigint sync_error_count:bigint
		   apply_blocked:boolean)],
	pg_lrstat_cluster_stat => [
		qw(recv_name:text ts:timestamptz remote_state:text
		   send_current_lsn:pg_lsn sent_lsn:pg_lsn
		   received_lsn:pg_lsn applied_lsn:pg_lsn
		   confirmed_flush_lsn:pg_lsn gen_mbps:double precision
		   send_mbps:double precision recv_mbps:double precision
		   apply_mbps:double precision backlog_unsent:double precision
		   backlog_inflight:double precision
		   backlog_unapplied:double precision
		   backlog_total:double precision
		   retained_wal:double precision
		   feedback_lag_mb:double precision write_lag:interval
		   flush_lag:interval replay_lag:interval
		   catchup_send_secs:double precision
		   catchup_total_secs:double precision
		   send_blocked:boolean apply_blocked:boolean
		   bottleneck:text)],
	pg_lrstat_send_history => [
		qw(name:text ts:timestamptz current_lsn:pg_lsn
		   sent_lsn:pg_lsn peer_recv_lsn:pg_lsn
		   peer_flush_lsn:pg_lsn peer_applied_lsn:pg_lsn
		   confirmed_flush_lsn:pg_lsn restart_lsn:pg_lsn
		   spill_bytes:bigint stream_bytes:bigint)],
	pg_lrstat_recv_history => [
		qw(name:text ts:timestamptz received_lsn:pg_lsn
		   applied_lsn:pg_lsn local_wal_lsn:pg_lsn)],
);
for my $view (sort keys %expected_cols)
{
	my $got = $node_subscriber->safe_psql('postgres', qq(
		SELECT string_agg(column_name || ':' ||
			CASE data_type
				WHEN 'character varying' THEN 'text'
				WHEN 'timestamp with time zone' THEN 'timestamptz'
				WHEN 'double precision' THEN 'double precision'
				ELSE data_type END,
			' ' ORDER BY ordinal_position)
		  FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = '$view'));
	my $got_norm = $got;
	$got_norm =~ s/\btimestamp with time zone\b/timestamptz/g;
	$got_norm =~ s/\bcharacter varying\b/text/g;
	$got_norm =~ s/\bARRAY(?!\()\b/text[]/g;
	my $want = join(' ', @{ $expected_cols{$view} });
	my $ncols = () = $want =~ /:/g;
	is($got_norm, $want, "schema of $view ($ncols columns)");
}

# No sampling before lrstat_start(): views stay empty on a fresh
# extension (regression guard for the ring surviving reinstalls).
sleep(3);
is($node_subscriber->safe_psql('postgres',
	q(SELECT count(*) FROM pg_lrstat_recv_history)),
	0, 'no samples before the first lrstat_start()');
is($node_subscriber->safe_psql('postgres',
	q(SELECT count(*) FROM pg_lrstat_recv_stat)),
	0, 'recv_stat empty before the first lrstat_start()');

# The first table synchronizes as part of CREATE SUBSCRIPTION.
$node_publisher->safe_psql(
	'postgres', q(
	CREATE TABLE lrstat_test (a int PRIMARY KEY, b text);
	INSERT INTO lrstat_test
	  SELECT g, repeat(md5(g::text), 50) FROM generate_series(1, 50000) g;
	CREATE PUBLICATION lrstat_pub FOR TABLE lrstat_test;
));
$node_subscriber->safe_psql('postgres',
	'CREATE TABLE lrstat_test (a int PRIMARY KEY, b text)');

my $publisher_connstr = $node_publisher->connstr;
$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SUBSCRIPTION lrstat_sub CONNECTION '$publisher_connstr'
	PUBLICATION lrstat_pub
));
$node_subscriber->wait_for_subscription_sync($node_publisher, 'lrstat_sub');

is($node_subscriber->safe_psql('postgres', 'SELECT count(*) FROM lrstat_test'),
	50000, 'initial table sync completed');

# A second table in the publication triggers a fresh table sync worker.
$node_publisher->safe_psql(
	'postgres', q(
	CREATE TABLE lrstat_test2 (a int PRIMARY KEY, b text);
	INSERT INTO lrstat_test2
	  SELECT g, repeat(md5(g::text), 50) FROM generate_series(50001, 100000) g;
	ALTER PUBLICATION lrstat_pub ADD TABLE lrstat_test2;
));
$node_subscriber->safe_psql('postgres',
	'CREATE TABLE lrstat_test2 (a int PRIMARY KEY, b text)');

# Sampling only happens inside a session: start it before triggering
# the sync worker so the views can see it.
$node_subscriber->safe_psql('postgres', 'SELECT lrstat_start()');

# Keep the new sync worker alive while we inspect it: its COPY needs an
# ACCESS EXCLUSIVE lock on the subscriber-side target table.  Holding a
# plain EXCLUSIVE lock blocks it deterministically while still letting
# REFRESH PUBLICATION through (which only needs ACCESS SHARE; and locking
# the publisher side instead would block the sync slot's
# consistent-point search and hang REFRESH itself).
my $lockholder = $node_subscriber->background_psql('postgres');
$lockholder->query_safe('BEGIN');
$lockholder->query_safe('LOCK TABLE lrstat_test2 IN EXCLUSIVE MODE');

$node_subscriber->safe_psql('postgres',
	'ALTER SUBSCRIPTION lrstat_sub REFRESH PUBLICATION');

# The table sync worker must show up in recv_stat with its target table.
my $result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_recv_stat
	WHERE worker_type = 'table synchronization' AND relid IS NOT NULL
));
is($result, 1, 'table sync worker visible in pg_lrstat_recv_stat');

# This is the spot where the subscriber used to crash.
is($node_subscriber->safe_psql('postgres', 'SELECT 1'),
	1, 'subscriber alive during table sync');

# History keeps recording samples while the sync worker exists.
$result = $node_subscriber->poll_query_until(
	'postgres', qq(SELECT count(*) > 0 FROM pg_lrstat_recv_history));
is($result, 1, 'recv_history records samples during table sync');

# The session started before the sync worker; confirm it is running
my $sess_sync = $node_subscriber->safe_psql('postgres',
	'SELECT session_name FROM pg_lrstat_info');
chomp $sess_sync;
is($node_subscriber->safe_psql('postgres',
	q(SELECT session_state FROM pg_lrstat_info)),
	'running', 'session running during table sync');
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_recv_stat
	WHERE worker_type = 'table synchronization' AND relid IS NOT NULL
));
is($result, 1, 'table sync worker re-registered after session start');

# Both ends must be visible at once: cluster_stat joins the subscriber
# side with the remotely polled publisher side.
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_cluster_stat
	WHERE recv_name = 'lrstat_sub' AND remote_state = 'ok'
));
is($result, 1, 'cluster_stat combines both ends during table sync');

# Release the lock and let the sync finish.
$lockholder->query_safe('COMMIT');
$lockholder->quit;

$node_subscriber->wait_for_subscription_sync($node_publisher, 'lrstat_sub');

is($node_subscriber->safe_psql('postgres', 'SELECT count(*) FROM lrstat_test2'),
	50000, 'second table replicated through its sync worker');

# Once the sync target expired (stale_target_ttl), the LATEST rows of
# the per-round time series are apply-only (sync rounds stay in history).
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT bool_and(worker_type = 'apply') FROM (
		SELECT worker_type FROM pg_lrstat_recv_stat
		ORDER BY ts DESC LIMIT 3
	) latest
));
is($result, 1, 'latest recv_stat rows are apply-only after sync');

# The subscription's slot shows up in send_stat via the polled mirror
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_send_stat WHERE slot_name = 'lrstat_sub'
));
is($result, 1, 'subscription slot visible in pg_lrstat_send_stat');

# Export still works after a full sync cycle.
$node_subscriber->safe_psql('postgres', 'SELECT lrstat_stop()');
is($node_subscriber->safe_psql('postgres',
	qq(SELECT length(lrstat_export('$sess_sync', 'json')) > 0)),
	't', 'json export works after table sync');

# ---- session-scoped data --------------------------------------------
# start/stop/export: the export's evidence tables must carry the raw
# samples the conclusions are built on

# a session with some load so the report has intervals to show
my $sess_archived = $node_subscriber->safe_psql('postgres',
	'SELECT lrstat_start()');
chomp $sess_archived;
$node_publisher->safe_psql('postgres',
	q(INSERT INTO lrstat_test SELECT g, repeat(md5(g::text), 50)
	  FROM generate_series(100001, 110000) g));
$result = $node_subscriber->poll_query_until('postgres', qq(
	SELECT count(*) >= 3 FROM pg_lrstat_recv_history
));
is($result, 1, 'session records history samples');

# stat views are per-round time series with derived rates (guards
# the one-row-per-target and always-NULL-rate regressions)
$result = $node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) >= 3 FROM pg_lrstat_recv_stat));
is($result, 't', 'recv_stat has one row per round');
$result = $node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) >= 2 FROM pg_lrstat_recv_stat
	WHERE apply_mbps IS NOT NULL));
is($result, 't', 'apply_mbps derived from round 2 onwards');

# ---- record every table into the test report ----------------------
# Full dumps of all six views plus the exported HTML are appended to
# the test log and copied into the node's data directory so a failed
# run leaves the exact evidence behind.
my $datadir = $node_subscriber->data_dir;
my $report_dir = "$datadir/pg_lrstat_test_report";
mkdir($report_dir) unless -d $report_dir;
for my $view (
	qw(pg_lrstat_info pg_lrstat_send_stat pg_lrstat_recv_stat
	   pg_lrstat_cluster_stat pg_lrstat_send_history
	   pg_lrstat_recv_history))
{
	# schema line (every field with its type) + CSV with header row, so
	# the report shows all fields of the table with example values
	my $schema = $node_subscriber->safe_psql('postgres', qq(
		SELECT string_agg(column_name || ':' || data_type, ', '
			ORDER BY ordinal_position)
		  FROM information_schema.columns
		 WHERE table_schema = 'public' AND table_name = '$view'));
	my $dump = $node_subscriber->safe_psql('postgres',
		"COPY (SELECT * FROM $view) TO STDOUT WITH (FORMAT csv, HEADER true)");
	note("===== $view =====\n[$schema]\n$dump");
	PostgreSQL::Test::Utils::append_to_file("$report_dir/$view.csv",
		"[$schema]\n$dump\n");
}
$node_subscriber->safe_psql('postgres', 'SELECT lrstat_stop()');

# export the named session BEFORE starting another one: start() wipes
# the previous session's data, and the written report is the keeper
my $archived_path = $node_subscriber->safe_psql('postgres',
	"SELECT lrstat_export('$sess_archived', 'html')");
$archived_path =~ s/^\s+|\s+$//g;
like($archived_path, qr{pg_lrstat/exports/\Q$sess_archived\E\.html$},
	'export returns the report file path');
ok(-f $archived_path, 'named session exported to a report file');
my $archived_html = PostgreSQL::Test::Utils::slurp_file($archived_path);
# keep the exported report with the test evidence
PostgreSQL::Test::Utils::append_to_file(
	"$report_dir/exported_report.html", $archived_html);
note("exported HTML report saved to $report_dir/exported_report.html "
	. "(" . length($archived_html) . " bytes)");
ok(index($archived_html, '<h1>Evidence</h1>') >= 0
	&& index($archived_html, 'send history samples') >= 0
	&& index($archived_html, 'recv history samples') >= 0,
	'report carries the raw history evidence tables');
ok(index($archived_html, 'send rate history (evidence)') >= 0
	&& index($archived_html, 'recv rate history (evidence)') >= 0,
	'report carries the per-interval rate evidence tables');

# the JSON export of the same session (still before the next start)
my $json_path = $node_subscriber->safe_psql('postgres',
	"SELECT lrstat_export('$sess_archived', 'json')");
$json_path =~ s/^\s+|\s+$//g;
my $archived_json = PostgreSQL::Test::Utils::slurp_file($json_path);
ok(index($archived_json, '"name": "' . $sess_archived . '"') >= 0,
	'archived export identifies the session');
ok(index($archived_json, '"samples": [') >= 0
	&& $archived_json =~ /"kind": "(send|recv)"/,
	'archived export contains raw history samples');

# a newer session wipes the previous session's data (clean slate).
# start() wakes the sampler for an immediate first round, so the ring
# may already hold the NEW session's first row here — the old
# session's rows must be gone (hence the small bound, not exactly 0).
$node_subscriber->safe_psql('postgres', 'SELECT lrstat_start()');
is($node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) <= 2 FROM pg_lrstat_recv_history)),
	't', 'new start wipes the previous session data');
sleep(3);
$node_subscriber->safe_psql('postgres', 'SELECT lrstat_stop()');

# the wiped session is no longer exportable; its report file remains
my ($ret2, $out2, $err2) = $node_subscriber->psql('postgres',
	"SELECT lrstat_export('$sess_archived', 'json')");
isnt($ret2, 0, 'wiped session no longer exportable by name');
like($err2, qr/no data for session/,
	'error mentions the missing session');
my $wiped_report = $node_subscriber->safe_psql('postgres',
	'SELECT lrstat_export()');
$wiped_report =~ s/^\s+|\s+$//g;
ok(-f $wiped_report, 'current session still exports fine');

# an unknown session is a clear error, not a wrong report
my ($ret, $out, $err) = $node_subscriber->psql('postgres',
	"SELECT lrstat_export('does_not_exist', 'json')");
isnt($ret, 0, 'unknown session name errors out');
like($err, qr/no data for session/,
	'error mentions the missing session');

done_testing();
