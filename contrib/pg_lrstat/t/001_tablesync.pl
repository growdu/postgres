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

# Session lifecycle across a table sync: lrstat_start resets all
# targets, which must also cope with the sync worker's target.
$node_subscriber->safe_psql('postgres', "SELECT lrstat_start('sync_sess')");
is($node_subscriber->safe_psql('postgres',
	q(SELECT session_state FROM pg_lrstat_info)),
	'running', 'session started during table sync');
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

# Once the sync target expired (stale_target_ttl), only the apply
# worker is left in the views.
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT bool_and(worker_type = 'apply') FROM pg_lrstat_recv_stat
));
is($result, 1, 'only apply worker left after sync finished');

# The publisher side sees the subscription's slot.
$result = $node_publisher->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_send_stat WHERE slot_name = 'lrstat_sub'
));
is($result, 1, 'subscription slot visible in pg_lrstat_send_stat');

# Export still works after a full sync cycle.
is($node_subscriber->safe_psql('postgres', "SELECT lrstat_stop('sync_sess')"),
	'sync_sess', 'session stopped after sync');
is($node_subscriber->safe_psql('postgres',
	q(SELECT length(lrstat_export('sync_sess', 'json')) > 0)),
	't', 'json export works after table sync');

# ---- session-scoped data --------------------------------------------
# History rows are stamped with the session they belong to; a second
# (persist) session must still be exportable by name after a third
# session has wiped the in-memory history.

$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_recv_history
	WHERE session_name = 'sync_sess'
));
is($result, 1, 'history rows carry the session name');

# a persist session with some load to aggregate; keep it alive for at
# least three sampling rounds so the rate views have intervals
$node_subscriber->safe_psql('postgres',
	"SELECT lrstat_start('archived', true)");
$node_publisher->safe_psql('postgres',
	q(INSERT INTO lrstat_test SELECT g, repeat(md5(g::text), 50)
	  FROM generate_series(100001, 110000) g));
$result = $node_subscriber->poll_query_until(
	'postgres', qq(
	SELECT count(*) >= 3 FROM pg_lrstat_recv_history
	WHERE session_name = 'archived'
));
is($result, 1, 'second session stamped in history');
is($node_subscriber->safe_psql('postgres', "SELECT lrstat_stop('archived')"),
	'archived', 'persist session stopped');

# session_stat aggregates the current in-memory session
$result = $node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_session_stat
	WHERE session_name = 'archived' AND n_samples > 0
));
is($result, 't', 'session_stat aggregates the archived session');

# per-interval rate views derive rates between consecutive samples
$result = $node_subscriber->poll_query_until('postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_recv_rate_history
	WHERE session_name = 'archived' AND recv_mbps IS NOT NULL
));
is($result, 1, 'recv_rate_history derives per-interval rates');
$result = $node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) FROM pg_lrstat_send_rate_history
	WHERE session_name = 'archived' AND interval_secs IS NULL
));
cmp_ok($result, '>=', 1, 'first sample of a session has NULL rate');

# a third session only re-anchors targets; previous sessions stay in
# the in-memory history until the ring overwrites them
$node_subscriber->safe_psql('postgres', "SELECT lrstat_start('wiper')");
sleep(3);
is($node_subscriber->safe_psql('postgres', "SELECT lrstat_stop('wiper')"),
	'wiper', 'third session stopped');
is($node_subscriber->safe_psql('postgres', qq(
	SELECT count(*) > 0 FROM pg_lrstat_recv_history
	WHERE session_name = 'archived')),
	't', 'older session survives a newer session start');

# export writes the report to pg_lrstat/exports and returns the path;
# the archived session is rebuilt from its file
my $archived_path = $node_subscriber->safe_psql('postgres',
	"SELECT lrstat_export('archived', 'json')");
$archived_path =~ s/^\s+|\s+$//g;
like($archived_path, qr{pg_lrstat/exports/archived\.json$},
	'export returns the report file path');
ok(-f $archived_path, 'archived session exported by name from its file');
my $archived_json = PostgreSQL::Test::Utils::slurp_file($archived_path);
ok(index($archived_json, '"name": "archived"') >= 0,
	'archived export identifies the session');
ok(index($archived_json, '"samples": [') >= 0
	&& $archived_json =~ /"kind": "(send|recv)"/,
	'archived export contains raw history samples');

# a persist=false session is exportable by name from memory alone
# (sync_sess was not persisted; it predates wiper)
my $mem_path = $node_subscriber->safe_psql('postgres',
	"SELECT lrstat_export('sync_sess', 'json')");
$mem_path =~ s/^\s+|\s+$//g;
like($mem_path, qr{pg_lrstat/exports/sync_sess\.json$},
	'non-persist session exported by name from memory');
my $mem_json = PostgreSQL::Test::Utils::slurp_file($mem_path);
ok(index($mem_json, '"name": "sync_sess"') >= 0,
	'memory export identifies the session');

# an unknown session is a clear error, not a wrong report
my ($ret, $out, $err) = $node_subscriber->psql('postgres',
	"SELECT lrstat_export('does_not_exist', 'json')");
isnt($ret, 0, 'unknown session name errors out');
like($err, qr/no data for session/,
	'error mentions the missing session');

done_testing();
