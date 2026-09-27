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

done_testing();
