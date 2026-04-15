# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify subscriber-side apply of pg_publication_sync rows.
#
# Message type "Q" should execute ddl_str on subscriber after the row itself
# is applied into pg_publication_sync.
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Create publisher node.
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->start;

# Create subscriber node.
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

# Base table required by FOR TABLE publication.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_sync_base (id int primary key)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tap_sync_base (id int primary key)");
$node_publisher->safe_psql('postgres', "CREATE SCHEMA tap_sync_nsp");
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA tap_sync_nsp");

my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub FOR TABLE tap_sync_base WITH (ddl = 'table')");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub "
	. "CONNECTION '$publisher_connstr application_name=tap_sub' "
	. "PUBLICATION tap_pub "
	. "WITH (copy_data = false)");

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub');

# Emit one DDL on publisher. It should be captured as a pg_publication_sync row
# (message_type = 'Q') and replayed on subscriber.
$node_publisher->safe_psql('postgres',
	"SET search_path = tap_sync_nsp; CREATE TABLE tap_sync_ddl_q (id int primary key)");
$node_publisher->wait_for_catchup('tap_sub');

my $result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT to_regclass('tap_sync_nsp.tap_sync_ddl_q') IS NOT NULL");
is($result, 't',
	'message type Q from pg_publication_sync executes ddl_str on subscriber');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND message_type = 'Q'
   AND search_path = 'tap_sync_nsp'
   AND position('CREATE TABLE tap_sync_ddl_q' in ddl_str) > 0
));
is($result, '1',
	'subscriber keeps synced pg_publication_sync row with captured search_path');

# Verify capture/apply filtering:
# 1) capture only publications whose WITH (ddl=...) matches statement kind
# 2) apply only rows whose publication_list intersects subscription publications
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_filter_t1 (id int primary key)");
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_filter_t2 (id int primary key)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tap_filter_t1 (id int primary key)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tap_filter_t2 (id int primary key)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_match FOR TABLE tap_filter_t1 WITH (ddl = 'table')");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_skip FOR TABLE tap_filter_t2 WITH (ddl = 'table')");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_idx FOR TABLE tap_filter_t1 WITH (ddl = 'index')");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_filter "
	. "CONNECTION '$publisher_connstr application_name=tap_sub_filter' "
	. "PUBLICATION tap_pub_match "
	. "WITH (copy_data = false, ddl = 'table')");

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_filter');

$node_publisher->safe_psql('postgres',
	"ALTER TABLE tap_filter_t1 ADD COLUMN c_match int");
$node_publisher->safe_psql('postgres',
	"ALTER TABLE tap_filter_t2 ADD COLUMN c_skip int");
$node_publisher->wait_for_catchup('tap_sub_filter');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT EXISTS (SELECT 1 FROM pg_attribute "
	. "WHERE attrelid = 'tap_filter_t1'::regclass "
	. "AND attname = 'c_match' AND NOT attisdropped)");
is($result, 't',
	'apply executes DDL when publication_list intersects subscription publications');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT EXISTS (SELECT 1 FROM pg_attribute "
	. "WHERE attrelid = 'tap_filter_t2'::regclass "
	. "AND attname = 'c_skip' AND NOT attisdropped)");
is($result, 'f',
	'apply skips DDL when publication_list does not intersect subscription publications');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT coalesce(bool_or(position('tap_pub_idx' in publication_list) > 0), false)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND position('ALTER TABLE tap_filter_t1 ADD COLUMN c_match' in ddl_str) > 0
));
is($result, 'f',
	'capture filters out publications whose ddl option does not include table');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
