# Copyright (c) 2026, PostgreSQL Global Development Group

# Verify subscriber-side apply of pg_publication_sync rows.
#
# Message type "Q" should execute pfsyncddlsql on subscriber after the row itself
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
# (pfsyncmsgtype = 'Q') and replayed on subscriber.
$node_publisher->safe_psql('postgres',
	"SET search_path = tap_sync_nsp; CREATE TABLE tap_sync_ddl_q (id int primary key)");
$node_publisher->wait_for_catchup('tap_sub');

my $result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT to_regclass('tap_sync_nsp.tap_sync_ddl_q') IS NOT NULL");
is($result, 't',
	'message type Q from pg_publication_sync executes pfsyncddlsql on subscriber');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype = 'Q'
   AND pfsyncsearchpath = 'tap_sync_nsp'
   AND position('CREATE TABLE tap_sync_ddl_q' in pfsyncddlsql) > 0
));
is($result, '1',
	'subscriber keeps synced pg_publication_sync row with captured search_path');

# Verify capture/apply filtering:
# 1) capture only publications whose WITH (ddl=...) matches statement kind
# 2) publish rows per publication and route by pfsyncpubid
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
	'apply executes DDL for matching publication');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT EXISTS (SELECT 1 FROM pg_attribute "
	. "WHERE attrelid = 'tap_filter_t2'::regclass "
	. "AND attname = 'c_skip' AND NOT attisdropped)");
is($result, 'f',
	'apply skips DDL for non-subscribed publication');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype = 'Q'
   AND position('ALTER TABLE tap_filter_t1 ADD COLUMN c_match' in pfsyncddlsql) > 0
));
is($result, '1',
	'Q messages are expanded per publication and routed to subscribed publication only');

# Verify publication table membership changes are propagated via
# message_type A/D and applied to subscription relation mapping.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_ad_t (id int primary key, v text)");
$node_subscriber->safe_psql('postgres',
	"CREATE TABLE tap_ad_t (id int primary key, v text)");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_ad FOR TABLE tap_sync_base WITH (ddl = 'table')");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_ad "
	. "CONNECTION '$publisher_connstr application_name=tap_sub_ad' "
	. "PUBLICATION tap_pub_ad "
	. "WITH (copy_data = false, ddl = 'table')");
$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_ad');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tap_ad_t VALUES (1, 'before-add')");
$node_publisher->wait_for_catchup('tap_sub_ad');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tap_ad_t WHERE id = 1");
is($result, '0',
	'before ALTER PUBLICATION ADD TABLE, table changes are not replicated');

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ad ADD TABLE tap_ad_t");
$node_publisher->wait_for_catchup('tap_sub_ad');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype = 'A'
   AND pfsynctargettable = 'public.tap_ad_t'
   AND pfsyncpubid IS NOT NULL
));
is($result, '1',
	'ALTER PUBLICATION ADD TABLE emits message type A');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tap_ad_t VALUES (2, 'after-add')");
$node_publisher->wait_for_catchup('tap_sub_ad');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tap_ad_t WHERE id = 2");
is($result, '1',
	'after message type A apply, new table changes are replicated');

$node_publisher->safe_psql('postgres',
	"ALTER PUBLICATION tap_pub_ad DROP TABLE tap_ad_t");
$node_publisher->wait_for_catchup('tap_sub_ad');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype = 'D'
   AND pfsynctargettable = 'public.tap_ad_t'
   AND pfsyncpubid IS NOT NULL
));
is($result, '1',
	'ALTER PUBLICATION DROP TABLE emits message type D');

$node_publisher->safe_psql('postgres',
	"INSERT INTO tap_ad_t VALUES (3, 'after-drop')");
$node_publisher->wait_for_catchup('tap_sub_ad');
$result = $node_subscriber->safe_psql('postgres',
	"SELECT count(*) FROM tap_ad_t WHERE id = 3");
is($result, '0',
	'after message type D apply, removed table changes are not replicated');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
 WHERE s.subname = 'tap_sub_ad'
   AND c.relname = 'tap_ad_t'
));
is($result, '0',
	'message type D removes table mapping from pg_subscription_rel');

# Verify FOR TABLES IN SCHEMA auto-tracking:
# 1) CREATE TABLE DDL apply should auto-add pg_subscription_rel state (READY)
# 2) Subsequent DML for the new table should be applied automatically
# 3) DROP TABLE should remove local table and subscription mapping
$node_publisher->safe_psql('postgres', "CREATE SCHEMA tap_sync_dyn");
$node_subscriber->safe_psql('postgres', "CREATE SCHEMA tap_sync_dyn");

$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_dyn FOR TABLES IN SCHEMA tap_sync_dyn WITH (ddl = 'table')");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_dyn "
	. "CONNECTION '$publisher_connstr application_name=tap_sub_dyn' "
	. "PUBLICATION tap_pub_dyn "
	. "WITH (copy_data = false, ddl = 'table')");

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_dyn');

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_sync_dyn.tap_dyn_t (id int primary key, v text)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tap_sync_dyn.tap_dyn_t VALUES (1, 'schema-auto')");
$node_publisher->wait_for_catchup('tap_sub_dyn');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT to_regclass('tap_sync_dyn.tap_dyn_t') IS NOT NULL");
is($result, 't',
	'FOR TABLES IN SCHEMA: CREATE TABLE is auto-applied on subscriber');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT count(*) FROM tap_sync_dyn.tap_dyn_t");
is($result, '1',
	'FOR TABLES IN SCHEMA: new table DML is auto-applied');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE s.subname = 'tap_sub_dyn'
   AND n.nspname = 'tap_sync_dyn'
   AND c.relname = 'tap_dyn_t'
));
is($result, '1',
	'FOR TABLES IN SCHEMA: new table is auto-tracked in pg_subscription_rel');

$node_publisher->safe_psql('postgres', "DROP TABLE tap_sync_dyn.tap_dyn_t");
$node_publisher->wait_for_catchup('tap_sub_dyn');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT to_regclass('tap_sync_dyn.tap_dyn_t') IS NULL");
is($result, 't',
	'FOR TABLES IN SCHEMA: DROP TABLE is applied on subscriber');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
 WHERE s.subname = 'tap_sub_dyn'
));
is($result, '0',
	'FOR TABLES IN SCHEMA: dropped table is removed from pg_subscription_rel');

# Verify FOR ALL TABLES has the same auto-tracking behavior for newly created
# tables.
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_pub_all FOR ALL TABLES WITH (ddl = 'table')");

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION tap_sub_all "
	. "CONNECTION '$publisher_connstr application_name=tap_sub_all' "
	. "PUBLICATION tap_pub_all "
	. "WITH (copy_data = false, ddl = 'table')");

$node_subscriber->wait_for_subscription_sync($node_publisher, 'tap_sub_all');

$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_all_t (id int primary key, v text)");
$node_publisher->safe_psql('postgres',
	"INSERT INTO tap_all_t VALUES (1, 'all-auto')");
$node_publisher->wait_for_catchup('tap_sub_all');

$result = $node_subscriber->safe_psql(
	'postgres',
	"SELECT count(*) FROM tap_all_t");
is($result, '1',
	'FOR ALL TABLES: new table DML is auto-applied');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
 WHERE s.subname = 'tap_sub_all'
   AND c.relname = 'tap_all_t'
));
is($result, '1',
	'FOR ALL TABLES: new table is auto-tracked in pg_subscription_rel');

$node_publisher->safe_psql('postgres', "DROP TABLE tap_all_t");
$node_publisher->wait_for_catchup('tap_sub_all');

$result = $node_subscriber->safe_psql(
	'postgres',
	qq(
SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
 WHERE s.subname = 'tap_sub_all'
   AND c.relname = 'tap_all_t'
));
is($result, '0',
	'FOR ALL TABLES: dropped table is removed from pg_subscription_rel');

# Verify publisher-side scope filtering:
# FOR TABLE publication only captures table/index DDL, while FOR TABLES IN
# SCHEMA and FOR ALL TABLES can capture function DDL.
$node_publisher->safe_psql('postgres',
	"CREATE TABLE tap_scope_base (id int primary key)");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_scope_for_table FOR TABLE tap_scope_base "
	. "WITH (ddl = 'table,index,trigger,view,rule,schema,function,type,domain,extension')");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_scope_for_schema FOR TABLES IN SCHEMA public "
	. "WITH (ddl = 'table,index,trigger,view,rule,schema,function,type,domain,extension')");
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION tap_scope_for_all FOR ALL TABLES "
	. "WITH (ddl = 'table,index,trigger,view,rule,schema,function,type,domain,extension')");

$node_publisher->safe_psql('postgres',
	"DROP FUNCTION IF EXISTS tap_scope_filter_fn()");
$node_publisher->safe_psql('postgres',
	"CREATE FUNCTION tap_scope_filter_fn() RETURNS int LANGUAGE sql AS 'SELECT 1'");

$result = $node_publisher->safe_psql(
	'postgres',
	qq(
SELECT coalesce(bool_or(p.pubname = 'tap_scope_for_table'), false)::text
       || '|' ||
       coalesce(bool_or(p.pubname = 'tap_scope_for_schema'), false)::text
       || '|' ||
       coalesce(bool_or(p.pubname = 'tap_scope_for_all'), false)::text
  FROM pg_publication_sync s
  JOIN pg_publication p ON p.oid = s.pfsyncpubid
 WHERE s.pfsyncmsgtype = 'Q'
   AND position('CREATE FUNCTION tap_scope_filter_fn' in s.pfsyncddlsql) > 0
));
is($result, 'false|true|true',
	'publisher scope filter: function DDL captured only by schema/all publications');

$node_subscriber->stop('fast');
$node_publisher->stop('fast');

done_testing();
