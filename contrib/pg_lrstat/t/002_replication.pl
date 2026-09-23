# Copyright (c) 2026, PostgreSQL Global Development Group

# End-to-end test with a real publisher/subscriber pair: both sides run
# pg_lrstat, the subscriber also polls the publisher remotely.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $pub = PostgreSQL::Test::Cluster->new('lrstat_publisher');
$pub->init(allows_streaming => 'logical');
$pub->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.sample_interval = '1s'
pg_lrstat.rate_window = '4s'
pg_lrstat.remote_poll_budget = '2s'
});
$pub->start;
$pub->safe_psql('postgres', 'CREATE EXTENSION pg_lrstat');

my $sub = PostgreSQL::Test::Cluster->new('lrstat_subscriber');
$sub->init;
$sub->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.sample_interval = '1s'
pg_lrstat.rate_window = '4s'
pg_lrstat.remote_poll_budget = '2s'
});
$sub->start;
$sub->safe_psql('postgres', 'CREATE EXTENSION pg_lrstat');

$pub->safe_psql('postgres',
	"CREATE TABLE t (a int, b text); ALTER TABLE t REPLICA IDENTITY FULL;");
$sub->safe_psql('postgres', "CREATE TABLE t (a int, b text);");
$pub->safe_psql('postgres',
	"CREATE PUBLICATION lrstat_pub FOR TABLE t;");
$sub->safe_psql('postgres',
	"CREATE SUBSCRIPTION lrstat_sub CONNECTION '" . $pub->connstr . "' PUBLICATION lrstat_pub;");

$sub->wait_for_subscription_sync($pub, 'lrstat_sub');

# Generate some WAL so the rates become non-zero.
$pub->safe_psql('postgres',
	"INSERT INTO t SELECT g, md5(g::text) FROM generate_series(1, 5000) g;");

# Publisher side: the slot shows up and send_rate differentiates.
ok($pub->poll_query_until('postgres', qq{
	SELECT count(*) > 0 FROM pg_lrstat_pub_sample
	WHERE slot_name = 'lrstat_sub' AND sent_lsn IS NOT NULL
}, undef, '90'), 'publisher sample view sees the slot');
ok($pub->poll_query_until('postgres', qq{
	SELECT count(*) > 0 FROM pg_lrstat_pub_rate
	WHERE send_rate IS NOT NULL
}, undef, '90'), 'publisher send rate computed');

# Subscriber side: local apply progress and remote poll health.
ok($sub->poll_query_until('postgres', qq{
	SELECT count(*) > 0 FROM pg_lrstat_sub_sample
	WHERE sub_name = 'lrstat_sub' AND applied_lsn IS NOT NULL
}, undef, '90'), 'subscriber sample view sees applied LSN');
ok($sub->poll_query_until('postgres', qq{
	SELECT count(*) > 0 FROM pg_lrstat_overall
	WHERE sub_name = 'lrstat_sub' AND remote_state = 'ok'
	  AND apply_rate IS NOT NULL
}, undef, '90'), 'overall view composes both ends');

# The overall backlog must equal the sum of its stages.
my $result = $sub->safe_psql('postgres', q{
	SELECT count(*) FROM pg_lrstat_overall
	WHERE backlog_total = backlog_unsent + backlog_inflight + backlog_unapplied
});
isnt($result, '0', 'stage backlogs sum up to the total');

$sub->stop;
$pub->stop;
done_testing();
