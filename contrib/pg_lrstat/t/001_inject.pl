# Copyright (c) 2026, PostgreSQL Global Development Group

# Deterministic tests of the rate arithmetic via the inject functions:
# no real replication is involved.
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

my $node = PostgreSQL::Test::Cluster->new('lrstat_inject');
$node->init;
$node->append_conf(
	'postgresql.conf', qq{
shared_preload_libraries = 'pg_lrstat'
pg_lrstat.allow_inject = on
});
$node->start;
$node->safe_psql('postgres', 'CREATE EXTENSION pg_lrstat');

# Two samples 110s apart inside the default 2min window:
#   gen_rate  = 16 MB / 110s = 0.1455 MB/s
#   send_rate =  8 MB / 110s = 0.0727 MB/s
$node->safe_psql('postgres', q{
SELECT pg_lrstat_inject_pub('t1', now() - interval '110s',
                            '0/0',        '0/0',
                            '0/0',        '0/0', '0/0');
SELECT pg_lrstat_inject_pub('t1', now(),
                            '0/1000000',  '0/800000',
                            '0/400000',   '0/400000', '0/200000');
});

my $result = $node->safe_psql('postgres', q{
SELECT round(send_rate::numeric, 4), round(gen_rate::numeric, 4)
FROM pg_lrstat_pub_rate WHERE slot_name = 't1';
});
is($result, '0.0727|0.1455', 'pub rate differentiation in MB/s');

$result = $node->safe_psql('postgres', q{
SELECT backlog_unsent, backlog_inflight, backlog_total, retained_wal
FROM pg_lrstat_pub_sample WHERE slot_name = 't1';
});
is($result, '8388608|4194304|12582912|14680064', 'pub backlogs in bytes');

# Overall view joins the local sub ring with the remote pub ring.
$node->safe_psql('postgres', q{
SELECT pg_lrstat_inject_rpub('s1', now() - interval '120s',
                             '0/0', '0/0', '0/0', '0/0', '0/0');
SELECT pg_lrstat_inject_rpub('s1', now(),
                             '0/1000000', '0/800000',
                             '0/400000', '0/400000', '0/200000');
SELECT pg_lrstat_inject_sub('s1', now() - interval '120s', '0/0', '0/0');
SELECT pg_lrstat_inject_sub('s1', now(), '0/780000', '0/600000');
});

$result = $node->safe_psql('postgres', q{
SELECT remote_state, backlog_unsent, backlog_inflight, backlog_unapplied,
       backlog_total
FROM pg_lrstat_overall WHERE sub_name = 's1';
});
# unsent = 16-8 MB, inflight = 8-7.5 MB, unapplied = 7.5-6 MB, total = 10 MB
is($result, 'ok|8388608|524288|1572864|10485760',
   'overall view composes both ends');

$result = $node->safe_psql('postgres', q{
SELECT round(apply_rate::numeric, 4) FROM pg_lrstat_sub_rate
WHERE sub_name = 's1' AND worker_type = 'apply';
});
is($result, '0.0500', 'sub apply rate in MB/s');

$node->safe_psql('postgres', "SELECT pg_lrstat_reset()");
$result = $node->safe_psql('postgres',
	"SELECT count(*) FROM pg_lrstat_pub_sample");
is($result, '0', 'reset clears all targets');

$node->stop;
done_testing();
