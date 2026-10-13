# Test parallel decode infrastructure: pool lifecycle, echo, heartbeat,
# health check, error channel.
#
# Every function in test_parallel_decode maps to a public entry point in
# paralleldecode.c, ensuring all stage-1 code paths are exercised.

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use TestLib;

my $node = PostgreSQL::Test::Cluster->new('pd_test_main');
$node->init;
$node->append_conf('postgresql.conf', 'max_worker_processes = 20');
$node->start;

# Install extension
$node->safe_psql('postgres', 'CREATE EXTENSION test_parallel_decode');

# Test 1: Pool creation with default 2 workers
my $result = $node->safe_psql('postgres', 'SELECT pd_create_pool(2)');
is($result, '', 'pool created');

# Test 2: Health check (workers alive, heartbeat fresh)
$result = $node->safe_psql('postgres', 'SELECT pd_pool_health()');
is($result, 't', 'health check passes');

# Test 3: Worker states (both IDLE)
$result = $node->safe_psql('postgres',
    'SELECT worker_id, state FROM pd_pool_states() ORDER BY worker_id');
is($result, "0|IDLE\n1|IDLE", 'both workers IDLE');

# Test 4: Echo roundtrip to worker 0
$result = $node->safe_psql('postgres', "SELECT pd_echo(0, 'hello')");
is($result, 'hello', 'echo to worker 0');

# Test 5: Echo roundtrip to worker 1
$result = $node->safe_psql('postgres', "SELECT pd_echo(1, 'world')");
is($result, 'world', 'echo to worker 1');

# Test 6: Empty payload echo
$result = $node->safe_psql('postgres', "SELECT pd_echo(0, '')");
is($result, '', 'empty echo');

# Test 7: Larger payload echo
$result = $node->safe_psql('postgres', "SELECT length(pd_echo(0, repeat('x', 10000)))");
is($result, '10000', 'large echo');

# Test 8: Worker state back to IDLE after echo
$result = $node->safe_psql('postgres',
    'SELECT worker_id, state FROM pd_pool_states() ORDER BY worker_id');
is($result, "0|IDLE\n1|IDLE", 'workers IDLE after echo');

# Test 9: Health check after echo roundtrips
$result = $node->safe_psql('postgres', 'SELECT pd_pool_health()');
is($result, 't', 'health check after echo');

# Test 10: Error channel - worker 0 raises error, leader re-raises
eval {
    $node->safe_psql('postgres', 'SELECT pd_test_error(0)');
};
like($@, qr/test error from parallel decode worker 0/, 'error propagated from worker 0');

# Test 11: Pool destroy
$result = $node->safe_psql('postgres', 'SELECT pd_destroy_pool()');
is($result, '', 'pool destroyed');

# Test 12: Post-destroy access fails
eval {
    $node->safe_psql('postgres', "SELECT pd_echo(0, 'should fail')");
};
like($@, qr/pool is not created/, 'post-destroy access fails');

# Test 13: Create pool with invalid worker count
eval {
    $node->safe_psql('postgres', 'SELECT pd_create_pool(0)');
};
like($@, qr/worker count must be between/, 'invalid worker count rejected');

# Test 14: Out-of-range worker_id
$node->safe_psql('postgres', 'SELECT pd_create_pool(1)');
eval {
    $node->safe_psql('postgres', "SELECT pd_echo(5, 'bad')");
};
like($@, qr/worker id 5 out of range/, 'out-of-range worker_id rejected');
$node->safe_psql('postgres', 'SELECT pd_destroy_pool()');

$node->stop('fast');
done_testing();
