-- Test parallel decode infrastructure: pool lifecycle, echo, heartbeat,
-- health check, error channel.

CREATE EXTENSION test_parallel_decode;

-- Create pool with 2 workers
SELECT pd_create_pool(2);

-- Check all workers are IDLE
SELECT worker_id, state FROM pd_pool_states() ORDER BY worker_id;

-- Echo roundtrip to worker 0
SELECT pd_echo(0, 'hello from leader');

-- Echo roundtrip to worker 1
SELECT pd_echo(1, 'second worker test');

-- Workers should be back to IDLE after RELEASE_ACK
SELECT worker_id, state FROM pd_pool_states() ORDER BY worker_id;

-- Health check
SELECT pd_pool_health();

-- Echo with empty payload
SELECT pd_echo(0, '');

-- Echo with larger payload
SELECT pd_echo(0, repeat('x', 1000));

-- Worker state should still be IDLE
SELECT worker_id, state FROM pd_pool_states() ORDER BY worker_id;

-- Error test: worker 0 raises an error, leader re-raises it
SELECT pd_test_error(0);

-- After error, pool should be unhealthy (worker 0 died)
SELECT pd_pool_health();

-- Destroy pool
SELECT pd_destroy_pool();

-- After destroy, calls should fail
SELECT pd_echo(0, 'should fail');
