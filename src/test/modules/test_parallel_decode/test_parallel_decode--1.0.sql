\echo Use "CREATE EXTENSION test_parallel_decode" to load this file. \quit

COMMENT ON EXTENSION test_parallel_decode IS 'Test module for parallel logical decoding infrastructure';

-- Pool lifecycle
CREATE FUNCTION pd_create_pool(nworkers integer DEFAULT 2)
RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FUNCTION pd_destroy_pool()
RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Echo roundtrip
CREATE FUNCTION pd_echo(worker_id integer, payload text)
RETURNS text AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

-- Worker state query
CREATE FUNCTION pd_pool_states()
RETURNS TABLE(worker_id integer, state text)
AS 'MODULE_PATHNAME' LANGUAGE C;

-- Health check
CREATE FUNCTION pd_pool_health()
RETURNS boolean AS 'MODULE_PATHNAME' LANGUAGE C;

-- Error channel test
CREATE FUNCTION pd_test_error(worker_id integer)
RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C STRICT;
