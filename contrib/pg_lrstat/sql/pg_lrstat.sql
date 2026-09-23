-- Copyright (c) 2026, PostgreSQL Global Development Group

-- Deterministic tests of the sampling/rate machinery via the inject
-- functions.  The server runs with shared_preload_libraries=pg_lrstat
-- and pg_lrstat.allow_inject=on (see pg_lrstat.conf); the database has
-- no real slots or subscriptions, so the only targets are the injected
-- ones and every output below is exact.  Rates are rounded to absorb
-- the few-millisecond gap between the two now() calls.

CREATE EXTENSION pg_lrstat;

-- Diagnostics view: in this configuration the extension is preloaded
SELECT loaded, layout_version, ntargets > 0, remote_poll
FROM pg_lrstat_info;

-- Two samples 110s apart, inside the default 2min rate window:
--   gen  = 16 MB / 110s, send = 8 MB / 110s
SELECT pg_lrstat_inject_pub('t1', now() - interval '110 seconds',
                            '0/0', '0/0', '0/0', '0/0', '0/0');
SELECT pg_lrstat_inject_pub('t1', now(),
                            '0/1000000', '0/800000',
                            '0/400000', '0/400000', '0/200000');

-- publisher rate view (MB/s)
SELECT round(send_rate::numeric, 2) AS send_mbps,
       round(gen_rate::numeric, 2) AS gen_mbps
FROM pg_lrstat_pub_rate WHERE slot_name = 't1';

-- publisher sample view: point-in-time backlogs (bytes)
SELECT backlog_unsent, backlog_inflight, backlog_peer_unapplied,
       backlog_total, retained_wal
FROM pg_lrstat_pub_sample WHERE slot_name = 't1';

-- stage decomposition
SELECT stage, backlog_bytes FROM pg_lrstat_pipeline
WHERE slot_name = 't1' ORDER BY stage;

-- raw history, one row per injected sample
SELECT count(*) FROM pg_lrstat_pub_history('t1');

-- subscriber + remotely-polled publisher rings compose the overall view
SELECT pg_lrstat_inject_rpub('s1', now() - interval '110 seconds',
                             '0/0', '0/0', '0/0', '0/0', '0/0');
SELECT pg_lrstat_inject_rpub('s1', now(),
                             '0/1000000', '0/800000',
                             '0/400000', '0/400000', '0/200000');
SELECT pg_lrstat_inject_sub('s1', now() - interval '110 seconds', '0/0', '0/0');
SELECT pg_lrstat_inject_sub('s1', now(), '0/780000', '0/600000');

SELECT remote_state, backlog_unsent, backlog_inflight, backlog_unapplied,
       backlog_total, feedback_lag_bytes
FROM pg_lrstat_overall WHERE sub_name = 's1';

-- subscriber rates (MB/s)
SELECT round(recv_rate::numeric, 2) AS recv_mbps,
       round(apply_rate::numeric, 2) AS apply_mbps
FROM pg_lrstat_sub_rate WHERE sub_name = 's1';

SELECT received_lsn, latest_end_lsn, applied_lsn, backlog_apply
FROM pg_lrstat_sub_sample WHERE sub_name = 's1';

SELECT count(*) FROM pg_lrstat_sub_history('s1');

-- a single sample cannot cover half of the window: rates must be NULL
SELECT pg_lrstat_inject_pub('t2', now(), '0/1000', '0/1000',
                            '0/1000', '0/1000', '0/1000');
SELECT send_rate IS NULL AS single_sample_rate_is_null
FROM pg_lrstat_pub_rate WHERE slot_name = 't2';

-- reset clears every target
SELECT pg_lrstat_reset();
SELECT count(*) FROM pg_lrstat_pub_sample;
SELECT count(*) FROM pg_lrstat_sub_sample;
SELECT count(*) FROM pg_lrstat_overall;
SELECT dropped_samples FROM pg_lrstat_info;

-- NULL arguments are rejected instead of being silently swallowed
SELECT pg_lrstat_inject_pub(NULL, now(), '0/0', '0/0', '0/0', '0/0', '0/0');
