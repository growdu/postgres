/* pg_lrstat--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_lrstat" to load this file. \quit

--
-- Sample views: latest per-target facts, with sample_time.
-- Rate views: window-differentiated rates in MB/s, with rate_time and
-- the actual window boundaries.  All views are empty unless pg_lrstat
-- is loaded via shared_preload_libraries.
--

CREATE FUNCTION pg_lrstat_pub_sample(
    OUT slot_name text, OUT sample_time timestamptz, OUT database text,
    OUT plugin text, OUT temporary boolean, OUT active boolean,
    OUT sender_pid int4, OUT application_name text, OUT client_addr text,
    OUT state text, OUT sync_state text, OUT current_lsn pg_lsn,
    OUT sent_lsn pg_lsn, OUT peer_recv_lsn pg_lsn, OUT peer_flush_lsn pg_lsn,
    OUT peer_applied_lsn pg_lsn, OUT confirmed_flush_lsn pg_lsn,
    OUT restart_lsn pg_lsn, OUT wal_status text, OUT safe_wal_size int8,
    OUT spill_bytes int8, OUT stream_bytes int8, OUT total_bytes int8,
    OUT backlog_unsent int8, OUT backlog_inflight int8,
    OUT backlog_peer_unapplied int8, OUT backlog_total int8,
    OUT retained_wal int8, OUT write_lag interval, OUT flush_lag interval,
    OUT replay_lag interval, OUT reply_time timestamptz)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_pub_sample'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_pub_rate(
    OUT slot_name text, OUT rate_time timestamptz,
    OUT window_start_time timestamptz, OUT window_end_time timestamptz,
    OUT window_secs float8, OUT gen_rate float8, OUT send_rate float8,
    OUT apply_rate float8, OUT confirm_rate float8,
    OUT spill_rate float8, OUT stream_rate float8, OUT eta_unsent float8,
    OUT eta_total float8, OUT send_stalled boolean)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_pub_rate'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_sub_sample(
    OUT sub_name text, OUT sample_time timestamptz, OUT subslotname text,
    OUT worker_type text, OUT worker_pid int4, OUT leader_pid int4,
    OUT relid oid, OUT received_lsn pg_lsn, OUT latest_end_lsn pg_lsn,
    OUT applied_lsn pg_lsn, OUT origin_local_lsn pg_lsn,
    OUT local_wal_lsn pg_lsn, OUT last_msg_send_time timestamptz,
    OUT last_msg_receipt_time timestamptz, OUT latest_end_time timestamptz,
    OUT backlog_apply int8, OUT apply_error_count int8,
    OUT sync_error_count int8)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_sub_sample'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_sub_rate(
    OUT sub_name text, OUT worker_type text, OUT relid oid,
    OUT rate_time timestamptz, OUT window_start_time timestamptz,
    OUT window_end_time timestamptz, OUT window_secs float8,
    OUT recv_rate float8, OUT apply_rate float8, OUT local_wal_rate float8,
    OUT eta_apply float8, OUT apply_stalled boolean)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_sub_rate'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_pipeline(
    OUT slot_name text, OUT stage text, OUT rate_time timestamptz,
    OUT window_start_time timestamptz, OUT window_end_time timestamptz,
    OUT backlog_bytes int8, OUT rate float8, OUT lag interval)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_pipeline'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_overall(
    OUT sub_name text, OUT subslotname text, OUT remote_state text,
    OUT last_remote_poll_time timestamptz, OUT sample_time timestamptz,
    OUT rate_time timestamptz, OUT window_start_time timestamptz,
    OUT window_end_time timestamptz, OUT window_secs float8,
    OUT pub_current_lsn pg_lsn, OUT sent_lsn pg_lsn,
    OUT received_lsn pg_lsn, OUT applied_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn, OUT restart_lsn pg_lsn,
    OUT gen_rate float8, OUT send_rate float8, OUT recv_rate float8,
    OUT apply_rate float8, OUT spill_rate float8, OUT stream_rate float8,
    OUT backlog_unsent int8, OUT backlog_inflight int8,
    OUT backlog_unapplied int8, OUT backlog_total int8,
    OUT retained_wal int8, OUT feedback_lag_bytes int8,
    OUT write_lag interval, OUT flush_lag interval,
    OUT replay_lag interval, OUT eta_unsent float8, OUT eta_total float8,
    OUT send_stalled boolean, OUT apply_stalled boolean)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_overall'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_pub_history(
    slot_name text DEFAULT NULL, since timestamptz DEFAULT NULL,
    OUT name text, OUT ts timestamptz, OUT current_lsn pg_lsn,
    OUT sent_lsn pg_lsn, OUT peer_recv_lsn pg_lsn,
    OUT peer_applied_lsn pg_lsn, OUT confirmed_flush_lsn pg_lsn,
    OUT restart_lsn pg_lsn)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_pub_history'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_sub_history(
    sub_name text DEFAULT NULL, since timestamptz DEFAULT NULL,
    OUT name text, OUT ts timestamptz, OUT received_lsn pg_lsn,
    OUT latest_end_lsn pg_lsn, OUT applied_lsn pg_lsn,
    OUT origin_local_lsn pg_lsn, OUT local_wal_lsn pg_lsn)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_sub_history'
LANGUAGE C STABLE;

CREATE FUNCTION pg_lrstat_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_lrstat_reset'
LANGUAGE C VOLATILE;

CREATE FUNCTION pg_lrstat_inject_pub(
    name text, ts timestamptz, current pg_lsn, sent pg_lsn,
    peer_applied pg_lsn, confirmed pg_lsn, restart pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_lrstat_inject_pub'
LANGUAGE C VOLATILE;

CREATE FUNCTION pg_lrstat_inject_rpub(
    name text, ts timestamptz, current pg_lsn, sent pg_lsn,
    peer_applied pg_lsn, confirmed pg_lsn, restart pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_lrstat_inject_rpub'
LANGUAGE C VOLATILE;

CREATE FUNCTION pg_lrstat_inject_sub(
    name text, ts timestamptz, received pg_lsn, applied pg_lsn)
RETURNS void
AS 'MODULE_PATHNAME', 'pg_lrstat_inject_sub'
LANGUAGE C VOLATILE;

CREATE FUNCTION pg_lrstat_info(
    OUT loaded bool, OUT layout_version int4, OUT ntargets int4,
    OUT ring_len int8, OUT sample_interval_ms int8,
    OUT rate_window_ms int8, OUT max_targets int8,
    OUT remote_poll bool, OUT last_round_ts timestamptz,
    OUT last_round_ok bool, OUT last_round_error text,
    OUT nrounds int8, OUT dropped_samples int8)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_info'
LANGUAGE C STABLE;

CREATE VIEW pg_lrstat_pub_sample AS SELECT * FROM pg_lrstat_pub_sample();
CREATE VIEW pg_lrstat_pub_rate AS SELECT * FROM pg_lrstat_pub_rate();
CREATE VIEW pg_lrstat_sub_sample AS SELECT * FROM pg_lrstat_sub_sample();
CREATE VIEW pg_lrstat_sub_rate AS SELECT * FROM pg_lrstat_sub_rate();
CREATE VIEW pg_lrstat_pipeline AS SELECT * FROM pg_lrstat_pipeline();
CREATE VIEW pg_lrstat_overall AS SELECT * FROM pg_lrstat_overall();
CREATE VIEW pg_lrstat_info AS SELECT * FROM pg_lrstat_info();
-- History views show every target, full ring; use the underlying
-- functions with (name, since) arguments for filtered queries.
CREATE VIEW pg_lrstat_pub_history AS SELECT * FROM pg_lrstat_pub_history();
CREATE VIEW pg_lrstat_sub_history AS SELECT * FROM pg_lrstat_sub_history();

REVOKE ALL ON FUNCTION
    pg_lrstat_pub_sample(), pg_lrstat_pub_rate(),
    pg_lrstat_sub_sample(), pg_lrstat_sub_rate(),
    pg_lrstat_pipeline(), pg_lrstat_overall(), pg_lrstat_info(),
    pg_lrstat_pub_history(text, timestamptz),
    pg_lrstat_sub_history(text, timestamptz)
FROM PUBLIC;

REVOKE ALL ON
    pg_lrstat_pub_sample, pg_lrstat_pub_rate,
    pg_lrstat_sub_sample, pg_lrstat_sub_rate,
    pg_lrstat_pipeline, pg_lrstat_overall, pg_lrstat_info,
    pg_lrstat_pub_history, pg_lrstat_sub_history
FROM PUBLIC;
GRANT SELECT ON
    pg_lrstat_pub_sample, pg_lrstat_pub_rate,
    pg_lrstat_sub_sample, pg_lrstat_sub_rate,
    pg_lrstat_pipeline, pg_lrstat_overall, pg_lrstat_info,
    pg_lrstat_pub_history, pg_lrstat_sub_history
TO pg_monitor;

--
-- Column comments: rates are MB/s (1 MB = 1048576 bytes), backlog
-- columns are bytes, eta_* columns are seconds.
--

COMMENT ON VIEW pg_lrstat_pub_sample IS
    'Latest per-slot sample of the logical replication pipeline (publisher side)';
COMMENT ON COLUMN pg_lrstat_pub_sample.sample_time IS 'timestamp of the sample round that produced this row';
COMMENT ON COLUMN pg_lrstat_pub_sample.current_lsn IS 'C0: pg_current_wal_lsn() at sample time';
COMMENT ON COLUMN pg_lrstat_pub_sample.sent_lsn IS 'C2: walsender sentPtr (NULL when the slot has no walsender)';
COMMENT ON COLUMN pg_lrstat_pub_sample.peer_recv_lsn IS 'C3: receive position reported by subscriber feedback';
COMMENT ON COLUMN pg_lrstat_pub_sample.peer_flush_lsn IS 'C4: flushed-commit position reported by subscriber feedback';
COMMENT ON COLUMN pg_lrstat_pub_sample.peer_applied_lsn IS 'C5: applied position reported by subscriber feedback';
COMMENT ON COLUMN pg_lrstat_pub_sample.confirmed_flush_lsn IS 'C6: slot confirmed_flush_lsn (release watermark)';
COMMENT ON COLUMN pg_lrstat_pub_sample.restart_lsn IS 'C7: slot restart_lsn (retention watermark)';
COMMENT ON COLUMN pg_lrstat_pub_sample.spill_bytes IS 'decoder spill counter from pg_stat_replication_slots';
COMMENT ON COLUMN pg_lrstat_pub_sample.stream_bytes IS 'decoder stream counter from pg_stat_replication_slots';
COMMENT ON COLUMN pg_lrstat_pub_sample.total_bytes IS 'decoder total counter from pg_stat_replication_slots';
COMMENT ON COLUMN pg_lrstat_pub_sample.backlog_unsent IS 'bytes: current_lsn - sent_lsn';
COMMENT ON COLUMN pg_lrstat_pub_sample.backlog_inflight IS 'bytes: sent_lsn - peer_recv_lsn (includes feedback-cycle lag)';
COMMENT ON COLUMN pg_lrstat_pub_sample.backlog_peer_unapplied IS 'bytes: peer_recv_lsn - peer_applied_lsn';
COMMENT ON COLUMN pg_lrstat_pub_sample.backlog_total IS 'bytes: current_lsn - peer_applied_lsn';
COMMENT ON COLUMN pg_lrstat_pub_sample.retained_wal IS 'bytes: current_lsn - restart_lsn';

COMMENT ON VIEW pg_lrstat_pub_rate IS
    'Window-differentiated rates per logical slot (publisher side, MB/s)';
COMMENT ON COLUMN pg_lrstat_pub_rate.rate_time IS 'when this row was computed';
COMMENT ON COLUMN pg_lrstat_pub_rate.window_start_time IS 'oldest sample actually used for the difference';
COMMENT ON COLUMN pg_lrstat_pub_rate.window_end_time IS 'newest sample actually used for the difference';
COMMENT ON COLUMN pg_lrstat_pub_rate.window_secs IS 'length of the actual differentiation window in seconds';
COMMENT ON COLUMN pg_lrstat_pub_rate.gen_rate IS 'WAL generation rate, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.send_rate IS 'walsender send rate, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.apply_rate IS 'peer apply rate from feedback, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.confirm_rate IS 'confirmed_flush_lsn advance rate, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.spill_rate IS 'decoder spill rate, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.stream_rate IS 'decoder streaming rate, MB/s';
COMMENT ON COLUMN pg_lrstat_pub_rate.eta_unsent IS 'seconds to drain backlog_unsent at send_rate';
COMMENT ON COLUMN pg_lrstat_pub_rate.eta_total IS 'seconds to drain backlog_total at send/apply rates';
COMMENT ON COLUMN pg_lrstat_pub_rate.send_stalled IS 'true when unsent backlog grows while send_rate is ~0';

COMMENT ON VIEW pg_lrstat_sub_sample IS
    'Latest per-worker sample of the subscription side';
COMMENT ON COLUMN pg_lrstat_sub_sample.received_lsn IS 'C3'': pg_stat_subscription.received_lsn';
COMMENT ON COLUMN pg_lrstat_sub_sample.latest_end_lsn IS 'last keepalive/data end LSN reported to the apply worker';
COMMENT ON COLUMN pg_lrstat_sub_sample.applied_lsn IS 'C5'': origin remote_lsn (applied position, no feedback delay)';
COMMENT ON COLUMN pg_lrstat_sub_sample.origin_local_lsn IS 'local LSN of the last applied commit record';
COMMENT ON COLUMN pg_lrstat_sub_sample.local_wal_lsn IS 'subscriber pg_current_wal_lsn() (includes non-replication writes)';
COMMENT ON COLUMN pg_lrstat_sub_sample.backlog_apply IS 'bytes: received - applied';

COMMENT ON VIEW pg_lrstat_sub_rate IS
    'Window-differentiated rates per subscription worker (MB/s)';
COMMENT ON COLUMN pg_lrstat_sub_rate.recv_rate IS 'receive rate, MB/s';
COMMENT ON COLUMN pg_lrstat_sub_rate.apply_rate IS 'apply rate from origin remote_lsn, MB/s';
COMMENT ON COLUMN pg_lrstat_sub_rate.local_wal_rate IS 'subscriber local WAL generation rate, MB/s';
COMMENT ON COLUMN pg_lrstat_sub_rate.eta_apply IS 'seconds to drain backlog_apply at apply_rate';
COMMENT ON COLUMN pg_lrstat_sub_rate.apply_stalled IS 'true when backlog grows while apply_rate is ~0';

COMMENT ON VIEW pg_lrstat_pipeline IS
    'Stage decomposition of the publisher-side pipeline, one row per stage';
COMMENT ON COLUMN pg_lrstat_pipeline.stage IS 'unsent | inflight | peer_unapplied | retained';
COMMENT ON COLUMN pg_lrstat_pipeline.backlog_bytes IS 'size of this stage''s backlog, bytes';
COMMENT ON COLUMN pg_lrstat_pipeline.rate IS 'rate draining this stage, MB/s';
COMMENT ON COLUMN pg_lrstat_pipeline.lag IS 'feedback time lag attributable to this stage';

COMMENT ON VIEW pg_lrstat_overall IS
    'Both ends combined per subscription (publisher side via remote polling)';
COMMENT ON COLUMN pg_lrstat_overall.remote_state IS 'ok | unreachable | stale | n/a (no remote data)';
COMMENT ON COLUMN pg_lrstat_overall.last_remote_poll_time IS 'last successful remote poll, local clock';
COMMENT ON COLUMN pg_lrstat_overall.backlog_unsent IS 'bytes: publisher current - sent';
COMMENT ON COLUMN pg_lrstat_overall.backlog_inflight IS 'bytes: publisher sent - locally received (true network inflight)';
COMMENT ON COLUMN pg_lrstat_overall.backlog_unapplied IS 'bytes: locally received - locally applied';
COMMENT ON COLUMN pg_lrstat_overall.backlog_total IS 'bytes: publisher current - locally applied';
COMMENT ON COLUMN pg_lrstat_overall.retained_wal IS 'bytes: publisher current - restart_lsn';
COMMENT ON COLUMN pg_lrstat_overall.feedback_lag_bytes IS 'bytes: local receive position minus feedback-reported receive position';
COMMENT ON COLUMN pg_lrstat_overall.eta_total IS 'seconds to drain the whole pipeline';

COMMENT ON VIEW pg_lrstat_info IS
    'Diagnostics: load state, configuration and sampler health';
COMMENT ON COLUMN pg_lrstat_info.loaded IS 'false when pg_lrstat is not in shared_preload_libraries (all other views are empty)';
COMMENT ON COLUMN pg_lrstat_info.layout_version IS 'shared memory layout version, must match the loaded library';
COMMENT ON COLUMN pg_lrstat_info.last_round_ts IS 'when the sampler last completed a round';
COMMENT ON COLUMN pg_lrstat_info.last_round_ok IS 'outcome of the last sampler round';
COMMENT ON COLUMN pg_lrstat_info.last_round_error IS 'error message when the last round failed';
COMMENT ON COLUMN pg_lrstat_info.nrounds IS 'completed sampler rounds since startup';
COMMENT ON COLUMN pg_lrstat_info.dropped_samples IS 'samples dropped because every target slot held fresh data; raise pg_lrstat.max_targets if nonzero';

COMMENT ON VIEW pg_lrstat_pub_history IS
    'Raw publisher-side samples, chronological (all targets, full ring)';
COMMENT ON VIEW pg_lrstat_sub_history IS
    'Raw subscriber-side samples, chronological (all targets, full ring)';
