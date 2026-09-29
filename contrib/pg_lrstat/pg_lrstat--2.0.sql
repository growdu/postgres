/* pg_lrstat--2.0.sql */

\echo Use "CREATE EXTENSION pg_lrstat" to load this file. \quit

-- Session commands
CREATE FUNCTION lrstat_start(name text DEFAULT NULL, persist boolean DEFAULT false)
RETURNS text
AS 'MODULE_PATHNAME', 'lrstat_start'
LANGUAGE C VOLATILE;

CREATE FUNCTION lrstat_stop(name text DEFAULT NULL)
RETURNS text
AS 'MODULE_PATHNAME', 'lrstat_stop'
LANGUAGE C VOLATILE;

CREATE FUNCTION pg_lrstat_reset()
RETURNS void
AS 'MODULE_PATHNAME', 'pg_lrstat_reset'
LANGUAGE C VOLATILE;

CREATE FUNCTION lrstat_delete(name text)
RETURNS void
AS 'MODULE_PATHNAME', 'lrstat_delete'
LANGUAGE C VOLATILE;

CREATE FUNCTION lrstat_export(
    name text DEFAULT NULL,
    format text DEFAULT 'html')
RETURNS text
AS 'MODULE_PATHNAME', 'lrstat_export'
LANGUAGE C VOLATILE;

-- View: info (1 row, health + session state)
CREATE FUNCTION pg_lrstat_info(
    OUT loaded bool, OUT session_name text, OUT session_state text,
    OUT session_truncated bool, OUT session_degraded bool,
    OUT session_start_ts timestamptz, OUT session_stop_ts timestamptz,
    OUT sample_interval_ms int8, OUT last_round_ts timestamptz,
    OUT last_round_ok bool, OUT last_round_error text,
    OUT nrounds int8, OUT dropped_samples int8,
    OUT remote_poll bool, OUT archived_session_names text[])
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_info'
LANGUAGE C STABLE;

-- View: send_stat (one row per send-side connection)
CREATE FUNCTION pg_lrstat_send_stat(
    OUT slot_name text, OUT plugin text, OUT temporary bool,
    OUT active bool, OUT sender_pid int4, OUT application_name text,
    OUT client_addr text, OUT state text, OUT sync_state text,
    OUT wal_status text, OUT safe_wal_size float8,
    OUT sample_time timestamptz,
    OUT current_lsn pg_lsn, OUT sent_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn,
    OUT backlog_unsent float8, OUT backlog_inflight float8,
    OUT backlog_peer_unapplied float8, OUT backlog_total float8,
    OUT retained_wal float8,
    OUT gen_instant float8, OUT gen_avg float8,
    OUT send_instant float8, OUT send_avg float8,
    OUT apply_instant float8, OUT apply_avg float8,
    OUT spill_instant float8, OUT spill_avg float8,
    OUT write_lag interval, OUT flush_lag interval,
    OUT replay_lag interval, OUT send_blocked bool)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_send_stat'
LANGUAGE C STABLE;

-- View: recv_stat (one row per recv worker/recovery)
CREATE FUNCTION pg_lrstat_recv_stat(
    OUT recv_name text, OUT worker_type text,
    OUT worker_pid int4, OUT leader_pid int4, OUT relid oid,
    OUT sample_time timestamptz,
    OUT received_lsn pg_lsn, OUT applied_lsn pg_lsn,
    OUT last_msg_send_time timestamptz, OUT last_msg_receipt_time timestamptz,
    OUT backlog_apply float8,
    OUT recv_instant float8, OUT recv_avg float8,
    OUT apply_instant float8, OUT apply_avg float8,
    OUT local_wal_instant float8, OUT local_wal_avg float8,
    OUT apply_error_count int8, OUT sync_error_count int8,
    OUT apply_blocked bool)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_recv_stat'
LANGUAGE C STABLE;

-- View: cluster_stat (one row per replication pair, both ends)
CREATE FUNCTION pg_lrstat_cluster_stat(
    OUT recv_name text, OUT remote_state text,
    OUT last_remote_poll_time timestamptz, OUT sample_time timestamptz,
    OUT send_current_lsn pg_lsn, OUT sent_lsn pg_lsn,
    OUT received_lsn pg_lsn, OUT applied_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn,
    OUT gen_instant float8, OUT gen_avg float8,
    OUT send_instant float8, OUT send_avg float8,
    OUT recv_instant float8, OUT recv_avg float8,
    OUT apply_instant float8, OUT apply_avg float8,
    OUT backlog_unsent float8, OUT backlog_inflight float8,
    OUT backlog_unapplied float8, OUT backlog_total float8,
    OUT retained_wal float8, OUT feedback_lag_mb float8,
    OUT write_lag interval, OUT flush_lag interval,
    OUT replay_lag interval,
    OUT catchup_send_secs float8, OUT catchup_total_secs float8,
    OUT send_blocked bool, OUT apply_blocked bool,
    OUT bottleneck text)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_cluster_stat'
LANGUAGE C STABLE;

-- View: send_history (full raw samples with all LSNs, always recording)
CREATE FUNCTION pg_lrstat_send_history(
    OUT name text, OUT ts timestamptz,
    OUT current_lsn pg_lsn, OUT sent_lsn pg_lsn,
    OUT peer_recv_lsn pg_lsn, OUT peer_flush_lsn pg_lsn,
    OUT peer_applied_lsn pg_lsn,
    OUT confirmed_flush_lsn pg_lsn, OUT restart_lsn pg_lsn,
    OUT spill_bytes int8, OUT stream_bytes int8)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_send_history'
LANGUAGE C STABLE;

-- View: recv_history (full raw samples with all LSNs, always recording)
CREATE FUNCTION pg_lrstat_recv_history(
    OUT name text, OUT ts timestamptz,
    OUT received_lsn pg_lsn, OUT applied_lsn pg_lsn,
    OUT local_wal_lsn pg_lsn)
RETURNS setof record
AS 'MODULE_PATHNAME', 'pg_lrstat_recv_history'
LANGUAGE C STABLE;

CREATE VIEW pg_lrstat_info AS SELECT * FROM pg_lrstat_info();
CREATE VIEW pg_lrstat_send_stat AS SELECT * FROM pg_lrstat_send_stat();
CREATE VIEW pg_lrstat_recv_stat AS SELECT * FROM pg_lrstat_recv_stat();
CREATE VIEW pg_lrstat_cluster_stat AS SELECT * FROM pg_lrstat_cluster_stat();
CREATE VIEW pg_lrstat_send_history AS SELECT * FROM pg_lrstat_send_history();
CREATE VIEW pg_lrstat_recv_history AS SELECT * FROM pg_lrstat_recv_history();


REVOKE ALL ON FUNCTION
    pg_lrstat_info(), pg_lrstat_send_stat(),
    pg_lrstat_recv_stat(), pg_lrstat_cluster_stat(),
    pg_lrstat_send_history(), pg_lrstat_recv_history()
FROM PUBLIC;

REVOKE ALL ON
    pg_lrstat_info, pg_lrstat_send_stat,
    pg_lrstat_recv_stat, pg_lrstat_cluster_stat,
    pg_lrstat_send_history, pg_lrstat_recv_history
FROM PUBLIC;
GRANT SELECT ON
    pg_lrstat_info, pg_lrstat_send_stat,
    pg_lrstat_recv_stat, pg_lrstat_cluster_stat,
    pg_lrstat_send_history, pg_lrstat_recv_history
TO pg_monitor;
