/*-------------------------------------------------------------------------
 *
 * lrstat.h
 *      Shared structures of the pg_lrstat extension.
 *
 * One sampler bgworker writes samples into per-target ring buffers in
 * shared memory; SQL functions read them back and derive rates at query
 * time.  Everything a rate is computed from must be a monotonically
 * increasing byte counter (an LSN or a pg_stat_replication_slots
 * counter); derived values are never stored.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LRSTAT_H
#define LRSTAT_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "port/atomics.h"
#include "storage/spin.h"
#include "utils/pg_lsn.h"

#define LRSTAT_MAGIC		0x4C525354		/* "LRST" */

/*
 * Bump whenever the layout of LRSample / LRTargetMeta / LRTargetCtl /
 * LRStatShared changes in an incompatible way; checked at attach time
 * so a stale segment can never be interpreted with the wrong layout.
 */
#define LRSTAT_LAYOUT_VERSION	1

#define LR_TEXT_LEN		64
#define LR_STATE_LEN	16
#define LR_ERROR_LEN	160

/*
 * Target kinds.  A target is identified by (kind, name, relid,
 * worker_char); the sample ring right after the control block is
 * interpreted as LRPubSample or LRSubSample according to the kind.
 */
typedef enum LRTargetKind
{
	LR_PUB = 1,				/* publisher slot, sampled locally			*/
	LR_SUB = 2,				/* subscriber worker, sampled locally		*/
	LR_RPUB = 3				/* publisher slot, polled from subscriber	*/
} LRTargetKind;

/* One publisher-side sample (per logical slot). */
typedef struct LRPubSample
{
	TimestampTz ts;			/* local clock at sampling time		*/
	XLogRecPtr	current_lsn;		/* C0: pg_current_wal_lsn()		*/
	XLogRecPtr	sent_lsn;		/* C2: walsender sentPtr		*/
	XLogRecPtr	peer_recv_lsn;		/* C3: feedback write			*/
	XLogRecPtr	peer_flush_lsn;		/* C4: feedback flush			*/
	XLogRecPtr	peer_applied_lsn;	/* C5: feedback apply			*/
	XLogRecPtr	confirmed_lsn;		/* C6: slot confirmed_flush		*/
	XLogRecPtr	restart_lsn;		/* C7: slot restart			*/
	uint64		spill_bytes;		/* D1: decode spill counter		*/
	uint64		stream_bytes;		/* D1: decode stream counter		*/
	uint64		total_bytes;		/* D1: decode total counter		*/
} LRPubSample;

/* One subscriber-side sample (per apply / tablesync worker). */
typedef struct LRSubSample
{
	TimestampTz ts;
	XLogRecPtr	received_lsn;		/* C3': received_lsn			*/
	XLogRecPtr	latest_end_lsn;		/* C3': latest_end_lsn			*/
	XLogRecPtr	applied_lsn;		/* C5': origin remote_lsn		*/
	XLogRecPtr	origin_local_lsn;	/* origin local_lsn			*/
	XLogRecPtr	local_wal_lsn;		/* subscriber pg_current_wal_lsn()	*/
} LRSubSample;

typedef union LRSample
{
	LRPubSample pub;
	LRSubSample sub;
} LRSample;

/*
 * Latest passthrough attributes that are not rate-bearing (identity,
 * states, lag intervals).  Rewritten in full by every sampler round;
 * readers copy the whole struct under the target spinlock.  A zero
 * pid / timestamp means "no value"; lag -1 means "unknown".
 */
typedef struct LRTargetMeta
{
	/* publisher / remote publisher */
	bool		active;
	bool		temporary;
	bool		safe_wal_size_valid;
	bool		reply_time_valid;
	pid_t		sender_pid;
	char		state[LR_STATE_LEN];
	char		sync_state[LR_STATE_LEN];
	char		wal_status[LR_STATE_LEN];
	char		database[NAMEDATALEN];
	char		plugin[LR_TEXT_LEN];
	char		application_name[NAMEDATALEN];
	char		client_addr[LR_TEXT_LEN];
	int64		safe_wal_size;
	int64		write_lag_us;
	int64		flush_lag_us;
	int64		replay_lag_us;
	TimestampTz reply_time;

	/* remote publisher only */
	char		remote_state[LR_STATE_LEN];
	TimestampTz last_remote_poll;

	/* subscriber */
	char		subslotname[NAMEDATALEN];
	char		worker_type[LR_STATE_LEN];
	pid_t		worker_pid;
	pid_t		leader_pid;
	TimestampTz last_msg_send_time;
	TimestampTz last_msg_receipt_time;
	TimestampTz latest_end_time;
	int64		apply_error_count;
	int64		sync_error_count;
} LRTargetMeta;

/*
 * Fixed-size control block; the ring of LRSample follows it in memory
 * (see LR_TARGET_RING and lrstat_target_at).  All fields are protected
 * by the spinlock; the single writer is the sampler worker.
 */
typedef struct LRTargetCtl
{
	LRTargetKind kind;
	char		name[NAMEDATALEN];	/* slot name or subscription name	*/
	char		worker_char;		/* 'a' apply, 't' tablesync (SUB)	*/
	Oid			relid;				/* tablesync target, else 0			*/
	bool		in_use;
	uint32		generation;
	TimestampTz last_sample_ts;		/* monotonicity and aging			*/
	slock_t		mutex;
	int			ring_len;
	uint32		head;				/* next slot to write				*/
	int			n_samples;
	LRTargetMeta meta;
} LRTargetCtl;

#define LR_TARGET_RING(t) \
	((LRSample *) ((char *) (t) + MAXALIGN(sizeof(LRTargetCtl))))

/*
 * Cluster-wide header + diagnostics.  The counters below are guarded
 * by their own spinlock; the sampler is the only writer except for
 * dropped_samples, which any process allocating a target may bump.
 */
typedef struct LRStatShared
{
	int32		magic;
	int32		layout_version;
	int			ntargets;
	slock_t		hdr_mutex;
	TimestampTz last_round_ts;		/* 0 = no round completed yet	*/
	bool		last_round_ok;
	char		last_round_error[LR_ERROR_LEN];
	pg_atomic_uint64 nrounds;
	pg_atomic_uint64 dropped_samples;
} LRStatShared;

/* A subscription to poll from the subscriber-side sampler. */
typedef struct LRRemoteSub
{
	char		subname[NAMEDATALEN];
	char		slotname[NAMEDATALEN];
	char	   *conninfo;		/* worker-lifetime palloc'd copy		*/
} LRRemoteSub;

/* GUC values (defined in pg_lrstat.c) */
extern PGDLLIMPORT int	lrstat_sample_interval_ms;
extern PGDLLIMPORT int	lrstat_rate_window_ms;
extern PGDLLIMPORT int	lrstat_max_targets;
extern PGDLLIMPORT int	lrstat_ring_len;
extern PGDLLIMPORT int	lrstat_stale_target_ttl_s;
extern PGDLLIMPORT int	lrstat_eta_min_rate;		/* bytes/sec		*/
extern PGDLLIMPORT bool lrstat_remote_poll;
extern PGDLLIMPORT int	lrstat_remote_connect_timeout_s;
extern PGDLLIMPORT int	lrstat_remote_poll_budget_ms;
extern PGDLLIMPORT char *lrstat_database;
extern PGDLLIMPORT bool lrstat_allow_inject;
extern PGDLLIMPORT char *lrstat_inject_name_prefix;

/* wait events registered by _PG_init (PG 18 dynamic wait events) */
extern PGDLLIMPORT uint32 lrstat_we_publisher_connect;
extern PGDLLIMPORT uint32 lrstat_we_publisher_query;

/* lrstat_shmem.c */
extern PGDLLIMPORT LRStatShared *lrstat;
extern Size lrstat_shmem_size(void);
extern void lrstat_shmem_startup(void);
extern void lrstat_note_preload(void);
extern bool lrstat_ready(void);
extern LRTargetCtl *lrstat_target_at(int i);
extern LRTargetCtl *lrstat_find_or_create(LRTargetKind kind, const char *name,
										  Oid relid, char worker_char);
extern bool lrstat_lookup(LRTargetKind kind, const char *name,
						  LRSample *ring, int maxn, int *n_out,
						  LRTargetMeta *meta_out);
extern void lrstat_push_sample(LRTargetCtl *target, const LRSample *sample);
extern void lrstat_set_meta(LRTargetCtl *target, const LRTargetMeta *meta);
extern int	lrstat_copy_ring(LRTargetCtl *target, LRSample *out, int maxn);
extern void lrstat_copy_meta(LRTargetCtl *target, LRTargetMeta *out);
extern void lrstat_reset_all(void);
extern void lrstat_note_round(bool ok, const char *error);
extern void lrstat_note_dropped(const char *name);

/* lrstat_worker.c */
extern PGDLLEXPORT pg_noreturn void pg_lrstat_worker_main(Datum arg);

/* lrstat_remote.c */
extern void lrstat_remote_round(const LRRemoteSub *subs, int nsubs,
								TimestampTz deadline);

#endif							/* LRSTAT_H */
