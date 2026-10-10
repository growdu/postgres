/*-------------------------------------------------------------------------
 *
 * lrstat.h
 *      Shared structures of the pg_lrstat session-model extension.
 *
 * One sampler bgworker writes into per-target three-slot samples
 * (anchor/prev/last) in shared memory.  A named session (start/stop)
 * defines the measurement window; rates are derived at query time.
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

#define LRSTAT_MAGIC             0x4C525354   /* "LRST" */
#define LRSTAT_LAYOUT_VERSION    6

#define LR_TEXT_LEN     64
#define LR_STATE_LEN    16
/* worker_type: apply / parallel apply / table sync / recovery */
#define LR_WTYPE_LEN    24
/* remembered sessions (name -> id mapping for views) */
#define LRSTAT_MAX_SESSIONS  16

/*
 * Target kinds: send side (SEND), recv side (RECV), remote-polled
 * send mirror (RSEND).
 */
typedef enum LRTargetKind
{
	LR_SEND = 1,
	LR_RECV = 2,
	LR_RSEND = 3
} LRTargetKind;

/*
 * Send-side sample (SEND and RSEND share this; logical + physical).
 * Rate-bearing fields only; passthrough attributes in LRTargetMeta.
 */
typedef struct LRSendSample
{
	TimestampTz ts;
	XLogRecPtr  current_lsn;        /* C0 */
	XLogRecPtr  sent_lsn;           /* C2 */
	XLogRecPtr  peer_recv_lsn;      /* C3  feedback write  */
	XLogRecPtr  peer_flush_lsn;     /* C4  feedback flush  */
	XLogRecPtr  peer_applied_lsn;   /* C5  feedback apply  */
	XLogRecPtr  confirmed_lsn;      /* C6  slot confirmed_flush */
	XLogRecPtr  restart_lsn;        /* C7  slot restart */
	uint64      spill_bytes;        /* D1  decode spill counter */
	uint64      stream_bytes;       /* D1  decode stream counter */
	uint64      total_bytes;        /* D1  decode total counter */
} LRSendSample;

/*
 * Recv-side sample (logical apply worker or physical recovery).
 */
typedef struct LRRecvSample
{
	TimestampTz ts;
	XLogRecPtr  received_lsn;       /* C3' */
	XLogRecPtr  applied_lsn;        /* C5' (origin∪feedback) */
	XLogRecPtr  local_wal_lsn;      /* recv-side pg_current_wal_lsn() */
} LRRecvSample;

typedef union LRSample
{
	LRSendSample send;
	LRRecvSample recv;
} LRSample;

/*
 * Passthrough attributes (not rate-bearing).  Rewritten each round.
 */
typedef struct LRTargetMeta
{
	/* send-side passthrough */
	bool        active;
	bool        temporary;
	bool        safe_wal_size_valid;
	pid_t       sender_pid;
	char        state[LR_STATE_LEN];
	char        sync_state[LR_STATE_LEN];
	char        wal_status[LR_STATE_LEN];
	char        database[LR_TEXT_LEN];
	char        plugin[LR_TEXT_LEN];
	char        application_name[LR_TEXT_LEN];
	char        client_addr[LR_TEXT_LEN];
	int64       safe_wal_size;
	int64       write_lag_us;       /* -1 unknown */
	int64       flush_lag_us;
	int64       replay_lag_us;
	/* rsend */
	char        remote_state[LR_STATE_LEN];
	TimestampTz last_remote_poll;
	/* recv */
	char        worker_type[LR_WTYPE_LEN];
	pid_t       worker_pid;
	pid_t       leader_pid;
	TimestampTz last_msg_send_time;
	TimestampTz last_msg_receipt_time;
	int64       apply_error_count;
	int64       sync_error_count;
	/* internal: not exposed in views */
	XLogRecPtr  origin_lsn;         /* for bump_applied comparison */
	XLogRecPtr  latest_end_lsn;     /* received_lsn fallback source */
} LRTargetMeta;

/*
 * Per-target: three slots (anchor/prev/last), no history ring.
 */
typedef struct LRTargetCtl
{
	LRTargetKind kind;
	char        name[NAMEDATALEN];
	char        worker_char;        /* 'a'/'t' logical, 'p' physical */
	Oid         relid;              /* tablesync target table, else 0 */
	bool        in_use;
	TimestampTz first_seen_ts;      /* partial target detection */
	TimestampTz last_sample_ts;
	slock_t     mutex;
	LRSample    anchor;
	LRSample    prev;
	LRSample    last;
	LRTargetMeta meta;
} LRTargetCtl;

/*
 * History entry: one full raw sample per target per sampling round.
 * This IS the primary data store — every round, every target's
 * complete LSN snapshot plus the per-round state is recorded here.
 * Stat views and rates derive from this; nothing is pre-computed at
 * write time.
 */
typedef struct LRHistoryEntry
{
	TimestampTz ts;
	int32       target_idx;
	int32       kind;              /* LRTargetKind of the target */
	uint64      session_id;         /* 0 = recorded outside any session */
	/* send-side LSNs (SEND / RSEND targets) */
	XLogRecPtr  current_lsn;        /* C0 */
	XLogRecPtr  sent_lsn;           /* C2 */
	XLogRecPtr  peer_recv_lsn;      /* C3  feedback write */
	XLogRecPtr  peer_flush_lsn;     /* C4  feedback flush */
	XLogRecPtr  peer_applied_lsn;   /* C5  feedback apply */
	XLogRecPtr  confirmed_lsn;      /* C6  slot confirmed_flush */
	XLogRecPtr  restart_lsn;        /* C7  slot restart */
	uint64      spill_bytes;        /* D1 */
	uint64      stream_bytes;       /* D1 */
	/* recv-side LSNs (RECV targets) */
	XLogRecPtr  received_lsn;       /* C3' */
	XLogRecPtr  applied_lsn;        /* C5' (origin∪feedback, monotonic) */
	XLogRecPtr  local_wal_lsn;      /* recv-side pg_current_wal_lsn() */
	/* per-round state (send side) */
	char        state[LR_STATE_LEN];       /* walsender state */
	char        sync_state[LR_STATE_LEN];  /* async/sync/quorum */
	char        wal_status[LR_STATE_LEN];  /* reserved/extended/... */
	bool        active;
	int32       sender_pid;
	int64       write_lag_us;             /* -1 unknown */
	int64       flush_lag_us;
	int64       replay_lag_us;
	/* per-round state (recv side) */
	char        worker_type[LR_WTYPE_LEN];/* apply / table sync */
	int32       worker_pid;
	int32       leader_pid;
	Oid         relid;                    /* tablesync target, else 0 */
	TimestampTz last_msg_send_time;
	TimestampTz last_msg_receipt_time;
	int64       apply_error_count;        /* -1 unknown */
	int64       sync_error_count;
} LRHistoryEntry;

/*
 * Remembered session (registry ring in shared memory).  Lets views map
 * history rows back to the session they belong to.
 */
typedef struct LRSessionRegEntry
{
	bool        in_use;
	uint64      session_id;
	char        name[NAMEDATALEN];
	TimestampTz start_ts;
	TimestampTz stop_ts;            /* 0 while running */
} LRSessionRegEntry;

/*
 * Target identity written to the session file footer at stop, so an
 * archived session can be exported by name after a restart.
 */
typedef struct LRSessTargetInfo
{
	int32       target_idx;
	int32       kind;              /* LRTargetKind */
	int32       relid;             /* tablesync target table, else 0 */
	char        name[NAMEDATALEN];
} LRSessTargetInfo;

/*
 * Global session state.
 */
typedef struct LRSessionState
{
	int32       magic;
	int32       layout_version;
	uint64      session_id;
	bool        running;
	char        name[NAMEDATALEN];
	TimestampTz start_ts;
	TimestampTz stop_ts;
	bool        truncated;
	bool        degraded;
	slock_t     mutex;
} LRSessionState;

/*
 * Shared memory layout:
 *   [LRSessionState header]
 *   [LRTargetCtl array (max_targets)]
 *   [LRHistoryEntry array (session_max_samples × max_targets)]
 *   [LRSessionRegEntry array (LRSTAT_MAX_SESSIONS)]
 */
typedef struct LRStatShared
{
	LRSessionState session;
	int         ntargets;
	pid_t       worker_pid;         /* for wake-on-start (SIGUSR1) */
	/* honest round bookkeeping for the info view (worker-maintained) */
	TimestampTz last_round_ts;
	bool        last_round_ok;
	uint64      nrounds;
	uint64      dropped_samples;    /* targets dropped: slot table full */
	int         ring_len;           /* session_max_samples */
	int         n_entries;         /* history write cursor (worker only) */
	/* targets, history entries and session registry follow, see
	 * lrstat_target_at / lrstat_history_at / lrstat_sessionreg_at */
} LRStatShared;

/*
 * A subscription/standby to poll from the recv side.
 */
typedef struct LRPollTarget
{
	char        recv_name[NAMEDATALEN];
	char        slot_name[NAMEDATALEN];
	char       *conninfo;           /* worker-lifetime palloc'd */
} LRPollTarget;

/* GUC values (defined in pg_lrstat.c) */
extern PGDLLIMPORT int  lrstat_sample_interval_ms;
extern PGDLLIMPORT int  lrstat_max_targets;
extern PGDLLIMPORT int  lrstat_ring_len;
extern PGDLLIMPORT int  lrstat_stale_target_ttl_s;
extern PGDLLIMPORT double lrstat_catchup_min_rate;
extern PGDLLIMPORT bool lrstat_remote_poll;
extern PGDLLIMPORT int  lrstat_remote_connect_timeout_s;
extern PGDLLIMPORT int  lrstat_remote_poll_budget_ms;
extern PGDLLIMPORT char *lrstat_database;
extern PGDLLIMPORT bool lrstat_allow_inject;

/* lrstat_shmem.c */
extern PGDLLIMPORT LRStatShared *lrstat;
extern Size lrstat_shmem_size(void);
extern void lrstat_shmem_startup(void);
extern void lrstat_note_preload(void);
extern bool lrstat_ready(void);
extern LRTargetCtl *lrstat_target_at(int i);
extern LRHistoryEntry *lrstat_history_at(int idx);
extern LRSessionRegEntry *lrstat_sessionreg_at(int i);
/* map a session id to its name (NULL when unknown); copies under lock */
extern bool lrstat_session_name(uint64 session_id, char *out, Size outlen);
extern void lrstat_sessionreg_add(uint64 session_id, const char *name,
								  TimestampTz start_ts);
extern void lrstat_sessionreg_close(uint64 session_id, TimestampTz stop_ts);
/* average-rate window clamp: min(last_ts, session stop_ts) */
extern TimestampTz lrstat_rate_end(TimestampTz last_ts);
extern uint64 lrstat_session_id_by_name(const char *name);
extern bool lrstat_sessionreg_lookup(const char *name, uint64 *session_id,
									 TimestampTz *start_ts,
									 TimestampTz *stop_ts);
extern LRTargetCtl *lrstat_find_or_create(LRTargetKind kind, const char *name,
										  Oid relid, char worker_char);
extern bool lrstat_lookup(LRTargetKind kind, const char *name,
						  LRSample *anchor, LRSample *prev, LRSample *last,
						  LRTargetMeta *meta_out);
extern void lrstat_push_sample(LRTargetCtl *target, const LRSample *sample);
extern void lrstat_set_meta(LRTargetCtl *target, const LRTargetMeta *meta);
extern void lrstat_copy_all(LRTargetCtl *target, LRSample *anchor,
							LRSample *prev, LRSample *last,
							LRTargetMeta *meta_out);
extern void lrstat_bump_applied(const char *recv_name, XLogRecPtr applied);
extern void lrstat_session_start(const char *name);
extern void lrstat_session_stop(void);
extern void lrstat_session_reset(void);
extern void lrstat_history_from_sample(LRHistoryEntry *e, int target_idx,
									   const LRSample *sample,
									   const LRTargetMeta *meta,
									   Oid relid, int kind);
extern void lrstat_append_history_entry(const LRHistoryEntry *e);
extern void lrstat_reset_entries(void);
extern int  lrstat_get_entry_count(void);
extern int  lrstat_history_count(void);   /* valid entries (== cap once wrapped) */
extern int  lrstat_history_slot(int i);   /* i-th oldest entry -> array index */
extern void lrstat_note_round(bool ok, const char *error);
extern void lrstat_note_dropped(const char *name);

/* lrstat_worker.c */
extern PGDLLEXPORT pg_noreturn void pg_lrstat_worker_main(Datum arg);

/* lrstat_export.c */
extern int  lrstat_export_list(char ***names_out);

/* lrstat_remote.c */
extern void lrstat_run_remote_poll(const LRPollTarget *targets, int ntargets,
							   TimestampTz deadline);

#endif                          /* LRSTAT_H */
