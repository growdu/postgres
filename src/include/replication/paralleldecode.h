/*-------------------------------------------------------------------------
 *
 * paralleldecode.h
 *		Infrastructure for publisher-side parallel logical decoding.
 *
 * Implements the full detailed design (stages 1-5):
 *   §2-3  DSM layout, message layer, worker pool lifecycle
 *   §4    Leader: record dispatch, snapshot registry, event publication,
 *         Result Store, slot position advancement, statistics
 *   §5    Worker: decode pipeline, base_snapshot, intermediate results,
 *         event consumption, Worker-mode plugin instances
 *   §7    Intermediate result format (centralized call)
 *   §11   pg_stat_parallel_decode view backing data
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/include/replication/paralleldecode.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef PARALLELDECODE_H
#define PARALLELDECODE_H

#include "postgres.h"

#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "port/atomics.h"
#include "postmaster/bgworker.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/s_lock.h"
#include "storage/shm_mq.h"
#include "utils/timestamp.h"

/* ------------------------------------------------------------------ */
/* DSM layout (§3.1)                                                   */
/* ------------------------------------------------------------------ */

#define PARALLEL_DECODE_TOC_MAGIC		UINT64CONST(0x5044524C303032)

#define PARALLEL_DECODE_TOC_WORKER_INFO	1
#define PARALLEL_DECODE_TOC_TASK_MQ		2
#define PARALLEL_DECODE_TOC_EVENT_MQ	3
#define PARALLEL_DECODE_TOC_RESULT_MQ	4
#define PARALLEL_DECODE_TOC_ERROR_MQ	5
#define PARALLEL_DECODE_TOC_CONTROL_REQ_MQ	6
#define PARALLEL_DECODE_TOC_CONTROL_CMD_MQ	7

#define PD_TASK_MQ_SIZE			(512 * 1024)
#define PD_EVENT_MQ_SIZE		(256 * 1024)
#define PD_RESULT_MQ_SIZE		(1024 * 1024)
#define PD_ERROR_MQ_SIZE		(16 * 1024)
#define PD_CTRL_MQ_SIZE			(8 * 1024)

#define PD_HEARTBEAT_INTERVAL_MS	1000
#define PD_HEARTBEAT_TIMEOUT_S		30
#define PD_WORKER_STARTUP_TIMEOUT_S	60
#define PD_POOL_SHUTDOWN_TIMEOUT_S	60
#define PD_ECHO_TIMEOUT_S			10
#define PD_RESULT_TIMEOUT_S			30

/* ------------------------------------------------------------------ */
/* bgw_extra (§2.1)                                                    */
/* ------------------------------------------------------------------ */

#define PARALLEL_DECODE_BGW_EXTRALEN	128

typedef struct ParallelDecodeWorkerStartup
{
	Oid			dboid;
	Oid			useroid;
	dsm_handle	dsm_handle;
	int			worker_id;
	uint32		startup_generation;
	char		slotname[NAMEDATALEN];
} ParallelDecodeWorkerStartup;

/* ------------------------------------------------------------------ */
/* Worker shared state (§6.1)                                          */
/* ------------------------------------------------------------------ */

#define PD_WORKER_STARTING	0
#define PD_WORKER_IDLE		1
#define PD_WORKER_BUSY		2

typedef struct ParallelDecodeWorkerInfo
{
	int			worker_id;
	pg_atomic_uint32 state;
	TransactionId top_xid;
	uint64		task_generation;
	slock_t		pd_mutex;
	pg_atomic_uint64 consumed_event_id;
	XLogRecPtr	last_input_lsn;
	XLogRecPtr	last_result_lsn;
	uint32		startup_generation;
	pid_t		pid;
	TimestampTz last_heartbeat;
} ParallelDecodeWorkerInfo;

/* ------------------------------------------------------------------ */
/* Message wire contract (§3.2/§3.3)                                   */
/* ------------------------------------------------------------------ */

typedef struct PdMsgHeader
{
	uint32		len;
	uint8		type;
} PdMsgHeader;

typedef enum PdMsgType
{
	PD_MSG_TASK_RECORD = 1,
	PD_MSG_TASK_COMMIT = 2,
	PD_MSG_TASK_ABORT = 3,
	PD_MSG_EVENT_SNAPSHOT = 4,
	PD_MSG_EVENT_INVAL = 5,
	PD_MSG_EVENT_FLUSH_ALL = 6,
	PD_MSG_RESULT_CHANGES = 7,
	PD_MSG_RESULT_FRAMES = 8,
	PD_MSG_ERROR = 9,
	PD_MSG_CTRL_HEARTBEAT = 10,
	PD_MSG_CTRL_RELEASE_ACK = 11,
	PD_MSG_CTRL_CANCEL = 12,
	PD_MSG_CTRL_SHUTDOWN = 13,
	/* Stage-1 verification; removed as real stages land. */
	PD_MSG_TASK_ECHO = 100,
	PD_MSG_RESULT_ECHO = 101,
	PD_MSG_TASK_RAISE_ERROR = 102
} PdMsgType;

typedef struct PdMsgTaskRecord
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	TransactionId xid;
	XLogRecPtr	orig_lsn;
	XLogRecPtr	end_lsn;
	uint64	latest_event_id;
	uint64	task_generation;
	uint32	rec_len;
	char	rec[FLEXIBLE_ARRAY_MEMBER];	/* raw XLogRecord bytes */
} PdMsgTaskRecord;

typedef struct PdMsgTaskCommit
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	XLogRecPtr	commit_lsn;
	XLogRecPtr	end_lsn;
	TimestampTz	commit_time;
	RepOriginId	origin_id;
	XLogRecPtr	origin_lsn;
	uint64	snapshot_version;
	uint64	latest_event_id;
	uint64	task_generation;
} PdMsgTaskCommit;

typedef struct PdMsgTaskAbort
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	XLogRecPtr	abort_lsn;
} PdMsgTaskAbort;

typedef struct PdMsgEventSnapshot
{
	PdMsgHeader hdr;
	uint64		event_id;
	XLogRecPtr	lsn;
	uint64		version;
	uint32		snap_len;
	char	snap[FLEXIBLE_ARRAY_MEMBER];	/* SerializeSnapshot output */
} PdMsgEventSnapshot;

typedef struct PdMsgEventInval
{
	PdMsgHeader hdr;
	uint64		event_id;
	XLogRecPtr	commit_lsn;
	uint32		nmsgs;
	SharedInvalidationMessage msgs[FLEXIBLE_ARRAY_MEMBER];
} PdMsgEventInval;

typedef struct PdMsgEventFlushAll
{
	PdMsgHeader hdr;
	uint64		event_id;
	XLogRecPtr	commit_lsn;
} PdMsgEventFlushAll;

typedef struct PdMsgResultChanges
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	XLogRecPtr	commit_lsn;
	uint64	task_generation;
	uint32	nchanges;
	char	data[FLEXIBLE_ARRAY_MEMBER];	/* PdIntermediateChange stream */
} PdMsgResultChanges;

typedef struct PdMsgResultFrames
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	XLogRecPtr	commit_lsn;
	uint64	task_generation;
	uint32	total_len;
	char	frames[FLEXIBLE_ARRAY_MEMBER];
} PdMsgResultFrames;

typedef struct PdMsgEcho
{
	PdMsgHeader hdr;
	uint32		payload_len;
	char		payload[FLEXIBLE_ARRAY_MEMBER];
} PdMsgEcho;

typedef struct PdMsgHeartbeat
{
	PdMsgHeader hdr;
	uint64		consumed_event_id;
	uint32		state;
} PdMsgHeartbeat;

typedef struct PdMsgReleaseAck
{
	PdMsgHeader hdr;
	TransactionId top_xid;
	uint64		task_generation;
} PdMsgReleaseAck;

typedef struct PdMsgError
{
	PdMsgHeader hdr;
	uint32		msg_len;
	char	msg[FLEXIBLE_ARRAY_MEMBER];
} PdMsgError;

/* ------------------------------------------------------------------ */
/* Intermediate result format (§7, centralized call)                   */
/* ------------------------------------------------------------------ */

typedef struct PdIntermediateChange
{
	uint8		op;				/* 'I'/'U'/'D'/'T' */
	Oid			relid;
	uint32		newtuple_len;	/* 0 = absent */
	uint32		oldtuple_len;	/* 0 = absent */
	/* followed by newtuple bytes, oldtuple bytes, and for 'T': nrelids + relids */
} PdIntermediateChange;

/* ------------------------------------------------------------------ */
/* Transaction entry (walsender-private, §6.2 / §10.2)                */
/* ------------------------------------------------------------------ */

#define PD_TXN_HAS_RESULT	0x01
#define PD_TXN_SPILLED		0x02
#define PD_TXN_FAILED		0x04
#define PD_TXN_OUTPUT_DONE	0x08

typedef struct ParallelDecodeTxnEntry
{
	TransactionId top_xid;
	int			worker_id;
	XLogRecPtr	first_lsn;
	XLogRecPtr	last_lsn;
	XLogRecPtr	commit_lsn;
	uint64		required_event_id;
	uint64		snapshot_version;
	uint64		task_generation;
	uint32		txn_flags;
	/* Result Store link (§4.4) */
	void	   *result_data;		/* PdMsgResultChanges/Frames in memory */
	Size		result_len;
	char	   *spill_path;		/* NULL if not spilled */
	/* Scheduling link */
	struct ParallelDecodeTxnEntry *next_pending;
} ParallelDecodeTxnEntry;

/* ------------------------------------------------------------------ */
/* GUC variables                                                       */
/* ------------------------------------------------------------------ */

extern int	max_parallel_decode_workers_per_walsender;
extern bool parallel_decode;
extern int	parallel_decode_input_queue_size;
extern int	parallel_decode_result_queue_size;

/* Fallback strategy enum values */
#define PD_FALLBACK_SERIAL	0
#define PD_FALLBACK_WAIT	1
extern int	parallel_decode_fallback;

/* ------------------------------------------------------------------ */
/* Pool API (leader side)                                              */
/* ------------------------------------------------------------------ */

typedef struct ParallelDecodePool ParallelDecodePool;

/* Stage 1: infrastructure */
extern ParallelDecodePool *ParallelDecodePoolCreate(int nworkers);
extern void ParallelDecodePoolDestroy(ParallelDecodePool *pool);
extern char *ParallelDecodePoolEcho(ParallelDecodePool *pool, int worker_id,
									const char *payload, Size payload_len,
									Size *rlen_p);
extern void ParallelDecodePoolTestError(ParallelDecodePool *pool,
										int worker_id);
extern bool ParallelDecodePoolCheckHealth(ParallelDecodePool *pool);
extern int ParallelDecodePoolGetWorkerStates(ParallelDecodePool *pool,
											 uint32 *states, int nstates);
extern int ParallelDecodePoolGetNWorkers(ParallelDecodePool *pool);
extern const char *ParallelDecodeWorkerStateName(uint32 state);

/* Stage 2: record dispatch and result handling */
extern void ParallelDecodeDispatchRecord(ParallelDecodePool *pool,
										 XLogReaderState *record,
										 TransactionId top_xid,
										 TransactionId xid);
extern void ParallelDecodeDispatchCommit(ParallelDecodePool *pool,
										 TransactionId top_xid,
										 XLogRecPtr commit_lsn,
										 XLogRecPtr end_lsn,
										 TimestampTz commit_time,
										 RepOriginId origin_id,
										 XLogRecPtr origin_lsn);
extern void ParallelDecodeDispatchAbort(ParallelDecodePool *pool,
										TransactionId top_xid,
										XLogRecPtr abort_lsn);
extern void ParallelDecodeCollectResults(ParallelDecodePool *pool);
extern void ParallelDecodeOutputPending(ParallelDecodePool *pool,
                                        struct LogicalDecodingContext *ctx);

/* Stage 3: event publication */
extern void ParallelDecodePublishSnapshot(ParallelDecodePool *pool,
										  SnapBuild *builder,
										  XLogRecPtr lsn);
extern void ParallelDecodePublishInvalidations(ParallelDecodePool *pool,
											   uint32 nmsgs,
											   SharedInvalidationMessage *msgs,
											   XLogRecPtr commit_lsn);
extern uint64 ParallelDecodeGetLatestEventId(ParallelDecodePool *pool);
extern void ParallelDecodeAdvanceSlotPositions(ParallelDecodePool *pool);

/* Statistics (§11.2) */
typedef struct ParallelDecodePoolMetrics
{
	int64		pool_size;
	int64		workers_idle;
	int64		workers_busy;
	int64		workers_failed;
	int64		pending_txns;
	int64		busy_time_ms;
	int64		inval_wait_time_ms;
	int64		task_queue_bytes;
	int64		result_queue_bytes;
	int64		result_spill_bytes;
	int64		txn_total;
	int64		txn_latency_sum_ms;
	int64		output_lag_lsn;
	int64		plugin_time_ms;
	int64		fallback_total;
	int64		worker_restart_total;
} ParallelDecodePoolMetrics;

extern void ParallelDecodePoolGetMetrics(ParallelDecodePool *pool,
										 ParallelDecodePoolMetrics *metrics);

/* Worker entry point */
extern void ParallelDecodeWorkerMain(Datum main_arg);

#endif							/* PARALLELDECODE_H */
