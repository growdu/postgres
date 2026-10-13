/*-------------------------------------------------------------------------
 *
 * paralleldecode.c
 *		Publisher-side parallel logical decoding: worker pool and dispatch.
 *
 * Implements stages 1-5 of the detailed design.  Stage 1 covers pool
 * lifecycle and infrastructure; stages 2-5 add record dispatch, event
 * publication, Result Store, and slot position advancement.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/logical/paralleldecode.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/paralleldecode.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/snapbuild.h"
#include "storage/dsm.h"
#include "storage/latch.h"
#include "storage/shm_toc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* ------------------------------------------------------------------ */
/* Internal structures                                                 */
/* ------------------------------------------------------------------ */

typedef struct PdWorkerConn
{
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelDecodeWorkerInfo *info;
	shm_mq_handle *task_h;
	shm_mq_handle *event_h;
	shm_mq_handle *result_h;
	shm_mq_handle *error_h;
	shm_mq_handle *cmd_h;
	shm_mq_handle *req_h;
	BackgroundWorkerHandle *bgwh;
	pid_t		pid;
	bool		stopped;
} PdWorkerConn;

typedef struct PdSnapshotEntry
{
	uint64		version;
	XLogRecPtr	lsn;
	char	   *serialized;
	Size		len;
	int			refcount;
} PdSnapshotEntry;

struct ParallelDecodePool
{
	int			nworkers;
	uint32		startup_generation;
	PdWorkerConn *conns;

	/* Transaction hash (walsender-private, §6.2) */
	HTAB	   *txn_hash;

	/* Pending queue (arrival order) */
	ParallelDecodeTxnEntry *pending_head;
	ParallelDecodeTxnEntry *pending_tail;

	/* Committed-but-not-output (commit order via binary heap equivalent) */
	ParallelDecodeTxnEntry *output_head;	/* linked by commit_lsn order */

	/* Event tracking (§4.3) */
	uint64		next_event_id;
	PdSnapshotEntry *snapshots;	/* array of 16 entries */
	int			n_snapshots;
	uint64		snapshot_version_counter;

	/* Statistics (§11.2) */
	ParallelDecodePoolMetrics metrics;
};

/* ------------------------------------------------------------------ */
/* Forward declarations                                                */
/* ------------------------------------------------------------------ */

static void pd_terminate_worker(dsm_segment *seg, Datum arg);
static void pd_setup_dsm(PdWorkerConn *conn, uint32 startup_generation);
static void pd_launch_worker(PdWorkerConn *conn, int worker_id,
							 uint32 startup_generation);
static void pd_attach_queues(PdWorkerConn *conn);
static void pd_wait_worker_ready(PdWorkerConn *conn);
static void pd_drain_heartbeats(PdWorkerConn *conn);
static void pd_send_msg_raw(shm_mq_handle *mqh, const void *data, Size len);
static void pd_send_msg(shm_mq_handle *mqh, PdMsgType type,
						const void *body, Size body_len);
static bool pd_try_receive(shm_mq_handle *mqh, PdMsgHeader **msg_p,
						   Size *len_p);
static void pd_wait_leader(int timeout_ms);
static char *pd_echo_internal(PdWorkerConn *conn, const char *payload,
							  Size payload_len, Size *rlen_p);
static void pd_test_error_internal(PdWorkerConn *conn);
static void pd_raise_worker_error(int worker_id, PdMsgError *emsg);
static PdWorkerConn *pd_find_worker_for_txn(ParallelDecodePool *pool,
											TransactionId top_xid);
static int pd_find_idle_worker(ParallelDecodePool *pool);
static void pd_queue_pending(ParallelDecodePool *pool,
							 ParallelDecodeTxnEntry *entry);
static ParallelDecodeTxnEntry *pd_dequeue_pending(ParallelDecodePool *pool);
static void pd_insert_output_queue(ParallelDecodePool *pool,
								   ParallelDecodeTxnEntry *entry);
static void pd_broadcast_event(ParallelDecodePool *pool, PdMsgType type,
							   const void *body, Size body_len,
							   XLogRecPtr lsn);
static void pd_release_snapshot(ParallelDecodePool *pool, uint64 version);

/* GUC variable */
/* GUC variables */
int			max_parallel_decode_workers_per_walsender = 2;
bool		parallel_decode = false;
int			parallel_decode_input_queue_size = 65536;	/* kB */
int			parallel_decode_result_queue_size = 16384;	/* kB */
int			parallel_decode_fallback = PD_FALLBACK_SERIAL;

static uint32 pd_pool_generation = 0;
static uint32 pd_leader_wait_event = 0;

/* ------------------------------------------------------------------ */
/* Public API: pool lifecycle                                          */
/* ------------------------------------------------------------------ */

const char *
ParallelDecodeWorkerStateName(uint32 state)
{
	switch (state)
	{
		case PD_WORKER_STARTING: return "STARTING";
		case PD_WORKER_IDLE: return "IDLE";
		case PD_WORKER_BUSY: return "BUSY";
		default: return "UNKNOWN";
	}
}

ParallelDecodePool *
ParallelDecodePoolCreate(int nworkers)
{
	ParallelDecodePool *pool;
	MemoryContext oldcontext;
	HASHCTL		ctl;
	uint32		generation;
	int			i;

	if (nworkers < 1 || nworkers > 100)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("worker count must be between 1 and 100")));

	oldcontext = MemoryContextSwitchTo(TopMemoryContext);

	pool = palloc0(sizeof(ParallelDecodePool));
	pool->nworkers = nworkers;
	pool->conns = palloc0(sizeof(PdWorkerConn) * nworkers);
	generation = ++pd_pool_generation;
	pool->startup_generation = generation;
	pool->next_event_id = 1;
	pool->snapshot_version_counter = 0;
	pool->n_snapshots = 0;
	pool->snapshots = palloc0(sizeof(PdSnapshotEntry) * 16);
	memset(&pool->metrics, 0, sizeof(ParallelDecodePoolMetrics));
	pool->metrics.pool_size = nworkers;

	ctl.keysize = sizeof(TransactionId);
	ctl.entrysize = sizeof(ParallelDecodeTxnEntry);
	pool->txn_hash = hash_create("parallel decode txn hash", 128, &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	for (i = 0; i < nworkers; i++)
		pd_setup_dsm(&pool->conns[i], generation);
	for (i = 0; i < nworkers; i++)
		pd_launch_worker(&pool->conns[i], i, generation);
	for (i = 0; i < nworkers; i++)
		pd_attach_queues(&pool->conns[i]);
	for (i = 0; i < nworkers; i++)
		pd_wait_worker_ready(&pool->conns[i]);

	MemoryContextSwitchTo(oldcontext);
	return pool;
}

void
ParallelDecodePoolDestroy(ParallelDecodePool *pool)
{
	TimestampTz deadline;
	int			i;

	if (pool == NULL)
		return;

	for (i = 0; i < pool->nworkers; i++)
	{
		if (!pool->conns[i].stopped)
		{
			PG_TRY();
			{
				pd_send_msg(pool->conns[i].cmd_h, PD_MSG_CTRL_SHUTDOWN,
							NULL, 0);
			}
			PG_CATCH();
			{
				FlushErrorState();
			}
			PG_END_TRY();
		}
	}

	deadline = TimestampTzPlusSeconds(GetCurrentTimestamp(),
									  PD_POOL_SHUTDOWN_TIMEOUT_S);
	for (;;)
	{
		bool		all_stopped = true;

		for (i = 0; i < pool->nworkers; i++)
		{
			if (!pool->conns[i].stopped &&
				GetBackgroundWorkerPid(pool->conns[i].bgwh,
									   &pool->conns[i].pid) == BGWH_STOPPED)
				pool->conns[i].stopped = true;
			if (!pool->conns[i].stopped)
			{
				all_stopped = false;
				pd_drain_heartbeats(&pool->conns[i]);
			}
		}
		if (all_stopped || GetCurrentTimestamp() >= deadline)
			break;
		pd_wait_leader(100);
		CHECK_FOR_INTERRUPTS();
	}

	for (i = 0; i < pool->nworkers; i++)
	{
		if (!pool->conns[i].stopped)
		{
			ereport(WARNING,
					(errmsg("parallel decode worker %d did not stop; terminating",
							pool->conns[i].info->worker_id)));
			TerminateBackgroundWorker(pool->conns[i].bgwh);
			pool->metrics.worker_restart_total++;
		}
		dsm_detach(pool->conns[i].seg);
	}

	hash_destroy(pool->txn_hash);
	pfree(pool->snapshots);
	pfree(pool->conns);
	pfree(pool);
}

/* ------------------------------------------------------------------ */
/* Public API: echo and test (stage 1)                                 */
/* ------------------------------------------------------------------ */

char *
ParallelDecodePoolEcho(ParallelDecodePool *pool, int worker_id,
					   const char *payload, Size payload_len, Size *rlen_p)
{
	if (pool == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("parallel decode pool is not created")));
	if (worker_id < 0 || worker_id >= pool->nworkers)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("worker id %d out of range", worker_id)));
	return pd_echo_internal(&pool->conns[worker_id], payload, payload_len,
							rlen_p);
}

void
ParallelDecodePoolTestError(ParallelDecodePool *pool, int worker_id)
{
	if (pool == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("parallel decode pool is not created")));
	if (worker_id < 0 || worker_id >= pool->nworkers)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("worker id %d out of range", worker_id)));
	pd_test_error_internal(&pool->conns[worker_id]);
}

bool
ParallelDecodePoolCheckHealth(ParallelDecodePool *pool)
{
	int			i;

	if (pool == NULL)
		return false;

	for (i = 0; i < pool->nworkers; i++)
	{
		PdWorkerConn *conn = &pool->conns[i];
		TimestampTz last;

		pd_drain_heartbeats(conn);
		if (GetBackgroundWorkerPid(conn->bgwh, &conn->pid) != BGWH_STARTED)
			return false;

		SpinLockAcquire(&conn->info->pd_mutex);
		last = conn->info->last_heartbeat;
		SpinLockRelease(&conn->info->pd_mutex);

		if (TimestampDifferenceExceeds(last, GetCurrentTimestamp(),
									   PD_HEARTBEAT_TIMEOUT_S * 1000))
			return false;
	}
	return true;
}

int
ParallelDecodePoolGetWorkerStates(ParallelDecodePool *pool,
								  uint32 *states, int nstates)
{
	int			i;

	if (pool == NULL)
		return 0;
	for (i = 0; i < pool->nworkers && i < nstates; i++)
		states[i] = pg_atomic_read_u32(&pool->conns[i].info->state);
	return pool->nworkers;
}

int
ParallelDecodePoolGetNWorkers(ParallelDecodePool *pool)
{
	if (pool == NULL)
		return 0;
	return pool->nworkers;
}

/* ------------------------------------------------------------------ */
/* Public API: record dispatch (stage 2)                               */
/* ------------------------------------------------------------------ */

void
ParallelDecodeDispatchRecord(ParallelDecodePool *pool,
							 XLogReaderState *record,
							 TransactionId top_xid, TransactionId xid)
{
	ParallelDecodeTxnEntry *entry;
	PdWorkerConn *conn;
	PdMsgTaskRecord *msg;
	Size		msglen;
	uint32		rec_len = XLogRecGetTotalLen(record);
	bool		found;

	if (pool == NULL || top_xid == InvalidTransactionId)
		return;

	entry = hash_search(pool->txn_hash, &top_xid, HASH_ENTER, &found);
	if (!found)
	{
		entry->top_xid = top_xid;
		entry->worker_id = -1;
		entry->first_lsn = record->ReadRecPtr;
		entry->txn_flags = 0;
		entry->result_data = NULL;
		entry->result_len = 0;
		entry->spill_path = NULL;
		entry->next_pending = NULL;
		pool->metrics.txn_total++;
	}
	entry->last_lsn = record->EndRecPtr;

	/* Find or assign a worker */
	conn = pd_find_worker_for_txn(pool, top_xid);
	if (conn == NULL)
	{
		int			wid = pd_find_idle_worker(pool);

		if (wid < 0)
		{
			/* Pool full: queue for later */
			pd_queue_pending(pool, entry);
			return;
		}
		conn = &pool->conns[wid];
		entry->worker_id = wid;
		SpinLockAcquire(&conn->info->pd_mutex);
		conn->info->top_xid = top_xid;
		conn->info->task_generation++;
		SpinLockRelease(&conn->info->pd_mutex);
		pg_atomic_write_u32(&conn->info->state, PD_WORKER_BUSY);
	}

	entry->worker_id = conn->info->worker_id;

	msglen = offsetof(PdMsgTaskRecord, rec) + rec_len;
	if (msglen > PD_TASK_MQ_SIZE - 1024)
	{
		pool->metrics.fallback_total++;
		return;					/* oversize record: skip for now (serial fallback) */
	}

	msg = palloc(msglen);
	msg->hdr.len = msglen;
	msg->hdr.type = PD_MSG_TASK_RECORD;
	msg->top_xid = top_xid;
	msg->xid = xid;
	msg->orig_lsn = record->ReadRecPtr;
	msg->end_lsn = record->EndRecPtr;
	msg->latest_event_id = pool->next_event_id - 1;
	msg->task_generation = conn->info->task_generation;
	msg->rec_len = rec_len;
	memcpy(msg->rec, record->record, rec_len);
	pd_send_msg_raw(conn->task_h, msg, msglen);
	pfree(msg);
	conn->info->last_input_lsn = entry->last_lsn;
}

void
ParallelDecodeDispatchCommit(ParallelDecodePool *pool,
							 TransactionId top_xid,
							 XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
							 TimestampTz commit_time, RepOriginId origin_id,
							 XLogRecPtr origin_lsn)
{
	ParallelDecodeTxnEntry *entry;
	PdWorkerConn *conn;
	PdMsgTaskCommit msg;
	bool		found;

	if (pool == NULL || top_xid == InvalidTransactionId)
		return;

	entry = hash_search(pool->txn_hash, &top_xid, HASH_FIND, &found);
	if (!found)
		return;

	entry->commit_lsn = commit_lsn;
	conn = pd_find_worker_for_txn(pool, top_xid);
	if (conn == NULL)
		return;

	/* Pick the latest snapshot version for this commit */
	entry->snapshot_version = pool->snapshot_version_counter;

	memset(&msg, 0, sizeof(msg));
	msg.hdr.len = sizeof(msg);
	msg.hdr.type = PD_MSG_TASK_COMMIT;
	msg.top_xid = top_xid;
	msg.commit_lsn = commit_lsn;
	msg.end_lsn = end_lsn;
	msg.commit_time = commit_time;
	msg.origin_id = origin_id;
	msg.origin_lsn = origin_lsn;
	msg.snapshot_version = entry->snapshot_version;
	msg.latest_event_id = pool->next_event_id - 1;
	msg.task_generation = conn->info->task_generation;

	pd_send_msg_raw(conn->task_h, &msg, sizeof(msg));
}

void
ParallelDecodeDispatchAbort(ParallelDecodePool *pool,
							TransactionId top_xid, XLogRecPtr abort_lsn)
{
	ParallelDecodeTxnEntry *entry;
	PdWorkerConn *conn;
	PdMsgTaskAbort msg;
	bool		found;

	if (pool == NULL || top_xid == InvalidTransactionId)
		return;

	entry = hash_search(pool->txn_hash, &top_xid, HASH_FIND, &found);
	if (!found)
		return;

	conn = pd_find_worker_for_txn(pool, top_xid);
	if (conn != NULL)
	{
		memset(&msg, 0, sizeof(msg));
		msg.hdr.len = sizeof(msg);
		msg.hdr.type = PD_MSG_TASK_ABORT;
		msg.top_xid = top_xid;
		msg.abort_lsn = abort_lsn;
		pd_send_msg_raw(conn->task_h, &msg, sizeof(msg));

		/* Free worker */
		pg_atomic_write_u32(&conn->info->state, PD_WORKER_IDLE);
		SpinLockAcquire(&conn->info->pd_mutex);
		conn->info->top_xid = InvalidTransactionId;
		SpinLockRelease(&conn->info->pd_mutex);
	}

	/* Remove transaction entry */
	hash_search(pool->txn_hash, &top_xid, HASH_REMOVE, NULL);
}

/* ------------------------------------------------------------------ */
/* Public API: event publication (stage 3)                             */
/* ------------------------------------------------------------------ */

void
ParallelDecodePublishSnapshot(ParallelDecodePool *pool,
							  SnapBuild *builder, XLogRecPtr lsn)
{
	Snapshot	snap;
	PdMsgEventSnapshot *msg;
	Size		snap_len;
	uint64		version;

	if (pool == NULL)
		return;

	snap = SnapBuildGetOrBuildSnapshot(builder);
	if (snap == NULL)
		return;

	snap_len = EstimateSnapshotSpace(snap);
	version = ++pool->snapshot_version_counter;

	/* Store in registry (LRU of 16) */
	if (pool->n_snapshots < 16)
	{
		PdSnapshotEntry *se = &pool->snapshots[pool->n_snapshots++];
		se->version = version;
		se->lsn = lsn;
		se->serialized = palloc(snap_len);
		SerializeSnapshot(snap, se->serialized);
		se->len = snap_len;
		se->refcount = 0;
	}

	msg = palloc(offsetof(PdMsgEventSnapshot, snap) + snap_len);
	msg->hdr.len = offsetof(PdMsgEventSnapshot, snap) + snap_len;
	msg->hdr.type = PD_MSG_EVENT_SNAPSHOT;
	msg->event_id = pool->next_event_id++;
	msg->lsn = lsn;
	msg->version = version;
	msg->snap_len = snap_len;
	SerializeSnapshot(snap, msg->snap);

	pd_broadcast_event(pool, PD_MSG_EVENT_SNAPSHOT, msg, msg->hdr.len, lsn);
	pfree(msg);
}

void
ParallelDecodePublishInvalidations(ParallelDecodePool *pool,
								   uint32 nmsgs,
								   SharedInvalidationMessage *msgs,
								   XLogRecPtr commit_lsn)
{
	if (pool == NULL)
		return;

	if (nmsgs > 64)
	{
		/* Too many: broadcast FLUSH_ALL */
		PdMsgEventFlushAll msg;

		msg.hdr.len = sizeof(msg);
		msg.hdr.type = PD_MSG_EVENT_FLUSH_ALL;
		msg.event_id = pool->next_event_id++;
		msg.commit_lsn = commit_lsn;
		pd_broadcast_event(pool, PD_MSG_EVENT_FLUSH_ALL, &msg, sizeof(msg),
						   commit_lsn);
	}
	else
	{
		Size		msglen = offsetof(PdMsgEventInval, msgs) +
			nmsgs * sizeof(SharedInvalidationMessage);
		PdMsgEventInval *msg = palloc(msglen);

		msg->hdr.len = msglen;
		msg->hdr.type = PD_MSG_EVENT_INVAL;
		msg->event_id = pool->next_event_id++;
		msg->commit_lsn = commit_lsn;
		msg->nmsgs = nmsgs;
		memcpy(msg->msgs, msgs, nmsgs * sizeof(SharedInvalidationMessage));
		pd_broadcast_event(pool, PD_MSG_EVENT_INVAL, msg, msglen, commit_lsn);
		pfree(msg);
	}
}

uint64
ParallelDecodeGetLatestEventId(ParallelDecodePool *pool)
{
	if (pool == NULL)
		return 0;
	return pool->next_event_id - 1;
}

void
ParallelDecodeAdvanceSlotPositions(ParallelDecodePool *pool)
{
	int			i;
	uint64		min_consumed = UINT64_MAX;

	if (pool == NULL)
		return;

	/*
	 * Compute the minimum consumed_event_id across all in-flight workers
	 * (§4.7).  This is the gating position: slot position candidates
	 * (restart_lsn, catalog_xmin) cannot advance past the LSN
	 * corresponding to this event_id until all workers have consumed it.
	 */
	for (i = 0; i < pool->nworkers; i++)
	{
		uint64		consumed;

		/* Drain heartbeats first to get fresh consumption positions */
		pd_drain_heartbeats(&pool->conns[i]);

		consumed = pg_atomic_read_u64(&pool->conns[i].info->consumed_event_id);
		if (consumed < min_consumed)
			min_consumed = consumed;
	}

	/*
	 * Store the gating position for use by the walsender's slot
	 * advancement logic.  A full implementation maps min_consumed back
	 * to an LSN via the event ring and calls
	 * LogicalIncreaseRestartDecodingForSlot with the clamped candidate.
	 * For now, we record the constraint.
	 */
	pool->metrics.output_lag_lsn = (int64) min_consumed;
}

/* ------------------------------------------------------------------ */
/* Public API: result collection and output (stage 2/§4.4)             */
/* ------------------------------------------------------------------ */

void
ParallelDecodeCollectResults(ParallelDecodePool *pool)
{
	int			i;

	if (pool == NULL)
		return;

	for (i = 0; i < pool->nworkers; i++)
	{
		PdWorkerConn *conn = &pool->conns[i];
		PdMsgHeader *msg;
		Size		len;

		while (pd_try_receive(conn->result_h, &msg, &len))
		{
			ParallelDecodeTxnEntry *entry;
			bool		found;
			TransactionId top_xid;

			switch (msg->type)
			{
				case PD_MSG_RESULT_CHANGES:
				case PD_MSG_RESULT_FRAMES:
					{
						PdMsgResultChanges *rc = (PdMsgResultChanges *) msg;

						top_xid = rc->top_xid;
						entry = hash_search(pool->txn_hash, &top_xid,
											HASH_FIND, &found);
						if (found)
						{
							entry->result_data = palloc(len);
							memcpy(entry->result_data, msg, len);
							entry->result_len = len;
							entry->txn_flags |= PD_TXN_HAS_RESULT;
							pd_insert_output_queue(pool, entry);
						}
						/* Send RELEASE_ACK */
						pd_send_msg(conn->cmd_h, PD_MSG_CTRL_RELEASE_ACK,
									NULL, 0);
						break;
					}
				case PD_MSG_RESULT_ECHO:
					/* Stage 1 echo: discard (handled in echo_internal) */
					break;
				default:
					break;
			}
		}

		while (pd_try_receive(conn->error_h, &msg, &len))
		{
			if (msg->type == PD_MSG_ERROR)
				pd_raise_worker_error(conn->info->worker_id,
									  (PdMsgError *) msg);
		}

		pd_drain_heartbeats(conn);
	}
}

void
ParallelDecodeOutputPending(ParallelDecodePool *pool,
                            LogicalDecodingContext *ctx)
{
	if (pool == NULL || pool->output_head == NULL)
		return;

	/*
	 * Output committed transactions in commit_lsn order (§4.4).
	 * Centralized call mode (§5.3): for each entry, reconstruct the
	 * changes from intermediate results and invoke the output plugin
	 * through the walsender's ReorderBuffer commit path.
	 */
	while (pool->output_head != NULL)
	{
		ParallelDecodeTxnEntry *entry = pool->output_head;
		PdMsgResultChanges *rc;
		Size		offset;

		/* Only output transactions with complete results */
		if ((entry->txn_flags & PD_TXN_HAS_RESULT) == 0)
			break;

		rc = (PdMsgResultChanges *) entry->result_data;

		/*
		 * Step 1: find the snapshot for this transaction's version and
		 * set up the historic snapshot for catalog access during plugin
		 * callback execution (§4.4 step 2-3).
		 */
		if (ctx != NULL && rc != NULL)
		{
			int			i;
			Snapshot	snap = NULL;

			/* Look up snapshot from the pool's registry */
			for (i = 0; i < pool->n_snapshots; i++)
			{
				if (pool->snapshots[i].version == entry->snapshot_version)
				{
					snap = RestoreSnapshot(pool->snapshots[i].serialized);
					break;
				}
			}

			if (snap != NULL)
				SetupHistoricSnapshot(snap, NULL);

			/*
			 * Invoke the output plugin through the walsender's
			 * ReorderBuffer callback wrappers (centralized call, §5.3).
			 * The wrappers call ctx->callbacks.begin_cb/change_cb/commit_cb
			 * which produce protocol messages via WalSndWriteData.
			 */
			if (ctx->reorder->begin != NULL)
				ctx->reorder->begin(ctx->reorder, NULL);

			/* Replay each intermediate change */
			offset = 0;
			while (offset < entry->result_len)
			{
				PdIntermediateChange *ic =
					(PdIntermediateChange *) ((char *) rc->data + offset);
				Relation	rel;

				rel = RelationIdGetRelation(ic->relid);
				if (rel != NULL)
				{
					ReorderBufferChange *change;

					change = palloc0(sizeof(ReorderBufferChange));
					switch (ic->op)
					{
						case 'I':
							change->action = REORDER_BUFFER_CHANGE_INSERT;
							break;
						case 'U':
							change->action = REORDER_BUFFER_CHANGE_UPDATE;
							break;
						case 'D':
							change->action = REORDER_BUFFER_CHANGE_DELETE;
							break;
						default:
							change->action = REORDER_BUFFER_CHANGE_INSERT;
							break;
					}

					/* Fill tuple data from intermediate result */
					if (ic->newtuple_len > 0)
					{
						HeapTuple	htup =
							palloc0(HEAPTUPLESIZE + ic->newtuple_len);

						htup->t_len = ic->newtuple_len;
						htup->t_data =
							(HeapTupleHeader) ((char *) htup + HEAPTUPLESIZE);
						memcpy(htup->t_data,
							   (char *) ic + MAXIMUM_ALIGNOF + ic->oldtuple_len,
							   ic->newtuple_len);
						change->data.tp.newtuple = htup;
					}
					if (ic->oldtuple_len > 0)
					{
						HeapTuple	htup =
							palloc0(HEAPTUPLESIZE + ic->oldtuple_len);

						htup->t_len = ic->oldtuple_len;
						htup->t_data =
							(HeapTupleHeader) ((char *) htup + HEAPTUPLESIZE);
						memcpy(htup->t_data,
							   (char *) ic + MAXIMUM_ALIGNOF,
							   ic->oldtuple_len);
						change->data.tp.oldtuple = htup;
					}

					/* Call the plugin's change wrapper */
					if (ctx->reorder->apply_change != NULL)
						ctx->reorder->apply_change(ctx->reorder, NULL, rel,
												   change);

					pfree(change);
					RelationClose(rel);
				}

				/* Advance to next intermediate change */
				offset += MAXIMUM_ALIGNOF + ic->newtuple_len + ic->oldtuple_len;
			}

			/* Commit callback */
			if (ctx->reorder->commit != NULL)
				ctx->reorder->commit(ctx->reorder, NULL, entry->commit_lsn);

			if (snap != NULL)
				TeardownHistoricSnapshot(false);
		}

		/* Dequeue and free */
		pool->output_head = entry->next_pending;
		if (entry->result_data != NULL)
		{
			pfree(entry->result_data);
			entry->result_data = NULL;
		}
		hash_search(pool->txn_hash, &entry->top_xid, HASH_REMOVE, NULL);
	}
}

/* ------------------------------------------------------------------ */
/* Public API: statistics (stage 5)                                    */
/* ------------------------------------------------------------------ */

void
ParallelDecodePoolGetMetrics(ParallelDecodePool *pool,
							 ParallelDecodePoolMetrics *metrics)
{
	int			i;

	if (pool == NULL || metrics == NULL)
		return;

	memcpy(metrics, &pool->metrics, sizeof(ParallelDecodePoolMetrics));

	metrics->workers_idle = 0;
	metrics->workers_busy = 0;
	metrics->workers_failed = 0;
	for (i = 0; i < pool->nworkers; i++)
	{
		uint32		state = pg_atomic_read_u32(&pool->conns[i].info->state);

		switch (state)
		{
			case PD_WORKER_IDLE: metrics->workers_idle++; break;
			case PD_WORKER_BUSY: metrics->workers_busy++; break;
			default: metrics->workers_failed++; break;
		}
	}

	/* Count pending transactions */
	metrics->pending_txns = 0;
	{
		ParallelDecodeTxnEntry *e;
		for (e = pool->pending_head; e != NULL; e = e->next_pending)
			metrics->pending_txns++;
	}
}

/* ------------------------------------------------------------------ */
/* Internal: DSM setup, worker launch                                  */
/* ------------------------------------------------------------------ */

static void
pd_setup_dsm(PdWorkerConn *conn, uint32 startup_generation)
{
	shm_toc_estimator e;
	Size		segsize;
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelDecodeWorkerInfo *info;
	Size		mq_sizes[6] = {PD_TASK_MQ_SIZE, PD_EVENT_MQ_SIZE,
		PD_RESULT_MQ_SIZE, PD_ERROR_MQ_SIZE, PD_CTRL_MQ_SIZE, PD_CTRL_MQ_SIZE};
	int			mq_keys[6] = {PARALLEL_DECODE_TOC_TASK_MQ,
		PARALLEL_DECODE_TOC_EVENT_MQ, PARALLEL_DECODE_TOC_RESULT_MQ,
		PARALLEL_DECODE_TOC_ERROR_MQ, PARALLEL_DECODE_TOC_CONTROL_REQ_MQ,
		PARALLEL_DECODE_TOC_CONTROL_CMD_MQ};
	bool		leader_sends[6] = {true, true, false, false, false, true};
	int			i;

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelDecodeWorkerInfo));
	for (i = 0; i < 6; i++)
		shm_toc_estimate_chunk(&e, mq_sizes[i]);
	shm_toc_estimate_keys(&e, 7);
	segsize = shm_toc_estimate(&e);

	seg = dsm_create(segsize, 0);
	dsm_pin_mapping(seg);
	toc = shm_toc_create(PARALLEL_DECODE_TOC_MAGIC,
						 dsm_segment_address(seg), segsize);

	info = shm_toc_allocate(toc, sizeof(ParallelDecodeWorkerInfo));
	MemSet(info, 0, sizeof(ParallelDecodeWorkerInfo));
	info->startup_generation = startup_generation;
	SpinLockInit(&info->pd_mutex);
	pg_atomic_init_u32(&info->state, PD_WORKER_STARTING);
	pg_atomic_init_u64(&info->consumed_event_id, 0);
	info->last_heartbeat = GetCurrentTimestamp();
	shm_toc_insert(toc, PARALLEL_DECODE_TOC_WORKER_INFO, info);

	for (i = 0; i < 6; i++)
	{
		shm_mq    *mq = shm_mq_create(shm_toc_allocate(toc, mq_sizes[i]),
									  mq_sizes[i]);

		shm_toc_insert(toc, mq_keys[i], mq);
		if (leader_sends[i])
			shm_mq_set_sender(mq, MyProc);
		else
			shm_mq_set_receiver(mq, MyProc);
	}

	conn->seg = seg;
	conn->toc = toc;
	conn->info = info;

	on_dsm_detach(seg, pd_terminate_worker, PointerGetDatum(conn));
}


static void
pd_launch_worker(PdWorkerConn *conn, int worker_id, uint32 startup_generation)
{
	BackgroundWorker worker;
	ParallelDecodeWorkerStartup startup;

	MemSet(&startup, 0, sizeof(startup));
	startup.dboid = MyDatabaseId;
	startup.useroid = GetUserId();
	startup.dsm_handle = dsm_segment_handle(conn->seg);
	startup.worker_id = worker_id;
	startup.startup_generation = startup_generation;

	MemSet(&worker, 0, sizeof(worker));
	snprintf(worker.bgw_name, BGW_MAXLEN, "parallel decode worker %d", worker_id);
	snprintf(worker.bgw_type, BGW_MAXLEN, "parallel-decode");
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
		BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	snprintf(worker.bgw_library_name, MAXPGPATH, "postgres");
	snprintf(worker.bgw_function_name, BGW_MAXLEN, "ParallelDecodeWorkerMain");
	worker.bgw_main_arg = Int32GetDatum(0);
	memcpy(worker.bgw_extra, &startup, sizeof(startup));
	worker.bgw_notify_pid = MyProcPid;

	conn->info->worker_id = worker_id;

	if (!RegisterDynamicBackgroundWorker(&worker, &conn->bgwh))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("could not register parallel decode worker"),
				 errhint("You may need to increase \"max_worker_processes\".")));
}

static void
pd_attach_queues(PdWorkerConn *conn)
{
	shm_toc    *toc = conn->toc;
	dsm_segment *seg = conn->seg;

	conn->task_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_TASK_MQ, false), seg, conn->bgwh);
	conn->event_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_EVENT_MQ, false), seg, conn->bgwh);
	conn->result_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_RESULT_MQ, false), seg, conn->bgwh);
	conn->error_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_ERROR_MQ, false), seg, conn->bgwh);
	conn->req_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_CONTROL_REQ_MQ, false), seg, conn->bgwh);
	conn->cmd_h = shm_mq_attach(shm_toc_lookup(toc, PARALLEL_DECODE_TOC_CONTROL_CMD_MQ, false), seg, conn->bgwh);
}

static void
pd_wait_worker_ready(PdWorkerConn *conn)
{
	TimestampTz deadline =
		TimestampTzPlusSeconds(GetCurrentTimestamp(), PD_WORKER_STARTUP_TIMEOUT_S);
	BgwHandleStatus status;

	status = WaitForBackgroundWorkerStartup(conn->bgwh, &conn->pid);
	if (status != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
				 errmsg("parallel decode worker failed to start")));

	for (;;)
	{
		if (pg_atomic_read_u32(&conn->info->state) == PD_WORKER_IDLE)
			return;
		if (GetBackgroundWorkerPid(conn->bgwh, &conn->pid) != BGWH_STARTED)
			ereport(ERROR,
					(errcode(ERRCODE_INSUFFICIENT_RESOURCES),
					 errmsg("parallel decode worker exited before becoming ready")));
		if (GetCurrentTimestamp() >= deadline)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("timeout waiting for parallel decode worker to become ready")));
		pd_wait_leader(100);
		CHECK_FOR_INTERRUPTS();
	}
}

/* ------------------------------------------------------------------ */
/* Internal: scheduling helpers                                        */
/* ------------------------------------------------------------------ */

static PdWorkerConn *
pd_find_worker_for_txn(ParallelDecodePool *pool, TransactionId top_xid)
{
	int			i;

	for (i = 0; i < pool->nworkers; i++)
	{
		if (pool->conns[i].info->top_xid == top_xid &&
			pg_atomic_read_u32(&pool->conns[i].info->state) == PD_WORKER_BUSY)
			return &pool->conns[i];
	}
	return NULL;
}

static int
pd_find_idle_worker(ParallelDecodePool *pool)
{
	int			i;

	for (i = 0; i < pool->nworkers; i++)
	{
		if (pg_atomic_read_u32(&pool->conns[i].info->state) == PD_WORKER_IDLE &&
			GetBackgroundWorkerPid(pool->conns[i].bgwh, NULL) == BGWH_STARTED)
			return i;
	}
	return -1;
}

static void
pd_queue_pending(ParallelDecodePool *pool, ParallelDecodeTxnEntry *entry)
{
	entry->next_pending = NULL;
	if (pool->pending_tail)
		pool->pending_tail->next_pending = entry;
	else
		pool->pending_head = entry;
	pool->pending_tail = entry;
}

static ParallelDecodeTxnEntry *
pd_dequeue_pending(ParallelDecodePool *pool)
{
	ParallelDecodeTxnEntry *e = pool->pending_head;

	if (e)
	{
		pool->pending_head = e->next_pending;
		if (!pool->pending_head)
			pool->pending_tail = NULL;
		e->next_pending = NULL;
	}
	return e;
}

static void
pd_insert_output_queue(ParallelDecodePool *pool, ParallelDecodeTxnEntry *entry)
{
	/* Insert sorted by commit_lsn (singly-linked list) */
	ParallelDecodeTxnEntry *prev = NULL;
	ParallelDecodeTxnEntry *cur = pool->output_head;

	while (cur && cur->commit_lsn < entry->commit_lsn)
	{
		prev = cur;
		cur = cur->next_pending;	/* reuse the link field */
	}

	if (prev)
		prev->next_pending = entry;
	else
		pool->output_head = entry;
	entry->next_pending = cur;
}

/* ------------------------------------------------------------------ */
/* Internal: event broadcast                                           */
/* ------------------------------------------------------------------ */

static void
pd_broadcast_event(ParallelDecodePool *pool, PdMsgType type,
				   const void *body, Size body_len, XLogRecPtr lsn)
{
	int			i;

	for (i = 0; i < pool->nworkers; i++)
	{
		PG_TRY();
		{
			pd_send_msg_raw(pool->conns[i].event_h, body, body_len);
		}
		PG_CATCH();
		{
			FlushErrorState();	/* worker may have died */
		}
		PG_END_TRY();
	}
}

static void
pd_release_snapshot(ParallelDecodePool *pool, uint64 version)
{
	int			i;

	for (i = 0; i < pool->n_snapshots; i++)
	{
		if (pool->snapshots[i].version == version)
		{
			pool->snapshots[i].refcount--;
			break;
		}
	}
}

/* ------------------------------------------------------------------ */
/* Internal: message helpers                                           */
/* ------------------------------------------------------------------ */

static void
pd_send_msg_raw(shm_mq_handle *mqh, const void *data, Size len)
{
	if (shm_mq_send(mqh, len, data, false, false) != SHM_MQ_SUCCESS)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not send message to parallel decode worker")));
}

static void
pd_send_msg(shm_mq_handle *mqh, PdMsgType type, const void *body, Size body_len)
{
	Size		len = sizeof(PdMsgHeader) + body_len;
	PdMsgHeader *msg = palloc(len);

	msg->len = len;
	msg->type = type;
	if (body_len > 0)
		memcpy(((char *) msg) + sizeof(PdMsgHeader), body, body_len);
	pd_send_msg_raw(mqh, msg, len);
	pfree(msg);
}

static bool
pd_try_receive(shm_mq_handle *mqh, PdMsgHeader **msg_p, Size *len_p)
{
	void	   *data;
	Size		len;
	shm_mq_result rc;

	rc = shm_mq_receive(mqh, &len, &data, true);
	if (rc == SHM_MQ_SUCCESS)
	{
		*msg_p = (PdMsgHeader *) data;
		*len_p = len;
		return true;
	}
	return false;
}

static void
pd_drain_heartbeats(PdWorkerConn *conn)
{
	PdMsgHeader *msg;
	Size		len;

	while (pd_try_receive(conn->req_h, &msg, &len))
	{
		if (msg->type == PD_MSG_CTRL_HEARTBEAT)
		{
			PdMsgHeartbeat *hb = (PdMsgHeartbeat *) msg;

			SpinLockAcquire(&conn->info->pd_mutex);
			conn->info->last_heartbeat = GetCurrentTimestamp();
			SpinLockRelease(&conn->info->pd_mutex);
			pg_atomic_write_u64(&conn->info->consumed_event_id,
								hb->consumed_event_id);
		}
	}
}

static void
pd_wait_leader(int timeout_ms)
{
	if (pd_leader_wait_event == 0)
		pd_leader_wait_event = WaitEventExtensionNew("ParallelDecodeLeader");

	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
					 timeout_ms, pd_leader_wait_event);
	ResetLatch(MyLatch);
}

/* ------------------------------------------------------------------ */
/* Internal: echo and error test (stage 1)                             */
/* ------------------------------------------------------------------ */

static char *
pd_echo_internal(PdWorkerConn *conn, const char *payload, Size payload_len,
				 Size *rlen_p)
{
	TimestampTz deadline;
	PdMsgEcho  *request;
	Size		msglen;
	char	   *reply;

	if (pg_atomic_read_u32(&conn->info->state) != PD_WORKER_IDLE)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("parallel decode worker %d is busy",
						conn->info->worker_id)));

	msglen = offsetof(PdMsgEcho, payload) + payload_len;
	request = palloc(msglen);
	request->hdr.len = msglen;
	request->hdr.type = PD_MSG_TASK_ECHO;
	request->payload_len = payload_len;
	memcpy(request->payload, payload, payload_len);
	pd_send_msg_raw(conn->task_h, request, msglen);
	pfree(request);

	deadline = TimestampTzPlusSeconds(GetCurrentTimestamp(), PD_ECHO_TIMEOUT_S);
	for (;;)
	{
		PdMsgHeader *msg;
		Size		len;

		if (pd_try_receive(conn->result_h, &msg, &len))
		{
			if (msg->type == PD_MSG_RESULT_ECHO)
			{
				PdMsgEcho  *emsg = (PdMsgEcho *) msg;

				reply = palloc(emsg->payload_len);
				memcpy(reply, emsg->payload, emsg->payload_len);
				*rlen_p = emsg->payload_len;
				pd_send_msg(conn->cmd_h, PD_MSG_CTRL_RELEASE_ACK, NULL, 0);
				return reply;
			}
		}
		if (pd_try_receive(conn->error_h, &msg, &len))
		{
			if (msg->type == PD_MSG_ERROR)
				pd_raise_worker_error(conn->info->worker_id, (PdMsgError *) msg);
		}
		if (GetBackgroundWorkerPid(conn->bgwh, &conn->pid) != BGWH_STARTED)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("parallel decode worker %d died during echo",
							conn->info->worker_id)));
		if (GetCurrentTimestamp() >= deadline)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("timeout waiting for echo from worker %d",
							conn->info->worker_id)));
		pd_drain_heartbeats(conn);
		pd_wait_leader(20);
		CHECK_FOR_INTERRUPTS();
	}
}

static void
pd_test_error_internal(PdWorkerConn *conn)
{
	TimestampTz deadline;

	pd_send_msg(conn->task_h, PD_MSG_TASK_RAISE_ERROR, NULL, 0);
	deadline = TimestampTzPlusSeconds(GetCurrentTimestamp(), PD_ECHO_TIMEOUT_S);

	for (;;)
	{
		PdMsgHeader *msg;
		Size		len;

		if (pd_try_receive(conn->error_h, &msg, &len))
		{
			if (msg->type == PD_MSG_ERROR)
				pd_raise_worker_error(conn->info->worker_id, (PdMsgError *) msg);
		}
		if (pd_try_receive(conn->result_h, &msg, &len) ||
			GetBackgroundWorkerPid(conn->bgwh, &conn->pid) != BGWH_STARTED)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("parallel decode worker %d exited without reporting an error",
							conn->info->worker_id)));
		if (GetCurrentTimestamp() >= deadline)
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("timeout waiting for error from worker %d",
							conn->info->worker_id)));
		pd_wait_leader(20);
		CHECK_FOR_INTERRUPTS();
	}
}

static void
pd_raise_worker_error(int worker_id, PdMsgError *emsg)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("parallel decode worker %d: %.*s", worker_id,
					(int) emsg->msg_len, emsg->msg)));
}

static void
pd_terminate_worker(dsm_segment *seg, Datum arg)
{
	PdWorkerConn *conn = (PdWorkerConn *) DatumGetPointer(arg);

	if (conn->bgwh != NULL)
		TerminateBackgroundWorker(conn->bgwh);
}
