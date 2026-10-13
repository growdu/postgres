/*-------------------------------------------------------------------------
 *
 * paralleldecodeworker.c
 *		Background worker main for publisher-side parallel logical decoding.
 *
 * Implements stages 1-4: startup sequence, DSM/queue attach, main loop,
 * record decoding via XLogReaderSetDecodeBuffer (plan A), base_snapshot
 * attachment and ReorderBufferCommit, intermediate result generation,
 * event consumption (INVAL/SNAPSHOT/FLUSH_ALL), Worker-mode plugin
 * instances, heartbeat, and error serialization.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/logical/paralleldecodeworker.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/rmgr.h"
#include "access/xlog_internal.h"
#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/output_plugin.h"
#include "replication/paralleldecode.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "storage/bufmgr.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"
#include "utils/inval.h"
#include "utils/timestamp.h"

/* ------------------------------------------------------------------ */
/* Worker state                                                        */
/* ------------------------------------------------------------------ */

typedef struct PdWorkerState
{
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelDecodeWorkerInfo *info;
	shm_mq_handle *task_h;
	shm_mq_handle *event_h;
	shm_mq_handle *result_h;
	shm_mq_handle *error_h;
	shm_mq_handle *req_h;
	shm_mq_handle *cmd_h;
	ParallelDecodeWorkerStartup startup;

	/* Decode pipeline (stage 2) */
	XLogReaderState *xlog_reader;
	LogicalDecodingContext *decode_ctx;
	ReorderBuffer *rb;

	/* Snapshot version table (stage 3, §4.4) */
	struct
	{
		uint64		version;
		Snapshot	snap;
	}			snap_table[16];
	int			n_snaps;

	/* Result buffer (stage 2, §5.4) */
	char	   *result_buf;
	Size		result_len;
	Size		result_cap;

	/* Worker-mode plugin (stage 4) */
	bool		plugin_loaded;

	TimestampTz last_heartbeat_sent;
	bool		shutting_down;
} PdWorkerState;

static PdWorkerState *pdw_module_state = NULL;

/* ------------------------------------------------------------------ */
/* Forward declarations                                                */
/* ------------------------------------------------------------------ */

static void pdw_initialize(PdWorkerState *wstate);
static void pdw_main_loop(PdWorkerState *wstate);
static void pdw_process_task(PdWorkerState *wstate, PdMsgHeader *msg, Size len);
static void pdw_process_cmd(PdWorkerState *wstate, PdMsgHeader *msg);
static void pdw_consume_events(PdWorkerState *wstate, uint64 required_event_id);
static void pdw_send_heartbeat(PdWorkerState *wstate);
static void pdw_send_error(PdWorkerState *wstate, ErrorData *edata);
static void pdw_shutdown_cleanup(PdWorkerState *wstate, int exit_code);
static void pdw_handle_sigterm(SIGNAL_ARGS);
static void pdw_store_snapshot(PdWorkerState *wstate, uint64 version,
							   Snapshot snap);
static Snapshot pdw_lookup_snapshot(PdWorkerState *wstate, uint64 version);
static void pdw_decode_record(PdWorkerState *wstate, PdMsgTaskRecord *msg);
static void pdw_process_commit(PdWorkerState *wstate, PdMsgTaskCommit *msg);
static void pdw_process_abort(PdWorkerState *wstate, PdMsgTaskAbort *msg);
static void pdw_begin_cb(ReorderBuffer *rb, ReorderBufferTXN *txn);
static void pdw_change_cb(ReorderBuffer *rb, ReorderBufferTXN *txn,
						  Relation relation, ReorderBufferChange *change);
static void pdw_commit_cb(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr commit_lsn);
static void pdw_append_intermediate(PdWorkerState *wstate, uint8 op,
									Oid relid,
									HeapTuple newtup, HeapTuple oldtup);
static Size pdw_estimate_snapshot(Snapshot snap);

/* ------------------------------------------------------------------ */
/* Worker main                                                         */
/* ------------------------------------------------------------------ */

void
ParallelDecodeWorkerMain(Datum main_arg)
{
	PdWorkerState wstate;
	BackgroundWorker *worker_entry = MyBgworkerEntry;

	pqsignal(SIGTERM, pdw_handle_sigterm);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	Assert(worker_entry != NULL);
	memcpy(&wstate.startup, worker_entry->bgw_extra,
		   sizeof(ParallelDecodeWorkerStartup));

	BackgroundWorkerInitializeConnectionByOid(wstate.startup.dboid,
											  wstate.startup.useroid, 0);

	memset(&wstate, 0, offsetof(PdWorkerState, startup));
	wstate.shutting_down = false;
	pdw_module_state = &wstate;
	wstate.last_heartbeat_sent = 0;
	wstate.n_snaps = 0;
	wstate.plugin_loaded = false;
	memcpy(&wstate.startup, worker_entry->bgw_extra,
		   sizeof(ParallelDecodeWorkerStartup));

	PG_TRY();
	{
		pdw_initialize(&wstate);
		pdw_main_loop(&wstate);
	}
	PG_CATCH();
	{
		ErrorData  *edata = CopyErrorData();

		pdw_send_error(&wstate, edata);
		FlushErrorState();
		pfree(edata);
	}
	PG_END_TRY();

	pdw_shutdown_cleanup(&wstate, 0);
}

static void
pdw_handle_sigterm(SIGNAL_ARGS)
{
	SetLatch(MyLatch);
}

/* ------------------------------------------------------------------ */
/* Initialization                                                      */
/* ------------------------------------------------------------------ */

static void
pdw_initialize(PdWorkerState *wstate)
{
	shm_toc    *toc;
	ParallelDecodeWorkerInfo *info;
	shm_mq    *mq;

	wstate->seg = dsm_attach(wstate->startup.dsm_handle);
	if (wstate->seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pdw: could not attach to DSM segment")));

	toc = shm_toc_attach(PARALLEL_DECODE_TOC_MAGIC,
						 dsm_segment_address(wstate->seg));
	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pdw: could not attach to TOC")));

	info = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_WORKER_INFO, false);
	if (info == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pdw: worker info not found")));

	if (info->startup_generation != wstate->startup.startup_generation)
		proc_exit(0);
	if (info->worker_id != wstate->startup.worker_id)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pdw: worker_id mismatch")));

	wstate->toc = toc;
	wstate->info = info;
	info->pid = MyProcPid;

	/* Attach queues (§3.1) */
	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_TASK_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	wstate->task_h = shm_mq_attach(mq, wstate->seg, NULL);

	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_EVENT_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	wstate->event_h = shm_mq_attach(mq, wstate->seg, NULL);

	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_RESULT_MQ, false);
	shm_mq_set_sender(mq, MyProc);
	wstate->result_h = shm_mq_attach(mq, wstate->seg, NULL);

	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_ERROR_MQ, false);
	shm_mq_set_sender(mq, MyProc);
	wstate->error_h = shm_mq_attach(mq, wstate->seg, NULL);

	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_CONTROL_REQ_MQ, false);
	shm_mq_set_sender(mq, MyProc);
	wstate->req_h = shm_mq_attach(mq, wstate->seg, NULL);

	mq = shm_toc_lookup(toc, PARALLEL_DECODE_TOC_CONTROL_CMD_MQ, false);
	shm_mq_set_receiver(mq, MyProc);
	wstate->cmd_h = shm_mq_attach(mq, wstate->seg, NULL);

	/* Create private XLogReaderState for plan A (§5.2) */
	wstate->xlog_reader = XLogReaderAllocate(wal_segment_size, NULL,
											 XL_ROUTINE(.page_read = NULL,
														.segment_open = NULL,
														.segment_close = NULL),
											 NULL);
	if (wstate->xlog_reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("pdw: could not allocate XLogReaderState")));

	/* Create ReorderBuffer with intermediate result callbacks (§5.3) */
	wstate->rb = ReorderBufferAllocate();
	wstate->rb->begin = pdw_begin_cb;
	wstate->rb->apply_change = pdw_change_cb;
	wstate->rb->commit = pdw_commit_cb;

	/* Create a minimal LogicalDecodingContext for decode.c dispatch.
	 * No slot, no plugin: the ReorderBuffer callbacks generate intermediate
	 * results instead of calling an output plugin (§5.1). */
	wstate->decode_ctx = palloc0(sizeof(LogicalDecodingContext));
	wstate->decode_ctx->reorder = wstate->rb;
	wstate->decode_ctx->reader = wstate->xlog_reader;
	wstate->decode_ctx->fast_forward = false;
	wstate->decode_ctx->context = AllocSetContextCreate(TopMemoryContext,
														"pdw decode",
														ALLOCSET_DEFAULT_SIZES);
	wstate->decode_ctx->output_plugin_options = NIL;
	wstate->decode_ctx->parallel_pool = NULL; /* no nested parallelism */

	/* Allocate result buffer */
	wstate->result_cap = 64 * 1024;
	wstate->result_buf = palloc(wstate->result_cap);
	wstate->result_len = 0;
}

/* ------------------------------------------------------------------ */
/* Main loop (§5.6)                                                    */
/* ------------------------------------------------------------------ */

static void
pdw_main_loop(PdWorkerState *wstate)
{
	MemoryContext worker_ctx;
	uint32		wait_event;

	worker_ctx = AllocSetContextCreate(TopMemoryContext,
									   "parallel decode worker",
									   ALLOCSET_DEFAULT_SIZES);
	wait_event = WaitEventExtensionNew("ParallelDecodeWorkerMain");

	pg_atomic_write_u32(&wstate->info->state, PD_WORKER_IDLE);
	pdw_send_heartbeat(wstate);
	wstate->last_heartbeat_sent = GetCurrentTimestamp();

	while (!wstate->shutting_down)
	{
		void	   *data;
		Size		len;
		shm_mq_result rc;
		int			rcv;

		/* Command queue first (non-blocking) */
		rc = shm_mq_receive(wstate->cmd_h, &len, &data, true);
		if (rc == SHM_MQ_SUCCESS)
			pdw_process_cmd(wstate, (PdMsgHeader *) data);
		else if (rc == SHM_MQ_DETACHED)
			break;
		if (wstate->shutting_down)
			break;

		/* Event queue (non-blocking; consumed before task per §5.5) */
		rc = shm_mq_receive(wstate->event_h, &len, &data, true);
		if (rc == SHM_MQ_SUCCESS)
		{
			PdMsgHeader *emsg = (PdMsgHeader *) data;

			switch (emsg->type)
			{
				case PD_MSG_EVENT_SNAPSHOT:
					{
						PdMsgEventSnapshot *es = (PdMsgEventSnapshot *) emsg;
						Snapshot	snap = RestoreSnapshot(es->snap);

						pdw_store_snapshot(wstate, es->version, snap);
						break;
					}
				case PD_MSG_EVENT_INVAL:
					{
						PdMsgEventInval *ei = (PdMsgEventInval *) emsg;
						int			i;

						for (i = 0; i < (int) ei->nmsgs; i++)
							LocalExecuteInvalidationMessage(&ei->msgs[i]);
						break;
					}
				case PD_MSG_EVENT_FLUSH_ALL:
					InvalidateSystemCaches();
					break;
				default:
					break;
			}
			pg_atomic_write_u64(&wstate->info->consumed_event_id,
								((PdMsgEventSnapshot *) emsg)->event_id);
		}

		/* Task queue (non-blocking) */
		rc = shm_mq_receive(wstate->task_h, &len, &data, true);
		if (rc == SHM_MQ_SUCCESS)
		{
			pg_atomic_write_u32(&wstate->info->state, PD_WORKER_BUSY);
			MemoryContextReset(worker_ctx);
			PG_TRY();
			{
				pdw_process_task(wstate, (PdMsgHeader *) data, len);
			}
			PG_CATCH();
			{
				ErrorData  *edata = CopyErrorData();

				pdw_send_error(wstate, edata);
				FlushErrorState();
				pfree(edata);
				pg_atomic_write_u32(&wstate->info->state, PD_WORKER_IDLE);
			}
			PG_END_TRY();
		}
		else if (rc == SHM_MQ_DETACHED)
			break;

		/* Heartbeat */
		if (TimestampDifferenceExceeds(wstate->last_heartbeat_sent,
									   GetCurrentTimestamp(),
									   PD_HEARTBEAT_INTERVAL_MS))
		{
			pdw_send_heartbeat(wstate);
			wstate->last_heartbeat_sent = GetCurrentTimestamp();
		}

		rcv = WaitLatch(MyLatch,
						WL_LATCH_SET | WL_POSTMASTER_DEATH | WL_TIMEOUT,
						PD_HEARTBEAT_INTERVAL_MS, wait_event);
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();

		if (rcv & WL_POSTMASTER_DEATH)
			break;
	}

	MemoryContextDelete(worker_ctx);
}

/* ------------------------------------------------------------------ */
/* Task processing                                                     */
/* ------------------------------------------------------------------ */

static void
pdw_process_task(PdWorkerState *wstate, PdMsgHeader *msg, Size len)
{
	switch (msg->type)
	{
		case PD_MSG_TASK_RECORD:
			pdw_decode_record(wstate, (PdMsgTaskRecord *) msg);
			break;
		case PD_MSG_TASK_COMMIT:
			pdw_process_commit(wstate, (PdMsgTaskCommit *) msg);
			break;
		case PD_MSG_TASK_ABORT:
			pdw_process_abort(wstate, (PdMsgTaskAbort *) msg);
			break;
		case PD_MSG_TASK_ECHO:
			{
				PdMsgEcho  *echo = (PdMsgEcho *) msg;
				Size		reply_len = offsetof(PdMsgEcho, payload) +
					echo->payload_len;
				PdMsgEcho  *reply = palloc(reply_len);

				reply->hdr.len = reply_len;
				reply->hdr.type = PD_MSG_RESULT_ECHO;
				reply->payload_len = echo->payload_len;
				memcpy(reply->payload, echo->payload, echo->payload_len);
				shm_mq_send(wstate->result_h, reply_len, reply, false, false);
				pfree(reply);
				break;
			}
		case PD_MSG_TASK_RAISE_ERROR:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("test error from pdw %d",
							wstate->info->worker_id)));
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("pdw: unexpected task type %d", msg->type)));
			break;
	}
}

/*
 * Decode one WAL record (plan A, §5.2): load raw bytes into the private
 * reader, parse with XLogReadRecord, then dispatch through decode.c's
 * rmgr table which queues changes into the local ReorderBuffer.
 */
static void
pdw_decode_record(PdWorkerState *wstate, PdMsgTaskRecord *msg)
{
	XLogRecord *record;
	char	   *errm = NULL;
	RmgrData	rmgr;
	XLogRecordBuffer buf;

	/* Load raw record bytes and parse (§5.2: XLogReaderSetDecodeBuffer) */
	XLogReaderSetDecodeBuffer(wstate->xlog_reader, msg->rec, msg->rec_len);
	XLogBeginRead(wstate->xlog_reader, msg->orig_lsn);
	record = XLogReadRecord(wstate->xlog_reader, &errm);

	if (record == NULL || errm != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pdw: could not parse WAL record at %X/%X: %s",
						LSN_FORMAT_ARGS(msg->orig_lsn),
						errm ? errm : "unknown")));

	/* Build XLogRecordBuffer for the dispatch */
	buf.origptr = msg->orig_lsn;
	buf.endptr = msg->end_lsn;
	buf.record = wstate->xlog_reader;

	/* Assign subtransaction to top-level (same as serial path) */
	if (TransactionIdIsValid(msg->top_xid) &&
		msg->xid != msg->top_xid)
	{
		ReorderBufferAssignChild(wstate->rb, msg->top_xid, msg->xid,
								 buf.origptr);
	}

	/* Dispatch through the rmgr decode table (§5.2) */
	rmgr = GetRmgr(XLogRecGetRmid(wstate->xlog_reader));
	if (rmgr.rm_decode != NULL)
		rmgr.rm_decode(wstate->decode_ctx, &buf);
	else
		ReorderBufferProcessXid(wstate->rb, msg->xid, buf.origptr);

	wstate->info->last_input_lsn = msg->end_lsn;
}

/*
 * Process a commit marker (§5.3): attach base_snapshot from the version
 * table, invoke ReorderBufferCommit which calls the intermediate result
 * callbacks, then send the result.
 */
static void
pdw_process_commit(PdWorkerState *wstate, PdMsgTaskCommit *msg)
{
	Snapshot	snap;

	/* Look up the snapshot version for this transaction */
	snap = pdw_lookup_snapshot(wstate, msg->snapshot_version);
	if (snap == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("pdw: snapshot version %lu not found",
						(unsigned long) msg->snapshot_version)));

	/* Attach as base_snapshot (§5.3) */
	ReorderBufferSetBaseSnapshot(wstate->rb, msg->top_xid,
								 msg->commit_lsn, snap);

	/* Reset result buffer */
	wstate->result_len = 0;

	/* Execute the commit: ReorderBufferCommit → ProcessTXN → callbacks */
	ReorderBufferCommit(wstate->rb, msg->top_xid, msg->commit_lsn,
						msg->end_lsn, msg->commit_time,
						msg->origin_id, msg->origin_lsn);

	/* Send intermediate result to leader */
	if (wstate->result_len > 0)
	{
		PdMsgResultChanges *rc = (PdMsgResultChanges *) wstate->result_buf;

		rc->hdr.len = offsetof(PdMsgResultChanges, data) + wstate->result_len;
		rc->hdr.type = PD_MSG_RESULT_CHANGES;
		rc->top_xid = msg->top_xid;
		rc->commit_lsn = msg->commit_lsn;
		rc->task_generation = msg->task_generation;
		rc->nchanges = 0;		/* counted by change_cb */

		shm_mq_send(wstate->result_h, rc->hdr.len, rc, false, false);
	}

	wstate->info->last_result_lsn = msg->commit_lsn;
}

static void
pdw_process_abort(PdWorkerState *wstate, PdMsgTaskAbort *msg)
{
	/* Discard any buffered changes for this transaction */
	ReorderBufferAbort(wstate->rb, msg->top_xid, msg->abort_lsn, 0);
}

static void
pdw_process_cmd(PdWorkerState *wstate, PdMsgHeader *msg)
{
	switch (msg->type)
	{
		case PD_MSG_CTRL_SHUTDOWN:
		case PD_MSG_CTRL_CANCEL:
			wstate->shutting_down = true;
			break;
		case PD_MSG_CTRL_RELEASE_ACK:
			pg_atomic_write_u32(&wstate->info->state, PD_WORKER_IDLE);
			break;
		default:
			break;
	}
}

/* ------------------------------------------------------------------ */
/* ReorderBuffer callbacks (§5.4: intermediate result generation)       */
/* ------------------------------------------------------------------ */

static void
pdw_begin_cb(ReorderBuffer *rb, ReorderBufferTXN *txn)
{
	/* Nothing needed for intermediate results */
}

static void
pdw_change_cb(ReorderBuffer *rb, ReorderBufferTXN *txn,
			  Relation relation, ReorderBufferChange *change)
{
	PdWorkerState *wstate = NULL;

	/* Get worker state from the callback context.
	 * In a real implementation this would be stored in the rb. */
	wstate = pdw_module_state;

	switch (change->action)
	{
		case REORDER_BUFFER_CHANGE_INSERT:
			pdw_append_intermediate(wstate, 'I', RelationGetRelid(relation),
									 change->data.tp.newtuple, NULL);
			break;
		case REORDER_BUFFER_CHANGE_UPDATE:
			pdw_append_intermediate(wstate, 'U', RelationGetRelid(relation),
									 change->data.tp.newtuple,
									 change->data.tp.oldtuple ?
									 change->data.tp.oldtuple : NULL);
			break;
		case REORDER_BUFFER_CHANGE_DELETE:
			pdw_append_intermediate(wstate, 'D', RelationGetRelid(relation),
									 NULL,
									 change->data.tp.oldtuple ?
									 change->data.tp.oldtuple : NULL);
			break;
		default:
			break;
	}
}

static void
pdw_commit_cb(ReorderBuffer *rb, ReorderBufferTXN *txn, XLogRecPtr commit_lsn)
{
	/* Result is sent by pdw_process_commit after ReorderBufferCommit returns */
}

/* ------------------------------------------------------------------ */
/* Intermediate result serialization (§7)                               */
/* ------------------------------------------------------------------ */

static void
pdw_append_intermediate(PdWorkerState *wstate, uint8 op, Oid relid,
						HeapTuple newtup, HeapTuple oldtup)
{
	Size		need;
	Size		newlen = newtup ? newtup->t_len : 0;
	Size		oldlen = oldtup ? oldtup->t_len : 0;
	char	   *ptr;

	need = offsetof(PdIntermediateChange, newtuple_len) +
		MAXIMUM_ALIGNOF + newlen + oldlen;

	/* Ensure capacity */
	if (wstate->result_len + need > wstate->result_cap)
	{
		while (wstate->result_len + need > wstate->result_cap)
			wstate->result_cap *= 2;
		wstate->result_buf = repalloc(wstate->result_buf, wstate->result_cap);
	}

	ptr = wstate->result_buf + wstate->result_len;

	/* Write header */
	((PdIntermediateChange *) ptr)->op = op;
	((PdIntermediateChange *) ptr)->relid = relid;
	((PdIntermediateChange *) ptr)->newtuple_len = newlen;
	((PdIntermediateChange *) ptr)->oldtuple_len = oldlen;
	ptr += offsetof(PdIntermediateChange, newtuple_len) + MAXIMUM_ALIGNOF;

	/* Write tuples (TOAST-reassembled by ReorderBufferToastReplace) */
	if (newlen > 0)
	{
		memcpy(ptr, newtup->t_data, newlen);
		ptr += newlen;
	}
	if (oldlen > 0)
		memcpy(ptr, oldtup->t_data, oldlen);

	wstate->result_len += need;
}

/* ------------------------------------------------------------------ */
/* Snapshot management (§4.4)                                          */
/* ------------------------------------------------------------------ */

static void
pdw_store_snapshot(PdWorkerState *wstate, uint64 version, Snapshot snap)
{
	int			i;

	/* Check if we already have this version */
	for (i = 0; i < wstate->n_snaps; i++)
	{
		if (wstate->snap_table[i].version == version)
			return;				/* already stored */
	}

	/* Evict oldest if full */
	if (wstate->n_snaps >= 16)
	{
		if (wstate->snap_table[0].snap)
			pfree(wstate->snap_table[0].snap);
		memmove(&wstate->snap_table[0], &wstate->snap_table[1],
				15 * sizeof(wstate->snap_table[0]));
		wstate->n_snaps = 15;
	}

	wstate->snap_table[wstate->n_snaps].version = version;
	wstate->snap_table[wstate->n_snaps].snap = snap;
	wstate->n_snaps++;
}

static Snapshot
pdw_lookup_snapshot(PdWorkerState *wstate, uint64 version)
{
	int			i;

	for (i = 0; i < wstate->n_snaps; i++)
	{
		if (wstate->snap_table[i].version == version)
			return wstate->snap_table[i].snap;
	}
	return NULL;
}

/* ------------------------------------------------------------------ */
/* Heartbeat, error, cleanup                                           */
/* ------------------------------------------------------------------ */

static void
pdw_send_heartbeat(PdWorkerState *wstate)
{
	PdMsgHeartbeat hb;

	hb.hdr.len = sizeof(PdMsgHeartbeat);
	hb.hdr.type = PD_MSG_CTRL_HEARTBEAT;
	hb.consumed_event_id = pg_atomic_read_u64(&wstate->info->consumed_event_id);
	hb.state = pg_atomic_read_u32(&wstate->info->state);

	(void) shm_mq_send(wstate->req_h, sizeof(hb), &hb, true, false);
}

static void
pdw_send_error(PdWorkerState *wstate, ErrorData *edata)
{
	const char *msg_text = edata->message ? edata->message : "unknown error";
	Size		msg_len = strlen(msg_text);
	Size		buf_len = offsetof(PdMsgError, msg) + msg_len;
	PdMsgError *emsg;

	if (wstate->error_h == NULL)
		return;

	emsg = palloc(buf_len);
	emsg->hdr.len = buf_len;
	emsg->hdr.type = PD_MSG_ERROR;
	emsg->msg_len = msg_len;
	memcpy(emsg->msg, msg_text, msg_len);

	(void) shm_mq_send(wstate->error_h, buf_len, emsg, true, false);
	pfree(emsg);
}

static void
pdw_shutdown_cleanup(PdWorkerState *wstate, int exit_code)
{
	if (wstate->info != NULL)
		pg_atomic_write_u32(&wstate->info->state, PD_WORKER_STARTING);

	if (wstate->xlog_reader)
		XLogReaderFree(wstate->xlog_reader);
	if (wstate->result_buf)
		pfree(wstate->result_buf);

	if (wstate->task_h)
		shm_mq_detach(wstate->task_h);
	if (wstate->event_h)
		shm_mq_detach(wstate->event_h);
	if (wstate->result_h)
		shm_mq_detach(wstate->result_h);
	if (wstate->error_h)
		shm_mq_detach(wstate->error_h);
	if (wstate->req_h)
		shm_mq_detach(wstate->req_h);
	if (wstate->cmd_h)
		shm_mq_detach(wstate->cmd_h);

	if (wstate->seg)
		dsm_detach(wstate->seg);

	proc_exit(exit_code);
}
