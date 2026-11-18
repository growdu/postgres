/*-------------------------------------------------------------------------
 *
 * parallel_decode_worker.c
 *		Publisher-side parallel logical decoding: decode worker process.
 *
 * A decode worker decodes the WAL records the walreader hands over into
 * replication protocol bytes and passes the bytes to the walsender over a
 * shared memory queue.
 *
 * The worker has a decoding context of its own: an output plugin instance, a
 * reorder buffer and a snapshot builder.  The walsender owns the replication
 * slot, so the worker works on a copy of the fields of the slot that the
 * decoding path looks at.  Apart from that the decoding path is the same as
 * the one of the serial path, which is what makes the frames the two paths
 * produce identical.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/parallel_decode.h"
#include "replication/slot.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the worker can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

/*
 * The control block of the session, the queues of the worker and the
 * decoding context.
 */
static ParallelDecodeControl *pd_ctl = NULL;
static shm_mq_handle *pd_wal_mqh = NULL;
static shm_mq_handle *pd_out_mqh = NULL;
static PDChunkReceiver pd_wal_rcv;
static LogicalDecodingContext *pd_ctx = NULL;

/*
 * The decoding position after the record that was processed last.  The frames
 * the worker produces carry it, so that the walsender knows how far the
 * decoding has come.
 */
static XLogRecPtr pd_end_lsn = InvalidXLogRecPtr;

/*
 * The index of this worker among the decode workers of the session.
 */
static int	pd_worker_index = -1;

/*
 * The header of the record that is being processed.
 *
 * It tells the worker which worker owns the transaction of the record, and
 * which commit sequence number the record was given when it ends a
 * transaction.
 */
static PDWalHeader pd_wal_header;

/*
 * The commit sequence number of the transaction that is being decoded.
 *
 * The frames the worker produces carry it, so that the walsender can hold
 * them back until the transaction has its turn.  It is zero while a record
 * that does not end a transaction is processed.
 */
static uint64 pd_current_cseq = 0;

/* The position the walsender has been told about. */
static XLogRecPtr pd_reported_lsn = InvalidXLogRecPtr;

/*
 * The number of records between two reports of the decoding position.
 *
 * The reports tell the walsender that the worker is still working: a worker
 * that stops reporting while it still has records to decode is stuck, and the
 * session that waits for it has to end.  The interval is a compromise between
 * the overhead of the reports and the time it takes to notice a stuck worker.
 */
#define PD_POSITION_REPORT_INTERVAL	256

/*
 * The frames of the stream segment that is being built.
 *
 * The subscriber accepts only one stream segment at a time, so the frames of
 * a segment have to reach it without the frames of another transaction in
 * between.  The worker collects them here and hands the whole segment over
 * to the walsender as a single message.
 */
static StringInfo pd_segment = NULL;
static int	pd_segment_frames = 0;
static bool pd_segment_active = false;

/*
 * The streaming callbacks of the output plugin.
 *
 * The worker wraps them, so that it knows which frames belong to a stream
 * segment.
 */
static LogicalDecodeStreamStartCB pd_plugin_stream_start = NULL;
static LogicalDecodeStreamStopCB pd_plugin_stream_stop = NULL;

/* The wait event the worker reports while it waits for the walreader. */
static uint32 pd_wait_event = PG_WAIT_EXTENSION;

/*
 * The result of a receive from the WAL channel.
 */
typedef enum PDRecvResult
{
	PD_RECV_RECORD,				/* a record was received */
	PD_RECV_EMPTY,				/* nothing is available at the moment */
	PD_RECV_STOPPED			/* the session is winding down */
} PDRecvResult;

/*
 * SIGTERM handler: ask the worker to exit at the next opportunity.
 */
static void
parallel_decode_worker_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
	SetLatch(MyLatch);
}

/*
 * Should the worker stop decoding?
 *
 * The walsender raises the shutdown flag when it goes away in an orderly
 * fashion.  If it was killed by a signal, the flag is never raised, so check
 * that the walsender is still there.
 */
static bool
pd_worker_stopped(void)
{
	if (got_sigterm)
		return true;
	if (pd_ctl == NULL)
		return true;
	if (pd_ctl->shutdown)
		return true;

	return !ParallelDecodeProcessAlive(pd_ctl->walsender_pid);
}

/*
 * XLogReaderRoutine->page_read callback.
 *
 * The worker never reads the WAL: the walreader is the only reader of it, and
 * it hands the records over as serialized copies.  The decoding path only
 * needs the reader to hold the record it is working on.
 */
static int
pd_worker_read_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
					int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("the parallel decode worker must not read the WAL")));
	return -1;
}

/*
 * The 'write' callback of the decoding context.
 *
 * The frame is complete at this point, so it is handed over to the walsender,
 * which writes it to the network.
 *
 * The frames of a stream segment are an exception: they are collected, so
 * that the segment reaches the subscriber as a whole.  The subscriber accepts
 * only one stream segment at a time.
 */
static void
pd_worker_write(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				bool last_write)
{
	uint32		len;

	if (pd_segment_active)
	{
		/*
		 * The frames of the segment are stored with their length in front of
		 * them, so that the walsender can find the boundaries of the frames.
		 */
		len = (uint32) ctx->out->len;
		appendBinaryStringInfo(pd_segment, (char *) &len, sizeof(len));
		appendBinaryStringInfo(pd_segment, ctx->out->data, ctx->out->len);
		pd_segment_frames++;

		return;
	}

	PDQueueSendFrame(pd_out_mqh, (uint64) pd_end_lsn, pd_current_cseq,
					 ctx->out->data, ctx->out->len);
}

/*
 * Hand the stream segment over to the walsender.
 *
 * The whole segment goes into a single message, so that the walsender can put
 * its frames on the wire one after the other.
 */
static void
pd_worker_send_segment(void)
{
	StringInfoData msg;

	if (pd_segment_frames == 0)
	{
		resetStringInfo(pd_segment);
		pd_segment_frames = 0;
		return;
	}

	resetStringInfo(&msg);
	PDQueueSendSegment(pd_out_mqh, (uint64) pd_end_lsn, pd_segment_frames,
					   pd_segment->data, pd_segment->len);

	resetStringInfo(pd_segment);
	pd_segment_frames = 0;
}

/*
 * The 'stream_start' callback of the output plugin.
 *
 * The frames the plugin writes from here on belong to the stream segment, so
 * they are collected instead of being sent one by one.
 */
static void
pd_worker_stream_start(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	resetStringInfo(pd_segment);
	pd_segment_frames = 0;
	pd_segment_active = true;

	pd_plugin_stream_start(ctx, txn);
}

/*
 * The 'stream_stop' callback of the output plugin.
 *
 * The segment is complete now, so it is handed over to the walsender.
 */
static void
pd_worker_stream_stop(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	pd_plugin_stream_stop(ctx, txn);

	pd_segment_active = false;
	pd_worker_send_segment();
}

/*
 * The 'update_progress' callback of the decoding context.
 *
 * The walsender tracks the lag of the session, so there is nothing for the
 * worker to do here.
 */
static void
pd_worker_update_progress(LogicalDecodingContext *ctx, XLogRecPtr lsn,
						   TransactionId xid, bool skipped_xact)
{
}

/*
 * Tell the walsender how far the decoding has come.
 *
 * The walsender uses the position for the keepalive frames, for the
 * caught-up decision and for the lag the subscriber sees.  The message is
 * sent when the worker runs out of work, so that the records that produced no
 * output still advance the position.
 */
static void
pd_worker_send_position(void)
{
	if (pd_end_lsn <= pd_reported_lsn)
		return;

	PDQueueSendPosition(pd_out_mqh, (uint64) pd_end_lsn);
	pd_reported_lsn = pd_end_lsn;
}

/*
 * Report an error to the walsender.
 *
 * The walsender has to end the session anyway: the worker cannot continue
 * with the transaction it was working on.  The report tells it what went
 * wrong, so that the session ends with a message that says what happened
 * instead of the generic "the worker died unexpectedly".
 *
 * The text of the error is put into the control block, because the worker is
 * gone by the time the walsender reads the report.
 */
static void
pd_worker_report_error(const char *message)
{
	PDErrorHeader eh;

	if (pd_ctl != NULL && pd_worker_index >= 0 &&
		pd_worker_index < pd_ctl->num_workers)
	{
		/*
		 * The message is written before the status, so that a reader that sees
		 * the status also sees the message.  The barrier makes sure that the
		 * write is not reordered past the status.
		 */
		pd_ctl->workers[pd_worker_index].error_lsn = (uint64) pd_end_lsn;
		strlcpy(pd_ctl->workers[pd_worker_index].errmsg, message,
				PD_MAX_ERROR_MESSAGE);
		pg_memory_barrier();
		pd_ctl->workers[pd_worker_index].status = PD_WORKER_STATUS_FAILED;
	}

	eh.worker = pd_worker_index;
	eh.lsn = (uint64) pd_end_lsn;

	/*
	 * The message is sent with the chunked framing, because the walsender
	 * reads every message of this queue with a single chunk receiver.
	 */
	PDQueueSendChunked(pd_out_mqh, PD_ENT_ERROR, &eh, sizeof(eh),
						PARALLEL_DECODE_CHUNK_SIZE, false, true);
}

/*
 * Get the next WAL record the walreader has read.
 *
 * On PD_RECV_RECORD the record belongs to the caller, which frees it with
 * PDRecordFree().
 */
static PDRecvResult
pd_worker_recv_record(DecodedXLogRecord **record, bool nowait)
{
	for (;;)
	{
		uint16		type;
		const void *payload;
		size_t		len;
		shm_mq_result res;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		if (pd_worker_stopped())
			return PD_RECV_STOPPED;

		/*
		 * Clear the latch before looking at the queue.  A record that arrives
		 * after this point sets the latch again, so the wait below returns
		 * immediately; a record that arrived before is found by the receive.
		 */
		ResetLatch(MyLatch);

		res = PDChunkReceiverRecv(&pd_wal_rcv, pd_wal_mqh, &type, &payload,
								   &len, nowait);
		if (res == SHM_MQ_SUCCESS)
		{
			if (type != PD_ENT_WAL)
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unexpected message of type %u on the parallel decode "
								"WAL channel", type)));

			if (len < sizeof(PDWalHeader))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("parallel decode WAL message is too short (%zu bytes)",
								(size_t) len)));

			/*
			 * The header tells the worker which worker owns the transaction of
			 * the record, and which commit sequence number the record was given
			 * when it ends a transaction.
			 */
			memcpy(&pd_wal_header, payload, sizeof(PDWalHeader));

			*record = PDRecordDeserialize((const char *) payload + sizeof(PDWalHeader),
										  len - sizeof(PDWalHeader));
			pfree((void *) payload);

			return PD_RECV_RECORD;
		}
		if (res == SHM_MQ_DETACHED)
			return PD_RECV_STOPPED;

		/*
		 * There is nothing to do but wait for the walreader.  The walreader is
		 * the only reader of the WAL, so the worker simply has to wait for the
		 * records to arrive.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   100L,
					   pd_wait_event);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);

		/* The caller asked not to wait, so give up after the first try. */
		if (nowait)
			return PD_RECV_EMPTY;
	}
}

/*
 * Decode the records the walreader hands over.
 */
static void
pd_worker_decode_loop(void)
{
	uint32		since_report = 0;

	/*
	 * The fault injection is a debugging facility: it makes the worker fail, so
	 * that the way the session reports and recovers from a failure can be
	 * verified.
	 */
	if (parallel_decode_fault_inject == PD_FAULT_WORKER)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("parallel decode worker %d failed on purpose",
						pd_worker_index)));

	for (;;)
	{
		DecodedXLogRecord *record = NULL;
		PDRecvResult res;

		CHECK_FOR_INTERRUPTS();

		if (pd_worker_stopped())
			break;

		/*
		 * Try to get a record without waiting first.  If there is none, tell
		 * the walsender how far the decoding has come: the records that
		 * produced no output would otherwise never advance the position.
		 */
		res = pd_worker_recv_record(&record, true);
		if (res == PD_RECV_EMPTY)
		{
			pd_worker_send_position();
			res = pd_worker_recv_record(&record, false);
		}
		if (res != PD_RECV_RECORD)
			break;

		/*
		 * Hand the record over to the decoding path as if this process had
		 * read it from the WAL itself.
		 *
		 * The decoding path has to know which worker owns the transaction of
		 * the record: the workers that do not own it decode the record for the
		 * sake of their snapshot builder, but they do not produce its output.
		 */
		pd_ctx->txn_owner = pd_wal_header.owner;
		pd_current_cseq = pd_wal_header.cseq;

		pd_ctx->reader->record = record;
		pd_ctx->reader->ReadRecPtr = record->lsn;
		pd_ctx->reader->EndRecPtr = record->next_lsn;
		pd_end_lsn = record->next_lsn;

		LogicalDecodingProcessRecord(pd_ctx, pd_ctx->reader);

		/*
		 * Tell the walsender now and then that the worker is still working.
		 * The report tells it that the worker is alive and how far it has come,
		 * and it is what the stall detection is based on.
		 */
		if (++since_report >= PD_POSITION_REPORT_INTERVAL)
		{
			since_report = 0;
			pd_worker_send_position();
		}

		/*
		 * The transaction the record ended is decoded now, so the walsender
		 * can let its output pass.  The report has to be sent even when the
		 * transaction produced no output at all, because the walsender waits
		 * for it before it lets the transactions that follow pass.
		 */
		if (pd_current_cseq != 0 && pd_wal_header.owner == pd_worker_index)
			PDQueueSendEvent(pd_out_mqh, pd_current_cseq, PD_EVENT_END_ACK);

		/*
		 * The decoding path copies the data it keeps, so the record can go
		 * away right away.
		 */
		PDRecordFree(record);
		pd_ctx->reader->record = NULL;
		pd_current_cseq = 0;
	}
}

/*
 * Clear the slot of this process before the backend exits.
 *
 * Every backend releases the slot it owns when it exits, but the worker does
 * not own a slot: it works on a copy of the fields of the slot of the
 * walsender.  The callback has to be a before_shmem_exit one, because that is
 * where the release of the slot happens.  The callbacks run in the reverse
 * order of their registration, so this one runs before the release.
 */
static void
pd_worker_clear_slot(int code, Datum arg)
{
	MyReplicationSlot = NULL;
}

/*
 * Build the copy of the replication slot the decoding path works on.
 *
 * The walsender owns the slot, so the worker cannot hold it.  The decoding
 * path only reads a few fields of it, so a copy that carries those fields is
 * enough.
 */
static ReplicationSlot *
pd_worker_build_slot_copy(ParallelDecodeControl *ctl)
{
	ReplicationSlot *slot;

	slot = palloc0(sizeof(ReplicationSlot));

	/*
	 * The copy is not a real slot, but every backend releases the slot it
	 * owns when it exits, so the fields that the release path touches have to
	 * be initialized.  The condition variable in particular: broadcasting on
	 * an uninitialized one never returns.
	 */
	SpinLockInit(&slot->mutex);
	ConditionVariableInit(&slot->active_cv);
	slot->active_pid = MyProcPid;

	namestrcpy(&slot->data.name, ctl->slot_name);
	namestrcpy(&slot->data.plugin, ctl->plugin_name);
	slot->data.database = ctl->database;
	slot->data.two_phase = ctl->two_phase;
	slot->data.two_phase_at = ctl->two_phase_at;
	slot->data.restart_lsn = ctl->start_lsn;
	slot->data.confirmed_flush = ctl->start_lsn;

	return slot;
}

/*
 * Main entry point of a decode worker.
 *
 * The worker attaches to the shared segment created by the walsender, claims
 * its slot in the control block, builds its decoding context and then decodes
 * the records the walreader hands over.
 */
void
ParallelDecodeWorkerMain(Datum main_arg)
{
	ParallelDecodeWorkerArg arg;
	ParallelDecodeControl *ctl;
	dsm_segment *seg = NULL;
	shm_toc    *toc = NULL;
	shm_mq	   *wal_mq;
	shm_mq	   *out_mq;
	ReplicationSlot *slot;
	List	   *options;
	int			slot_index = -1;

	/* Setup signal handling. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	/* Exit cleanly on SIGTERM so that the session shutdown is not
	 * reported as a background worker crash. */
	pqsignal(SIGTERM, parallel_decode_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(ParallelDecodeWorkerArg));

	ctl = ParallelDecodeWorkerAttach(&arg, &slot_index, &seg, &toc);
	if (ctl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not attach to parallel decoding shared segment")));

	if (slot_index < 0 || slot_index >= ctl->num_workers)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid parallel decode worker index %d", slot_index)));

	/*
	 * The output plugin needs access to the system catalogs, so the worker
	 * must have a database connection.
	 */
	BackgroundWorkerInitializeConnectionByOid(arg.db_oid, InvalidOid, 0);

	/* Claim the slot. */
	ctl->workers[slot_index].pid = MyProcPid;
	ctl->workers[slot_index].status = PD_WORKER_STATUS_RUNNING;
	ctl->workers[slot_index].slot_index = slot_index;

	pd_ctl = ctl;
	pd_wait_event = WaitEventExtensionNew("paralleldecodedecodeworker");

	/*
	 * Attach to the queue the WAL records arrive on.  The walreader is the
	 * sender, the worker is the receiver.
	 */
	wal_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_WAL_MQ_BASE + slot_index);
	if (wal_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding WAL channel")));
	shm_mq_set_receiver(wal_mq, MyProc);
	pd_wal_mqh = shm_mq_attach(wal_mq, seg, NULL);
	PDChunkReceiverInit(&pd_wal_rcv);

	/*
	 * Attach to the queue the frames are handed over on.  The worker is the
	 * sender, the walsender is the receiver.
	 */
	out_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_OUT_MQ_BASE + slot_index);
	if (out_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding output channel")));
	shm_mq_set_sender(out_mq, MyProc);
	pd_out_mqh = shm_mq_attach(out_mq, seg, NULL);

	/*
	 * Build the decoding context.  The output plugin options and the name of
	 * the plugin are known by the walsender, which put them into the control
	 * block before it started the worker.
	 */
	options = PDParsePluginOptions(ctl->plugin_options, ctl->plugin_options_len);
	slot = pd_worker_build_slot_copy(ctl);

	pd_ctx = CreateParallelDecodingContext(slot, options, ctl->start_lsn,
											XL_ROUTINE(.page_read = pd_worker_read_page),
											PDPrepareWrite, pd_worker_write,
											pd_worker_update_progress);

	/*
	 * The decoding context published the copy of the slot as the slot of this
	 * process.  Make sure that the exit path does not try to release it.
	 */
	before_shmem_exit(pd_worker_clear_slot, (Datum) 0);
	pd_end_lsn = ctl->start_lsn;
	pd_reported_lsn = ctl->start_lsn;

	/*
	 * Tell the decoding path which worker this is, so that it can tell the
	 * transactions of the other workers apart from its own.
	 */
	pd_worker_index = slot_index;
	pd_ctx->worker_index = slot_index;

	/*
	 * The amount of memory the reorder buffer of this worker may use.  The
	 * walsender decided on it, so that the workers do not use more memory
	 * than the session is allowed to use.
	 */
	if (ctl->work_mem_kb > 0)
		SetConfigOption("logical_decoding_work_mem",
						psprintf("%d", ctl->work_mem_kb),
						PGC_USERSET, PGC_S_SESSION);

	/*
	 * How eagerly the changes of a transaction are streamed.  A session that
	 * does not stream leaves the sizes at zero, which keeps the behavior of
	 * the serial path: the changes are streamed only when the decoding path
	 * runs out of memory.
	 */
	pd_ctx->stream_segment_size = (Size) ctl->stream_segment_size;
	pd_ctx->stream_min_size = (Size) ctl->stream_min_size;

	/*
	 * The frames of a stream segment have to reach the subscriber without the
	 * frames of another transaction in between, so the worker collects them
	 * and hands the whole segment over at once.
	 */
	pd_segment = makeStringInfo();
	if (pd_ctx->callbacks.stream_start_cb != NULL &&
		pd_ctx->callbacks.stream_stop_cb != NULL)
	{
		pd_plugin_stream_start = pd_ctx->callbacks.stream_start_cb;
		pd_plugin_stream_stop = pd_ctx->callbacks.stream_stop_cb;
		pd_ctx->callbacks.stream_start_cb = pd_worker_stream_start;
		pd_ctx->callbacks.stream_stop_cb = pd_worker_stream_stop;
	}

	ereport(LOG,
			(errmsg("parallel decode worker %d started", slot_index)));

	/*
	 * Decode the records the walreader hands over.
	 *
	 * An error that happens while decoding ends the session: the worker cannot
	 * continue with the transaction it was working on, and the state of that
	 * transaction cannot be handed over to another worker without breaking the
	 * affinity of the transactions to their worker.  The error is reported to
	 * the walsender first, so that the session ends with a message that says
	 * what went wrong.
	 */
	PG_TRY();
	{
		pd_worker_decode_loop();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/*
		 * Copy the error data before anything else: the error context is gone
		 * as soon as the error is rethrown.
		 */
		edata = CopyErrorData();

		/*
		 * The report is best effort: if it cannot be sent, the walsender
		 * notices that the worker is gone anyway.
		 */
		PG_TRY(report);
		{
			pd_worker_report_error(edata->message);
		}
		PG_CATCH(report);
		{
			/* The error could not be reported. */
		}
		PG_END_TRY(report);

		FreeErrorData(edata);

		/*
		 * Rethrow the error, so that it is logged and the worker exits with a
		 * code that does not make the postmaster restart the cluster.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();

	ctl->workers[slot_index].status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode worker %d stopped", slot_index)));

	/*
	 * The decoding context published the copy of the slot as the slot of this
	 * process.  Clear it before exiting, so that the exit path does not try to
	 * release a slot that does not exist.
	 */
	MyReplicationSlot = NULL;

	proc_exit(0);
}
