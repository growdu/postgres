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
 */
static void
pd_worker_write(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				bool last_write)
{
	PDQueueSendFrame(pd_out_mqh, (uint64) pd_end_lsn, pd_current_cseq,
					 ctx->out->data, ctx->out->len);
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

	ereport(LOG,
			(errmsg("parallel decode worker %d started", slot_index)));

	/* Decode the records the walreader hands over. */
	pd_worker_decode_loop();

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
