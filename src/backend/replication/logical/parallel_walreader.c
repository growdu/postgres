/*-------------------------------------------------------------------------
 *
 * parallel_walreader.c
 *		Publisher-side parallel logical decoding: walreader process.
 *
 * The walreader is the only reader of WAL in a parallel decoding session.
 * It drives the snapshot builder to a consistent state, routes WAL records to
 * the decode workers and allocates the commit sequence numbers that define
 * the order in which the walsender emits commit/abort/cancel.
 *
 * During the P1 milestone the decoding still happens in the walsender, so
 * the walreader reads the WAL and hands the records over the WAL queue; the
 * records are serialized copies of the decoded records, so that the decoding
 * side does not have to read the WAL itself.
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
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/parallel_decode.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the walreader can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

/*
 * The control block of the session and the queue the records are handed over
 * on.  Both are set up before the reading loop starts.
 */
static ParallelDecodeControl *pd_ctl = NULL;
static shm_mq_handle *pd_wal_mqh = NULL;

/* The wait event the walreader reports while it waits for new WAL. */
static uint32 pd_wait_event = PG_WAIT_EXTENSION;

/*
 * SIGTERM handler: ask the walreader to exit at the next opportunity.
 */
static void
parallel_walreader_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
	SetLatch(MyLatch);
}

/*
 * Should the walreader stop reading?
 *
 * The walsender raises the shutdown flag when it goes away in an orderly
 * fashion.  If it was killed by a signal, the flag is never raised, so check
 * that the walsender is still there.
 */
static bool
pd_walreader_stopped(void)
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
 * This is where the walreader waits for new WAL.  It cannot use the
 * walsender's read callback, because that one is tied to the walsender's
 * connection handling.
 */
static int
pd_walreader_read_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
					   int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
	XLogRecPtr	read_upto;
	TimeLineID	tli;
	int			count;
	WALReadError errinfo;

	(void) targetRecPtr;

	for (;;)
	{
		TimeLineID currTLI;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		if (pd_walreader_stopped())
			return -1;

		if (!RecoveryInProgress())
			read_upto = GetFlushRecPtr(&currTLI);
		else
			read_upto = GetXLogReplayRecPtr(&currTLI);

		/*
		 * Check which timeline the record is on.  This has to be done on
		 * every pass through the loop, because the current timeline can
		 * become historical while we wait for the WAL to be flushed.
		 */
		XLogReadDetermineTimeline(state, targetPagePtr, reqLen, currTLI);

		if (state->currTLI == currTLI)
		{
			if (targetPagePtr + reqLen <= read_upto)
			{
				tli = currTLI;
				break;
			}

			/* The WAL is not there yet, so wait for it. */
			rc = WaitLatch(MyLatch,
						   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						   100L,
						   pd_wait_event);
			if (rc & WL_LATCH_SET)
				ResetLatch(MyLatch);

			continue;
		}

		/*
		 * We are on a historical timeline, so we can only read up to the
		 * point where the timeline was switched away from.
		 */
		read_upto = state->currTLIValidUntil;
		tli = state->currTLI;
		break;
	}

	if (targetPagePtr + XLOG_BLCKSZ <= read_upto)
	{
		/* More than one block available; read only that block. */
		count = XLOG_BLCKSZ;
	}
	else if (targetPagePtr + reqLen > read_upto)
	{
		/* Not enough data there. */
		return -1;
	}
	else
	{
		/* Enough bytes available to satisfy the request. */
		count = read_upto - targetPagePtr;
	}

	if (!WALRead(state, readBuf, targetPagePtr, count, tli, &errinfo))
		WALReadRaiseError(&errinfo);

	return count;
}

/*
 * Read the WAL and hand the records over to the decoding side.
 */
static void
pd_walreader_read_loop(XLogRecPtr start_lsn)
{
	XLogReaderState *reader;
	StringInfo	buf;

	reader = XLogReaderAllocate(wal_segment_size, NULL,
								XL_ROUTINE(.page_read = pd_walreader_read_page,
										   .segment_open = wal_segment_open,
										   .segment_close = wal_segment_close),
								NULL);
	if (reader == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Failed while allocating a WAL reading processor.")));

	XLogBeginRead(reader, start_lsn);

	buf = makeStringInfo();

	for (;;)
	{
		char	   *errm = NULL;
		XLogRecord *record;

		CHECK_FOR_INTERRUPTS();

		if (pd_walreader_stopped())
			break;

		/*
		 * Read the next record.  The read blocks in the page read callback
		 * until the WAL has been flushed, so it only returns when there is
		 * something to read or when the session is winding down.
		 */
		record = XLogReadRecord(reader, &errm);

		if (errm != NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("could not find record while reading logically-decoded data: %s",
							errm)));

		if (record == NULL)
		{
			/*
			 * There is no more WAL at the moment.  Wait a little and try
			 * again; the page read callback normally blocks instead of
			 * getting us here.
			 */
			WaitLatch(MyLatch,
					  WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					  100L,
					  pd_wait_event);
			if (got_sigterm)
				break;
			ResetLatch(MyLatch);
			continue;
		}

		/*
		 * Hand the record over.  The record points into the reader's decode
		 * buffer, which the next read reuses, so it has to be copied before
		 * we go on.
		 */
		resetStringInfo(buf);
		PDRecordSerialize(reader->record, buf);

		if (!PDQueueSendChunked(pd_wal_mqh, PD_ENT_WAL, buf->data, buf->len,
								 PARALLEL_DECODE_CHUNK_SIZE, false, true))
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not hand a WAL record to the parallel decoding session")));

		/* Tell the walsender how far we have come. */
		pd_ctl->walreader_end_lsn = (uint64) reader->record->next_lsn;
	}

	XLogReaderFree(reader);
}

/*
 * Main entry point of the walreader.
 */
void
ParallelWalreaderMain(Datum main_arg)
{
	ParallelDecodeWorkerArg arg;
	ParallelDecodeControl *ctl;
	dsm_segment *seg = NULL;
	shm_toc    *toc = NULL;
	shm_mq	   *control_mq;
	shm_mq	   *wal_mq;
	shm_mq_handle *control_mqh;
	int			slot_index = -1;
	int64		ready_payload[2];

	/* Setup signal handling. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	/* Exit cleanly on SIGTERM so that the session shutdown is not
	 * reported as a background worker crash. */
	pqsignal(SIGTERM, parallel_walreader_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(ParallelDecodeWorkerArg));

	ctl = ParallelDecodeWorkerAttach(&arg, &slot_index, &seg, &toc);
	if (ctl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not attach to parallel decoding shared segment")));

	/*
	 * The walreader owns the snapshot builder, which needs access to the
	 * system catalogs of the database the output plugin works on.
	 */
	BackgroundWorkerInitializeConnectionByOid(arg.db_oid, InvalidOid, 0);

	pd_ctl = ctl;
	pd_wait_event = WaitEventExtensionNew("paralleldecodedewalreader");

	ctl->walreader_pid = MyProcPid;
	ctl->walreader_status = PD_WORKER_STATUS_RUNNING;

	/*
	 * Attach to the queue the WAL records are handed over on.  The walsender
	 * is the receiver, the walreader is the sender.
	 */
	wal_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_WALREADER_MQ);
	if (wal_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding WAL channel")));
	shm_mq_set_sender(wal_mq, MyProc);
	pd_wal_mqh = shm_mq_attach(wal_mq, seg, NULL);

	/*
	 * Attach to the control channel and tell the walsender that we are up.
	 * The walsender waits for this message before it starts the replication
	 * session proper, so a walreader that dies during startup is reported
	 * as a failure instead of leaving the session without a WAL reader.
	 */
	control_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_CONTROL_MQ);
	if (control_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding control channel")));
	shm_mq_set_sender(control_mq, MyProc);
	control_mqh = shm_mq_attach(control_mq, seg, NULL);

	ready_payload[0] = (int64) MyProcPid;
	ready_payload[1] = 0;
	if (!PDQueueSend(control_mqh, PD_ENT_READY, ready_payload,
					 sizeof(ready_payload), false, true))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not notify the walsender that the parallel decode "
						"walreader is ready")));

	ereport(LOG,
			(errmsg("parallel decode walreader started")));

	/* Read the WAL and hand the records over to the walsender. */
	pd_walreader_read_loop(arg.start_lsn);

	ctl->walreader_status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode walreader stopped")));

	proc_exit(0);
}
