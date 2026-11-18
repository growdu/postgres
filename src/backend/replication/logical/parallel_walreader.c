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
 * The walreader reads the WAL and hands the records over to the decode
 * workers; the records are serialized copies of the decoded records, so that
 * the decoding side does not have to read the WAL itself.
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
#include "access/xact.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/parallel_decode.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the walreader can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

/*
 * The control block of the session and the queues the records are handed over
 * on.  Both are set up before the reading loop starts.
 *
 * There is one queue per decode worker.  The walreader is the only reader of
 * the WAL, so every worker has to see the whole stream of records: the
 * snapshot builder of a worker needs the same records as the one of the
 * others.
 */
static ParallelDecodeControl *pd_ctl = NULL;
static shm_mq_handle *pd_wal_mqh[MAX_PARALLEL_DECODE_WORKERS];
static int	pd_num_workers = 0;

/*
 * The control channel the walreader reports on.
 *
 * The walreader is the only reader of the WAL, so it is the only one that
 * knows where the records that end the transactions are.  The walsender needs
 * that information to compute the release frontier.
 */
static shm_mq_handle *pd_control_mqh = NULL;

/* The wait event the walreader reports while it waits for new WAL. */
static uint32 pd_wait_event = PG_WAIT_EXTENSION;

/*
 * The information the walreader keeps about the transactions it has seen.
 *
 * The table is keyed by the transaction id, and it holds an entry for the top
 * level transaction as well as for every one of its subtransactions: the
 * records of a subtransaction carry the id of the subtransaction, so the
 * walreader has to know which top level transaction they belong to.
 */
typedef struct PDTxnInfo
{
	TransactionId toplevel;		/* the top level transaction */
	int			worker;			/* the worker that owns the transaction */
	int64		bytes;			/* the bytes routed to the worker so far */
	List	   *members;		/* the ids of the subtransactions */
	struct PDTxnInfo *top;		/* the entry of the top level transaction */
} PDTxnInfo;

typedef struct PDTxnEntry
{
	TransactionId xid;			/* the key of the entry */
	PDTxnInfo	info;
} PDTxnEntry;

static HTAB *pd_txns = NULL;

/*
 * The number of bytes that are in flight per worker.  The walreader uses it
 * to decide which worker a new transaction is assigned to.
 */
static int64 pd_worker_bytes[MAX_PARALLEL_DECODE_WORKERS] = {0};

/*
 * The commit sequence number of the next transaction that ends.
 *
 * The walreader is the only reader of the WAL, so the order in which it sees
 * the transactions end is the order in which the transactions have to be
 * applied.  The number it assigns to every transaction that ends is what the
 * walsender uses to restore that order.
 */
static uint64 pd_next_cseq = 1;

/*
 * The WAL position the walreader has read up to.
 *
 * It is reported with an error, so that the walsender can tell where the
 * failure happened.
 */
static XLogRecPtr pd_read_lsn = InvalidXLogRecPtr;

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
 * Look up a transaction.
 */
static PDTxnInfo *
pd_walreader_find_txn(TransactionId xid)
{
	PDTxnEntry *entry;

	entry = (PDTxnEntry *) hash_search(pd_txns, &xid, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	return &entry->info;
}

/*
 * Create the entry of a transaction.
 */
static PDTxnInfo *
pd_walreader_enter_txn(TransactionId xid)
{
	PDTxnEntry *entry;
	bool		found;

	entry = (PDTxnEntry *) hash_search(pd_txns, &xid, HASH_ENTER, &found);
	Assert(!found);
	memset(&entry->info, 0, sizeof(PDTxnInfo));

	return &entry->info;
}

/*
 * Pick the worker a new transaction is assigned to.
 *
 * The transaction goes to the worker that has the fewest bytes in flight, so
 * that a large transaction does not end up on the same worker as the other
 * ones.
 */
static int
pd_walreader_pick_worker(void)
{
	int			best = 0;
	int			i;

	for (i = 1; i < pd_num_workers; i++)
	{
		if (pd_worker_bytes[i] < pd_worker_bytes[best])
			best = i;
	}

	return best;
}

/*
 * The transaction id a record belongs to.
 *
 * Most records carry the id of the transaction they belong to.  The records
 * that end a prepared transaction do not: they are written by a backend that
 * is not in a transaction, and the id of the prepared transaction is in their
 * data.
 */
static TransactionId
pd_walreader_record_xid(XLogReaderState *reader)
{
	uint8		info;

	if (XLogRecGetRmid(reader) != RM_XACT_ID)
		return XLogRecGetXid(reader);

	info = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

	switch (info)
	{
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec;
				xl_xact_parsed_commit parsed;

				xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				return parsed.twophase_xid;
			}
		case XLOG_XACT_ABORT_PREPARED:
			{
				xl_xact_abort *xlrec;
				xl_xact_parsed_abort parsed;

				xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				return parsed.twophase_xid;
			}
		default:
			return XLogRecGetXid(reader);
	}
}

/*
 * The top level transaction a record belongs to.
 *
 * The first record of a subtransaction carries the id of the top level
 * transaction it belongs to.  The later records of the subtransaction do not,
 * so the walreader has to remember the relation, which is what the table of
 * the transactions is for.  A record that belongs to a top level transaction
 * itself carries no such information: its own transaction id is the top level
 * one.
 *
 * This mirrors what the decoding path does, so that the walreader and the
 * decode workers agree on which records belong to which transaction.
 */
static TransactionId
pd_walreader_toplevel_of(XLogReaderState *reader, TransactionId xid)
{
	if (TransactionIdIsValid(XLogRecGetTopXid(reader)))
		return XLogRecGetTopXid(reader);

	return xid;
}

/*
 * Find the transaction a record belongs to.
 *
 * The entry of the top level transaction is returned, because that is the
 * transaction the worker that owns it works on.  The entry is created when
 * the walreader sees the transaction for the first time.
 */
static PDTxnInfo *
pd_walreader_record_txn(XLogReaderState *reader)
{
	TransactionId xid;
	TransactionId toplevel;
	PDTxnInfo *info;

	xid = pd_walreader_record_xid(reader);
	if (!TransactionIdIsValid(xid))
		return NULL;

	info = pd_walreader_find_txn(xid);
	if (info != NULL)
		return info->top;

	toplevel = pd_walreader_toplevel_of(reader, xid);

	if (TransactionIdIsValid(toplevel) && toplevel != xid)
	{
		PDTxnInfo *top;

		/*
		 * The record belongs to a subtransaction, so the entry of the top
		 * level transaction decides which worker owns it.
		 */
		top = pd_walreader_find_txn(toplevel);
		if (top == NULL)
		{
			top = pd_walreader_enter_txn(toplevel);
			top->toplevel = toplevel;
			top->worker = pd_walreader_pick_worker();
			top->top = top;
		}

		info = pd_walreader_enter_txn(xid);
		info->toplevel = toplevel;
		info->worker = top->worker;
		info->top = top;
		top->members = lappend_int(top->members, xid);
	}
	else
	{
		info = pd_walreader_enter_txn(xid);
		info->toplevel = xid;
		info->worker = pd_walreader_pick_worker();
		info->top = info;
	}

	return info->top;
}

/*
 * Forget a transaction that has ended.
 *
 * The subtransactions of the transaction are gone as well, so their entries
 * are removed from the table as well.
 */
static void
pd_walreader_forget_txn(PDTxnInfo *info)
{
	ListCell   *lc;

	if (info == NULL)
		return;

	foreach(lc, info->members)
	{
		TransactionId subxid = lfirst_int(lc);

		hash_search(pd_txns, &subxid, HASH_REMOVE, NULL);
	}
	list_free(info->members);
	info->members = NIL;

	/*
	 * The bytes of the transaction are no longer in flight, so the worker
	 * that owned it is up for the next transaction.
	 */
	pd_worker_bytes[info->worker] -= info->bytes;

	hash_search(pd_txns, &info->toplevel, HASH_REMOVE, NULL);
}

/*
 * Does this record end a top level transaction?
 *
 * The commit sequence numbers are assigned to the records that end a top
 * level transaction, because the order in which the transactions end is the
 * order in which they have to be applied.
 *
 * The abort of a subtransaction does not end the top level transaction: the
 * subtransaction is only a part of it, and the top level transaction goes on.
 * The record of such an abort carries the id of the subtransaction, so it is
 * recognized by the id it carries.
 */
static bool
pd_walreader_record_ends_xact(XLogReaderState *reader, PDTxnInfo *info)
{
	TransactionId xid;
	uint8		info_code;

	if (XLogRecGetRmid(reader) != RM_XACT_ID)
		return false;

	info_code = XLogRecGetInfo(reader) & XLOG_XACT_OPMASK;

	switch (info_code)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_ABORT:
		case XLOG_XACT_PREPARE:
		case XLOG_XACT_COMMIT_PREPARED:
		case XLOG_XACT_ABORT_PREPARED:
			break;
		default:
			return false;
	}

	/*
	 * The record has to end the top level transaction it belongs to.
	 */
	if (info == NULL)
		return false;

	xid = pd_walreader_record_xid(reader);
	if (!TransactionIdIsValid(xid) || xid != info->toplevel)
		return false;

	return true;
}

/*
 * Does this record leave the transaction in the prepared state?
 *
 * A prepared transaction is not over yet: the record that ends it comes later,
 * and it has to be decoded by the same worker, so the transaction must not be
 * forgotten yet.
 */
static bool
pd_walreader_record_prepares_xact(XLogReaderState *reader)
{
	if (XLogRecGetRmid(reader) != RM_XACT_ID)
		return false;

	return (XLogRecGetInfo(reader) & XLOG_XACT_OPMASK) == XLOG_XACT_PREPARE;
}

/*
 * The transaction a record belongs to, and the commit sequence number it was
 * given.
 */
typedef struct PDRecordInfo
{
	PDTxnInfo   *txn;		/* the top level transaction, NULL if there is none */
	int			worker;		/* the worker that owns the transaction */
	uint64		cseq;		/* the commit sequence number, zero if the record
							 * does not end a transaction */
	bool		ended;		/* the transaction is over, its entry is gone */
} PDRecordInfo;

/*
 * Find the transaction a record belongs to, and give the record the next
 * commit sequence number if it ends a transaction.
 */
static PDRecordInfo
pd_walreader_classify(XLogReaderState *reader)
{
	PDRecordInfo result;

	result.txn = NULL;
	result.worker = -1;
	result.cseq = 0;
	result.ended = false;

	if (!TransactionIdIsValid(pd_walreader_record_xid(reader)))
		return result;

	result.txn = pd_walreader_record_txn(reader);

	/*
	 * A record that ends a top level transaction gets the next commit sequence
	 * number.  The number is what the walsender uses to restore the order of
	 * the transactions, no matter which worker decoded them.
	 */
	if (!pd_walreader_record_ends_xact(reader, result.txn))
	{
		if (result.txn != NULL)
			result.worker = result.txn->worker;
		return result;
	}

	result.cseq = pd_next_cseq++;

	/*
	 * Remember the owner of the transaction before the entry is removed: the
	 * entry is freed when the transaction ends, so it may not be looked at
	 * afterwards.
	 */
	result.worker = result.txn->worker;

	/*
	 * The transaction is over, unless it is only prepared.
	 */
	if (!pd_walreader_record_prepares_xact(reader))
	{
		pd_walreader_forget_txn(result.txn);
		result.ended = true;
	}

	return result;
}

/*
 * Tell the walsender where the record that ended a transaction is.
 *
 * The walsender needs the position of that record to compute the release
 * frontier: the frames it writes may not carry a position beyond it, or the
 * subscriber would advance the flush position of the slot beyond a
 * transaction that has not been delivered yet.
 *
 * The message is sent before the record itself is handed over to the decode
 * workers, so the walsender always knows the position of a transaction before
 * it sees the report that the transaction has been decoded.
 */
static void
pd_walreader_send_cseq(uint64 cseq, XLogRecPtr end_lsn, XLogRecPtr read_lsn,
					   int worker)
{
	PDCseqInfo	info;

	info.cseq = cseq;

	/*
	 * The frontier is built from the start of the record that ended the
	 * transaction, because that is the position the skip test of the decoding
	 * path compares with the flush position of the slot.
	 */
	info.end_lsn = (uint64) end_lsn;

	/*
	 * The position the walreader has read up to.  It is the position of the
	 * record itself, so it covers every transaction that ended before it.
	 */
	info.read_lsn = (uint64) read_lsn;

	/*
	 * The worker that decoded the transaction.  The walsender uses it to find
	 * out which worker it has to wait for when the transaction is next in line.
	 */
	info.owner = worker;

	if (!PDQueueSend(pd_control_mqh, PD_ENT_CSEQ, &info, sizeof(info),
					 false, true))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand a commit sequence number to the parallel "
						"decoding session")));
}

/*
 * Report an error to the walsender.
 *
 * The walsender has to end the session anyway: the walreader is the only
 * reader of the WAL, so without it no new record can be decoded.  The report
 * tells it what went wrong, so that the session ends with a message that says
 * what happened instead of the generic "the walreader died unexpectedly".
 *
 * The text of the error is put into the control block, because the walreader
 * is gone by the time the walsender reads the report.
 */
static void
pd_walreader_report_error(const char *message)
{
	PDErrorHeader eh;

	if (pd_ctl != NULL)
	{
		/*
		 * The message is written before the status, so that a reader that sees
		 * the status also sees the message.  The barrier makes sure that the
		 * write is not reordered past the status.
		 */
		pd_ctl->walreader_error_lsn = (uint64) pd_read_lsn;
		strlcpy(pd_ctl->walreader_errmsg, message, PD_MAX_ERROR_MESSAGE);
		pg_memory_barrier();
		pd_ctl->walreader_status = PD_WORKER_STATUS_FAILED;
	}

	eh.worker = -1;
	eh.lsn = (uint64) pd_read_lsn;

	/*
	 * The control channel is not chunked: every message of it is a plain
	 * message with a fixed size header.
	 */
	if (!PDQueueSend(pd_control_mqh, PD_ENT_ERROR, &eh, sizeof(eh),
					 false, true))
		return;
}

/*
 * Read the WAL and hand the records over to the decoding side.
 */
static void
pd_walreader_read_loop(XLogRecPtr start_lsn)
{
	XLogReaderState *reader;
	StringInfo	buf;
	PDWalHeader	wal_header;
	PDRecordInfo	recdesc;
	PDTxnInfo   *info;
	uint64		cseq;
	int			worker;
	int			i;

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

	/*
	 * The fault injection is a debugging facility: it makes the walreader fail,
	 * so that the way the session reports and recovers from a failure can be
	 * verified.
	 */
	if (parallel_decode_fault_inject == PD_FAULT_WALREADER)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("parallel decode walreader failed on purpose")));

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
		 * Find out which transaction the record belongs to.  The walreader is
		 * the only reader of the WAL, so it is the one that decides which
		 * worker owns the transaction.  A record that ends a top level
		 * transaction also gets the commit sequence number that defines the
		 * order in which the transactions have to be applied.
		 */
		recdesc = pd_walreader_classify(reader);
		info = recdesc.txn;
		worker = recdesc.worker;
		cseq = recdesc.cseq;

		/*
		 * The record that ends a transaction gets a commit sequence number.
		 * Tell the walsender where the record is, so that it can limit the
		 * positions it reports to the release frontier.
		 */
		if (cseq != 0)
			pd_walreader_send_cseq(cseq, reader->record->lsn,
								   reader->record->next_lsn, worker);

		/*
		 * Hand the record over.  The record points into the reader's decode
		 * buffer, which the next read reuses, so it has to be copied before
		 * we go on.
		 *
		 * Every worker sees every record: the snapshot builders of the
		 * workers have to stay in step with each other, or the snapshots
		 * they build would differ.  The header tells the workers who owns
		 * the transaction of the record; the ones that do not own it decode
		 * the record for the sake of their snapshot builder, but they do not
		 * accumulate its changes.
		 */
		resetStringInfo(buf);
		wal_header.owner = worker;
		wal_header.cseq = cseq;
		appendBinaryStringInfo(buf, (const char *) &wal_header,
								sizeof(wal_header));
		PDRecordSerialize(reader->record, buf);

		for (i = 0; i < pd_num_workers; i++)
		{
			if (!PDQueueSendChunked(pd_wal_mqh[i], PD_ENT_WAL, buf->data,
									 buf->len, PARALLEL_DECODE_CHUNK_SIZE, false,
									 true))
				ereport(ERROR,
						(errcode(ERRCODE_IO_ERROR),
						 errmsg("could not hand a WAL record to the parallel decoding session")));
		}

		/*
		 * Account for the bytes the transaction has produced, so that the
		 * next transaction goes to the worker that has the least work.
		 *
		 * The transaction of a record that ends it is over, and its entry is
		 * gone: the bytes of such a record do not have to be accounted for,
		 * because the in-flight bytes of the transaction have been released
		 * already.
		 */
		if (info != NULL && !recdesc.ended)
		{
			info->bytes += buf->len;
			pd_worker_bytes[worker] += buf->len;
		}

		/* Tell the walsender how far we have come. */
		pd_ctl->walreader_end_lsn = (uint64) reader->record->next_lsn;
		pd_read_lsn = reader->record->next_lsn;
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
	int			i;
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

	/*
	 * Create the table of the transactions the walreader has seen.  The
	 * walreader is the only reader of the WAL, so it is the one that decides
	 * which worker owns which transaction.
	 */
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(TransactionId);
		hash_ctl.entrysize = sizeof(PDTxnEntry);
		hash_ctl.hcxt = CurrentMemoryContext;
		pd_txns = hash_create("Parallel decode transactions",
							   128, &hash_ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	ctl->walreader_pid = MyProcPid;
	ctl->walreader_status = PD_WORKER_STATUS_RUNNING;

	/*
	 * Attach to the queues the WAL records are handed over on.  The decode
	 * workers are the receivers, the walreader is the sender.
	 */
	pd_num_workers = ctl->num_workers;
	for (i = 0; i < pd_num_workers; i++)
	{
		wal_mq = ParallelDecodeLookupMq(toc,
										PARALLEL_DECODE_KEY_WAL_MQ_BASE + i);
		if (wal_mq == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("could not find the parallel decoding WAL channel")));
		shm_mq_set_sender(wal_mq, MyProc);
		pd_wal_mqh[i] = shm_mq_attach(wal_mq, seg, NULL);
	}

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
	pd_control_mqh = control_mqh;

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

	/*
	 * Read the WAL and hand the records over to the walsender.
	 *
	 * An error that happens here ends the session: the walreader is the only
	 * reader of the WAL, and the state it holds cannot be handed over to
	 * another process.  The error is reported to the walsender first, so that
	 * the session ends with a message that says what went wrong.
	 */
	PG_TRY();
	{
		pd_walreader_read_loop(arg.start_lsn);
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
		 * notices that the walreader is gone anyway.
		 */
		PG_TRY(report);
		{
			pd_walreader_report_error(edata->message);
		}
		PG_CATCH(report);
		{
			/* The error could not be reported. */
		}
		PG_END_TRY(report);

		FreeErrorData(edata);

		/*
		 * Rethrow the error, so that it is logged and the walreader exits with
		 * a code that does not make the postmaster restart the cluster.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();

	ctl->walreader_status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode walreader stopped")));

	proc_exit(0);
}
