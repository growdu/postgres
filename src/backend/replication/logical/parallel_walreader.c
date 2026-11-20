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

#include "access/heapam_xlog.h"
#include "access/xlog.h"
#include "access/xlogreader.h"
#include "access/xlogrecovery.h"
#include "access/xlogutils.h"
#include "access/xact.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/logical.h"
#include "replication/message.h"
#include "replication/parallel_decode.h"
#include "replication/reorderbuffer.h"
#include "replication/slot.h"
#include "replication/snapbuild.h"
#include "replication/walsender_private.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/sinval.h"
#include "storage/standbydefs.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
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
static PDQueueSender pd_wal_sender[MAX_PARALLEL_DECODE_WORKERS];
static int	pd_num_workers = 0;

/*
 * The number of bytes that were written to the queues of the decode workers
 * since the last flush.
 *
 * The counter is shared by all the queues of the session: a worker that owns
 * no transaction receives only the markers, and a counter per queue would let
 * those markers pile up for a long time before the worker learns how far the
 * walreader has come.
 */
static size_t pd_wal_pending = 0;

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

/* Publish the records that were written to the queues of the workers. */
static void pd_walreader_flush_queues(void);

/* Tell the workers and the walsender how far the walreader has read. */
static void pd_walreader_publish_position(XLogRecPtr lsn);

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
 * The number of transactions that are in flight per worker.  It is part of
 * the statistics of the monitoring views (11.3).
 */
static int64 pd_worker_txns[MAX_PARALLEL_DECODE_WORKERS] = {0};

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
 * The invalidation epoch of the session.
 *
 * The epoch grows every time the catalog the decoding depends on changes,
 * that is, every time a new snapshot is published or a set of invalidation
 * messages is broadcast.  The workers use it to tell whether the definition
 * of a relation that has been sent already still describes the relation: a
 * definition that was sent before the last change of the catalog does not
 * describe the relation anymore.
 *
 * The epoch is the same for the whole session: the walreader is the only
 * process that drives the snapshot builder, so it is the only one that knows
 * when the catalog changed.
 */
static uint64 pd_epoch = 0;

/*
 * The tracking reorder buffer and the snapshot builder of the walreader.
 *
 * The walreader is the only reader of the WAL, so it is the only process that
 * can drive the snapshot builder: it decides which snapshot a transaction is
 * decoded with, and when the transactions that are in flight have to switch
 * over to a new one.  The decode workers do not have a builder of their own,
 * they use a read-only stand-in that is told about the state of the builder
 * of the walreader.
 *
 * The reorder buffer does not hold any changes: it mirrors the state the
 * snapshot builder works on, so that the builder can tell which transactions
 * are in flight, which of them have a base snapshot and which of them changed
 * the catalogs.
 */
static ReorderBuffer *pd_rb = NULL;
static SnapBuild *pd_builder = NULL;

/*
 * The decoding context of the shadow decoding.
 *
 * The walreader does not produce any output, so it has no output plugin.  The
 * context exists because the snapshot builder reaches for it through the
 * reorder buffer: it uses it to find out which transactions it owns.  The
 * walreader owns all of them.
 */
static LogicalDecodingContext *pd_shadow_ctx = NULL;

/*
 * The copy of the replication slot the walreader works on.
 *
 * The walsender owns the slot, so the walreader cannot hold it.  The
 * decoding path only reads a few fields of it, so a copy that carries those
 * fields is enough.
 */
static ReplicationSlot *pd_slot_copy = NULL;

/*
 * The version of the snapshot that was published last.
 *
 * The version is what the workers refer to a snapshot by: the walreader hands
 * the content of a snapshot down together with its version, and the workers
 * refer to the snapshot by the version when they are told which snapshot a
 * transaction is decoded with.
 */
static uint64 pd_snapshot_version = 0;

/*
 * The snapshot that was published last, NULL if there is none.
 *
 * The walreader compares it with the snapshot the builder holds to find out
 * whether a new snapshot has been built, which is when the content of the
 * snapshot has to be handed down.
 */
static Snapshot pd_published_snapshot = NULL;

/*
 * The state of the builder and the start of the decoding window that were
 * published last.
 *
 * They are published whenever they change, so that the workers skip exactly
 * the transactions the serial path skips.
 */
static int pd_published_state = -1;
static XLogRecPtr pd_published_start_decoding_at = InvalidXLogRecPtr;
static TransactionId pd_published_next_phase_at = InvalidTransactionId;

/*
 * The result of the shadow decoding of a record: which worker the record is
 * forwarded to.
 *
 * PD_FORWARD_NONE means that the record is not forwarded at all, PD_FORWARD_ALL
 * that it goes to every worker, and PD_FORWARD_OWNER that it goes to the
 * worker that owns the transaction of the record.  Every other value is the
 * index of the worker the record belongs to.
 */
#define PD_FORWARD_NONE		(-1)
#define PD_FORWARD_ALL		(-2)
#define PD_FORWARD_OWNER		(-3)

/*
 * The number of records between two markers.
 *
 * A marker tells the decode workers how far the walreader has read.  The
 * records of a transaction are only handed to the worker that owns it, so a
 * worker that owns no transaction does not see any records at all.  Without
 * the marker such a worker could not tell that it has nothing left to do.
 */
#define PD_POSITION_MARK_RECORDS	64

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
	{
		return true;
	}
	if (pd_ctl == NULL)
	{
		return true;
	}
	if (pd_ctl->shutdown)
	{
		return true;
	}

	if (!ParallelDecodeProcessAlive(pd_ctl->walsender_pid))
	{
		return true;
	}

	return false;
}

/*
 * Has the session been asked to wind down?
 *
 * This is the cheap half of pd_walreader_stopped(): it does not ask the
 * operating system whether the walsender is still there, so it can be called
 * on the path of every record.  It is used to keep the walreader from handing
 * over a record that nobody is going to read: the send blocks when the queue
 * of the worker is full, and a worker that is winding down does not read any
 * more.
 */
static bool
pd_walreader_shutdown_requested(void)
{
	if (got_sigterm)
		return true;
	if (pd_ctl == NULL)
		return true;

	return pd_ctl->shutdown != 0;
}

/*
 * Detach from the queues of the walreader.
 *
 * The walreader is the sender on the channels the workers receive the records
 * on, and on the control channel the walsender reads.  Detaching from them
 * tells the processes on the other end that nothing more is going to come: a
 * worker does not wait for a record that is never going to arrive, and the
 * walsender does not wait for a message that is never going to be sent.
 */
static void
pd_walreader_detach_queues(void)
{
	int			i;

	if (pd_control_mqh != NULL)
	{
		shm_mq_detach(pd_control_mqh);
		pd_control_mqh = NULL;
	}

	for (i = 0; i < pd_num_workers; i++)
	{
		if (pd_wal_sender[i].mqh != NULL)
		{
			shm_mq_detach(pd_wal_sender[i].mqh);
			pd_wal_sender[i].mqh = NULL;
		}
	}
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

			/*
			 * The WAL is not there yet, so wait for it.  The records that
			 * were read so far are published first: the workers cannot
			 * decode a record that they cannot see.
			 *
			 * The position the walreader has read up to is published as
			 * well.  The walsender may only report a position that is
			 * covered by the transactions that have been released, and with
			 * no WAL to decode there is nothing that would move that
			 * position forward: the session would stay where it started,
			 * and the subscriber would never learn that the session has
			 * made progress.  That matters for the table synchronization
			 * of the subscriber, which only ends once the position of the
			 * session has passed the point the table was synchronized at.
			 *
			 * This is the parallel equivalent of the keepalive the serial
			 * path sends while it waits for the WAL.
			 */
			pd_walreader_publish_position(pd_read_lsn);

			pd_walreader_flush_queues();

			/*
			 * Sleep until the WAL has been flushed.
			 *
			 * The walreader sleeps on the same condition variable that the
			 * logical walsenders sleep on.  The WAL flush broadcasts it (see
			 * WalSndWakeup()), so the walreader learns about the new WAL as
			 * soon as it has been flushed, the same way the serial path
			 * learns about it.  Nothing else sets the walreader's latch, so
			 * without this the new WAL would only be found by the timeout
			 * below, and the session would advance in waves of the timeout
			 * instead of continuously.
			 *
			 * The timeout is the fallback: it bounds the delay if a
			 * broadcast is missed.
			 */
			if (WalSndCtl != NULL)
			{
				/*
				 * Prepare to sleep before the last check of the flush
				 * position, so that a flush that happens right now cannot be
				 * missed.
				 */
				ConditionVariablePrepareToSleep(&WalSndCtl->wal_replay_cv);

				/*
				 * The same position the loop above uses: the flush position
				 * on a primary, the replay position on a standby.
				 */
				if (RecoveryInProgress())
					read_upto = GetXLogReplayRecPtr(&currTLI);
				else
					read_upto = GetFlushRecPtr(&currTLI);
				if (targetPagePtr + reqLen <= read_upto)
				{
					ConditionVariableCancelSleep();
					continue;
				}

				(void) ConditionVariableTimedSleep(&WalSndCtl->wal_replay_cv,
													100L, pd_wait_event);

				/*
				 * ConditionVariableTimedSleep() leaves us on the wait list
				 * of the condition variable, whether we were signaled or
				 * the timeout expired.  We have to take ourselves off it
				 * before we do anything else: the next check of the flush
				 * position, and the exit from the wait altogether, must
				 * not be done while we are still registered as a sleeper.
				 */
				ConditionVariableCancelSleep();
			}
			else
			{
				rc = WaitLatch(MyLatch,
							   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
							   100L,
							   pd_wait_event);
				if (rc & WL_LATCH_SET)
					ResetLatch(MyLatch);
			}

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
 * Publish the counters of a worker for the monitoring views (11.3).
 */
static void
pd_walreader_publish_worker_counters(int worker)
{
	if (pd_ctl == NULL || worker < 0 || worker >= pd_num_workers)
		return;

	pd_ctl->workers[worker].in_flight_bytes =
		(uint64) Max(pd_worker_bytes[worker], 0);
	pd_ctl->workers[worker].in_flight_txns =
		(uint64) Max(pd_worker_txns[worker], 0);
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
		case XLOG_XACT_PREPARE:
			{
				xl_xact_parsed_prepare parsed;
				xl_xact_prepare *xlrec;

				xlrec = (xl_xact_prepare *) XLogRecGetData(reader);
				ParsePrepareRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				/*
				 * The record belongs to the prepared transaction, not to the
				 * backend that prepared it: the changes of the prepared
				 * transaction are decoded by the worker that owns it.
				 */
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
			pd_worker_txns[top->worker]++;
			pd_walreader_publish_worker_counters(top->worker);
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
		pd_worker_txns[info->worker]++;
		pd_walreader_publish_worker_counters(info->worker);
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
	pd_worker_txns[info->worker]--;

	pd_walreader_publish_worker_counters(info->worker);

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

	/*
	 * The session is winding down, so the walsender is not reading the control
	 * channel any more.  The message is not sent: the send blocks when the
	 * channel is full, and the walsender would not read it anyway.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	if (!PDQueueSend(pd_control_mqh, PD_ENT_CSEQ, &info, sizeof(info),
					 false, true))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand a commit sequence number to the parallel "
						"decoding session")));
}

/*
 * Tell the walsender how far the walreader has read.
 *
 * The walsender needs a position it may report to the subscriber: the
 * position of the subscriber may not pass a transaction that has not been
 * delivered yet.  When every transaction the walreader has seen ending has
 * been released, the position the walreader has read up to is the release
 * frontier.
 *
 * The message goes over the control channel, which keeps the order of the
 * messages: every transaction that ends before the position has been reported
 * before the message is sent, so the walsender knows about all of them when
 * it reads the message.  That is what makes the position a certified one, and
 * not a guess.
 */
static void
pd_walreader_send_read_pos(XLogRecPtr lsn)
{
	static XLogRecPtr last_sent = InvalidXLogRecPtr;
	uint64		pos = (uint64) lsn;

	/*
	 * The walsender keeps the last position it was told about, so there is
	 * nothing to gain from sending the same position twice.  The walreader
	 * repeats the report whenever it runs out of WAL, so this keeps the
	 * control channel quiet while the publisher is idle.
	 */
	if (last_sent == lsn)
		return;

	/*
	 * The session is winding down, so the walsender is not reading the control
	 * channel any more.  The message is not sent: the send blocks when the
	 * channel is full, and the walsender would not read it anyway.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	if (!PDQueueSend(pd_control_mqh, PD_ENT_READ_POS, &pos, sizeof(pos),
					 false, true))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand the read position to the parallel "
						"decoding session")));

	last_sent = lsn;
}

/*
 * Hand the watermarks of the slot over to the walsender.
 *
 * The slot of the session has two watermarks that depend on the transactions
 * that are in flight: the xmin of the catalog and the position the decoding
 * may restart from.  The walreader does not own the slot, so it cannot
 * advance them, but it is the only process that sees every transaction of the
 * session, so it is the one that computes them.
 *
 * The computation is the one the serial path does when it sees a running
 * xacts record, and the values are the same: the walreader holds every
 * transaction of the session in its tracking buffer, so the oldest
 * transaction it holds is the oldest transaction of the session.
 *
 * The walsender advances the watermarks of the slot once the subscriber has
 * confirmed the position the values are valid at.
 */
static void
pd_walreader_send_watermark(XLogRecPtr lsn, TransactionId oldest_running)
{
	PDWatermark	wm;
	ReorderBufferTXN *txn;

	/*
	 * The xmin of the catalog is the xmin of the snapshot of the oldest
	 * transaction that is still in flight.  When no transaction holds a base
	 * snapshot, the oldest running transaction is the best guess.
	 */
	wm.xmin = ReorderBufferGetOldestXmin(pd_rb);
	if (wm.xmin == InvalidTransactionId)
		wm.xmin = oldest_running;

	wm.oldest_running = oldest_running;

	/*
	 * The position the values are valid at.  The watermarks of the slot may
	 * only be advanced once the subscriber has confirmed this position.
	 */
	wm.read_lsn = (uint64) lsn;

	/*
	 * The position the decoding may restart from is the restart point of the
	 * oldest transaction that is still in flight: the decoding has to start
	 * there, so that the changes of that transaction are not lost.
	 *
	 * It is only known when the builder is consistent: before that the
	 * snapshot the decoding works on is not stable yet.
	 */
	wm.restart_lsn = 0;

	if (SnapBuildCurrentState(pd_builder) >= SNAPBUILD_CONSISTENT)
	{
		txn = ReorderBufferGetOldestTXN(pd_rb);

		if (txn != NULL && txn->restart_decoding_lsn != InvalidXLogRecPtr)
			wm.restart_lsn = (uint64) txn->restart_decoding_lsn;

		/*
		 * No transaction is in flight, so the decoding may restart from the
		 * last snapshot the builder has serialized.  The restart point of
		 * the reorder buffer is only set at the positions where the snapshot
		 * is on disk, so it is a safe place to restart from.
		 */
		else if (txn == NULL &&
				 pd_rb->current_restart_decoding_lsn != InvalidXLogRecPtr)
			wm.restart_lsn = (uint64) pd_rb->current_restart_decoding_lsn;
	}

	/*
	 * The session is winding down, so the walsender is not reading the control
	 * channel any more.  The message is not sent: the send blocks when the
	 * channel is full, and the walsender would not read it anyway.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	if (!PDQueueSend(pd_control_mqh, PD_ENT_WATERMARK, &wm, sizeof(wm),
					 false, true))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand the watermarks of the slot to the parallel "
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
 * Build the copy of the replication slot the walreader works on.
 *
 * The walsender owns the slot, so the walreader cannot hold it.  The decoding
 * path only reads a few fields of it, so a copy that carries those fields is
 * enough.
 */
static ReplicationSlot *
pd_walreader_build_slot_copy(ParallelDecodeControl *ctl)
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
 * Clear the copy of the slot before the walreader exits.
 *
 * The walreader does not own a slot: it works on a copy of the fields of the
 * slot of the walsender.  The callback has to be a before_shmem_exit one,
 * because that is where the release of the slot happens.  The callbacks run in
 * the reverse order of their registration, so this one runs before the
 * release.
 */
static void
pd_walreader_clear_slot(int code, Datum arg)
{
	MyReplicationSlot = NULL;
}

/*
 * Set up the state the walreader shares with the decode workers.
 *
 * The walreader drives the snapshot builder, so it owns a builder of its own.
 * The reorder buffer it works on does not hold the changes of the
 * transactions: it mirrors the state the builder works on.
 */
static void
pd_walreader_setup_shadow(ParallelDecodeControl *ctl)
{
	MemoryContext ctx_context;

	/*
	 * The copy of the slot has to be published before the reorder buffer is
	 * allocated: the allocation looks at it.
	 */
	pd_slot_copy = pd_walreader_build_slot_copy(ctl);
	MyReplicationSlot = pd_slot_copy;

	pd_rb = ReorderBufferAllocate();

	/*
	 * The builder starts at the position the decoding starts at, which is the
	 * same value the decode workers use for their own stand-in.  The walreader
	 * reads the WAL from an earlier position, so that it sees the records that
	 * bring the builder to a consistent state.
	 */
	pd_builder = AllocateSnapshotBuilder(pd_rb, InvalidTransactionId,
										 ctl->start_lsn, false, false,
										 ctl->two_phase_at);
	SnapBuildSetRole(pd_builder, SNAPBUILD_ROLE_WALREADER);

	ctx_context = AllocSetContextCreate(CurrentMemoryContext,
										"parallel decode walreader shadow context",
										ALLOCSET_DEFAULT_SIZES);

	pd_shadow_ctx = palloc0(sizeof(LogicalDecodingContext));
	pd_shadow_ctx->context = ctx_context;
	pd_shadow_ctx->slot = pd_slot_copy;
	pd_shadow_ctx->reorder = pd_rb;
	pd_shadow_ctx->snapshot_builder = pd_builder;
	pd_shadow_ctx->fast_forward = false;

	/*
	 * The walreader owns every transaction: the workers are the ones that
	 * decode the transactions, and they only decode the ones they own.
	 */
	pd_shadow_ctx->txn_owner = -1;
	pd_shadow_ctx->worker_index = -1;

	/*
	 * The walreader has no output plugin, so it cannot tell whether a
	 * prepared transaction is filtered.  It treats every prepare as filtered,
	 * which does not matter: the tracking buffer does not decode anything,
	 * and the worker that owns the transaction makes the decision that
	 * counts.
	 */
	pd_shadow_ctx->twophase = false;

	/* The snapshot builder reaches for the context through the buffer. */
	pd_rb->private_data = pd_shadow_ctx;

	pd_published_state = SNAPBUILD_START;
	pd_published_start_decoding_at = ctl->start_lsn;
}

/*
 * Publish the records that were written to the queues of the decode workers.
 *
 * The queue publishes what the sender has written only when the sender asks
 * for it, so the walreader has to publish the records before it goes to sleep
 * for the next WAL.  The flush is a message of its own, which the workers
 * ignore.
 */
static void
pd_walreader_flush_queues(void)
{
	int			i;

	for (i = 0; i < pd_num_workers; i++)
	{
		if (pd_wal_sender[i].pending == 0)
			continue;

		/*
		 * The flush counts the wakeup of the worker, so the counter of the
		 * session does not have to be touched here.
		 */
		PDQueueSenderFlush(&pd_wal_sender[i]);
	}

	pd_wal_pending = 0;
}

/*
 * Send a message to every decode worker.
 *
 * The message is appended to the queue of every worker, so that it reaches
 * them in the order of the WAL: the position of an event in the WAL is the
 * position of the message in the queue.
 *
 * The message is flushed right away: it is not a part of the data of a
 * transaction, and the workers have to see it before they go on.
 */
static void
pd_walreader_broadcast(uint16 type, const void *payload, size_t len)
{
	int			i;

	/*
	 * The session is winding down, so the workers are not reading their queues
	 * any more.  The message is not sent: the send blocks when the queue is
	 * full, and the workers would not read it anyway.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	for (i = 0; i < pd_num_workers; i++)
	{
		size_t		pending = pd_wal_sender[i].pending;

		if (!PDQueueSendUnbatched(&pd_wal_sender[i], type, payload, len))
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not hand a message to the parallel decoding session")));

		/* The message was flushed, so the queue holds nothing pending. */
		pd_wal_pending -= pending;
	}
}

/*
 * Send a message to one decode worker.
 */
static void
pd_walreader_send_to(int worker, uint16 type, const void *payload, size_t len)
{
	size_t		pending = pd_wal_sender[worker].pending;

	/*
	 * The session is winding down, so the worker is not reading its queue any
	 * more.  The message is not sent: the send blocks when the queue is full,
	 * and the worker would not read it anyway.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	if (!PDQueueSendUnbatched(&pd_wal_sender[worker], type, payload, len))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand a message to the parallel decoding session")));

	/* The message was flushed, so the queue holds nothing pending. */
	pd_wal_pending -= pending;
}

/*
 * Write a message to the queue of a decode worker.
 *
 * The message is batched: the queue publishes what the walreader has written
 * only when the walreader asks for it, and the receiver is woken up once per
 * batch instead of once per message.
 *
 * The batch is counted over all the queues of the session, so that a worker
 * that receives only the markers learns how far the walreader has come as
 * often as the workers that receive the records.
 */
static void
pd_walreader_send_worker(int worker, uint16 type, const void *payload, size_t len)
{
	PDQueueSender *sender = &pd_wal_sender[worker];

	/*
	 * The session is winding down, so the worker is not reading its queue any
	 * more.  The message is not sent: the send blocks when the queue is full,
	 * and the worker would not read it anyway.  A worker that is waiting for a
	 * record notices that the walreader is gone when the walreader detaches
	 * from the queue.
	 */
	if (pd_walreader_shutdown_requested())
		return;

	if (!PDQueueSendChunked(sender->mqh, type, payload, len,
							 PARALLEL_DECODE_CHUNK_SIZE, false, false))
		ereport(ERROR,
				(errcode(ERRCODE_IO_ERROR),
				 errmsg("could not hand a message to the parallel decoding session")));

	/*
	 * The message was written but not published: the receiver learns about
	 * it when the batch is flushed.
	 */
	sender->pending += len;
	pd_wal_pending += len;

	if (pd_wal_pending >= PDQueueBatchSize())
		pd_walreader_flush_queues();
}

/*
 * Tell the decode workers how far the walreader has read.
 *
 * The marker is batched: it is a message about the progress of the WAL, not
 * about the data of a transaction.
 *
 * The same position is not sent twice: the marker only says how far the
 * walreader has come, so a second marker with the same position tells the
 * workers nothing they do not know already.  The wait for new WAL would
 * otherwise send a marker to every worker every now and then.
 */
static void
pd_walreader_send_marker(XLogRecPtr position)
{
	static XLogRecPtr last_sent = InvalidXLogRecPtr;
	uint64		pos = (uint64) position;
	int			i;

	if (last_sent == position)
		return;

	for (i = 0; i < pd_num_workers; i++)
		pd_walreader_send_worker(i, PD_ENT_MARKER, &pos, sizeof(pos));

	last_sent = position;
}

/*
 * Tell the workers and the walsender how far the walreader has read.
 *
 * The workers need the marker to know that they have decoded everything the
 * walreader has read, and the walsender needs the position to know how far it
 * may report to the subscriber.
 *
 * The position is a certified one: the walreader sends the messages about the
 * transactions that end before it before it sends the position, and the
 * control channel keeps the order of the messages.  So the walsender knows
 * about every transaction that ends before the position when it reads it.
 */
static void
pd_walreader_publish_position(XLogRecPtr lsn)
{
	if (XLogRecPtrIsInvalid(lsn))
		return;

	pd_walreader_send_marker(lsn);
	pd_walreader_send_read_pos(lsn);
}

/*
 * Publish the content of the snapshot the builder holds.
 *
 * The content is sent to every worker: the transactions the workers hold may
 * have to switch over to the snapshot, and the transactions that start from
 * here on are decoded with it.
 *
 * The version of the snapshot is what the workers refer to it by, so it grows
 * every time a new snapshot is published.
 */
static void
pd_walreader_publish_content(XLogRecPtr lsn)
{
	PDSnapshotHeader header;
	PDSnapshotContent content;
	StringInfoData buf;
	Snapshot	snap;
	int			i;

	snap = SnapBuildGetSnapshot(pd_builder);
	if (snap == NULL)
		return;

	initStringInfo(&buf);

	/*
	 * A new snapshot means that the catalog the decoding depends on has
	 * changed, so the epoch of the session grows.  The workers use it to tell
	 * whether the definitions they have sent still describe the relations.
	 */
	pd_epoch++;

	header.version = ++pd_snapshot_version;
	header.epoch = pd_epoch;
	header.flags = PD_SNAPSHOT_CONTENT;
	header.ninvalidations = 0;
	header.state = SnapBuildCurrentState(pd_builder);
	header.lsn = lsn;
	header.start_decoding_at = SnapBuildGetStartDecodingAt(pd_builder);
	header.xid = InvalidTransactionId;
	header.next_phase_at = SnapBuildGetNextPhaseAt(pd_builder);

	content.xmin = snap->xmin;
	content.xmax = snap->xmax;
	content.curcid = snap->curcid;
	content.xcnt = snap->xcnt;

	appendBinaryStringInfo(&buf, (const char *) &header, sizeof(header));
	appendBinaryStringInfo(&buf, (const char *) &content, sizeof(content));

	for (i = 0; i < snap->xcnt; i++)
		appendBinaryStringInfo(&buf, (const char *) &snap->xip[i],
							   sizeof(TransactionId));

	pd_walreader_broadcast(PD_ENT_SNAPSHOT, buf.data, buf.len);

	pfree(buf.data);

	pd_published_snapshot = snap;
}

/*
 * Publish the content of the snapshot, if the snapshot has changed.
 *
 * The content has to reach the workers before they are told which snapshot
 * they are to use, so that they have it when the message that refers to it
 * arrives.
 */
static void
pd_walreader_publish_content_if_changed(XLogRecPtr lsn)
{
	if (SnapBuildGetSnapshot(pd_builder) != pd_published_snapshot)
		pd_walreader_publish_content(lsn);
}

/*
 * Tell the worker that owns a transaction which snapshot the transaction is
 * decoded with.
 *
 * The message goes to the owner of the transaction only: the other workers do
 * not hold the transaction.
 *
 * The transaction is named by its top level transaction id, because that is
 * the transaction the snapshot belongs to: the snapshot of a transaction that
 * has subtransactions is the snapshot of its top level transaction.
 *
 * The position the message carries is the position of the change that gave the
 * transaction its base snapshot, which is what the serial path records as the
 * position of the base snapshot of the transaction.
 */
static void
pd_walreader_publish_base(XLogRecPtr lsn, TransactionId xid, int worker)
{
	PDSnapshotHeader header;

	header.version = pd_snapshot_version;
	header.epoch = pd_epoch;
	header.flags = PD_SNAPSHOT_BASE;
	header.ninvalidations = 0;
	header.state = SnapBuildCurrentState(pd_builder);
	header.lsn = lsn;
	header.start_decoding_at = SnapBuildGetStartDecodingAt(pd_builder);
	header.xid = xid;
	header.next_phase_at = SnapBuildGetNextPhaseAt(pd_builder);

	pd_walreader_send_to(worker, PD_ENT_SNAPSHOT, &header, sizeof(header));
}

/*
 * Tell the workers that the transactions which are in flight switch over to
 * the snapshot that was published last.
 *
 * The invalidation messages of the transaction that committed go with the
 * message: they are added to the transactions that are in flight, so that
 * they see the new catalog contents.  The committing transaction itself
 * executes them when it ends, which is what the serial path does as well.
 */
static void
pd_walreader_publish_switch(XLogRecPtr lsn, TransactionId xid,
							uint32 ninvalidations,
							SharedInvalidationMessage *msgs)
{
	PDSnapshotHeader header;
	StringInfoData buf;

	initStringInfo(&buf);

	header.version = pd_snapshot_version;
	header.epoch = pd_epoch;
	header.flags = PD_SNAPSHOT_SWITCH;
	header.ninvalidations = ninvalidations;
	header.state = SnapBuildCurrentState(pd_builder);
	header.lsn = lsn;
	header.start_decoding_at = SnapBuildGetStartDecodingAt(pd_builder);
	header.xid = xid;
	header.next_phase_at = SnapBuildGetNextPhaseAt(pd_builder);

	if (ninvalidations > 0)
		header.flags |= PD_SNAPSHOT_INVAL;

	appendBinaryStringInfo(&buf, (const char *) &header, sizeof(header));
	if (ninvalidations > 0)
		appendBinaryStringInfo(&buf, (const char *) msgs,
							   ninvalidations * sizeof(SharedInvalidationMessage));

	pd_walreader_broadcast(PD_ENT_SNAPSHOT, buf.data, buf.len);

	pfree(buf.data);
}

/*
 * Tell the workers about the state of the snapshot builder.
 *
 * The workers need the state to decide whether they decode a record at all,
 * they need the start of the decoding window to skip the transactions that
 * ended before the decoding started, and they need the transaction the next
 * phase of the builder starts at to skip the transactions that the builder
 * itself skips.  All of them are published whenever they change, so that the
 * workers are in step with the walreader.
 */
static void
pd_walreader_publish_state(XLogRecPtr lsn)
{
	PDSnapshotHeader header;
	int			state;
	XLogRecPtr	start_decoding_at;
	TransactionId next_phase_at;

	state = SnapBuildCurrentState(pd_builder);
	start_decoding_at = SnapBuildGetStartDecodingAt(pd_builder);
	next_phase_at = SnapBuildGetNextPhaseAt(pd_builder);

	if (state == pd_published_state &&
		start_decoding_at == pd_published_start_decoding_at &&
		next_phase_at == pd_published_next_phase_at)
		return;

	header.version = pd_snapshot_version;
	header.epoch = pd_epoch;
	header.flags = PD_SNAPSHOT_STATE;
	header.ninvalidations = 0;
	header.state = state;
	header.lsn = lsn;
	header.start_decoding_at = start_decoding_at;
	header.xid = InvalidTransactionId;
	header.next_phase_at = next_phase_at;

	pd_walreader_broadcast(PD_ENT_SNAPSHOT, &header, sizeof(header));

	pd_published_state = state;
	pd_published_start_decoding_at = start_decoding_at;
	pd_published_next_phase_at = next_phase_at;
}

/*
 * Broadcast the invalidation messages of a record that has no transaction.
 *
 * The invalidation messages of such a record affect the catalog state of the
 * whole cluster, so every worker has to execute them: the catalog caches of
 * the workers are private to their process.
 */
static void
pd_walreader_publish_inval(uint32 ninvalidations,
						   SharedInvalidationMessage *msgs)
{
	PDInvalHeader header;
	StringInfoData buf;

	initStringInfo(&buf);

	/*
	 * The invalidation messages change the catalog caches of the workers, so
	 * the definitions they have sent do not describe the relations anymore.
	 * The epoch of the session grows, so that the workers know it.
	 */
	pd_epoch++;

	header.epoch = pd_epoch;
	header.ninvalidations = ninvalidations;

	appendBinaryStringInfo(&buf, (const char *) &header, sizeof(header));
	if (ninvalidations > 0)
		appendBinaryStringInfo(&buf, (const char *) msgs,
							   ninvalidations * sizeof(SharedInvalidationMessage));

	pd_walreader_broadcast(PD_ENT_GLOBAL_INVAL, buf.data, buf.len);

	pfree(buf.data);
}

/*
 * The worker that owns a transaction.
 *
 * The entry of the transaction holds the worker that owns it, so the lookup
 * is the same one the routing of the records uses.
 */
static int
pd_walreader_owner_of(TransactionId xid)
{
	PDTxnInfo *info;

	info = pd_walreader_find_txn(xid);
	if (info == NULL)
		return PD_FORWARD_NONE;

	return info->top->worker;
}

/*
 * Apply the gating of the decoding path to a record that changes a table.
 *
 * The gating decides whether the change is decoded at all.  The first change
 * of a transaction also determines the snapshot the transaction is decoded
 * with, so the base snapshot is published here as well.
 *
 * Returns true when the change is decoded.
 */
static bool
pd_walreader_gate_change(TransactionId xid, XLogRecPtr lsn)
{
	PDTxnInfo *info;
	bool		had_base;
	bool		gated;
	int			worker;

	had_base = ReorderBufferXidHasBaseSnapshot(pd_rb, xid);

	gated = SnapBuildProcessChange(pd_builder, xid, lsn);

	/*
	 * The snapshot may have been built just now, in which case its content
	 * has to be handed down before the transaction is told which snapshot it
	 * is decoded with.
	 */
	pd_walreader_publish_content_if_changed(lsn);

	if (!had_base && ReorderBufferXidHasBaseSnapshot(pd_rb, xid))
	{
		worker = pd_walreader_owner_of(xid);
		if (worker >= 0)
		{
			info = pd_walreader_find_txn(xid);

			/*
			 * The position the message carries is the position of the change
			 * that gave the transaction its base snapshot.  That is the
			 * position the serial path records as the position of the base
			 * snapshot of the transaction.
			 */
			pd_walreader_publish_base(lsn, info->top->toplevel, worker);
		}
	}

	return gated;
}

/*
 * Handle a record that ends a transaction.
 *
 * The snapshot builder has to know about the commit: a transaction that
 * changed the catalogs makes the transactions that are in flight switch over
 * to a new snapshot, and the invalidation messages of the transaction go with
 * the switch.
 */
static void
pd_walreader_shadow_commit(XLogRecPtr lsn, TransactionId xid,
						   int nsubxacts, TransactionId *subxacts,
						   uint32 xinfo, bool aborted)
{
	Snapshot	old_snapshot;
	uint32		ninvalidations;
	SharedInvalidationMessage *msgs = NULL;

	old_snapshot = SnapBuildGetSnapshot(pd_builder);

	SnapBuildCommitTxn(pd_builder, lsn, xid, nsubxacts, subxacts, xinfo);

	/*
	 * The invalidation messages of the transaction have to be read before the
	 * transaction is forgotten.  They are accumulated under the top level
	 * transaction, so that is the one they have to be asked for.
	 */
	ninvalidations = ReorderBufferGetInvalidations(pd_rb, xid, &msgs);

	/*
	 * The transaction changed the catalogs, so a new snapshot was built.  Its
	 * content is handed down first, so that the workers have it when they are
	 * told to switch over to it.
	 */
	if (SnapBuildGetSnapshot(pd_builder) != old_snapshot)
	{
		pd_walreader_publish_content(lsn);
		pd_walreader_publish_switch(lsn, xid, ninvalidations, msgs);
	}

	/*
	 * The transaction is over, so the tracking buffer does not have to keep
	 * it.  The forget executes the invalidation messages of the transaction,
	 * which is what the serial path does as well.  An aborted transaction did
	 * not change the catalogs, so its invalidation messages are dropped.
	 */
	if (aborted)
		ReorderBufferAbort(pd_rb, xid, lsn, (TimestampTz) 0);
	else
		ReorderBufferForget(pd_rb, xid, lsn);
}

/*
 * The shadow decoding of a record of the heap rmgr.
 *
 * The walreader does not decode the tuples, but it has to know what the
 * records do to the snapshot builder, and which worker has to decode them.
 */
static int
pd_walreader_shadow_heap(XLogReaderState *reader, XLogRecPtr lsn, uint8 info,
						  TransactionId xid)
{
	ReorderBufferProcessXid(pd_rb, xid, lsn);

	if (SnapBuildCurrentState(pd_builder) < SNAPBUILD_FULL_SNAPSHOT)
		return PD_FORWARD_NONE;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP_INSERT:
		case XLOG_HEAP_HOT_UPDATE:
		case XLOG_HEAP_UPDATE:
		case XLOG_HEAP_DELETE:
		case XLOG_HEAP_TRUNCATE:
			if (pd_walreader_gate_change(xid, lsn))
				return PD_FORWARD_OWNER;
			return PD_FORWARD_NONE;

		case XLOG_HEAP_INPLACE:
			/*
			 * In-place updates only ever happen on catalog tuples, so they
			 * mark the transaction as catalog modifying.
			 */
			if (!TransactionIdIsValid(xid))
				return PD_FORWARD_NONE;

			(void) pd_walreader_gate_change(xid, lsn);
			ReorderBufferXidSetCatalogChanges(pd_rb, xid, lsn);
			return PD_FORWARD_OWNER;

		case XLOG_HEAP_LOCK:
			/* we don't care about row level locks */
			return PD_FORWARD_NONE;

		case XLOG_HEAP_CONFIRM:
			if (pd_walreader_gate_change(xid, lsn))
				return PD_FORWARD_OWNER;
			return PD_FORWARD_NONE;

		default:
			elog(ERROR, "unexpected RM_HEAP_ID record type: %u", info);
			return PD_FORWARD_NONE;
	}
}

/*
 * The shadow decoding of a record of the heap2 rmgr.
 */
static int
pd_walreader_shadow_heap2(XLogReaderState *reader, XLogRecPtr lsn, uint8 info,
						   TransactionId xid)
{
	ReorderBufferProcessXid(pd_rb, xid, lsn);

	if (SnapBuildCurrentState(pd_builder) < SNAPBUILD_FULL_SNAPSHOT)
		return PD_FORWARD_NONE;

	switch (info & XLOG_HEAP_OPMASK)
	{
		case XLOG_HEAP2_MULTI_INSERT:
			if (pd_walreader_gate_change(xid, lsn))
				return PD_FORWARD_OWNER;
			return PD_FORWARD_NONE;

		case XLOG_HEAP2_NEW_CID:
			{
				xl_heap_new_cid *xlrec;

				xlrec = (xl_heap_new_cid *) XLogRecGetData(reader);
				SnapBuildProcessNewCid(pd_builder, xid, lsn, xlrec);

				return PD_FORWARD_OWNER;
			}

		case XLOG_HEAP2_REWRITE:
		case XLOG_HEAP2_PRUNE_ON_ACCESS:
		case XLOG_HEAP2_PRUNE_VACUUM_SCAN:
		case XLOG_HEAP2_PRUNE_VACUUM_CLEANUP:
		case XLOG_HEAP2_VISIBLE:
		case XLOG_HEAP2_LOCK_UPDATED:
			return PD_FORWARD_NONE;

		default:
			elog(ERROR, "unexpected RM_HEAP2_ID record type: %u", info);
			return PD_FORWARD_NONE;
	}
}

/*
 * The shadow decoding of a record of the xact rmgr.
 */
static int
pd_walreader_shadow_xact(XLogReaderState *reader, XLogRecPtr lsn, uint8 info)
{
	TransactionId xid = XLogRecGetXid(reader);

	switch (info & XLOG_XACT_OPMASK)
	{
		case XLOG_XACT_COMMIT:
		case XLOG_XACT_COMMIT_PREPARED:
			{
				xl_xact_commit *xlrec;
				xl_xact_parsed_commit parsed;
				TransactionId commit_xid;

				xlrec = (xl_xact_commit *) XLogRecGetData(reader);
				ParseCommitRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				commit_xid = TransactionIdIsValid(parsed.twophase_xid) ?
					parsed.twophase_xid : xid;

				pd_walreader_shadow_commit(lsn, commit_xid,
										   parsed.nsubxacts, parsed.subxacts,
										   parsed.xinfo, false);

				return PD_FORWARD_OWNER;
			}

		case XLOG_XACT_ABORT:
		case XLOG_XACT_ABORT_PREPARED:
			{
				xl_xact_abort *xlrec;
				xl_xact_parsed_abort parsed;
				TransactionId abort_xid;

				xlrec = (xl_xact_abort *) XLogRecGetData(reader);
				ParseAbortRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				abort_xid = TransactionIdIsValid(parsed.twophase_xid) ?
					parsed.twophase_xid : xid;

				pd_walreader_shadow_commit(lsn, abort_xid, 0, NULL, 0, true);

				return PD_FORWARD_OWNER;
			}

		case XLOG_XACT_ASSIGNMENT:
			/*
			 * The subtransaction is assigned to the top level transaction
			 * while the records are processed, so there is nothing to do
			 * here.  The record is forwarded, because the worker needs it to
			 * make the same assignment.
			 */
			return PD_FORWARD_OWNER;

		case XLOG_XACT_INVALIDATIONS:
			{
				xl_xact_invals *invals;

				invals = (xl_xact_invals *) XLogRecGetData(reader);

				if (TransactionIdIsValid(xid))
				{
					/*
					 * The invalidation messages are accumulated, so that they
					 * are executed when the transaction ends.
					 */
					ReorderBufferAddInvalidations(pd_rb, xid, lsn,
												  invals->nmsgs, invals->msgs);
					ReorderBufferXidSetCatalogChanges(pd_rb, xid, lsn);

					return PD_FORWARD_OWNER;
				}

				/*
				 * The invalidation messages of a record that has no
				 * transaction affect the catalog state of the whole cluster,
				 * so every worker has to execute them.
				 */
				ReorderBufferImmediateInvalidation(pd_rb, invals->nmsgs,
												   invals->msgs);
				pd_walreader_publish_inval(invals->nmsgs, invals->msgs);

				return PD_FORWARD_NONE;
			}

		case XLOG_XACT_PREPARE:
			{
				xl_xact_parsed_prepare parsed;
				xl_xact_prepare *xlrec;

				xlrec = (xl_xact_prepare *) XLogRecGetData(reader);
				ParsePrepareRecord(XLogRecGetInfo(reader), xlrec, &parsed);

				/*
				 * The walreader has no output plugin, so it cannot tell whether
				 * the prepared transaction is filtered.  It treats every
				 * prepare as filtered, which does not matter: the tracking
				 * buffer does not decode anything, and the worker that owns
				 * the transaction makes the decision that counts.
				 */
				ReorderBufferProcessXid(pd_rb, parsed.twophase_xid, lsn);

				return PD_FORWARD_OWNER;
			}

		default:
			elog(ERROR, "unexpected RM_XACT_ID record type: %u", info);
			return PD_FORWARD_NONE;
	}
}

/*
 * The shadow decoding of a record of the standby rmgr.
 */
static int
pd_walreader_shadow_standby(XLogReaderState *reader, XLogRecPtr lsn,
							 uint8 info)
{
	ReorderBufferProcessXid(pd_rb, XLogRecGetXid(reader), lsn);

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_RUNNING_XACTS:
			{
				xl_running_xacts *running;

				running = (xl_running_xacts *) XLogRecGetData(reader);

				SnapBuildProcessRunningXacts(pd_builder, lsn, running);

				/*
				 * Abort the transactions the walreader still holds that are
				 * older than the oldest transaction that is running.  This is
				 * what the serial path does with the buffer that holds the
				 * transactions of the session, and it keeps the buffer from
				 * holding transactions that will never end: such a
				 * transaction would hold the position the decoding may
				 * restart from back forever.
				 */
				ReorderBufferAbortOld(pd_rb, running->oldestRunningXid);

				/*
				 * The watermarks of the slot depend on the transactions that
				 * are in flight.  The walreader computes them and hands them
				 * over to the walsender, which owns the slot.
				 */
				pd_walreader_send_watermark(lsn, running->oldestRunningXid);

				/*
				 * The workers have to abort the transactions they still hold
				 * that are not running anymore, so the record goes to all of
				 * them.
				 */
				return PD_FORWARD_ALL;
			}

		case XLOG_STANDBY_LOCK:
			return PD_FORWARD_NONE;

		case XLOG_INVALIDATIONS:
			/*
			 * The invalidation messages are processed at the command level
			 * through XLOG_XACT_INVALIDATIONS.
			 */
			return PD_FORWARD_NONE;

		default:
			elog(ERROR, "unexpected RM_STANDBY_ID record type: %u", info);
			return PD_FORWARD_NONE;
	}
}

/*
 * The shadow decoding of a record of the logical message rmgr.
 */
static int
pd_walreader_shadow_logicalmsg(XLogReaderState *reader, XLogRecPtr lsn,
								TransactionId xid)
{
	xl_logical_message *message;

	ReorderBufferProcessXid(pd_rb, xid, lsn);

	if (SnapBuildCurrentState(pd_builder) < SNAPBUILD_FULL_SNAPSHOT)
		return PD_FORWARD_NONE;

	message = (xl_logical_message *) XLogRecGetData(reader);

	/*
	 * The messages of another database are not decoded.
	 */
	if (message->dbId != pd_shadow_ctx->slot->data.database)
		return PD_FORWARD_NONE;

	if (message->transactional)
	{
		if (!pd_walreader_gate_change(xid, lsn))
			return PD_FORWARD_NONE;

		return PD_FORWARD_OWNER;
	}

	/*
	 * A message that is not part of a transaction is decoded with the
	 * snapshot the builder holds, so the snapshot has to exist and its
	 * content has to be handed down.
	 */
	if (SnapBuildCurrentState(pd_builder) != SNAPBUILD_CONSISTENT ||
		SnapBuildXactNeedsSkip(pd_builder, lsn))
		return PD_FORWARD_NONE;

	SnapBuildGetOrBuildSnapshot(pd_builder);
	pd_walreader_publish_content_if_changed(lsn);

	/*
	 * A message that is not part of a transaction does not belong to a
	 * transaction, so there is no owner to pick.  It is handed to the worker
	 * that has the least work, so that the messages are spread out.  It must
	 * go to exactly one worker: the message is not part of a transaction, so
	 * there is nothing that would keep the workers from producing it twice.
	 */
	return pd_walreader_pick_worker();
}

/*
 * The shadow decoding of a record of the xlog rmgr.
 */
static int
pd_walreader_shadow_xlog(XLogReaderState *reader, XLogRecPtr lsn, uint8 info)
{
	ReorderBufferProcessXid(pd_rb, XLogRecGetXid(reader), lsn);

	switch (info & ~XLR_INFO_MASK)
	{
		case XLOG_CHECKPOINT_SHUTDOWN:
		case XLOG_END_OF_RECOVERY:
			SnapBuildSerializationPoint(pd_builder, lsn);
			return PD_FORWARD_NONE;

		case XLOG_CHECKPOINT_ONLINE:
		case XLOG_PARAMETER_CHANGE:
		case XLOG_NOOP:
		case XLOG_NEXTOID:
		case XLOG_SWITCH:
		case XLOG_BACKUP_END:
		case XLOG_RESTORE_POINT:
		case XLOG_FPW_CHANGE:
		case XLOG_FPI_FOR_HINT:
		case XLOG_FPI:
		case XLOG_OVERWRITE_CONTRECORD:
		case XLOG_CHECKPOINT_REDO:
			return PD_FORWARD_NONE;

		default:
			elog(ERROR, "unexpected RM_XLOG_ID record type: %u", info);
			return PD_FORWARD_NONE;
	}
}

/*
 * The shadow decoding of a record.
 *
 * The walreader does not decode the tuples of the records, but it has to know
 * what the records do to the snapshot builder: which transactions are
 * catalog modifying, which snapshot a transaction is decoded with, and when
 * the transactions that are in flight have to switch over to a new one.
 *
 * The function mirrors the gating of the decoding path (see decode.c) and
 * tells which worker the record has to be forwarded to.
 */
static int
pd_walreader_shadow_decode(XLogReaderState *reader, XLogRecPtr lsn)
{
	uint8		rmgr_id = XLogRecGetRmid(reader);
	uint8		info = XLogRecGetInfo(reader);
	TransactionId xid = XLogRecGetXid(reader);
	int			routing;

	/*
	 * If the top level xid is valid, the subtransaction has to be assigned to
	 * the top level transaction.  This is done for every record, before the
	 * record is looked at, which is what the decoding path does as well.
	 */
	if (TransactionIdIsValid(XLogRecGetTopXid(reader)))
		ReorderBufferAssignChild(pd_rb,
								  XLogRecGetTopXid(reader),
								  xid, lsn);

	switch (rmgr_id)
	{
		case RM_XLOG_ID:
			routing = pd_walreader_shadow_xlog(reader, lsn, info);
			break;

		case RM_XACT_ID:
			routing = pd_walreader_shadow_xact(reader, lsn, info);
			break;

		case RM_STANDBY_ID:
			routing = pd_walreader_shadow_standby(reader, lsn, info);
			break;

		case RM_HEAP_ID:
			routing = pd_walreader_shadow_heap(reader, lsn, info, xid);
			break;

		case RM_HEAP2_ID:
			routing = pd_walreader_shadow_heap2(reader, lsn, info, xid);
			break;

		case RM_LOGICALMSG_ID:
			routing = pd_walreader_shadow_logicalmsg(reader, lsn, xid);
			break;

		default:
			/*
			 * The rmgr has no decoding callback, so the decoding path only
			 * deals with the transaction id of the record.
			 */
			ReorderBufferProcessXid(pd_rb, xid, lsn);
			routing = PD_FORWARD_NONE;
			break;
	}

	/*
	 * The state of the builder and the snapshot it holds are handed down to
	 * the workers before the record they belong to is forwarded, so that the
	 * workers decode the record with the same state the walreader has.
	 */
	pd_walreader_publish_content_if_changed(lsn);
	pd_walreader_publish_state(lsn);

	return routing;
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
	int			routing;
	int			marked;
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

	/*
	 * The shadow context needs the reader, because the assertions of the
	 * reorder buffer look at the position the reader is at.
	 */
	pd_shadow_ctx->reader = reader;

	buf = makeStringInfo();

	marked = 0;

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
			 * There is no more WAL at the moment.  Tell the workers how far the
			 * walreader has come, so that they can report that they have decoded
			 * everything that was meant for them, and tell the walsender how far
			 * the walreader has read, so that it may report the position to the
			 * subscriber: with no WAL to decode, the position of the session would
			 * otherwise stay where it was.
			 */
			pd_walreader_publish_position(pd_read_lsn);

			/*
			 * There is no more WAL at the moment, so publish the records
			 * that were read: the workers are waiting for them.
			 */
			pd_walreader_flush_queues();

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
		 * Let the snapshot builder of the walreader see the record.  The
		 * builder is the one that decides which snapshot a transaction is
		 * decoded with, and the workers are told about it, so that they decode
		 * the transactions with the same snapshot the serial path uses.
		 *
		 * The shadow decoding also tells which worker the record belongs to.
		 */
		routing = pd_walreader_shadow_decode(reader, reader->record->lsn);

		/*
		 * Hand the record over.  The record points into the reader's decode
		 * buffer, which the next read reuses, so it has to be copied before
		 * we go on.
		 *
		 * Only the worker that owns the transaction gets the record: the
		 * workers hold the transactions of their own, and the snapshot builder
		 * of the walreader is the one that keeps them in step.  The records
		 * that every worker needs are the ones that say which transactions are
		 * still running.
		 */
		if (routing != PD_FORWARD_NONE)
		{
			int		target;

			switch (routing)
			{
				case PD_FORWARD_ALL:
					target = -1;
					break;
				case PD_FORWARD_OWNER:
					target = worker;
					break;
				default:
					target = routing;
					break;
			}

			resetStringInfo(buf);
			wal_header.owner = target;
			wal_header.cseq = cseq;
			appendBinaryStringInfo(buf, (const char *) &wal_header,
								sizeof(wal_header));
			PDRecordSerialize(reader->record, buf);

			if (target < 0)
			{
				for (i = 0; i < pd_num_workers; i++)
				{
					/*
					 * The record has been handed over.  The worker is woken
					 * up when the batch is flushed, not for every record.
					 */
					pd_walreader_send_worker(i, PD_ENT_WAL, buf->data, buf->len);

					pd_ctl->workers[i].records_sent++;
				}
			}
			else
			{
				pd_walreader_send_worker(target, PD_ENT_WAL, buf->data, buf->len);

				pd_ctl->workers[target].records_sent++;
			}

			pd_ctl->wal_records++;

			/*
			 * Account for the bytes the transaction has produced, so that the
			 * next transaction goes to the worker that has the least work.
			 *
			 * The transaction of a record that ends it is over, and its entry is
			 * gone: the bytes of such a record do not have to be accounted for,
			 * because the in-flight bytes of the transaction have been released
			 * already.
			 */
			if (info != NULL && !recdesc.ended && target >= 0)
			{
				info->bytes += buf->len;
				pd_worker_bytes[worker] += buf->len;

				pd_walreader_publish_worker_counters(worker);
			}
		}

		/*
		 * Tell the workers how far the walreader has come every now and then.
		 * A worker that owns no transaction does not see any records, so the
		 * marker is what tells it that it has nothing left to do.
		 */
		if (++marked >= PD_POSITION_MARK_RECORDS)
		{
			marked = 0;

			/*
			 * Tell the workers how far the walreader has come, and tell the
			 * walsender as well, so that it may report the position to the
			 * subscriber.  The message about the position goes out after the
			 * messages about the transactions that ended before this position, so
			 * the walsender knows about all of them when it reads it.
			 */
			pd_walreader_publish_position(reader->record->next_lsn);
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
	volatile bool stopping = false;

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

	/*
	 * Set up the state the walreader shares with the decode workers: the
	 * snapshot builder and the reorder buffer that mirrors it.
	 *
	 * The walreader does not own the slot, so it works on a copy of the
	 * fields of the slot of the walsender.  The exit path must not try to
	 * release it.
	 */
	pd_walreader_setup_shadow(ctl);
	before_shmem_exit(pd_walreader_clear_slot, (Datum) 0);

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
		PDQueueSenderInit(&pd_wal_sender[i], shm_mq_attach(wal_mq, seg, NULL),
						   &pd_ctl->walreader_wakeups);
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

	/*
	 * The control queue is flushed for every message, so every message wakes
	 * the walsender up.  Count the wakeups, so that the monitoring views show
	 * how often the walreader wakes the walsender up.  Without the counter
	 * these wakeups are invisible: the counter of the wakeups of the worker
	 * queues counts the wakeups of the walreader down to the workers, and
	 * the control queue does not go through a sender of its own.
	 */
	PDQueueSetWakeupCounter(pd_control_mqh, &pd_ctl->walreader_wakeups);

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
		/*
		 * The queue was detached, so the session is going away: the walsender
		 * is gone, or it has asked the participants to stop.  That is not a
		 * failure of the walreader, so the walreader stops quietly instead of
		 * reporting an error that nobody is going to read.
		 */
		if (PDQueueDetached() && pd_walreader_stopped())
		{
			stopping = true;
		}
		else
		{
			ErrorData  *edata;

			/*
			 * Copy the error data before anything else: the error context is
			 * gone as soon as the error is rethrown.
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
			 * Rethrow the error, so that it is logged and the walreader exits
			 * with a code that does not make the postmaster restart the
			 * cluster.
			 */
			PG_RE_THROW();
		}
	}
	PG_END_TRY();

	ctl->walreader_status = PD_WORKER_STATUS_EXITED;

	if (stopping)
		ereport(LOG,
				(errmsg("parallel decode walreader stopped while the session was "
						"ending")));
	else
		ereport(LOG,
				(errmsg("parallel decode walreader stopped")));

	/*
	 * Detach from the queues, so that the processes on the other end do not
	 * wait for a message that is never going to come: the workers would wait
	 * for a record, and the walsender for a message on the control channel.
	 */
	pd_walreader_detach_queues();

	proc_exit(0);
}
