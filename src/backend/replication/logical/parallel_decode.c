/*-------------------------------------------------------------------------
 *
 * parallel_decode.c
 *		Publisher-side parallel logical decoding: session control.
 *
 * This file owns the pieces that live in the walsender process:
 *
 *   - the shared control block and the shared message queues;
 *   - the lifecycle of the walreader and of the decode workers, which are
 *     registered as dynamic background workers;
 *   - the decision whether the parallel path is used at all.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <errno.h>
#include <signal.h>

#include "catalog/pg_subscription.h"
#include "miscadmin.h"
#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "nodes/value.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/parallel_decode.h"
#include "replication/walsender_private.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/shm_mq.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/wait_event.h"
#include "utils/memutils.h"

/* GUCs */
int			max_parallel_decode_workers = 0;
int			parallel_decode_streaming_mode = PARALLEL_DECODE_STREAMING_AUTO;
int			parallel_decode_stream_min_size = 0;
int			parallel_decode_stream_segment_size = 1024 * 1024;
int			parallel_decode_work_mem = 0;	/* 0 means: use the PG default */
int			parallel_decode_batch_size = 64 * 1024;
int			parallel_decode_barrier_timeout = 60;	/* seconds, 0 disables it */
int			parallel_decode_fault_inject = PD_FAULT_NONE;

/* Session state, only touched by the walsender. */
static dsm_segment *parallel_decode_seg = NULL;
static shm_toc *parallel_decode_toc = NULL;
static ParallelDecodeControl *parallel_decode_ctl = NULL;
static BackgroundWorkerHandle *parallel_decode_walreader_handle = NULL;
static shm_mq_handle *parallel_decode_control_mqh = NULL;
static shm_mq_handle *parallel_decode_out_mqh[MAX_PARALLEL_DECODE_WORKERS];
static PDChunkReceiver parallel_decode_chunk_rcv[MAX_PARALLEL_DECODE_WORKERS];
static ParallelDecodeIdleCallback parallel_decode_idle_cb = NULL;
static BackgroundWorkerHandle *parallel_decode_worker_handles[MAX_PARALLEL_DECODE_WORKERS];
static pid_t	parallel_decode_worker_pids[MAX_PARALLEL_DECODE_WORKERS];
static int	parallel_decode_num_workers = 0;
static bool parallel_decode_session_active = false;

/*
 * The WAL position the decoding has come to.  It advances with the frames the
 * decode worker produces and with the position messages it sends when it
 * runs out of work.
 */
static XLogRecPtr parallel_decode_position = InvalidXLogRecPtr;

/*
 * A frame that is waiting to be sent.
 */
typedef struct PDQueuedFrame
{
	char	   *data;			/* the frame */
	size_t		len;			/* the length of the frame */
	XLogRecPtr	end_lsn;		/* the decoding position after the record */
	Oid			relid;			/* the relation the frame defines, or
									 * InvalidOid for the other frames */
	uint64		epoch;			/* the epoch of the session the definition
									 * was produced in */
} PDQueuedFrame;

/*
 * The frames of a transaction that are waiting for their turn.
 *
 * The frames of a transaction are held back until the transaction has its
 * turn, so that the transactions reach the subscriber in the order they were
 * committed in.  The turn comes when the worker that owns the transaction has
 * reported that it has decoded it: the report is sent after the frames of the
 * transaction, so the transaction is complete when it arrives.
 */
typedef struct PDPendingTxn
{
	uint64		cseq;			/* the commit sequence number of the transaction */
	List	   *frames;			/* the frames, in the order they were produced */
	bool		acked;			/* the worker has reported the end of the transaction */
	TimestampTz alloc_time;	/* when the walreader reported the transaction */
	TimestampTz ack_time;		/* when the worker reported it decoded */
} PDPendingTxn;

/*
 * The transactions whose frames are waiting for their turn, keyed by their
 * commit sequence number.
 */
static HTAB *parallel_decode_pending = NULL;

/*
 * The relations whose definition the session has sent to the subscriber.
 *
 * Every decode worker has its own cache of the relations it has sent, so
 * every worker that touches a relation produces its definition.  The walsender
 * is the one that knows in which order the transactions reach the
 * subscriber, so it is the one that decides which of the definitions are
 * needed: the definition of a relation has to reach the subscriber before the
 * first change of the relation, and the transactions reach the subscriber in
 * the order of their commit sequence numbers.  The definition of the
 * transaction with the smallest sequence number is therefore the one that is
 * needed, and the others are dropped.
 *
 * The table maps a relation to the epoch of the session in which its
 * definition was sent.  A definition that was produced before the last change
 * of the catalog does not describe the relation anymore, so it may not be
 * reused: the epoch tells the definitions apart.
 */
static HTAB *parallel_decode_sent_relations = NULL;

/*
 * The commit sequence number of the transaction whose frames may be sent next.
 */
static uint64 parallel_decode_next_cseq = 1;

/*
 * The frames that may be sent, in the order they have to be sent.
 */
static List *parallel_decode_ready = NIL;

/*
 * The transactions that have been given a commit sequence number, with the
 * position of the record that ended them.
 *
 * The list is ordered by the commit sequence number, and the entries of the
 * transactions that have been released are removed from its head.  The first
 * entry is therefore the transaction that holds the release frontier back.
 */
static List *parallel_decode_frontier = NIL;

/*
 * The position the walreader has read up to, and the commit sequence number
 * it had given out at that point.
 *
 * The position may only be used as the release frontier when every
 * transaction that ended before it has been released: the walreader reports
 * the position together with the sequence number, so the two together say
 * which transactions the position covers.
 */
static XLogRecPtr parallel_decode_certified_lsn = InvalidXLogRecPtr;
static uint64 parallel_decode_certified_cseq = 0;

/*
 * The decoding position of every worker, and the time of the last message of
 * every worker.
 *
 * The positions tell the walsender whether a worker still has records to
 * decode, and the times tell it whether a worker that still has work is making
 * any progress.  A worker that stops making progress while it still has work
 * to do is stuck, and the session that waits for it has to end.
 */
static XLogRecPtr parallel_decode_worker_position[MAX_PARALLEL_DECODE_WORKERS];
static instr_time parallel_decode_worker_progress[MAX_PARALLEL_DECODE_WORKERS];

/*
 * Make sure that the workers and the walreader never outlive the session
 * that started them.  This runs on every process exit path, including the
 * ones that do not go through ParallelDecodeSessionStop(), so it only raises
 * the shutdown flag: waiting for the workers here would not be safe.
 *
 * The callback has to be a before_shmem_exit one.  proc_exit() runs the
 * before_shmem_exit callbacks first, then unmaps the dynamic shared memory
 * segments of this process, and only then runs the on_proc_exit callbacks.
 * An on_proc_exit callback would therefore touch a segment that is no longer
 * mapped.
 */
static void
parallel_decode_before_shmem_exit(int code, Datum arg)
{
	if (parallel_decode_ctl)
		parallel_decode_ctl->shutdown = 1;
}

/*
 * Returns true when the given process still exists.
 *
 * The workers use this to notice that the walsender died without a chance to
 * raise the shutdown flag, which happens when the walsender is killed by a
 * signal.  The walsender is a child of the postmaster, which reaps it as soon
 * as it is gone, so the process disappears from the process table promptly.
 */
bool
ParallelDecodeProcessAlive(pid_t pid)
{
	if (pid <= 0)
		return false;

	if (kill(pid, 0) == 0)
		return true;

	/* EPERM means the process exists but belongs to another user. */
	return errno == EPERM;
}

/*
 * Returns true when the publisher-side parallel decoding path was requested.
 */
bool
ParallelDecodeEnabled(void)
{
	return max_parallel_decode_workers > 0;
}

/*
 * The mode of the streaming the subscriber asked for.
 *
 * The subscriber asks for a mode with the "streaming" option of the output
 * plugin: "off", "on" or "parallel".  The option is absent when the
 * subscriber does not want any streaming.
 */
static char
parallel_decode_subscriber_streaming_mode(List *plugin_options)
{
	ListCell   *lc;

	foreach(lc, plugin_options)
	{
		DefElem    *defel = (DefElem *) lfirst(lc);

		if (strcmp(defel->defname, "streaming") != 0)
			continue;

		/* No value means that streaming is wanted. */
		if (defel->arg == NULL)
			return LOGICALREP_STREAM_ON;

		if (IsA(defel->arg, String))
		{
			char	   *value = strVal(defel->arg);

			if (pg_strcasecmp(value, "parallel") == 0)
				return LOGICALREP_STREAM_PARALLEL;
			if (pg_strcasecmp(value, "on") == 0 ||
				pg_strcasecmp(value, "true") == 0 ||
				pg_strcasecmp(value, "1") == 0)
				return LOGICALREP_STREAM_ON;
			if (pg_strcasecmp(value, "off") == 0 ||
				pg_strcasecmp(value, "false") == 0 ||
				pg_strcasecmp(value, "0") == 0)
				return LOGICALREP_STREAM_OFF;
		}
		else if (IsA(defel->arg, Integer))
		{
			if (intVal(defel->arg) == 0)
				return LOGICALREP_STREAM_OFF;

			return LOGICALREP_STREAM_ON;
		}
	}

	return LOGICALREP_STREAM_OFF;
}

/*
 * Resolve the streaming mode of a session.
 *
 * The mode is the combination of the GUC with the mode the subscriber asked
 * for.  A subscriber that did not ask for streaming never gets a stream
 * segment: the output plugin of the publisher decides whether it produces
 * them, and it does so based on the mode the subscriber asked for.
 */
int
ParallelDecodeResolveStreamingMode(List *plugin_options)
{
	char		subscriber_mode;

	subscriber_mode = parallel_decode_subscriber_streaming_mode(plugin_options);

	if (subscriber_mode == LOGICALREP_STREAM_OFF)
		return PD_STREAMING_OFF;

	switch (parallel_decode_streaming_mode)
	{
		case PARALLEL_DECODE_STREAMING_IMMEDIATE:
			return PD_STREAMING_IMMEDIATE;

		case PARALLEL_DECODE_STREAMING_BUFFERED:
			return PD_STREAMING_BUFFERED;

		case PARALLEL_DECODE_STREAMING_AUTO:
		default:
			/*
			 * A subscriber that applies the transactions in parallel needs the
			 * publisher to produce the stream segments early: without them the
			 * parallel apply workers have nothing to do.  A subscriber that
			 * applies the transactions on its own has no parallel apply
			 * worker, so streaming its transactions early would only push
			 * them onto the spool path of the subscriber.
			 */
			if (subscriber_mode == LOGICALREP_STREAM_PARALLEL)
				return PD_STREAMING_IMMEDIATE;

			return PD_STREAMING_BUFFERED;
	}
}

/*
 * Create a shared message queue and store it in the table of contents.
 */
static shm_mq *
parallel_decode_create_mq(shm_toc *toc, int key)
{
	shm_mq	   *mq;

	mq = shm_mq_create(shm_toc_allocate(toc, PARALLEL_DECODE_QUEUE_SIZE),
						PARALLEL_DECODE_QUEUE_SIZE);
	shm_toc_insert(toc, key, mq);

	return mq;
}

/*
 * Register a dynamically started background worker that takes part in the
 * parallel decoding session.
 */
static void
parallel_decode_register_worker(const char *name,
								 const char *type,
								 const char *function,
								 ParallelDecodeWorkerArg *arg,
								 BackgroundWorkerHandle **handle,
								 pid_t *pid)
{
	BackgroundWorker bgw;
	BackgroundWorkerHandle *h = NULL;

	memset(&bgw, 0, sizeof(bgw));

	snprintf(bgw.bgw_name, BGW_MAXLEN, "%s", name);
	snprintf(bgw.bgw_type, BGW_MAXLEN, "%s", type);
	snprintf(bgw.bgw_function_name, BGW_MAXLEN, "%s", function);
	/*
	 * The entry points live in the core "postgres" library, so the postmaster
	 * must look them up in the internal worker table rather than trying to
	 * dlopen() a shared library.
	 */
	snprintf(bgw.bgw_library_name, MAXPGPATH, "%s", "postgres");

	bgw.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	bgw.bgw_start_time = BgWorkerStart_RecoveryFinished;
	bgw.bgw_restart_time = BGW_NEVER_RESTART;
	bgw.bgw_notify_pid = MyProcPid;
	bgw.bgw_main_arg = 0;

	memcpy(bgw.bgw_extra, arg, sizeof(ParallelDecodeWorkerArg));

	if (!RegisterDynamicBackgroundWorker(&bgw, &h))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
				 errmsg("could not register background worker \"%s\"", name),
				 errhint("You might need to increase \"%s\".", "max_worker_processes")));

	if (WaitForBackgroundWorkerStartup(h, pid) != BGWH_STARTED)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("background worker \"%s\" failed to start", name)));

	if (handle)
		*handle = h;
}

/*
 * Wait for the walreader's readiness message on the control channel.
 */
static bool
parallel_decode_wait_for_walreader(void)
{
	instr_time	deadline;
	instr_time	now;

	INSTR_TIME_SET_CURRENT(deadline);

	for (;;)
	{
		shm_mq_result res;
		uint16		type;
		const void *payload;
		size_t		len;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		res = PDQueueRecv(parallel_decode_control_mqh, &type, &payload, &len,
						   true);
		if (res == SHM_MQ_SUCCESS)
		{
			if (type == PD_ENT_ERROR)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the parallel decode walreader failed: %s",
								parallel_decode_ctl ?
								parallel_decode_ctl->walreader_errmsg : "")));
			}

			if (type != PD_ENT_READY || len < sizeof(int64))
			{
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("unexpected message of type %u on the parallel "
								"decoding control channel", type)));
			}
			return true;
		}
		if (res == SHM_MQ_DETACHED)
			return false;

		/* The queue is empty: wait for the walreader to say something. */
		INSTR_TIME_SET_CURRENT(now);
		if (INSTR_TIME_GET_DOUBLE(now) - INSTR_TIME_GET_DOUBLE(deadline) >= 30.0)
			return false;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   100L,
					   PG_WAIT_EXTENSION);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * Register the callback the session uses while it waits for the walreader.
 *
 * The walreader is the only reader of the WAL, so the walsender has to wait
 * for the records to arrive.  The replication connection still has to be
 * serviced while that happens, and only the walsender can do that.
 */
void
ParallelDecodeSetIdleCallback(ParallelDecodeIdleCallback callback)
{
	parallel_decode_idle_cb = callback;
}

/*
 * The WAL position the walreader has reached.
 */
XLogRecPtr
ParallelDecodeWalreaderPosition(void)
{
	if (parallel_decode_ctl == NULL)
		return InvalidXLogRecPtr;

	return (XLogRecPtr) parallel_decode_ctl->walreader_end_lsn;
}

/*
 * Remember a frame of a transaction that is waiting for its turn.
 *
 * relid is the relation the frame defines, or InvalidOid when the frame does
 * not carry the definition of a relation.
 */
static void
parallel_decode_add_pending_frame(uint64 cseq, const char *data, size_t len,
								   XLogRecPtr end_lsn, Oid relid, uint64 epoch)
{
	PDPendingTxn *pending;
	PDQueuedFrame *frame;
	bool		found;

	pending = (PDPendingTxn *) hash_search(parallel_decode_pending, &cseq,
										   HASH_ENTER, &found);
	if (!found)
	{
		pending->cseq = cseq;
		pending->frames = NIL;
		pending->acked = false;
		pending->alloc_time = 0;
		pending->ack_time = 0;
	}

	/*
	 * The frames of a transaction are sent before the report that the
	 * transaction has been decoded, so a frame that arrives after the report
	 * means that the participants of the session disagree.
	 */
	if (pending->acked)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode frame of transaction %llu arrived after "
						"the transaction was reported",
						(unsigned long long) cseq)));

	frame = palloc(sizeof(PDQueuedFrame));
	frame->data = palloc(len > 0 ? len : 1);
	memcpy(frame->data, data, len);
	frame->len = len;
	frame->end_lsn = end_lsn;
	frame->relid = relid;
	frame->epoch = epoch;

	pending->frames = lappend(pending->frames, frame);
}

/*
 * Note that the worker has decoded the transaction with the given commit
 * sequence number.
 *
 * The report arrives after the frames of the transaction, so the transaction
 * is complete when it arrives.
 */
static void
parallel_decode_ack_txn(uint64 cseq)
{
	PDPendingTxn *pending;
	bool		found;

	pending = (PDPendingTxn *) hash_search(parallel_decode_pending, &cseq,
										   HASH_ENTER, &found);
	if (!found)
	{
		pending->cseq = cseq;
		pending->frames = NIL;
		pending->acked = false;
		pending->alloc_time = 0;
		pending->ack_time = 0;
	}

	pending->acked = true;
	pending->ack_time = GetCurrentTimestamp();
}

/*
 * The entry of the table of the relations whose definition has been sent.
 */
typedef struct PDSentRelation
{
	Oid			relid;			/* the relation */
	uint64		epoch;			/* the epoch the definition was sent in */
	bool		sent;			/* has the definition been sent at all */
} PDSentRelation;

/*
 * Decide whether the definition of a relation has to be dropped.
 *
 * The definition is not needed when the subscriber has seen it already.  The
 * transactions reach the subscriber in the order of their commit sequence
 * numbers, so the definition that was sent by the transaction with the
 * smallest sequence number reaches the subscriber before the changes of the
 * transactions that follow it.
 *
 * A definition of an older epoch does not describe the relation anymore: it
 * was produced before the last change of the catalog.  Such a definition is
 * not sent either, because the subscriber has the newer one, which describes
 * the relation as it is now.
 */
static bool
parallel_decode_relation_sent(PDQueuedFrame *frame)
{
	PDSentRelation *entry;
	bool		found;

	entry = (PDSentRelation *) hash_search(parallel_decode_sent_relations,
										   &frame->relid, HASH_ENTER, &found);
	if (!found)
	{
		entry->relid = frame->relid;
		entry->epoch = 0;
		entry->sent = false;
	}

	/*
	 * The subscriber has the definition already.
	 */
	if (entry->sent && entry->epoch >= frame->epoch)
		return true;

	/*
	 * Remember the definition.  A definition of an older epoch does not
	 * replace the one of a newer epoch.
	 */
	if (!entry->sent || frame->epoch > entry->epoch)
	{
		entry->epoch = frame->epoch;
		entry->sent = true;
	}

	return false;
}

/*
 * Let the transactions that have their turn pass.
 *
 * A transaction has its turn when it is the one the walsender is waiting for
 * and the worker that owns it has reported that it has decoded it.  The
 * frames of the transaction are then moved to the list of the frames that may
 * be sent.
 */
static void
parallel_decode_release_txns(void)
{
	for (;;)
	{
		PDPendingTxn *pending;
		ListCell   *lc;
		bool		found;

		pending = (PDPendingTxn *) hash_search(parallel_decode_pending,
											   &parallel_decode_next_cseq,
											   HASH_FIND, &found);
		if (!found || !pending->acked)
			break;

		foreach(lc, pending->frames)
		{
			PDQueuedFrame *frame = (PDQueuedFrame *) lfirst(lc);

			/*
			 * The definition of a relation is sent to the subscriber once.
			 * The transactions reach the subscriber in the order of their
			 * commit sequence numbers, so the definition that the transaction
			 * with the smallest sequence number produces is the one that has
			 * to be sent: the subscriber sees it before the changes of the
			 * transactions that follow it.
			 */
			if (frame->relid != InvalidOid &&
				parallel_decode_relation_sent(frame))
				continue;

			parallel_decode_ready = lappend(parallel_decode_ready, frame);
		}

		list_free(pending->frames);
		hash_search(parallel_decode_pending, &parallel_decode_next_cseq,
					HASH_REMOVE, NULL);

		/*
		 * The transaction has made the round trip from the walreader to the
		 * worker and back.  The time it took is the time the transaction
		 * waited for its turn plus the time it was decoded, which is what the
		 * monitoring views report as the latency of the transaction.
		 */
		if (pending->alloc_time != 0 && pending->ack_time != 0 &&
			parallel_decode_ctl != NULL)
		{
			int64		latency;

			/* the timestamps are in microseconds */
			latency = pending->ack_time - pending->alloc_time;
			if (latency < 0)
				latency = 0;

			parallel_decode_ctl->txn_count++;
			parallel_decode_ctl->txn_latency_us_sum += (uint64) latency;
			if ((uint64) latency > parallel_decode_ctl->txn_latency_us_max)
				parallel_decode_ctl->txn_latency_us_max = (uint64) latency;
		}

		if (parallel_decode_ctl != NULL)
			parallel_decode_ctl->cseq_released = parallel_decode_next_cseq;

		/*
		 * The transaction has been released, so it does not hold the release
		 * frontier back anymore.
		 */
		if (parallel_decode_frontier != NIL)
		{
			PDCseqInfo *info = (PDCseqInfo *) linitial(parallel_decode_frontier);

			if (info->cseq == parallel_decode_next_cseq)
			{
				parallel_decode_frontier =
					list_delete_first(parallel_decode_frontier);
				pfree(info);
			}
		}

		parallel_decode_next_cseq++;
	}
}

/*
 * Remember where the record that ended a transaction is.
 *
 * The walreader is the only reader of the WAL, so it is the one that knows
 * the position.  The walsender needs it to compute the release frontier.
 */
static void
parallel_decode_record_cseq(const PDCseqInfo *info)
{
	PDCseqInfo *entry;
	PDPendingTxn *pending;
	bool		found;

	/*
	 * The walreader hands out the commit sequence numbers in the order in
	 * which the transactions end, so the highest one it reported is the
	 * number of transactions that have ended (11.3).
	 */
	if (parallel_decode_ctl != NULL &&
		info->cseq > parallel_decode_ctl->cseq_allocated)
		parallel_decode_ctl->cseq_allocated = info->cseq;

	/*
	 * The position the walreader has read up to is only usable as the
	 * frontier when every transaction that ended before it has been
	 * released.  The commit sequence number the walreader reported says
	 * which transactions the position covers.
	 */
	if ((XLogRecPtr) info->read_lsn > parallel_decode_certified_lsn)
	{
		parallel_decode_certified_lsn = (XLogRecPtr) info->read_lsn;
		parallel_decode_certified_cseq = info->cseq;
	}

	/*
	 * The transaction has been released already, so it does not hold the
	 * frontier back.  This happens when the report of the transaction was
	 * handled before the message of the walreader was read.
	 */
	if (info->cseq < parallel_decode_next_cseq)
		return;

	/*
	 * Remember when the transaction ended.  The time is the start of the
	 * waiting of the transaction for its turn, which is what the monitoring
	 * views report as the age of the longest pending commit.
	 */
	pending = (PDPendingTxn *) hash_search(parallel_decode_pending,
										   &info->cseq, HASH_ENTER, &found);
	if (!found)
	{
		pending->cseq = info->cseq;
		pending->frames = NIL;
		pending->acked = false;
		pending->alloc_time = 0;
		pending->ack_time = 0;
	}
	if (pending->alloc_time == 0)
		pending->alloc_time = GetCurrentTimestamp();

	entry = palloc(sizeof(PDCseqInfo));
	memcpy(entry, info, sizeof(PDCseqInfo));

	parallel_decode_frontier = lappend(parallel_decode_frontier, entry);
}

/*
 * Read the messages the walreader has sent on the control channel.
 *
 * The walreader tells the walsender where the transactions that have ended
 * are, which is what the release frontier is computed from.  The messages
 * have to be read before the frames of the workers are handled: the frames
 * of a stream segment may have to be limited to the frontier, and the
 * frontier is only known once the mapping of the commit sequence numbers is
 * up to date.
 */
static void
parallel_decode_drain_control(void)
{
	if (parallel_decode_control_mqh == NULL)
		return;

	for (;;)
	{
		uint16		type;
		const void *payload;
		size_t		len;
		shm_mq_result res;

		res = PDQueueRecv(parallel_decode_control_mqh, &type, &payload, &len,
						   true);
		if (res != SHM_MQ_SUCCESS)
			return;

		if (type == PD_ENT_CSEQ)
		{
			PDCseqInfo	info;

			if (len < sizeof(info))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("parallel decode commit sequence message is too short "
								"(%zu bytes)", (size_t) len)));

			memcpy(&info, payload, sizeof(info));

			parallel_decode_record_cseq(&info);
		}
		else if (type == PD_ENT_WATERMARK)
		{
			PDWatermark	wm;

			if (len < sizeof(wm))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("parallel decode watermark message is too short "
								"(%zu bytes)", (size_t) len)));

			memcpy(&wm, payload, sizeof(wm));

			/*
			 * The walreader computed the watermarks of the slot: it is the
			 * only process that sees every transaction of the session.  The
			 * walsender owns the slot, so it is the one that may advance
			 * them.
			 *
			 * The watermarks may only be advanced once the subscriber has
			 * confirmed the position they are valid at.  The functions of
			 * the decoding path take care of that: they remember the
			 * values as candidates, and a candidate is applied when the
			 * confirmation of the subscriber reaches the position it was
			 * computed at.
			 */
			if (TransactionIdIsValid(wm.xmin))
				LogicalIncreaseXminForSlot((XLogRecPtr) wm.read_lsn,
										   wm.xmin);

			if (wm.restart_lsn != 0)
				LogicalIncreaseRestartDecodingForSlot((XLogRecPtr) wm.read_lsn,
										  (XLogRecPtr) wm.restart_lsn);
		}
		else if (type == PD_ENT_ERROR)
		{
			PDErrorHeader eh;

			if (len < sizeof(eh))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("parallel decode error message is too short (%zu bytes)",
								(size_t) len)));

			memcpy(&eh, payload, sizeof(eh));

			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the parallel decode walreader failed: %s",
							parallel_decode_ctl ?
							parallel_decode_ctl->walreader_errmsg : ""),
					 errdetail("The walreader failed at WAL position %X/%X.",
							   LSN_FORMAT_ARGS((XLogRecPtr) eh.lsn))));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected message of type %u on the parallel decoding "
							"control channel", type)));
	}
}

/*
 * The release frontier of the session.
 *
 * The frontier is the position of the record that ended the first transaction
 * that has not been released to the subscriber yet.  No frame the walsender
 * writes may carry a position that is beyond the frontier: the subscriber
 * uses the position of the frames it receives to advance the flush position
 * of the slot, and a position beyond the frontier would make the subscriber
 * skip the transactions that have not been delivered yet.
 */
XLogRecPtr
ParallelDecodeFrontier(void)
{
	if (parallel_decode_frontier != NIL)
	{
		PDCseqInfo *info = (PDCseqInfo *) linitial(parallel_decode_frontier);

		return (XLogRecPtr) info->end_lsn;
	}

	/*
	 * Every transaction the walreader has reported has been released, so the
	 * position the walreader has read up to is the frontier.  The walreader
	 * reports the position together with the commit sequence number it had
	 * given out at that point, so the position covers exactly the
	 * transactions that have been released.
	 */
	if (!XLogRecPtrIsInvalid(parallel_decode_certified_lsn))
		return parallel_decode_certified_lsn;

	/*
	 * The walreader has not reported anything yet, so the only position that
	 * is known to be safe is the one the session started at: every
	 * transaction the session decodes ends after it.
	 */
	if (parallel_decode_ctl != NULL)
		return (XLogRecPtr) parallel_decode_ctl->start_lsn;

	return InvalidXLogRecPtr;
}

/*
 * Limit a position to the release frontier.
 */
XLogRecPtr
ParallelDecodeClampLSN(XLogRecPtr lsn)
{
	XLogRecPtr	frontier;

	/* There is nothing to limit. */
	if (XLogRecPtrIsInvalid(lsn))
		return lsn;

	frontier = ParallelDecodeFrontier();

	/*
	 * The frontier is not known yet, so there is nothing to limit the position
	 * to.
	 */
	if (XLogRecPtrIsInvalid(frontier))
		return lsn;

	if (lsn > frontier)
		return frontier;

	return lsn;
}

/*
 * Limit the positions of a frame to the release frontier.
 *
 * The frame is the 'w' frame of the replication protocol, whose header holds
 * two positions.  The positions of the frames of a transaction that has been
 * released are always within the frontier, so this only ever changes the
 * frames of a stream segment.
 */
void
ParallelDecodeClampFrame(StringInfo frame)
{
	XLogRecPtr	frontier;
	uint64		network;
	int64		lsn;

	if (frame->len < 1 + 2 * sizeof(int64))
		return;

	frontier = ParallelDecodeFrontier();

	/*
	 * The frontier is not known yet, so there is nothing to limit the
	 * positions of the frame to.
	 */
	if (XLogRecPtrIsInvalid(frontier))
		return;

	/*
	 * The header of the frame holds the position of the record the frame
	 * belongs to, twice.  The positions are sent in the network byte order,
	 * and the header is not aligned, so they are read and written with
	 * memcpy().
	 */
	memcpy(&network, frame->data + 1, sizeof(network));
	lsn = (int64) pg_ntoh64(network);
	if (lsn != 0 && (XLogRecPtr) lsn > frontier)
	{
		network = pg_hton64((uint64) frontier);
		memcpy(frame->data + 1, &network, sizeof(network));
	}

	memcpy(&network, frame->data + 1 + sizeof(uint64), sizeof(network));
	lsn = (int64) pg_ntoh64(network);
	if (lsn != 0 && (XLogRecPtr) lsn > frontier)
	{
		network = pg_hton64((uint64) frontier);
		memcpy(frame->data + 1 + sizeof(uint64), &network, sizeof(network));
	}
}

/*
 * Take the next frame that may be sent.
 *
 * Returns false when there is no such frame at the moment.
 */
static bool
parallel_decode_take_ready_frame(StringInfo buf, XLogRecPtr *end_lsn)
{
	PDQueuedFrame *frame;
	ListCell   *lc;

	if (parallel_decode_ready == NIL)
		return false;

	lc = list_head(parallel_decode_ready);
	frame = (PDQueuedFrame *) lfirst(lc);
	parallel_decode_ready = list_delete_first(parallel_decode_ready);

	resetStringInfo(buf);
	appendBinaryStringInfo(buf, frame->data, frame->len);
	*end_lsn = frame->end_lsn;

	if (*end_lsn > parallel_decode_position)
		parallel_decode_position = *end_lsn;

	pfree(frame->data);
	pfree(frame);

	return true;
}

/*
 * Note that a worker has made progress.
 *
 * The position is the position the worker has decoded up to.  It tells the
 * walsender whether the worker still has records to decode, and the time of
 * the message tells it whether the worker is still working on them.
 */
static void
parallel_decode_note_worker_progress(int worker, XLogRecPtr position)
{
	if (worker < 0 || worker >= parallel_decode_num_workers)
		return;

	if (position > parallel_decode_worker_position[worker])
		parallel_decode_worker_position[worker] = position;

	INSTR_TIME_SET_CURRENT(parallel_decode_worker_progress[worker]);
}

/*
 * Handle a message of one of the decode workers.
 *
 * The payload belongs to the caller.
 */
static void
parallel_decode_handle_message(int worker, uint16 type, const void *payload,
								size_t len)
{
	switch (type)
	{
		case PD_ENT_POSITION:
			{
				uint64		position;

				if (len < sizeof(position))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode position message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&position, payload, sizeof(position));

				if ((XLogRecPtr) position > parallel_decode_position)
					parallel_decode_position = (XLogRecPtr) position;

				parallel_decode_note_worker_progress(worker,
														(XLogRecPtr) position);

				break;
			}

		case PD_ENT_EVENT:
			{
				PDEventHeader eh;

				if (len < sizeof(eh))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode event message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&eh, payload, sizeof(eh));

				if (eh.event != PD_EVENT_END_ACK)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("unexpected parallel decode event %u", eh.event)));

				/*
				 * The transaction has been decoded, so it may pass as soon as it
				 * is its turn.
				 */
				parallel_decode_ack_txn(eh.cseq);
				parallel_decode_release_txns();

				break;
			}

		case PD_ENT_FRAME:
			{
				PDFrameHeader fh;

				if (len < sizeof(fh))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode frame message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&fh, payload, sizeof(fh));

				if ((size_t) fh.length + sizeof(fh) != len)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode frame message has an inconsistent length "
									"(%u declared, %zu received)",
									fh.length, (size_t) len - sizeof(fh))));

				parallel_decode_note_worker_progress(worker,
														(XLogRecPtr) fh.end_lsn);

				if (fh.cseq == 0)
				{
					/*
					 * The frame is not part of a transaction, which happens for
					 * the fragments of a streamed transaction.  There is no
					 * commit sequence number to order it by, so it is sent as
					 * soon as it arrives.
					 */
					PDQueuedFrame *frame;

					frame = palloc(sizeof(PDQueuedFrame));
					frame->data = palloc(fh.length > 0 ? fh.length : 1);
					memcpy(frame->data, (const char *) payload + sizeof(fh),
						   fh.length);
					frame->len = fh.length;
					frame->end_lsn = (XLogRecPtr) fh.end_lsn;
					frame->relid = InvalidOid;
					frame->epoch = 0;

					parallel_decode_ready = lappend(parallel_decode_ready, frame);
				}
				else
					parallel_decode_add_pending_frame(fh.cseq,
													 (const char *) payload + sizeof(fh),
													 fh.length,
													 (XLogRecPtr) fh.end_lsn,
													 InvalidOid, 0);

				break;
			}

		case PD_ENT_RELATION:
			{
				PDRelationHeader rh;

				if (len < sizeof(rh))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode relation message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&rh, payload, sizeof(rh));

				if ((size_t) rh.length + sizeof(rh) != len)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode relation message has an inconsistent length "
									"(%u declared, %zu received)",
									rh.length, (size_t) len - sizeof(rh))));

				parallel_decode_note_worker_progress(worker,
														(XLogRecPtr) rh.end_lsn);

				/*
				 * The frame carries the definition of a relation.  It belongs to
				 * a transaction, so it waits for the turn of the transaction
				 * like the other frames do.
				 */
				if (rh.cseq == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode relation message does not belong to "
									"a transaction")));

				parallel_decode_add_pending_frame(rh.cseq,
												 (const char *) payload + sizeof(rh),
												 rh.length,
												 (XLogRecPtr) rh.end_lsn,
												 (Oid) rh.relid, rh.epoch);

				break;
			}

		case PD_ENT_SEGMENT:
			{
				PDSegmentHeader sh;
				const char *data;
				size_t		remaining;
				uint32		i;

				if (len < sizeof(sh))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode segment message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&sh, payload, sizeof(sh));

				parallel_decode_note_worker_progress(worker,
														(XLogRecPtr) sh.end_lsn);

				data = (const char *) payload + sizeof(sh);
				remaining = len - sizeof(sh);

				/*
				 * The frames of the segment are appended one after the other,
				 * so that they reach the subscriber without the bytes of
				 * another transaction in between.
				 */
				for (i = 0; i < sh.nframes; i++)
				{
					uint32		frame_len;
					PDQueuedFrame *frame;

					if (remaining < sizeof(uint32))
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("parallel decode segment message is truncated")));

					memcpy(&frame_len, data, sizeof(uint32));
					data += sizeof(uint32);
					remaining -= sizeof(uint32);

					if (frame_len > remaining)
						ereport(ERROR,
								(errcode(ERRCODE_DATA_CORRUPTED),
								 errmsg("parallel decode segment message is truncated")));

					frame = palloc(sizeof(PDQueuedFrame));
					frame->data = palloc(frame_len > 0 ? frame_len : 1);
					memcpy(frame->data, data, frame_len);
					frame->len = frame_len;
					frame->end_lsn = (XLogRecPtr) sh.end_lsn;
					frame->relid = InvalidOid;
					frame->epoch = 0;

					parallel_decode_ready = lappend(parallel_decode_ready, frame);

					data += frame_len;
					remaining -= frame_len;
				}

				break;
			}

		case PD_ENT_ERROR:
			{
				PDErrorHeader eh;
				const char *message;

				if (len < sizeof(eh))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode error message is too short (%zu bytes)",
									(size_t) len)));

				memcpy(&eh, payload, sizeof(eh));

				/*
				 * The text of the error is in the control block: the worker is
				 * gone by the time the report is read, so it cannot be taken
				 * from the message itself.
				 */
				if (eh.worker >= 0 && eh.worker < parallel_decode_num_workers)
					message = parallel_decode_ctl->workers[eh.worker].errmsg;
				else
					message = "";

				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the parallel decode worker failed: %s",
								message),
						 errdetail("Decode worker %d failed at WAL position %X/%X.",
								   eh.worker,
								   LSN_FORMAT_ARGS((XLogRecPtr) eh.lsn))));
			}

			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected message of type %u on the parallel decode "
							"output channel", type)));
	}
}

/*
 * Report the failure of a decode worker.
 *
 * A worker that fails puts the reason of the failure into the control block
 * before it exits, so that the reason is still there when the walsender
 * notices that the worker is gone.  A worker that dies without saying anything
 * leaves the status word alone, and the session ends with the generic
 * message.
 */
static void
parallel_decode_report_worker_failure(int worker)
{
	const char *message;

	/*
	 * The worker wrote the message before it published the status, so the
	 * message is there when the status says that the worker failed.  The
	 * barrier makes sure that the write is visible before the status is read.
	 */
	pg_memory_barrier();

	if (parallel_decode_ctl->workers[worker].status != PD_WORKER_STATUS_FAILED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the parallel decode worker died unexpectedly"),
				 errdetail("Decode worker %d is gone.", worker)));
	}

	message = parallel_decode_ctl->workers[worker].errmsg;

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("the parallel decode worker failed: %s", message),
			 errdetail("Decode worker %d failed at WAL position %X/%X.",
					   worker,
					   LSN_FORMAT_ARGS((XLogRecPtr)
										parallel_decode_ctl->workers[worker].error_lsn))));
}

/*
 * Check that the session is still making progress.
 *
 * The walsender holds the frames of a transaction back until the transaction
 * has its turn, so it waits for the worker that owns the transaction.  A
 * worker that dies is noticed by the liveness check, but a worker that is
 * stuck is not: the session would wait for it forever, and the replication
 * would look like it had simply caught up.
 *
 * The timeout is the backstop for that case.  It only applies to a worker
 * that still has records to decode: a worker that has decoded everything the
 * walreader has read is waiting for new WAL, which is not a stall.
 */
static void
parallel_decode_check_stall(void)
{
	instr_time	now;
	XLogRecPtr	walreader_pos;
	int			worker;

	if (parallel_decode_barrier_timeout <= 0)
		return;

	/*
	 * The session is not waiting for a transaction to be released, so there is
	 * nothing that could be stuck.
	 */
	if (parallel_decode_pending == NULL)
		return;

	walreader_pos = (XLogRecPtr) parallel_decode_ctl->walreader_end_lsn;

	INSTR_TIME_SET_CURRENT(now);

	for (worker = 0; worker < parallel_decode_num_workers; worker++)
	{
		double		idle;

		/*
		 * The worker has decoded everything the walreader has read, so it is
		 * waiting for new WAL rather than working on something.
		 */
		if (parallel_decode_worker_position[worker] >= walreader_pos)
			continue;

		idle = INSTR_TIME_GET_DOUBLE(now) -
			INSTR_TIME_GET_DOUBLE(parallel_decode_worker_progress[worker]);

		if (idle < parallel_decode_barrier_timeout)
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("the parallel decode session stalled"),
				 errdetail("Decode worker %d made no progress for %.0f seconds.",
						   worker, idle),
				 errhint("The session was waiting for a transaction to be released "
						 "to the subscriber.")));
	}
}

/*
 * Wait for the next frame the decode workers produced.
 *
 * The frame is copied into buf, so that the caller can patch the send time
 * into it and write it out to the network.
 *
 * The frames of a transaction are held back until the transaction has its
 * turn, so that the transactions reach the subscriber in the order they were
 * committed in.
 *
 * Returns false when no frame arrived within the wait interval.  That is not
 * an error: the caller returns to its main loop and comes back later.  The
 * function also returns false when the session is winding down.
 */
bool
ParallelDecodeNextFrame(StringInfo buf, XLogRecPtr *end_lsn)
{
	ParallelDecodeControl *ctl = parallel_decode_ctl;
	int			worker;

	Assert(parallel_decode_num_workers > 0);

	for (;;)
	{
		uint16		type;
		const void *payload;
		size_t		len;
		shm_mq_result res;
		int			rc;
		bool		got_message = false;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Read what the walreader has to say before anything else: the
		 * release frontier is computed from the positions the walreader
		 * reports, and the frames that are written may have to be limited to
		 * it.
		 */
		parallel_decode_drain_control();

		/*
		 * A frame that has its turn is sent first.  The frames of a
		 * transaction that does not have its turn yet are still waiting.
		 */
		if (parallel_decode_take_ready_frame(buf, end_lsn))
			return true;

		/*
		 * The session may be winding down: the walsender that started it may
		 * be gone, or it may have asked the participants to stop.  In both
		 * cases there is nothing left to wait for.
		 */
		if (ctl == NULL || ctl->shutdown)
			return false;
		if (!ParallelDecodeProcessAlive(ctl->walsender_pid))
			return false;

		/*
		 * Clear the latch before looking at the queues.  A frame that arrives
		 * after this point sets the latch again, so the wait below returns
		 * immediately; a frame that arrived before is found by the receive.
		 */
		ResetLatch(MyLatch);

		/*
		 * Look at the queues of all the workers: every worker produces the
		 * frames of the transactions it owns.
		 */
		for (worker = 0; worker < parallel_decode_num_workers; worker++)
		{
			res = PDChunkReceiverRecv(&parallel_decode_chunk_rcv[worker],
									   parallel_decode_out_mqh[worker], &type,
									   &payload, &len, true);
			if (res == SHM_MQ_SUCCESS)
			{
				parallel_decode_handle_message(worker, type, payload, len);
				pfree((void *) payload);
				got_message = true;
				break;
			}
			if (res == SHM_MQ_DETACHED)
			{
				/*
				 * The worker is gone, so the frames it was going to produce
				 * will never arrive.  When the session is winding down, that
				 * is what happens; otherwise the session cannot continue.
				 */
				if (!ParallelDecodeProcessAlive(parallel_decode_worker_pids[worker]))
				{
					if (ctl->shutdown)
						return false;

					parallel_decode_report_worker_failure(worker);
				}
			}
		}

		if (got_message)
			continue;

		/*
		 * No frame yet.  Check that the participants of the session are still
		 * there: without them no new frame can arrive.
		 *
		 * The walreader is the only reader of the WAL, so without it no new
		 * record can arrive and the session cannot continue.
		 */
		if (!ParallelDecodeProcessAlive(ctl->walreader_pid))
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("the parallel decode walreader died unexpectedly")));

		for (worker = 0; worker < parallel_decode_num_workers; worker++)
		{
			/*
			 * A worker that has not claimed its slot yet is still starting
			 * up.  It is not dead, so the session has to wait for it.
			 */
			if (ctl->workers[worker].status == PD_WORKER_STATUS_UNUSED)
			{
				if (!ParallelDecodeProcessAlive(parallel_decode_worker_pids[worker]))
					ereport(ERROR,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("the parallel decode worker died unexpectedly"),
							 errdetail("Decode worker %d died before it joined the session.",
									   worker)));
				continue;
			}

			if (!ParallelDecodeProcessAlive(parallel_decode_worker_pids[worker]))
				parallel_decode_report_worker_failure(worker);
		}

		/*
		 * All the participants are alive, but one of them may be stuck.  The
		 * check is the backstop for that case: without it the session would wait
		 * for a transaction that is never going to be released.
		 */
		parallel_decode_check_stall();

		/*
		 * There is nothing to do but wait for the decode workers.  The callback
		 * takes care of the replication connection, which would otherwise not
		 * be serviced at all while we wait.  It also tells us when the client
		 * has asked to stop the streaming: the session has to end then, even
		 * when no further frame is going to arrive.
		 */
		if (parallel_decode_idle_cb && parallel_decode_idle_cb())
			return false;

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   100L,
					   PG_WAIT_EXTENSION);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * The number of messages the non-blocking frame fetch reads at most.
 *
 * The fetch reads the messages of the decode workers until it finds a frame
 * or until the workers have nothing to say.  The limit keeps the fetch from
 * running for a long time when a worker keeps sending progress messages: the
 * frames that arrive meanwhile are taken by the next call anyway.
 */
#define PD_MAX_NOWAIT_MESSAGES	64

/*
 * Take the next frame that is ready to be sent, without waiting for one.
 *
 * The frame is copied into buf, in the same way ParallelDecodeNextFrame()
 * does it.  The function returns false when there is no frame at the moment,
 * which is not an error: the caller simply has nothing to write.
 *
 * The function is used to fill a batch of frames.  The first frame of a batch
 * is waited for, the frames that follow it are taken as they are there, so
 * that the batch can be written out to the client in one go.
 */
bool
ParallelDecodeNextFrameNoWait(StringInfo buf, XLogRecPtr *end_lsn)
{
	ParallelDecodeControl *ctl = parallel_decode_ctl;
	int			worker;
	int			messages = 0;

	for (;;)
	{
		uint16		type;
		const void *payload;
		size_t		len;
		shm_mq_result res;
		bool		got_message = false;

		CHECK_FOR_INTERRUPTS();

		/*
		 * Read what the walreader has to say before anything else: the
		 * release frontier is computed from the positions the walreader
		 * reports, and the frames that are written may have to be limited
		 * to it.
		 */
		parallel_decode_drain_control();

		/*
		 * A frame that has its turn is sent first.  The frames of a
		 * transaction that does not have its turn yet are still waiting.
		 */
		if (parallel_decode_take_ready_frame(buf, end_lsn))
			return true;

		/*
		 * The session may be winding down: the walsender that started it may
		 * be gone, or it may have asked the participants to stop.  In both
		 * cases there is nothing left to wait for.
		 */
		if (ctl == NULL || ctl->shutdown)
			return false;

		/*
		 * Look at the queues of all the workers.  The receive does not wait,
		 * so the function returns as soon as the workers have nothing to
		 * say.
		 */
		for (worker = 0; worker < parallel_decode_num_workers; worker++)
		{
			res = PDChunkReceiverRecv(&parallel_decode_chunk_rcv[worker],
									   parallel_decode_out_mqh[worker], &type,
									   &payload, &len, true);
			if (res == SHM_MQ_SUCCESS)
			{
				parallel_decode_handle_message(worker, type, payload, len);
				pfree((void *) payload);
				got_message = true;
				break;
			}
			if (res == SHM_MQ_DETACHED)
			{
				/*
				 * The worker is gone, so the frames it was going to produce
				 * will never arrive.  When the session is winding down, that
				 * is what happens; otherwise the session cannot continue.
				 */
				if (!ParallelDecodeProcessAlive(parallel_decode_worker_pids[worker]))
				{
					if (ctl->shutdown)
						return false;

					parallel_decode_report_worker_failure(worker);
				}
			}
		}

		/*
		 * The workers have nothing to say, so there is no frame at the
		 * moment.
		 */
		if (!got_message)
			return false;

		/*
		 * Do not read an unbounded number of messages.  The frames the
		 * messages carry are taken by the next call, so nothing is lost.
		 */
		if (++messages >= PD_MAX_NOWAIT_MESSAGES)
			return false;
	}
}

/*
 * The WAL position the decoding has come to.
 */
XLogRecPtr
ParallelDecodePosition(void)
{
	return parallel_decode_position;
}

/*
 * The time between two publications of the statistics of the session.
 *
 * The statistics are read by the SQL functions of the monitoring views, and
 * the walsender publishes them from its main loop.  Publishing them for every
 * frame would take the spinlock of the walsenders for every batch of frames,
 * so they are published at a fixed rate instead.
 */
#define PD_STATS_PUBLISH_INTERVAL	100	/* milliseconds */

/*
 * When the statistics of the session were published the last time.
 */
static TimestampTz parallel_decode_stats_published = 0;

/*
 * Note that the session has written a frame of the given length to the
 * subscriber.
 */
void
ParallelDecodeCountOutput(size_t bytes)
{
	if (parallel_decode_ctl == NULL)
		return;

	parallel_decode_ctl->frames++;
	parallel_decode_ctl->frame_bytes += bytes;
}

/*
 * Note that the session has flushed its output buffer to the network.
 *
 * The counter is the number of the flushes, not the number of the write
 * calls: a flush that finds nothing to write does not reach the network.
 */
void
ParallelDecodeCountSocketWrite(void)
{
	if (parallel_decode_ctl == NULL)
		return;

	parallel_decode_ctl->socket_writes++;
}

/*
 * Publish the statistics of the session for the monitoring views (11.3).
 *
 * The counters of the session live in the shared control block, which only
 * the participants of the session can see.  They are copied into the shared
 * memory of the walsenders, where the SQL functions read them.
 */
void
ParallelDecodePublishStats(bool force)
{
	ParallelDecodeStats *stats;
	PDPendingTxn *pending;
	TimestampTz now;
	bool		found;
	int			i;

	if (parallel_decode_ctl == NULL || MyWalSnd == NULL)
		return;

	now = GetCurrentTimestamp();

	if (!force && parallel_decode_stats_published != 0)
	{
		long		elapsed;

		elapsed = (long) (now - parallel_decode_stats_published) / 1000;
		if (elapsed < PD_STATS_PUBLISH_INTERVAL)
			return;
	}
	parallel_decode_stats_published = now;

	/*
	 * The transaction that has been waiting for its turn the longest is the
	 * one at the head of the queue of the pending transactions.
	 */
	pending = (PDPendingTxn *) hash_search(parallel_decode_pending,
										   &parallel_decode_next_cseq,
										   HASH_FIND, &found);

	SpinLockAcquire(&MyWalSnd->mutex);

	stats = &MyWalSnd->pd_stats;
	stats->active = true;
	stats->num_workers = parallel_decode_ctl->num_workers;
	if (stats->start_time == 0)
		stats->start_time = now;
	stats->cseq_allocated = parallel_decode_ctl->cseq_allocated;
	stats->cseq_released = parallel_decode_ctl->cseq_released;
	stats->pending_commit_age_us = (found && pending->alloc_time != 0) ?
		(uint64) Max(now - pending->alloc_time, 0) : 0;
	stats->txn_count = parallel_decode_ctl->txn_count;
	stats->txn_latency_us_sum = parallel_decode_ctl->txn_latency_us_sum;
	stats->txn_latency_us_max = parallel_decode_ctl->txn_latency_us_max;
	stats->frames = parallel_decode_ctl->frames;
	stats->frame_bytes = parallel_decode_ctl->frame_bytes;
	stats->socket_writes = parallel_decode_ctl->socket_writes;
	stats->wakeups_down = parallel_decode_ctl->walreader_wakeups;
	stats->wakeups_up = 0;

	for (i = 0; i < parallel_decode_ctl->num_workers; i++)
	{
		ParallelDecodeWorkerStats *wstats = &stats->workers[i];
		ParallelDecodeWorkerSlot *slot = &parallel_decode_ctl->workers[i];

		wstats->pid = slot->pid;
		wstats->status = slot->status;
		wstats->in_flight_txns = slot->in_flight_txns;
		wstats->decoded_txns = slot->decoded_txns;
		wstats->in_flight_bytes = slot->in_flight_bytes;
		/*
		 * The queue of the worker holds the records the walreader has handed
		 * over and the worker has not decoded yet.
		 */
		wstats->queue_depth = (slot->records_sent >= slot->records_consumed) ?
			slot->records_sent - slot->records_consumed : 0;
		wstats->wakeups = slot->wakeups;
		stats->wakeups_up += slot->wakeups;
	}

	SpinLockRelease(&MyWalSnd->mutex);
}

/*
 * Create the shared segment, the control block and the message queues, and
 * start the walreader and the decode workers.
 */
void
ParallelDecodeSessionStart(ReplicationSlot *slot,
							const char *plugin_options,
							uint32 plugin_options_len,
							int streaming_mode,
							XLogRecPtr start_lsn)
{
	shm_toc_estimator e;
	size_t		segsize;
	ParallelDecodeWorkerArg arg;
	shm_mq	   *control_mq;
	pid_t		pid;
	int			i;

	Assert(!parallel_decode_session_active);
	Assert(ParallelDecodeEnabled());
	Assert(slot != NULL);

	if (max_parallel_decode_workers > MAX_PARALLEL_DECODE_WORKERS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_parallel_decode_workers must be at most %d",
						MAX_PARALLEL_DECODE_WORKERS)));

	/*
	 * The frames of the transactions that are decoded by the workers have to
	 * reach the subscriber in the order the transactions were committed in,
	 * which is what the commit barrier is for.
	 */
	parallel_decode_num_workers = max_parallel_decode_workers;

	/*
	 * Estimate the size of the shared segment: the control block, the control
	 * channel and one message queue per direction and per worker.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelDecodeControl));
	shm_toc_estimate_chunk(&e, (size_t) PARALLEL_DECODE_QUEUE_SIZE *
						   (1 + 2 * parallel_decode_num_workers));
	shm_toc_estimate_keys(&e, 4 + 2 * parallel_decode_num_workers);
	segsize = shm_toc_estimate(&e);

	parallel_decode_seg = dsm_create(segsize, 0);
	if (!parallel_decode_seg)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not create shared memory segment for parallel decoding")));

	parallel_decode_toc = shm_toc_create(PARALLEL_DECODE_SHM_MAGIC,
										 dsm_segment_address(parallel_decode_seg),
										 segsize);

	/* Control block. */
	parallel_decode_ctl = shm_toc_allocate(parallel_decode_toc,
										   sizeof(ParallelDecodeControl));
	memset(parallel_decode_ctl, 0, sizeof(ParallelDecodeControl));
	shm_toc_insert(parallel_decode_toc, PARALLEL_DECODE_KEY_SHARED,
				   parallel_decode_ctl);

	parallel_decode_ctl->walsender_pid = MyProcPid;
	parallel_decode_ctl->num_workers = parallel_decode_num_workers;

	/*
	 * The statistics of the session start from scratch: the slot of the
	 * walsender may have been used by another session before.
	 */
	if (MyWalSnd != NULL)
	{
		SpinLockAcquire(&MyWalSnd->mutex);
		MemSet(&MyWalSnd->pd_stats, 0, sizeof(MyWalSnd->pd_stats));
		SpinLockRelease(&MyWalSnd->mutex);
	}
	parallel_decode_stats_published = 0;
	parallel_decode_ctl->streaming_mode = streaming_mode;
	parallel_decode_ctl->start_lsn = start_lsn;
	parallel_decode_ctl->database = slot->data.database;
	parallel_decode_ctl->two_phase = slot->data.two_phase;
	parallel_decode_ctl->two_phase_at = slot->data.two_phase_at;
	strlcpy(parallel_decode_ctl->slot_name, NameStr(slot->data.name),
			NAMEDATALEN);
	strlcpy(parallel_decode_ctl->plugin_name, NameStr(slot->data.plugin),
			NAMEDATALEN);
	if (plugin_options_len > sizeof(parallel_decode_ctl->plugin_options))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("the serialized output plugin options do not fit into %zu bytes",
						sizeof(parallel_decode_ctl->plugin_options))));
	memcpy(parallel_decode_ctl->plugin_options, plugin_options, plugin_options_len);
	parallel_decode_ctl->plugin_options_len = plugin_options_len;

	/*
	 * The size of the stream segments the workers produce, and the smallest
	 * size a transaction may have before it is streamed at all.
	 *
	 * A session that does not stream leaves both at zero: the changes of a
	 * transaction are then only streamed when the decoding path runs out of
	 * memory, which is the behavior of the serial path.
	 */
	if (streaming_mode == PD_STREAMING_IMMEDIATE)
	{
		parallel_decode_ctl->stream_segment_size = parallel_decode_stream_segment_size;
		parallel_decode_ctl->stream_min_size = parallel_decode_stream_min_size;
	}
	else if (streaming_mode == PD_STREAMING_BUFFERED)
	{
		/*
		 * The session streams the changes of a transaction when the worker
		 * runs out of memory, like the serial path does.  The size of the
		 * stream segments is then not limited, so the segment size is zero.
		 */
		parallel_decode_ctl->stream_segment_size = 0;
		parallel_decode_ctl->stream_min_size = 0;
	}
	else
	{
		parallel_decode_ctl->stream_segment_size = 0;
		parallel_decode_ctl->stream_min_size = 0;
	}

	/*
	 * The amount of memory each worker may use for the changes it has
	 * accumulated.
	 */
	parallel_decode_ctl->work_mem_kb = parallel_decode_work_mem;

	/*
	 * Control channel, walreader -> walsender.  The walreader sets itself as
	 * the sender once it has attached; the walsender is the receiver.
	 */
	control_mq = parallel_decode_create_mq(parallel_decode_toc,
										   PARALLEL_DECODE_KEY_CONTROL_MQ);
	shm_mq_set_receiver(control_mq, MyProc);
	parallel_decode_control_mqh =
		shm_mq_attach(control_mq, parallel_decode_seg, NULL);

	/*
	 * Walreader -> worker queues.  The walreader is the sender, the workers
	 * are the receivers; neither identity is known yet, so both sides set
	 * their own after attaching.
	 */
	for (i = 0; i < parallel_decode_num_workers; i++)
		parallel_decode_create_mq(parallel_decode_toc,
								  PARALLEL_DECODE_KEY_WAL_MQ_BASE + i);

	/*
	 * Worker -> walsender queues.  The walsender is the receiver.
	 */
	for (i = 0; i < parallel_decode_num_workers; i++)
	{
		shm_mq	   *mq;

		mq = parallel_decode_create_mq(parallel_decode_toc,
									   PARALLEL_DECODE_KEY_OUT_MQ_BASE + i);
		shm_mq_set_receiver(mq, MyProc);
		parallel_decode_out_mqh[i] = shm_mq_attach(mq, parallel_decode_seg,
												   NULL);
	}

	for (i = 0; i < parallel_decode_num_workers; i++)
		PDChunkReceiverInit(&parallel_decode_chunk_rcv[i]);
	parallel_decode_position = start_lsn;

	/*
	 * The workers start out at the position the session starts at, and they
	 * have made no progress yet.  The stall detection uses both values, so
	 * they have to be initialized before the first check.
	 */
	for (i = 0; i < parallel_decode_num_workers; i++)
	{
		parallel_decode_worker_position[i] = start_lsn;
		INSTR_TIME_SET_CURRENT(parallel_decode_worker_progress[i]);
	}

	/*
	 * Create the table of the transactions whose frames are waiting for their
	 * turn, and reset the state of the commit barrier.
	 */
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(uint64);
		hash_ctl.entrysize = sizeof(PDPendingTxn);
		hash_ctl.hcxt = CurrentMemoryContext;
		parallel_decode_pending = hash_create("Parallel decode pending transactions",
											  128, &hash_ctl,
											  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	/*
	 * The table of the relations whose definition the session has sent to the
	 * subscriber.  Only the walsender writes to it, so it needs no lock.
	 */
	{
		HASHCTL		hash_ctl;

		memset(&hash_ctl, 0, sizeof(hash_ctl));
		hash_ctl.keysize = sizeof(Oid);
		hash_ctl.entrysize = sizeof(PDSentRelation);
		hash_ctl.hcxt = CurrentMemoryContext;
		parallel_decode_sent_relations = hash_create("Parallel decode sent relations",
													  128, &hash_ctl,
													  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}
	parallel_decode_next_cseq = 1;
	parallel_decode_ready = NIL;

	/* Start the walreader. */
	arg.dsm_handle = (int) dsm_segment_handle(parallel_decode_seg);
	arg.slot_index = -1;
	arg.db_oid = MyDatabaseId;
	arg.start_lsn = slot->data.restart_lsn;
	parallel_decode_register_worker("parallel decode walreader",
									"parallel decode walreader",
									"ParallelWalreaderMain",
									&arg,
									&parallel_decode_walreader_handle,
									&pid);
	parallel_decode_ctl->walreader_pid = pid;

	/* Start the decode workers. */
	for (i = 0; i < parallel_decode_num_workers; i++)
	{
		arg.dsm_handle = (int) dsm_segment_handle(parallel_decode_seg);
		arg.slot_index = i;
		arg.db_oid = MyDatabaseId;
		arg.start_lsn = start_lsn;
		parallel_decode_register_worker("parallel decode worker",
										"parallel decode worker",
										"ParallelDecodeWorkerMain",
										&arg,
										&parallel_decode_worker_handles[i],
										&parallel_decode_worker_pids[i]);
	}

	/*
	 * Wait for the walreader to report that it is up.  Without this the
	 * session would start with no WAL reader at all, and the failure would
	 * only show up as a stalled replication.
	 */
	if (!parallel_decode_wait_for_walreader())
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("the parallel decode walreader did not report readiness")));

	parallel_decode_session_active = true;

	before_shmem_exit(parallel_decode_before_shmem_exit, (Datum) 0);

	ereport(LOG,
			(errmsg("parallel logical decoding session started with %d decode worker(s)",
					parallel_decode_num_workers)));
}

/*
 * Drain the output queues.
 *
 * The frames that are on their way to the walsender are of no use anymore
 * once the session is winding down, but they have to be taken off the queues:
 * a worker that is blocked handing over a frame can only notice the shutdown
 * flag after it has finished the transfer, and it can only finish the transfer
 * when the queue has room for it.
 */
static void
parallel_decode_drain_output_queues(void)
{
	int			worker;

	for (worker = 0; worker < parallel_decode_num_workers; worker++)
	{
		shm_mq_handle *mqh = parallel_decode_out_mqh[worker];
		uint16		type;
		const void *payload;
		size_t		len;

		if (mqh == NULL)
			continue;

		while (PDQueueRecv(mqh, &type, &payload, &len, true) == SHM_MQ_SUCCESS)
		{
			/* The frame is dropped. */
		}
	}
}

/*
 * Wait for a background worker to exit.
 *
 * The output queues are drained while we wait, so that a worker that is
 * blocked handing over a frame can finish the transfer and notice the
 * shutdown flag.
 */
static void
parallel_decode_wait_for_worker(BackgroundWorkerHandle *handle)
{
	for (;;)
	{
		pid_t		pid;
		BgwHandleStatus status;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		status = GetBackgroundWorkerPid(handle, &pid);
		if (status == BGWH_STOPPED)
			break;

		parallel_decode_drain_output_queues();

		/*
		 * The postmaster sets our latch when the worker exits, so the wait
		 * returns as soon as that has happened.  The timeout is only a safety
		 * net.
		 */
		ResetLatch(MyLatch);
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
					   100L,
					   PG_WAIT_EXTENSION);
		if (rc & WL_POSTMASTER_DEATH)
			break;
	}
}

/*
 * Terminate the workers and release the shared segment.
 */
void
ParallelDecodeSessionStop(void)
{
	int			i;

	if (!parallel_decode_session_active)
		return;

	/*
	 * Ask the workers and the walreader to stop on their own.  The explicit
	 * termination below is still needed to make sure they are gone before the
	 * shared segment goes away.
	 */
	parallel_decode_ctl->shutdown = 1;

	/*
	 * Terminate the decode workers first, then the walreader: the walreader
	 * may still be feeding the workers.
	 */
	for (i = 0; i < parallel_decode_num_workers; i++)
	{
		if (parallel_decode_worker_handles[i])
		{
			TerminateBackgroundWorker(parallel_decode_worker_handles[i]);
			parallel_decode_wait_for_worker(parallel_decode_worker_handles[i]);
			parallel_decode_worker_handles[i] = NULL;
		}
	}

	if (parallel_decode_walreader_handle)
	{
		TerminateBackgroundWorker(parallel_decode_walreader_handle);
		parallel_decode_wait_for_worker(parallel_decode_walreader_handle);
		parallel_decode_walreader_handle = NULL;
	}

	for (i = 0; i < MAX_PARALLEL_DECODE_WORKERS; i++)
		PDChunkReceiverReset(&parallel_decode_chunk_rcv[i]);
	parallel_decode_idle_cb = NULL;
	parallel_decode_position = InvalidXLogRecPtr;
	parallel_decode_next_cseq = 1;
	parallel_decode_ready = NIL;

	/*
	 * The frontier of the session goes away with it: it describes the
	 * transactions of this session only.
	 */
	while (parallel_decode_frontier != NIL)
	{
		PDCseqInfo *info = (PDCseqInfo *) linitial(parallel_decode_frontier);

		parallel_decode_frontier = list_delete_first(parallel_decode_frontier);
		pfree(info);
	}
	parallel_decode_certified_lsn = InvalidXLogRecPtr;
	parallel_decode_certified_cseq = 0;

	if (parallel_decode_pending)
	{
		hash_destroy(parallel_decode_pending);
		parallel_decode_pending = NULL;
	}

	if (parallel_decode_sent_relations)
	{
		hash_destroy(parallel_decode_sent_relations);
		parallel_decode_sent_relations = NULL;
	}

	for (i = 0; i < MAX_PARALLEL_DECODE_WORKERS; i++)
		parallel_decode_out_mqh[i] = NULL;

	if (parallel_decode_seg)
	{
		dsm_detach(parallel_decode_seg);
		parallel_decode_seg = NULL;
	}

	parallel_decode_toc = NULL;
	parallel_decode_ctl = NULL;
	parallel_decode_num_workers = 0;
	parallel_decode_session_active = false;
}

/*
 * Common worker-side startup: attach to the shared segment, look up the
 * control block and register ourselves in it.
 *
 * Returns the control block, or NULL if the segment could not be attached.
 * The segment and the table of contents are handed back as well, because
 * the participants need them to attach to the message queues.
 */
ParallelDecodeControl *
ParallelDecodeWorkerAttach(ParallelDecodeWorkerArg *arg, int *slot_index,
							dsm_segment **seg_out, shm_toc **toc_out)
{
	dsm_segment *seg;
	shm_toc    *toc;
	ParallelDecodeControl *ctl;

	seg = dsm_attach((dsm_handle) arg->dsm_handle);
	if (!seg)
		return NULL;

	toc = shm_toc_attach(PARALLEL_DECODE_SHM_MAGIC, dsm_segment_address(seg));
	if (!toc)
	{
		dsm_detach(seg);
		return NULL;
	}

	ctl = shm_toc_lookup(toc, PARALLEL_DECODE_KEY_SHARED, false);
	if (!ctl)
	{
		dsm_detach(seg);
		return NULL;
	}

	if (slot_index)
		*slot_index = arg->slot_index;

	if (seg_out)
		*seg_out = seg;
	if (toc_out)
		*toc_out = toc;

	return ctl;
}

/*
 * Look up a shared message queue by TOC key.
 */
shm_mq *
ParallelDecodeLookupMq(shm_toc *toc, int key)
{
	return shm_toc_lookup(toc, key, false);
}
