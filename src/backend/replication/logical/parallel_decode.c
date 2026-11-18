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

#include "miscadmin.h"
#include "nodes/pg_list.h"
#include "portability/instr_time.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/parallel_decode.h"
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
} PDPendingTxn;

/*
 * The transactions whose frames are waiting for their turn, keyed by their
 * commit sequence number.
 */
static HTAB *parallel_decode_pending = NULL;

/*
 * The commit sequence number of the transaction whose frames may be sent next.
 */
static uint64 parallel_decode_next_cseq = 1;

/*
 * The frames that may be sent, in the order they have to be sent.
 */
static List *parallel_decode_ready = NIL;

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
 */
static void
parallel_decode_add_pending_frame(uint64 cseq, const char *data, size_t len,
								   XLogRecPtr end_lsn)
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
	}

	pending->acked = true;
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
			parallel_decode_ready = lappend(parallel_decode_ready, lfirst(lc));

		list_free(pending->frames);
		hash_search(parallel_decode_pending, &parallel_decode_next_cseq,
					HASH_REMOVE, NULL);
		parallel_decode_next_cseq++;
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
 * Handle a message of one of the decode workers.
 *
 * The payload belongs to the caller.
 */
static void
parallel_decode_handle_message(uint16 type, const void *payload, size_t len)
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

					parallel_decode_ready = lappend(parallel_decode_ready, frame);
				}
				else
					parallel_decode_add_pending_frame(fh.cseq,
													 (const char *) payload + sizeof(fh),
													 fh.length,
													 (XLogRecPtr) fh.end_lsn);

				break;
			}

		default:
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("unexpected message of type %u on the parallel decode "
							"output channel", type)));
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
		 * A frame that has its turn is sent first.  The frames of a
		 * transaction that does not have its turn yet are still waiting.
		 */
		if (parallel_decode_take_ready_frame(buf, end_lsn))
			return true;

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
				parallel_decode_handle_message(type, payload, len);
				pfree((void *) payload);
				got_message = true;
				break;
			}
			if (res == SHM_MQ_DETACHED)
			{
				/*
				 * The worker is gone, so the frames it was going to produce
				 * will never arrive.  The session cannot continue.
				 */
				if (!ParallelDecodeProcessAlive(ctl->workers[worker].pid))
					return false;
			}
		}

		if (got_message)
			continue;

		/* No frame yet.  Check whether the session is still alive. */
		if (ctl == NULL || ctl->shutdown)
			return false;
		if (!ParallelDecodeProcessAlive(ctl->walsender_pid))
			return false;
		for (worker = 0; worker < parallel_decode_num_workers; worker++)
		{
			if (!ParallelDecodeProcessAlive(ctl->workers[worker].pid))
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("the parallel decode worker died unexpectedly")));
		}

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
 * The WAL position the decoding has come to.
 */
XLogRecPtr
ParallelDecodePosition(void)
{
	return parallel_decode_position;
}

/*
 * Create the shared segment, the control block and the message queues, and
 * start the walreader and the decode workers.
 */
void
ParallelDecodeSessionStart(ReplicationSlot *slot,
							const char *plugin_options,
							uint32 plugin_options_len,
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
	parallel_decode_ctl->streaming_mode = parallel_decode_streaming_mode;
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

	if (parallel_decode_pending)
	{
		hash_destroy(parallel_decode_pending);
		parallel_decode_pending = NULL;
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
