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

#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/parallel_decode.h"
#include "storage/dsm.h"
#include "storage/ipc.h"
#include "storage/shm_mq.h"
#include "storage/proc.h"
#include "storage/shm_toc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
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
static BackgroundWorkerHandle *parallel_decode_worker_handles[MAX_PARALLEL_DECODE_WORKERS];
static pid_t	parallel_decode_worker_pids[MAX_PARALLEL_DECODE_WORKERS];
static int	parallel_decode_num_workers = 0;
static bool parallel_decode_session_active = false;

/*
 * Make sure that the workers and the walreader never outlive the session
 * that started them.  This runs on every process exit path, including the
 * ones that do not go through ParallelDecodeSessionStop(), so it only raises
 * the shutdown flag: waiting for the workers here would not be safe.
 */
static void
parallel_decode_on_proc_exit(int code, Datum arg)
{
	if (parallel_decode_session_active && parallel_decode_ctl)
		parallel_decode_ctl->shutdown = 1;
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
 * Create the shared segment, the control block and the message queues, and
 * start the walreader and the decode workers.
 */
void
ParallelDecodeSessionStart(void)
{
	shm_toc_estimator e;
	size_t		segsize;
	ParallelDecodeWorkerArg arg;
	pid_t		pid;
	int			i;

	Assert(!parallel_decode_session_active);
	Assert(ParallelDecodeEnabled());

	if (max_parallel_decode_workers > MAX_PARALLEL_DECODE_WORKERS)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("max_parallel_decode_workers must be at most %d",
						MAX_PARALLEL_DECODE_WORKERS)));

	parallel_decode_num_workers = max_parallel_decode_workers;

	/*
	 * Estimate the size of the shared segment: the control block, the control
	 * channel and one message queue per direction and per worker.
	 */
	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_chunk(&e, sizeof(ParallelDecodeControl));
	shm_toc_estimate_chunk(&e, (size_t) PARALLEL_DECODE_QUEUE_SIZE *
						   (2 + 2 * parallel_decode_num_workers));
	shm_toc_estimate_keys(&e, 3 + 2 * parallel_decode_num_workers);
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

	/*
	 * Control channel, walreader -> walsender.  The walreader sets itself as
	 * the sender once it has attached; the walsender is the receiver.
	 */
	shm_mq_set_receiver(parallel_decode_create_mq(parallel_decode_toc,
													   PARALLEL_DECODE_KEY_CONTROL_MQ),
						 MyProc);

	/*
	 * Walreader -> worker queues.  The walreader is the sender, the workers
	 * are the receivers; neither identity is known yet, so both sides set
	 * their own after attaching.
	 */
	for (i = 0; i < parallel_decode_num_workers; i++)
		parallel_decode_create_mq(parallel_decode_toc,
								  PARALLEL_DECODE_KEY_WAL_MQ_BASE + i);

	/* Worker -> walsender queues.  The walsender is the receiver. */
	for (i = 0; i < parallel_decode_num_workers; i++)
	{
		shm_mq	   *mq;

		mq = parallel_decode_create_mq(parallel_decode_toc,
									   PARALLEL_DECODE_KEY_OUT_MQ_BASE + i);
		shm_mq_set_receiver(mq, MyProc);
	}

	/* Start the walreader. */
	arg.dsm_handle = (int) dsm_segment_handle(parallel_decode_seg);
	arg.slot_index = -1;
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
		parallel_decode_register_worker("parallel decode worker",
										"parallel decode worker",
										"ParallelDecodeWorkerMain",
										&arg,
										&parallel_decode_worker_handles[i],
										&parallel_decode_worker_pids[i]);
	}

	parallel_decode_session_active = true;

	on_proc_exit(parallel_decode_on_proc_exit, (Datum) 0);

	ereport(LOG,
			(errmsg("parallel logical decoding session started with %d decode worker(s)",
					parallel_decode_num_workers)));
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
			WaitForBackgroundWorkerShutdown(parallel_decode_worker_handles[i]);
			parallel_decode_worker_handles[i] = NULL;
		}
	}

	if (parallel_decode_walreader_handle)
	{
		TerminateBackgroundWorker(parallel_decode_walreader_handle);
		WaitForBackgroundWorkerShutdown(parallel_decode_walreader_handle);
		parallel_decode_walreader_handle = NULL;
	}

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
 */
ParallelDecodeControl *
ParallelDecodeWorkerAttach(ParallelDecodeWorkerArg *arg, int *slot_index)
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
