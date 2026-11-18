/*-------------------------------------------------------------------------
 *
 * parallel_decode_worker.c
 *		Publisher-side parallel logical decoding: decode worker process.
 *
 * A decode worker owns a set of transactions, decodes their WAL records into
 * replication protocol bytes and hands the bytes to the walsender over a
 * shared memory queue.  See doc/pa_decode/design.md for the full design.
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
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the worker can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

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
 * Main entry point of a decode worker.
 *
 * The worker attaches to the shared segment created by the walsender, claims
 * its slot in the control block and then waits for work.
 */
void
ParallelDecodeWorkerMain(Datum main_arg)
{
	ParallelDecodeWorkerArg arg;
	ParallelDecodeControl *ctl;
	int			slot_index = -1;
	uint32		wait_event;

	/* Setup signal handling. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	/* Exit cleanly on SIGTERM so that the session shutdown is not
	 * reported as a background worker crash. */
	pqsignal(SIGTERM, parallel_decode_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(ParallelDecodeWorkerArg));

	ctl = ParallelDecodeWorkerAttach(&arg, &slot_index);
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
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* Claim the slot. */
	ctl->workers[slot_index].pid = MyProcPid;
	ctl->workers[slot_index].status = PD_WORKER_STATUS_RUNNING;

	wait_event = WaitEventExtensionNew("paralleldecodedecodeworker");

	ereport(LOG,
			(errmsg("parallel decode worker %d started", slot_index)));

	/*
	 * Wait for work.  The decoding loop proper is implemented in a later
	 * milestone; until then the worker simply stays alive so that the process
	 * plumbing can be validated independently of the decoding path.
	 */
	while (!ctl->shutdown && !got_sigterm)
	{
		int			rc;

		CHECK_FOR_INTERRUPTS();

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   1000L,
					   wait_event);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}

	ctl->workers[slot_index].status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode worker %d stopped", slot_index)));

	proc_exit(0);
}
