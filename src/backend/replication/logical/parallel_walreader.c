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
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "tcop/tcopprot.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the walreader can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

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
 * Main entry point of the walreader.
 */
void
ParallelWalreaderMain(Datum main_arg)
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
	pqsignal(SIGTERM, parallel_walreader_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(ParallelDecodeWorkerArg));

	ctl = ParallelDecodeWorkerAttach(&arg, &slot_index);
	if (ctl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not attach to parallel decoding shared segment")));

	/*
	 * The walreader owns the snapshot builder, which needs access to the
	 * system catalogs.
	 */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	ctl->walreader_pid = MyProcPid;
	ctl->walreader_status = PD_WORKER_STATUS_RUNNING;

	wait_event = WaitEventExtensionNew("paralleldecodedewalreader");

	ereport(LOG,
			(errmsg("parallel decode walreader started")));

	/*
	 * Wait for work.  The WAL reading loop proper is implemented in a later
	 * milestone; until then the walreader simply stays alive so that the
	 * process plumbing can be validated independently of the decoding path.
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

	ctl->walreader_status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode walreader stopped")));

	proc_exit(0);
}
