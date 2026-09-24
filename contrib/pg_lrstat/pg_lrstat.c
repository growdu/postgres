/*-------------------------------------------------------------------------
 *
 * pg_lrstat.c
 *      Extension entry point: GUCs, shmem hooks, worker registration.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/pg_lrstat.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <limits.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/shmem.h"
#include "utils/guc.h"

#include "lrstat.h"

PG_MODULE_MAGIC;

void        _PG_init(void);

int         lrstat_sample_interval_ms = 30000;
int         lrstat_max_targets = 32;
int         lrstat_ring_len = 2880;
int         lrstat_stale_target_ttl_s = 600;
double      lrstat_catchup_min_rate = 0.001;  /* MB/s */
bool        lrstat_remote_poll = true;
int         lrstat_remote_connect_timeout_s = 5;
int         lrstat_remote_poll_budget_ms = 500;
char       *lrstat_database = (char *) "postgres";
bool        lrstat_allow_inject = false;

static shmem_request_hook_type prev_shmem_request_hook = NULL;
static shmem_startup_hook_type prev_shmem_startup_hook = NULL;

static void
pg_lrstat_shmem_request(void)
{
	if (prev_shmem_request_hook)
		prev_shmem_request_hook();

	RequestAddinShmemSpace(lrstat_shmem_size());
}

static void
pg_lrstat_shmem_startup(void)
{
	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	lrstat_shmem_startup();
}

void
_PG_init(void)
{
	/*
	 * Remember whether we ran during preload (EXEC_BACKEND backends
	 * re-load preloaded libraries and need to know they may attach).
	 */
	lrstat_note_preload();

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_lrstat.sample_interval",
							"Sampling interval (also instant-rate grain)",
							NULL,
							&lrstat_sample_interval_ms,
							30000, 1000, 3600000,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.session_max_samples",
							"Session log capacity per target",
							"30s interval ≈ 24h; 0 disables interval log",
							&lrstat_ring_len,
							2880, 0, 10000,
							PGC_POSTMASTER, 0,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.max_targets",
							"Maximum monitored targets",
							NULL,
							&lrstat_max_targets,
							32, 1, 1024,
							PGC_POSTMASTER, GUC_NO_SHOW_ALL,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.stale_target_ttl",
							"Time before an unsampled target is recycled",
							NULL,
							&lrstat_stale_target_ttl_s,
							600, 1, 86400,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomRealVariable("pg_lrstat.catchup_min_rate",
							 "Min avg rate (MB/s) for non-null catchup estimate",
							 NULL,
							 &lrstat_catchup_min_rate,
							 0.001, 0.0, 1000000.0,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lrstat.remote_poll",
							 "Poll send side from recv side",
							 NULL,
							 &lrstat_remote_poll,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.remote_connect_timeout",
							"Connect timeout for remote polling",
							NULL,
							&lrstat_remote_connect_timeout_s,
							5, 1, 60,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.remote_poll_budget",
							"Total time budget for one remote poll round",
							NULL,
							&lrstat_remote_poll_budget_ms,
							500, 100, 60000,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lrstat.database",
							   "Database the sampler connects to",
							   "Any database works (stats are cluster-wide). Restart required.",
							   &lrstat_database,
							   "postgres",
							   PGC_POSTMASTER, GUC_NO_SHOW_ALL,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lrstat.allow_inject",
							 "Allow test injection functions",
							 NULL,
							 &lrstat_allow_inject,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_lrstat");

	/*
	 * Wait events are registered lazily in the remote poll path;
	 * calling WaitEventExtensionNew() here would crash the postmaster.
	 */

	prev_shmem_request_hook = shmem_request_hook;
	shmem_request_hook = pg_lrstat_shmem_request;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pg_lrstat_shmem_startup;

	{
		BackgroundWorker worker;

		memset(&worker, 0, sizeof(worker));
		snprintf(worker.bgw_name, BGW_MAXLEN, "pg_lrstat sampler");
		snprintf(worker.bgw_type, BGW_MAXLEN, "pg_lrstat");
		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_ConsistentState;
		worker.bgw_restart_time = 10;
		snprintf(worker.bgw_library_name, BGW_MAXLEN, "pg_lrstat");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "pg_lrstat_worker_main");
		worker.bgw_main_arg = (Datum) 0;
		RegisterBackgroundWorker(&worker);
	}
}
