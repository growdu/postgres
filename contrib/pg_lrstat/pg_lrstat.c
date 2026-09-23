/*-------------------------------------------------------------------------
 *
 * pg_lrstat.c
 *      Extension entry point: GUCs, shared memory hook and registration
 *      of the sampler background worker.
 *
 *      The extension must be loaded via shared_preload_libraries; in a
 *      backend that loads it late (CREATE EXTENSION without preload)
 *      _PG_init does nothing and the views simply return no rows.
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
#include "utils/wait_event.h"

#include "lrstat.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

/* GUC storage (extern'd in lrstat.h) */
int			lrstat_sample_interval_ms = 30000;
int			lrstat_rate_window_ms = 120000;
int			lrstat_max_targets = 32;
int			lrstat_ring_len = 1800;
int			lrstat_stale_target_ttl_s = 600;
int			lrstat_eta_min_rate = 1024;		/* 1 kB/s */
bool		lrstat_remote_poll = true;
int			lrstat_remote_connect_timeout_s = 5;
int			lrstat_remote_poll_budget_ms = 500;
char	   *lrstat_database = (char *) "postgres";
bool		lrstat_allow_inject = false;
char	   *lrstat_inject_name_prefix = (char *) "";

/* PG 18 dynamic wait events: registered in _PG_init below. */
uint32		lrstat_we_publisher_connect = 0;
uint32		lrstat_we_publisher_query = 0;

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
	 * Remember whether we ran during preload, before the early return:
	 * EXEC_BACKEND backends re-load preloaded libraries and need to know
	 * they may attach the shared segment (see lrstat_attach()).
	 */
	lrstat_note_preload();

	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_lrstat.sample_interval",
							"Sampling interval of the pg_lrstat sampler",
							NULL,
							&lrstat_sample_interval_ms,
							30000, 1000, 3600000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.rate_window",
							"Window over which rates are differentiated",
							"Must be at least twice the sampling interval",
							&lrstat_rate_window_ms,
							120000, 2000, 3600000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.max_targets",
							"Maximum number of monitored targets",
							NULL,
							&lrstat_max_targets,
							32, 1, 1024,
							PGC_POSTMASTER,
							GUC_NO_SHOW_ALL,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.raw_history_samples",
							"Ring buffer length of each target, in samples",
							NULL,
							&lrstat_ring_len,
							1800, 60, 10000,
							PGC_POSTMASTER,
							GUC_NO_SHOW_ALL,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.stale_target_ttl",
							"Time after which an unsampled target may be recycled",
							NULL,
							&lrstat_stale_target_ttl_s,
							600, 1, 86400,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.eta_min_rate",
							"Minimum rate in bytes/sec for a non-null ETA",
							NULL,
							&lrstat_eta_min_rate,
							1024, 0, INT_MAX,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lrstat.remote_poll",
							 "Poll publishers from the subscriber side",
							 NULL,
							 &lrstat_remote_poll,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.remote_connect_timeout",
							"Connect timeout for publisher polling",
							NULL,
							&lrstat_remote_connect_timeout_s,
							5, 1, 60,
							PGC_SIGHUP,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lrstat.remote_poll_budget",
							"Total time budget of one remote polling round",
							NULL,
							&lrstat_remote_poll_budget_ms,
							500, 100, 60000,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lrstat.database",
							   "Database the sampler worker connects to",
							   "Any database works: the underlying catalogs and "
							   "statistics are cluster-wide.  Needs a restart.",
							   &lrstat_database,
							   "postgres",
							   PGC_POSTMASTER,
							   GUC_NO_SHOW_ALL,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lrstat.allow_inject",
							 "Allow the pg_lrstat_inject_* test functions",
							 NULL,
							 &lrstat_allow_inject,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lrstat.inject_name_prefix",
							   "Required prefix for target names in pg_lrstat_inject_*",
							   "If non-empty, the inject functions only accept names "
							   "starting with this prefix; recommended value is "
							   "'pg_lrstat_test_' so test targets are easy to spot "
							   "and cannot collide with real slot / subscription "
							   "names.  Empty disables the check.",
							   &lrstat_inject_name_prefix,
							   "",
							   PGC_SUSET,
							   0,
							   NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_lrstat");

	/*
	 * Note: the publisher-polling wait events are registered lazily in
	 * lrstat_remote_round(), NOT here.  At shared_preload_libraries time
	 * the LWLock machinery behind WaitEventExtensionNew() does not exist
	 * yet (calling it from _PG_init crashes the postmaster); the
	 * postgres_fdw / dblink contribs use the same lazy pattern.
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
