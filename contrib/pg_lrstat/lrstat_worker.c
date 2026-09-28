/*-------------------------------------------------------------------------
 *
 * lrstat_worker.c
 *      The sampler background worker (v2.0 session model).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "access/xact.h"
#include "executor/spi.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_classes.h"

#include "lrstat.h"

pg_noreturn void pg_lrstat_worker_main(Datum arg);

static void lrstat_round(void);
static void lrstat_record_history(void);
static void sample_send_side(TimestampTz now);
static LRPollTarget *sample_recv_side(TimestampTz now, int *n_targets,
									  MemoryContext poll_ctx);

#define SEND_SQL \
	"SELECT s.slot_name::text, d.datname::text, s.plugin::text, " \
	"       s.temporary, s.active, r.pid AS sender_pid, " \
	"       r.application_name::text, host(r.client_addr) AS client_addr, " \
	"       r.state, r.sync_state, " \
	"       pg_current_wal_lsn() AS current_lsn, " \
	"       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn, " \
	"       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status, " \
	"       s.safe_wal_size, " \
	"       rs.spill_bytes, rs.stream_bytes, rs.total_bytes, " \
	"       (EXTRACT(EPOCH FROM r.write_lag)  * 1000000)::bigint AS wl_us, " \
	"       (EXTRACT(EPOCH FROM r.flush_lag)  * 1000000)::bigint AS fl_us, " \
	"       (EXTRACT(EPOCH FROM r.replay_lag) * 1000000)::bigint AS rl_us " \
	"FROM pg_replication_slots s " \
	"LEFT JOIN pg_database d ON d.oid = s.datoid " \
	"LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid " \
	"LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name " \
	"WHERE s.slot_type IN ('logical', 'physical')"

#define RECV_SQL \
	"SELECT su.subname::text, su.subslotname::text, su.subconninfo, " \
	"       st.worker_type, st.pid AS worker_pid, st.leader_pid, st.relid, " \
	"       st.received_lsn, st.latest_end_lsn, " \
	"       o.remote_lsn, " \
	"       pg_current_wal_lsn() AS local_wal_lsn, " \
	"       st.last_msg_send_time, st.last_msg_receipt_time, " \
	"       ss.apply_error_count, ss.sync_error_count " \
	"FROM pg_subscription su " \
	"LEFT JOIN pg_stat_subscription st ON st.subid = su.oid " \
	"LEFT JOIN pg_replication_origin_status o " \
	"       ON o.external_id IN ('pg_' || su.oid, su.subname) " \
	"LEFT JOIN pg_stat_subscription_stats ss ON ss.subid = su.oid"

/* ---- SPI helpers ---- */
static Datum
col_val(HeapTuple tup, TupleDesc td, int fn, bool *isnull)
{
	if (fn < 1) { *isnull = true; return (Datum) 0; }
	return SPI_getbinval(tup, td, fn, isnull);
}

static void
col_text(HeapTuple tup, TupleDesc td, int fn, char *out, Size outlen)
{
	if (fn >= 1)
	{
		char *val = SPI_getvalue(tup, td, fn);
		if (val) { strlcpy(out, val, outlen); pfree(val); }
	}
}

static bool
col_bool(HeapTuple tup, TupleDesc td, int fn)
{
	bool isnull;
	Datum d = col_val(tup, td, fn, &isnull);
	return !isnull && DatumGetBool(d);
}

static int64
col_i8(HeapTuple tup, TupleDesc td, int fn, int64 def)
{
	bool isnull;
	Datum d = col_val(tup, td, fn, &isnull);
	return isnull ? def : DatumGetInt64(d);
}

static XLogRecPtr
col_lsn(HeapTuple tup, TupleDesc td, int fn)
{
	bool isnull;
	Datum d = col_val(tup, td, fn, &isnull);
	return isnull ? 0 : DatumGetLSN(d);
}

static TimestampTz
col_ts(HeapTuple tup, TupleDesc td, int fn)
{
	bool isnull;
	Datum d = col_val(tup, td, fn, &isnull);
	return isnull ? 0 : DatumGetTimestampTz(d);
}

#define FN(td, name) SPI_fnumber((td), (name))

pg_noreturn void
pg_lrstat_worker_main(Datum main_arg)
{
	MemoryContext round_ctx;

	(void) main_arg;
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(lrstat_database, NULL, 0);

	round_ctx = AllocSetContextCreate(TopMemoryContext,
									   "pg_lrstat round",
									   ALLOCSET_DEFAULT_SIZES);

	for (;;)
	{
		MemoryContext old;
		ProcessMainLoopInterrupts();

		old = MemoryContextSwitchTo(round_ctx);
		PG_TRY();
		{
			lrstat_round();
		}
		PG_CATCH();
		{
			ErrorData *edata = CopyErrorData();

			/* leave no aborted transaction behind across rounds */
			AbortOutOfAnyTransaction();
			FlushErrorState();
			elog(LOG, "pg_lrstat: round failed: %s (%s:%d in %s)%s%s",
				 edata->message ? edata->message : "unknown",
				 edata->filename ? edata->filename : "?",
				 edata->lineno, edata->funcname ? edata->funcname : "?",
				 edata->backtrace ? "\nbacktrace: " : "",
				 edata->backtrace ? edata->backtrace : "");
			FreeErrorData(edata);
		}
		PG_END_TRY();
		MemoryContextSwitchTo(old);
		MemoryContextReset(round_ctx);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 lrstat_sample_interval_ms,
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);
	}

	proc_exit(0);
}

static void
lrstat_round(void)
{
	TimestampTz now = GetCurrentTimestamp();
	LRPollTarget *targets = NULL;
	int n_targets = 0;
	MemoryContext poll_ctx;
	bool running;
	bool round_failed;

	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	running = lrstat->session.running;
	SpinLockRelease(&lrstat->session.mutex);

	/*
	 * The poll target list and its conninfo copies must outlive the
	 * round's SPI session, so remember this (pre-SPI) context for them.
	 */
	poll_ctx = CurrentMemoryContext;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();

	round_failed = false;
	PG_TRY();
	{
		targets = sample_recv_side(now, &n_targets, poll_ctx);
		sample_send_side(now);
		SPI_finish();
	}
	PG_CATCH();
	{
		ErrorData *edata;

		/* leave no SPI session or aborted transaction behind */
		SPI_finish();
		AbortOutOfAnyTransaction();
		round_failed = true;
		edata = CopyErrorData();
		FlushErrorState();
		elog(LOG, "pg_lrstat: round failed: %s (%s:%d in %s)",
			 edata->message ? edata->message : "unknown",
			 edata->filename ? edata->filename : "?",
			 edata->lineno, edata->funcname ? edata->funcname : "?");
		FreeErrorData(edata);
	}
	PG_END_TRY();

	if (!round_failed)
	{
		if (ActiveSnapshotSet())
			PopActiveSnapshot();
		CommitTransactionCommand();
	}

	/*
	 * History: record full raw samples for every active target,
	 * every round, regardless of session state.  This IS the
	 * primary data store — rates and stat views derive from it.
	 */
	lrstat_record_history();

	/* remote polling outside the transaction */
	if (running && lrstat_remote_poll && targets != NULL && n_targets > 0)
		lrstat_run_remote_poll(targets, n_targets,
							   now + (int64) lrstat_remote_poll_budget_ms * 1000);

	if (targets != NULL)
	{
		int i;
		for (i = 0; i < n_targets; i++)
			if (targets[i].conninfo != NULL)
				pfree(targets[i].conninfo);
		pfree(targets);
	}
}

/*
 * Record one full raw sample per active target into the history array
 * every round (always, not only during sessions) — history IS the
 * primary data store.  For persist sessions, append the same entries
 * to the session file.  Targets whose sample did not advance since
 * the previous round (e.g. a remote-polled sender after session stop)
 * are skipped so history is not flooded with duplicate samples.
 */
static TimestampTz *recorded_ts = NULL;    /* per target, worker-lifetime */

static void
lrstat_record_history(void)
{
	LRHistoryEntry *newents = NULL;
	int n_new = 0;
	uint64 session_id;
	int i;

	if (!lrstat_ready())
		return;

	/* stamp entries with the running session (0 when idle) */
	SpinLockAcquire(&lrstat->session.mutex);
	session_id = lrstat->session.running ? lrstat->session.session_id : 0;
	SpinLockRelease(&lrstat->session.mutex);

	if (recorded_ts == NULL)
	{
		MemoryContext old = MemoryContextSwitchTo(TopMemoryContext);

		recorded_ts = palloc0(lrstat->ntargets * sizeof(TimestampTz));
		MemoryContextSwitchTo(old);
	}

	/* persist sessions write their full history to a file */
	if (lrstat->session.running)
	{
		char	   *path = psprintf("%s/pg_lrstat/sessions/%s.sess",
									DataDir, lrstat->session.name);
		struct stat st;
		bool		persist = (stat(path, &st) == 0);

		pfree(path);
		if (persist)
			newents = palloc(lrstat->ntargets * sizeof(LRHistoryEntry));
	}

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool		use;
		LRSample	last;
		LRHistoryEntry e;

		SpinLockAcquire(&t->mutex);
		use = t->in_use;
		if (use)
			memcpy(&last, &t->last, sizeof(LRSample));
		SpinLockRelease(&t->mutex);

		if (!use || last.send.ts <= 0 || last.send.ts == recorded_ts[i])
			continue;

		lrstat_history_from_sample(&e, i, &last);
		e.session_id = session_id;
		lrstat_append_history_entry(&e);
		recorded_ts[i] = last.send.ts;
		if (newents != NULL)
			newents[n_new++] = e;
	}

	if (newents != NULL)
	{
		if (n_new > 0)
			lrstat_store_append(n_new, newents);
		pfree(newents);
	}
}

/*
 * Send-side sampling; runs inside the round's SPI session.
 */
static void
sample_send_side(TimestampTz now)
{
	int ret = SPI_execute(SEND_SQL, true, 0);
	SPITupleTable *tuptab = SPI_tuptable;
	TupleDesc td = tuptab->tupdesc;
	uint64 i;

	if (ret != SPI_OK_SELECT)
		elog(ERROR, "pg_lrstat: send-side sampling failed");

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple tup = tuptab->vals[i];
			char name[NAMEDATALEN];
			LRSample s;
			LRTargetMeta m;
			LRTargetCtl *t;
			int fn_ci = FN(td, "current_lsn");
			int fn_st = FN(td, "sent_lsn");
			int fn_wr = FN(td, "write_lsn");
			int fn_fl = FN(td, "flush_lsn");
			int fn_rp = FN(td, "replay_lsn");
			int fn_cf = FN(td, "confirmed_flush_lsn");
			int fn_rs = FN(td, "restart_lsn");
			int fn_sp = FN(td, "spill_bytes");
			int fn_sm = FN(td, "stream_bytes");
			int fn_tb = FN(td, "total_bytes");

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.send.ts = now;

			col_text(tup, td, FN(td, "slot_name"), name, sizeof(name));
			col_text(tup, td, FN(td, "datname"), m.database, LR_TEXT_LEN);
			col_text(tup, td, FN(td, "plugin"), m.plugin, LR_TEXT_LEN);
			col_text(tup, td, FN(td, "application_name"),
					 m.application_name, LR_TEXT_LEN);
			col_text(tup, td, FN(td, "client_addr"), m.client_addr,
					 LR_TEXT_LEN);
			col_text(tup, td, FN(td, "state"), m.state, LR_STATE_LEN);
			col_text(tup, td, FN(td, "sync_state"), m.sync_state,
					 LR_STATE_LEN);
			col_text(tup, td, FN(td, "wal_status"), m.wal_status,
					 LR_STATE_LEN);

			m.active = col_bool(tup, td, FN(td, "active"));
			m.temporary = col_bool(tup, td, FN(td, "temporary"));
			m.sender_pid = (pid_t) col_i8(tup, td, FN(td, "sender_pid"), 0);

			s.send.current_lsn = col_lsn(tup, td, fn_ci);
			s.send.sent_lsn = col_lsn(tup, td, fn_st);
			s.send.peer_recv_lsn = col_lsn(tup, td, fn_wr);
			s.send.peer_flush_lsn = col_lsn(tup, td, fn_fl);
			s.send.peer_applied_lsn = col_lsn(tup, td, fn_rp);
			s.send.confirmed_lsn = col_lsn(tup, td, fn_cf);
			s.send.restart_lsn = col_lsn(tup, td, fn_rs);
			s.send.spill_bytes = (uint64) col_i8(tup, td, fn_sp, 0);
			s.send.stream_bytes = (uint64) col_i8(tup, td, fn_sm, 0);
			s.send.total_bytes = (uint64) col_i8(tup, td, fn_tb, 0);

			m.write_lag_us = col_i8(tup, td, FN(td, "wl_us"), -1);
			m.flush_lag_us = col_i8(tup, td, FN(td, "fl_us"), -1);
			m.replay_lag_us = col_i8(tup, td, FN(td, "rl_us"), -1);

			{
				bool isnull;
				Datum d = col_val(tup, td, FN(td, "safe_wal_size"), &isnull);
				m.safe_wal_size_valid = !isnull;
				m.safe_wal_size = isnull ? 0 : DatumGetInt64(d);
			}

			t = lrstat_find_or_create(LR_SEND, name, 0, 0);
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}
	}
}

/*
 * Recv-side sampling; runs inside the round's SPI session.  Returns the
 * list of subscriptions to remote-poll, allocated (like the conninfo
 * copies) in poll_ctx — the caller's pre-SPI context — so it survives
 * the round's SPI_finish.
 */
static LRPollTarget *
sample_recv_side(TimestampTz now, int *n_targets, MemoryContext poll_ctx)
{
	LRPollTarget *targets = NULL;
	int n = 0, nalloc = 0;

	*n_targets = 0;
	{
		int ret = SPI_execute(RECV_SQL, true, 0);
		SPITupleTable *tuptab = SPI_tuptable;
		TupleDesc td = tuptab->tupdesc;
		uint64 i;

		if (ret != SPI_OK_SELECT)
			elog(ERROR, "pg_lrstat: recv-side sampling failed");

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple tup = tuptab->vals[i];
			char recv_name[NAMEDATALEN];
			char slot_name[NAMEDATALEN];
			char *conninfo = NULL;
			bool isnull;
			LRSample s;
			LRTargetMeta m;
			LRTargetCtl *t;
			Oid relid;
			char wc;

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.recv.ts = now;

			col_text(tup, td, FN(td, "subname"), recv_name, sizeof(recv_name));
			col_text(tup, td, FN(td, "worker_type"), m.worker_type,
					 LR_WTYPE_LEN);

			/* skip parallel apply rows; leader is canonical */
			if (strncmp(m.worker_type, "parallel", 8) == 0)
				continue;

			wc = (strncmp(m.worker_type, "table", 5) == 0) ? 't' : 'a';

			/* full copy of conninfo into caller-lifetime memory */
			(void) col_val(tup, td, FN(td, "subconninfo"), &isnull);
			if (!isnull)
			{
				char *val = SPI_getvalue(tup, td, FN(td, "subconninfo"));

				if (val)
				{
					conninfo = MemoryContextStrdup(poll_ctx, val);
					pfree(val);
				}
			}

			/* read the slot name for remote polling */
			col_text(tup, td, FN(td, "subslotname"), slot_name, sizeof(slot_name));

			relid = DatumGetObjectId(col_val(tup, td, FN(td, "relid"), &isnull));
			if (isnull) relid = 0;

			s.recv.received_lsn = col_lsn(tup, td, FN(td, "received_lsn"));
			s.recv.applied_lsn = col_lsn(tup, td, FN(td, "remote_lsn"));
			s.recv.local_wal_lsn = col_lsn(tup, td, FN(td, "local_wal_lsn"));

			m.worker_pid = (pid_t) col_i8(tup, td, FN(td, "worker_pid"), 0);
			m.leader_pid = (pid_t) col_i8(tup, td, FN(td, "leader_pid"), 0);
			m.last_msg_send_time = col_ts(tup, td, FN(td, "last_msg_send_time"));
			m.last_msg_receipt_time = col_ts(tup, td, FN(td, "last_msg_receipt_time"));
			m.apply_error_count = col_i8(tup, td, FN(td, "apply_error_count"), -1);
			m.sync_error_count = col_i8(tup, td, FN(td, "sync_error_count"), -1);
			m.origin_lsn = col_lsn(tup, td, FN(td, "remote_lsn"));
			m.latest_end_lsn = col_lsn(tup, td, FN(td, "latest_end_lsn"));

			t = lrstat_find_or_create(LR_RECV, recv_name, relid, wc);
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}

			/* collect for remote polling (dedup by name) */
			if (conninfo != NULL)
			{
				int j;
				bool dup = false;
				for (j = 0; j < n; j++)
					if (strcmp(targets[j].recv_name, recv_name) == 0)
					{ dup = true; break; }
				if (!dup)
				{
					if (n >= nalloc)
					{
						LRPollTarget *grow;

						nalloc = nalloc == 0 ? 8 : nalloc * 2;
						/* repalloc requires non-NULL; grow in the
						 * caller's context, not the SPI context */
						grow = MemoryContextAlloc(poll_ctx,
												  nalloc * sizeof(LRPollTarget));
						if (targets != NULL)
						{
							memcpy(grow, targets, n * sizeof(LRPollTarget));
							pfree(targets);
						}
						targets = grow;
					}
					strlcpy(targets[n].recv_name, recv_name, NAMEDATALEN);
					strlcpy(targets[n].slot_name, slot_name, NAMEDATALEN);
					targets[n].conninfo = conninfo;
					n++;
				}
				else
					pfree(conninfo);
			}
		}
	}

	*n_targets = n;
	return targets;
}
