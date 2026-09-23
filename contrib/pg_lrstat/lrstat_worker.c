/*-------------------------------------------------------------------------
 *
 * lrstat_worker.c
 *      The sampler background worker.
 *
 *      One worker, connected to a single database (any one: slots,
 *      subscriptions, walsender stats and origin progress are all
 *      cluster-wide).  Each round it snapshots the publisher-side and
 *      subscriber-side system views via SPI, then polls the publishers
 *      of all subscriptions found, and writes everything into the
 *      shared memory rings.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_worker.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

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

static void lrstat_round(void);
static void sample_publisher(TimestampTz now);
static LRRemoteSub *sample_subscriber(TimestampTz now, int *nsubs);

/*
 * Publisher-side snapshot.  pg_stat_replication joins the slot through
 * active_pid: the walsender serving a logical slot owns that slot, so
 * the join is exact.  Name-typed columns are cast to text so that the
 * C side can treat every string column as a varlena.
 */
#define PUB_SQL \
	"SELECT s.slot_name::text, d.datname::text, s.plugin::text, " \
	"       s.temporary, s.active, " \
	"       r.pid AS sender_pid, r.application_name::text, " \
	"       host(r.client_addr) AS client_addr, r.state, r.sync_state, " \
	"       pg_current_wal_lsn() AS current_lsn, " \
	"       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn, " \
	"       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status, s.safe_wal_size, " \
	"       rs.spill_bytes, rs.stream_bytes, rs.total_bytes, " \
	"       (EXTRACT(EPOCH FROM r.write_lag) * 1000000)::bigint AS write_lag_us, " \
	"       (EXTRACT(EPOCH FROM r.flush_lag) * 1000000)::bigint AS flush_lag_us, " \
	"       (EXTRACT(EPOCH FROM r.replay_lag) * 1000000)::bigint AS replay_lag_us, " \
	"       r.reply_time " \
	"FROM pg_replication_slots s " \
	"LEFT JOIN pg_database d ON d.oid = s.datoid " \
	"LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid " \
	"LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name " \
	"WHERE s.slot_type = 'logical'"

/* Subscriber-side snapshot, one row per subscription worker. */
#define SUB_SQL \
	"SELECT su.subname::text, su.subslotname::text, su.subconninfo, " \
	"       st.worker_type, st.pid AS worker_pid, st.leader_pid, st.relid, " \
	"       st.received_lsn, st.latest_end_lsn, " \
	"       o.remote_lsn, o.local_lsn AS origin_local_lsn, " \
	"       pg_current_wal_lsn() AS local_wal_lsn, " \
	"       st.last_msg_send_time, st.last_msg_receipt_time, st.latest_end_time, " \
	"       ss.apply_error_count, ss.sync_error_count " \
	"FROM pg_subscription su " \
	"LEFT JOIN pg_stat_subscription st ON st.subid = su.oid " \
	"LEFT JOIN pg_replication_origin_status o " \
	"       ON o.external_id = su.subname::text " \
	"LEFT JOIN pg_stat_subscription_stats ss ON ss.subid = su.oid"

/* ---- SPI column helpers (keep the row loops short) ---- */

static Datum
col_val(HeapTuple tup, TupleDesc td, int fn, bool *isnull)
{
	if (fn < 1)
	{
		*isnull = true;
		return (Datum) 0;
	}
	return SPI_getbinval(tup, td, fn, isnull);
}

static void
col_text(HeapTuple tup, TupleDesc td, int fn, char *out, Size outlen)
{
	bool	isnull;

	(void) col_val(tup, td, fn, &isnull);
	if (!isnull)
		strlcpy(out, TextDatumGetCString(col_val(tup, td, fn, &isnull)),
				outlen);
}

static bool
col_bool(HeapTuple tup, TupleDesc td, int fn)
{
	bool	isnull;
	Datum	d = col_val(tup, td, fn, &isnull);

	return !isnull && DatumGetBool(d);
}

static int64
col_int8(HeapTuple tup, TupleDesc td, int fn, int64 defval)
{
	bool	isnull;
	Datum	d = col_val(tup, td, fn, &isnull);

	return isnull ? defval : DatumGetInt64(d);
}

static XLogRecPtr
col_lsn(HeapTuple tup, TupleDesc td, int fn)
{
	bool	isnull;
	Datum	d = col_val(tup, td, fn, &isnull);

	return isnull ? 0 : DatumGetLSN(d);
}

static TimestampTz
col_ts(HeapTuple tup, TupleDesc td, int fn)
{
	bool	isnull;
	Datum	d = col_val(tup, td, fn, &isnull);

	return isnull ? 0 : DatumGetTimestampTz(d);
}

#define FN(td, name) SPI_fnumber((td), (name))

void
pg_lrstat_worker_main(Datum main_arg)
{
	MemoryContext round_ctx;

	(void) main_arg;

	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(lrstat_database, NULL, 0);

	round_ctx = AllocSetContextCreate(TopMemoryContext,
									   "pg_lrstat sampler round",
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
			EmitErrorReport();
			FlushErrorState();
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
	TimestampTz	now = GetCurrentTimestamp();
	LRRemoteSub *subs = NULL;
	int			nsubs = 0;
	int			i;
	bool		failed = false;
	char		round_error[LR_ERROR_LEN];

	round_error[0] = '\0';

	if (!lrstat_ready())
		return;

	/*
	 * SPI needs a transaction and a resource owner; this is the
	 * canonical bgworker pattern from the documentation.
	 */
	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());

	PG_TRY();
	{
		subs = sample_subscriber(now, &nsubs);
		sample_publisher(now);
	}
	PG_CATCH();
	{
		ErrorData  *edata = CopyErrorData();

		strlcpy(round_error,
				edata->message ? edata->message : "unknown error",
				sizeof(round_error));
		FreeErrorData(edata);
		FlushErrorState();
		subs = NULL;
		nsubs = 0;
		failed = true;
	}
	PG_END_TRY();

	if (ActiveSnapshotSet())
		PopActiveSnapshot();

	/*
	 * CommitTransactionCommand() also cleans up a transaction left
	 * aborted by a caught error, so both paths are safe.
	 */
	CommitTransactionCommand();

	/*
	 * Log failures only when the state flips or half-hourly while the
	 * failure persists, so a long outage does not flood the log with
	 * one ERROR per sampling round.  The outcome always lands in the
	 * shared header for pg_lrstat_info() regardless.
	 */
	{
		static bool prev_ok = true;
		static TimestampTz prev_fail_log = 0;

		if (!failed)
		{
			if (!prev_ok)
				ereport(LOG, (errmsg("pg_lrstat: sampling round recovered")));
			prev_ok = true;
		}
		else
		{
			TimestampTz now2 = GetCurrentTimestamp();

			if (prev_ok || prev_fail_log == 0 ||
				TimestampDifferenceMilliseconds(prev_fail_log, now2) >=
				30 * 60 * 1000.0)
				ereport(LOG,
						(errmsg("pg_lrstat: sampling round failed: %s",
								round_error)));
			prev_ok = false;
			prev_fail_log = now2;
		}
		lrstat_note_round(!failed, round_error);
	}

	/* remote polling runs outside the transaction */
	if (!failed && lrstat_remote_poll && subs != NULL && nsubs > 0)
		lrstat_remote_round(subs, nsubs,
							now + (int64) lrstat_remote_poll_budget_ms * 1000);

	if (subs != NULL)
	{
		for (i = 0; i < nsubs; i++)
			if (subs[i].conninfo != NULL)
				pfree(subs[i].conninfo);
		pfree(subs);
	}
}

static void
sample_publisher(TimestampTz now)
{
	SPI_connect();

	PG_TRY();
	{
		int			ret = SPI_execute(PUB_SQL, true, 0);
		SPITupleTable *tuptab = SPI_tuptable;
		TupleDesc	td = tuptab->tupdesc;
		uint64		i;

		if (ret != SPI_OK_SELECT)
			elog(ERROR, "pg_lrstat: publisher sampling query failed");

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tup = tuptab->vals[i];
			char		name[NAMEDATALEN];
			LRSample	s;
			LRTargetMeta m;
			LRTargetCtl *t;
			bool		isnull;
			Datum		d;

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.pub.ts = now;

			col_text(tup, td, FN(td, "slot_name"), name, sizeof(name));
			col_text(tup, td, FN(td, "datname"), m.database, NAMEDATALEN);
			col_text(tup, td, FN(td, "plugin"), m.plugin, LR_TEXT_LEN);
			col_text(tup, td, FN(td, "application_name"), m.application_name,
					 NAMEDATALEN);
			col_text(tup, td, FN(td, "client_addr"), m.client_addr,
					 LR_TEXT_LEN);
			col_text(tup, td, FN(td, "state"), m.state, LR_STATE_LEN);
			col_text(tup, td, FN(td, "sync_state"), m.sync_state,
					 LR_STATE_LEN);
			col_text(tup, td, FN(td, "wal_status"), m.wal_status,
					 LR_STATE_LEN);

			m.active = col_bool(tup, td, FN(td, "active"));
			m.temporary = col_bool(tup, td, FN(td, "temporary"));
			m.sender_pid = (pid_t) col_int8(tup, td, FN(td, "sender_pid"), 0);

			s.pub.current_lsn = col_lsn(tup, td, FN(td, "current_lsn"));
			s.pub.sent_lsn = col_lsn(tup, td, FN(td, "sent_lsn"));
			s.pub.peer_recv_lsn = col_lsn(tup, td, FN(td, "write_lsn"));
			s.pub.peer_flush_lsn = col_lsn(tup, td, FN(td, "flush_lsn"));
			s.pub.peer_applied_lsn = col_lsn(tup, td, FN(td, "replay_lsn"));
			s.pub.confirmed_lsn = col_lsn(tup, td, FN(td, "confirmed_flush_lsn"));
			s.pub.restart_lsn = col_lsn(tup, td, FN(td, "restart_lsn"));
			s.pub.spill_bytes = (uint64) col_int8(tup, td, FN(td, "spill_bytes"), 0);
			s.pub.stream_bytes = (uint64) col_int8(tup, td, FN(td, "stream_bytes"), 0);
			s.pub.total_bytes = (uint64) col_int8(tup, td, FN(td, "total_bytes"), 0);

			m.write_lag_us = col_int8(tup, td, FN(td, "write_lag_us"), -1);
			m.flush_lag_us = col_int8(tup, td, FN(td, "flush_lag_us"), -1);
			m.replay_lag_us = col_int8(tup, td, FN(td, "replay_lag_us"), -1);

			d = col_val(tup, td, FN(td, "safe_wal_size"), &isnull);
			m.safe_wal_size_valid = !isnull;
			m.safe_wal_size = isnull ? 0 : DatumGetInt64(d);

			d = col_val(tup, td, FN(td, "reply_time"), &isnull);
			m.reply_time_valid = !isnull;
			m.reply_time = isnull ? 0 : DatumGetTimestampTz(d);

			t = lrstat_find_or_create(LR_PUB, name, 0, 0);
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}
		}
	}
	PG_FINALLY();
	{
		SPI_finish();
	}
	PG_END_TRY();
}

/*
 * Snapshot the subscriber side.  Returns a palloc'd list of distinct
 * subscriptions (name, slot, conninfo) for the remote polling phase;
 * rows without conninfo (should not happen) are skipped in the list.
 */
static LRRemoteSub *
sample_subscriber(TimestampTz now, int *nsubs)
{
	LRRemoteSub *subs = NULL;
	int			n = 0;
	int			nalloc = 0;

	*nsubs = 0;

	SPI_connect();

	PG_TRY();
	{
		int			ret = SPI_execute(SUB_SQL, true, 0);
		SPITupleTable *tuptab = SPI_tuptable;
		TupleDesc	td = tuptab->tupdesc;
		uint64		i;

		if (ret != SPI_OK_SELECT)
			elog(ERROR, "pg_lrstat: subscriber sampling query failed");

		for (i = 0; i < SPI_processed; i++)
		{
			HeapTuple	tup = tuptab->vals[i];
			char		subname[NAMEDATALEN];
			char		slotname[NAMEDATALEN];
			char	   *conninfo = NULL;
			bool		isnull;
			LRSample	s;
			LRTargetMeta m;
			LRTargetCtl *t;
			Oid			relid;

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.sub.ts = now;

			col_text(tup, td, FN(td, "subname"), subname, sizeof(subname));
			col_text(tup, td, FN(td, "subslotname"), m.subslotname,
					 NAMEDATALEN);
			col_text(tup, td, FN(td, "worker_type"), m.worker_type,
					 LR_STATE_LEN);
			/* full copy: conninfo may exceed any sane stack buffer */
			(void) col_val(tup, td, FN(td, "subconninfo"), &isnull);
			if (!isnull)
				conninfo = TextDatumGetCString(col_val(tup, td,
											   FN(td, "subconninfo"),
											   &isnull));

			relid = DatumGetObjectId(col_val(tup, td, FN(td, "relid"), &isnull));
			if (isnull)
				relid = 0;

			s.sub.received_lsn = col_lsn(tup, td, FN(td, "received_lsn"));
			s.sub.latest_end_lsn = col_lsn(tup, td, FN(td, "latest_end_lsn"));
			s.sub.applied_lsn = col_lsn(tup, td, FN(td, "remote_lsn"));
			s.sub.origin_local_lsn = col_lsn(tup, td, FN(td, "origin_local_lsn"));
			s.sub.local_wal_lsn = col_lsn(tup, td, FN(td, "local_wal_lsn"));

			m.worker_pid = (pid_t) col_int8(tup, td, FN(td, "worker_pid"), 0);
			m.leader_pid = (pid_t) col_int8(tup, td, FN(td, "leader_pid"), 0);
			m.last_msg_send_time = col_ts(tup, td, FN(td, "last_msg_send_time"));
			m.last_msg_receipt_time = col_ts(tup, td, FN(td, "last_msg_receipt_time"));
			m.latest_end_time = col_ts(tup, td, FN(td, "latest_end_time"));
			m.apply_error_count = col_int8(tup, td, FN(td, "apply_error_count"), -1);
			m.sync_error_count = col_int8(tup, td, FN(td, "sync_error_count"), -1);

			t = lrstat_find_or_create(LR_SUB, subname, relid,
									  strncmp(m.worker_type, "tablesync", 9) == 0 ? 't' : 'a');
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}

			/* collect each subscription once for the remote phase */
			if (conninfo == NULL)
				continue;
			slotname[0] = '\0';
			col_text(tup, td, FN(td, "subslotname"), slotname, sizeof(slotname));
			{
				int			j;
				bool		dup = false;

				for (j = 0; j < n; j++)
					if (strcmp(subs[j].subname, subname) == 0)
					{
						dup = true;
						break;
					}
				if (dup)
					continue;
			}

			if (n >= nalloc)
			{
				nalloc = nalloc == 0 ? 8 : nalloc * 2;
				subs = repalloc(subs, nalloc * sizeof(LRRemoteSub));
			}
			strlcpy(subs[n].subname, subname, NAMEDATALEN);
			strlcpy(subs[n].slotname, slotname, NAMEDATALEN);
			subs[n].conninfo = conninfo;	/* ownership moves to subs */
			n++;
		}
	}
	PG_FINALLY();
	{
		SPI_finish();
	}
	PG_END_TRY();

	*nsubs = n;
	return subs;
}
