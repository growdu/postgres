/*-------------------------------------------------------------------------
 *
 * lrstat_remote.c
 *      Poll the publisher of each subscription from the subscriber side.
 *
 *      The worker keeps one plain (non-replication) libpq connection
 *      per subscription, reusing the subscription's own conninfo (only
 *      application_name, connect_timeout and a statement_timeout are
 *      injected; the password never leaves memory).  Samples are
 *      stamped with the local clock at poll time, so publisher-side
 *      rates are differentiated on a single local timeline and clock
 *      skew between the two hosts never enters any computation.
 *
 *      Connection establishment and query submission go through the
 *      libpqsrv_* helpers in src/include/libpq/libpq-be-fe-helpers.h,
 *      so that WaitLatchOrSocket (and thus signal handling) is
 *      registered in pg_stat_activity while the worker is waiting on
 *      the publisher, and the FD accounting in fd.c stays correct.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_remote.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "libpq-fe.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "miscadmin.h"
#include "postmaster/interrupt.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "lrstat.h"

#define REMOTE_SQL \
	"SELECT pg_current_wal_lsn(), " \
	"       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status, " \
	"       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn, " \
	"       rs.spill_bytes, rs.stream_bytes, rs.total_bytes, " \
	"       (EXTRACT(EPOCH FROM r.write_lag) * 1000000)::bigint AS write_lag_us, " \
	"       (EXTRACT(EPOCH FROM r.flush_lag) * 1000000)::bigint AS flush_lag_us, " \
	"       (EXTRACT(EPOCH FROM r.replay_lag) * 1000000)::bigint AS replay_lag_us " \
	"FROM pg_replication_slots s " \
	"LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid " \
	"LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name " \
	"WHERE s.slot_name = $1"

typedef struct LRConnSlot
{
	char		subname[NAMEDATALEN];
	PGconn	   *conn;
	int			failures;
	TimestampTz next_retry;
} LRConnSlot;

static LRConnSlot *conns = NULL;
static int	nconns = 0;
static int	conns_alloc = 0;

static PGconn *lr_remote_connect(const char *conninfo);
static bool lr_parse_lsn(const char *s, XLogRecPtr *lsn);
static int64 lr_parse_i64(const char *s, int64 defval);
static void lr_set_remote_state(const char *subname, const char *state);

/*
 * Defensive: libpqsrv_* throws ERROR on signal interrupts, so any
 * caller of lrstat_remote_round must be wrapped in PG_TRY or be a
 * bgworker that already catches errors at the round boundary.
 */

/*
 * Build a connection from the subscription conninfo, overriding
 * application_name / connect_timeout / options.  Returns NULL on
 * failure (the caller applies backoff).
 */
static PGconn *
lr_remote_connect(const char *conninfo)
{
	PQconninfoOption *options;
	PQconninfoOption *option;
	char	   *errmsg = NULL;
	const char *keys[40];
	const char *vals[40];
	int			n = 0;
	char		timeout_s[16];
	char		statement_buf[64];
	char		options_buf[512];
	char	   *existing_options = NULL;
	PGconn	   *conn;

	options = PQconninfoParse(conninfo, &errmsg);
	if (options == NULL)
	{
		if (errmsg != NULL)
			PQfreemem(errmsg);
		return NULL;
	}

	snprintf(timeout_s, sizeof(timeout_s), "%d",
			 lrstat_remote_connect_timeout_s);
	snprintf(statement_buf, sizeof(statement_buf),
			 "-c statement_timeout=%d",
			 Max(100, Min(lrstat_remote_poll_budget_ms, 60000)));

	for (option = options; option->keyword != NULL; option++)
	{
		if (option->val == NULL)
			continue;
		if (strcmp(option->keyword, "application_name") == 0 ||
			strcmp(option->keyword, "connect_timeout") == 0)
			continue;			/* overridden below */
		if (strcmp(option->keyword, "options") == 0)
		{
			/*
			 * Preserve the user's options verbatim.  We track this
			 * separately rather than copying into a fixed-size buffer
			 * because PG conninfo can legitimately exceed 128 bytes
			 * (e.g. long SSL cert / CRL / service values); truncation
			 * here used to silently drop GUC settings on the publisher.
			 */
			existing_options = pstrdup(option->val);
			continue;
		}
		if (n >= 36)
			break;
		keys[n] = option->keyword;
		vals[n] = option->val;
		n++;
	}
	keys[n] = "application_name";
	vals[n] = "pg_lrstat";
	n++;
	keys[n] = "connect_timeout";
	vals[n] = timeout_s;
	n++;
	if (existing_options != NULL)
	{
		int			written = snprintf(options_buf, sizeof(options_buf),
										"%s %s", existing_options,
										statement_buf);

		if (written < 0 || (size_t) written >= sizeof(options_buf))
		{
			/*
			 * Combined options longer than 512 B: log and fall back to
			 * just the statement_timeout so the publisher still gets a
			 * timeout, but the user's extra GUC settings are dropped.
			 */
			elog(WARNING,
				 "pg_lrstat: options for publisher connection truncated, "
					 "extra GUC settings dropped");
			snprintf(options_buf, sizeof(options_buf), "%s", statement_buf);
		}
		pfree(existing_options);
		keys[n] = "options";
		vals[n] = options_buf;
	}
	else
	{
		keys[n] = "options";
		vals[n] = statement_buf;
	}
	n++;
	keys[n] = NULL;
	vals[n] = NULL;

	/*
	 * libpqsrv_connect_params also AcquireExternalFD()s; the matching
	 * libpqsrv_disconnect() below releases it.  It returns NULL on
	 * connect failure (the FD is released inside the helper), in
	 * which case the caller will apply backoff.
	 */
	conn = libpqsrv_connect_params(keys, vals, true,
								   lrstat_we_publisher_connect);
	PQconninfoFree(options);

	if (conn == NULL || PQstatus(conn) != CONNECTION_OK)
	{
		libpqsrv_disconnect(conn);
		return NULL;
	}
	return conn;
}

static bool
lr_parse_lsn(const char *s, XLogRecPtr *lsn)
{
	unsigned int	hi;
	unsigned int	lo;

	if (s == NULL || sscanf(s, "%8x/%8x", &hi, &lo) != 2)
		return false;
	*lsn = ((XLogRecPtr) hi << 32) | lo;
	return true;
}

static int64
lr_parse_i64(const char *s, int64 defval)
{
	if (s == NULL || *s == '\0')
		return defval;
	return strtoll(s, NULL, 10);
}

/* Update remote_state (and nothing else) of an RPUB target. */
static void
lr_set_remote_state(const char *subname, const char *state)
{
	LRSample   *ring;
	LRTargetMeta m;
	int			n;

	ring = palloc(lrstat_ring_len * sizeof(LRSample));
	if (lrstat_lookup(LR_RPUB, subname, ring, lrstat_ring_len, &n, &m))
	{
		strlcpy(m.remote_state, state, LR_STATE_LEN);
		{
			LRTargetCtl *t = lrstat_find_or_create(LR_RPUB, subname, 0, 0);

			if (t != NULL)
				lrstat_set_meta(t, &m);
		}
	}
	pfree(ring);
}

void
lrstat_remote_round(const LRRemoteSub *subs, int nsubs, TimestampTz deadline)
{
	int			i,
				j;
	TimestampTz now;

	/*
	 * Wait events must be registered after shared memory exists, so do
	 * it lazily on first use (postgres_fdw / dblink pattern); they stay
	 * 0 only if this round is never reached.
	 */
	if (lrstat_we_publisher_connect == 0)
		lrstat_we_publisher_connect =
			WaitEventExtensionNew("pg_lrstat publisher connect");
	if (lrstat_we_publisher_query == 0)
		lrstat_we_publisher_query =
			WaitEventExtensionNew("pg_lrstat publisher query");

	if (conns == NULL)
	{
		conns_alloc = Max(8, lrstat_max_targets);
		conns = MemoryContextAllocZero(TopMemoryContext,
									   conns_alloc * sizeof(LRConnSlot));
	}

	/* drop connections of subscriptions that no longer exist */
	for (i = 0; i < nconns; i++)
	{
		bool	found = false;

		for (j = 0; j < nsubs; j++)
			if (strcmp(conns[i].subname, subs[j].subname) == 0)
			{
				found = true;
				break;
			}
		if (!found)
		{
			if (conns[i].conn != NULL)
				libpqsrv_disconnect(conns[i].conn);
			conns[i] = conns[nconns - 1];
			nconns--;
			i--;
		}
	}

	for (j = 0; j < nsubs; j++)
	{
		const char *param = subs[j].slotname;
		PGresult   *res;
		LRConnSlot *c = NULL;
		LRTargetCtl *t;

		/*
		 * Drain pending signals between subscriptions so SIGHUP
		 * (config reload) and SIGTERM (graceful stop) get a chance to
		 * make progress.  libpqsrv_exec_params below also drains
		 * signals inside each call.
		 */
		ProcessMainLoopInterrupts();

		now = GetCurrentTimestamp();
		if (now >= deadline)
		{
			lr_set_remote_state(subs[j].subname, "stale");
			continue;
		}

		for (i = 0; i < nconns; i++)
			if (strcmp(conns[i].subname, subs[j].subname) == 0)
			{
				c = &conns[i];
				break;
			}
		if (c == NULL)
		{
			if (nconns >= conns_alloc)
				continue;		/* out of cache slots: skip this one */
			c = &conns[nconns++];
			memset(c, 0, sizeof(LRConnSlot));
			strlcpy(c->subname, subs[j].subname, NAMEDATALEN);
		}

		if (now < c->next_retry)
			continue;			/* in backoff */

		if (c->conn == NULL)
			c->conn = lr_remote_connect(subs[j].conninfo);

		if (c->conn == NULL)
			goto fail;

		/*
		 * libpqsrv_exec_params is synchronous from the caller's POV
		 * (it internally polls the socket and waits on MyLatch), but
		 * it processes interrupts and reports the registered wait
		 * event so pg_stat_activity reflects the actual wait reason.
		 * On interrupt the helper ereport(ERROR); lr_remote_round
		 * is called from a thread inside the bgworker loop that
		 * already catches errors (lrstat_round).
		 */
		res = libpqsrv_exec_params(c->conn, REMOTE_SQL, 1, NULL, &param,
								   NULL, NULL, 0,
								   lrstat_we_publisher_query);
		if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			libpqsrv_disconnect(c->conn);
			c->conn = NULL;
			goto fail;
		}

		if (PQntuples(res) == 0)
		{
			/* publisher reachable, but the slot is not there */
			PQclear(res);
			lr_set_remote_state(subs[j].subname, "stale");
			c->failures = 0;
			c->next_retry = 0;
			continue;
		}

		{
			LRSample	s;
			LRTargetMeta m;

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.pub.ts = GetCurrentTimestamp();

			(void) lr_parse_lsn(PQgetvalue(res, 0, 0), &s.pub.current_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 1), &s.pub.confirmed_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 2), &s.pub.restart_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 4), &s.pub.sent_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 5), &s.pub.peer_recv_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 6), &s.pub.peer_flush_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 7), &s.pub.peer_applied_lsn);
			s.pub.spill_bytes = lr_parse_i64(PQgetvalue(res, 0, 8), 0);
			s.pub.stream_bytes = lr_parse_i64(PQgetvalue(res, 0, 9), 0);
			s.pub.total_bytes = lr_parse_i64(PQgetvalue(res, 0, 10), 0);

			if (!PQgetisnull(res, 0, 3))
				strlcpy(m.wal_status, PQgetvalue(res, 0, 3), LR_STATE_LEN);
			m.write_lag_us = lr_parse_i64(PQgetvalue(res, 0, 11), -1);
			m.flush_lag_us = lr_parse_i64(PQgetvalue(res, 0, 12), -1);
			m.replay_lag_us = lr_parse_i64(PQgetvalue(res, 0, 13), -1);
			strlcpy(m.remote_state, "ok", LR_STATE_LEN);
			m.last_remote_poll = s.pub.ts;

			t = lrstat_find_or_create(LR_RPUB, subs[j].subname, 0, 0);
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}

			/*
			 * Keep the subscriber-side applied series consistent with
			 * what the publisher's feedback says we applied (the
			 * origin remote_lsn only advances at commit boundaries).
			 */
			lrstat_bump_applied(subs[j].subname, s.pub.peer_flush_lsn);

			c->failures = 0;
			c->next_retry = 0;
		}

		PQclear(res);
		continue;

fail:
		c->failures++;
		{
			/*
			 * Exponential backoff: 1s, 2s, 4s, 8s, ... capped at 60s.
			 * Matches design §1.4 ("1s→2s→…→60s 封顶").  Note the loop
			 * is bounded by both failure count and the cap; the old
			 * version used lrstat_sample_interval_ms as the base unit
			 * which made the cap useless at the default 30s cadence.
			 */
			int64		backoff_us = USECS_PER_SEC;
			int			step;

			for (step = 1; step < c->failures; step++)
			{
				backoff_us *= 2;
				if (backoff_us >= 60 * USECS_PER_SEC)
				{
					backoff_us = 60 * USECS_PER_SEC;
					break;
				}
			}
			c->next_retry = GetCurrentTimestamp() + backoff_us;
		}
		lr_set_remote_state(subs[j].subname, "unreachable");
	}
}
