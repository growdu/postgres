/*-------------------------------------------------------------------------
 *
 * lrstat_remote.c
 *      Poll the send side from the recv side.
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
	"       (EXTRACT(EPOCH FROM r.write_lag)  * 1000000)::bigint AS wl_us, " \
	"       (EXTRACT(EPOCH FROM r.flush_lag)  * 1000000)::bigint AS fl_us, " \
	"       (EXTRACT(EPOCH FROM r.replay_lag) * 1000000)::bigint AS rl_us " \
	"FROM pg_replication_slots s " \
	"LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid " \
	"LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name " \
	"WHERE s.slot_name = $1"

typedef struct LRConnSlot
{
	char recv_name[NAMEDATALEN];
	PGconn *conn;
	int failures;
	TimestampTz next_retry;
} LRConnSlot;

static LRConnSlot *conns = NULL;
static int nconns = 0;
static int conns_alloc = 0;

static uint32 we_connect = 0;
static uint32 we_query = 0;

static PGconn *lr_remote_connect(const char *conninfo);
static bool lr_parse_lsn(const char *s, XLogRecPtr *lsn);
static int64 lr_parse_i64(const char *s, int64 def);
static void lr_set_remote_state(const char *name, const char *state);

static PGconn *
lr_remote_connect(const char *conninfo)
{
	PQconninfoOption *options;
	PQconninfoOption *option;
	char *errmsg = NULL;
	const char *keys[40];
	const char *vals[40];
	int n = 0;
	char timeout_s[16];
	char options_buf[512];
	char *existing_options = NULL;
	PGconn *conn;

	options = PQconninfoParse(conninfo, &errmsg);
	if (options == NULL)
	{
		if (errmsg) PQfreemem(errmsg);
		return NULL;
	}

	snprintf(timeout_s, sizeof(timeout_s), "%d",
			 lrstat_remote_connect_timeout_s);
	snprintf(options_buf, sizeof(options_buf), "-c statement_timeout=%d",
			 Max(100, Min(lrstat_remote_poll_budget_ms, 60000)));

	for (option = options; option->keyword != NULL; option++)
	{
		if (option->val == NULL) continue;
		if (strcmp(option->keyword, "application_name") == 0 ||
			strcmp(option->keyword, "connect_timeout") == 0)
			continue;
		if (strcmp(option->keyword, "options") == 0)
		{
			existing_options = pstrdup(option->val);
			continue;
		}
		if (n >= 36) break;
		keys[n] = option->keyword;
		vals[n] = option->val;
		n++;
	}
	keys[n] = "application_name"; vals[n] = "pg_lrstat"; n++;
	keys[n] = "connect_timeout"; vals[n] = timeout_s; n++;
	if (existing_options != NULL)
		snprintf(options_buf, sizeof(options_buf), "%s -c statement_timeout=%d",
				 existing_options, Max(100, Min(lrstat_remote_poll_budget_ms, 60000)));
	keys[n] = "options"; vals[n] = options_buf; n++;
	keys[n] = NULL; vals[n] = NULL;

	conn = libpqsrv_connect_params(keys, vals, true,
								   we_connect ? we_connect : 0);
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
	unsigned int hi, lo;
	if (s == NULL || sscanf(s, "%8x/%8x", &hi, &lo) != 2)
		return false;
	*lsn = ((XLogRecPtr) hi << 32) | lo;
	return true;
}

static int64
lr_parse_i64(const char *s, int64 def)
{
	if (s == NULL || *s == '\0') return def;
	return strtoll(s, NULL, 10);
}

static void
lr_set_remote_state(const char *name, const char *state)
{
	LRTargetCtl *t = lrstat_find_or_create(LR_RSEND, name, 0, 0);
	if (t != NULL)
	{
		LRTargetMeta m;
		LRSample a, p, l;
		lrstat_copy_all(t, &a, &p, &l, &m);
		strlcpy(m.remote_state, state, LR_STATE_LEN);
		lrstat_set_meta(t, &m);
	}
}

void
lrstat_run_remote_poll(const LRPollTarget *targets, int ntargets,
				   TimestampTz deadline)
{
	int i, j;

	/* lazy wait event registration */
	if (we_connect == 0)
		we_connect = WaitEventExtensionNew("pg_lrstat connect");
	if (we_query == 0)
		we_query = WaitEventExtensionNew("pg_lrstat query");

	if (conns == NULL)
	{
		conns_alloc = Max(8, lrstat_max_targets);
		conns = MemoryContextAllocZero(TopMemoryContext,
									   conns_alloc * sizeof(LRConnSlot));
	}

	/* drop connections of vanished targets */
	for (i = 0; i < nconns; i++)
	{
		bool found = false;
		for (j = 0; j < ntargets; j++)
			if (strcmp(conns[i].recv_name, targets[j].recv_name) == 0)
			{ found = true; break; }
		if (!found)
		{
			if (conns[i].conn != NULL)
				libpqsrv_disconnect(conns[i].conn);
			conns[i] = conns[nconns - 1];
			nconns--;
			i--;
		}
	}

	for (j = 0; j < ntargets; j++)
	{
		const char *param = targets[j].slot_name;
		PGresult *res;
		LRConnSlot *c = NULL;

		ProcessMainLoopInterrupts();

		if (GetCurrentTimestamp() >= deadline)
		{
			lr_set_remote_state(targets[j].recv_name, "stale");
			continue;
		}

		for (i = 0; i < nconns; i++)
			if (strcmp(conns[i].recv_name, targets[j].recv_name) == 0)
			{ c = &conns[i]; break; }
		if (c == NULL)
		{
			if (nconns >= conns_alloc)
				continue;
			c = &conns[nconns++];
			memset(c, 0, sizeof(LRConnSlot));
			strlcpy(c->recv_name, targets[j].recv_name, NAMEDATALEN);
		}

		if (GetCurrentTimestamp() < c->next_retry)
			continue;

		if (c->conn == NULL)
			c->conn = lr_remote_connect(targets[j].conninfo);
		if (c->conn == NULL)
			goto fail;

		res = libpqsrv_exec_params(c->conn, REMOTE_SQL, 1, NULL, &param,
								   NULL, NULL, 0, we_query);
		if (res == NULL || PQresultStatus(res) != PGRES_TUPLES_OK)
		{
			PQclear(res);
			libpqsrv_disconnect(c->conn);
			c->conn = NULL;
			goto fail;
		}

		if (PQntuples(res) == 0)
		{
			PQclear(res);
			lr_set_remote_state(targets[j].recv_name, "stale");
			c->failures = 0;
			c->next_retry = 0;
			continue;
		}

		{
			LRSample s;
			LRTargetMeta m;
			LRTargetCtl *t;

			memset(&s, 0, sizeof(s));
			memset(&m, 0, sizeof(m));
			s.send.ts = GetCurrentTimestamp();

			(void) lr_parse_lsn(PQgetvalue(res, 0, 0), &s.send.current_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 1), &s.send.confirmed_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 2), &s.send.restart_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 4), &s.send.sent_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 5), &s.send.peer_recv_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 6), &s.send.peer_flush_lsn);
			(void) lr_parse_lsn(PQgetvalue(res, 0, 7), &s.send.peer_applied_lsn);
			s.send.spill_bytes = lr_parse_i64(PQgetvalue(res, 0, 8), 0);
			s.send.stream_bytes = lr_parse_i64(PQgetvalue(res, 0, 9), 0);
			s.send.total_bytes = lr_parse_i64(PQgetvalue(res, 0, 10), 0);

			if (!PQgetisnull(res, 0, 3))
				strlcpy(m.wal_status, PQgetvalue(res, 0, 3), LR_STATE_LEN);
			m.write_lag_us = lr_parse_i64(PQgetvalue(res, 0, 11), -1);
			m.flush_lag_us = lr_parse_i64(PQgetvalue(res, 0, 12), -1);
			m.replay_lag_us = lr_parse_i64(PQgetvalue(res, 0, 13), -1);
			strlcpy(m.remote_state, "ok", LR_STATE_LEN);
			m.last_remote_poll = s.send.ts;

			t = lrstat_find_or_create(LR_RSEND, targets[j].recv_name, 0, 0);
			if (t != NULL)
			{
				lrstat_push_sample(t, &s);
				lrstat_set_meta(t, &m);
			}

			/* keep recv applied consistent with feedback */
			/*
			 * Fold the feedback APPLY position (walsender's replay_lsn)
			 * into the newest recv sample's applied_lsn.  The flush slot
			 * must NOT be used here: it is ahead of the apply position
			 * and would overstate applied progress.
			 */
			lrstat_bump_applied(targets[j].recv_name,
								s.send.peer_applied_lsn);

			c->failures = 0;
			c->next_retry = 0;
		}

		PQclear(res);
		continue;

fail:
		c->failures++;
		{
			int64 backoff_us = USECS_PER_SEC;
			int step;
			for (step = 1; step < c->failures; step++)
			{
				backoff_us *= 2;
				if (backoff_us >= 60 * USECS_PER_SEC)
				{ backoff_us = 60 * USECS_PER_SEC; break; }
			}
			c->next_retry = GetCurrentTimestamp() + backoff_us;
		}
		lr_set_remote_state(targets[j].recv_name, "unreachable");
	}
}
