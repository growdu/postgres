/*-------------------------------------------------------------------------
 *
 * lrstat_export.c
 *      Session report export: JSON (machine-readable) and
 *      HTML (self-contained, inline SVG charts, analysis card).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_export.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>

#include "funcapi.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

#include "lrstat.h"

#define MB_DIV (1024.0 * 1024.0)
#define EXPORT_DIR "pg_lrstat/exports"
#define EXPORT_MAX_HISTORY 500    /* most recent samples in a report */

/* ----------------------------------------------------------------
 * Data gathering: snapshot of everything needed for the report
 * ----------------------------------------------------------------
 */

typedef struct LRExportTarget
{
	int         ctl_idx;           /* index in the shared target array */
	char        name[NAMEDATALEN];
	LRTargetKind kind;
	char        worker_char;
	Oid         relid;
	LRSample    anchor, prev, last;
	LRTargetMeta meta;
	bool        has_prev;
} LRExportTarget;

typedef struct LRExportData
{
	char        session_name[NAMEDATALEN];
	bool        session_running;
	TimestampTz start_ts, stop_ts;
	bool        truncated, degraded;
	int         n_targets;
	LRExportTarget targets[64];    /* max_targets */
	int         n_entries;         /* entries copied into the report */
	int         total_entries;     /* valid entries in shared memory */
	LRHistoryEntry *entries;       /* chronological, newest last */
} LRExportData;

/* "0/1A2B3C" text form of an LSN ("" when unset) */
static const char *
lsn_str(XLogRecPtr lsn)
{
	if (lsn == 0)
		return "";
	return psprintf("%X/%X", (uint32) (lsn >> 32), (uint32) lsn);
}

static const LRExportTarget *
find_target_by_ctl(const LRExportData *d, int ctl_idx)
{
	int i;
	for (i = 0; i < d->n_targets; i++)
		if (d->targets[i].ctl_idx == ctl_idx)
			return &d->targets[i];
	return NULL;
}

static void
gather_data(LRExportData *d)
{
	int i;

	MemSet(d, 0, sizeof(LRExportData));

	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	strlcpy(d->session_name, lrstat->session.name, NAMEDATALEN);
	d->session_running = lrstat->session.running;
	d->start_ts = lrstat->session.start_ts;
	d->stop_ts = lrstat->session.stop_ts;
	d->truncated = lrstat->session.truncated;
	d->degraded = lrstat->session.degraded;
	SpinLockRelease(&lrstat->session.mutex);

	d->n_targets = 0;
	for (i = 0; i < lrstat->ntargets && d->n_targets < 64; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		LRExportTarget *et = &d->targets[d->n_targets];
		bool use;

		SpinLockAcquire(&t->mutex);
		use = t->in_use;
		if (use)
		{
			et->ctl_idx = i;
			et->kind = t->kind;
			strlcpy(et->name, t->name, NAMEDATALEN);
			et->worker_char = t->worker_char;
			et->relid = t->relid;
			memcpy(&et->anchor, &t->anchor, sizeof(LRSample));
			memcpy(&et->prev, &t->prev, sizeof(LRSample));
			memcpy(&et->last, &t->last, sizeof(LRSample));
			memcpy(&et->meta, &t->meta, sizeof(LRTargetMeta));
			et->has_prev = (t->prev.send.ts > 0);
		}
		SpinLockRelease(&t->mutex);
		if (use)
			d->n_targets++;
	}

	/* history: chronological copy of THIS session's samples (idle
	 * samples recorded outside the session are excluded), capped to
	 * the most recent EXPORT_MAX_HISTORY */
	{
		uint64 sess_id = lrstat_session_id_by_name(d->session_name);

		d->total_entries = 0;
		for (i = 0; i < lrstat_history_count(); i++)
		{
			LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));

			if (e->session_id == sess_id && sess_id != 0)
				d->total_entries++;
		}
		d->n_entries = Min(d->total_entries, EXPORT_MAX_HISTORY);
		if (d->n_entries > 0)
		{
			int skip = d->total_entries - d->n_entries;
			int copied = 0;

			d->entries = palloc(d->n_entries * sizeof(LRHistoryEntry));
			for (i = 0; i < lrstat_history_count() && copied < skip + d->n_entries; i++)
			{
				LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));

				if (e->session_id != sess_id || sess_id == 0)
					continue;
				if (copied >= skip)
					memcpy(&d->entries[copied - skip], e,
						   sizeof(LRHistoryEntry));
				copied++;
			}
		}

	}

	/*
	 * Re-anchor every target from THIS session's first/last valid
	 * samples instead of the live three-slot anchor/last: the slot
	 * values carry post-sampling feedback folds that are not in the
	 * recorded history, which made the report's averages differ
	 * from a manual recomputation over the history views.
	 */
	if (d->n_entries > 0)
	{
		int ti, ei;

		for (ti = 0; ti < d->n_targets; ti++)
		{
			LRExportTarget *et = &d->targets[ti];

			MemSet(&et->anchor, 0, sizeof(LRSample));
			MemSet(&et->last, 0, sizeof(LRSample));
			et->has_prev = false;
			for (ei = 0; ei < d->n_entries; ei++)
			{
					LRHistoryEntry *e = &d->entries[ei];

					if (e->target_idx != et->ctl_idx)
						continue;
					if (et->kind == LR_RECV)
					{
						if (e->applied_lsn == 0 && e->received_lsn == 0)
							continue;
						if (et->anchor.recv.ts == 0)
						{
							et->anchor.recv.ts = e->ts;
							et->anchor.recv.received_lsn = e->received_lsn;
							et->anchor.recv.applied_lsn = e->applied_lsn;
						}
						et->last.recv.ts = e->ts;
						et->last.recv.received_lsn = e->received_lsn;
						et->last.recv.applied_lsn = e->applied_lsn;
					}
					else
					{
						if (e->current_lsn == 0 && e->sent_lsn == 0)
							continue;
						if (et->anchor.send.ts == 0)
						{
							et->anchor.send.ts = e->ts;
							et->anchor.send.current_lsn = e->current_lsn;
							et->anchor.send.sent_lsn = e->sent_lsn;
						}
						et->last.send.ts = e->ts;
						et->last.send.current_lsn = e->current_lsn;
						et->last.send.sent_lsn = e->sent_lsn;
					}
				}
			}
	}
}

/*
 * Build export data from an archived session file (export by name).
 * Reconstructs per-target anchor/last samples from the first/last
 * history entries of each target; passthrough metadata is not stored
 * in files and stays empty.
 */
/*
 * Shared rebuild from a raw entry array + target info list: fills the
 * export targets and history copy, reconstructs per-target anchor/last
 * samples.  The ts and the LSNs must come from the SAME sample — the
 * first/last one with non-zero LSNs — or dt and bytes drift apart and
 * the average rate comes out wrong.
 */
static void
rebuild_from_history(LRExportData *d,
					 const LRHistoryEntry *ents, int n_ents,
					 const LRSessTargetInfo *tinfos, int n_tinfos)
{
	int i, j;

	/* targets */
	d->n_targets = Min(n_tinfos, 64);
	for (i = 0; i < d->n_targets; i++)
	{
		LRExportTarget *et = &d->targets[i];

		et->ctl_idx = tinfos[i].target_idx;
		et->kind = (LRTargetKind) tinfos[i].kind;
		strlcpy(et->name, tinfos[i].name, NAMEDATALEN);
		/* target info carries no worker_char/relid; treat every recv
		 * target as an apply-leader candidate */
		if (et->kind == LR_RECV)
		{
			et->worker_char = 'a';
			et->relid = 0;
		}
	}

	/* history copy (most recent EXPORT_MAX_HISTORY), chronological */
	d->total_entries = n_ents;
	d->n_entries = Min(n_ents, EXPORT_MAX_HISTORY);
	if (d->n_entries > 0)
	{
		int skip = n_ents - d->n_entries;

		d->entries = palloc(d->n_entries * sizeof(LRHistoryEntry));
		for (i = 0; i < d->n_entries; i++)
			memcpy(&d->entries[i], &ents[skip + i], sizeof(LRHistoryEntry));
	}

	/* anchor/last per target */
	for (i = 0; i < d->n_targets; i++)
	{
		LRExportTarget *et = &d->targets[i];

		for (j = 0; j < n_ents; j++)
		{
			const LRHistoryEntry *e = &ents[j];

			if (e->target_idx != et->ctl_idx)
				continue;
			if (et->kind == LR_RECV)
			{
				bool valid = (e->received_lsn != 0 || e->applied_lsn != 0);

				if (valid && et->anchor.recv.ts == 0)
				{
					et->anchor.recv.ts = e->ts;
					et->anchor.recv.received_lsn = e->received_lsn;
					et->anchor.recv.applied_lsn = e->applied_lsn;
				}
				if (valid)
				{
					et->last.recv.ts = e->ts;
					et->last.recv.received_lsn = e->received_lsn;
					et->last.recv.applied_lsn = e->applied_lsn;
				}
			}
			else
			{
				bool valid = (e->current_lsn != 0 || e->sent_lsn != 0);

				if (valid && et->anchor.send.ts == 0)
				{
					et->anchor.send.ts = e->ts;
					et->anchor.send.current_lsn = e->current_lsn;
					et->anchor.send.sent_lsn = e->sent_lsn;
				}
				if (valid)
				{
					et->last.send.ts = e->ts;
					et->last.send.current_lsn = e->current_lsn;
					et->last.send.sent_lsn = e->sent_lsn;
				}
			}
		}
		et->has_prev = false;
	}
}

/*
 * Export a remembered (non-current) session straight from the in-memory
 * history ring by name.  Works for persist=false sessions as long as
 * the ring has not overwritten their samples yet.
 */
static bool
gather_memory_by_name(LRExportData *d, const char *name)
{
	uint64		sid;
	TimestampTz start_ts, stop_ts;
	LRHistoryEntry *ents = NULL;
	LRSessTargetInfo *tinfos;
	int n_ents = 0, n_tinfos = 0;
	int i, j;

	MemSet(d, 0, sizeof(LRExportData));

	if (!lrstat_sessionreg_lookup(name, &sid, &start_ts, &stop_ts))
		return false;

	strlcpy(d->session_name, name, NAMEDATALEN);
	d->start_ts = start_ts;
	d->stop_ts = stop_ts;
	d->session_running = false;

	/* collect this session's entries in chronological order */
	for (i = 0; i < lrstat_history_count(); i++)
	{
		LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));

		if (e->session_id != sid)
			continue;
		if (ents == NULL)
			ents = palloc(sizeof(LRHistoryEntry) *
						  Max(16, lrstat_history_count()));
		ents[n_ents++] = *e;
	}
	if (n_ents == 0)
	{
		/* known session but the ring no longer holds its samples */
		pfree(ents);
		return false;
	}

	/* build the target list from the entries; prefer live ctl names */
	tinfos = palloc0(Max(16, n_ents) * sizeof(LRSessTargetInfo));
	for (i = 0; i < n_ents; i++)
	{
		int idx = ents[i].target_idx;
		bool known = false;

		for (j = 0; j < n_tinfos; j++)
			if (tinfos[j].target_idx == idx)
			{ known = true; break; }
		if (known || n_tinfos >= 64)
			continue;

		tinfos[n_tinfos].target_idx = idx;
		if (idx >= 0 && idx < lrstat->ntargets)
		{
			LRTargetCtl *t = lrstat_target_at(idx);

			SpinLockAcquire(&t->mutex);
			if (t->in_use)
			{
				tinfos[n_tinfos].kind = t->kind;
				strlcpy(tinfos[n_tinfos].name, t->name, NAMEDATALEN);
			}
			SpinLockRelease(&t->mutex);
		}
		if (tinfos[n_tinfos].name[0] == '\0')
		{
			/* slot reused or gone: the entry carries its own kind */
			tinfos[n_tinfos].kind = ents[i].kind;
			snprintf(tinfos[n_tinfos].name, NAMEDATALEN, "?%d", idx);
		}
		n_tinfos++;
	}

	rebuild_from_history(d, ents, n_ents, tinfos, n_tinfos);

	pfree(ents);
	pfree(tinfos);
	return true;
}

/* ----------------------------------------------------------------
 * Analysis computation
 * ----------------------------------------------------------------
 */

typedef struct LRAnalysis
{
	const char *bottleneck;       /* send / recv_apply / network / none */
	const char *deep_cause;       /* decode / sync_rep / lock / "" */
	double      gen_avg, send_avg, recv_avg, apply_avg;
	double      backlog_unsent_mb, backlog_unapplied_mb, backlog_inflight_mb;
	double      backlog_total_mb;
	double      catchup_secs;
	double      net_catchup_mbps;
	double      sync_50g_secs, sync_100g_secs, sync_200g_secs;
	bool        has_data;
	/* shared-array indexes the rate chart is built from (-1: none) */
	int         chart_send_ctl;
	int         chart_recv_ctl;
} LRAnalysis;

static void
compute_analysis(LRExportData *d, LRAnalysis *a)
{
	LRExportTarget *recv_t = NULL;
	LRExportTarget *rsend_t = NULL;
	int i;

	MemSet(a, 0, sizeof(LRAnalysis));
	a->bottleneck = "none";
	a->deep_cause = "";
	a->chart_send_ctl = -1;
	a->chart_recv_ctl = -1;

	if (d->n_targets == 0)
	{
		a->has_data = false;
		return;
	}
	a->has_data = true;

	/* chart fallbacks: first send target, first leader recv target */
	for (i = 0; i < d->n_targets; i++)
	{
		if (a->chart_send_ctl < 0 &&
			(d->targets[i].kind == LR_SEND || d->targets[i].kind == LR_RSEND))
			a->chart_send_ctl = d->targets[i].ctl_idx;
		if (a->chart_recv_ctl < 0 &&
			d->targets[i].kind == LR_RECV && d->targets[i].worker_char == 'a' &&
			d->targets[i].relid == 0)
			a->chart_recv_ctl = d->targets[i].ctl_idx;
	}

	/* find the first recv target (leader) and its rsend mirror */
	for (i = 0; i < d->n_targets; i++)
	{
		if (d->targets[i].kind == LR_RECV && d->targets[i].worker_char == 'a'
			&& d->targets[i].relid == 0)
		{
			recv_t = &d->targets[i];
			break;
		}
	}
	if (recv_t == NULL)
	{
		a->has_data = false;
		return;
	}
	for (i = 0; i < d->n_targets; i++)
	{
		if (d->targets[i].kind == LR_RSEND &&
			strcmp(d->targets[i].name, recv_t->name) == 0)
		{
			rsend_t = &d->targets[i];
			break;
		}
	}
	if (rsend_t == NULL)
	{
		a->has_data = false;
		return;
	}

	/* prefer the matched pair for the chart */
	a->chart_send_ctl = rsend_t->ctl_idx;
	a->chart_recv_ctl = recv_t->ctl_idx;

	{
		const LRSendSample *rs = &rsend_t->last.send;
		const LRRecvSample *rv = &recv_t->last.recv;
		double dt;
		int64 d_gen, d_send, d_recv, d_apply;
		int64 unsent, inflight, unapplied, total;

		/* avg rates from anchor to last, with the window clamped to the
		 * session stop so post-stop idle samples do not dilute them */
		dt = (double)(lrstat_rate_end(rsend_t->last.send.ts) -
					  rsend_t->anchor.send.ts) / 1e6;
		if (dt <= 0) dt = 1.0;

		d_gen = (int64)(rs->current_lsn - rsend_t->anchor.send.current_lsn);
		d_send = (int64)(rs->sent_lsn - rsend_t->anchor.send.sent_lsn);
		a->gen_avg = d_gen > 0 ? (double)d_gen / dt / MB_DIV : 0;
		a->send_avg = d_send > 0 ? (double)d_send / dt / MB_DIV : 0;

		{
			double rdt = (double)(lrstat_rate_end(recv_t->last.recv.ts) -
								  recv_t->anchor.recv.ts) / 1e6;
			if (rdt <= 0) rdt = dt;
			d_recv = (int64)(rv->received_lsn - recv_t->anchor.recv.received_lsn);
			d_apply = (int64)(rv->applied_lsn - recv_t->anchor.recv.applied_lsn);
			a->recv_avg = d_recv > 0 ? (double)d_recv / rdt / MB_DIV : 0;
			a->apply_avg = d_apply > 0 ? (double)d_apply / rdt / MB_DIV : 0;
		}

		/* backlogs */
		unsent = rs->current_lsn > rs->sent_lsn ?
			(int64)(rs->current_lsn - rs->sent_lsn) : 0;
		inflight = rs->sent_lsn > rv->received_lsn ?
			(int64)(rs->sent_lsn - rv->received_lsn) : 0;
		unapplied = rv->received_lsn > rv->applied_lsn ?
			(int64)(rv->received_lsn - rv->applied_lsn) : 0;
		total = unsent + inflight + unapplied;

		a->backlog_unsent_mb = (double)unsent / MB_DIV;
		a->backlog_inflight_mb = (double)inflight / MB_DIV;
		a->backlog_unapplied_mb = (double)unapplied / MB_DIV;
		a->backlog_total_mb = (double)total / MB_DIV;

		/* bottleneck */
		if (a->send_avg < a->gen_avg && unsent > total / 2)
		{
			a->bottleneck = "send";
			if (rsend_t->meta.write_lag_us >= 0 && a->gen_avg > 0)
			{
				/* decode pressure heuristic: spill would show in history */
				a->deep_cause = "";
			}
			if (strcmp(rsend_t->meta.sync_state, "async") != 0)
				a->deep_cause = "sync_rep";
		}
		else if (a->apply_avg < a->recv_avg && unapplied > total / 2)
		{
			a->bottleneck = "recv_apply";
			a->deep_cause = "";  /* lock detection needs pg_stat_activity */
		}
		else if (inflight > total / 2)
			a->bottleneck = "network";
		else
			a->bottleneck = "none";

		/* catchup + capacity */
		if (a->apply_avg > 0.001)
		{
			a->catchup_secs = a->backlog_unapplied_mb / a->apply_avg;
			a->sync_50g_secs = 50.0 * 1024 / a->apply_avg;
			a->sync_100g_secs = 100.0 * 1024 / a->apply_avg;
			a->sync_200g_secs = 200.0 * 1024 / a->apply_avg;
		}
		a->net_catchup_mbps =
			(a->send_avg < a->apply_avg ? a->send_avg : a->apply_avg) - a->gen_avg;
	}
}

/* ----------------------------------------------------------------
 * JSON export
 * ----------------------------------------------------------------
 */

static StringInfo
build_json(LRExportData *d, LRAnalysis *a)
{
	StringInfo s = makeStringInfo();
	int i;

	appendStringInfoString(s, "{\n");

	/* session */
	appendStringInfo(s, "  \"session\": {\n");
	appendStringInfo(s, "    \"name\": \"%s\",\n", d->session_name);
	appendStringInfo(s, "    \"running\": %s,\n", d->session_running ? "true" : "false");
	if (d->start_ts > 0)
		appendStringInfo(s, "    \"start\": \"%s\",\n",
						 timestamptz_to_str(d->start_ts));
	if (d->stop_ts > 0)
		appendStringInfo(s, "    \"stop\": \"%s\",\n",
						 timestamptz_to_str(d->stop_ts));
	appendStringInfo(s, "    \"truncated\": %s,\n", d->truncated ? "true" : "false");
	appendStringInfo(s, "    \"degraded\": %s\n", d->degraded ? "true" : "false");
	appendStringInfo(s, "  },\n");

	/* analysis */
	appendStringInfo(s, "  \"analysis\": {\n");
	appendStringInfo(s, "    \"bottleneck\": \"%s\",\n", a->bottleneck);
	appendStringInfo(s, "    \"deep_cause\": \"%s\",\n", a->deep_cause);
	appendStringInfo(s, "    \"gen_avg\": %.4f,\n", a->gen_avg);
	appendStringInfo(s, "    \"send_avg\": %.4f,\n", a->send_avg);
	appendStringInfo(s, "    \"recv_avg\": %.4f,\n", a->recv_avg);
	appendStringInfo(s, "    \"apply_avg\": %.4f,\n", a->apply_avg);
	appendStringInfo(s, "    \"backlog_unsent_mb\": %.1f,\n", a->backlog_unsent_mb);
	appendStringInfo(s, "    \"backlog_inflight_mb\": %.1f,\n", a->backlog_inflight_mb);
	appendStringInfo(s, "    \"backlog_unapplied_mb\": %.1f,\n", a->backlog_unapplied_mb);
	appendStringInfo(s, "    \"backlog_total_mb\": %.1f\n", a->backlog_total_mb);
	appendStringInfo(s, "  },\n");

	/* capacity */
	appendStringInfo(s, "  \"capacity\": {\n");
	appendStringInfo(s, "    \"catchup_secs\": %.0f,\n", a->catchup_secs);
	appendStringInfo(s, "    \"net_catchup_mbps\": %.4f,\n", a->net_catchup_mbps);
	appendStringInfo(s, "    \"sync_50g_secs\": %.0f,\n", a->sync_50g_secs);
	appendStringInfo(s, "    \"sync_100g_secs\": %.0f,\n", a->sync_100g_secs);
	appendStringInfo(s, "    \"sync_200g_secs\": %.0f\n", a->sync_200g_secs);
	appendStringInfo(s, "  },\n");

	/* send_stat */
	appendStringInfo(s, "  \"send_stat\": [\n");
	for (i = 0; i < d->n_targets; i++)
	{
		LRExportTarget *t = &d->targets[i];
		if (t->kind != LR_SEND && t->kind != LR_RSEND) continue;
		appendStringInfo(s, "    {\"name\": \"%s\", \"kind\": \"%s\","
						 " \"state\": \"%s\", \"active\": %s,"
						 " \"wal_status\": \"%s\"",
						 t->name, t->kind == LR_SEND ? "send" : "rsend",
						 t->meta.state, t->meta.active ? "true" : "false",
						 t->meta.wal_status);
		if (t->has_prev)
		{
			double dt = (double)(t->last.send.ts - t->prev.send.ts) / 1e6;
			if (dt > 0)
			{
				int64 dg = (int64)(t->last.send.current_lsn - t->prev.send.current_lsn);
				int64 ds = (int64)(t->last.send.sent_lsn - t->prev.send.sent_lsn);
				appendStringInfo(s, ", \"gen_instant\": %.4f, \"send_instant\": %.4f",
								 dg > 0 ? (double)dg/dt/MB_DIV : 0,
								 ds > 0 ? (double)ds/dt/MB_DIV : 0);
			}
		}
		appendStringInfo(s, "}%s\n", i < d->n_targets - 1 ? "," : "");
	}
	appendStringInfo(s, "  ],\n");

	/* recv_stat */
	appendStringInfo(s, "  \"recv_stat\": [\n");
	{
		bool first = true;
		for (i = 0; i < d->n_targets; i++)
		{
			LRExportTarget *t = &d->targets[i];
			if (t->kind != LR_RECV) continue;
			if (!first) appendStringInfoString(s, ",\n");
			first = false;
			appendStringInfo(s, "    {\"name\": \"%s\", \"worker_type\": \"%s\"}",
							 t->name, t->meta.worker_type);
		}
		appendStringInfo(s, "\n  ],\n");
	}

	/* history: raw samples (most recent EXPORT_MAX_HISTORY) */
	appendStringInfo(s, "  \"history\": {\n");
	appendStringInfo(s, "    \"total_samples\": %d,\n", d->total_entries);
	appendStringInfo(s, "    \"exported_samples\": %d,\n", d->n_entries);
	appendStringInfo(s, "    \"samples\": [\n");
	for (i = 0; i < d->n_entries; i++)
	{
		LRHistoryEntry *e = &d->entries[i];
		const LRExportTarget *t = find_target_by_ctl(d, e->target_idx);
		bool is_recv = (t != NULL && t->kind == LR_RECV);

		appendStringInfo(s, "    {\"name\": \"%s\", \"ts\": \"%s\","
						 " \"kind\": \"%s\",",
						 t ? t->name : "?",
						 timestamptz_to_str(e->ts),
						 is_recv ? "recv" : "send");
		if (is_recv)
			appendStringInfo(s,
							 " \"received_lsn\": \"%s\","
							 " \"applied_lsn\": \"%s\","
							 " \"local_wal_lsn\": \"%s\"}",
							 lsn_str(e->received_lsn),
							 lsn_str(e->applied_lsn),
							 lsn_str(e->local_wal_lsn));
		else
			appendStringInfo(s,
							 " \"current_lsn\": \"%s\","
							 " \"sent_lsn\": \"%s\","
							 " \"confirmed_flush_lsn\": \"%s\","
							 " \"restart_lsn\": \"%s\","
							 " \"spill_bytes\": " UINT64_FORMAT ","
							 " \"stream_bytes\": " UINT64_FORMAT "}",
							 lsn_str(e->current_lsn),
							 lsn_str(e->sent_lsn),
							 lsn_str(e->confirmed_lsn),
							 lsn_str(e->restart_lsn),
							 e->spill_bytes,
							 e->stream_bytes);
		appendStringInfo(s, "%s\n", i < d->n_entries - 1 ? "," : "");
	}
	appendStringInfo(s, "    ]\n  }\n");

	appendStringInfoString(s, "}\n");
	return s;
}

/* ----------------------------------------------------------------
 * HTML export (self-contained, inline SVG)
 * ----------------------------------------------------------------
 */

/*
 * One chart point: an interval rate derived from two consecutive
 * raw samples of the same target.
 */
typedef struct LRSeriesPoint
{
	TimestampTz ts;
	double      mbps;
} LRSeriesPoint;

/* field: 0 = send current_lsn (gen), 1 = send sent_lsn, 2 = recv applied_lsn */
static void
series_for_target(const LRExportData *d, int ctl_idx, int field,
				  LRSeriesPoint **pts_out, int *n_out)
{
	LRSeriesPoint *pts = palloc(Max(d->n_entries, 1) * sizeof(LRSeriesPoint));
	LRHistoryEntry prev;
	bool has_prev = false;
	int n = 0;
	int i;

	for (i = 0; i < d->n_entries; i++)
	{
		LRHistoryEntry *e = &d->entries[i];

		if (e->target_idx != ctl_idx)
			continue;
		if (has_prev)
		{
			double dt = (double)(e->ts - prev.ts) / 1e6;

			if (dt > 0)
			{
				int64		dv;
				double		mbps = 0;

				switch (field)
				{
					case 0:
						dv = (int64)(e->current_lsn - prev.current_lsn); break;
					case 1:
						dv = (int64)(e->sent_lsn - prev.sent_lsn); break;
					default:
						dv = (int64)(e->applied_lsn - prev.applied_lsn); break;
				}
				if (dv > 0)
					mbps = (double)dv / dt / MB_DIV;
				pts[n].ts = e->ts;
				pts[n].mbps = mbps;
				n++;
			}
		}
		prev = *e;
		has_prev = true;
	}

	*pts_out = pts;
	*n_out = n;
}

static void
polyline_series(StringInfo s, LRSeriesPoint *pts, int n, const char *color,
				int width, int height, int margin, int max_n, double yscale)
{
	int i;
	double xstep = max_n > 1 ? (double)(width - 2 * margin) / (max_n - 1) : 1;

	appendStringInfo(s,
					 "  <polyline fill='none' stroke='%s' stroke-width='2' points='",
					 color);
	for (i = 0; i < n; i++)
		appendStringInfo(s, "%s%.1f,%.1f", i > 0 ? " " : "",
						 margin + i * xstep,
						 height - margin - pts[i].mbps * yscale);
	appendStringInfoString(s, "'/>\n");
}

static void
svg_rates(StringInfo s, LRExportData *d, LRAnalysis *a)
{
	int width = 800, height = 300, margin = 40;
	LRSeriesPoint *gen = NULL, *snd = NULL, *app = NULL;
	int n_gen = 0, n_snd = 0, n_app = 0, max_n;
	double max_mb = 0.001;
	int i;

	if (a->chart_send_ctl >= 0)
	{
		series_for_target(d, a->chart_send_ctl, 0, &gen, &n_gen);
		series_for_target(d, a->chart_send_ctl, 1, &snd, &n_snd);
	}
	if (a->chart_recv_ctl >= 0)
		series_for_target(d, a->chart_recv_ctl, 2, &app, &n_app);

	for (i = 0; i < n_gen; i++) if (gen[i].mbps > max_mb) max_mb = gen[i].mbps;
	for (i = 0; i < n_snd; i++) if (snd[i].mbps > max_mb) max_mb = snd[i].mbps;
	for (i = 0; i < n_app; i++) if (app[i].mbps > max_mb) max_mb = app[i].mbps;

	appendStringInfo(s, "<svg width='%d' height='%d' xmlns='http://www.w3.org/2000/svg'>\n",
					 width, height);
	appendStringInfo(s, "  <rect width='100%%' height='100%%' fill='#fafafa'/>\n");
	appendStringInfo(s, "  <text x='%d' y='20' font-size='14' font-weight='bold'>Rates (MB/s)</text>\n",
					 margin);
	appendStringInfo(s, "  <line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#333'/>",
					 margin, height - margin, width - margin, height - margin);
	appendStringInfo(s, "  <line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#333'/>",
					 margin, margin, margin, height - margin);

	max_n = Max(n_gen, Max(n_snd, n_app));
	if (max_n > 1)
	{
		double yscale = (double)(height - 2 * margin) / max_mb;

		polyline_series(s, gen, n_gen, "#2563eb", width, height, margin, max_n, yscale);
		polyline_series(s, snd, n_snd, "#dc2626", width, height, margin, max_n, yscale);
		polyline_series(s, app, n_app, "#16a34a", width, height, margin, max_n, yscale);
	}

	/* legend */
	appendStringInfo(s, "  <line x1='%d' y1='30' x2='%d' y2='30' stroke='#2563eb' stroke-width='2'/>",
					 width - 200, width - 180);
	appendStringInfo(s, "  <text x='%d' y='34' font-size='11'>gen</text>", width - 170);
	appendStringInfo(s, "  <line x1='%d' y1='30' x2='%d' y2='30' stroke='#dc2626' stroke-width='2'/>",
					 width - 130, width - 110);
	appendStringInfo(s, "  <text x='%d' y='34' font-size='11'>send</text>", width - 100);
	appendStringInfo(s, "  <line x1='%d' y1='30' x2='%d' y2='30' stroke='#16a34a' stroke-width='2'/>",
					 width - 60, width - 40);
	appendStringInfo(s, "  <text x='%d' y='34' font-size='11'>apply</text>", width - 30);

	appendStringInfoString(s, "</svg>\n");

	if (gen) pfree(gen);
	if (snd) pfree(snd);
	if (app) pfree(app);
}

/*
 * Per-interval rate evidence table: derives each interval's rate from
 * two consecutive samples of the same target, exactly the way an
 * auditor would recompute it by hand from the history tables below.
 */
static void
html_rate_table(StringInfo s, const LRExportData *d, bool recv_side)
{
	const char *title = recv_side ? "recv rate history (evidence)"
		: "send rate history (evidence)";
	int i;

	appendStringInfo(s,
		"<div class='card'>\n<h2>%s</h2>\n"
		"<table><tr><th>ts</th><th>target</th><th>interval s</th>",
		title);
	if (recv_side)
		appendStringInfoString(s,
			"<th>recv MB/s</th><th>apply MB/s</th></tr>\n");
	else
		appendStringInfoString(s,
			"<th>gen MB/s</th><th>send MB/s</th><th>feedback apply MB/s</th>"
			"<th>spill MB</th></tr>\n");

	for (i = 0; i < d->n_entries; i++)
	{
		const LRHistoryEntry *e = &d->entries[i];
		const LRExportTarget *t = find_target_by_ctl(d, e->target_idx);
		const LRHistoryEntry *prev = NULL;
		int j;

		/* find the previous sample of the same target */
		for (j = i - 1; j >= 0; j--)
			if (d->entries[j].target_idx == e->target_idx)
			{ prev = &d->entries[j]; break; }
		if (prev == NULL)
			continue;

		{
			double ival = (double)(e->ts - prev->ts) / 1e6;
			double r1, r2, r3, spill;

			if (ival <= 0)
				continue;
			if (recv_side)
			{
				if (t == NULL || t->kind != LR_RECV)
					continue;
				r1 = (double)(e->received_lsn - prev->received_lsn) / ival / MB_DIV;
				r2 = (double)(e->applied_lsn - prev->applied_lsn) / ival / MB_DIV;
				appendStringInfo(s, "<tr><td>%s</td><td>%s</td><td>%.1f</td>"
								 "<td>%.3f</td><td>%.3f</td></tr>\n",
								 timestamptz_to_str(e->ts),
								 t ? t->name : "?", ival,
								 r1 > 0 ? r1 : 0, r2 > 0 ? r2 : 0);
			}
			else
			{
				if (t == NULL || t->kind == LR_RECV)
					continue;
				r1 = (double)(e->current_lsn - prev->current_lsn) / ival / MB_DIV;
				r2 = (double)(e->sent_lsn - prev->sent_lsn) / ival / MB_DIV;
				r3 = (double)(e->peer_applied_lsn - prev->peer_applied_lsn) / ival / MB_DIV;
				spill = (double)(e->spill_bytes - prev->spill_bytes) / MB_DIV;
				appendStringInfo(s, "<tr><td>%s</td><td>%s</td><td>%.1f</td>"
								 "<td>%.3f</td><td>%.3f</td><td>%.3f</td><td>%.3f</td></tr>\n",
								 timestamptz_to_str(e->ts),
								 t ? t->name : "?", ival,
								 r1 > 0 ? r1 : 0, r2 > 0 ? r2 : 0,
								 r3 > 0 ? r3 : 0, spill > 0 ? spill : 0);
			}
		}
	}
	appendStringInfoString(s, "</table>\n</div>\n");
}

static StringInfo
build_html(LRExportData *d, LRAnalysis *a)
{
	StringInfo s = makeStringInfo();

	appendStringInfoString(s,
		"<!DOCTYPE html>\n<html>\n<head>\n<meta charset='utf-8'>\n"
		"<title>pg_lrstat: ");
	appendStringInfoString(s, d->session_name);
	appendStringInfoString(s,
		"</title>\n<style>\n"
		"body{font-family:system-ui,sans-serif;margin:20px;background:#f5f5f5;}\n"
		".card{background:#fff;border-radius:8px;padding:16px;margin-bottom:16px;"
		"box-shadow:0 1px 3px rgba(0,0,0,.1);}\n"
		".label{font-size:12px;color:#666;}\n"
		".value{font-size:24px;font-weight:bold;}\n"
		".bottleneck{font-size:28px;font-weight:bold;padding:8px 16px;"
		"border-radius:4px;display:inline-block;}\n"
		".b-send{background:#fee2e2;color:#991b1b;}\n"
		".b-recv{background:#fef3c7;color:#92400e;}\n"
		".b-net{background:#e0e7ff;color:#3730a3;}\n"
		".b-none{background:#d1fae5;color:#065f46;}\n"
		"table{border-collapse:collapse;width:100%;}\n"
		"th,td{border:1px solid #e5e7eb;padding:6px 10px;text-align:left;}\n"
		"th{background:#f9fafb;}\n"
		"</style>\n</head>\n<body>\n");

	/* Analysis card */
	appendStringInfoString(s, "<div class='card'>\n<h2>Analysis</h2>\n");
	{
		const char *cls = strcmp(a->bottleneck, "none") == 0 ? "b-none"
			: strcmp(a->bottleneck, "send") == 0 ? "b-send"
			: strcmp(a->bottleneck, "recv_apply") == 0 ? "b-recv" : "b-net";
		appendStringInfo(s, "<span class='bottleneck %s'>%s</span>",
						 cls, a->bottleneck);
		if (a->deep_cause[0])
			appendStringInfo(s, " <span class='label'>cause: %s</span>", a->deep_cause);
	}
	appendStringInfoString(s, "<table><tr>");
	appendStringInfo(s, "<th>gen avg</th><td>%.2f MB/s</td>", a->gen_avg);
	appendStringInfo(s, "<th>send avg</th><td>%.2f MB/s</td>", a->send_avg);
	appendStringInfo(s, "<th>recv avg</th><td>%.2f MB/s</td>", a->recv_avg);
	appendStringInfo(s, "<th>apply avg</th><td>%.2f MB/s</td>", a->apply_avg);
	appendStringInfoString(s, "</tr><tr>");
	appendStringInfo(s, "<th>unsent</th><td>%.1f MB</td>", a->backlog_unsent_mb);
	appendStringInfo(s, "<th>inflight</th><td>%.1f MB</td>", a->backlog_inflight_mb);
	appendStringInfo(s, "<th>unapplied</th><td>%.1f MB</td>", a->backlog_unapplied_mb);
	appendStringInfo(s, "<th>total</th><td>%.1f MB</td>", a->backlog_total_mb);
	appendStringInfoString(s, "</tr><tr>");
	appendStringInfo(s, "<th>catchup</th><td>%.0f s</td>", a->catchup_secs);
	appendStringInfo(s, "<th>net catchup</th><td>%.2f MB/s %s</td>",
					 a->net_catchup_mbps,
					 a->net_catchup_mbps >= 0 ? "(can catch up)" : "(CANNOT catch up)");
	appendStringInfoString(s, "</tr></table>\n");

	/* capacity table */
	appendStringInfoString(s,
		"<h3>Capacity extrapolation (at avg apply rate)</h3><table>"
		"<tr><th>Sync amount</th><th>Estimated time</th></tr>");
	if (a->catchup_secs > 0)
		appendStringInfo(s, "<tr><td>Current backlog (%.1f MB)</td><td>%.0f s</td></tr>",
						 a->backlog_unapplied_mb, a->catchup_secs);
	if (a->sync_50g_secs > 0)
	{
		appendStringInfo(s, "<tr><td>50 GB</td><td>%.1f h</td></tr>", a->sync_50g_secs / 3600);
		appendStringInfo(s, "<tr><td>100 GB</td><td>%.1f h</td></tr>", a->sync_100g_secs / 3600);
		appendStringInfo(s, "<tr><td>200 GB</td><td>%.1f h</td></tr>", a->sync_200g_secs / 3600);
	}
	appendStringInfoString(s, "</table>\n</div>\n");

	/* charts, right after the analysis */
	appendStringInfoString(s, "<div class='card'>\n");
	svg_rates(s, d, a);
	appendStringInfoString(s, "</div>\n");

	/* session info card */
	appendStringInfoString(s, "<div class='card'>\n<h2>Session</h2>\n");
	appendStringInfo(s, "<p><b>%s</b> &mdash; %s", d->session_name,
					 d->session_running ? "running" : "stopped");
	if (d->start_ts > 0)
		appendStringInfo(s, " from %s", timestamptz_to_str(d->start_ts));
	if (d->stop_ts > 0)
		appendStringInfo(s, " to %s", timestamptz_to_str(d->stop_ts));
	appendStringInfo(s, " &mdash; %d targets, %d samples",
					 d->n_targets, d->total_entries);
	if (d->truncated) appendStringInfoString(s, " <b>[truncated]</b>");
	if (d->degraded) appendStringInfoString(s, " <b>[degraded]</b>");
	appendStringInfoString(s, "</p>\n</div>\n");

	/* per-target rates card */
	appendStringInfoString(s,
		"<div class='card'>\n<h2>Targets</h2>\n"
		"<table><tr><th>target</th><th>side</th><th>state</th>"
		"<th>avg MB/s</th><th>last LSN</th></tr>\n");
	{
		int i;

		for (i = 0; i < d->n_targets; i++)
		{
			LRExportTarget *t = &d->targets[i];
			bool		is_recv = (t->kind == LR_RECV);

			if (is_recv)
			{
				double rdt = (double)(t->last.recv.ts - t->anchor.recv.ts) / 1e6;
				double rmb = (double)(t->last.recv.received_lsn -
									  t->anchor.recv.received_lsn) / MB_DIV;
				double amb = (double)(t->last.recv.applied_lsn -
									  t->anchor.recv.applied_lsn) / MB_DIV;

				if (rdt <= 0) rdt = 1.0;
				appendStringInfo(s, "<tr><td>%s</td><td>recv</td><td>%s</td>"
								 "<td>recv %.2f / apply %.2f</td><td>%s</td></tr>\n",
								 t->name,
								 t->meta.worker_type[0] ? t->meta.worker_type : "-",
								 rmb / rdt, amb / rdt,
								 t->last.recv.applied_lsn ?
								 lsn_str(t->last.recv.applied_lsn) : "-");
			}
			else
			{
				double dt = (double)(t->last.send.ts - t->anchor.send.ts) / 1e6;
				double gmb = (double)(t->last.send.current_lsn -
									  t->anchor.send.current_lsn) / MB_DIV;
				double smb = (double)(t->last.send.sent_lsn -
									  t->anchor.send.sent_lsn) / MB_DIV;

				if (dt <= 0) dt = 1.0;
				appendStringInfo(s, "<tr><td>%s</td><td>%s</td><td>%s</td>"
								 "<td>gen %.2f / send %.2f</td><td>%s</td></tr>\n",
								 t->name,
								 t->kind == LR_RSEND ? "send (polled)" : "send",
								 t->meta.state[0] ? t->meta.state : "-",
								 gmb / dt, smb / dt,
								 t->last.send.sent_lsn ?
								 lsn_str(t->last.send.sent_lsn) : "-");
			}
		}
	}
	appendStringInfoString(s, "</table>\n</div>\n");

	/*
	 * Evidence section at the bottom: the raw samples and the derived
	 * per-interval rates each conclusion is built on, so any number in
	 * the report can be recomputed by hand.
	 */
	appendStringInfoString(s,
		"<h1>Evidence</h1>\n<p class='label'>the raw samples and "
		"per-interval rates behind the analysis above</p>\n");

	/* send history table */
	if (d->n_entries > 0)
	{
		int i;
		appendStringInfo(s,
			"<div class='card'>\n<h2>send history samples</h2>\n"
			"<p class='label'>showing the most recent %d of %d samples</p>\n"
			"<table><tr><th>ts</th><th>target</th>"
			"<th>current_lsn</th><th>sent_lsn</th><th>confirmed</th>"
			"<th>restart_lsn</th><th>spill MB</th><th>stream MB</th></tr>\n",
			d->n_entries, d->total_entries);
		for (i = 0; i < d->n_entries; i++)
		{
			LRHistoryEntry *e = &d->entries[i];
			const LRExportTarget *t = find_target_by_ctl(d, e->target_idx);

			if (t != NULL && t->kind == LR_RECV)
				continue;
			appendStringInfo(s, "<tr><td>%s</td><td>%s</td>"
							 "<td>%s</td><td>%s</td><td>%s</td><td>%s</td>"
							 "<td>%.1f</td><td>%.1f</td></tr>\n",
							 timestamptz_to_str(e->ts),
							 t ? t->name : "?",
							 lsn_str(e->current_lsn),
							 lsn_str(e->sent_lsn),
							 lsn_str(e->confirmed_lsn),
							 lsn_str(e->restart_lsn),
							 (double) e->spill_bytes / MB_DIV,
							 (double) e->stream_bytes / MB_DIV);
		}
		appendStringInfoString(s, "</table>\n</div>\n");
	}

	/* recv history table */
	if (d->n_entries > 0)
	{
		int i;
		appendStringInfo(s,
			"<div class='card'>\n<h2>recv history samples</h2>\n"
			"<table><tr><th>ts</th><th>target</th>"
			"<th>received_lsn</th><th>applied_lsn</th><th>local_wal_lsn</th></tr>\n");
		for (i = 0; i < d->n_entries; i++)
		{
			LRHistoryEntry *e = &d->entries[i];
			const LRExportTarget *t = find_target_by_ctl(d, e->target_idx);

			if (t == NULL || t->kind != LR_RECV)
				continue;
			appendStringInfo(s, "<tr><td>%s</td><td>%s</td>"
							 "<td>%s</td><td>%s</td><td>%s</td></tr>\n",
							 timestamptz_to_str(e->ts),
							 t ? t->name : "?",
							 lsn_str(e->received_lsn),
							 lsn_str(e->applied_lsn),
							 lsn_str(e->local_wal_lsn));
		}
		appendStringInfoString(s, "</table>\n</div>\n");
	}

	/* per-interval rate evidence tables */
	html_rate_table(s, d, false);
	html_rate_table(s, d, true);

	appendStringInfoString(s, "</body>\n</html>\n");
	return s;
}


/*
 * List the report files in $PGDATA/pg_lrstat/exports (for the info
 * view's exported_report_names column) — the reports ARE the durable
 * artifact of a session now.
 */
int
lrstat_export_list(char ***names_out)
{
	char	   *dir_path = psprintf("%s/" EXPORT_DIR, DataDir);
	DIR		   *dir;
	struct dirent *de;
	char	  **names = NULL;
	int			n = 0, nalloc = 0;

	*names_out = NULL;
	dir = AllocateDir(dir_path);
	if (dir == NULL)
		return 0;

	while ((de = ReadDir(dir, dir_path)) != NULL)
	{
		char	   *fname = de->d_name;
		size_t		len = strlen(fname);

		/* strip .html / .json suffix, dedup by report name */
		if (len > 5 && (strcmp(fname + len - 5, ".html") == 0 ||
						strcmp(fname + len - 5, ".json") == 0))
		{
			char	base[NAMEDATALEN];
			int		i;
			bool	dup = false;

			strlcpy(base, fname, (int) Min(len - 4, NAMEDATALEN));
			for (i = 0; i < n; i++)
				if (strcmp(names[i], base) == 0)
				{ dup = true; break; }
			if (dup)
				continue;

			if (n >= nalloc)
			{
				nalloc = nalloc == 0 ? 8 : nalloc * 2;
				if (names == NULL)
					names = palloc(nalloc * sizeof(char *));
				else
				{
					char **grow = palloc(nalloc * sizeof(char *));

					memcpy(grow, names, n * sizeof(char *));
					pfree(names);
					names = grow;
				}
			}
			names[n++] = pstrdup(base);
		}
	}

	FreeDir(dir);
	*names_out = names;
	return n;
}

/* ----------------------------------------------------------------
 * lrstat_export entry point: writes the report to
 * $PGDATA/pg_lrstat/exports/<name>.<format> and returns the path.
 * ----------------------------------------------------------------
 */

/* only allow filesystem-safe characters in the report file name */
static void
sanitize_name(const char *in, char *out, Size outlen)
{
	Size		o = 0;

	for (; *in && o + 1 < outlen; in++)
	{
		char		c = *in;

		if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-' || c == '.')
			out[o++] = c;
		else
			out[o++] = '_';
	}
	out[o] = '\0';
	if (o == 0)
		strlcpy(out, "session", outlen);
}

PG_FUNCTION_INFO_V1(lrstat_export);
Datum
lrstat_export(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_PP(0);
	text	   *format_arg = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_PP(1);
	char	   *format = format_arg ? text_to_cstring(format_arg) : "html";
	char	   *sess = "session";
	char	   *dir1;
	char	   *dir2;
	char	   *path;
	char		safe[NAMEDATALEN];
	FILE	   *f;
	LRExportData data;
	LRAnalysis analysis;
	StringInfo result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser")));
	if (!lrstat_ready())
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lrstat not loaded")));
	if (strcmp(format, "json") != 0 && strcmp(format, "html") != 0)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported format '%s' (use 'html' or 'json')",
							   format)));

	if (name_arg != NULL)
		sess = text_to_cstring(name_arg);

	/*
	 * Export by name: the current session, or a remembered session
	 * still in the in-memory ring; anything else is an error.  The
	 * written report files are the durable artifact — export right
	 * after stop to keep a session.
	 */
	if (name_arg == NULL)
		gather_data(&data);
	else
	{
		char name[NAMEDATALEN];
		char cur[NAMEDATALEN];

		strlcpy(name, sess, NAMEDATALEN);
		SpinLockAcquire(&lrstat->session.mutex);
		strlcpy(cur, lrstat->session.name, NAMEDATALEN);
		SpinLockRelease(&lrstat->session.mutex);

		if (strcmp(name, cur) == 0)
			gather_data(&data);
		else if (!gather_memory_by_name(&data, name))
			ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
							errmsg("pg_lrstat: no data for session '%s' "
								   "in the in-memory ring (older sessions "
								   "age out; run lrstat_export right after "
								   "stop to keep a report)", name)));
	}

	compute_analysis(&data, &analysis);

	if (strcmp(format, "json") == 0)
		result = build_json(&data, &analysis);
	else
		result = build_html(&data, &analysis);

	/* write the report to $PGDATA/pg_lrstat/exports/<name>.<format> */
	sanitize_name(sess, safe, sizeof(safe));
	dir1 = psprintf("%s/pg_lrstat", DataDir);
	dir2 = psprintf("%s/" EXPORT_DIR, DataDir);
	if (MakePGDirectory(dir1) < 0 && errno != EEXIST)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("pg_lrstat: could not create directory %s", dir1)));
	if (MakePGDirectory(dir2) < 0 && errno != EEXIST)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("pg_lrstat: could not create directory %s", dir2)));

	path = psprintf("%s/" EXPORT_DIR "/%s.%s", DataDir, safe, format);
	f = AllocateFile(path, PG_BINARY_W);
	if (f == NULL)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("pg_lrstat: could not open report file %s", path)));
	if (fwrite(result->data, 1, result->len, f) != result->len)
	{
		FreeFile(f);
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("pg_lrstat: could not write report file %s", path)));
	}
	if (FreeFile(f) != 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("pg_lrstat: could not close report file %s", path)));

	ereport(NOTICE, (errmsg("pg_lrstat: report written to %s", path)));
	PG_RETURN_DATUM(CStringGetTextDatum(path));
}
