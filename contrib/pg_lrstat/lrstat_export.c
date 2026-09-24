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

#include "funcapi.h"
#include "miscadmin.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"

#include "lrstat.h"

#define MB_DIV (1024.0 * 1024.0)
#define EXPORT_DIR "pg_lrstat/exports"

/* ----------------------------------------------------------------
 * Data gathering: snapshot of everything needed for the report
 * ----------------------------------------------------------------
 */

typedef struct LRExportTarget
{
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
	int         n_entries;
	LRSessionEntry *entries;       /* palloc'd copy */
} LRExportData;

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

	d->n_entries = lrstat_get_entry_count();
	if (d->n_entries > 0)
	{
		d->entries = palloc(d->n_entries * sizeof(LRSessionEntry));
		for (i = 0; i < d->n_entries; i++)
			memcpy(&d->entries[i], lrstat_entry_at(i), sizeof(LRSessionEntry));
	}
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

	if (d->n_targets == 0)
	{
		a->has_data = false;
		return;
	}
	a->has_data = true;

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

	{
		const LRSendSample *rs = &rsend_t->last.send;
		const LRRecvSample *rv = &recv_t->last.recv;
		double dt;
		int64 d_gen, d_send, d_recv, d_apply;
		int64 unsent, inflight, unapplied, total;

		/* avg rates from anchor to last */
		dt = (double)(rsend_t->last.send.ts - rsend_t->anchor.send.ts) / 1e6;
		if (dt <= 0) dt = 1.0;

		d_gen = (int64)(rs->current_lsn - rsend_t->anchor.send.current_lsn);
		d_send = (int64)(rs->sent_lsn - rsend_t->anchor.send.sent_lsn);
		a->gen_avg = d_gen > 0 ? (double)d_gen / dt / MB_DIV : 0;
		a->send_avg = d_send > 0 ? (double)d_send / dt / MB_DIV : 0;

		{
			double rdt = (double)(recv_t->last.recv.ts - recv_t->anchor.recv.ts) / 1e6;
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
	appendStringInfo(s, "    \"gen_avg\": %.2f,\n", a->gen_avg);
	appendStringInfo(s, "    \"send_avg\": %.2f,\n", a->send_avg);
	appendStringInfo(s, "    \"recv_avg\": %.2f,\n", a->recv_avg);
	appendStringInfo(s, "    \"apply_avg\": %.2f,\n", a->apply_avg);
	appendStringInfo(s, "    \"backlog_unsent_mb\": %.1f,\n", a->backlog_unsent_mb);
	appendStringInfo(s, "    \"backlog_inflight_mb\": %.1f,\n", a->backlog_inflight_mb);
	appendStringInfo(s, "    \"backlog_unapplied_mb\": %.1f,\n", a->backlog_unapplied_mb);
	appendStringInfo(s, "    \"backlog_total_mb\": %.1f\n", a->backlog_total_mb);
	appendStringInfo(s, "  },\n");

	/* capacity */
	appendStringInfo(s, "  \"capacity\": {\n");
	appendStringInfo(s, "    \"catchup_secs\": %.0f,\n", a->catchup_secs);
	appendStringInfo(s, "    \"net_catchup_mbps\": %.2f,\n", a->net_catchup_mbps);
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
				appendStringInfo(s, ", \"gen_instant\": %.2f, \"send_instant\": %.2f",
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

	/* history (per-interval) */
	appendStringInfo(s, "  \"history\": [\n");
	for (i = 0; i < d->n_entries; i++)
	{
		LRSessionEntry *e = &d->entries[i];
		if (e->target_idx >= 0 && e->target_idx < d->n_targets)
		{
			appendStringInfo(s,
							 "    {\"name\": \"%s\", \"ts\": \"%s\","
							 " \"d_current_mb\": %.2f, \"d_sent_mb\": %.2f,"
							 " \"d_received_mb\": %.2f, \"d_applied_mb\": %.2f}%s\n",
							 d->targets[e->target_idx].name,
							 timestamptz_to_str(e->ts),
							 (double)e->d_current / MB_DIV,
							 (double)e->d_sent / MB_DIV,
							 (double)e->d_received / MB_DIV,
							 (double)e->d_applied / MB_DIV,
							 i < d->n_entries - 1 ? "," : "");
		}
	}
	appendStringInfo(s, "  ]\n");

	appendStringInfoString(s, "}\n");
	return s;
}

/* ----------------------------------------------------------------
 * HTML export (self-contained, inline SVG)
 * ----------------------------------------------------------------
 */

static void
svg_rates(StringInfo s, LRExportData *d)
{
	int i;
	int width = 800, height = 300, margin = 40;
	double max_mb = 0.001;

	/* find max for scaling */
	for (i = 0; i < d->n_entries; i++)
	{
		double v = (double)d->entries[i].d_current / MB_DIV;
		if (v > max_mb) max_mb = v;
		v = (double)d->entries[i].d_sent / MB_DIV;
		if (v > max_mb) max_mb = v;
		v = (double)d->entries[i].d_applied / MB_DIV;
		if (v > max_mb) max_mb = v;
	}

	appendStringInfo(s, "<svg width='%d' height='%d' xmlns='http://www.w3.org/2000/svg'>\n",
					 width, height);
	appendStringInfo(s, "  <rect width='100%%' height='100%%' fill='#fafafa'/>\n");
	appendStringInfo(s, "  <text x='%d' y='20' font-size='14' font-weight='bold'>Rates (MB/s)</text>\n",
					 margin);
	appendStringInfo(s, "  <line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#333'/>",
					 margin, height - margin, width - margin, height - margin);
	appendStringInfo(s, "  <line x1='%d' y1='%d' x2='%d' y2='%d' stroke='#333'/>",
					 margin, margin, margin, height - margin);

	if (d->n_entries > 1)
	{
		double xstep = (double)(width - 2 * margin) / (d->n_entries - 1);
		double yscale = (double)(height - 2 * margin) / max_mb;

		/* gen line */
		appendStringInfoString(s, "  <polyline fill='none' stroke='#2563eb' stroke-width='2' points='");
		for (i = 0; i < d->n_entries; i++)
		{
			double v = (double)d->entries[i].d_current / MB_DIV;
			appendStringInfo(s, "%s%.1f,%.1f", i > 0 ? " " : "",
							 margin + i * xstep, height - margin - v * yscale);
		}
		appendStringInfoString(s, "'/>\n");

		/* sent line */
		appendStringInfoString(s, "  <polyline fill='none' stroke='#dc2626' stroke-width='2' points='");
		for (i = 0; i < d->n_entries; i++)
		{
			double v = (double)d->entries[i].d_sent / MB_DIV;
			appendStringInfo(s, "%s%.1f,%.1f", i > 0 ? " " : "",
							 margin + i * xstep, height - margin - v * yscale);
		}
		appendStringInfoString(s, "'/>\n");

		/* applied line */
		appendStringInfoString(s, "  <polyline fill='none' stroke='#16a34a' stroke-width='2' points='");
		for (i = 0; i < d->n_entries; i++)
		{
			double v = (double)d->entries[i].d_applied / MB_DIV;
			appendStringInfo(s, "%s%.1f,%.1f", i > 0 ? " " : "",
							 margin + i * xstep, height - margin - v * yscale);
		}
		appendStringInfoString(s, "'/>\n");
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

	/* session info card */
	appendStringInfoString(s, "<div class='card'>\n<h2>Session</h2>\n");
	appendStringInfo(s, "<p><b>%s</b> &mdash; %s", d->session_name,
					 d->session_running ? "running" : "stopped");
	if (d->start_ts > 0)
		appendStringInfo(s, " from %s", timestamptz_to_str(d->start_ts));
	if (d->stop_ts > 0)
		appendStringInfo(s, " to %s", timestamptz_to_str(d->stop_ts));
	appendStringInfo(s, " &mdash; %d targets, %d intervals",
					 d->n_targets, d->n_entries);
	if (d->truncated) appendStringInfoString(s, " <b>[truncated]</b>");
	if (d->degraded) appendStringInfoString(s, " <b>[degraded]</b>");
	appendStringInfoString(s, "</p>\n</div>\n");

	/* charts */
	appendStringInfoString(s, "<div class='card'>\n");
	svg_rates(s, d);
	appendStringInfoString(s, "</div>\n");

	/* raw history table */
	if (d->n_entries > 0)
	{
		int i;
		appendStringInfoString(s,
			"<div class='card'>\n<h2>Per-interval data</h2>\n"
			"<table><tr><th>ts</th><th>name</th>"
			"<th>d_current MB</th><th>d_sent MB</th>"
			"<th>d_received MB</th><th>d_applied MB</th></tr>\n");
		for (i = 0; i < d->n_entries; i++)
		{
			LRSessionEntry *e = &d->entries[i];
			const char *nm = (e->target_idx >= 0 && e->target_idx < d->n_targets)
				? d->targets[e->target_idx].name : "?";
			appendStringInfo(s, "<tr><td>%s</td><td>%s</td>"
							 "<td>%.2f</td><td>%.2f</td><td>%.2f</td><td>%.2f</td></tr>\n",
							 timestamptz_to_str(e->ts), nm,
							 (double)e->d_current / MB_DIV,
							 (double)e->d_sent / MB_DIV,
							 (double)e->d_received / MB_DIV,
							 (double)e->d_applied / MB_DIV);
		}
		appendStringInfoString(s, "</table>\n</div>\n");
	}

	appendStringInfoString(s, "</body>\n</html>\n");
	return s;
}

/* ----------------------------------------------------------------
 * lrstat_export entry point
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(lrstat_export);
Datum
lrstat_export(PG_FUNCTION_ARGS)
{
	text	   *name_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_PP(0);
	text	   *format_arg = PG_ARGISNULL(1) ? NULL : PG_GETARG_TEXT_PP(1);
	char	   *format = format_arg ? text_to_cstring(format_arg) : "html";
	LRExportData data;
	LRAnalysis analysis;
	StringInfo result;

	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("must be superuser")));
	if (!lrstat_ready())
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lrstat not loaded")));

	/* TODO: if a name is given and differs from current, read from file */
	if (name_arg != NULL)
	{
		char name[NAMEDATALEN];
		strlcpy(name, text_to_cstring(name_arg), NAMEDATALEN);
		if (strcmp(name, lrstat->session.name) != 0)
			ereport(NOTICE, (errmsg("exporting current session %s (archived reads P4)",
									 lrstat->session.name)));
	}

	gather_data(&data);
	compute_analysis(&data, &analysis);

	if (strcmp(format, "json") == 0)
		result = build_json(&data, &analysis);
	else if (strcmp(format, "html") == 0)
		result = build_html(&data, &analysis);
	else
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("unsupported format '%s' (use 'html' or 'json')",
							   format)));

	PG_RETURN_DATUM(CStringGetTextDatum(result->data));
}
