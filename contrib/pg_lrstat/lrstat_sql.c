/*-------------------------------------------------------------------------
 *
 * lrstat_sql.c
 *      SQL-facing set-returning functions behind the pg_lrstat views.
 *
 *      Views come in two flavours: sample views (the latest per-target
 *      facts, with sample_time) and rate views (window-differentiated
 *      rates in MB/s, with rate_time and the actual window borders).
 *      Rates are computed at query time from the ring history.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_sql.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "funcapi.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "lrstat.h"

#define LR_MAX_COLS 40
#define BYTES_PER_MB (1024.0 * 1024.0)

/* ----------------------------------------------------------------
 * Row building helpers
 * ----------------------------------------------------------------
 */

typedef struct LRRow
{
	Datum		v[LR_MAX_COLS];
	bool		isnull[LR_MAX_COLS];
	int			n;
} LRRow;

static void
lr_row_reset(LRRow *r)
{
	r->n = 0;
}

static void
lr_put(LRRow *r, Datum d, bool isnull)
{
	r->v[r->n] = d;
	r->isnull[r->n] = isnull;
	r->n++;
}

static void
lr_put_lsn(LRRow *r, XLogRecPtr lsn)
{
	lr_put(r, Int64GetDatum((int64) lsn), lsn == 0);
}

static void
lr_put_ts(LRRow *r, TimestampTz ts)
{
	lr_put(r, Int64GetDatum((int64) ts), ts == 0);
}

static void
lr_put_i8(LRRow *r, int64 v)
{
	lr_put(r, Int64GetDatum(v), false);
}

static void
lr_put_count(LRRow *r, int64 v)
{
	/* -1 is the "unknown" sentinel of the error counters */
	lr_put(r, Int64GetDatum(v), v < 0);
}

static void
lr_put_f8n(LRRow *r, bool valid, double v)
{
	lr_put(r, Float8GetDatum(valid ? v : 0), !valid);
}

static void
lr_put_i8n(LRRow *r, bool valid, int64 v)
{
	lr_put(r, Int64GetDatum(valid ? v : 0), !valid);
}

static void
lr_put_bool(LRRow *r, bool v)
{
	lr_put(r, BoolGetDatum(v), false);
}

static void
lr_put_text(LRRow *r, const char *s)
{
	bool		isnull = (s == NULL || s[0] == '\0');

	lr_put(r, isnull ? (Datum) 0 : CStringGetTextDatum(s), isnull);
}

static void
lr_put_pid(LRRow *r, pid_t pid)
{
	lr_put(r, Int32GetDatum((int32) pid), pid == 0);
}

static void
lr_put_lag(LRRow *r, int64 lag_us)
{
	if (lag_us < 0)
		lr_put(r, (Datum) 0, true);
	else
	{
		Interval   *iv = (Interval *) palloc0(sizeof(Interval));

		iv->time = lag_us;
		lr_put(r, PointerGetDatum(iv), false);
	}
}

static void
lr_put_eta(LRRow *r, int64 backlog, bool rate_ok, double rate_bps)
{
	if (rate_ok && backlog > 0 && rate_bps >= (double) lrstat_eta_min_rate)
		lr_put_f8n(r, true, (double) backlog / rate_bps);
	else
		lr_put_f8n(r, false, 0);
}

static void
lr_emit_row(ReturnSetInfo *rsinfo, LRRow *r)
{
	if (r->n != rsinfo->setDesc->natts)
		elog(ERROR, "pg_lrstat: internal column count mismatch (%d vs %d)",
			 r->n, rsinfo->setDesc->natts);
	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc,
						 r->v, r->isnull);
}

/* byte backlog a - b, clamped at zero */
static int64
lsn_diff(XLogRecPtr a, XLogRecPtr b)
{
	int64		d = (int64) a - (int64) b;

	return d < 0 ? 0 : d;
}

/* ----------------------------------------------------------------
 * Rate computation over the newest-first ring copy
 * ----------------------------------------------------------------
 */

typedef int64 (*LRFieldFn) (const LRSample *s);

#define LR_PUB_FIELD(fnname, member) \
	static int64 fnname(const LRSample *s) pg_attribute_unused(); \
	static int64 fnname(const LRSample *s) { return (int64) s->pub.member; }
#define LR_SUB_FIELD(fnname, member) \
	static int64 fnname(const LRSample *s) pg_attribute_unused(); \
	static int64 fnname(const LRSample *s) { return (int64) s->sub.member; }

LR_PUB_FIELD(f_pub_current, current_lsn)
LR_PUB_FIELD(f_pub_sent, sent_lsn)
LR_PUB_FIELD(f_pub_applied, peer_applied_lsn)
LR_PUB_FIELD(f_pub_confirmed, confirmed_lsn)
LR_PUB_FIELD(f_pub_spill, spill_bytes)
LR_PUB_FIELD(f_pub_stream, stream_bytes)

LR_SUB_FIELD(f_sub_latest_end, latest_end_lsn)
LR_SUB_FIELD(f_sub_applied, applied_lsn)
LR_SUB_FIELD(f_sub_origin_local, origin_local_lsn)
LR_SUB_FIELD(f_sub_local_wal, local_wal_lsn)

/* received position: received_lsn, falling back to latest_end_lsn */
static int64
f_sub_received(const LRSample *s)
{
	return (int64) (s->sub.received_lsn ? s->sub.received_lsn :
					s->sub.latest_end_lsn);
}

/*
 * Windowed rate in bytes/sec over the newest-first ring; also reports
 * the actual sample interval used.  Returns false when data is
 * insufficient or the newest sample is too old (sampling interrupted).
 */
static bool
lr_rate(const LRSample *ring, int n, LRFieldFn field,
		double *rate_bps, TimestampTz *t0, TimestampTz *t1)
{
	TimestampTz newest = ring[0].pub.ts;	/* ts is first in both structs */
	int64		window_us = (int64) lrstat_rate_window_ms * 1000;
	int			j = n - 1;
	double		dt;
	int64		d;

	if (newest + 3 * (int64) lrstat_sample_interval_ms * 1000
		< GetCurrentTimestamp())
		return false;

	/* oldest sample still inside the window */
	while (j > 0 && newest - ring[j].pub.ts > window_us)
		j--;

	dt = (double) (newest - ring[j].pub.ts) / 1000000.0;
	if (dt < 0.5 * lrstat_rate_window_ms / 1000.0)
		return false;

	d = field(&ring[0]) - field(&ring[j]);
	*rate_bps = d > 0 ? (double) d / dt : 0.0;
	*t0 = ring[j].pub.ts;
	*t1 = newest;
	return true;
}

static double
mbps(double rate_bps)
{
	return rate_bps / BYTES_PER_MB;
}

/* stalled = backlog while the responsible rate sits at ~zero */
static bool
lr_stalled(int64 backlog, bool rate_ok, double rate_bps)
{
	return backlog > 0 && rate_ok && rate_bps < 1.0;
}

/* ----------------------------------------------------------------
 * Target iteration
 * ----------------------------------------------------------------
 */

/*
 * One remotely-polled publisher target copied out under its spinlock,
 * used by pg_lrstat_overall to avoid re-scanning the whole target
 * table for every subscription row.
 */
typedef struct LRRPUBSnapEntry
{
	char		name[NAMEDATALEN];
	LRTargetMeta meta;
	int			n;
	LRSample   *ring;			/* points into the snapshot block */
} LRRPUBSnapEntry;

/*
 * Per-call context: name/since filters, the reusable sample scratch
 * buffer (so each query pallocs once instead of once per target), and
 * an optional prebuilt RPUB snapshot.  Lifetime is bounded by the SRF
 * invocation.
 */
typedef struct LRWalkCtx
{
	const char *filter_name;
	TimestampTz filter_since;
	LRSample   *scratch;
	int			max_samples;
	LRRPUBSnapEntry *rpubs;
	int			nrpubs;
} LRWalkCtx;

/*
 * Sample count a rate computation can ever need: the window span plus
 * slack for the partial samples at both ends.  Copying only this many
 * samples (instead of the whole ring) keeps the spinlock critical
 * section short even with a large raw_history_samples setting.
 */
static int
lr_rate_samples(void)
{
	int			need = lrstat_rate_window_ms / lrstat_sample_interval_ms + 2;

	return Max(Min(need, lrstat_ring_len), 2);
}

typedef void (*LREmitFn) (const char *name, char worker_char, Oid relid,
						  const LRSample *ring, int n,
						  const LRTargetMeta *meta,
						  ReturnSetInfo *rsinfo,
						  const LRWalkCtx *ctx);

static void
walk_targets(LRTargetKind kind, LREmitFn emit, ReturnSetInfo *rsinfo,
			 bool oldest_first, LRWalkCtx *ctx)
{
	LRSample   *ring = ctx->scratch;
	int			i;

	if (!lrstat_ready())
		return;

	Assert(ctx->max_samples >= 2);

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		char		name[NAMEDATALEN];
		char		worker_char;
		Oid			relid;
		LRTargetMeta m;
		int			n = 0;
		int			k;
		bool		use;

		SpinLockAcquire(&t->mutex);
		use = (t->in_use && t->kind == kind);
		if (use)
		{
			strlcpy(name, t->name, NAMEDATALEN);
			worker_char = t->worker_char;
			relid = t->relid;
			lrstat_copy_meta(t, &m);
			n = lrstat_copy_ring(t, ring, ctx->max_samples);
		}
		SpinLockRelease(&t->mutex);

		if (!use || n < 1)
			continue;
		if (ctx->filter_name != NULL && strcmp(name, ctx->filter_name) != 0)
			continue;

		if (!oldest_first)
			emit(name, worker_char, relid, ring, n, &m, rsinfo, ctx);
		else
		{
			/* ring is newest first; walk backwards chronologically */
			for (k = n - 1; k >= 0; k--)
			{
				if (ctx->filter_since != 0 &&
					ring[k].pub.ts < ctx->filter_since)
					continue;
				emit(name, worker_char, relid, &ring[k], 1, &m, rsinfo, ctx);
			}
		}
	}
}

/* ----------------------------------------------------------------
 * Publisher sample view (32 columns)
 * ----------------------------------------------------------------
 */

static void
emit_pub_sample(const char *name, char worker_char, Oid relid,
				const LRSample *ring, int n, const LRTargetMeta *m,
				ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRPubSample *s = &ring[0].pub;
	LRRow		r;

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_ts(&r, s->ts);
	lr_put_text(&r, m->database);
	lr_put_text(&r, m->plugin);
	lr_put_bool(&r, m->temporary);
	lr_put_bool(&r, m->active);
	lr_put_pid(&r, m->sender_pid);
	lr_put_text(&r, m->application_name);
	lr_put_text(&r, m->client_addr);
	lr_put_text(&r, m->state);
	lr_put_text(&r, m->sync_state);
	lr_put_lsn(&r, s->current_lsn);
	lr_put_lsn(&r, s->sent_lsn);
	lr_put_lsn(&r, s->peer_recv_lsn);
	lr_put_lsn(&r, s->peer_flush_lsn);
	lr_put_lsn(&r, s->peer_applied_lsn);
	lr_put_lsn(&r, s->confirmed_lsn);
	lr_put_lsn(&r, s->restart_lsn);
	lr_put_text(&r, m->wal_status);
	lr_put(&r, Int64GetDatum(m->safe_wal_size), !m->safe_wal_size_valid);
	lr_put_i8(&r, (int64) s->spill_bytes);
	lr_put_i8(&r, (int64) s->stream_bytes);
	lr_put_i8(&r, (int64) s->total_bytes);
	lr_put_i8(&r, lsn_diff(s->current_lsn, s->sent_lsn));
	lr_put_i8(&r, lsn_diff(s->sent_lsn, s->peer_recv_lsn));
	lr_put_i8(&r, lsn_diff(s->peer_recv_lsn, s->peer_applied_lsn));
	lr_put_i8(&r, lsn_diff(s->current_lsn, s->peer_applied_lsn));
	lr_put_i8(&r, lsn_diff(s->current_lsn, s->restart_lsn));
	lr_put_lag(&r, m->write_lag_us);
	lr_put_lag(&r, m->flush_lag_us);
	lr_put_lag(&r, m->replay_lag_us);
	lr_put_ts(&r, m->reply_time);

	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * Publisher rate view (15 columns)
 * ----------------------------------------------------------------
 */

static void
emit_pub_rate(const char *name, char worker_char, Oid relid,
			  const LRSample *ring, int n, const LRTargetMeta *m,
			  ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRPubSample *s = &ring[0].pub;
	double		gen, send, apply, confirm, spill, stream;
	TimestampTz t0, t1;
	bool		gen_ok, send_ok, apply_ok, confirm_ok, spill_ok, stream_ok;
	int64		unsent = lsn_diff(s->current_lsn, s->sent_lsn);
	int64		total = lsn_diff(s->current_lsn, s->peer_applied_lsn);
	double		eta_total = 0;
	bool		eta_total_ok;
	LRRow		r;

	send_ok = lr_rate(ring, n, f_pub_sent, &send, &t0, &t1);
	gen_ok = lr_rate(ring, n, f_pub_current, &gen, &t0, &t1);
	apply_ok = lr_rate(ring, n, f_pub_applied, &apply, &t0, &t1);
	confirm_ok = lr_rate(ring, n, f_pub_confirmed, &confirm, &t0, &t1);
	spill_ok = lr_rate(ring, n, f_pub_spill, &spill, &t0, &t1);
	stream_ok = lr_rate(ring, n, f_pub_stream, &stream, &t0, &t1);

	eta_total_ok = send_ok && apply_ok &&
		(send >= (double) lrstat_eta_min_rate ||
		 apply >= (double) lrstat_eta_min_rate);
	if (eta_total_ok)
		eta_total = (double) unsent / (send > 0 ? send : 1.0) +
			(double) (total - unsent) / (apply > 0 ? apply : 1.0);

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_ts(&r, GetCurrentTimestamp());
	lr_put_ts(&r, send_ok ? t0 : 0);
	lr_put_ts(&r, send_ok ? t1 : 0);
	lr_put_f8n(&r, send_ok, (double) (t1 - t0) / 1000000.0);
	lr_put_f8n(&r, gen_ok, mbps(gen));
	lr_put_f8n(&r, send_ok, mbps(send));
	lr_put_f8n(&r, apply_ok, mbps(apply));
	lr_put_f8n(&r, confirm_ok, mbps(confirm));
	lr_put_f8n(&r, spill_ok, mbps(spill));
	lr_put_f8n(&r, stream_ok, mbps(stream));
	lr_put_eta(&r, unsent, send_ok, send);
	lr_put_f8n(&r, eta_total_ok, eta_total);
	lr_put_bool(&r, lr_stalled(unsent, send_ok, send));

	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * Subscriber sample view (18 columns)
 * ----------------------------------------------------------------
 */

static void
emit_sub_sample(const char *name, char worker_char, Oid relid,
				const LRSample *ring, int n, const LRTargetMeta *m,
				ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRSubSample *s = &ring[0].sub;
	LRRow		r;

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_ts(&r, s->ts);
	lr_put_text(&r, m->subslotname);
	lr_put_text(&r, m->worker_type);
	lr_put_pid(&r, m->worker_pid);
	lr_put_pid(&r, m->leader_pid);
	lr_put(&r, ObjectIdGetDatum(relid), relid == 0);
	lr_put_lsn(&r, s->received_lsn);
	lr_put_lsn(&r, s->latest_end_lsn);
	lr_put_lsn(&r, s->applied_lsn);
	lr_put_lsn(&r, s->origin_local_lsn);
	lr_put_lsn(&r, s->local_wal_lsn);
	lr_put_ts(&r, m->last_msg_send_time);
	lr_put_ts(&r, m->last_msg_receipt_time);
	lr_put_ts(&r, m->latest_end_time);
	lr_put_i8(&r, lsn_diff(s->received_lsn ? s->received_lsn :
						   s->latest_end_lsn, s->applied_lsn));
	lr_put_count(&r, m->apply_error_count);
	lr_put_count(&r, m->sync_error_count);

	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * Subscriber rate view (13 columns)
 * ----------------------------------------------------------------
 */

static void
emit_sub_rate(const char *name, char worker_char, Oid relid,
			  const LRSample *ring, int n, const LRTargetMeta *m,
			  ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRSubSample *s = &ring[0].sub;
	double		recv, apply, local_wal;
	TimestampTz t0, t1;
	bool		recv_ok, apply_ok, local_ok;
	int64		backlog = lsn_diff(s->received_lsn ? s->received_lsn :
								 s->latest_end_lsn, s->applied_lsn);
	LRRow		r;

	apply_ok = lr_rate(ring, n, f_sub_applied, &apply, &t0, &t1);
	recv_ok = lr_rate(ring, n, f_sub_received, &recv, &t0, &t1);
	local_ok = lr_rate(ring, n, f_sub_local_wal, &local_wal, &t0, &t1);

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_text(&r, m->worker_type);
	lr_put(&r, ObjectIdGetDatum(relid), relid == 0);
	lr_put_ts(&r, GetCurrentTimestamp());
	lr_put_ts(&r, apply_ok ? t0 : 0);
	lr_put_ts(&r, apply_ok ? t1 : 0);
	lr_put_f8n(&r, apply_ok, (double) (t1 - t0) / 1000000.0);
	lr_put_f8n(&r, recv_ok, mbps(recv));
	lr_put_f8n(&r, apply_ok, mbps(apply));
	lr_put_f8n(&r, local_ok, mbps(local_wal));
	lr_put_eta(&r, backlog, apply_ok, apply);
	lr_put_bool(&r, lr_stalled(backlog, apply_ok, apply));

	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * Pipeline view (8 columns, four stage rows per slot)
 * ----------------------------------------------------------------
 */

static void
emit_pipeline(const char *name, char worker_char, Oid relid,
			  const LRSample *ring, int n, const LRTargetMeta *m,
			  ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRPubSample *s = &ring[0].pub;
	double		send, apply;
	TimestampTz t0, t1;
	bool		send_ok, apply_ok;
	int64		replay_minus_write = -1;
	int			i;
	struct
	{
		const char *stage;
		int64		bytes;
		bool		rate_ok;
		double		rate_bps;
		int64		lag_us;
	}			stages[4];

	send_ok = lr_rate(ring, n, f_pub_sent, &send, &t0, &t1);
	apply_ok = lr_rate(ring, n, f_pub_applied, &apply, &t0, &t1);
	if (m->write_lag_us >= 0 && m->replay_lag_us >= 0 &&
		m->replay_lag_us >= m->write_lag_us)
		replay_minus_write = m->replay_lag_us - m->write_lag_us;

	stages[0].stage = "unsent";
	stages[0].bytes = lsn_diff(s->current_lsn, s->sent_lsn);
	stages[0].rate_ok = send_ok;
	stages[0].rate_bps = send;
	stages[0].lag_us = -1;
	stages[1].stage = "inflight";
	stages[1].bytes = lsn_diff(s->sent_lsn, s->peer_recv_lsn);
	stages[1].rate_ok = send_ok;
	stages[1].rate_bps = send;
	stages[1].lag_us = m->write_lag_us;
	stages[2].stage = "peer_unapplied";
	stages[2].bytes = lsn_diff(s->peer_recv_lsn, s->peer_applied_lsn);
	stages[2].rate_ok = apply_ok;
	stages[2].rate_bps = apply;
	stages[2].lag_us = replay_minus_write;
	stages[3].stage = "retained";
	stages[3].bytes = lsn_diff(s->current_lsn, s->restart_lsn);
	stages[3].rate_ok = false;
	stages[3].rate_bps = 0;
	stages[3].lag_us = -1;

	for (i = 0; i < 4; i++)
	{
		LRRow		r;

		lr_row_reset(&r);
		lr_put_text(&r, name);
		lr_put_text(&r, stages[i].stage);
		lr_put_ts(&r, GetCurrentTimestamp());
		lr_put_ts(&r, send_ok ? t0 : 0);
		lr_put_ts(&r, send_ok ? t1 : 0);
		lr_put_i8(&r, stages[i].bytes);
		lr_put_f8n(&r, stages[i].rate_ok, mbps(stages[i].rate_bps));
		lr_put_lag(&r, stages[i].lag_us);
		lr_emit_row(rsinfo, &r);
	}
}

/* ----------------------------------------------------------------
 * Overall view (34 columns, one row per subscription leader)
 * ----------------------------------------------------------------
 */

static void
emit_overall(const char *name, char worker_char, Oid relid,
			 const LRSample *ring, int n, const LRTargetMeta *m,
			 ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRSubSample *s = &ring[0].sub;
	const LRRPUBSnapEntry *re = NULL;
	bool		has_rpub;
	XLogRecPtr	recv = s->received_lsn ? s->received_lsn :
		s->latest_end_lsn;
	XLogRecPtr	applied = 0;
	double		gen = 0, send = 0, spill = 0, stream = 0, recv_rate = 0,
				apply = 0;
	TimestampTz t0 = 0, t1 = 0;
	bool		gen_ok = false, send_ok = false, spill_ok = false;
	bool		stream_ok = false, recv_ok, apply_ok;
	int64		unsent = 0, inflight = 0, unapplied = 0, total = 0,
				retained = 0, feedback_lag = 0;
	double		eta_total = 0;
	bool		eta_total_ok = false;
	LRRow		r;
	int			i;

	/* leader rows only; tablesync has its own sub view rows */
	if (worker_char != 'a' || relid != 0)
		return;

	if (ctx != NULL && ctx->rpubs != NULL)
	{
		for (i = 0; i < ctx->nrpubs; i++)
			if (strcmp(ctx->rpubs[i].name, name) == 0)
			{
				re = &ctx->rpubs[i];
				break;
			}
	}
	has_rpub = (re != NULL && re->n > 0);

	apply_ok = lr_rate(ring, n, f_sub_applied, &apply, &t0, &t1);
	recv_ok = lr_rate(ring, n, f_sub_received, &recv_rate, &t0, &t1);

	if (has_rpub)
	{
		const LRPubSample *rs = &re->ring[0].pub;

		send_ok = lr_rate(re->ring, re->n, f_pub_sent, &send, &t0, &t1);
		gen_ok = lr_rate(re->ring, re->n, f_pub_current, &gen, &t0, &t1);
		spill_ok = lr_rate(re->ring, re->n, f_pub_spill, &spill, &t0, &t1);
		stream_ok = lr_rate(re->ring, re->n, f_pub_stream, &stream, &t0, &t1);

		/*
		 * Applied position: v18+ does not advance the subscription
		 * origin's remote_lsn for regular streaming, so prefer the
		 * feedback flush position (what the publisher's walsender
		 * believes we applied); origin wins only when it is ahead.
		 */
		applied = Max(s->applied_lsn, rs->peer_flush_lsn);

		unsent = lsn_diff(rs->current_lsn, rs->sent_lsn);
		inflight = lsn_diff(rs->sent_lsn, recv);
		total = lsn_diff(rs->current_lsn, applied);
		retained = lsn_diff(rs->current_lsn, rs->restart_lsn);
		feedback_lag = lsn_diff(recv, rs->peer_recv_lsn);
		unapplied = lsn_diff(recv, applied);

		eta_total_ok = send_ok && apply_ok;
		if (eta_total_ok)
			eta_total = (double) unsent / (send > 0 ? send : 1.0) +
				(double) (inflight + unapplied) / (apply > 0 ? apply : 1.0);
	}
	else
	{
		applied = s->applied_lsn;
		unapplied = lsn_diff(recv, applied);
	}

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_text(&r, m->subslotname);
	lr_put_text(&r, has_rpub ? re->meta.remote_state : "n/a");
	lr_put_ts(&r, has_rpub ? re->meta.last_remote_poll : 0);
	lr_put_ts(&r, s->ts);
	lr_put_ts(&r, GetCurrentTimestamp());
	lr_put_ts(&r, apply_ok ? t0 : 0);
	lr_put_ts(&r, apply_ok ? t1 : 0);
	lr_put_f8n(&r, apply_ok, (double) (t1 - t0) / 1000000.0);
	lr_put_lsn(&r, has_rpub ? re->ring[0].pub.current_lsn : 0);
	lr_put_lsn(&r, has_rpub ? re->ring[0].pub.sent_lsn : 0);
	lr_put_lsn(&r, recv);
	lr_put_lsn(&r, s->applied_lsn);
	lr_put_lsn(&r, has_rpub ? re->ring[0].pub.confirmed_lsn : 0);
	lr_put_lsn(&r, has_rpub ? re->ring[0].pub.restart_lsn : 0);
	lr_put_f8n(&r, gen_ok, mbps(gen));
	lr_put_f8n(&r, send_ok, mbps(send));
	lr_put_f8n(&r, recv_ok, mbps(recv_rate));
	lr_put_f8n(&r, apply_ok, mbps(apply));
	lr_put_f8n(&r, spill_ok, mbps(spill));
	lr_put_f8n(&r, stream_ok, mbps(stream));
	lr_put_i8n(&r, has_rpub, unsent);
	lr_put_i8n(&r, has_rpub, inflight);
	lr_put_i8n(&r, true, unapplied);
	lr_put_i8n(&r, has_rpub, total);
	lr_put_i8n(&r, has_rpub, retained);
	lr_put_i8n(&r, has_rpub, feedback_lag);
	lr_put_lag(&r, has_rpub ? re->meta.write_lag_us : -1);
	lr_put_lag(&r, has_rpub ? re->meta.flush_lag_us : -1);
	lr_put_lag(&r, has_rpub ? re->meta.replay_lag_us : -1);
	lr_put_eta(&r, unsent, send_ok, send);
	lr_put_f8n(&r, eta_total_ok, eta_total);
	lr_put_bool(&r, lr_stalled(unsent, send_ok, send));
	lr_put_bool(&r, lr_stalled(unapplied, apply_ok, apply));

	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * History emits (ring arrives as a single sample per call)
 * ----------------------------------------------------------------
 */

static void
emit_pub_history(const char *name, char worker_char, Oid relid,
				 const LRSample *ring, int n, const LRTargetMeta *m,
				 ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRPubSample *s = &ring[0].pub;
	LRRow		r;

	(void) ctx;				/* filtering already done in walk_targets */
	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_ts(&r, s->ts);
	lr_put_lsn(&r, s->current_lsn);
	lr_put_lsn(&r, s->sent_lsn);
	lr_put_lsn(&r, s->peer_recv_lsn);
	lr_put_lsn(&r, s->peer_applied_lsn);
	lr_put_lsn(&r, s->confirmed_lsn);
	lr_put_lsn(&r, s->restart_lsn);
	lr_emit_row(rsinfo, &r);
}

static void
emit_sub_history(const char *name, char worker_char, Oid relid,
				 const LRSample *ring, int n, const LRTargetMeta *m,
				 ReturnSetInfo *rsinfo, const LRWalkCtx *ctx)
{
	const LRSubSample *s = &ring[0].sub;
	LRRow		r;

	(void) ctx;				/* filtering already done in walk_targets */

	lr_row_reset(&r);
	lr_put_text(&r, name);
	lr_put_ts(&r, s->ts);
	lr_put_lsn(&r, s->received_lsn);
	lr_put_lsn(&r, s->latest_end_lsn);
	lr_put_lsn(&r, s->applied_lsn);
	lr_put_lsn(&r, s->origin_local_lsn);
	lr_put_lsn(&r, s->local_wal_lsn);
	lr_emit_row(rsinfo, &r);
}

/* ----------------------------------------------------------------
 * SRF wrappers
 * ----------------------------------------------------------------
 */

static void
lr_ctx_init(LRWalkCtx *ctx, int max_samples)
{
	ctx->filter_name = NULL;
	ctx->filter_since = 0;
	ctx->max_samples = max_samples;
	ctx->scratch = (LRSample *) palloc((Size) max_samples * sizeof(LRSample));
	ctx->rpubs = NULL;
	ctx->nrpubs = 0;
}

static void
lr_ctx_fini(LRWalkCtx *ctx)
{
	pfree(ctx->scratch);
}

/* Prebuild the remotely-polled publisher snapshot for pg_lrstat_overall. */
static void
build_rpub_snapshot(LRWalkCtx *ctx)
{
	int			maxn = lr_rate_samples();
	int			used = 0;
	char	   *block;
	Size		off = 0;
	int			i;

	if (!lrstat_ready())
		return;

	ctx->rpubs = (LRRPUBSnapEntry *) palloc0((Size) lrstat->ntargets *
											 sizeof(LRRPUBSnapEntry));
	block = (char *) palloc((Size) lrstat->ntargets * maxn *
							sizeof(LRSample));

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool		use;

		SpinLockAcquire(&t->mutex);
		use = (t->in_use && t->kind == LR_RPUB);
		if (use)
		{
			LRRPUBSnapEntry *e = &ctx->rpubs[used];

			strlcpy(e->name, t->name, NAMEDATALEN);
			lrstat_copy_meta(t, &e->meta);
			e->ring = (LRSample *) (block + off);
			e->n = lrstat_copy_ring(t, e->ring, maxn);
			off += (Size) maxn * sizeof(LRSample);
			used++;
		}
		SpinLockRelease(&t->mutex);
	}
	ctx->nrpubs = used;
}

PG_FUNCTION_INFO_V1(pg_lrstat_pub_sample);
Datum
pg_lrstat_pub_sample(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	walk_targets(LR_PUB, emit_pub_sample, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_pub_rate);
Datum
pg_lrstat_pub_rate(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	walk_targets(LR_PUB, emit_pub_rate, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_sub_sample);
Datum
pg_lrstat_sub_sample(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	walk_targets(LR_SUB, emit_sub_sample, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_sub_rate);
Datum
pg_lrstat_sub_rate(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	walk_targets(LR_SUB, emit_sub_rate, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_pipeline);
Datum
pg_lrstat_pipeline(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	walk_targets(LR_PUB, emit_pipeline, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_overall);
Datum
pg_lrstat_overall(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	lr_ctx_init(&ctx, lr_rate_samples());
	build_rpub_snapshot(&ctx);
	walk_targets(LR_SUB, emit_overall, rsinfo, false, &ctx);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_pub_history);
Datum
pg_lrstat_pub_history(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;
	char	   *name_copy = NULL;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	/* history needs the whole ring, not just the rate window */
	lr_ctx_init(&ctx, lrstat_ring_len);
	if (!PG_ARGISNULL(0))
	{
		name_copy = text_to_cstring(PG_GETARG_TEXT_PP(0));
		ctx.filter_name = name_copy;
	}
	ctx.filter_since = PG_ARGISNULL(1) ? 0 :
		DatumGetTimestampTz(PG_GETARG_DATUM(1));
	walk_targets(LR_PUB, emit_pub_history, rsinfo, true, &ctx);
	if (name_copy != NULL)
		pfree(name_copy);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_sub_history);
Datum
pg_lrstat_sub_history(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	LRWalkCtx	ctx;
	char	   *name_copy = NULL;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
	/* history needs the whole ring, not just the rate window */
	lr_ctx_init(&ctx, lrstat_ring_len);
	if (!PG_ARGISNULL(0))
	{
		name_copy = text_to_cstring(PG_GETARG_TEXT_PP(0));
		ctx.filter_name = name_copy;
	}
	ctx.filter_since = PG_ARGISNULL(1) ? 0 :
		DatumGetTimestampTz(PG_GETARG_DATUM(1));
	walk_targets(LR_SUB, emit_sub_history, rsinfo, true, &ctx);
	if (name_copy != NULL)
		pfree(name_copy);
	lr_ctx_fini(&ctx);
	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * Diagnostics
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(pg_lrstat_info);
Datum
pg_lrstat_info(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	bool		ready = lrstat_ready();
	TimestampTz last_ts = 0;
	bool		last_ok = false;
	char		last_err[LR_ERROR_LEN];
	uint64		nrounds = 0;
	uint64		dropped = 0;
	LRRow		r;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	last_err[0] = '\0';
	if (ready)
	{
		SpinLockAcquire(&lrstat->hdr_mutex);
		last_ts = lrstat->last_round_ts;
		last_ok = lrstat->last_round_ok;
		strlcpy(last_err, lrstat->last_round_error, LR_ERROR_LEN);
		SpinLockRelease(&lrstat->hdr_mutex);
		nrounds = pg_atomic_read_u64(&lrstat->nrounds);
		dropped = pg_atomic_read_u64(&lrstat->dropped_samples);
	}

	lr_row_reset(&r);
	lr_put_bool(&r, ready);					/* loaded */
	lr_put(&r, Int32GetDatum(lrstat->layout_version), !ready);
	lr_put(&r, Int32GetDatum(lrstat->ntargets), !ready);
	lr_put_i8(&r, lrstat_ring_len);
	lr_put_i8(&r, lrstat_sample_interval_ms);
	lr_put_i8(&r, lrstat_rate_window_ms);
	lr_put_i8(&r, lrstat_max_targets);
	lr_put_bool(&r, lrstat_remote_poll);
	lr_put_ts(&r, last_ts);
	lr_put(&r, BoolGetDatum(last_ok), !ready);
	lr_put_text(&r, last_err);
	lr_put(&r, Int64GetDatum((int64) nrounds), !ready);
	lr_put(&r, Int64GetDatum((int64) dropped), !ready);

	lr_emit_row(rsinfo, &r);
	PG_RETURN_NULL();
}

/* ----------------------------------------------------------------
 * Reset and test injection
 * ----------------------------------------------------------------
 */

PG_FUNCTION_INFO_V1(pg_lrstat_reset);
Datum
pg_lrstat_reset(PG_FUNCTION_ARGS)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to reset pg_lrstat")));

	lrstat_reset_all();
	PG_RETURN_VOID();
}

static void
lr_inject_check(void)
{
	if (!superuser())
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("must be a superuser to inject pg_lrstat samples")));
	if (!lrstat_allow_inject)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_lrstat.allow_inject is off")));
	if (!lrstat_ready())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_lrstat is not loaded via shared_preload_libraries")));
}

/* Injected NULLs would be silently swallowed by the ts monotonicity
 * check, so reject them up front with a proper error. */
static void
lr_check_nonnull(FunctionCallInfo fcinfo, int nargs)
{
	int			i;

	for (i = 0; i < nargs; i++)
		if (PG_ARGISNULL(i))
			ereport(ERROR,
					(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
					 errmsg("pg_lrstat inject arguments must not be NULL")));
}

/*
 * Reject the call if pg_lrstat.inject_name_prefix is set and the name
 * does not start with it.  Returns the cstring the caller can use
 * directly.  Caller is responsible for pfree'ing in the same memory
 * context (typically the SRF's per-call context).
 */
static char *
lr_inject_resolve_name(FunctionCallInfo fcinfo)
{
	char	   *name = text_to_cstring(PG_GETARG_TEXT_PP(0));

	if (lrstat_inject_name_prefix[0] != '\0' &&
		strncmp(name, lrstat_inject_name_prefix,
				strlen(lrstat_inject_name_prefix)) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_lrstat inject target name must start with \"%s\"",
						lrstat_inject_name_prefix),
				 errhint("set pg_lrstat.inject_name_prefix to '' to disable "
						 "this check, or use a name with the configured prefix.")));
	}
	return name;
}

static void
lr_inject_pub(LRTargetKind kind, FunctionCallInfo fcinfo)
{
	LRSample	s;
	LRTargetMeta m;
	LRTargetCtl *t;
	char	   *name;

	lr_inject_check();
	lr_check_nonnull(fcinfo, 7);
	name = lr_inject_resolve_name(fcinfo);
	memset(&s, 0, sizeof(s));
	memset(&m, 0, sizeof(m));
	s.pub.ts = DatumGetTimestampTz(PG_GETARG_DATUM(1));
	s.pub.current_lsn = DatumGetLSN(PG_GETARG_DATUM(2));
	s.pub.sent_lsn = DatumGetLSN(PG_GETARG_DATUM(3));
	s.pub.peer_applied_lsn = DatumGetLSN(PG_GETARG_DATUM(4));
	s.pub.peer_recv_lsn = s.pub.peer_applied_lsn;
	s.pub.peer_flush_lsn = s.pub.peer_applied_lsn;
	s.pub.confirmed_lsn = DatumGetLSN(PG_GETARG_DATUM(5));
	s.pub.restart_lsn = DatumGetLSN(PG_GETARG_DATUM(6));
	m.write_lag_us = m.flush_lag_us = m.replay_lag_us = -1;
	strlcpy(m.state, "injected", LR_STATE_LEN);
	strlcpy(m.remote_state, "ok", LR_STATE_LEN);
	m.last_remote_poll = s.pub.ts;

	t = lrstat_find_or_create(kind, name, 0, 0);
	if (t != NULL)
	{
		lrstat_push_sample(t, &s);
		lrstat_set_meta(t, &m);
	}
	pfree(name);
}

PG_FUNCTION_INFO_V1(pg_lrstat_inject_pub);
Datum
pg_lrstat_inject_pub(PG_FUNCTION_ARGS)
{
	lr_inject_pub(LR_PUB, fcinfo);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_lrstat_inject_rpub);
Datum
pg_lrstat_inject_rpub(PG_FUNCTION_ARGS)
{
	lr_inject_pub(LR_RPUB, fcinfo);
	PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pg_lrstat_inject_sub);
Datum
pg_lrstat_inject_sub(PG_FUNCTION_ARGS)
{
	LRSample	s;
	LRTargetMeta m;
	LRTargetCtl *t;
	char	   *name;

	lr_inject_check();
	lr_check_nonnull(fcinfo, 4);
	name = lr_inject_resolve_name(fcinfo);
	memset(&s, 0, sizeof(s));
	memset(&m, 0, sizeof(m));
	s.sub.ts = DatumGetTimestampTz(PG_GETARG_DATUM(1));
	s.sub.received_lsn = DatumGetLSN(PG_GETARG_DATUM(2));
	s.sub.latest_end_lsn = s.sub.received_lsn;
	s.sub.applied_lsn = DatumGetLSN(PG_GETARG_DATUM(3));
	s.sub.origin_local_lsn = s.sub.applied_lsn;
	strlcpy(m.worker_type, "apply", LR_STATE_LEN);

	t = lrstat_find_or_create(LR_SUB, name, 0, 'a');
	if (t != NULL)
	{
		lrstat_push_sample(t, &s);
		lrstat_set_meta(t, &m);
	}
	pfree(name);
	PG_RETURN_VOID();
}
