/*-------------------------------------------------------------------------
 *
 * lrstat_sql.c
 *      SQL functions: session commands and 6 views.
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
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "lrstat.h"

#define MB_DIV (1024.0 * 1024.0)
#define MAX_COLS 40

/* ---- row building helpers ---- */

typedef struct LRRow
{
    Datum v[MAX_COLS];
    bool isnull[MAX_COLS];
    int n;
} LRRow;

static void lr_row_reset(LRRow *r) { r->n = 0; }
static void lr_put(LRRow *r, Datum d, bool isnull) { r->v[r->n]=d; r->isnull[r->n]=isnull; r->n++; }
static void lr_put_lsn(LRRow *r, XLogRecPtr l) { lr_put(r, Int64GetDatum((int64)l), l==0); }
static void lr_put_ts(LRRow *r, TimestampTz ts) { lr_put(r, Int64GetDatum((int64)ts), ts==0); }
static void lr_put_i8(LRRow *r, int64 v) { lr_put(r, Int64GetDatum(v), false); }
static void lr_put_f8(LRRow *r, bool ok, double v) { lr_put(r, Float8GetDatum(ok?v:0), !ok); }
static void lr_put_bool(LRRow *r, bool v) { lr_put(r, BoolGetDatum(v), false); }
static void lr_put_text(LRRow *r, const char *s)
{ bool n = (s==NULL||!s[0]); lr_put(r, n?0:CStringGetTextDatum(s), n); }
static void lr_put_pid(LRRow *r, pid_t p) { lr_put(r, Int32GetDatum((int32)p), p==0); }
static void lr_put_lag(LRRow *r, int64 us)
{
    if (us < 0) { lr_put(r, 0, true); return; }
    Interval *iv = palloc0(sizeof(Interval));
    iv->time = us;
    lr_put(r, PointerGetDatum(iv), false);
}
static void lr_put_mb(LRRow *r, int64 bytes)
{ lr_put_f8(r, bytes > 0, bytes > 0 ? (double)bytes / MB_DIV : 0.0); }

static void lr_emit(ReturnSetInfo *rsinfo, LRRow *r)
{
    if (r->n != rsinfo->setDesc->natts)
        elog(ERROR, "pg_lrstat: column count mismatch (%d vs %d)",
             r->n, rsinfo->setDesc->natts);
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, r->v, r->isnull);
}

static int64 lsn_diff(XLogRecPtr a, XLogRecPtr b)
{ int64 d = (int64)a - (int64)b; return d < 0 ? 0 : d; }

/* ---- rate helpers ---- */

typedef int64 (*RateFn)(const LRSample *);

#define SEND_FIELD(fn, member) \
    static int64 fn(const LRSample *s) { return (int64) s->send.member; }
#define RECV_FIELD(fn, member) \
    static int64 fn(const LRSample *s) { return (int64) s->recv.member; }

SEND_FIELD(r_curr, current_lsn)
SEND_FIELD(r_sent, sent_lsn)
SEND_FIELD(r_peer_app, peer_applied_lsn)
SEND_FIELD(r_spill, spill_bytes)
SEND_FIELD(r_stream, stream_bytes)
RECV_FIELD(r_recv, received_lsn)
RECV_FIELD(r_appl, applied_lsn)
RECV_FIELD(r_lwal, local_wal_lsn)

static bool
calc_rate(const LRSample *last, const LRSample *ref, RateFn f,
          bool *is_first, double *mbps_out)
{
    double dt = (double)(last->send.ts - ref->send.ts) / 1e6;
    int64 d;
    if (dt <= 0) return false;
    d = f(last) - f(ref);
    *mbps_out = d > 0 ? (double)d / dt / MB_DIV : 0.0;
    *is_first = false;
    return true;
}

/* ---- target iteration ---- */

typedef void (*EmitFn)(const char *name, char wc, Oid relid,
                       const LRSample *a, const LRSample *p,
                       const LRSample *l, const LRTargetMeta *m,
                       ReturnSetInfo *rsi);

static void
walk(LRTargetKind kind, EmitFn emit, ReturnSetInfo *rsi)
{
    int i;
    if (!lrstat_ready()) return;

    for (i = 0; i < lrstat->ntargets; i++)
    {
        LRTargetCtl *t = lrstat_target_at(i);
        char name[NAMEDATALEN]; char wc; Oid relid;
        LRSample a, p, l; LRTargetMeta m;
        bool use;

        SpinLockAcquire(&t->mutex);
        use = (t->in_use && t->kind == kind);
        if (use)
        {
            strlcpy(name, t->name, NAMEDATALEN);
            wc = t->worker_char; relid = t->relid;
            memcpy(&a, &t->anchor, sizeof(LRSample));
            memcpy(&p, &t->prev, sizeof(LRSample));
            memcpy(&l, &t->last, sizeof(LRSample));
            memcpy(&m, &t->meta, sizeof(LRTargetMeta));
        }
        SpinLockRelease(&t->mutex);

        if (use && l.send.ts > 0)
            emit(name, wc, relid, &a, &p, &l, &m, rsi);
    }
}

/* =================================================================
 * pg_lrstat_info (1 row)
 * =================================================================
 */
PG_FUNCTION_INFO_V1(pg_lrstat_info);
Datum
pg_lrstat_info(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    bool ready = lrstat_ready();
    bool running = false;
    char name[NAMEDATALEN] = "";
    TimestampTz start_ts = 0, stop_ts = 0;
    bool trunc = false, degr = false;
    LRRow r;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    if (ready)
    {
        SpinLockAcquire(&lrstat->session.mutex);
        running = lrstat->session.running;
        strlcpy(name, lrstat->session.name, NAMEDATALEN);
        start_ts = lrstat->session.start_ts;
        stop_ts = lrstat->session.stop_ts;
        trunc = lrstat->session.truncated;
        degr = lrstat->session.degraded;
        SpinLockRelease(&lrstat->session.mutex);
    }

    lr_row_reset(&r);
    lr_put_bool(&r, ready);
    lr_put_text(&r, name);
    lr_put_text(&r, running ? "running" : (stop_ts ? "stopped" : "idle"));
    lr_put_bool(&r, trunc);
    lr_put_bool(&r, degr);
    lr_put_ts(&r, start_ts);
    lr_put_ts(&r, stop_ts);
    lr_put_i8(&r, lrstat_sample_interval_ms);
    lr_put_ts(&r, GetCurrentTimestamp());
    lr_put_bool(&r, true);
    lr_put_text(&r, NULL);
    lr_put_i8(&r, 0);
    lr_put_i8(&r, 0);
    lr_put_bool(&r, lrstat_remote_poll);
    lr_put_text(&r, NULL);
    lr_emit(rsinfo, &r);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_send_stat (one row per send-side connection)
 * =================================================================
 */

static void
emit_send(const char *name, char wc, Oid relid,
          const LRSample *a, const LRSample *p, const LRSample *l,
          const LRTargetMeta *m, ReturnSetInfo *rsi)
{
    const LRSendSample *s = &l->send;
    double mbps;
    bool ok;
    LRRow r;

    lr_row_reset(&r);
    lr_put_text(&r, name);
    lr_put_text(&r, m->plugin);
    lr_put_bool(&r, m->temporary);
    lr_put_bool(&r, m->active);
    lr_put_pid(&r, m->sender_pid);
    lr_put_text(&r, m->application_name);
    lr_put_text(&r, m->client_addr);
    lr_put_text(&r, m->state);
    lr_put_text(&r, m->sync_state);
    lr_put_text(&r, m->wal_status);
    lr_put(&r, Float8GetDatum(m->safe_wal_size_valid ? (double)m->safe_wal_size/MB_DIV : 0),
           !m->safe_wal_size_valid);
    lr_put_ts(&r, s->ts);
    lr_put_lsn(&r, s->current_lsn);
    lr_put_lsn(&r, s->sent_lsn);
    lr_put_lsn(&r, s->confirmed_lsn);

    /* backlogs in MB */
    lr_put_mb(&r, lsn_diff(s->current_lsn, s->sent_lsn));
    lr_put_mb(&r, lsn_diff(s->sent_lsn, s->peer_recv_lsn));
    lr_put_mb(&r, lsn_diff(s->peer_recv_lsn, s->peer_applied_lsn));
    lr_put_mb(&r, lsn_diff(s->current_lsn, s->peer_applied_lsn));
    lr_put_mb(&r, lsn_diff(s->current_lsn, s->restart_lsn));

    /* rates: instant from prev→last, avg from anchor→last */
    ok = (p->send.ts > 0) && calc_rate(l, p, r_curr, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* gen_instant */
    ok = (a->send.ts > 0) && calc_rate(l, a, r_curr, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* gen_avg */
    ok = (p->send.ts > 0) && calc_rate(l, p, r_sent, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* send_instant */
    ok = (a->send.ts > 0) && calc_rate(l, a, r_sent, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* send_avg */
    ok = (p->send.ts > 0) && calc_rate(l, p, r_peer_app, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* apply_instant */
    ok = (a->send.ts > 0) && calc_rate(l, a, r_peer_app, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* apply_avg */
    ok = (p->send.ts > 0) && calc_rate(l, p, r_spill, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* spill_instant */
    ok = (a->send.ts > 0) && calc_rate(l, a, r_spill, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);       /* spill_avg */

    lr_put_lag(&r, m->write_lag_us);
    lr_put_lag(&r, m->flush_lag_us);
    lr_put_lag(&r, m->replay_lag_us);

    {
        int64 unsent = lsn_diff(s->current_lsn, s->sent_lsn);
        double send_avg = 0; bool sok = false;
        calc_rate(l, a, r_sent, &sok, &send_avg);
        lr_put_bool(&r, unsent > 0 && sok && send_avg < 0.001);
    }

    lr_emit(rsi, &r);
}

PG_FUNCTION_INFO_V1(pg_lrstat_send_stat);
Datum
pg_lrstat_send_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    walk(LR_SEND, emit_send, rsinfo);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_recv_stat (one row per recv worker)
 * =================================================================
 */

static void
emit_recv(const char *name, char wc, Oid relid,
          const LRSample *a, const LRSample *p, const LRSample *l,
          const LRTargetMeta *m, ReturnSetInfo *rsi)
{
    const LRRecvSample *s = &l->recv;
    double mbps;
    bool ok;
    LRRow r;

    lr_row_reset(&r);
    lr_put_text(&r, name);
    lr_put_text(&r, m->worker_type);
    lr_put_pid(&r, m->worker_pid);
    lr_put_pid(&r, m->leader_pid);
    lr_put(&r, ObjectIdGetDatum(relid), relid == 0);
    lr_put_ts(&r, s->ts);
    lr_put_lsn(&r, s->received_lsn);
    lr_put_lsn(&r, s->applied_lsn);
    lr_put_ts(&r, m->last_msg_send_time);
    lr_put_ts(&r, m->last_msg_receipt_time);
    lr_put_mb(&r, lsn_diff(s->received_lsn, s->applied_lsn));

    ok = (p->recv.ts > 0) && calc_rate(l, p, r_recv, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);
    ok = (a->recv.ts > 0) && calc_rate(l, a, r_recv, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);
    ok = (p->recv.ts > 0) && calc_rate(l, p, r_appl, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);
    ok = (a->recv.ts > 0) && calc_rate(l, a, r_appl, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);
    ok = (p->recv.ts > 0) && calc_rate(l, p, r_lwal, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);
    ok = (a->recv.ts > 0) && calc_rate(l, a, r_lwal, &ok, &mbps);
    lr_put_f8(&r, ok, mbps);

    lr_put(&r, Int64GetDatum(m->apply_error_count), m->apply_error_count < 0);
    lr_put(&r, Int64GetDatum(m->sync_error_count), m->sync_error_count < 0);

    {
        int64 bl = lsn_diff(s->received_lsn, s->applied_lsn);
        double aa = 0; bool aok = false;
        calc_rate(l, a, r_appl, &aok, &aa);
        lr_put_bool(&r, bl > 0 && aok && aa < 0.001);
    }

    lr_emit(rsi, &r);
}

PG_FUNCTION_INFO_V1(pg_lrstat_recv_stat);
Datum
pg_lrstat_recv_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    walk(LR_RECV, emit_recv, rsinfo);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_cluster_stat (one row per replication pair)
 * =================================================================
 */

static void
emit_cluster(const char *name, char wc, Oid relid,
             const LRSample *a, const LRSample *p, const LRSample *l,
             const LRTargetMeta *m, ReturnSetInfo *rsi)
{
    /* only leader apply workers (relid==0, 'a') or physical ('p') */
    if (wc == 't')
        return;

    {
        /* look up RSEND mirror for this recv_name */
        LRSample ra, rp, rl;
        LRTargetMeta rm;
        bool has_rsend = lrstat_lookup(LR_RSEND, name, &ra, &rp, &rl, &rm);
        const LRRecvSample *sv = &l->recv;
        const LRSendSample *rs_ = has_rsend ? &rl.send : NULL;
        double mbps;
        bool ok;
        double gen_i=0, gen_a=0, snd_i=0, snd_a=0, app_a=0;
        bool gen_io=false, gen_ao=false, snd_io=false, snd_ao=false, app_ao=false;
        LRRow r;
        int64 unsent=0, inflight=0, unapplied=0, total=0;

        if (rs_)
        {
            unsent = lsn_diff(rs_->current_lsn, rs_->sent_lsn);
            inflight = lsn_diff(rs_->sent_lsn, sv->received_lsn);
            total = lsn_diff(rs_->current_lsn, sv->applied_lsn);
            snd_io = (rp.send.ts > 0) && calc_rate(&rl, &rp, r_sent, &ok, &snd_i);
            snd_ao = (ra.send.ts > 0) && calc_rate(&rl, &ra, r_sent, &ok, &snd_a);
            gen_io = (rp.send.ts > 0) && calc_rate(&rl, &rp, r_curr, &ok, &gen_i);
            gen_ao = (ra.send.ts > 0) && calc_rate(&rl, &ra, r_curr, &ok, &gen_a);
        }
        unapplied = lsn_diff(sv->received_lsn, sv->applied_lsn);
        app_ao = (a->recv.ts > 0) && calc_rate(l, a, r_appl, &ok, &app_a);

        lr_row_reset(&r);
        lr_put_text(&r, name);
        lr_put_text(&r, has_rsend ? rm.remote_state : "n/a");
        lr_put_ts(&r, has_rsend ? rm.last_remote_poll : 0);
        lr_put_ts(&r, sv->ts);
        lr_put_lsn(&r, rs_ ? rs_->current_lsn : 0);
        lr_put_lsn(&r, rs_ ? rs_->sent_lsn : 0);
        lr_put_lsn(&r, sv->received_lsn);
        lr_put_lsn(&r, sv->applied_lsn);
        lr_put_lsn(&r, rs_ ? rs_->confirmed_lsn : 0);

        lr_put_f8(&r, gen_io, gen_i);
        lr_put_f8(&r, gen_ao, gen_a);
        lr_put_f8(&r, snd_io, snd_i);
        lr_put_f8(&r, snd_ao, snd_a);
        ok = (p->recv.ts > 0) && calc_rate(l, p, r_recv, &ok, &mbps);
        lr_put_f8(&r, ok, mbps);   /* recv_instant */
        ok = (a->recv.ts > 0) && calc_rate(l, a, r_recv, &ok, &mbps);
        lr_put_f8(&r, ok, mbps);   /* recv_avg */
        ok = (p->recv.ts > 0) && calc_rate(l, p, r_appl, &ok, &mbps);
        lr_put_f8(&r, ok, mbps);   /* apply_instant */
        lr_put_f8(&r, app_ao, app_a);

        lr_put_mb(&r, unsent);
        lr_put_mb(&r, inflight);
        lr_put_mb(&r, unapplied);
        lr_put_mb(&r, total);
        if (rs_)
        {
            lr_put_mb(&r, lsn_diff(rs_->current_lsn, rs_->restart_lsn));
            lr_put_mb(&r, lsn_diff(sv->received_lsn, rs_->peer_recv_lsn));
        }
        else
        {
            lr_put_f8(&r, false, 0);
            lr_put_f8(&r, false, 0);
        }

        lr_put_lag(&r, has_rsend ? rm.write_lag_us : -1);
        lr_put_lag(&r, has_rsend ? rm.flush_lag_us : -1);
        lr_put_lag(&r, has_rsend ? rm.replay_lag_us : -1);

        /* catchup estimates */
        if (snd_ao && snd_a > 0.001 && unsent > 0)
            lr_put_f8(&r, true, (double)unsent / MB_DIV / snd_a);
        else
            lr_put_f8(&r, false, 0);
        if (snd_ao && app_ao && snd_a > 0.001 && app_a > 0.001)
            lr_put_f8(&r, true,
                      (double)unsent / MB_DIV / snd_a +
                      (double)(inflight + unapplied) / MB_DIV / app_a);
        else
            lr_put_f8(&r, false, 0);

        lr_put_bool(&r, unsent > 0 && snd_ao && snd_a < 0.001);
        lr_put_bool(&r, unapplied > 0 && app_ao && app_a < 0.001);

        /* bottleneck: real-time CASE WHEN */
        if (gen_ao && snd_ao && snd_a < gen_a && unsent > total / 2)
            lr_put_text(&r, "send");
        else if (app_ao && unapplied > total / 2)
            lr_put_text(&r, "recv_apply");
        else if (inflight > total / 2)
            lr_put_text(&r, "network");
        else
            lr_put_text(&r, "none");

        lr_emit(rsi, &r);
    }
}

PG_FUNCTION_INFO_V1(pg_lrstat_cluster_stat);
Datum
pg_lrstat_cluster_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    walk(LR_RECV, emit_cluster, rsinfo);
    PG_RETURN_NULL();
}

/* =================================================================
 * lrstat_start / lrstat_stop / lrstat_reset
 * =================================================================
 */

PG_FUNCTION_INFO_V1(lrstat_start);
Datum
lrstat_start(PG_FUNCTION_ARGS)
{
    text *name_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_PP(0);
    char name[NAMEDATALEN];
    char auto_name[64];
    TimestampTz now;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser")));
    if (!lrstat_ready())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("pg_lrstat not loaded")));

    SpinLockAcquire(&lrstat->session.mutex);
    if (lrstat->session.running)
    {
        SpinLockRelease(&lrstat->session.mutex);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("session %s already running",
                               lrstat->session.name)));
    }
    SpinLockRelease(&lrstat->session.mutex);

    if (name_arg != NULL)
        strlcpy(name, text_to_cstring(name_arg), NAMEDATALEN);
    else
    {
        snprintf(auto_name, sizeof(auto_name), "sess_%lld_%s",
                 (long long)(lrstat->session.session_id + 1),
                 timestamptz_to_str(GetCurrentTimestamp()));
        strlcpy(name, auto_name, NAMEDATALEN);
    }

    lrstat_session_start(name);
    now = GetCurrentTimestamp();

    PG_RETURN_DATUM(CStringGetTextDatum(name));
}

PG_FUNCTION_INFO_V1(lrstat_stop);
Datum
lrstat_stop(PG_FUNCTION_ARGS)
{
    text *name_arg = PG_ARGISNULL(0) ? NULL : PG_GETARG_TEXT_PP(0);
    char name[NAMEDATALEN];
    TimestampTz start_ts, stop_ts;

    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser")));
    if (!lrstat_ready())
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("pg_lrstat not loaded")));

    SpinLockAcquire(&lrstat->session.mutex);
    if (!lrstat->session.running)
    {
        SpinLockRelease(&lrstat->session.mutex);
        ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg("no running session")));
    }
    strlcpy(name, lrstat->session.name, NAMEDATALEN);
    start_ts = lrstat->session.start_ts;
    SpinLockRelease(&lrstat->session.mutex);

    if (name_arg != NULL)
    {
        char expect[NAMEDATALEN];
        strlcpy(expect, text_to_cstring(name_arg), NAMEDATALEN);
        if (strcmp(expect, name) != 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                            errmsg("session name mismatch: running=%s given=%s",
                                   name, expect)));
    }

    lrstat_session_stop();
    stop_ts = GetCurrentTimestamp();

    PG_RETURN_DATUM(CStringGetTextDatum(name));
}

PG_FUNCTION_INFO_V1(pg_lrstat_reset);
Datum
pg_lrstat_reset(PG_FUNCTION_ARGS)
{
    if (!superuser())
        ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                        errmsg("must be superuser")));
    lrstat_session_reset();
    PG_RETURN_VOID();
}
