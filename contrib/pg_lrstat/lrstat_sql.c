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
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "lrstat.h"

extern bool lrstat_persist_requested;

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


/* ---- history-driven stat views --------------------------------------------
 * send_stat / recv_stat / cluster_stat emit one row per target per
 * sampling round, derived from the history ring: interval rates are
 * diffs against the previous sample of the same target, watermarks and
 * per-round state come from the entry itself.  Passthrough columns
 * (plugin, application_name, ...) are not historized and come from the
 * live target slots.
 */

static int64 lsn_diff(XLogRecPtr a, XLogRecPtr b)
{ int64 d = (int64)a - (int64)b; return d < 0 ? 0 : d; }

/* interval rate in MB/s between two consecutive samples of a target */
static double
ival_mbps(const LRHistoryEntry *cur, const LRHistoryEntry *prev,
          XLogRecPtr lcur, XLogRecPtr lprev)
{
    double dt = (double)(cur->ts - prev->ts) / 1e6;
    int64 d;
    if (dt <= 0) return 0.0;
    d = (int64) lcur - (int64) lprev;
    return d > 0 ? (double)d / dt / MB_DIV : 0.0;
}

/* per-target scan state during one chronological pass */
typedef struct HistTargetState
{
    bool known;                     /* slot resolved from live registry */
    char name[NAMEDATALEN];
    /* passthrough extras from the live slot */
    char plugin[LR_TEXT_LEN];
    char application_name[LR_TEXT_LEN];
    char client_addr[LR_TEXT_LEN];
    bool temporary;
    char remote_state[LR_STATE_LEN];
    bool have_meta;
    /* previous entry of this target */
    bool has_prev;
    LRHistoryEntry prev;
} HistTargetState;

static void
hist_targets_init(HistTargetState *st, int n)
{
    int i;
    memset(st, 0, n * sizeof(HistTargetState));
    for (i = 0; i < n && i < lrstat->ntargets; i++)
    {
        LRTargetCtl *t = lrstat_target_at(i);
        SpinLockAcquire(&t->mutex);
        if (t->in_use)
        {
            st[i].known = true;
            strlcpy(st[i].name, t->name, NAMEDATALEN);
            strlcpy(st[i].plugin, t->meta.plugin, LR_TEXT_LEN);
            strlcpy(st[i].application_name, t->meta.application_name,
                    LR_TEXT_LEN);
            strlcpy(st[i].client_addr, t->meta.client_addr, LR_TEXT_LEN);
            st[i].temporary = t->meta.temporary;
            strlcpy(st[i].remote_state, t->meta.remote_state, LR_STATE_LEN);
            st[i].have_meta = true;
        }
        SpinLockRelease(&t->mutex);
    }
}

/* side of an entry, from the recorded target kind */
#define ENTRY_IS_RECV(e) ((e)->kind == LR_RECV)

/*
 * Sentinel referenced by the 2.1 install/upgrade scripts: if the
 * postmaster still has an older library loaded (no restart after
 * replacing the .so), CREATE/ALTER EXTENSION fails here with
 * "could not find function" instead of silently mixing versions.
 */
PG_FUNCTION_INFO_V1(pg_lrstat_layout_version);
Datum
pg_lrstat_layout_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(LRSTAT_LAYOUT_VERSION);
}

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
    lr_put_ts(&r, lrstat->last_round_ts);
    lr_put_bool(&r, lrstat->last_round_ok);
    lr_put_text(&r, NULL);
    lr_put_i8(&r, (int64) lrstat->nrounds);
    lr_put_i8(&r, (int64) lrstat->dropped_samples);
    lr_put_bool(&r, lrstat_remote_poll);

    /* exported report names */
    {
        char **names = NULL;
        int count = 0;
        if (ready)
            count = lrstat_export_list(&names);
        if (count > 0 && names != NULL)
        {
            ArrayType *arr;
            Datum *elems = palloc(count * sizeof(Datum));
            int i;
            for (i = 0; i < count; i++)
                elems[i] = CStringGetTextDatum(names[i]);
            arr = construct_array(elems, count, TEXTOID, -1,
                                  InvalidOid, TYPALIGN_INT);
            lr_put(&r, PointerGetDatum(arr), false);
        }
        else
        {
            lr_put(&r, 0, true);
        }
    }

    lr_emit(rsinfo, &r);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_send_stat — one row per send-side target per round
 * =================================================================
 */
PG_FUNCTION_INFO_V1(pg_lrstat_send_stat);
Datum
pg_lrstat_send_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    HistTargetState *st;
    int n = lrstat->ntargets;
    int i, cnt;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    if (!lrstat_ready())
        PG_RETURN_NULL();

    st = palloc(n * sizeof(HistTargetState));
    hist_targets_init(st, n);

    cnt = lrstat_history_count();
    for (i = 0; i < cnt; i++)
    {
        LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));
        HistTargetState *ts;
        LRRow r;
        double gen = 0, snd = 0, appl = 0, spill = 0;

        if (ENTRY_IS_RECV(e))
            continue;
        ts = &st[e->target_idx];

        if (ts->has_prev)
        {
            gen = ival_mbps(e, &ts->prev, e->current_lsn, ts->prev.current_lsn);
            snd = ival_mbps(e, &ts->prev, e->sent_lsn, ts->prev.sent_lsn);
            appl = ival_mbps(e, &ts->prev, e->peer_applied_lsn,
                             ts->prev.peer_applied_lsn);
            spill = (double) lsn_diff(e->spill_bytes, ts->prev.spill_bytes)
                / MB_DIV;
        }

        lr_row_reset(&r);
        lr_put_text(&r, ts->known ? ts->name : "?");
        lr_put_ts(&r, e->ts);
        lr_put_text(&r, ts->have_meta ? ts->plugin : NULL);
        lr_put_bool(&r, ts->have_meta && ts->temporary);
        lr_put_bool(&r, e->active);
        lr_put_pid(&r, (pid_t) e->sender_pid);
        lr_put_text(&r, ts->have_meta ? ts->application_name : NULL);
        lr_put_text(&r, ts->have_meta ? ts->client_addr : NULL);
        lr_put_text(&r, e->state);
        lr_put_text(&r, e->sync_state);
        lr_put_text(&r, e->wal_status);
        lr_put_lsn(&r, e->current_lsn);
        lr_put_lsn(&r, e->sent_lsn);
        lr_put_lsn(&r, e->confirmed_lsn);
        lr_put_mb(&r, lsn_diff(e->current_lsn, e->sent_lsn));
        lr_put_mb(&r, lsn_diff(e->sent_lsn, e->peer_recv_lsn));
        lr_put_mb(&r, lsn_diff(e->peer_recv_lsn, e->peer_applied_lsn));
        lr_put_mb(&r, lsn_diff(e->current_lsn, e->peer_applied_lsn));
        lr_put_mb(&r, lsn_diff(e->current_lsn, e->restart_lsn));
        lr_put_f8(&r, ts->has_prev, gen);
        lr_put_f8(&r, ts->has_prev, snd);
        lr_put_f8(&r, ts->has_prev, appl);
        lr_put_f8(&r, ts->has_prev, spill);
        lr_put_lag(&r, e->write_lag_us);
        lr_put_lag(&r, e->flush_lag_us);
        lr_put_lag(&r, e->replay_lag_us);
        lr_put_bool(&r, ts->has_prev && lsn_diff(e->current_lsn, e->sent_lsn) > 0
                    && snd < 0.001);
        lr_emit(rsinfo, &r);
        ts->prev = *e;
        ts->has_prev = true;
    }
    pfree(st);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_recv_stat — one row per recv worker per round
 * =================================================================
 */
PG_FUNCTION_INFO_V1(pg_lrstat_recv_stat);
Datum
pg_lrstat_recv_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    HistTargetState *st;
    int n = lrstat->ntargets;
    int i, cnt;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    if (!lrstat_ready())
        PG_RETURN_NULL();

    st = palloc(n * sizeof(HistTargetState));
    hist_targets_init(st, n);

    cnt = lrstat_history_count();
    for (i = 0; i < cnt; i++)
    {
        LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));
        HistTargetState *ts;
        LRRow r;
        double rcv = 0, appl = 0, lwal = 0;

        if (!ENTRY_IS_RECV(e))
            continue;
        ts = &st[e->target_idx];

        if (ts->has_prev)
        {
            rcv = ival_mbps(e, &ts->prev, e->received_lsn, ts->prev.received_lsn);
            appl = ival_mbps(e, &ts->prev, e->applied_lsn, ts->prev.applied_lsn);
            lwal = ival_mbps(e, &ts->prev, e->local_wal_lsn,
                             ts->prev.local_wal_lsn);
        }
        /* remember whether this row HAD a previous sample before
         * overwriting prev — the first row of a target emits NULL
         * rates, not zeros */

        lr_row_reset(&r);
        lr_put_text(&r, ts->known ? ts->name : "?");
        lr_put_ts(&r, e->ts);
        lr_put_text(&r, e->worker_type);
        lr_put_pid(&r, (pid_t) e->worker_pid);
        lr_put_pid(&r, (pid_t) e->leader_pid);
        lr_put(&r, ObjectIdGetDatum(e->relid), e->relid == 0);
        lr_put_lsn(&r, e->received_lsn);
        lr_put_lsn(&r, e->applied_lsn);
        lr_put_ts(&r, e->last_msg_send_time);
        lr_put_ts(&r, e->last_msg_receipt_time);
        lr_put_mb(&r, lsn_diff(e->received_lsn, e->applied_lsn));
        lr_put_f8(&r, ts->has_prev, rcv);
        lr_put_f8(&r, ts->has_prev, appl);
        lr_put_f8(&r, ts->has_prev, lwal);
        lr_put(&r, Int64GetDatum(e->apply_error_count),
               e->apply_error_count < 0);
        lr_put(&r, Int64GetDatum(e->sync_error_count),
               e->sync_error_count < 0);
        lr_put_bool(&r, ts->has_prev
                    && lsn_diff(e->received_lsn, e->applied_lsn) > 0
                    && appl < 0.001);
        lr_emit(rsinfo, &r);
        ts->prev = *e;
        ts->has_prev = true;
    }
    pfree(st);
    PG_RETURN_NULL();
}

/* =================================================================
 * pg_lrstat_cluster_stat — one row per replication pair per round;
 * each recv row is paired with the newest RSEND sample at or before
 * its timestamp (both sides advance chronologically in one pass).
 * =================================================================
 */
PG_FUNCTION_INFO_V1(pg_lrstat_cluster_stat);
Datum
pg_lrstat_cluster_stat(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    HistTargetState *st;
    LRHistoryEntry *peers;          /* per target idx: newest send-side entry */
    LRHistoryEntry *peers_prev;     /* per target idx: the one before it */
    bool *has_peer, *has_peer_prev;
    int n = lrstat->ntargets;
    int i, j, cnt;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    if (!lrstat_ready())
        PG_RETURN_NULL();

    st = palloc(n * sizeof(HistTargetState));
    peers = palloc0(n * sizeof(LRHistoryEntry));
    peers_prev = palloc0(n * sizeof(LRHistoryEntry));
    has_peer = palloc0(n * sizeof(bool));
    has_peer_prev = palloc0(n * sizeof(bool));
    hist_targets_init(st, n);

    cnt = lrstat_history_count();
    for (i = 0; i < cnt; i++)
    {
        LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));
        HistTargetState *ts = &st[e->target_idx];
        int peer_idx = -1;
        LRRow r;
        double gen = 0, snd = 0, rcv = 0, appl = 0;
        int64 unsent = 0, inflight = 0, unapplied = 0, total = 0;
        XLogRecPtr cur = 0, sent = 0, confirmed = 0, restart = 0,
                   peer_recv = 0;
        bool have_peer = false, peer_rates = false;

        if (!ENTRY_IS_RECV(e))
        {
            /* send-side sample: advance this target's peer cursor */
            if (has_peer[e->target_idx])
            {
                peers_prev[e->target_idx] = peers[e->target_idx];
                has_peer_prev[e->target_idx] = true;
            }
            peers[e->target_idx] = *e;
            has_peer[e->target_idx] = true;
            continue;
        }

        /*
         * Skip table-sync workers here: their received/applied are the
         * COPY position of one table, not the subscription's chain
         * progress — pairing them into the cluster view is noise.
         */
        if (e->relid != 0)
            continue;

        /* pair with the newest send-side sample of the same name at or
         * before this timestamp (both cursors move chronologically) */
        for (j = 0; j < n; j++)
        {
            if (!has_peer[j] || !st[j].known || peers[j].ts > e->ts)
                continue;
            if (strcmp(st[j].name, ts->known ? ts->name : "?") == 0)
            { peer_idx = j; break; }
        }

        if (ts->has_prev)
        {
            rcv = ival_mbps(e, &ts->prev, e->received_lsn,
                            ts->prev.received_lsn);
            appl = ival_mbps(e, &ts->prev, e->applied_lsn,
                             ts->prev.applied_lsn);
        }
        if (peer_idx >= 0)
        {
            LRHistoryEntry *pe = &peers[peer_idx];

            have_peer = true;
            cur = pe->current_lsn;
            sent = pe->sent_lsn;
            confirmed = pe->confirmed_lsn;
            restart = pe->restart_lsn;
            peer_recv = pe->peer_recv_lsn;
            if (has_peer_prev[peer_idx])
            {
                LRHistoryEntry *pp = &peers_prev[peer_idx];

                peer_rates = true;
                gen = ival_mbps(pe, pp, pe->current_lsn, pp->current_lsn);
                snd = ival_mbps(pe, pp, pe->sent_lsn, pp->sent_lsn);
            }
        }

        unsent = lsn_diff(cur, sent);
        inflight = lsn_diff(sent, e->received_lsn);
        unapplied = lsn_diff(e->received_lsn, e->applied_lsn);
        total = lsn_diff(cur, e->applied_lsn);

        lr_row_reset(&r);
        lr_put_text(&r, ts->known ? ts->name : "?");
        lr_put_ts(&r, e->ts);
        lr_put_text(&r, have_peer && st[peer_idx].have_meta
                    ? st[peer_idx].remote_state : "n/a");
        lr_put_lsn(&r, cur);
        lr_put_lsn(&r, sent);
        lr_put_lsn(&r, e->received_lsn);
        lr_put_lsn(&r, e->applied_lsn);
        lr_put_lsn(&r, confirmed);
        lr_put_f8(&r, peer_rates, gen);
        lr_put_f8(&r, peer_rates, snd);
        lr_put_f8(&r, ts->has_prev, rcv);
        lr_put_f8(&r, ts->has_prev, appl);
        lr_put_mb(&r, unsent);
        lr_put_mb(&r, inflight);
        lr_put_mb(&r, unapplied);
        lr_put_mb(&r, total);
        lr_put_mb(&r, lsn_diff(cur, restart));
        lr_put_mb(&r, lsn_diff(e->received_lsn, peer_recv));
        lr_put_lag(&r, have_peer ? peers[peer_idx].write_lag_us : -1);
        lr_put_lag(&r, have_peer ? peers[peer_idx].flush_lag_us : -1);
        lr_put_lag(&r, have_peer ? peers[peer_idx].replay_lag_us : -1);
        if (peer_rates && snd > 0.001 && unsent > 0)
            lr_put_f8(&r, true, (double) unsent / MB_DIV / snd);
        else
            lr_put_f8(&r, false, 0);
        if (ts->has_prev && appl > 0.001)
            lr_put_f8(&r, true,
                      (double) (unapplied + inflight) / MB_DIV / appl);
        else
            lr_put_f8(&r, false, 0);
        lr_put_bool(&r, peer_rates && unsent > 0 && snd < 0.001);
        lr_put_bool(&r, ts->has_prev && unapplied > 0 && appl < 0.001);
        if (peer_rates && snd < gen && unsent > total / 2)
            lr_put_text(&r, "send");
        else if (unapplied > total / 2)
            lr_put_text(&r, "recv_apply");
        else if (inflight > total / 2)
            lr_put_text(&r, "network");
        else
            lr_put_text(&r, "none");
        lr_emit(rsinfo, &r);
        ts->prev = *e;
        ts->has_prev = true;
    }
    pfree(st); pfree(peers); pfree(peers_prev); pfree(has_peer);
    pfree(has_peer_prev);
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_send_history);
Datum
pg_lrstat_send_history(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    int n, i;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    if (!lrstat_ready())
        PG_RETURN_NULL();

    n = lrstat_history_count();
    for (i = 0; i < n; i++)
    {
        LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));
        LRTargetCtl *t = lrstat_target_at(e->target_idx);
        char name[NAMEDATALEN];
        LRTargetKind kind;
        LRRow r;

        SpinLockAcquire(&t->mutex);
        kind = t->kind;
        strlcpy(name, t->name, NAMEDATALEN);
        SpinLockRelease(&t->mutex);
        if (kind != LR_SEND && kind != LR_RSEND)
            continue;

        lr_row_reset(&r);
        lr_put_text(&r, name);
        lr_put_ts(&r, e->ts);
        lr_put_lsn(&r, e->current_lsn);
        lr_put_lsn(&r, e->sent_lsn);
        lr_put_lsn(&r, e->peer_recv_lsn);
        lr_put_lsn(&r, e->peer_flush_lsn);
        lr_put_lsn(&r, e->peer_applied_lsn);
        lr_put_lsn(&r, e->confirmed_lsn);
        lr_put_lsn(&r, e->restart_lsn);
        lr_put_i8(&r, (int64) e->spill_bytes);
        lr_put_i8(&r, (int64) e->stream_bytes);
        lr_emit(rsinfo, &r);
    }
    PG_RETURN_NULL();
}

PG_FUNCTION_INFO_V1(pg_lrstat_recv_history);
Datum
pg_lrstat_recv_history(PG_FUNCTION_ARGS)
{
    ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
    int n, i;

    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);
    if (!lrstat_ready())
        PG_RETURN_NULL();

    n = lrstat_history_count();
    for (i = 0; i < n; i++)
    {
        LRHistoryEntry *e = lrstat_history_at(lrstat_history_slot(i));
        LRTargetCtl *t = lrstat_target_at(e->target_idx);
        char name[NAMEDATALEN];
        LRTargetKind kind;
        LRRow r;

        SpinLockAcquire(&t->mutex);
        kind = t->kind;
        strlcpy(name, t->name, NAMEDATALEN);
        SpinLockRelease(&t->mutex);
        if (kind != LR_RECV)
            continue;

        lr_row_reset(&r);
        lr_put_text(&r, name);
        lr_put_ts(&r, e->ts);
        lr_put_lsn(&r, e->received_lsn);
        lr_put_lsn(&r, e->applied_lsn);
        lr_put_lsn(&r, e->local_wal_lsn);
        lr_emit(rsinfo, &r);
    }
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
    char name[NAMEDATALEN];

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

    /* the session is global and unnamed; the auto name only identifies
     * the report file and the archived-session list for export */
    snprintf(name, sizeof(name), "sess_%lld",
             (long long) (lrstat->session.session_id + 1));

    lrstat_session_start(name);

    /* wake the sampler for an immediate first round (design §1.3):
     * without this the first sample waits out the whole interval */
    if (lrstat->worker_pid > 0)
        kill(lrstat->worker_pid, SIGUSR1);

    PG_RETURN_DATUM(CStringGetTextDatum(name));
}

PG_FUNCTION_INFO_V1(lrstat_stop);
Datum
lrstat_stop(PG_FUNCTION_ARGS)
{
    char name[NAMEDATALEN];

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
    SpinLockRelease(&lrstat->session.mutex);

    lrstat_session_stop();
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

