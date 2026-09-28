/*-------------------------------------------------------------------------
 *
 * lrstat_shmem.c
 *      Shared memory: session state, target three-slot samples,
 *      session interval log.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_shmem.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

#include "lrstat.h"

LRStatShared *lrstat = NULL;

/* set by lrstat_start() before lrstat_session_start() */
bool lrstat_persist_requested = false;

static bool lrstat_was_preloaded = false;

/* Target array starts right after the shared header */
static char *
target_base(void)
{
	return ((char *) lrstat) + MAXALIGN(sizeof(LRStatShared));
}

static Size
target_stride(void)
{
	return MAXALIGN(sizeof(LRTargetCtl));
}

/* Session entry array starts after the target array */
static char *
entry_base(void)
{
	return target_base() + (Size) lrstat->ntargets * target_stride();
}

/* Session registry starts after the history array */
static char *
sessions_base(void)
{
	return entry_base() + (Size) lrstat->ntargets * (Size) lrstat->ring_len *
		sizeof(LRHistoryEntry);
}

Size
lrstat_shmem_size(void)
{
	Size header = MAXALIGN(sizeof(LRStatShared));
	Size targets = (Size) lrstat_max_targets * MAXALIGN(sizeof(LRTargetCtl));
	Size entries = (Size) lrstat_max_targets * (Size) lrstat_ring_len *
		sizeof(LRHistoryEntry);
	Size sessions = (Size) LRSTAT_MAX_SESSIONS * sizeof(LRSessionRegEntry);
	return header + targets + entries + sessions;
}

void
lrstat_note_preload(void)
{
	lrstat_was_preloaded = true;
}

static void
lrstat_attach(void)
{
#ifdef EXEC_BACKEND
	if (lrstat == NULL && IsUnderPostmaster && lrstat_was_preloaded)
	{
		bool found;
		lrstat = (LRStatShared *) ShmemInitStruct("pg_lrstat",
												  lrstat_shmem_size(),
												  &found);
		if (!found)
			elog(WARNING, "pg_lrstat: shared segment missing at attach");
	}
#endif
}

void
lrstat_shmem_startup(void)
{
	bool found;
	Size size = lrstat_shmem_size();
	int i;

	lrstat = (LRStatShared *) ShmemInitStruct("pg_lrstat", size, &found);
	if (found)
		return;

	lrstat->session.magic = LRSTAT_MAGIC;
	lrstat->session.layout_version = LRSTAT_LAYOUT_VERSION;
	lrstat->session.session_id = 0;
	lrstat->session.running = false;
	lrstat->session.name[0] = '\0';
	lrstat->session.start_ts = 0;
	lrstat->session.stop_ts = 0;
	lrstat->session.truncated = false;
	lrstat->session.degraded = false;
	SpinLockInit(&lrstat->session.mutex);

	lrstat->ntargets = lrstat_max_targets;
	lrstat->ring_len = lrstat_ring_len;
	lrstat->n_entries = 0;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		MemSet(t, 0, sizeof(LRTargetCtl));
		SpinLockInit(&t->mutex);
	}

	for (i = 0; i < LRSTAT_MAX_SESSIONS; i++)
		MemSet(lrstat_sessionreg_at(i), 0, sizeof(LRSessionRegEntry));

	/* Recover interrupted sessions from files */
	lrstat_store_recover();
}

bool
lrstat_ready(void)
{
	lrstat_attach();
	return lrstat != NULL &&
		lrstat->session.magic == LRSTAT_MAGIC &&
		lrstat->session.layout_version == LRSTAT_LAYOUT_VERSION;
}

LRTargetCtl *
lrstat_target_at(int i)
{
	return (LRTargetCtl *) (target_base() + (Size) i * target_stride());
}

LRHistoryEntry *
lrstat_history_at(int idx)
{
	return (LRHistoryEntry *) (entry_base() +
							   (Size) idx * sizeof(LRHistoryEntry));
}

LRSessionRegEntry *
lrstat_sessionreg_at(int i)
{
	return (LRSessionRegEntry *) (sessions_base() +
								  (Size) i * sizeof(LRSessionRegEntry));
}

/* map a session id to its name; false when unknown or idle (id 0) */
bool
lrstat_session_name(uint64 session_id, char *out, Size outlen)
{
	int i;

	if (!lrstat_ready() || session_id == 0)
		return false;

	for (i = 0; i < LRSTAT_MAX_SESSIONS; i++)
	{
		LRSessionRegEntry *r = lrstat_sessionreg_at(i);

		SpinLockAcquire(&lrstat->session.mutex);
		if (r->in_use && r->session_id == session_id)
		{
			strlcpy(out, r->name, outlen);
			SpinLockRelease(&lrstat->session.mutex);
			return true;
		}
		SpinLockRelease(&lrstat->session.mutex);
	}
	return false;
}

/* remember a session in the registry ring (oldest slot reused) */
void
lrstat_sessionreg_add(uint64 session_id, const char *name,
					  TimestampTz start_ts)
{
	LRSessionRegEntry *victim = NULL;
	int i;

	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	victim = lrstat_sessionreg_at(0);
	for (i = 0; i < LRSTAT_MAX_SESSIONS; i++)
	{
		LRSessionRegEntry *r = lrstat_sessionreg_at(i);

		if (!r->in_use)
		{ victim = r; break; }
		if (r->start_ts < victim->start_ts)
			victim = r;
	}
	victim->in_use = true;
	victim->session_id = session_id;
	strlcpy(victim->name, name, NAMEDATALEN);
	victim->start_ts = start_ts;
	victim->stop_ts = 0;
	SpinLockRelease(&lrstat->session.mutex);
}

void
lrstat_sessionreg_close(uint64 session_id, TimestampTz stop_ts)
{
	int i;

	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	for (i = 0; i < LRSTAT_MAX_SESSIONS; i++)
	{
		LRSessionRegEntry *r = lrstat_sessionreg_at(i);

		if (r->in_use && r->session_id == session_id)
		{
			r->stop_ts = stop_ts;
			break;
		}
	}
	SpinLockRelease(&lrstat->session.mutex);
}

static bool
target_key_equal(const LRTargetCtl *t, LRTargetKind kind,
				 const char *name, Oid relid, char worker_char)
{
	if (!t->in_use || t->kind != kind || strcmp(t->name, name) != 0)
		return false;
	if (kind != LR_RECV)
		return true;
	return t->relid == relid && t->worker_char == worker_char;
}

static void
target_init(LRTargetCtl *t, LRTargetKind kind, const char *name,
			Oid relid, char worker_char)
{
	t->kind = kind;
	strlcpy(t->name, name, NAMEDATALEN);
	t->worker_char = worker_char;
	t->relid = relid;
	t->in_use = true;
	t->first_seen_ts = GetCurrentTimestamp();
	t->last_sample_ts = 0;
	MemSet(&t->anchor, 0, sizeof(LRSample));
	MemSet(&t->prev, 0, sizeof(LRSample));
	MemSet(&t->last, 0, sizeof(LRSample));
	MemSet(&t->meta, 0, sizeof(LRTargetMeta));
	t->meta.write_lag_us = -1;
	t->meta.flush_lag_us = -1;
	t->meta.replay_lag_us = -1;
	strlcpy(t->meta.remote_state, "n/a", LR_STATE_LEN);
}

LRTargetCtl *
lrstat_find_or_create(LRTargetKind kind, const char *name,
					  Oid relid, char worker_char)
{
	LRTargetCtl *victim = NULL;
	TimestampTz victim_ts = 0;
	int i;

	if (!lrstat_ready())
		return NULL;

	/* exact match */
	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool match;
		SpinLockAcquire(&t->mutex);
		match = target_key_equal(t, kind, name, relid, worker_char);
		SpinLockRelease(&t->mutex);
		if (match)
			return t;
	}

	/* free slot or stale victim */
	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		SpinLockAcquire(&t->mutex);
		if (!t->in_use)
		{
			target_init(t, kind, name, relid, worker_char);
			SpinLockRelease(&t->mutex);
			return t;
		}
		if (victim == NULL || t->last_sample_ts < victim_ts)
		{
			victim = t;
			victim_ts = t->last_sample_ts;
		}
		SpinLockRelease(&t->mutex);
	}

	if (victim != NULL &&
		TimestampDifferenceMilliseconds(victim_ts, GetCurrentTimestamp()) >=
		(double) lrstat_stale_target_ttl_s * 1000.0)
	{
		SpinLockAcquire(&victim->mutex);
		target_init(victim, kind, name, relid, worker_char);
		SpinLockRelease(&victim->mutex);
		return victim;
	}

	lrstat_note_dropped(name);
	return NULL;
}

bool
lrstat_lookup(LRTargetKind kind, const char *name,
			  LRSample *anchor, LRSample *prev, LRSample *last,
			  LRTargetMeta *meta_out)
{
	int i;
	if (!lrstat_ready())
		return false;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool match;
		SpinLockAcquire(&t->mutex);
		match = (t->in_use && t->kind == kind &&
				 strcmp(t->name, name) == 0);
		if (match)
		{
			memcpy(anchor, &t->anchor, sizeof(LRSample));
			memcpy(prev, &t->prev, sizeof(LRSample));
			memcpy(last, &t->last, sizeof(LRSample));
			memcpy(meta_out, &t->meta, sizeof(LRTargetMeta));
		}
		SpinLockRelease(&t->mutex);
		if (match)
			return true;
	}
	return false;
}

/*
 * A sample with all-zero LSNs (e.g. a subscription whose worker is not
 * running) is not a valid measurement baseline; never let it become
 * the anchor — avg rates would count the whole WAL history as delta.
 */
static bool
sample_valid(const LRTargetCtl *t, const LRSample *s)
{
	if (t->kind == LR_RECV)
		return s->recv.received_lsn != 0 || s->recv.applied_lsn != 0;
	return s->send.current_lsn != 0 || s->send.sent_lsn != 0 ||
		s->send.restart_lsn != 0;
}

void
lrstat_push_sample(LRTargetCtl *target, const LRSample *sample)
{
	SpinLockAcquire(&target->mutex);
	if (sample->send.ts > target->last_sample_ts)
	{
		/* rotate: prev = last; last = new; first valid sample sets anchor */
		if (target->last.send.ts > 0)
		{
			target->prev = target->last;
		}
		if (target->anchor.send.ts == 0 && sample_valid(target, sample))
		{
			target->anchor = *sample;
		}
		target->last = *sample;
		target->last_sample_ts = sample->send.ts;
	}
	SpinLockRelease(&target->mutex);
}

void
lrstat_set_meta(LRTargetCtl *target, const LRTargetMeta *meta)
{
	SpinLockAcquire(&target->mutex);
	memcpy(&target->meta, meta, sizeof(LRTargetMeta));
	SpinLockRelease(&target->mutex);
}

void
lrstat_copy_all(LRTargetCtl *target, LRSample *anchor, LRSample *prev,
				LRSample *last, LRTargetMeta *meta_out)
{
	SpinLockAcquire(&target->mutex);
	memcpy(anchor, &target->anchor, sizeof(LRSample));
	memcpy(prev, &target->prev, sizeof(LRSample));
	memcpy(last, &target->last, sizeof(LRSample));
	memcpy(meta_out, &target->meta, sizeof(LRTargetMeta));
	SpinLockRelease(&target->mutex);
}

/*
 * Fold feedback flush position into the newest recv sample's applied.
 * Value only ever grows; ts untouched (keeps differentiators monotonic).
 */
void
lrstat_bump_applied(const char *recv_name, XLogRecPtr applied)
{
	int i;
	if (!lrstat_ready() || applied == 0)
		return;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool match;
		SpinLockAcquire(&t->mutex);
		match = (t->in_use && t->kind == LR_RECV &&
				 t->worker_char == 'a' && t->relid == 0 &&
				 strcmp(t->name, recv_name) == 0);
		if (match)
		{
			if (t->last.recv.applied_lsn < applied)
				t->last.recv.applied_lsn = applied;
			SpinLockRelease(&t->mutex);
			return;
		}
		SpinLockRelease(&t->mutex);
	}
}

void
lrstat_session_start(const char *name)
{
	int i;
	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	lrstat->session.session_id++;
	lrstat->session.running = true;
	strlcpy(lrstat->session.name, name, NAMEDATALEN);
	lrstat->session.start_ts = GetCurrentTimestamp();
	lrstat->session.stop_ts = 0;
	lrstat->session.truncated = false;
	lrstat->session.degraded = false;
	SpinLockRelease(&lrstat->session.mutex);

	/* remember the session so history rows can be mapped back to it */
	lrstat_sessionreg_add(lrstat->session.session_id, name,
						  lrstat->session.start_ts);

	/* reset all targets and session log */
	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		SpinLockAcquire(&t->mutex);
		t->in_use = false;
		MemSet(&t->anchor, 0, sizeof(LRSample));
		MemSet(&t->prev, 0, sizeof(LRSample));
		MemSet(&t->last, 0, sizeof(LRSample));
		t->last_sample_ts = 0;
		SpinLockRelease(&t->mutex);
	}
	lrstat->n_entries = 0;

	/* Create session file only when persist was explicitly requested */
	if (lrstat_persist_requested &&
		lrstat_store_create(name, lrstat->session.session_id,
							 lrstat->session.start_ts) != 0)
		lrstat->session.degraded = true;
	lrstat_persist_requested = false;  /* reset for next session */
}

void
lrstat_session_stop(void)
{
	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	lrstat->session.running = false;
	lrstat->session.stop_ts = GetCurrentTimestamp();
	SpinLockRelease(&lrstat->session.mutex);
	lrstat_sessionreg_close(lrstat->session.session_id,
							lrstat->session.stop_ts);

	/* Finalize session file only if it exists (persist session) */
	{
		char *path = psprintf("%s/pg_lrstat/sessions/%s.sess", DataDir,
							  lrstat->session.name);
		struct stat st;
		if (stat(path, &st) == 0)
			lrstat_store_finalize(lrstat->session.name, "stopped",
								  lrstat->session.truncated,
								  lrstat->session.degraded);
		pfree(path);
	}
}

void
lrstat_session_reset(void)
{
	int i;
	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->session.mutex);
	lrstat->session.running = false;
	lrstat->session.name[0] = '\0';
	lrstat->session.start_ts = 0;
	lrstat->session.stop_ts = 0;
	lrstat->session.truncated = false;
	lrstat->session.degraded = false;
	SpinLockRelease(&lrstat->session.mutex);

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		SpinLockAcquire(&t->mutex);
		t->in_use = false;
		SpinLockRelease(&t->mutex);
	}
}

/*
 * Fill one history entry from a raw sample (send fields for SEND/RSEND
 * targets, recv fields for RECV targets — the unused side stays zero).
 */
void
lrstat_history_from_sample(LRHistoryEntry *e, int target_idx,
						   const LRSample *sample)
{
	memset(e, 0, sizeof(LRHistoryEntry));
	e->ts = sample->send.ts;    /* ts is first member of both structs */
	e->target_idx = target_idx;
	e->current_lsn = sample->send.current_lsn;
	e->sent_lsn = sample->send.sent_lsn;
	e->peer_recv_lsn = sample->send.peer_recv_lsn;
	e->peer_flush_lsn = sample->send.peer_flush_lsn;
	e->peer_applied_lsn = sample->send.peer_applied_lsn;
	e->confirmed_lsn = sample->send.confirmed_lsn;
	e->restart_lsn = sample->send.restart_lsn;
	e->spill_bytes = sample->send.spill_bytes;
	e->stream_bytes = sample->send.stream_bytes;
	e->received_lsn = sample->recv.received_lsn;
	e->applied_lsn = sample->recv.applied_lsn;
	e->local_wal_lsn = sample->recv.local_wal_lsn;
}

/*
 * Append one full history entry (raw sample).  The array wraps when
 * full: history always covers the most recent rounds, and `truncated`
 * marks that the oldest samples were overwritten.  n_entries is the
 * write cursor (single writer: the worker).
 */
void
lrstat_append_history_entry(const LRHistoryEntry *e)
{
	int max_entries;

	if (!lrstat_ready())
		return;

	max_entries = lrstat->ntargets * lrstat->ring_len;
	if (max_entries <= 0)
		return;

	if (lrstat->n_entries >= max_entries)
	{
		lrstat->n_entries = 0;
		lrstat->session.truncated = true;
	}

	*lrstat_history_at(lrstat->n_entries) = *e;
	lrstat->n_entries++;
}

void
lrstat_reset_entries(void)
{
	if (!lrstat_ready())
		return;
	lrstat->n_entries = 0;
	lrstat->session.truncated = false;
}

int
lrstat_get_entry_count(void)
{
	if (!lrstat_ready())
		return 0;
	return lrstat->n_entries;
}

/* Number of valid entries: cursor while filling, capacity once wrapped. */
int
lrstat_history_count(void)
{
	if (!lrstat_ready())
		return 0;
	if (lrstat->session.truncated)
		return lrstat->ntargets * lrstat->ring_len;
	return lrstat->n_entries;
}

/* Index of the i-th oldest entry (i = 0 .. lrstat_history_count()-1). */
int
lrstat_history_slot(int i)
{
	int max_entries = lrstat->ntargets * lrstat->ring_len;
	int oldest = lrstat->session.truncated ? lrstat->n_entries : 0;
	return (oldest + i) % max_entries;
}

void
lrstat_note_round(bool ok, const char *error)
{
	if (!lrstat_ready())
		return;
	/* stored in info view via shared header; simplified for P0 */
}

void
lrstat_note_dropped(const char *name)
{
	if (!lrstat_ready())
		return;
	/* counted via pg_atomic; simplified for P0 */
}
