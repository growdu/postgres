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

#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/shmem.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

#include "lrstat.h"

LRStatShared *lrstat = NULL;

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

Size
lrstat_shmem_size(void)
{
	Size header = MAXALIGN(sizeof(LRStatShared));
	Size targets = (Size) lrstat_max_targets * MAXALIGN(sizeof(LRTargetCtl));
	Size entries = (Size) lrstat_max_targets * (Size) lrstat_ring_len *
		sizeof(LRSessionEntry);
	return header + targets + entries;
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

LRSessionEntry *
lrstat_entry_at(int idx)
{
	return (LRSessionEntry *) (entry_base() +
							   (Size) idx * sizeof(LRSessionEntry));
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

void
lrstat_push_sample(LRTargetCtl *target, const LRSample *sample)
{
	SpinLockAcquire(&target->mutex);
	if (sample->send.ts > target->last_sample_ts)
	{
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

	/* Create session file for persist sessions */
	if (lrstat_store_create(name, lrstat->session.session_id,
							 lrstat->session.start_ts) != 0)
		lrstat->session.degraded = true;
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

	/* Finalize session file */
	lrstat_store_finalize(lrstat->session.name, "stopped",
						  lrstat->ntargets, lrstat->n_entries,
						  lrstat->session.truncated,
						  lrstat->session.degraded);
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
 * Append one interval entry.  Returns entry index or -1 if full.
 * n_entries tracks the write position (single writer: the worker).
 */
int
lrstat_append_entry(int target_idx, TimestampTz ts,
					int64 d_curr, int64 d_sent, int64 d_recv,
					int64 d_applied, int64 d_spill, int64 d_stream)
{
	LRSessionEntry *e;
	int max_entries;

	if (!lrstat_ready())
		return -1;

	max_entries = lrstat->ntargets * lrstat->ring_len;
	if (lrstat->n_entries >= max_entries)
	{
		lrstat->session.truncated = true;
		return -1;
	}

	e = lrstat_entry_at(lrstat->n_entries);
	e->ts = ts;
	e->target_idx = target_idx;
	e->d_current = d_curr;
	e->d_sent = d_sent;
	e->d_received = d_recv;
	e->d_applied = d_applied;
	e->d_spill = d_spill;
	e->d_stream = d_stream;
	lrstat->n_entries++;
	return lrstat->n_entries - 1;
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
