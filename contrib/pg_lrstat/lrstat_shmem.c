/*-------------------------------------------------------------------------
 *
 * lrstat_shmem.c
 *      Shared memory layout and the per-target ring buffer.
 *
 *      Layout: [LRStatShared header][target 0][ring 0][target 1][ring 1]...
 *      Each target is stride-spaced; the ring directly follows its
 *      control block (LR_TARGET_RING).  The single writer is the
 *      sampler worker; readers only copy data out under the spinlock,
 *      so a spinlock per target is sufficient.
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
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "lrstat.h"

/* Set in the postmaster by lrstat_shmem_startup(); inherited by fork. */
LRStatShared *lrstat = NULL;

/* Set by _PG_init: true when the library ran during preload. */
static bool lrstat_was_preloaded = false;

/*
 * Attach the shared segment created by the postmaster.  On fork-based
 * platforms backends inherit the pointer; on EXEC_BACKEND platforms
 * (Windows) they must find it in the shmem index.  The lookup must
 * never create an entry, which is guaranteed because we only get here
 * when the library was preloaded.
 */
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
		bool		found;

		lrstat = (LRStatShared *) ShmemInitStruct("pg_lrstat",
												  lrstat_shmem_size(),
												  &found);
		if (!found)
		{
			/* cannot happen: postmaster created the segment */
			elog(WARNING, "pg_lrstat: shared segment missing at attach");
			lrstat = NULL;
		}
	}
#endif							/* EXEC_BACKEND */
}

static Size
target_stride(void)
{
	return MAXALIGN(sizeof(LRTargetCtl)) +
		(Size) lrstat_ring_len * sizeof(LRSample);
}

Size
lrstat_shmem_size(void)
{
	return MAXALIGN(sizeof(LRStatShared)) +
		(Size) lrstat_max_targets * target_stride();
}

/*
 * on_shmem_callback entry: create or attach the segment.  Runs in the
 * postmaster before backends are forked.
 */
void
lrstat_shmem_startup(void)
{
	bool	found;
	Size	size = lrstat_shmem_size();
	int		i;

	lrstat = (LRStatShared *) ShmemInitStruct("pg_lrstat", size, &found);

	if (found)
		return;

	lrstat->magic = LRSTAT_MAGIC;
	lrstat->layout_version = LRSTAT_LAYOUT_VERSION;
	lrstat->ntargets = lrstat_max_targets;
	SpinLockInit(&lrstat->hdr_mutex);
	lrstat->last_round_ts = 0;
	lrstat->last_round_ok = false;
	lrstat->last_round_error[0] = '\0';
	pg_atomic_init_u64(&lrstat->nrounds, 0);
	pg_atomic_init_u64(&lrstat->dropped_samples, 0);

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);

		MemSet(t, 0, sizeof(LRTargetCtl));
		SpinLockInit(&t->mutex);
		t->ring_len = lrstat_ring_len;
	}
}

bool
lrstat_ready(void)
{
	lrstat_attach();

	return lrstat != NULL &&
		lrstat->magic == LRSTAT_MAGIC &&
		lrstat->layout_version == LRSTAT_LAYOUT_VERSION;
}

LRTargetCtl *
lrstat_target_at(int i)
{
	char	   *base = ((char *) lrstat) + MAXALIGN(sizeof(LRStatShared));

	return (LRTargetCtl *) (base + (Size) i * target_stride());
}

/* Initialize a claimed control block; caller holds the spinlock. */
static void
target_init(LRTargetCtl *t, LRTargetKind kind, const char *name,
			Oid relid, char worker_char)
{
	t->kind = kind;
	strlcpy(t->name, name, NAMEDATALEN);
	t->worker_char = worker_char;
	t->relid = relid;
	t->in_use = true;
	t->generation++;
	t->head = 0;
	t->n_samples = 0;
	t->last_sample_ts = 0;
	MemSet(&t->meta, 0, sizeof(t->meta));
	t->meta.write_lag_us = -1;
	t->meta.flush_lag_us = -1;
	t->meta.replay_lag_us = -1;
}

/* Does this control block hold the given target key?  Lock held. */
static bool
target_key_equal(const LRTargetCtl *t, LRTargetKind kind, const char *name,
				 Oid relid, char worker_char)
{
	if (!t->in_use || t->kind != kind || strcmp(t->name, name) != 0)
		return false;
	if (kind != LR_SUB)
		return true;
	return t->relid == relid && t->worker_char == worker_char;
}

/*
 * Find the target with the given key, or claim a slot for it.  When all
 * slots are busy, the target with the oldest sample is recycled; if
 * that one has been sampled recently, the sample is dropped instead of
 * evicting live data (and counted in LRStatShared.dropped_samples).
 */
LRTargetCtl *
lrstat_find_or_create(LRTargetKind kind, const char *name,
					  Oid relid, char worker_char)
{
	LRTargetCtl *victim = NULL;
	TimestampTz victim_ts = 0;
	int			i;

	if (!lrstat_ready())
		return NULL;

	/* exact match */
	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool		match;

		SpinLockAcquire(&t->mutex);
		match = target_key_equal(t, kind, name, relid, worker_char);
		SpinLockRelease(&t->mutex);
		if (match)
			return t;
	}

	/* no match: claim a free slot, or the oldest one if it went stale */
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

/*
 * Look up a target by (kind, name) and copy out its ring (newest first)
 * and meta.  Returns false if the target does not exist.
 */
bool
lrstat_lookup(LRTargetKind kind, const char *name,
			  LRSample *ring, int maxn, int *n_out, LRTargetMeta *meta_out)
{
	int			i;

	if (!lrstat_ready())
		return false;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool		match;

		SpinLockAcquire(&t->mutex);
		match = (t->in_use && t->kind == kind &&
				 strcmp(t->name, name) == 0);
		if (match)
		{
			*n_out = lrstat_copy_ring(t, ring, maxn);
			memcpy(meta_out, &t->meta, sizeof(LRTargetMeta));
		}
		SpinLockRelease(&t->mutex);
		if (match)
			return true;
	}
	return false;
}

/* Caller must not hold the spinlock (we take it here). */
void
lrstat_push_sample(LRTargetCtl *target, const LRSample *sample)
{
	SpinLockAcquire(&target->mutex);

	/* ts is the first member of both structs; keep the ring monotonic */
	if (sample->pub.ts > target->last_sample_ts)
	{
		LR_TARGET_RING(target)[target->head] = *sample;
		target->head = (target->head + 1) % target->ring_len;
		if (target->n_samples < target->ring_len)
			target->n_samples++;
		target->last_sample_ts = sample->pub.ts;
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

/*
 * Copy up to maxn samples out, newest first.  Caller must hold the
 * spinlock.
 */
int
lrstat_copy_ring(LRTargetCtl *target, LRSample *out, int maxn)
{
	int			n = Min(target->n_samples, maxn);
	int			i;

	for (i = 0; i < n; i++)
	{
		uint32		idx = (target->head + target->ring_len - 1 - i) %
			target->ring_len;

		out[i] = LR_TARGET_RING(target)[idx];
	}
	return n;
}

/* Caller must hold the spinlock. */
void
lrstat_copy_meta(LRTargetCtl *target, LRTargetMeta *out)
{
	memcpy(out, &target->meta, sizeof(LRTargetMeta));
}

void
lrstat_reset_all(void)
{
	int			i;

	if (!lrstat_ready())
		return;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);

		SpinLockAcquire(&t->mutex);
		t->in_use = false;
		t->n_samples = 0;
		t->head = 0;
		SpinLockRelease(&t->mutex);
	}
}

/*
 * Fold the feedback flush position into the newest sample of the
 * named subscription's leader target.  The origin remote_lsn only
 * moves at transaction-commit boundaries, so the applied series (and
 * every rate derived from it) can sit still while the subscription is
 * visibly making progress -- folding the feedback position in keeps
 * apply_rate and backlog_unapplied on one consistent series.  The
 * value only ever grows and the sample timestamp is untouched, so
 * differentiators still see a monotonic sequence.
 */
void
lrstat_bump_applied(const char *subname, XLogRecPtr applied)
{
	int			i;

	if (!lrstat_ready() || applied == 0)
		return;

	for (i = 0; i < lrstat->ntargets; i++)
	{
		LRTargetCtl *t = lrstat_target_at(i);
		bool		match;

		SpinLockAcquire(&t->mutex);
		match = (t->in_use && t->kind == LR_SUB &&
				 t->worker_char == 'a' && t->relid == 0 &&
				 strcmp(t->name, subname) == 0);
		if (match && t->n_samples > 0)
		{
			uint32		idx = (t->head + t->ring_len - 1) % t->ring_len;

			if (LR_TARGET_RING(t)[idx].sub.applied_lsn < applied)
				LR_TARGET_RING(t)[idx].sub.applied_lsn = applied;
		}
		SpinLockRelease(&t->mutex);
		if (match)
			return;
	}
}

/*
 * Record the outcome of one sampler round.  The error text is copied
 * under the header spinlock; truncation is acceptable for diagnostics.
 */
void
lrstat_note_round(bool ok, const char *error)
{
	if (!lrstat_ready())
		return;

	SpinLockAcquire(&lrstat->hdr_mutex);
	lrstat->last_round_ts = GetCurrentTimestamp();
	lrstat->last_round_ok = ok;
	if (ok)
		lrstat->last_round_error[0] = '\0';
	else
		strlcpy(lrstat->last_round_error, error ? error : "unknown error",
				LR_ERROR_LEN);
	SpinLockRelease(&lrstat->hdr_mutex);

	pg_atomic_fetch_add_u64(&lrstat->nrounds, 1);
}

/*
 * A sample was dropped because every target slot was busy with fresh
 * data.  Count it and complain, rate-limited to once a minute per
 * process, so capacity problems are visible instead of silent.
 */
void
lrstat_note_dropped(const char *name)
{
	static TimestampTz last_warn = 0;
	TimestampTz now;

	if (!lrstat_ready())
		return;

	pg_atomic_fetch_add_u64(&lrstat->dropped_samples, 1);

	now = GetCurrentTimestamp();
	if (last_warn == 0 ||
		TimestampDifferenceMilliseconds(last_warn, now) >= 60 * 1000.0)
	{
		last_warn = now;
		ereport(WARNING,
				(errmsg("pg_lrstat: dropped a sample for \"%s\": all %d "
						"target slots hold fresh data",
						name, lrstat->ntargets),
				 errhint("consider raising pg_lrstat.max_targets "
						 "(restart required).")));
	}
}
