/*-------------------------------------------------------------------------
 *
 * lrstat_store.c
 *      Session file persistence: write, recover, list, delete.
 *
 *      persist=true sessions live as files under
 *      $PGDATA/pg_lrstat/sessions/<name>.sess, written directly by
 *      the worker (no SQL catalog, no WAL).
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 * contrib/pg_lrstat/lrstat_store.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "miscadmin.h"
#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "lrstat.h"

#define LRSTAT_DIR     "pg_lrstat"
#define LRSTAT_SESSIONS "pg_lrstat/sessions"
#define LRSTAT_SUFFIX  ".sess"

/* File header (written at start, updated at stop) */
typedef struct LRFileHeader
{
	int32		magic;
	int32		layout_version;
	uint64		session_id;
	char		name[NAMEDATALEN];
	TimestampTz start_ts;
	TimestampTz stop_ts;
	char		state[16];          /* running / stopped / interrupted */
	bool		truncated;
	bool		degraded;
	int32		n_targets;
	int32		n_entries;
} LRFileHeader;

static char *
lrstat_dir_path(void)
{
	return psprintf("%s/%s", DataDir, LRSTAT_SESSIONS);
}

static char *
lrstat_file_path(const char *name)
{
	return psprintf("%s/%s%s", lrstat_dir_path(), name, LRSTAT_SUFFIX);
}

/* Ensure the sessions directory exists (only for persist sessions) */
static void
lrstat_ensure_dir(void)
{
	char	   *parent = psprintf("%s/%s", DataDir, LRSTAT_DIR);
	char	   *dir = lrstat_dir_path();

	/* two levels: pg_lrstat/ then pg_lrstat/sessions/ */
	if (MakePGDirectory(parent) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_lrstat: could not create directory %s", parent)));

	/* Use MakePGDirectory which respects PGDIRPERM */
	if (MakePGDirectory(dir) < 0 && errno != EEXIST)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("pg_lrstat: could not create directory %s", dir)));

	pfree(parent);
}

/*
 * Create a session file for a newly started persist session.
 * Returns 0 on success, -1 on failure (sets degraded).
 */
int
lrstat_store_create(const char *name, uint64 session_id, TimestampTz start_ts)
{
	char	   *path;
	LRFileHeader hdr;
	int			fd;

	lrstat_ensure_dir();
	path = lrstat_file_path(name);

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = LRSTAT_MAGIC;
	hdr.layout_version = LRSTAT_LAYOUT_VERSION;
	hdr.session_id = session_id;
	strlcpy(hdr.name, name, NAMEDATALEN);
	hdr.start_ts = start_ts;
	hdr.stop_ts = 0;
	strlcpy(hdr.state, "running", sizeof(hdr.state));
	hdr.truncated = false;
	hdr.degraded = false;
	hdr.n_targets = 0;
	hdr.n_entries = 0;

	fd = OpenTransientFile(path, O_CREAT | O_WRONLY | O_TRUNC);
	if (fd < 0)
		return -1;

	if (write(fd, &hdr, sizeof(hdr)) != sizeof(hdr))
	{
		CloseTransientFile(fd);
		return -1;
	}

	CloseTransientFile(fd);
	return 0;
}

/*
 * Append entries to a session file and fsync.
 * Called by the worker after each sampling round.
 */
void
lrstat_store_append(int n_entries, LRHistoryEntry *entries)
{
	char	   *path = lrstat_file_path(lrstat->session.name);
	int			fd;

	if (n_entries <= 0)
		return;

	fd = OpenTransientFile(path, O_WRONLY | O_APPEND);
	if (fd < 0)
	{
		lrstat->session.degraded = true;
		return;
	}

	if (write(fd, entries, n_entries * sizeof(LRHistoryEntry)) !=
		(size_t)(n_entries * sizeof(LRHistoryEntry)))
	{
		lrstat->session.degraded = true;
		CloseTransientFile(fd);
		return;
	}

	if (pg_fsync(fd) != 0)
		lrstat->session.degraded = true;

	CloseTransientFile(fd);
}

/*
 * Finalize a session file (stop or interrupted).
 * Appends a footer mapping target_idx -> target name (so the archived
 * session can be exported by name after restart), then rewrites the
 * header with final state; the entry count is derived from the file
 * size so wrapped in-memory rings don't undercount.
 */
void
lrstat_store_finalize(const char *name, const char *state,
					  bool truncated, bool degraded)
{
	char	   *path = lrstat_file_path(name);
	LRFileHeader hdr;
	struct stat st;
	int			fd;

	fd = OpenTransientFile(path, O_RDWR);
	if (fd < 0)
		return;

	if (fstat(fd, &st) != 0 ||
		read(fd, &hdr, sizeof(hdr)) != sizeof(hdr))
	{
		CloseTransientFile(fd);
		return;
	}

	/* footer: magic + live target names, appended at end of file */
	{
		int32		foot[2];
		int			n = 0, i;
		LRSessTargetInfo *infos;

		infos = palloc(lrstat->ntargets * sizeof(LRSessTargetInfo));
		for (i = 0; i < lrstat->ntargets; i++)
		{
			LRTargetCtl *t = lrstat_target_at(i);

			SpinLockAcquire(&t->mutex);
			if (t->in_use)
			{
				infos[n].target_idx = i;
				infos[n].kind = t->kind;
				strlcpy(infos[n].name, t->name, NAMEDATALEN);
				n++;
			}
			SpinLockRelease(&t->mutex);
		}

	/*
	 * Footer layout: [target infos][magic, n] with the magic at the very
	 * end of the file, so a reader can find it from EOF.
	 */
	if (lseek(fd, 0, SEEK_END) >= 0)
	{
		if (n == 0 ||
			write(fd, infos, n * sizeof(LRSessTargetInfo)) ==
			(size_t)(n * sizeof(LRSessTargetInfo)))
		{
			foot[0] = LRSTAT_MAGIC;
			foot[1] = n;
			write(fd, foot, sizeof(foot));
		}
	}
	pfree(infos);
	}

	strlcpy(hdr.state, state, sizeof(hdr.state));
	hdr.stop_ts = GetCurrentTimestamp();
	hdr.n_targets = lrstat->ntargets;
	hdr.n_entries = st.st_size > (off_t) sizeof(hdr)
		? (int32)((st.st_size - sizeof(hdr)) / sizeof(LRHistoryEntry))
		: 0;
	hdr.truncated = truncated;
	hdr.degraded = degraded;

	/* rewrite header at position 0 */
	if (lseek(fd, 0, SEEK_SET) < 0 ||
		write(fd, &hdr, sizeof(hdr)) != sizeof(hdr))
	{
		CloseTransientFile(fd);
		return;
	}

	pg_fsync(fd);
	CloseTransientFile(fd);
}

/*
 * Read an archived session file for export-by-name.  All out-parameters
 * except entries/targets may be NULL.  Returns 0 on success, -1 if the
 * file is missing or unreadable.
 */
int
lrstat_store_load(const char *name,
				  int64 *session_id, char *sess_name, int sess_name_len,
				  TimestampTz *start_ts, TimestampTz *stop_ts,
				  bool *truncated, bool *degraded,
				  LRHistoryEntry **entries, int *n_entries,
				  LRSessTargetInfo **targets, int *n_targets)
{
	char	   *path = lrstat_file_path(name);
	LRFileHeader hdr;
	struct stat st;
	int			fd;
	int32		foot[2] = {0, 0};
	off_t		entries_end;

	fd = OpenTransientFile(path, O_RDONLY);
	if (fd < 0)
		return -1;

	if (fstat(fd, &st) != 0 ||
		read(fd, &hdr, sizeof(hdr)) != sizeof(hdr) ||
		hdr.magic != LRSTAT_MAGIC ||
		hdr.layout_version != LRSTAT_LAYOUT_VERSION)
	{
		CloseTransientFile(fd);
		return -1;
	}

	/*
	 * Optional footer at EOF: [target infos][magic, n].  The magic sits in
	 * the last 8 bytes, so probe those first, then back up for the infos.
	 */
	entries_end = st.st_size;
	if (st.st_size >= (off_t) (sizeof(hdr) + sizeof(foot)))
	{
		if (lseek(fd, -(off_t) sizeof(foot), SEEK_END) >= 0 &&
			read(fd, foot, sizeof(foot)) == sizeof(foot) &&
			foot[0] == LRSTAT_MAGIC &&
			foot[1] >= 0 && foot[1] <= lrstat->ntargets &&
			st.st_size >= (off_t) (sizeof(hdr) + sizeof(foot) +
								   (off_t) foot[1] * sizeof(LRSessTargetInfo)))
		{
			entries_end = st.st_size - sizeof(foot) -
				(off_t) foot[1] * sizeof(LRSessTargetInfo);
			if (targets != NULL && foot[1] > 0)
			{
				*targets = palloc(foot[1] * sizeof(LRSessTargetInfo));
				if (lseek(fd, entries_end, SEEK_SET) >= 0 &&
					read(fd, *targets, foot[1] * sizeof(LRSessTargetInfo)) ==
					(size_t) (foot[1] * sizeof(LRSessTargetInfo)))
				{
					*n_targets = foot[1];
				}
				else
				{
					pfree(*targets);
					*targets = NULL;
					*n_targets = 0;
					entries_end = st.st_size;    /* footer unreadable */
				}
			}
		}
	}

	if (entries != NULL && entries_end > (off_t) sizeof(hdr))
	{
		int			n = (int)((entries_end - sizeof(hdr)) /
							   sizeof(LRHistoryEntry));

		if (n > 0)
		{
			*entries = palloc(n * sizeof(LRHistoryEntry));
			if (lseek(fd, sizeof(hdr), SEEK_SET) >= 0 &&
				read(fd, *entries, n * sizeof(LRHistoryEntry)) ==
				(size_t) (n * sizeof(LRHistoryEntry)))
			{
				*n_entries = n;
			}
			else
			{
				pfree(*entries);
				*entries = NULL;
				*n_entries = 0;
			}
		}
	}

	CloseTransientFile(fd);

	if (session_id != NULL)
		*session_id = (int64) hdr.session_id;
	if (sess_name != NULL)
		strlcpy(sess_name, hdr.name, sess_name_len);
	if (start_ts != NULL)
		*start_ts = hdr.start_ts;
	if (stop_ts != NULL)
		*stop_ts = hdr.stop_ts;
	if (truncated != NULL)
		*truncated = hdr.truncated;
	if (degraded != NULL)
		*degraded = hdr.degraded;
	return 0;
}

/*
 * Scan the sessions directory at startup: finalize any 'running'
 * files as 'interrupted'.  Called from shmem startup hook.
 */
void
lrstat_store_recover(void)
{
	char	   *dir_path = lrstat_dir_path();
	DIR		   *dir;
	struct dirent *de;

	dir = AllocateDir(dir_path);
	if (dir == NULL)
		return;     /* directory doesn't exist — default for persist=false */

	while ((de = ReadDir(dir, dir_path)) != NULL)
	{
		char	   *fname = de->d_name;
		size_t		namelen;
		char	   *path;
		int			fd;
		LRFileHeader hdr;

		if (strncmp(fname, ".", 1) == 0)
			continue;
		namelen = strlen(fname);
		if (namelen < 6 || strcmp(fname + namelen - 5, LRSTAT_SUFFIX) != 0)
			continue;

		path = psprintf("%s/%s", dir_path, fname);
		fd = OpenTransientFile(path, O_RDWR);
		if (fd < 0)
			continue;

		if (read(fd, &hdr, sizeof(hdr)) == sizeof(hdr) &&
			hdr.magic == LRSTAT_MAGIC &&
			hdr.layout_version == LRSTAT_LAYOUT_VERSION &&
			strcmp(hdr.state, "running") == 0)
		{
			elog(LOG, "pg_lrstat: recovering interrupted session %s", hdr.name);
			lrstat_store_finalize(hdr.name, "interrupted",
								  hdr.truncated, hdr.degraded);
		}

		CloseTransientFile(fd);
		pfree(path);
	}

	FreeDir(dir);
}

/*
 * List archived session names.  Returns palloc'd array of palloc'd strings.
 */
int
lrstat_store_list(char ***names_out)
{
	char	   *dir_path = lrstat_dir_path();
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
		size_t		namelen;

		if (strncmp(fname, ".", 1) == 0)
			continue;
		namelen = strlen(fname);
		if (namelen < 6 || strcmp(fname + namelen - 5, LRSTAT_SUFFIX) != 0)
			continue;

		if (n >= nalloc)
		{
			nalloc = nalloc == 0 ? 8 : nalloc * 2;
			/* repalloc requires non-NULL; first allocation via palloc */
			if (names == NULL)
				names = palloc(nalloc * sizeof(char *));
			else
				names = repalloc(names, nalloc * sizeof(char *));
		}
		/* strip .sess suffix */
		names[n] = pstrdup(fname);
		names[n][namelen - 5] = '\0';
		n++;
	}

	FreeDir(dir);
	*names_out = names;
	return n;
}

/*
 * Delete a session file.  Returns 0 on success, -1 if not found.
 */
int
lrstat_store_delete(const char *name)
{
	char	   *path = lrstat_file_path(name);
	struct stat st;

	if (stat(path, &st) != 0)
		return -1;

	if (unlink(path) != 0)
		return -1;

	return 0;
}
