/*-------------------------------------------------------------------------
 *
 * parallel_decode_trace.c
 *		Publisher-side parallel logical decoding: wire trace facility.
 *
 * The trace facility records the replication frames that the walsender sends
 * to the subscriber.  It exists so that the frame level dump/diff tool can
 * compare the bytes produced by the serial decoding path against the bytes
 * produced by the parallel path: the parallel path is only correct if the
 * two are equivalent up to frame order and up to the clamping of the frame
 * header LSNs.
 *
 * The facility is inactive unless parallel_decode_trace_file names a file.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>

#include "miscadmin.h"
#include "replication/parallel_decode.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* GUC */
char	   *parallel_decode_trace_file = NULL;

static FILE *pd_trace_file = NULL;
static bool pd_trace_broken = false;

/*
 * Encode a length in network byte order.  Done by hand so that the trace
 * file does not depend on the host's byte order.
 */
static uint32
pd_trace_be32(uint32 v)
{
	uint8		b[4];

	b[0] = (v >> 24) & 0xFF;
	b[1] = (v >> 16) & 0xFF;
	b[2] = (v >> 8) & 0xFF;
	b[3] = v & 0xFF;

	return (uint32) b[0] | ((uint32) b[1] << 8) |
		((uint32) b[2] << 16) | ((uint32) b[3] << 24);
}

/*
 * Open the trace file and write the session marker.
 */
void
PDTraceInit(void)
{
	uint8		marker[9];
	uint32		netlen;

	if (parallel_decode_trace_file == NULL ||
		parallel_decode_trace_file[0] == '\0')
		return;

	if (pd_trace_file != NULL)
		return;					/* already open */

	pd_trace_broken = false;

	pd_trace_file = AllocateFile(parallel_decode_trace_file, "ab");
	if (pd_trace_file == NULL)
	{
		ereport(WARNING,
				(errmsg("could not open parallel decode trace file \"%s\": %m",
						parallel_decode_trace_file)));
		return;
	}

	/*
	 * A session marker separates the frames of different sessions in the
	 * same file.  The first byte is zero, which no replication frame can
	 * have, the remaining eight bytes hold the session id.
	 */
	marker[0] = 0;
	{
		uint64		session_id = (uint64) MyProcPid;

		marker[1] = (session_id >> 56) & 0xFF;
		marker[2] = (session_id >> 48) & 0xFF;
		marker[3] = (session_id >> 40) & 0xFF;
		marker[4] = (session_id >> 32) & 0xFF;
		marker[5] = (session_id >> 24) & 0xFF;
		marker[6] = (session_id >> 16) & 0xFF;
		marker[7] = (session_id >> 8) & 0xFF;
		marker[8] = session_id & 0xFF;
	}

	netlen = pd_trace_be32((uint32) sizeof(marker));
	if (fwrite(&netlen, 1, sizeof(netlen), pd_trace_file) != sizeof(netlen) ||
		fwrite(marker, 1, sizeof(marker), pd_trace_file) != sizeof(marker))
		pd_trace_broken = true;
	else
		fflush(pd_trace_file);
}

/*
 * Record one replication frame.
 */
void
PDTraceWrite(const char *data, size_t len)
{
	uint32		netlen;

	if (pd_trace_file == NULL || pd_trace_broken)
		return;

	netlen = pd_trace_be32((uint32) len);

	if (fwrite(&netlen, 1, sizeof(netlen), pd_trace_file) != sizeof(netlen) ||
		fwrite(data, 1, len, pd_trace_file) != len)
	{
		ereport(WARNING,
				(errmsg("could not write to parallel decode trace file \"%s\": %m",
						parallel_decode_trace_file)));
		pd_trace_broken = true;
		return;
	}

	/*
	 * Flush every record: the interesting traces are written by sessions that
	 * are killed on purpose, and a partially written record would make the
	 * trace file unparsable.
	 */
	fflush(pd_trace_file);
}

/*
 * Close the trace file.
 */
void
PDTraceShutdown(void)
{
	if (pd_trace_file == NULL)
		return;

	fflush(pd_trace_file);
	FreeFile(pd_trace_file);
	pd_trace_file = NULL;
	pd_trace_broken = false;
}
