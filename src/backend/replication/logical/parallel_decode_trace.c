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
#include <unistd.h>

#include "miscadmin.h"
#include "replication/parallel_decode.h"
#include "utils/guc.h"
#include "utils/memutils.h"

/* GUC */
char	   *parallel_decode_trace_file = NULL;

/*
 * The trace file is held open for the whole session, so it must not use
 * AllocateFile(): AllocateFile() registers the file with the transaction
 * machinery, which closes it at the end of every transaction.  A walsender
 * decodes many transactions in a row, so the file would be closed right
 * after the first one, and the descriptor number could be reused by an
 * unrelated file.  A raw descriptor avoids both problems.
 */
static int	pd_trace_fd = -1;
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
 * Write the whole buffer, retrying on partial writes and on EINTR.
 */
static bool
pd_trace_write_all(const void *buf, size_t len)
{
	const char *p = (const char *) buf;

	while (len > 0)
	{
		ssize_t		written = write(pd_trace_fd, p, len);

		if (written < 0)
		{
			if (errno == EINTR)
				continue;
			return false;
		}
		if (written == 0)
			return false;

		p += written;
		len -= written;
	}

	return true;
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

	if (pd_trace_fd >= 0)
		return;					/* already open */

	pd_trace_broken = false;

	pd_trace_fd = open(parallel_decode_trace_file,
					   O_WRONLY | O_CREAT | O_APPEND, 0600);
	if (pd_trace_fd < 0)
	{
		ereport(LOG,
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
	if (!pd_trace_write_all(&netlen, sizeof(netlen)) ||
		!pd_trace_write_all(marker, sizeof(marker)))
	{
		ereport(LOG,
				(errmsg("could not write to parallel decode trace file "
						"\"%s\": %m",
						parallel_decode_trace_file)));
		pd_trace_broken = true;
	}
}

/*
 * Record one replication frame.
 */
void
PDTraceWrite(const char *data, size_t len)
{
	uint32		netlen;

	if (pd_trace_fd < 0)
		return;
	if (pd_trace_broken)
		return;

	netlen = pd_trace_be32((uint32) len);

	if (!pd_trace_write_all(&netlen, sizeof(netlen)) ||
		!pd_trace_write_all(data, len))
	{
		ereport(LOG,
				(errmsg("could not write to parallel decode trace file "
						"\"%s\": %m",
						parallel_decode_trace_file)));
		pd_trace_broken = true;
	}
}

/*
 * Record the release of a phase of the commit_sync barrier.
 *
 * The record tells what the barrier did with the phase: the phase number of
 * the transaction, whether the phase had an end event frame, and how many
 * frames and bytes it held.
 */
void
PDTraceWritePhase(uint64 cseq, uint32 flags, uint32 nframes, uint32 nbytes)
{
	uint8		buf[18];
	uint32		net;

	if (pd_trace_fd < 0 || pd_trace_broken)
		return;

	buf[0] = 'p';
	net = pd_trace_be32((uint32) (cseq >> 32));
	memcpy(buf + 1, &net, sizeof(net));
	net = pd_trace_be32((uint32) (cseq & 0xFFFFFFFF));
	memcpy(buf + 5, &net, sizeof(net));
	buf[9] = (uint8) flags;
	net = pd_trace_be32(nframes);
	memcpy(buf + 10, &net, sizeof(net));
	net = pd_trace_be32(nbytes);
	memcpy(buf + 14, &net, sizeof(net));

	PDTraceWrite((const char *) buf, sizeof(buf));
}

/*
 * Close the trace file.
 */
void
PDTraceShutdown(void)
{
	if (pd_trace_fd < 0)
		return;

	close(pd_trace_fd);
	pd_trace_fd = -1;
	pd_trace_broken = false;
}
