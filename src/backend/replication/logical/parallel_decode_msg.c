/*-------------------------------------------------------------------------
 *
 * parallel_decode_msg.c
 *		Publisher-side parallel logical decoding: message framing.
 *
 * Every entry that travels between the participants of a parallel decoding
 * session is a fixed size header followed by a payload.  The framing layer
 * here is deliberately thin:
 *
 *   - the sender hands the header and the payload to the queue as two
 *     separate vectors, so the payload is never copied into a scratch
 *     buffer first;
 *   - the receiver gets a pointer into the ring buffer of the queue, so the
 *     payload is not copied out either.
 *
 * All queues are single sender / single receiver, which is what makes the
 * zero copy receive safe: the payload of an entry stays valid until the
 * next receive from the same queue.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "replication/parallel_decode.h"
#include "storage/shm_mq.h"

static const char *
pd_mq_result_name(shm_mq_result res)
{
	switch (res)
	{
		case SHM_MQ_SUCCESS:
			return "success";
		case SHM_MQ_WOULD_BLOCK:
			return "would block";
		case SHM_MQ_DETACHED:
			return "detached";
	}
	return "unknown";
}

/*
 * Send one entry.
 *
 * Returns true when the entry was handed over to the queue, and false when
 * the queue was full and nowait was requested.  The caller is expected to
 * apply backpressure in the latter case.
 */
bool
PDQueueSend(shm_mq_handle *mqh, uint16 type, const void *payload, size_t len,
			bool nowait, bool force_flush)
{
	ParallelDecodeMessageHeader header;
	shm_mq_iovec iov[2];
	shm_mq_result res;
	int			iovcnt;

	if (len > PD_MESSAGE_MAX_PAYLOAD)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("parallel decode message of %zu bytes exceeds the limit of %d bytes",
						len, PD_MESSAGE_MAX_PAYLOAD)));

	header.magic = PD_MESSAGE_MAGIC;
	header.type = type;
	header.flags = 0;
	header.length = (uint32) len;

	iov[0].data = (const char *) &header;
	iov[0].len = sizeof(header);

	if (payload != NULL && len > 0)
	{
		iov[1].data = (const char *) payload;
		iov[1].len = len;
		iovcnt = 2;
	}
	else
		iovcnt = 1;

	res = shm_mq_sendv(mqh, iov, iovcnt, nowait, force_flush);

	if (res == SHM_MQ_SUCCESS)
		return true;

	if (res == SHM_MQ_WOULD_BLOCK)
		return false;

	ereport(ERROR,
			(errcode(ERRCODE_IO_ERROR),
			 errmsg("could not send a parallel decode message: %s",
					pd_mq_result_name(res))));
	return false;
}

/*
 * Receive one entry.
 *
 * On success the payload points into the ring buffer of the queue and is
 * valid only until the next receive from the same queue.
 */
shm_mq_result
PDQueueRecv(shm_mq_handle *mqh, uint16 *type, const void **payload, size_t *len,
			bool nowait)
{
	ParallelDecodeMessageHeader header;
	Size		nbytes;
	void	   *data;
	shm_mq_result res;

	res = shm_mq_receive(mqh, &nbytes, &data, nowait);
	if (res != SHM_MQ_SUCCESS)
		return res;

	if (nbytes < sizeof(ParallelDecodeMessageHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode message is too short (%zu bytes)",
						(size_t) nbytes)));

	memcpy(&header, data, sizeof(header));

	if (header.magic != PD_MESSAGE_MAGIC)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode message has a bad magic number")));

	if ((Size) header.length + sizeof(header) != nbytes)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode message has an inconsistent length (%u declared, %zu received)",
						header.length, (size_t) nbytes)));

	*type = header.type;
	*payload = (const char *) data + sizeof(header);
	*len = header.length;

	return SHM_MQ_SUCCESS;
}
