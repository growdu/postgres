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
#include "utils/memutils.h"

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

/*
 * Initialize a chunk receiver.
 */
void
PDChunkReceiverInit(PDChunkReceiver *rcv)
{
	memset(rcv, 0, sizeof(*rcv));
}

/*
 * Throw away a partially received payload.
 */
void
PDChunkReceiverReset(PDChunkReceiver *rcv)
{
	if (rcv->buffer)
		pfree(rcv->buffer);
	memset(rcv, 0, sizeof(*rcv));
}

/*
 * Receive a payload that may be split over several queue entries.
 *
 * The first entry of a payload carries the total length of the payload, the
 * following ones carry plain data.  A transfer that is not complete yet is
 * kept in the receiver, so the caller can wait for more data and pick the
 * transfer up again.
 *
 * On success the payload points to a buffer the caller owns and has to
 * free.  The payload stays valid until the next call on the same receiver.
 */
shm_mq_result
PDChunkReceiverRecv(PDChunkReceiver *rcv, shm_mq_handle *mqh, uint16 *type,
					const void **payload, size_t *len)
{
	for (;;)
	{
		uint16		msgtype;
		const void *data;
		size_t		datalen;
		shm_mq_result res;

		res = PDQueueRecv(mqh, &msgtype, &data, &datalen, true);
		if (res != SHM_MQ_SUCCESS)
			return res;

		if (!rcv->started)
		{
			uint32		total;

			if (datalen < sizeof(uint32))
				ereport(ERROR,
						(errcode(ERRCODE_DATA_CORRUPTED),
						 errmsg("parallel decode chunk is too short (%zu bytes)",
								(size_t) datalen)));

			memcpy(&total, data, sizeof(total));

			if (total > PD_MESSAGE_MAX_PAYLOAD)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("parallel decode payload of %u bytes exceeds the limit of %d bytes",
								total, PD_MESSAGE_MAX_PAYLOAD)));

			rcv->expected = total;
			rcv->len = 0;
			rcv->started = true;
			rcv->buffer = palloc(total > 0 ? total : 1);

			*type = msgtype;

			/* The first four bytes are the length; the rest is data. */
			data = (const char *) data + sizeof(uint32);
			datalen -= sizeof(uint32);
		}

		if (rcv->len + datalen > rcv->expected)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode chunk carries more data than announced")));

		if (datalen > 0)
			memcpy(rcv->buffer + rcv->len, data, datalen);
		rcv->len += datalen;

		if (rcv->len == rcv->expected)
		{
			*payload = rcv->buffer;
			*len = rcv->len;

			/* The caller owns the buffer now. */
			rcv->buffer = NULL;
			rcv->len = 0;
			rcv->expected = 0;
			rcv->started = false;

			return SHM_MQ_SUCCESS;
		}

		/*
		 * The payload is not complete yet.  Go back to the top and try to get
		 * the next chunk; if the queue is empty, the partial payload stays in
		 * the receiver and the caller can wait for more data.
		 */
	}
}

/*
 * Send a payload that may be larger than one queue entry.
 *
 * The payload is split into chunks of at most chunk_size bytes.  The first
 * chunk carries the total length of the payload, the following ones carry
 * plain data.
 *
 * The transfer always blocks until the whole payload has been handed over to
 * the queue, so that a partially written payload can never be mistaken for a
 * complete one.
 */
bool
PDQueueSendChunked(shm_mq_handle *mqh, uint16 type, const void *payload,
				   size_t len, size_t chunk_size, bool nowait, bool force_flush)
{
	const char *data = payload;
	size_t		remaining = len;
	bool		first = true;

	if (nowait)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a chunked parallel decode payload cannot be sent without waiting")));

	if (chunk_size <= sizeof(uint32))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the chunk size of a parallel decode payload is too small")));

	if (len > PD_MESSAGE_MAX_PAYLOAD)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("parallel decode payload of %zu bytes exceeds the limit of %d bytes",
						len, PD_MESSAGE_MAX_PAYLOAD)));

	do
	{
		ParallelDecodeMessageHeader header;
		shm_mq_iovec iov[3];
		size_t		msglen = 0;
		size_t		this_chunk;
		int			iovcnt;
		int			i;
		shm_mq_result res;

		header.magic = PD_MESSAGE_MAGIC;
		header.type = type;
		header.flags = 0;
		header.length = 0;

		iov[0].data = (const char *) &header;
		iov[0].len = sizeof(header);
		iovcnt = 1;

		if (first)
		{
			uint32		total = (uint32) len;

			iov[1].data = (const char *) &total;
			iov[1].len = sizeof(total);
			iovcnt = 2;

			this_chunk = Min(remaining, chunk_size - sizeof(uint32));
		}
		else
			this_chunk = Min(remaining, chunk_size);

		if (this_chunk > 0)
		{
			iov[iovcnt].data = data;
			iov[iovcnt].len = this_chunk;
			iovcnt++;
		}

		for (i = 1; i < iovcnt; i++)
			msglen += iov[i].len;
		header.length = (uint32) msglen;

		res = shm_mq_sendv(mqh, iov, iovcnt, nowait, force_flush);

		if (res == SHM_MQ_WOULD_BLOCK)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not send a parallel decode payload: the queue is full")));
		if (res != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not send a parallel decode payload: %s",
							pd_mq_result_name(res))));

		data += this_chunk;
		remaining -= this_chunk;
		first = false;
	} while (remaining > 0);

	return true;
}
