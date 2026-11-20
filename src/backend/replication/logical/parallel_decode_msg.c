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

#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/value.h"
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
 * Set when a send failed because the queue was detached.
 *
 * The queue is detached when the process on the other end has gone away, or
 * when the shared segment of the session is going away.  Both are the normal
 * way for a participant to learn that the session is ending, so the
 * participants look at this flag to tell the end of the session apart from a
 * failure of the decoding.
 */
static bool pd_queue_detached = false;

/*
 * Did the last send fail because the queue was detached?
 */
bool
PDQueueDetached(void)
{
	return pd_queue_detached;
}

/*
 * The counters of the wakeups of the queues that are written with
 * PDQueueSend().
 *
 * The queues that carry the payload of the session have a sender of their own
 * (PDQueueSender), which counts the wakeups it causes.  The control queue is
 * written with PDQueueSend(), which has no sender, so the counter of that
 * queue is registered here.
 *
 * Every queue has exactly one sender, so the counters need no lock.
 */
#define PD_WAKEUP_COUNTERS_MAX	4

static struct
{
	shm_mq_handle *mqh;			/* the queue */
	volatile uint64 *wakeups;	/* the counter of the wakeups, may be NULL */
} pd_wakeup_counters[PD_WAKEUP_COUNTERS_MAX];

static int pd_wakeup_counters_used = 0;

/*
 * Register the counter of the wakeups of a queue.
 *
 * The counter counts the times the receiver of the queue is woken up.  A
 * queue that is registered twice keeps its counter.
 */
void
PDQueueSetWakeupCounter(shm_mq_handle *mqh, volatile uint64 *wakeups)
{
	int			i;

	for (i = 0; i < pd_wakeup_counters_used; i++)
	{
		if (pd_wakeup_counters[i].mqh == mqh)
		{
			pd_wakeup_counters[i].wakeups = wakeups;
			return;
		}
	}

	if (pd_wakeup_counters_used >= PD_WAKEUP_COUNTERS_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("too many parallel decode queues with a wakeup counter")));

	pd_wakeup_counters[pd_wakeup_counters_used].mqh = mqh;
	pd_wakeup_counters[pd_wakeup_counters_used].wakeups = wakeups;
	pd_wakeup_counters_used++;
}

/*
 * Note that the receiver of a queue has been woken up.
 *
 * A flush publishes what the sender has written and sets the latch of the
 * receiver, so the number of the flushes is the number of the wakeups of the
 * receiver.  The counter is part of the statistics of the monitoring views
 * (11.3).
 */
static void
pd_queue_count_queue_wakeup(shm_mq_handle *mqh)
{
	int			i;

	for (i = 0; i < pd_wakeup_counters_used; i++)
	{
		if (pd_wakeup_counters[i].mqh == mqh)
		{
			if (pd_wakeup_counters[i].wakeups)
				(*pd_wakeup_counters[i].wakeups)++;
			return;
		}
	}
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
	{
		/*
		 * A flush publishes what the sender has written and sets the latch of
		 * the receiver, so the number of the flushes is the number of the
		 * wakeups of the receiver.  The queues that count their wakeups have
		 * registered their counter.
		 *
		 * A send without a flush does not wake the receiver up right away:
		 * the queue publishes what the sender has written when the sender has
		 * written more than a quarter of the ring.  Such a send is not
		 * counted here, because the wakeup happens somewhere else.
		 */
		if (force_flush)
			pd_queue_count_queue_wakeup(mqh);

		return true;
	}

	if (res == SHM_MQ_WOULD_BLOCK)
		return false;

	/*
	 * The queue was detached: the process on the other end is gone, or the
	 * shared segment of the session is going away.  The flag tells the
	 * participant that this is the end of the session and not a failure of
	 * the decoding.
	 */
	pd_queue_detached = true;

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
					const void **payload, size_t *len, bool nowait)
{
	for (;;)
	{
		uint16		msgtype;
		const void *data;
		size_t		datalen;
		shm_mq_result res;

		res = PDQueueRecv(mqh, &msgtype, &data, &datalen, nowait);
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

			if (total > PD_MAX_TRANSFER_SIZE)
				ereport(ERROR,
						(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
						 errmsg("parallel decode payload of %u bytes exceeds the limit of %d bytes",
								total, PD_MAX_TRANSFER_SIZE)));

			rcv->expected = total;
			rcv->len = 0;
			rcv->started = true;
			rcv->buffer = palloc(total > 0 ? total : 1);

			/*
			 * The type of the message belongs to the transfer, not to the
			 * queue entry that happens to carry its first chunk.  Remember
			 * it here: the transfer may be completed by a later call, and
			 * that call has to report the type of the message that is
			 * completed.
			 */
			rcv->type = msgtype;

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
			/*
			 * The transfer is complete.  Report the type of the transfer:
			 * the caller must not be left with whatever its own variable
			 * happened to hold.
			 */
			*type = rcv->type;

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
static bool
pd_queue_send_chunked(shm_mq_handle *mqh, uint16 type,
					   const void *prefix, size_t prefixlen,
					   const void *payload, size_t len,
					   size_t chunk_size, bool nowait, bool force_flush)
{
	const char *data = payload;
	size_t		remaining = len;
	bool		first = true;

	if (nowait)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("a chunked parallel decode payload cannot be sent without waiting")));

	if (chunk_size <= sizeof(uint32) + prefixlen)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("the chunk size of a parallel decode payload is too small")));

	if (len + prefixlen > PD_MAX_TRANSFER_SIZE)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("parallel decode payload of %zu bytes exceeds the limit of %d bytes",
						len + prefixlen, PD_MAX_TRANSFER_SIZE)));

	do
	{
		ParallelDecodeMessageHeader header;
		shm_mq_iovec iov[4];
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
			uint32		total = (uint32) (prefixlen + len);

			iov[1].data = (const char *) &total;
			iov[1].len = sizeof(total);
			iovcnt = 2;

			if (prefixlen > 0)
			{
				iov[iovcnt].data = (const char *) prefix;
				iov[iovcnt].len = prefixlen;
				iovcnt++;
			}

			this_chunk = Min(remaining, chunk_size - sizeof(uint32) - prefixlen);
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
		{
			/*
			 * The queue was detached: the process on the other end is gone, or
			 * the shared segment of the session is going away.  The flag tells
			 * the participant that this is the end of the session and not a
			 * failure of the decoding.
			 *
			 * The transfer is incomplete, but that does not matter: the
			 * receiver is gone as well, or it is about to go.
			 */
			pd_queue_detached = true;

			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not send a parallel decode payload: %s",
							pd_mq_result_name(res))));
		}

		data += this_chunk;
		remaining -= this_chunk;
		first = false;
	} while (remaining > 0);

	return true;
}

/*
 * Send a payload that may be larger than one queue entry.
 */
bool
PDQueueSendChunked(shm_mq_handle *mqh, uint16 type, const void *payload,
				   size_t len, size_t chunk_size, bool nowait, bool force_flush)
{
	return pd_queue_send_chunked(mqh, type, NULL, 0, payload, len, chunk_size,
								  nowait, force_flush);
}

/*
 * The batch size that is in effect, in bytes.
 *
 * The size is capped at a quarter of the queue.  The queue publishes what the
 * sender has written when a quarter of the ring is full, so a larger batch
 * would not hold the data back any longer.  A batch that is larger than the
 * ring would be worse than that: the sender would wait for room that the
 * receiver cannot free, because the receiver cannot read the data that was
 * not published yet.
 *
 * A size of zero means that the messages are not batched: every one of them
 * is flushed on its own.
 */
size_t
PDQueueBatchSize(void)
{
	int			batch = parallel_decode_batch_size;

	if (batch <= 0)
		return 0;

	return (size_t) Min(batch, PARALLEL_DECODE_QUEUE_SIZE / 4);
}

void
PDQueueSenderInit(PDQueueSender *sender, shm_mq_handle *mqh,
				   volatile uint64 *wakeups)
{
	sender->mqh = mqh;
	sender->pending = 0;
	sender->wakeups = wakeups;
}

/*
 * Note that the receiver of the queue has been woken up.
 *
 * A flush publishes what the sender has written and sets the latch of the
 * receiver, so the number of the flushes is the number of the wakeups of the
 * receiver.  The counter is part of the statistics of the monitoring views
 * (11.3).
 */
static void
pd_queue_count_wakeup(PDQueueSender *sender)
{
	if (sender->wakeups)
		(*sender->wakeups)++;
}

/*
 * Account for the bytes of a message that was written to the queue.
 *
 * Returns true when the message has to be flushed, which is the case when
 * the batch is full.  The count is the number of bytes that were written
 * since the last flush: the receiver is woken up once per batch instead of
 * once per message.
 */
static bool
PDQueueSenderAccount(PDQueueSender *sender, size_t len)
{
	size_t		batch = PDQueueBatchSize();

	if (batch == 0)
	{
		/* The messages are not batched, so every one of them is flushed. */
		sender->pending = 0;
		return true;
	}

	sender->pending += len;

	if (sender->pending >= batch)
	{
		sender->pending = 0;
		return true;
	}

	return false;
}

/*
 * Publish the data that was written to the queue but not flushed yet.
 *
 * The queue has no notify call of its own, so the flush is a message of its
 * own: an empty message that the receiver ignores.  The sender calls this
 * before it goes to sleep, so that the receiver does not wait for data that
 * is already in the ring buffer.
 */
void
PDQueueSenderFlush(PDQueueSender *sender)
{
	if (sender->pending == 0)
		return;

	pd_queue_send_chunked(sender->mqh, PD_ENT_FLUSH, NULL, 0, NULL, 0,
						  PARALLEL_DECODE_CHUNK_SIZE, false, true);

	sender->pending = 0;

	/* The receiver has been woken up. */
	pd_queue_count_wakeup(sender);
}

/*
 * Write a chunked message to the queue of the sender.
 *
 * The message is batched: the receiver is woken up when the batch is full,
 * not for every message.  The bytes of the message are added to the pending
 * counter of the sender, which is what decides when the batch is full.
 *
 * The payload is preceded by a prefix, which is the header that tells the
 * receiver what the message is about.
 */
static bool
pd_queue_send_batched(PDQueueSender *sender, uint16 type,
					   const void *prefix, size_t prefixlen,
					   const void *payload, size_t len)
{
	bool		flush = PDQueueSenderAccount(sender, prefixlen + len);

	if (!pd_queue_send_chunked(sender->mqh, type, prefix, prefixlen,
							   payload, len, PARALLEL_DECODE_CHUNK_SIZE,
							   false, flush))
		return false;

	if (flush)
	{
		/* The receiver has been woken up. */
		pd_queue_count_wakeup(sender);
	}

	return true;
}

/*
 * Write a chunked message to the queue of the sender, without batching it.
 *
 * The messages that carry the progress of the session are written this way:
 * the receiver has to see them right away, because they are what tells it
 * how far the session has come.
 */
bool
PDQueueSendUnbatched(PDQueueSender *sender, uint16 type,
					 const void *payload, size_t len)
{
	sender->pending = 0;

	if (!pd_queue_send_chunked(sender->mqh, type, NULL, 0, payload, len,
							   PARALLEL_DECODE_CHUNK_SIZE, false, true))
		return false;

	/* The receiver has been woken up. */
	pd_queue_count_wakeup(sender);

	return true;
}

/*
 * Send a replication frame to the walsender.
 *
 * The frame is preceded by a PDFrameHeader, which tells the walsender how far
 * the decoding has come and which transaction the frame belongs to.  A frame
 * can be larger than one queue entry, so the transfer is chunked.
 */
void
PDQueueSendFrame(PDQueueSender *sender, uint64 end_lsn, uint64 cseq,
				 const char *frame, size_t len)
{
	PDFrameHeader fh;

	fh.end_lsn = end_lsn;
	fh.cseq = cseq;
	fh.length = (uint32) len;

	pd_queue_send_batched(sender, PD_ENT_FRAME, &fh, sizeof(fh), frame, len);
}

/*
 * Send a frame that carries the definition of a relation to the walsender.
 *
 * The frame is preceded by a PDRelationHeader, which tells the walsender
 * which relation the definition belongs to and in which epoch of the session
 * it was produced.  The walsender uses that information to send the
 * definition of a relation to the subscriber once.
 */
void
PDQueueSendRelation(PDQueueSender *sender, uint64 end_lsn, uint64 cseq,
					uint64 epoch, Oid relid,
					const char *frame, size_t len)
{
	PDRelationHeader rh;

	rh.end_lsn = end_lsn;
	rh.cseq = cseq;
	rh.epoch = epoch;
	rh.relid = relid;
	rh.length = (uint32) len;

	pd_queue_send_batched(sender, PD_ENT_RELATION, &rh, sizeof(rh), frame, len);
}

/*
 * Send a stream segment to the walsender.
 *
 * The segment is preceded by a PDSegmentHeader, which tells the walsender
 * how far the decoding has come and how many frames the segment holds.  The
 * frames follow the header, each of them preceded by its length.
 *
 * The whole segment goes into a single message, so that the walsender can put
 * the frames on the wire one after the other: the subscriber accepts only one
 * stream segment at a time.
 */
void
PDQueueSendSegment(PDQueueSender *sender, uint64 end_lsn, uint32 nframes,
				   const char *payload, size_t len)
{
	PDSegmentHeader sh;

	sh.end_lsn = end_lsn;
	sh.nframes = nframes;

	pd_queue_send_batched(sender, PD_ENT_SEGMENT, &sh, sizeof(sh), payload, len);
}

/*
 * Report an event to the walsender.
 *
 * The message uses the chunked framing, because the receiver of the queue
 * uses a single chunk receiver for all of its messages.
 *
 * The event is not batched: it tells the walsender that a transaction has
 * been decoded, and the walsender holds the frames of that transaction back
 * until the event arrives.  Batching it would delay the transaction itself.
 */
void
PDQueueSendEvent(PDQueueSender *sender, uint64 cseq, uint32 event)
{
	PDEventHeader eh;

	eh.cseq = cseq;
	eh.event = event;

	PDQueueSendUnbatched(sender, PD_ENT_EVENT, &eh, sizeof(eh));
}

/*
 * Report the decoding position to the walsender.
 *
 * The position tells the walsender how far the decoding has come, even when
 * the records that were decoded produced no output at all.  The message uses
 * the chunked framing, because the receiver of a queue uses a single chunk
 * receiver for all of its messages.
 *
 * The position is not batched: it is what tells the walsender that the worker
 * is still making progress, and the stall detection depends on it.
 */
void
PDQueueSendPosition(PDQueueSender *sender, uint64 end_lsn)
{
	PDQueueSendUnbatched(sender, PD_ENT_POSITION, &end_lsn,
						  sizeof(end_lsn));
}

/*
 * Serialize the options of the output plugin.
 *
 * Every option of the plugins that ship with PostgreSQL carries a plain
 * string argument, so the serialized form is a list of length prefixed names
 * and arguments.  The options that have no argument are stored with an
 * argument length of zero.
 */
void
PDSerializePluginOptions(List *options, char *buf, size_t buflen, uint32 *len)
{
	ListCell   *lc;
	size_t		pos;
	uint32		count = 0;

	/* The number of options comes first. */
	if (buflen < sizeof(uint32))
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("the buffer for the output plugin options is too small")));

	pos = sizeof(uint32);

	for (lc = list_head(options); lc != NULL; lc = lnext(options, lc))
	{
		DefElem    *defel = (DefElem *) lfirst(lc);
		const char *name = defel->defname;
		const char *arg = NULL;
		uint16		namelen;
		uint16		arglen = 0;

		if (defel->arg != NULL)
		{
			if (!IsA(defel->arg, String))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("the parallel decoding path cannot use the output plugin option \"%s\"",
								name),
						 errdetail("The argument of the option is not a plain string.")));
			arg = strVal(defel->arg);
			arglen = (uint16) strlen(arg);
		}

		namelen = (uint16) strlen(name);

		if (pos + sizeof(uint16) + namelen + sizeof(uint16) + arglen > buflen)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("the output plugin options do not fit into %zu bytes",
							buflen)));

		memcpy(buf + pos, &namelen, sizeof(uint16));
		pos += sizeof(uint16);
		memcpy(buf + pos, name, namelen);
		pos += namelen;
		memcpy(buf + pos, &arglen, sizeof(uint16));
		pos += sizeof(uint16);
		if (arglen > 0)
		{
			memcpy(buf + pos, arg, arglen);
			pos += arglen;
		}

		count++;
	}

	memcpy(buf, &count, sizeof(uint32));

	*len = (uint32) pos;
}

/*
 * Parse the options of the output plugin.
 *
 * The result is a list of DefElem, exactly like the one the walsender got
 * from the replication command.
 */
List *
PDParsePluginOptions(const char *buf, uint32 len)
{
	List	   *options = NIL;
	const char *pos = buf;
	const char *end = buf + len;
	uint32		count;
	uint32		i;

	if (len < sizeof(uint32))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("the serialized output plugin options are too short")));

	memcpy(&count, pos, sizeof(count));
	pos += sizeof(count);

	for (i = 0; i < count; i++)
	{
		DefElem    *defel;
		uint16		namelen;
		uint16		arglen;
		char	   *name;
		char	   *arg = NULL;

		if (pos + sizeof(uint16) > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("the serialized output plugin options are truncated")));

		memcpy(&namelen, pos, sizeof(namelen));
		pos += sizeof(namelen);

		if (pos + namelen + sizeof(uint16) > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("the serialized output plugin options are truncated")));

		name = palloc(namelen + 1);
		memcpy(name, pos, namelen);
		name[namelen] = '\0';
		pos += namelen;

		memcpy(&arglen, pos, sizeof(arglen));
		pos += sizeof(arglen);

		if (pos + arglen > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("the serialized output plugin options are truncated")));

		if (arglen > 0)
		{
			arg = palloc(arglen + 1);
			memcpy(arg, pos, arglen);
			arg[arglen] = '\0';
			pos += arglen;
		}

		defel = makeDefElem(name, arg ? (Node *) makeString(arg) : NULL, -1);
		options = lappend(options, defel);
	}

	return options;
}
