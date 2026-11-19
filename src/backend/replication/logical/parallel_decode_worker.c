/*-------------------------------------------------------------------------
 *
 * parallel_decode_worker.c
 *		Publisher-side parallel logical decoding: decode worker process.
 *
 * A decode worker decodes the WAL records the walreader hands over into
 * replication protocol bytes and passes the bytes to the walsender over a
 * shared memory queue.
 *
 * The worker has a decoding context of its own: an output plugin instance, a
 * reorder buffer and a snapshot builder.  The walsender owns the replication
 * slot, so the worker works on a copy of the fields of the slot that the
 * decoding path looks at.  Apart from that the decoding path is the same as
 * the one of the serial path, which is what makes the frames the two paths
 * produce identical.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "replication/decode.h"
#include "replication/logical.h"
#include "replication/parallel_decode.h"
#include "replication/reorderbuffer.h"
#include "replication/snapbuild.h"
#include "replication/slot.h"
#include "storage/condition_variable.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "storage/spin.h"
#include "tcop/tcopprot.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

/*
 * Set when SIGTERM arrives, so that the worker can shut down gracefully
 * instead of being reported as a crashed background worker.
 */
static volatile sig_atomic_t got_sigterm = false;

/*
 * The control block of the session, the queues of the worker and the
 * decoding context.
 */
static ParallelDecodeControl *pd_ctl = NULL;
static shm_mq_handle *pd_wal_mqh = NULL;
static shm_mq_handle *pd_out_mqh = NULL;
static PDChunkReceiver pd_wal_rcv;
static LogicalDecodingContext *pd_ctx = NULL;

/*
 * The decoding position after the record that was processed last.  The frames
 * the worker produces carry it, so that the walsender knows how far the
 * decoding has come.
 */
static XLogRecPtr pd_end_lsn = InvalidXLogRecPtr;

/*
 * The index of this worker among the decode workers of the session.
 */
static int	pd_worker_index = -1;

/*
 * The header of the record that is being processed.
 *
 * It tells the worker which worker owns the transaction of the record, and
 * which commit sequence number the record was given when it ends a
 * transaction.
 */
static PDWalHeader pd_wal_header;

/*
 * The commit sequence number of the transaction that is being decoded.
 *
 * The frames the worker produces carry it, so that the walsender can hold
 * them back until the transaction has its turn.  It is zero while a record
 * that does not end a transaction is processed.
 */
static uint64 pd_current_cseq = 0;

/* The position the walsender has been told about. */
static XLogRecPtr pd_reported_lsn = InvalidXLogRecPtr;

/*
 * The invalidation epoch of the session.
 *
 * The walreader numbers the epochs of the session, and it tells the
 * workers about them together with the invalidation messages.  The
 * epoch tells the walsender whether the definition of a relation that has
 * been sent already still describes the relation: a definition that was
 * sent before the last change of the catalog does not.
 */
static uint64 pd_epoch = 0;

/*
 * The relation whose definition the plugin is about to write.
 *
 * The output plugin tells the worker that the frame it writes next carries
 * the definition of a relation, so that the worker can hand the frame over
 * to the walsender as a definition.  The walsender decides whether the
 * definition is needed at all: it knows in which order the transactions
 * reach the subscriber.
 */
static Oid pd_pending_relid = InvalidOid;

/*
 * The epoch the definition belongs to.
 *
 * It is the epoch the worker knows when the definition is produced.  The
 * walsender uses it to tell the definitions of a relation that were produced
 * before the last change of the catalog apart from the ones that describe
 * the relation as it is now.
 */
static uint64 pd_pending_epoch = 0;

/*
 * The version of the snapshot the worker holds.
 *
 * The walreader numbers the snapshots it publishes, and the workers refer to
 * a snapshot by its version.  The version tells the worker whether the
 * messages it receives are about the snapshot it holds.
 */
static uint64 pd_snapshot_version = 0;

/*
 * The number of records between two reports of the decoding position.
 *
 * The reports tell the walsender that the worker is still working: a worker
 * that stops reporting while it still has records to decode is stuck, and the
 * session that waits for it has to end.  The interval is a compromise between
 * the overhead of the reports and the time it takes to notice a stuck worker.
 */
#define PD_POSITION_REPORT_INTERVAL	256

/*
 * The frames of the stream segment that is being built.
 *
 * The subscriber accepts only one stream segment at a time, so the frames of
 * a segment have to reach it without the frames of another transaction in
 * between.  The worker collects them here and hands the whole segment over
 * to the walsender as a single message.
 */
static StringInfo pd_segment = NULL;
static int	pd_segment_frames = 0;
static bool pd_segment_active = false;

/*
 * The streaming callbacks of the output plugin.
 *
 * The worker wraps them, so that it knows which frames belong to a stream
 * segment.
 */
static LogicalDecodeStreamStartCB pd_plugin_stream_start = NULL;
static LogicalDecodeStreamStopCB pd_plugin_stream_stop = NULL;

/* The wait event the worker reports while it waits for the walreader. */
static uint32 pd_wait_event = PG_WAIT_EXTENSION;

/*
 * The result of a receive from the WAL channel.
 */
typedef enum PDRecvResult
{
	PD_RECV_RECORD,				/* a record was received */
	PD_RECV_EMPTY,				/* nothing is available at the moment */
	PD_RECV_STOPPED			/* the session is winding down */
} PDRecvResult;

/*
 * SIGTERM handler: ask the worker to exit at the next opportunity.
 */
static void
parallel_decode_worker_sigterm(SIGNAL_ARGS)
{
	got_sigterm = true;
	SetLatch(MyLatch);
}

/*
 * Should the worker stop decoding?
 *
 * The walsender raises the shutdown flag when it goes away in an orderly
 * fashion.  If it was killed by a signal, the flag is never raised, so check
 * that the walsender is still there.
 */
static bool
pd_worker_stopped(void)
{
	if (got_sigterm)
		return true;
	if (pd_ctl == NULL)
		return true;
	if (pd_ctl->shutdown)
		return true;

	return !ParallelDecodeProcessAlive(pd_ctl->walsender_pid);
}

/*
 * XLogReaderRoutine->page_read callback.
 *
 * The worker never reads the WAL: the walreader is the only reader of it, and
 * it hands the records over as serialized copies.  The decoding path only
 * needs the reader to hold the record it is working on.
 */
static int
pd_worker_read_page(XLogReaderState *state, XLogRecPtr targetPagePtr,
					int reqLen, XLogRecPtr targetRecPtr, char *readBuf)
{
	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("the parallel decode worker must not read the WAL")));
	return -1;
}

/*
 * The 'write' callback of the decoding context.
 *
 * The frame is complete at this point, so it is handed over to the walsender,
 * which writes it to the network.
 *
 * The frames of a stream segment are an exception: they are collected, so
 * that the segment reaches the subscriber as a whole.  The subscriber accepts
 * only one stream segment at a time.
 */
static void
pd_worker_write(LogicalDecodingContext *ctx, XLogRecPtr lsn, TransactionId xid,
				bool last_write)
{
	uint32		len;
	Oid			relid;
	uint64		epoch;

	/*
	 * The plugin may have told us that this frame carries the definition of a
	 * relation.  The information is used once: the frame that follows the
	 * call of the hook is the one that carries the definition.
	 */
	relid = pd_pending_relid;
	epoch = pd_pending_epoch;
	pd_pending_relid = InvalidOid;
	pd_pending_epoch = 0;

	if (pd_segment_active)
	{
		/*
		 * The frames of the segment are stored with their length in front of
		 * them, so that the walsender can find the boundaries of the frames.
		 */
		len = (uint32) ctx->out->len;
		appendBinaryStringInfo(pd_segment, (char *) &len, sizeof(len));
		appendBinaryStringInfo(pd_segment, ctx->out->data, ctx->out->len);
		pd_segment_frames++;

		return;
	}

	if (relid != InvalidOid)
	{
		/*
		 * The frame carries the definition of a relation.  It is sent as a
		 * message of its own type, so that the walsender can tell it apart
		 * from the other frames and decide whether the subscriber needs it.
		 */
		PDQueueSendRelation(pd_out_mqh, (uint64) pd_end_lsn, pd_current_cseq,
							epoch, relid, ctx->out->data, ctx->out->len);

		return;
	}

	PDQueueSendFrame(pd_out_mqh, (uint64) pd_end_lsn, pd_current_cseq,
					 ctx->out->data, ctx->out->len);
}

/*
 * Hand the stream segment over to the walsender.
 *
 * The whole segment goes into a single message, so that the walsender can put
 * its frames on the wire one after the other.
 */
static void
pd_worker_send_segment(void)
{
	if (pd_segment_frames == 0)
	{
		resetStringInfo(pd_segment);
		pd_segment_frames = 0;
		return;
	}

	PDQueueSendSegment(pd_out_mqh, (uint64) pd_end_lsn, pd_segment_frames,
					   pd_segment->data, pd_segment->len);

	resetStringInfo(pd_segment);
	pd_segment_frames = 0;
}

/*
 * The 'stream_start' callback of the output plugin.
 *
 * The frames the plugin writes from here on belong to the stream segment, so
 * they are collected instead of being sent one by one.
 */
static void
pd_worker_stream_start(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	resetStringInfo(pd_segment);
	pd_segment_frames = 0;
	pd_segment_active = true;

	pd_plugin_stream_start(ctx, txn);
}

/*
 * The 'stream_stop' callback of the output plugin.
 *
 * The segment is complete now, so it is handed over to the walsender.
 */
static void
pd_worker_stream_stop(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
	pd_plugin_stream_stop(ctx, txn);

	pd_segment_active = false;
	pd_worker_send_segment();
}

/*
 * The 'update_progress' callback of the decoding context.
 *
 * The walsender tracks the lag of the session, so there is nothing for the
 * worker to do here.
 */
static void
pd_worker_update_progress(LogicalDecodingContext *ctx, XLogRecPtr lsn,
						   TransactionId xid, bool skipped_xact)
{
}

/*
 * Tell the walsender how far the decoding has come.
 *
 * The walsender uses the position for the keepalive frames, for the
 * caught-up decision and for the lag the subscriber sees.  The message is
 * sent when the worker runs out of work, so that the records that produced no
 * output still advance the position.
 *
 * A report that does not advance the position is sent when it is forced: the
 * walsender tells from the time of the report whether the worker is still
 * working, and a worker that owns no transaction never advances its position.
 */
static void
pd_worker_send_position(XLogRecPtr position, bool force)
{
	if (!force && position <= pd_reported_lsn)
		return;

	/*
	 * The position never goes backwards: the marker the walreader sends may
	 * name a position that the worker has already reported past.
	 */
	if (position < pd_reported_lsn)
		position = pd_reported_lsn;

	PDQueueSendPosition(pd_out_mqh, (uint64) position);
	pd_reported_lsn = position;
}

/*
 * Build the snapshot the walreader handed down.
 *
 * The walreader is the only process that reads the WAL, so it is the only one
 * that builds the snapshots.  The worker materializes the snapshot here,
 * because the decoding path needs a snapshot it can install as the historic
 * snapshot.
 *
 * The snapshot and the array of the committed transactions are allocated in one
 * block: they are freed together.
 */
static Snapshot
pd_worker_build_snapshot(const PDSnapshotContent *content,
						 const TransactionId *xip)
{
	SnapshotData *snap;
	MemoryContext oldcontext;

	oldcontext = MemoryContextSwitchTo(pd_ctx->context);

	snap = palloc0(sizeof(SnapshotData) +
				   sizeof(TransactionId) * content->xcnt);

	snap->snapshot_type = SNAPSHOT_HISTORIC_MVCC;
	snap->xmin = content->xmin;
	snap->xmax = content->xmax;
	snap->curcid = content->curcid;
	snap->xcnt = content->xcnt;
	snap->xip = (TransactionId *) (snap + 1);
	snap->subxcnt = 0;
	snap->subxip = NULL;
	snap->suboverflowed = false;
	snap->takenDuringRecovery = false;
	snap->copied = false;
	snap->active_count = 0;
	snap->regd_count = 0;
	snap->snapXactCompletionCount = 0;

	if (content->xcnt > 0)
	{
		memcpy(snap->xip, xip, sizeof(TransactionId) * content->xcnt);

		/*
		 * The array has to be sorted: the snapshot is searched with bsearch().
		 */
		qsort(snap->xip, snap->xcnt, sizeof(TransactionId), xidComparator);
	}

	MemoryContextSwitchTo(oldcontext);

	return (Snapshot) snap;
}

/*
 * Handle a snapshot message.
 *
 * The messages tell the worker about the state of the snapshot builder of the
 * walreader, about the content of the snapshot the transactions are decoded
 * with, and about the moments at which the transactions that are in flight
 * have to switch over to a new snapshot.
 */
static void
pd_worker_handle_snapshot(const void *payload, size_t len)
{
	const PDSnapshotHeader *header = payload;
	const char *data;
	size_t		datalen;

	if (len < sizeof(PDSnapshotHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode snapshot message is too short (%zu bytes)",
						(size_t) len)));

	data = (const char *) payload + sizeof(PDSnapshotHeader);
	datalen = len - sizeof(PDSnapshotHeader);

	if (header->flags & PD_SNAPSHOT_CONTENT)
	{
		const PDSnapshotContent *content = (const PDSnapshotContent *) data;
		Snapshot	snap;

		if (datalen < sizeof(PDSnapshotContent))
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode snapshot message is too short (%zu bytes)",
							(size_t) len)));

		if (datalen < sizeof(PDSnapshotContent) +
			sizeof(TransactionId) * content->xcnt)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode snapshot message is truncated")));

		/*
		 * The epoch of the session grows every time the catalog changes,
		 * and the messages carry it, so that the worker knows which
		 * definitions it has sent still describe the relations.
		 */
		if (header->epoch > pd_epoch)
			pd_epoch = header->epoch;

		/*
		 * The versions of the snapshots grow, so a message that carries a
		 * version the worker has already seen is out of order.
		 */
		if (header->version <= pd_snapshot_version)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode snapshot message is out of order")));

		snap = pd_worker_build_snapshot(content,
										(const TransactionId *) (data + sizeof(PDSnapshotContent)));

		pd_snapshot_version = header->version;

		SnapBuildSetSnapshot(pd_ctx->snapshot_builder, snap);
	}
	else if (header->flags & PD_SNAPSHOT_BASE)
	{
		Snapshot	snap;

		snap = SnapBuildGetSnapshot(pd_ctx->snapshot_builder);
		if (snap == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode base snapshot message arrived before the "
							"content of the snapshot")));

		if (header->version != pd_snapshot_version)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode base snapshot message refers to an unknown "
							"version of the snapshot")));

		/*
		 * The transaction holds a reference of its own to the snapshot.
		 */
		SnapBuildSnapIncRefcount(snap);
		ReorderBufferSetBaseSnapshot(pd_ctx->reorder, header->xid,
									  header->lsn, snap);
	}
	else if (header->flags & PD_SNAPSHOT_SWITCH)
	{
		Snapshot	snap;
		SharedInvalidationMessage *msgs = NULL;

		if (datalen < sizeof(SharedInvalidationMessage) * header->ninvalidations)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode snapshot message is truncated")));

		if (header->ninvalidations > 0)
			msgs = (SharedInvalidationMessage *) data;

		snap = SnapBuildGetSnapshot(pd_ctx->snapshot_builder);
		if (snap == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("parallel decode snapshot switch message arrived before the "
							"content of the snapshot")));

		/*
		 * The transactions that are in flight switch over to the new snapshot,
		 * and the invalidation messages of the transaction that committed are
		 * added to them.
		 */
		ReorderBufferDistributeSnapshot(pd_ctx->reorder, header->lsn, snap,
										 header->xid, header->ninvalidations,
										 msgs);

		/*
		 * The worker that owns the transaction that committed executes the
		 * invalidation messages itself, when the transaction ends.  Every other
		 * worker has to execute them right away: the catalog caches of a
		 * worker are private to its process, and nothing else would ever
		 * invalidate them.
		 */
		if (header->ninvalidations > 0 &&
			!ReorderBufferHasTransaction(pd_ctx->reorder, header->xid))
			ReorderBufferImmediateInvalidation(pd_ctx->reorder,
											   header->ninvalidations, msgs);
	}
	else if (header->flags & PD_SNAPSHOT_STATE)
	{
		SnapBuildSetState(pd_ctx->snapshot_builder,
						  (SnapBuildState) header->state,
						  header->start_decoding_at,
						  header->next_phase_at);
	}
	else
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode snapshot message has unknown flags (%u)",
						header->flags)));
}

/*
 * Handle the invalidation messages of a record that has no transaction.
 *
 * The invalidation messages affect the catalog state of the whole cluster, so
 * every worker has to execute them.
 */
static void
pd_worker_handle_inval(const void *payload, size_t len)
{
	const PDInvalHeader *header = payload;
	const char *data;
	size_t		datalen;

	if (len < sizeof(PDInvalHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode invalidation message is too short (%zu bytes)",
						(size_t) len)));

	data = (const char *) payload + sizeof(PDInvalHeader);
	datalen = len - sizeof(PDInvalHeader);

	if (datalen < sizeof(SharedInvalidationMessage) * header->ninvalidations)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode invalidation message is truncated")));

	/*
	 * The invalidation messages change the catalog caches of the worker,
	 * so the definitions it has sent do not describe the relations
	 * anymore.  The epoch of the session tells the worker that.
	 */
	if (header->epoch > pd_epoch)
		pd_epoch = header->epoch;

	if (header->ninvalidations > 0)
		ReorderBufferImmediateInvalidation(pd_ctx->reorder,
										   header->ninvalidations,
										   (SharedInvalidationMessage *) data);
}

/*
 * Handle the marker the walreader sends.
 *
 * The records of a transaction are only handed to the worker that owns it, so
 * a worker that owns no transaction does not see the records the walreader
 * reads.  The marker tells it how far the walreader has come, so that it can
 * report that it has decoded everything that was meant for it.
 *
 * The marker does not advance the decoding position of the worker itself: it
 * says that the walreader has come further.  The worker reports the position
 * of the marker, because every record that was meant for it has been decoded
 * by the time the marker reaches it.  That way the position of the session is
 * the position the walreader has read up to, exactly as it is in the serial
 * path, even when the worker owns no transaction at all.
 */
static void
pd_worker_handle_marker(const void *payload, size_t len)
{
	uint64		position;

	if (len < sizeof(position))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("parallel decode marker message is too short (%zu bytes)",
						(size_t) len)));

	memcpy(&position, payload, sizeof(position));

	pd_worker_send_position((XLogRecPtr) position, true);
}

/*
 * Report an error to the walsender.
 *
 * The walsender has to end the session anyway: the worker cannot continue
 * with the transaction it was working on.  The report tells it what went
 * wrong, so that the session ends with a message that says what happened
 * instead of the generic "the worker died unexpectedly".
 *
 * The text of the error is put into the control block, because the worker is
 * gone by the time the walsender reads the report.
 */
static void
pd_worker_report_error(const char *message)
{
	PDErrorHeader eh;

	if (pd_ctl != NULL && pd_worker_index >= 0 &&
		pd_worker_index < pd_ctl->num_workers)
	{
		/*
		 * The message is written before the status, so that a reader that sees
		 * the status also sees the message.  The barrier makes sure that the
		 * write is not reordered past the status.
		 */
		pd_ctl->workers[pd_worker_index].error_lsn = (uint64) pd_end_lsn;
		strlcpy(pd_ctl->workers[pd_worker_index].errmsg, message,
				PD_MAX_ERROR_MESSAGE);
		pg_memory_barrier();
		pd_ctl->workers[pd_worker_index].status = PD_WORKER_STATUS_FAILED;
	}

	eh.worker = pd_worker_index;
	eh.lsn = (uint64) pd_end_lsn;

	/*
	 * The message is sent with the chunked framing, because the walsender
	 * reads every message of this queue with a single chunk receiver.
	 */
	PDQueueSendChunked(pd_out_mqh, PD_ENT_ERROR, &eh, sizeof(eh),
						PARALLEL_DECODE_CHUNK_SIZE, false, true);
}

/*
 * Get the next WAL record the walreader has read.
 *
 * The walreader sends more than records: it also tells the worker about the
 * snapshots the transactions are decoded with, about the invalidation messages
 * that affect the whole cluster, and how far it has read.  Those messages are
 * handled here, and the loop goes on to the next message.
 *
 * On PD_RECV_RECORD the record belongs to the caller, which frees it with
 * PDRecordFree().
 */
static PDRecvResult
pd_worker_recv_record(DecodedXLogRecord **record, bool nowait)
{
	for (;;)
	{
		uint16		type;
		const void *payload;
		size_t		len;
		shm_mq_result res;
		int			rc;

		CHECK_FOR_INTERRUPTS();

		if (pd_worker_stopped())
			return PD_RECV_STOPPED;

		/*
		 * Clear the latch before looking at the queue.  A record that arrives
		 * after this point sets the latch again, so the wait below returns
		 * immediately; a record that arrived before is found by the receive.
		 */
		ResetLatch(MyLatch);

		res = PDChunkReceiverRecv(&pd_wal_rcv, pd_wal_mqh, &type, &payload,
								   &len, nowait);
		if (res == SHM_MQ_SUCCESS)
		{
			if (type == PD_ENT_WAL)
			{
				if (len < sizeof(PDWalHeader))
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("parallel decode WAL message is too short (%zu bytes)",
									(size_t) len)));

				/*
				 * The header tells the worker which worker owns the transaction
				 * of the record, and which commit sequence number the record
				 * was given when it ends a transaction.
				 */
				memcpy(&pd_wal_header, payload, sizeof(PDWalHeader));

				*record = PDRecordDeserialize((const char *) payload + sizeof(PDWalHeader),
											  len - sizeof(PDWalHeader));
				pfree((void *) payload);

				return PD_RECV_RECORD;
			}

			switch (type)
			{
				case PD_ENT_SNAPSHOT:
					pd_worker_handle_snapshot(payload, len);
					break;

				case PD_ENT_GLOBAL_INVAL:
					pd_worker_handle_inval(payload, len);
					break;

				case PD_ENT_MARKER:
					pd_worker_handle_marker(payload, len);
					break;

				default:
					ereport(ERROR,
							(errcode(ERRCODE_DATA_CORRUPTED),
							 errmsg("unexpected message of type %u on the parallel decode "
									"WAL channel", type)));
			}

			/*
			 * The message was not a record, so the loop goes on to the next
			 * message of the queue.
			 */
			pfree((void *) payload);
			continue;
		}
		if (res == SHM_MQ_DETACHED)
			return PD_RECV_STOPPED;

		/* The caller asked not to wait, so give up after the first try. */
		if (nowait)
			return PD_RECV_EMPTY;

		/*
		 * There is nothing to do but wait for the walreader.  The walreader is
		 * the only reader of the WAL, so the worker simply has to wait for the
		 * records to arrive.
		 */
		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   100L,
					   pd_wait_event);
		if (rc & WL_LATCH_SET)
			ResetLatch(MyLatch);
	}
}

/*
 * Note that the plugin is about to write the definition of a relation.
 *
 * The frame that follows the call is the one that carries the definition.
 * The worker remembers the relation until the frame has been sent, so that
 * it can hand the frame over to the walsender as a definition.
 *
 * The walsender decides whether the subscriber needs the definition: it
 * knows in which order the transactions reach the subscriber, and the
 * definition of a relation has to reach the subscriber before the first
 * change of the relation.
 */
static void
pd_worker_rel_definition_hook(LogicalDecodingContext *ctx, Oid relid)
{
	pd_pending_relid = relid;
	pd_pending_epoch = pd_epoch;
}

/*
 * Decode the records the walreader hands over.
 */
static void
pd_worker_decode_loop(void)
{
	uint32		since_report = 0;

	/*
	 * The fault injection is a debugging facility: it makes the worker fail, so
	 * that the way the session reports and recovers from a failure can be
	 * verified.
	 */
	if (parallel_decode_fault_inject == PD_FAULT_WORKER)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("parallel decode worker %d failed on purpose",
						pd_worker_index)));

	for (;;)
	{
		DecodedXLogRecord *record = NULL;
		PDRecvResult res;

		CHECK_FOR_INTERRUPTS();

		if (pd_worker_stopped())
			break;

		/*
		 * Try to get a record without waiting first.  If there is none, tell
		 * the walsender how far the decoding has come: the records that
		 * produced no output would otherwise never advance the position.
		 */
		res = pd_worker_recv_record(&record, true);
		if (res == PD_RECV_EMPTY)
		{
			pd_worker_send_position(pd_end_lsn, false);
			res = pd_worker_recv_record(&record, false);
		}
		if (res != PD_RECV_RECORD)
			break;

		/*
		 * Hand the record over to the decoding path as if this process had
		 * read it from the WAL itself.
		 *
		 * The decoding path has to know which worker owns the transaction of
		 * the record: the workers that do not own it decode the record for the
		 * sake of their snapshot builder, but they do not produce its output.
		 */
		pd_ctx->txn_owner = pd_wal_header.owner;
		pd_current_cseq = pd_wal_header.cseq;

		pd_ctx->reader->record = record;
		pd_ctx->reader->ReadRecPtr = record->lsn;
		pd_ctx->reader->EndRecPtr = record->next_lsn;
		pd_end_lsn = record->next_lsn;

		LogicalDecodingProcessRecord(pd_ctx, pd_ctx->reader);

		/*
		 * Tell the walsender now and then that the worker is still working.
		 * The report tells it that the worker is alive and how far it has come,
		 * and it is what the stall detection is based on.
		 */
		if (++since_report >= PD_POSITION_REPORT_INTERVAL)
		{
			since_report = 0;
			pd_worker_send_position(pd_end_lsn, false);
		}

		/*
		 * The transaction the record ended is decoded now, so the walsender
		 * can let its output pass.  The report has to be sent even when the
		 * transaction produced no output at all, because the walsender waits
		 * for it before it lets the transactions that follow pass.
		 */
		if (pd_current_cseq != 0 && pd_wal_header.owner == pd_worker_index)
			PDQueueSendEvent(pd_out_mqh, pd_current_cseq, PD_EVENT_END_ACK);

		/*
		 * The decoding path copies the data it keeps, so the record can go
		 * away right away.
		 */
		PDRecordFree(record);
		pd_ctx->reader->record = NULL;
		pd_current_cseq = 0;
	}
}

/*
 * Clear the slot of this process before the backend exits.
 *
 * Every backend releases the slot it owns when it exits, but the worker does
 * not own a slot: it works on a copy of the fields of the slot of the
 * walsender.  The callback has to be a before_shmem_exit one, because that is
 * where the release of the slot happens.  The callbacks run in the reverse
 * order of their registration, so this one runs before the release.
 */
static void
pd_worker_clear_slot(int code, Datum arg)
{
	MyReplicationSlot = NULL;
}

/*
 * Build the copy of the replication slot the decoding path works on.
 *
 * The walsender owns the slot, so the worker cannot hold it.  The decoding
 * path only reads a few fields of it, so a copy that carries those fields is
 * enough.
 */
static ReplicationSlot *
pd_worker_build_slot_copy(ParallelDecodeControl *ctl)
{
	ReplicationSlot *slot;

	slot = palloc0(sizeof(ReplicationSlot));

	/*
	 * The copy is not a real slot, but every backend releases the slot it
	 * owns when it exits, so the fields that the release path touches have to
	 * be initialized.  The condition variable in particular: broadcasting on
	 * an uninitialized one never returns.
	 */
	SpinLockInit(&slot->mutex);
	ConditionVariableInit(&slot->active_cv);
	slot->active_pid = MyProcPid;

	namestrcpy(&slot->data.name, ctl->slot_name);
	namestrcpy(&slot->data.plugin, ctl->plugin_name);
	slot->data.database = ctl->database;
	slot->data.two_phase = ctl->two_phase;
	slot->data.two_phase_at = ctl->two_phase_at;
	slot->data.restart_lsn = ctl->start_lsn;
	slot->data.confirmed_flush = ctl->start_lsn;

	return slot;
}

/*
 * Main entry point of a decode worker.
 *
 * The worker attaches to the shared segment created by the walsender, claims
 * its slot in the control block, builds its decoding context and then decodes
 * the records the walreader hands over.
 */
void
ParallelDecodeWorkerMain(Datum main_arg)
{
	ParallelDecodeWorkerArg arg;
	ParallelDecodeControl *ctl;
	dsm_segment *seg = NULL;
	shm_toc    *toc = NULL;
	shm_mq	   *wal_mq;
	shm_mq	   *out_mq;
	ReplicationSlot *slot;
	List	   *options;
	int			slot_index = -1;

	/* Setup signal handling. */
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	pqsignal(SIGINT, SignalHandlerForShutdownRequest);
	/* Exit cleanly on SIGTERM so that the session shutdown is not
	 * reported as a background worker crash. */
	pqsignal(SIGTERM, parallel_decode_worker_sigterm);
	BackgroundWorkerUnblockSignals();

	memcpy(&arg, MyBgworkerEntry->bgw_extra, sizeof(ParallelDecodeWorkerArg));

	ctl = ParallelDecodeWorkerAttach(&arg, &slot_index, &seg, &toc);
	if (ctl == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not attach to parallel decoding shared segment")));

	if (slot_index < 0 || slot_index >= ctl->num_workers)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("invalid parallel decode worker index %d", slot_index)));

	/*
	 * The output plugin needs access to the system catalogs, so the worker
	 * must have a database connection.
	 */
	BackgroundWorkerInitializeConnectionByOid(arg.db_oid, InvalidOid, 0);

	/* Claim the slot. */
	ctl->workers[slot_index].pid = MyProcPid;
	ctl->workers[slot_index].status = PD_WORKER_STATUS_RUNNING;
	ctl->workers[slot_index].slot_index = slot_index;

	pd_ctl = ctl;
	pd_wait_event = WaitEventExtensionNew("paralleldecodedecodeworker");

	/*
	 * Attach to the queue the WAL records arrive on.  The walreader is the
	 * sender, the worker is the receiver.
	 */
	wal_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_WAL_MQ_BASE + slot_index);
	if (wal_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding WAL channel")));
	shm_mq_set_receiver(wal_mq, MyProc);
	pd_wal_mqh = shm_mq_attach(wal_mq, seg, NULL);
	PDChunkReceiverInit(&pd_wal_rcv);

	/*
	 * Attach to the queue the frames are handed over on.  The worker is the
	 * sender, the walsender is the receiver.
	 */
	out_mq = ParallelDecodeLookupMq(toc, PARALLEL_DECODE_KEY_OUT_MQ_BASE + slot_index);
	if (out_mq == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("could not find the parallel decoding output channel")));
	shm_mq_set_sender(out_mq, MyProc);
	pd_out_mqh = shm_mq_attach(out_mq, seg, NULL);

	/*
	 * Build the decoding context.  The output plugin options and the name of
	 * the plugin are known by the walsender, which put them into the control
	 * block before it started the worker.
	 */
	options = PDParsePluginOptions(ctl->plugin_options, ctl->plugin_options_len);
	slot = pd_worker_build_slot_copy(ctl);

	pd_ctx = CreateParallelDecodingContext(slot, options, ctl->start_lsn,
											XL_ROUTINE(.page_read = pd_worker_read_page),
											PDPrepareWrite, pd_worker_write,
											pd_worker_update_progress);

	/*
	 * The worker does not drive the snapshot builder.  The walreader is the
	 * only process that reads the WAL, so it is the only one that knows which
	 * snapshot a transaction is decoded with and when the transactions that are
	 * in flight have to switch over to a new one.  The builder of the worker is
	 * a stand-in: it is told about the state of the builder of the walreader
	 * and about the snapshots the transactions are decoded with.
	 */
	SnapBuildSetRole(pd_ctx->snapshot_builder, SNAPBUILD_ROLE_STANDIN);

	/*
	 * The decoding context published the copy of the slot as the slot of this
	 * process.  Make sure that the exit path does not try to release it.
	 */
	before_shmem_exit(pd_worker_clear_slot, (Datum) 0);
	pd_end_lsn = ctl->start_lsn;
	pd_reported_lsn = ctl->start_lsn;

	/*
	 * Tell the decoding path which worker this is, so that it can tell the
	 * transactions of the other workers apart from its own.
	 */
	pd_worker_index = slot_index;
	pd_ctx->worker_index = slot_index;

	/*
	 * The amount of memory the reorder buffer of this worker may use.  The
	 * walsender decided on it, so that the workers do not use more memory
	 * than the session is allowed to use.
	 */
	if (ctl->work_mem_kb > 0)
		SetConfigOption("logical_decoding_work_mem",
						psprintf("%d", ctl->work_mem_kb),
						PGC_USERSET, PGC_S_SESSION);

	/*
	 * How eagerly the changes of a transaction are streamed.  A session that
	 * does not stream leaves the sizes at zero, which keeps the behavior of
	 * the serial path: the changes are streamed only when the decoding path
	 * runs out of memory.
	 */
	pd_ctx->stream_segment_size = (Size) ctl->stream_segment_size;
	pd_ctx->stream_min_size = (Size) ctl->stream_min_size;

	/*
	 * The worker tells the walsender which frames carry the definition of a
	 * relation, so that the definition of a relation is sent to the
	 * subscriber once and not once for every worker.
	 */
	pd_ctx->rel_definition_hook = pd_worker_rel_definition_hook;

	/*
	 * The frames of a stream segment have to reach the subscriber without the
	 * frames of another transaction in between, so the worker collects them
	 * and hands the whole segment over at once.
	 */
	pd_segment = makeStringInfo();
	if (pd_ctx->callbacks.stream_start_cb != NULL &&
		pd_ctx->callbacks.stream_stop_cb != NULL)
	{
		pd_plugin_stream_start = pd_ctx->callbacks.stream_start_cb;
		pd_plugin_stream_stop = pd_ctx->callbacks.stream_stop_cb;
		pd_ctx->callbacks.stream_start_cb = pd_worker_stream_start;
		pd_ctx->callbacks.stream_stop_cb = pd_worker_stream_stop;
	}

	ereport(LOG,
			(errmsg("parallel decode worker %d started", slot_index)));

	/*
	 * Decode the records the walreader hands over.
	 *
	 * An error that happens while decoding ends the session: the worker cannot
	 * continue with the transaction it was working on, and the state of that
	 * transaction cannot be handed over to another worker without breaking the
	 * affinity of the transactions to their worker.  The error is reported to
	 * the walsender first, so that the session ends with a message that says
	 * what went wrong.
	 */
	PG_TRY();
	{
		pd_worker_decode_loop();
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		/*
		 * Copy the error data before anything else: the error context is gone
		 * as soon as the error is rethrown.
		 */
		edata = CopyErrorData();

		/*
		 * The report is best effort: if it cannot be sent, the walsender
		 * notices that the worker is gone anyway.
		 */
		PG_TRY(report);
		{
			pd_worker_report_error(edata->message);
		}
		PG_CATCH(report);
		{
			/* The error could not be reported. */
		}
		PG_END_TRY(report);

		FreeErrorData(edata);

		/*
		 * Rethrow the error, so that it is logged and the worker exits with a
		 * code that does not make the postmaster restart the cluster.
		 */
		PG_RE_THROW();
	}
	PG_END_TRY();

	ctl->workers[slot_index].status = PD_WORKER_STATUS_EXITED;

	ereport(LOG,
			(errmsg("parallel decode worker %d stopped", slot_index)));

	/*
	 * The decoding context published the copy of the slot as the slot of this
	 * process.  Clear it before exiting, so that the exit path does not try to
	 * release a slot that does not exist.
	 */
	MyReplicationSlot = NULL;

	proc_exit(0);
}
