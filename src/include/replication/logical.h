/*-------------------------------------------------------------------------
 * logical.h
 *	   PostgreSQL logical decoding coordination
 *
 * Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICAL_H
#define LOGICAL_H

#include "access/xlog.h"
#include "access/xlogreader.h"
#include "replication/output_plugin.h"
#include "replication/slot.h"

struct LogicalDecodingContext;

typedef void (*LogicalOutputPluginWriterWrite) (struct LogicalDecodingContext *lr,
												XLogRecPtr Ptr,
												TransactionId xid,
												bool last_write
);

typedef LogicalOutputPluginWriterWrite LogicalOutputPluginWriterPrepareWrite;

typedef void (*LogicalOutputPluginWriterUpdateProgress) (struct LogicalDecodingContext *lr,
														 XLogRecPtr Ptr,
														 TransactionId xid,
														 bool skipped_xact
);

typedef struct LogicalDecodingContext
{
	/* memory context this is all allocated in */
	MemoryContext context;

	/* The associated replication slot */
	ReplicationSlot *slot;

	/* infrastructure pieces for decoding */
	XLogReaderState *reader;
	struct ReorderBuffer *reorder;
	struct SnapBuild *snapshot_builder;

	/*
	 * Marks the logical decoding context as fast forward decoding one. Such a
	 * context does not have plugin loaded so most of the following properties
	 * are unused.
	 */
	bool		fast_forward;

	OutputPluginCallbacks callbacks;
	OutputPluginOptions options;

	/*
	 * User specified options
	 */
	List	   *output_plugin_options;

	/*
	 * User-Provided callback for writing/streaming out data.
	 */
	LogicalOutputPluginWriterPrepareWrite prepare_write;
	LogicalOutputPluginWriterWrite write;
	LogicalOutputPluginWriterUpdateProgress update_progress;

	/*
	 * Output buffer.
	 */
	StringInfo	out;

	/*
	 * Private data pointer of the output plugin.
	 */
	void	   *output_plugin_private;

	/*
	 * Private data pointer for the data writer.
	 */
	void	   *output_writer_private;

	/*
	 * Does the output plugin support streaming, and is it enabled?
	 */
	bool		streaming;

	/*
	 * Does the output plugin support two-phase decoding, and is it enabled?
	 */
	bool		twophase;

	/*
	 * Is two-phase option given by output plugin?
	 *
	 * This flag indicates that the plugin passed in the two-phase option as
	 * part of the START_STREAMING command. We can't rely solely on the
	 * twophase flag which only tells whether the plugin provided all the
	 * necessary two-phase callbacks.
	 */
	bool		twophase_opt_given;

	/*
	 * State for writing output.
	 */
	bool		accept_writes;
	bool		prepared_write;
	XLogRecPtr	write_location;
	TransactionId write_xid;
	/* Are we processing the end LSN of a transaction? */
	bool		end_xact;

	/* Do we need to process any change in fast_forward mode? */
	bool		processing_required;

	/*
	 * The index of the decode worker that owns the transaction of the record
	 * that is being processed, and the index of this process among the decode
	 * workers of the session.
	 *
	 * The parallel decoding path assigns every transaction to one of the
	 * decode workers.  The workers that do not own the transaction see its
	 * records as well, so that their snapshot builders stay in step with the
	 * one of the owner, but they must not accumulate the changes: the owner
	 * is the one that produces the output of the transaction.
	 *
	 * txn_owner is -1 when the process decodes the transactions of every
	 * owner, which is the case of the serial decoding path.
	 */
	int			txn_owner;
	int			worker_index;

	/*
	 * The hook that is called before the output plugin writes the definition
	 * of a relation.
	 *
	 * The serial path has one decoding context and one cache of the relations
	 * it has sent already, so it sends the definition of a relation once.  The
	 * parallel path has one decoding context per decode worker, so every
	 * worker would send the definition of a relation once, and the subscriber
	 * would receive it as many times as there are workers.
	 *
	 * The hook tells the session that the frame that is written next carries
	 * the definition of a relation.  The session uses that knowledge to send
	 * the definition of a relation to the subscriber once: the walsender is
	 * the one that knows in which order the transactions reach the
	 * subscriber, so it is the one that can tell whether the definition that
	 * a worker has produced is needed at all.
	 *
	 * A NULL hook means that the session does not know anything about the
	 * frames the plugin produces, which is what the serial path does.
	 */
	void (*rel_definition_hook) (struct LogicalDecodingContext *ctx, Oid relid);

	/*
	 * The publisher-side parallel decoding path streams the changes of a
	 * transaction before the transaction is complete, so that they reach the
	 * subscriber early.  These three fields say how eagerly that happens.
	 *
	 * stream_segment_size is the size of a stream segment.  A transaction
	 * that has accumulated that many bytes is streamed in a segment of that
	 * size, so that it reaches the subscriber before it is complete; a
	 * transaction that stays below it is streamed in a single segment when it
	 * commits.  Zero means that the changes are only streamed when the
	 * decoding path runs out of memory, which is the behavior of the serial
	 * decoding path.
	 *
	 * stream_min_size is the number of bytes a transaction has to have
	 * accumulated before it may be streamed at all.  A transaction below the
	 * threshold reaches the subscriber as a whole transaction block.  A
	 * threshold larger than the size of the typical transaction keeps the
	 * subscriber from receiving any stream segment.  Zero means that every
	 * transaction is streamed.
	 *
	 * stream_all_transactions tells the decoding path to stream every
	 * transaction, including the transactions that were not streamed while
	 * they were in progress: those are streamed when they commit.  The
	 * subscriber applies the transactions in parallel, and a transaction that
	 * is not streamed gives its apply worker nothing to do.  It is false in
	 * the serial decoding path.
	 *
	 * The first two are zero in the serial decoding path, so its behavior is
	 * unchanged.
	 */
	Size		stream_segment_size;
	Size		stream_min_size;
	bool		stream_all_transactions;
} LogicalDecodingContext;


extern void CheckLogicalDecodingRequirements(void);

extern LogicalDecodingContext *CreateInitDecodingContext(const char *plugin,
														 List *output_plugin_options,
														 bool need_full_snapshot,
														 XLogRecPtr restart_lsn,
														 XLogReaderRoutine *xl_routine,
														 LogicalOutputPluginWriterPrepareWrite prepare_write,
														 LogicalOutputPluginWriterWrite do_write,
														 LogicalOutputPluginWriterUpdateProgress update_progress);
extern LogicalDecodingContext *CreateDecodingContext(XLogRecPtr start_lsn,
													 List *output_plugin_options,
													 bool fast_forward,
													 XLogReaderRoutine *xl_routine,
													 LogicalOutputPluginWriterPrepareWrite prepare_write,
													 LogicalOutputPluginWriterWrite do_write,
													 LogicalOutputPluginWriterUpdateProgress update_progress);
extern LogicalDecodingContext *CreateParallelDecodingContext(ReplicationSlot *slot,
															 List *output_plugin_options,
															 XLogRecPtr start_lsn,
															 XLogReaderRoutine *xl_routine,
															 LogicalOutputPluginWriterPrepareWrite prepare_write,
															 LogicalOutputPluginWriterWrite do_write,
															 LogicalOutputPluginWriterUpdateProgress update_progress);
extern void DecodingContextFindStartpoint(LogicalDecodingContext *ctx);
extern bool DecodingContextReady(LogicalDecodingContext *ctx);
extern void FreeDecodingContext(LogicalDecodingContext *ctx);

extern void LogicalIncreaseXminForSlot(XLogRecPtr current_lsn,
									   TransactionId xmin);
extern void LogicalIncreaseRestartDecodingForSlot(XLogRecPtr current_lsn,
												  XLogRecPtr restart_lsn);
extern void LogicalConfirmReceivedLocation(XLogRecPtr lsn);

extern bool filter_prepare_cb_wrapper(LogicalDecodingContext *ctx,
									  TransactionId xid, const char *gid);
extern bool filter_by_origin_cb_wrapper(LogicalDecodingContext *ctx, RepOriginId origin_id);
extern void ResetLogicalStreamingState(void);
extern void UpdateDecodingStats(LogicalDecodingContext *ctx);

extern bool LogicalReplicationSlotHasPendingWal(XLogRecPtr end_of_wal);
extern XLogRecPtr LogicalSlotAdvanceAndCheckSnapState(XLogRecPtr moveto,
													  bool *found_consistent_snapshot);

#endif
