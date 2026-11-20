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
	 *
	 * worker_index is -1 when the process is not a decode worker: that is the
	 * case of the serial decoding path and of the walreader, which decodes
	 * the records for the sake of its snapshot builder.  It says which worker
	 * the process is, so that the decoding path can tell the transactions of
	 * the workers apart.  Whether the process is a decode worker at all is
	 * told by AmDecodeWorkerProcess(), see parallel_decode.h.
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
	 * The number of changes a transaction has to accumulate before the
	 * parallel decoding path sends them as a stream segment of their own.
	 *
	 * The changes of a transaction that reaches this number are sent before
	 * the transaction is complete, so that the segments of the transactions
	 * that are in flight overlap in time: that is what the parallel apply of
	 * the subscriber works on.
	 *
	 * Zero means that the changes of a transaction are not cut: the
	 * transaction is streamed in a single segment when it commits.  The serial
	 * decoding path leaves this at zero.
	 */
	uint64		stream_segment_changes;
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
