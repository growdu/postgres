/*-------------------------------------------------------------------------
 *
 * paralleldecode_walsender.c
 *		Walsender-side integration for parallel logical decoding.
 *
 * Provides the dispatch entry points called from walsender.c's
 * XLogSendLogical path: pool creation at connection setup, record
 * dispatch per WAL record, event publication on snapshot/invalidation,
 * and result collection in the main loop.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/backend/replication/logical/paralleldecode_walsender.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogrecord.h"
#include "miscadmin.h"
#include "replication/paralleldecode.h"
#include "replication/snapbuild.h"
#include "replication/walsender_private.h"
#include "utils/memutils.h"

/* Module-level pool pointer, one per walsender session */
ParallelDecodePool *pd_walsender_pool = NULL;

/*
 * Create the parallel decode pool for this walsender session.
 * Called from StartLogicalReplication when parallel_decode is enabled.
 */
void
ParallelDecodeWalsenderInit(int nworkers)
{
	if (pd_walsender_pool != NULL)
		ParallelDecodePoolDestroy(pd_walsender_pool);

	pd_walsender_pool = ParallelDecodePoolCreate(nworkers);
}

/*
 * Destroy the pool at session end.
 */
void
ParallelDecodeWalsenderFini(void)
{
	if (pd_walsender_pool != NULL)
	{
		ParallelDecodePoolDestroy(pd_walsender_pool);
		pd_walsender_pool = NULL;
	}
}

/*
 * Dispatch one WAL record to the pool.  Called from the decode hook
 * after LogicalDecodingProcessRecord identifies the record's owner.
 */
void
ParallelDecodeWalsenderDispatch(XLogReaderState *record,
								TransactionId top_xid,
								TransactionId xid)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodeDispatchRecord(pd_walsender_pool, record, top_xid, xid);
}

/*
 * Dispatch a commit record.
 */
void
ParallelDecodeWalsenderCommit(TransactionId top_xid,
							  XLogRecPtr commit_lsn, XLogRecPtr end_lsn,
							  TimestampTz commit_time,
							  RepOriginId origin_id, XLogRecPtr origin_lsn)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodeDispatchCommit(pd_walsender_pool, top_xid, commit_lsn,
								 end_lsn, commit_time, origin_id, origin_lsn);
}

/*
 * Dispatch an abort record.
 */
void
ParallelDecodeWalsenderAbort(TransactionId top_xid, XLogRecPtr abort_lsn)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodeDispatchAbort(pd_walsender_pool, top_xid, abort_lsn);
}

/*
 * Publish a new snapshot version to all workers.
 */
void
ParallelDecodeWalsenderSnapshot(struct SnapBuild *builder, XLogRecPtr lsn)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodePublishSnapshot(pd_walsender_pool, builder, lsn);
}

/*
 * Publish invalidation messages to all workers.
 */
void
ParallelDecodeWalsenderInval(uint32 nmsgs,
							 SharedInvalidationMessage *msgs,
							 XLogRecPtr commit_lsn)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodePublishInvalidations(pd_walsender_pool, nmsgs, msgs,
									   commit_lsn);
}

/*
 * Collect results from all workers.  Called periodically in the
 * walsender main loop.
 */
void
ParallelDecodeWalsenderCollect(void)
{
	if (pd_walsender_pool == NULL)
		return;

	ParallelDecodeCollectResults(pd_walsender_pool);
	ParallelDecodeOutputPending(pd_walsender_pool);
	ParallelDecodeAdvanceSlotPositions(pd_walsender_pool);
}
