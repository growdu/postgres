/*-------------------------------------------------------------------------
 *
 * snapbuild.h
 *	  Exports from replication/logical/snapbuild.c.
 *
 * Copyright (c) 2012-2024, PostgreSQL Global Development Group
 *
 * src/include/replication/snapbuild.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef SNAPBUILD_H
#define SNAPBUILD_H

#include "access/xlogdefs.h"
#include "utils/snapmgr.h"

typedef enum
{
	/*
	 * Initial state, we can't do much yet.
	 */
	SNAPBUILD_START = -1,

	/*
	 * Collecting committed transactions, to build the initial catalog
	 * snapshot.
	 */
	SNAPBUILD_BUILDING_SNAPSHOT = 0,

	/*
	 * We have collected enough information to decode tuples in transactions
	 * that started after this.
	 *
	 * Once we reached this we start to collect changes. We cannot apply them
	 * yet, because they might be based on transactions that were still
	 * running when FULL_SNAPSHOT was reached.
	 */
	SNAPBUILD_FULL_SNAPSHOT = 1,

	/*
	 * Found a point after SNAPBUILD_FULL_SNAPSHOT where all transactions that
	 * were running at that point finished. Till we reach that we hold off
	 * calling any commit callbacks.
	 */
	SNAPBUILD_CONSISTENT = 2,
} SnapBuildState;

/* forward declare so we don't have to expose the struct to the public */
struct SnapBuild;
typedef struct SnapBuild SnapBuild;

/* forward declare so we don't have to include reorderbuffer.h */
struct ReorderBuffer;

/* forward declare so we don't have to include heapam_xlog.h */
struct xl_heap_new_cid;
struct xl_running_xacts;

/*
 * The role of a snapshot builder.
 *
 * In a parallel decoding session the walreader is the only process that reads
 * the WAL, so it is the only one that can drive the snapshot builder: it knows
 * which snapshot a transaction is decoded with, and when the transactions that
 * are in flight have to switch over to a new one.
 *
 * The decode workers therefore do not need a builder of their own.  They use a
 * stand-in instead, which answers the few questions the decoding path asks
 * from the state the walreader hands down.
 */
typedef enum SnapBuildRole
{
	/* the serial path: the builder is driven by the decoding process */
	SNAPBUILD_ROLE_SERIAL = 0,

	/* the walreader of a parallel decoding session */
	SNAPBUILD_ROLE_WALREADER,

	/* the read-only stand-in of a decode worker */
	SNAPBUILD_ROLE_STANDIN
} SnapBuildRole;

extern void CheckPointSnapBuild(void);

extern SnapBuild *AllocateSnapshotBuilder(struct ReorderBuffer *reorder,
										  TransactionId xmin_horizon, XLogRecPtr start_lsn,
										  bool need_full_snapshot,
										  bool in_slot_creation,
										  XLogRecPtr two_phase_at);
extern void FreeSnapshotBuilder(SnapBuild *builder);

/*
 * Turn the builder into the stand-in of a decode worker, or into the builder
 * of the walreader.
 */
extern void SnapBuildSetRole(SnapBuild *builder, SnapBuildRole role);

/*
 * Hand the state of the builder down to a decode worker.
 *
 * The state of the builder is advanced by the walreader, which is the only
 * process that reads the WAL.  The workers learn about the changes from the
 * messages the walreader sends them.
 *
 * next_phase_at is the transaction the next phase of the builder starts at.
 * It tells the worker which transactions it has to skip, exactly like the
 * builder of the serial path does.
 */
extern void SnapBuildSetState(SnapBuild *builder, SnapBuildState state,
							  XLogRecPtr start_decoding_at,
							  TransactionId next_phase_at);

/*
 * The snapshot the builder currently holds, or NULL if it has none.
 */
extern Snapshot SnapBuildGetSnapshot(SnapBuild *builder);

/*
 * Install the snapshot the stand-in of a decode worker works with.
 *
 * The walreader is the only process that builds snapshots, so it is the one
 * that hands the content of the snapshot down to the workers.  The worker
 * keeps the snapshot in its stand-in builder, from where the decoding path
 * picks it up.
 */
extern void SnapBuildSetSnapshot(SnapBuild *builder, Snapshot snap);

/*
 * The position from which on the transactions are decoded.
 *
 * The transactions that ended before it are skipped.  The walreader knows the
 * value and hands it down to the workers, so that they skip exactly the
 * transactions the serial path skips.
 */
extern XLogRecPtr SnapBuildGetStartDecodingAt(SnapBuild *builder);

/*
 * The transaction the next phase of the builder starts at.
 *
 * The transactions that precede it are skipped while the builder has not
 * reached a consistent state yet.
 */
extern TransactionId SnapBuildGetNextPhaseAt(SnapBuild *builder);

extern void SnapBuildSnapIncRefcount(Snapshot snap);
extern void SnapBuildSnapDecRefcount(Snapshot snap);

extern Snapshot SnapBuildInitialSnapshot(SnapBuild *builder);
extern const char *SnapBuildExportSnapshot(SnapBuild *builder);
extern void SnapBuildClearExportedSnapshot(void);
extern void SnapBuildResetExportedSnapshotState(void);

extern SnapBuildState SnapBuildCurrentState(SnapBuild *builder);
extern Snapshot SnapBuildGetOrBuildSnapshot(SnapBuild *builder);

extern bool SnapBuildXactNeedsSkip(SnapBuild *builder, XLogRecPtr ptr);
extern XLogRecPtr SnapBuildGetTwoPhaseAt(SnapBuild *builder);
extern void SnapBuildSetTwoPhaseAt(SnapBuild *builder, XLogRecPtr ptr);

extern void SnapBuildCommitTxn(SnapBuild *builder, XLogRecPtr lsn,
							   TransactionId xid, int nsubxacts,
							   TransactionId *subxacts, uint32 xinfo);
extern bool SnapBuildProcessChange(SnapBuild *builder, TransactionId xid,
								   XLogRecPtr lsn);
extern void SnapBuildProcessNewCid(SnapBuild *builder, TransactionId xid,
								   XLogRecPtr lsn,
								   struct xl_heap_new_cid *xlrec);
extern void SnapBuildProcessRunningXacts(SnapBuild *builder, XLogRecPtr lsn,
										 struct xl_running_xacts *running);
extern void SnapBuildSerializationPoint(SnapBuild *builder, XLogRecPtr lsn);

extern bool SnapBuildSnapshotExists(XLogRecPtr lsn);

#endif							/* SNAPBUILD_H */
