/*-------------------------------------------------------------------------
 *
 * logicalddl.h
 *	  Definitions for logical replication DDL support
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALDDL_H
#define LOGICALDDL_H

#include "access/xlogdefs.h"
#include "catalog/pg_publication.h"
#include "nodes/pg_list.h"
#include "tcop/utility.h"
#include "utils/timestamp.h"

/*
 * DDL kinds that can be replicated.
 */
typedef enum DDLKind
{
	DDL_KIND_NONE = 0,
	DDL_KIND_TABLE = (1 << 0),
	DDL_KIND_INDEX = (1 << 1),
	DDL_KIND_ALL = (DDL_KIND_TABLE | DDL_KIND_INDEX)
} DDLKind;

/*
 * Message types for pg_publication_sync entries.
 */
#define PSN_MSG_TYPE_DDL 'Q'		/* DDL SQL statement */
#define PSN_MSG_TYPE_ADD 'A'		/* Add object */
#define PSN_MSG_TYPE_DELETE 'D'	/* Delete object */

/*
 * Logical DDL command structure for representing DDL operations
 * in the replication pipeline.
 */
typedef struct LogicalDDLCommand
{
	/* DDL kind bitmask */
	int			ddl_kind;

	/* Only used on publisher side internally */
	Oid			relid;
	Oid			nspid;

	/* Command tag (e.g., "CREATE TABLE", "CREATE INDEX") */
	char	   *command_tag;

	/* Original query string */
	char	   *query_string;

	/* Normalized SQL for execution on subscriber */
	char	   *normalized_sql;

	/* Object identity string */
	char	   *object_identity;

	/* Target table name */
	char	   *target_table;

	/* List of publication names (List<char *>) */
	List	   *publication_names;

	/* List of DDL kind names (List<char *>) */
	List	   *ddl_kind_names;

	/* Transaction information */
	uint64		xid;
	uint32		ddl_seqno;
	TimestampTz ts;
	XLogRecPtr	lsn;

	/* Extra information as JSONB */
	Datum		extra;			/* JsonbDatum */
} LogicalDDLCommand;

/* Function declarations */

/*
 * Build LogicalDDLCommand from a PlannedStmt and query string.
 * Returns true if the statement is a replicable DDL.
 */
extern bool BuildLogicalDDLCommandIfNeeded(PlannedStmt *pstmt,
										   const char *queryString,
										   ProcessUtilityContext context,
										   LogicalDDLCommand *cmd);

/*
 * Get list of publications that should receive this DDL command.
 */
extern List *GetDDLTargetPublications(LogicalDDLCommand *cmd);

/*
 * Insert a DDL command into pg_publication_sync.
 */
extern void PublicationSyncInsert(LogicalDDLCommand *cmd);

/*
 * Build a tuple for pg_publication_sync from LogicalDDLCommand.
 */
extern HeapTuple PublicationSyncBuildTuple(Relation rel,
										 LogicalDDLCommand *cmd);

/*
 * Check if a DDL command should be replicated to a subscription.
 */
extern bool DDLMessageMatchesSubscription(LogicalDDLCommand *cmd,
										  Oid subid);

/*
 * Prune old entries from pg_publication_sync.
 */
extern void PublicationSyncPrune(void);

/*
 * Convert a tuple from pg_publication_sync to LogicalDDLCommand.
 */
extern LogicalDDLCommand *SyncTupleToLogicalDDLCommand(HeapTuple tuple, TupleDesc tupdesc);

/*
 * Get DDL bitmap from string representation (e.g., "table,index").
 */
extern int	parse_ddl_string(const char *ddl_str);

/*
 * Register the DDL capture ProcessUtility hook.
 * Call this during startup to enable DDL capture.
 */
extern void RegisterLogicalDDLCaptureHook(void);

/*
 * Read and deserialize a DDL message from the logical replication stream.
 * Returns a newly allocated string containing the SQL, or NULL if not a DDL message.
 * The caller is responsible for freeing the returned string.
 *
 * Note: This only reads the DDL-specific payload, not the full MESSAGE protocol.
 * The caller should have already read the MESSAGE header (xid, flags, lsn, prefix).
 */
extern char *logicalrep_read_ddl_message(StringInfo s);

#endif							/* LOGICALDDL_H */
