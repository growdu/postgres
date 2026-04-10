/*-------------------------------------------------------------------------
 *
 * logicalddl.h
 *	  Infrastructure for identifying and normalizing DDL suitable for
 *	  logical replication.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/replication/logicalddl.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALDDL_H
#define LOGICALDDL_H

#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"
#include "nodes/plannodes.h"
#include "tcop/utility.h"

typedef enum ReplicableDDLKind
{
	REPL_DDL_KIND_INVALID = 0,
	REPL_DDL_TABLE,
	REPL_DDL_INDEX
} ReplicableDDLKind;

#define LOGICAL_DDL_MESSAGE_PREFIX_TABLE	"pg_ddl_table"
#define LOGICAL_DDL_MESSAGE_PREFIX_INDEX	"pg_ddl_index"

typedef struct LogicalDDLCommand
{
	ReplicableDDLKind kind;

	Oid			classid;
	Oid			objid;
	int32		objsubid;

	Oid			relid;			/* relation being changed (if available) */
	Oid			nspid;			/* namespace of relid/object (if available) */

	char	   *command_tag;
	char	   *query_string;
	char	   *normalized_sql;
	char	   *object_identity;

	List	   *pubids;			/* publications selected later in pipeline */

	uint32		flags;
} LogicalDDLCommand;

#define LOGICAL_DDL_FLAG_TRANSACTIONAL			(1 << 0)

extern bool GetLogicalDDLInfo(PlannedStmt *pstmt,
							  const char *queryString,
							  ProcessUtilityContext context,
							  const ObjectAddress *address,
							  Oid relid_hint,
							  LogicalDDLCommand *cmd);
extern void FreeLogicalDDLCommand(LogicalDDLCommand *cmd);
extern const char *LogicalDDLMessagePrefix(ReplicableDDLKind kind);
extern ReplicableDDLKind LogicalDDLKindFromMessagePrefix(const char *prefix);

#endif							/* LOGICALDDL_H */
