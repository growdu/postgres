/*-------------------------------------------------------------------------
 *
 * logicalddl.c
 *	  Identify utility commands that can be represented as logical DDL
 *	  commands.
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logicalddl.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "catalog/namespace.h"
#include "catalog/objectaddress.h"
#include "nodes/parsenodes.h"
#include "replication/logicalddl.h"
#include "tcop/cmdtag.h"
#include "utils/lsyscache.h"

static bool get_replicable_ddl_kind(Node *parsetree, ReplicableDDLKind *kind);
static char *extract_stmt_sql(PlannedStmt *pstmt, const char *queryString);

/*
 * Fill logical DDL metadata for supported utility statements.
 */
bool
GetLogicalDDLInfo(PlannedStmt *pstmt,
				  const char *queryString,
				  ProcessUtilityContext context,
				  const ObjectAddress *address,
				  Oid relid_hint,
				  LogicalDDLCommand *cmd)
{
	Node	   *parsetree;
	ObjectAddress object;
	const char *command_tag;

	memset(cmd, 0, sizeof(LogicalDDLCommand));

	if (pstmt == NULL || pstmt->utilityStmt == NULL)
		return false;

	/*
	 * Skip utility processing that is not tied to a normal utility statement
	 * execution path.
	 */
	if (context == PROCESS_UTILITY_QUERY_NONATOMIC)
		return false;

	parsetree = pstmt->utilityStmt;
	if (!get_replicable_ddl_kind(parsetree, &cmd->kind))
		return false;

	command_tag = GetCommandTagName(CreateCommandTag(parsetree));
	cmd->command_tag = pstrdup(command_tag);
	cmd->query_string = extract_stmt_sql(pstmt, queryString);
	cmd->normalized_sql = cmd->query_string ? pstrdup(cmd->query_string) : NULL;
	cmd->flags = LOGICAL_DDL_FLAG_TRANSACTIONAL;

	if (address != NULL)
		object = *address;
	else
		object = InvalidObjectAddress;

	if (OidIsValid(object.classId) && OidIsValid(object.objectId))
	{
		cmd->classid = object.classId;
		cmd->objid = object.objectId;
		cmd->objsubid = object.objectSubId;
	}

	if (OidIsValid(relid_hint))
		cmd->relid = relid_hint;
	else if (cmd->classid == RelationRelationId)
		cmd->relid = cmd->objid;

	if (OidIsValid(cmd->relid))
		cmd->nspid = get_rel_namespace(cmd->relid);
	else if (cmd->classid == NamespaceRelationId)
		cmd->nspid = cmd->objid;

	if (cmd->classid != InvalidOid && cmd->objid != InvalidOid)
	{
		ObjectAddress addr;

		ObjectAddressSet(addr, cmd->classid, cmd->objid);
		addr.objectSubId = cmd->objsubid;
		cmd->object_identity = getObjectIdentity(&addr, true);
	}

	return true;
}

void
FreeLogicalDDLCommand(LogicalDDLCommand *cmd)
{
	if (cmd->command_tag != NULL)
		pfree(cmd->command_tag);
	if (cmd->query_string != NULL)
		pfree(cmd->query_string);
	if (cmd->normalized_sql != NULL)
		pfree(cmd->normalized_sql);
	if (cmd->object_identity != NULL)
		pfree(cmd->object_identity);
	if (cmd->pubids != NIL)
		list_free(cmd->pubids);

	memset(cmd, 0, sizeof(LogicalDDLCommand));
}

/*
 * Identify first-phase supported DDL classes (table/index family).
 */
static bool
get_replicable_ddl_kind(Node *parsetree, ReplicableDDLKind *kind)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateStmt:
		case T_CreateForeignTableStmt:
		case T_CreateTableAsStmt:
		case T_AlterTableStmt:
			*kind = REPL_DDL_TABLE;
			return true;

		case T_IndexStmt:
			*kind = REPL_DDL_INDEX;
			return true;

		case T_DropStmt:
			{
				DropStmt   *stmt = castNode(DropStmt, parsetree);

				if (stmt->removeType == OBJECT_INDEX)
				{
					*kind = REPL_DDL_INDEX;
					return true;
				}

				if (stmt->removeType == OBJECT_TABLE ||
					stmt->removeType == OBJECT_FOREIGN_TABLE)
				{
					*kind = REPL_DDL_TABLE;
					return true;
				}
			}
			break;

		case T_RenameStmt:
			{
				RenameStmt *stmt = castNode(RenameStmt, parsetree);

				if (stmt->renameType == OBJECT_INDEX)
				{
					*kind = REPL_DDL_INDEX;
					return true;
				}

				if (stmt->renameType == OBJECT_TABLE ||
					stmt->renameType == OBJECT_FOREIGN_TABLE ||
					stmt->renameType == OBJECT_COLUMN)
				{
					*kind = REPL_DDL_TABLE;
					return true;
				}
			}
			break;

		default:
			break;
	}

	return false;
}

/*
 * Extract just the current utility statement text from the whole query string.
 */
static char *
extract_stmt_sql(PlannedStmt *pstmt, const char *queryString)
{
	if (queryString == NULL)
		return NULL;

	if (pstmt->stmt_location < 0)
		return pstrdup(queryString);

	if (pstmt->stmt_len > 0)
		return pnstrdup(queryString + pstmt->stmt_location, pstmt->stmt_len);

	return pstrdup(queryString + pstmt->stmt_location);
}

const char *
LogicalDDLMessagePrefix(ReplicableDDLKind kind)
{
	switch (kind)
	{
		case REPL_DDL_TABLE:
			return LOGICAL_DDL_MESSAGE_PREFIX_TABLE;
		case REPL_DDL_INDEX:
			return LOGICAL_DDL_MESSAGE_PREFIX_INDEX;
		case REPL_DDL_KIND_INVALID:
			break;
	}

	return NULL;
}

ReplicableDDLKind
LogicalDDLKindFromMessagePrefix(const char *prefix)
{
	if (prefix == NULL)
		return REPL_DDL_KIND_INVALID;

	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_TABLE) == 0)
		return REPL_DDL_TABLE;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_INDEX) == 0)
		return REPL_DDL_INDEX;

	return REPL_DDL_KIND_INVALID;
}
