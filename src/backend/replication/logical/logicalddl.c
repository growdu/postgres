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
#include "tcop/tcopprot.h"
#include "tcop/cmdtag.h"
#include "utils/lsyscache.h"

static bool get_replicable_ddl_kind(Node *parsetree, Oid relid_hint,
									 ReplicableDDLKind *kind);
static ReplicableDDLKind get_drop_stmt_kind(DropStmt *stmt);
static ReplicableDDLKind get_rename_stmt_kind(RenameStmt *stmt);
static ReplicableDDLKind get_alter_table_kind(AlterTableStmt *stmt,
											  Oid relid_hint);
static bool is_trigger_alter_table_subcmd(AlterTableType subtype);
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
	if (context == PROCESS_UTILITY_QUERY_NONATOMIC ||
		context == PROCESS_UTILITY_SUBCOMMAND)
		return false;

	parsetree = pstmt->utilityStmt;
	if (!get_replicable_ddl_kind(parsetree, relid_hint, &cmd->kind))
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
 * Identify supported DDL classes from utility parse trees.
 */
static bool
get_replicable_ddl_kind(Node *parsetree, Oid relid_hint,
						ReplicableDDLKind *kind)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateStmt:
		case T_CreateForeignTableStmt:
			*kind = REPL_DDL_TABLE;
			return true;

		case T_CreateTableAsStmt:
			{
				CreateTableAsStmt *stmt = castNode(CreateTableAsStmt, parsetree);

				if (stmt->objtype == OBJECT_MATVIEW)
					*kind = REPL_DDL_VIEW;
				else
					*kind = REPL_DDL_TABLE;
				return true;
			}

		case T_AlterTableStmt:
			*kind = get_alter_table_kind(castNode(AlterTableStmt, parsetree),
										 relid_hint);
			return *kind != REPL_DDL_KIND_INVALID;

		case T_IndexStmt:
			*kind = REPL_DDL_INDEX;
			return true;

		case T_DefineStmt:
			{
				DefineStmt *stmt = castNode(DefineStmt, parsetree);

				if (stmt->kind == OBJECT_TYPE)
				{
					*kind = REPL_DDL_TYPE;
					return true;
				}
			}
			break;

		case T_CompositeTypeStmt:
		case T_CreateEnumStmt:
		case T_CreateRangeStmt:
		case T_AlterEnumStmt:
		case T_AlterTypeStmt:
			*kind = REPL_DDL_TYPE;
			return true;

		case T_CreateFunctionStmt:
			*kind = REPL_DDL_FUNCTION;
			return true;

		case T_AlterFunctionStmt:
			{
				AlterFunctionStmt *stmt = castNode(AlterFunctionStmt, parsetree);

				if (stmt->objtype == OBJECT_FUNCTION ||
					stmt->objtype == OBJECT_PROCEDURE ||
					stmt->objtype == OBJECT_ROUTINE)
				{
					*kind = REPL_DDL_FUNCTION;
					return true;
				}
			}
			break;

		case T_CreateDomainStmt:
		case T_AlterDomainStmt:
			*kind = REPL_DDL_DOMAIN;
			return true;

		case T_CreateTrigStmt:
			*kind = REPL_DDL_TRIGGER;
			return true;

		case T_ViewStmt:
			*kind = REPL_DDL_VIEW;
			return true;

		case T_RuleStmt:
			*kind = REPL_DDL_RULE;
			return true;

		case T_CreateSchemaStmt:
			*kind = REPL_DDL_SCHEMA;
			return true;

		case T_CreateExtensionStmt:
		case T_AlterExtensionStmt:
		case T_AlterExtensionContentsStmt:
			*kind = REPL_DDL_EXTENSION;
			return true;

		case T_DropStmt:
			*kind = get_drop_stmt_kind(castNode(DropStmt, parsetree));
			return *kind != REPL_DDL_KIND_INVALID;

		case T_RenameStmt:
			*kind = get_rename_stmt_kind(castNode(RenameStmt, parsetree));
			return *kind != REPL_DDL_KIND_INVALID;

		default:
			break;
	}

	return false;
}

static ReplicableDDLKind
get_drop_stmt_kind(DropStmt *stmt)
{
	switch (stmt->removeType)
	{
		case OBJECT_INDEX:
			return REPL_DDL_INDEX;
		case OBJECT_TABLE:
		case OBJECT_FOREIGN_TABLE:
			return REPL_DDL_TABLE;
		case OBJECT_TYPE:
			return REPL_DDL_TYPE;
		case OBJECT_DOMAIN:
			return REPL_DDL_DOMAIN;
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
			return REPL_DDL_FUNCTION;
		case OBJECT_TRIGGER:
			return REPL_DDL_TRIGGER;
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
			return REPL_DDL_VIEW;
		case OBJECT_RULE:
			return REPL_DDL_RULE;
		case OBJECT_SCHEMA:
			return REPL_DDL_SCHEMA;
		case OBJECT_EXTENSION:
			return REPL_DDL_EXTENSION;
		default:
			break;
	}

	return REPL_DDL_KIND_INVALID;
}

static ReplicableDDLKind
get_rename_stmt_kind(RenameStmt *stmt)
{
	switch (stmt->renameType)
	{
		case OBJECT_INDEX:
			return REPL_DDL_INDEX;
		case OBJECT_TABLE:
		case OBJECT_FOREIGN_TABLE:
			return REPL_DDL_TABLE;
		case OBJECT_COLUMN:
			if (stmt->relationType == OBJECT_VIEW ||
				stmt->relationType == OBJECT_MATVIEW)
				return REPL_DDL_VIEW;
			return REPL_DDL_TABLE;
		case OBJECT_TYPE:
			return REPL_DDL_TYPE;
		case OBJECT_DOMAIN:
			return REPL_DDL_DOMAIN;
		case OBJECT_FUNCTION:
		case OBJECT_PROCEDURE:
		case OBJECT_ROUTINE:
			return REPL_DDL_FUNCTION;
		case OBJECT_TRIGGER:
			return REPL_DDL_TRIGGER;
		case OBJECT_VIEW:
		case OBJECT_MATVIEW:
			return REPL_DDL_VIEW;
		case OBJECT_RULE:
			return REPL_DDL_RULE;
		case OBJECT_SCHEMA:
			return REPL_DDL_SCHEMA;
		case OBJECT_EXTENSION:
			return REPL_DDL_EXTENSION;
		default:
			break;
	}

	return REPL_DDL_KIND_INVALID;
}

static ReplicableDDLKind
get_alter_table_kind(AlterTableStmt *stmt, Oid relid_hint)
{
	bool		has_trigger_cmd = false;
	bool		has_non_trigger_cmd = false;
	ListCell   *lc;

	foreach(lc, stmt->cmds)
	{
		AlterTableCmd *cmd = lfirst_node(AlterTableCmd, lc);

		if (is_trigger_alter_table_subcmd(cmd->subtype))
			has_trigger_cmd = true;
		else
			has_non_trigger_cmd = true;
	}

	if (has_trigger_cmd && !has_non_trigger_cmd)
		return REPL_DDL_TRIGGER;

	if (OidIsValid(relid_hint))
	{
		char		relkind = get_rel_relkind(relid_hint);

		if (relkind == RELKIND_VIEW || relkind == RELKIND_MATVIEW)
			return REPL_DDL_VIEW;
	}

	return REPL_DDL_TABLE;
}

static bool
is_trigger_alter_table_subcmd(AlterTableType subtype)
{
	switch (subtype)
	{
		case AT_EnableTrig:
		case AT_EnableAlwaysTrig:
		case AT_EnableReplicaTrig:
		case AT_DisableTrig:
		case AT_EnableTrigAll:
		case AT_DisableTrigAll:
		case AT_EnableTrigUser:
		case AT_DisableTrigUser:
			return true;
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
	List	   *raw_parsetree_list;
	ReplicableDDLKind target_kind = REPL_DDL_KIND_INVALID;
	RawStmt    *candidate = NULL;
	ListCell   *lc;
	RawStmt    *rawstmt;

	if (queryString == NULL)
		return NULL;

	if (pstmt->stmt_location < 0)
		return pstrdup(queryString);

	if (pstmt->stmt_len > 0)
		return pnstrdup(queryString + pstmt->stmt_location, pstmt->stmt_len);

	/*
	 * Some utility recursion paths preserve only the outer stmt_len (or leave
	 * it as -1), which can make a naïve substring include following SQL
	 * statements. Re-parse from current stmt_location and keep only the first
	 * matching utility statement slice.
	 */
	raw_parsetree_list = pg_parse_query(queryString);
	(void) get_replicable_ddl_kind(pstmt->utilityStmt, InvalidOid,
								   &target_kind);

	foreach(lc, raw_parsetree_list)
	{
		ReplicableDDLKind raw_kind = REPL_DDL_KIND_INVALID;

		rawstmt = lfirst_node(RawStmt, lc);

		if (rawstmt->stmt_location < 0 || rawstmt->stmt_len <= 0)
			continue;

		if (!get_replicable_ddl_kind(rawstmt->stmt, InvalidOid, &raw_kind))
			continue;

		if (target_kind != REPL_DDL_KIND_INVALID && raw_kind != target_kind)
			continue;

		/*
		 * Prefer raw statements that start at or after the planner-reported
		 * location, but keep a fallback in case that location was inherited
		 * from a wrapper statement.
		 */
		if (pstmt->stmt_location >= 0 &&
			rawstmt->stmt_location >= pstmt->stmt_location)
		{
			candidate = rawstmt;
			break;
		}

		if (candidate == NULL)
			candidate = rawstmt;
	}

	if (candidate != NULL)
		return pnstrdup(queryString + candidate->stmt_location, candidate->stmt_len);

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
		case REPL_DDL_TYPE:
			return LOGICAL_DDL_MESSAGE_PREFIX_TYPE;
		case REPL_DDL_FUNCTION:
			return LOGICAL_DDL_MESSAGE_PREFIX_FUNCTION;
		case REPL_DDL_DOMAIN:
			return LOGICAL_DDL_MESSAGE_PREFIX_DOMAIN;
		case REPL_DDL_TRIGGER:
			return LOGICAL_DDL_MESSAGE_PREFIX_TRIGGER;
		case REPL_DDL_VIEW:
			return LOGICAL_DDL_MESSAGE_PREFIX_VIEW;
		case REPL_DDL_RULE:
			return LOGICAL_DDL_MESSAGE_PREFIX_RULE;
		case REPL_DDL_SCHEMA:
			return LOGICAL_DDL_MESSAGE_PREFIX_SCHEMA;
		case REPL_DDL_EXTENSION:
			return LOGICAL_DDL_MESSAGE_PREFIX_EXTENSION;
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
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_TYPE) == 0)
		return REPL_DDL_TYPE;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_FUNCTION) == 0)
		return REPL_DDL_FUNCTION;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_DOMAIN) == 0)
		return REPL_DDL_DOMAIN;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_TRIGGER) == 0)
		return REPL_DDL_TRIGGER;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_VIEW) == 0)
		return REPL_DDL_VIEW;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_RULE) == 0)
		return REPL_DDL_RULE;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_SCHEMA) == 0)
		return REPL_DDL_SCHEMA;
	if (strcmp(prefix, LOGICAL_DDL_MESSAGE_PREFIX_EXTENSION) == 0)
		return REPL_DDL_EXTENSION;

	return REPL_DDL_KIND_INVALID;
}
