/*-------------------------------------------------------------------------
 *
 * logicalddl.c
 *		Logical replication DDL support
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *		src/backend/replication/logical/logicalddl.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/xact.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "catalog/heap.h"
#include "catalog/indexing.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_sync.h"
#include "catalog/pg_subscription.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "replication/logicalddl.h"
#include "replication/logical.h"
#include "storage/lmgr.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/pg_lsn.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/varlena.h"

/*
 * Parse a DDL string (e.g., "table,index") into a DDL kind bitmask.
 */
int
parse_ddl_string(const char *ddl_str)
{
	int			ddl_kind = DDL_KIND_NONE;
	char	   *token;
	char	   *rawstr;
	char	   *saveptr;

	if (ddl_str == NULL || *ddl_str == '\0')
		return DDL_KIND_NONE;

	rawstr = pstrdup(ddl_str);
	token = strtok_r(rawstr, ",", &saveptr);

	while (token != NULL)
	{
		/* Skip leading/trailing whitespace */
		while (*token == ' ')
			token++;
		char *end = token + strlen(token) - 1;
		while (end > token && *end == ' ')
			*end-- = '\0';

		if (strcmp(token, "table") == 0)
			ddl_kind |= DDL_KIND_TABLE;
		else if (strcmp(token, "index") == 0)
			ddl_kind |= DDL_KIND_INDEX;
		else
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("unrecognized DDL kind: \"%s\"", token)));

		token = strtok_r(NULL, ",", &saveptr);
	}

	pfree(rawstr);
	return ddl_kind;
}

/*
 * Check if a statement is a DDL statement we want to replicate.
 *
 * Currently supported DDL types:
 * - CREATE TABLE / DROP TABLE / ALTER TABLE (table DDL)
 * - Index statements (IndexStmt - includes CREATE/DROP INDEX)
 */
static bool
is_replicable_ddl(Node *stmt)
{
	if (stmt == NULL)
		return false;

	switch (nodeTag(stmt))
	{
			/* Table DDL */
		case T_CreateStmt:
		case T_DropStmt:
		case T_AlterTableStmt:
			return true;

			/* Index DDL - all index operations are T_IndexStmt */
		case T_IndexStmt:
			return true;

		default:
			return false;
	}
}

/*
 * Get DDL kind from statement node.
 */
static int
get_ddl_kind(Node *stmt)
{
	if (stmt == NULL)
		return DDL_KIND_NONE;

	switch (nodeTag(stmt))
	{
			/* Table DDL */
		case T_CreateStmt:
		case T_DropStmt:
		case T_AlterTableStmt:
			return DDL_KIND_TABLE;

			/* Index DDL */
		case T_IndexStmt:
			return DDL_KIND_INDEX;

		default:
			return DDL_KIND_NONE;
	}
}

/*
 * Get command tag for a DDL statement.
 */
static const char *
get_ddl_command_tag(Node *stmt)
{
	if (stmt == NULL)
		return "";

	switch (nodeTag(stmt))
	{
		case T_CreateStmt:
			return "CREATE TABLE";
		case T_DropStmt:
			return "DROP TABLE";
		case T_AlterTableStmt:
			return "ALTER TABLE";
		case T_IndexStmt:
			{
				/* We can't easily distinguish CREATE/DROP/ALTER here
				 * without more context, so use a generic tag */
				return "INDEX";
			}
		default:
			return "";
	}
}

/*
 * Get target table/index name from a DDL statement.
 */
static char *
get_ddl_target_table(Node *stmt)
{
	if (stmt == NULL)
		return NULL;

	switch (nodeTag(stmt))
	{
		case T_CreateStmt:
			{
				CreateStmt *create = (CreateStmt *) stmt;
				return create->relation->relname;
			}

		case T_DropStmt:
			{
				DropStmt *drop = (DropStmt *) stmt;
				if (drop->objects != NIL)
				{
					List *obj = (List *) linitial(drop->objects);
					if (obj != NIL && IsA(linitial(obj), RangeVar))
					{
						RangeVar *rv = (RangeVar *) linitial(obj);
						return rv->relname;
					}
				}
				return NULL;
			}

		case T_AlterTableStmt:
			{
				AlterTableStmt *alter = (AlterTableStmt *) stmt;
				if (alter->relation != NULL)
					return alter->relation->relname;
				return NULL;
			}

		case T_IndexStmt:
			{
				IndexStmt *idx = (IndexStmt *) stmt;
				if (idx->relation != NULL)
					return idx->relation->relname;
				return NULL;
			}

		default:
			return NULL;
	}
}

/*
 * Build LogicalDDLCommand from a PlannedStmt and query string.
 *
 * Returns true if the statement is a replicable DDL.
 */
bool
BuildLogicalDDLCommandIfNeeded(PlannedStmt *pstmt, const char *queryString,
							   ProcessUtilityContext context,
							   LogicalDDLCommand *cmd)
{
	Node	   *stmt;
	int			ddl_kind;

	/* Initialize command */
	memset(cmd, 0, sizeof(LogicalDDLCommand));

	/* We only handle utility statements (DDL are utility statements) */
	if (pstmt->utilityStmt == NULL)
		return false;

	stmt = pstmt->utilityStmt;

	/* Check if it's a DDL type we want to replicate */
	if (!is_replicable_ddl(stmt))
		return false;

	/* Get the DDL kind */
	ddl_kind = get_ddl_kind(stmt);
	if (ddl_kind == DDL_KIND_NONE)
		return false;

	cmd->ddl_kind = ddl_kind;

	/* Get command tag */
	cmd->command_tag = pstrdup(get_ddl_command_tag(stmt));

	/* Store query string */
	cmd->query_string = pstrdup(queryString);

	/* Get target table */
	cmd->target_table = get_ddl_target_table(stmt);

	/* Store transaction info */
	cmd->xid = GetCurrentTransactionId();
	cmd->ts = GetCurrentTransactionStartTimestamp();
	cmd->lsn = GetXLogInsertRecPtr();

	/* DDL seqno - could be a sequence number within the transaction */
	cmd->ddl_seqno = 0;

	return true;
}

/*
 * Build a tuple for pg_publication_sync from a LogicalDDLCommand.
 */
HeapTuple
PublicationSyncBuildTuple(Relation rel, LogicalDDLCommand *cmd)
{
	Datum		values[Natts_pg_publication_sync];
	bool		nulls[Natts_pg_publication_sync];
	HeapTuple	tup;
	ListCell   *lc;
	ArrayType  *publications_arr;
	char	   *namespace_str;
	int			i;
	int			num_pubs;
	char	  **pub_names;

	/* Initialize values and nulls */
	memset(values, 0, sizeof(values));
	memset(nulls, false, sizeof(nulls));

	/* LSN - use current LSN position */
	values[Anum_pg_publication_sync_psnlsn - 1] = GetXLogInsertRecPtr();

	/* Timestamp - current transaction timestamp */
	values[Anum_pg_publication_sync_pstimestamp - 1] = TimestampTzGetDatum(GetCurrentTransactionStartTimestamp());

	/* Message type */
	values[Anum_pg_publication_sync_psnmsgtype - 1] = CharGetDatum(PSN_MSG_TYPE_DDL);

	/* Message data - store as text (normalized SQL) */
	if (cmd->normalized_sql != NULL)
		values[Anum_pg_publication_sync_psnmsgdata - 1] = CStringGetTextDatum(cmd->normalized_sql);
	else if (cmd->query_string != NULL)
		values[Anum_pg_publication_sync_psnmsgdata - 1] = CStringGetTextDatum(cmd->query_string);
	else
		nulls[Anum_pg_publication_sync_psnmsgdata - 1] = true;

	/* Namespace - search_path at DDL execution time */
	namespace_str = NULL;
	if (namespace_str != NULL)
		values[Anum_pg_publication_sync_psnnamespace - 1] = CStringGetTextDatum(namespace_str);
	else
		nulls[Anum_pg_publication_sync_psnnamespace - 1] = true;

	/* Publications - array of publication names */
	if (cmd->publication_names != NIL)
	{
		num_pubs = list_length(cmd->publication_names);
		pub_names = (char **) palloc(num_pubs * sizeof(char *));
		i = 0;
		foreach(lc, cmd->publication_names)
		{
			pub_names[i++] = (char *) lfirst(lc);
		}

		publications_arr = construct_array((Datum *) pub_names, num_pubs,
										   TEXTOID, -1, false, 'i');
		values[Anum_pg_publication_sync_psnpublications - 1] = PointerGetDatum(publications_arr);
	}
	else
		nulls[Anum_pg_publication_sync_psnpublications - 1] = true;

	/* Extra information as JSONB - build from command data */
	if (cmd->extra != 0)
		values[Anum_pg_publication_sync_psnmsgextra - 1] = cmd->extra;
	else
		nulls[Anum_pg_publication_sync_psnmsgextra - 1] = true;

	/* Build the tuple */
	tup = heap_form_tuple(RelationGetDescr(rel), values, nulls);

	return tup;
}

/*
 * Insert a DDL command into pg_publication_sync.
 */
void
PublicationSyncInsert(LogicalDDLCommand *cmd)
{
	Relation	rel;
	HeapTuple	tup;

	/* Open pg_publication_sync for insertion */
	rel = table_open(PublicationSyncRelationId, RowExclusiveLock);

	/* Build the tuple */
	tup = PublicationSyncBuildTuple(rel, cmd);

	/* Insert into catalog */
	CatalogTupleInsert(rel, tup);

	/* Cleanup */
	heap_freetuple(tup);
	table_close(rel, RowExclusiveLock);
}

/*
 * Get list of publications that should receive this DDL command.
 */
List *
GetDDLTargetPublications(LogicalDDLCommand *cmd)
{
	List	   *result = NIL;
	ListCell   *lc;

	/* The publication names are already stored in the command */
	foreach(lc, cmd->publication_names)
	{
		char	   *pubname = (char *) lfirst(lc);
		Oid			pubid = get_publication_oid(pubname, false);

		result = lappend_oid(result, pubid);
	}

	return result;
}

/*
 * Check if a DDL command should be replicated to a subscription.
 */
bool
DDLMessageMatchesSubscription(LogicalDDLCommand *cmd, Oid subid)
{
	Subscription *sub;
	bool		matches = false;
	ListCell   *lc;

	/* Get the subscription */
	sub = GetSubscription(subid, false);

	/* Check if subscription is enabled */
	if (!sub->enabled)
	{
		FreeSubscription(sub);
		return false;
	}

	/* Check if the DDL kind matches the subscription's DDL setting */
	if ((cmd->ddl_kind & sub->subddl) == 0)
	{
		/* DDL kind not in subscription's DDL settings */
		FreeSubscription(sub);
		return false;
	}

	/* Check if any of the command's publications match the subscription's publications */
	foreach(lc, cmd->publication_names)
	{
		char	   *cmd_pubname = (char *) lfirst(lc);
		ListCell   *lc2;

		foreach(lc2, sub->publications)
		{
			char	   *sub_pubname = (char *) lfirst(lc2);

			if (strcmp(cmd_pubname, sub_pubname) == 0)
			{
				matches = true;
				break;
			}
		}

		if (matches)
			break;
	}

	FreeSubscription(sub);
	return matches;
}

/*
 * Convert a tuple from pg_publication_sync to LogicalDDLCommand.
 */
LogicalDDLCommand *
SyncTupleToLogicalDDLCommand(HeapTuple tuple, TupleDesc tupdesc)
{
	LogicalDDLCommand *cmd;
	bool		isnull;
	Datum		datum;
	ArrayType  *arr;
	int			i;

	cmd = (LogicalDDLCommand *) palloc(sizeof(LogicalDDLCommand));
	memset(cmd, 0, sizeof(LogicalDDLCommand));

	/* Get LSN */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnlsn,
						 tupdesc, &isnull);
	cmd->lsn = DatumGetLSN(datum);

	/* Get timestamp */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_pstimestamp,
						 tupdesc, &isnull);
	cmd->ts = DatumGetTimestamp(datum);

	/* Get message type */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnmsgtype,
						 tupdesc, &isnull);
	cmd->command_tag = (char *) palloc(2);
	cmd->command_tag[0] = DatumGetChar(datum);
	cmd->command_tag[1] = '\0';

	/* Get message data (SQL string) */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnmsgdata,
						 tupdesc, &isnull);
	if (!isnull)
		cmd->normalized_sql = TextDatumGetCString(datum);
	else
		cmd->normalized_sql = NULL;

	/* Get namespace */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnnamespace,
						 tupdesc, &isnull);
	if (!isnull)
		cmd->target_table = TextDatumGetCString(datum);
	else
		cmd->target_table = NULL;

	/* Get publications array */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnpublications,
						 tupdesc, &isnull);
	if (!isnull)
	{
		arr = DatumGetArrayTypeP(datum);
		Datum	   *elems;
		int			nelems;
		deconstruct_array_builtin(arr, TEXTOID, &elems, NULL, &nelems);

		for (i = 0; i < nelems; i++)
		{
			cmd->publication_names = lappend(cmd->publication_names,
											 pstrdup(TextDatumGetCString(elems[i])));
		}
	}

	/* Get extra information */
	datum = heap_getattr(tuple, Anum_pg_publication_sync_psnmsgextra,
						 tupdesc, &isnull);
	if (!isnull)
		cmd->extra = datum;
	else
		cmd->extra = 0;

	return cmd;
}

/*
 * Prune old entries from pg_publication_sync.
 *
 * This removes entries that are no longer needed based on the
 * subscription's confirmed LSN positions.
 *
 * For now, this is a placeholder that could be extended to prune
 * entries based on subscription progress.
 */
void
PublicationSyncPrune(void)
{
	/*
	 * XXX: Implementation needed.
	 *
	 * To implement proper pruning, we would need to:
	 * 1. Track the oldest LSN that all active subscriptions have confirmed
	 * 2. Delete entries with LSNs older than that cutoff
	 *
	 * For now, we don't prune anything to be safe.
	 */
}
