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
#include "catalog/namespace.h"
#include "catalog/pg_publication.h"
#include "catalog/pg_publication_sync.h"
#include "catalog/pg_subscription.h"
#include "commands/defrem.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_utilcmd.h"
#include "replication/logicalddl.h"
#include "replication/logical.h"
#include "replication/slot.h"
#include "replication/worker_internal.h"
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
#include "tcop/utility.h"
#include "libpq/pqformat.h"

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
		char	   *end;
		/* Skip leading/trailing whitespace */
		while (*token == ' ')
			token++;
		end = token + strlen(token) - 1;
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
 * Get target table Oid from a DDL statement.
 */
static Oid
get_ddl_target_relid(Node *stmt)
{
	if (stmt == NULL)
		return InvalidOid;

	switch (nodeTag(stmt))
	{
		case T_CreateStmt:
			{
				/* For CREATE TABLE, we don't have the relid yet as the table doesn't exist */
				return InvalidOid;
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
						return RangeVarGetRelid(rv, AccessShareLock, true);
					}
				}
				return InvalidOid;
			}

		case T_AlterTableStmt:
			{
				AlterTableStmt *alter = (AlterTableStmt *) stmt;
				if (alter->relation != NULL)
					return RangeVarGetRelid(alter->relation, AccessShareLock, true);
				return InvalidOid;
			}

		case T_IndexStmt:
			{
				IndexStmt *idx = (IndexStmt *) stmt;
				if (idx->relation != NULL)
					return RangeVarGetRelid(idx->relation, AccessShareLock, true);
				return InvalidOid;
			}

		default:
			return InvalidOid;
	}
}

/*
 * Get the namespace Oid from a DDL statement.
 */
static Oid
get_ddl_target_namespace(Node *stmt)
{
	if (stmt == NULL)
		return InvalidOid;

	switch (nodeTag(stmt))
	{
		case T_CreateStmt:
			{
				CreateStmt *create = (CreateStmt *) stmt;
				if (create->relation != NULL)
					return RangeVarGetCreationNamespace(create->relation);
				return InvalidOid;
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
						return RangeVarGetCreationNamespace(rv);
					}
				}
				return InvalidOid;
			}

		case T_AlterTableStmt:
			{
				AlterTableStmt *alter = (AlterTableStmt *) stmt;
				if (alter->relation != NULL)
					return RangeVarGetCreationNamespace(alter->relation);
				return InvalidOid;
			}

		case T_IndexStmt:
			{
				IndexStmt *idx = (IndexStmt *) stmt;
				if (idx->relation != NULL)
					return RangeVarGetCreationNamespace(idx->relation);
				return InvalidOid;
			}

		default:
			return InvalidOid;
	}
}

/*
 * Get ddl kind name from DDL kind bitmask.
 */
static const char *
get_ddl_kind_name(int ddl_kind)
{
	switch (ddl_kind)
	{
		case DDL_KIND_TABLE:
			return "table";
		case DDL_KIND_INDEX:
			return "index";
		default:
			return "unknown";
	}
}

/*
 * Build object identity string for a DDL command.
 */
static char *
build_object_identity(Node *stmt)
{
	Oid			nspid;
	const char *nspname;
	const char *relname;
	StringInfoData buf;

	if (stmt == NULL)
		return NULL;

	initStringInfo(&buf);

	nspid = get_ddl_target_namespace(stmt);
	nspname = get_namespace_name(nspid);
	relname = get_ddl_target_table(stmt);

	appendStringInfo(&buf, "%s.%s", nspname ? nspname : "public", relname ? relname : "");

	return buf.data;
}

/*
 * Build normalized SQL for a DDL command.
 * For now, we use the query string as-is but could apply normalization.
 */
static char *
build_normalized_sql(Node *stmt, const char *query_string)
{
	if (query_string == NULL)
		return NULL;

	/*
	 * For now, we use the query string directly.
	 * In future, we could use pg_query_normalize to normalize the SQL.
	 */
	return pstrdup(query_string);
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
	Oid			relid;
	Oid			nspid = InvalidOid;
	List	   *pubids = NIL;
	List	   *relpubids = NIL;
	List	   *schemapubids = NIL;
	List	   *allpubids = NIL;
	ListCell   *lc;

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

	/*
	 * Skip capture if we're executing replicated DDL.
	 * This prevents circular replication where DDL received from
	 * publisher and executed on subscriber is not re-captured.
	 */
	if (in_ddl_replay)
		return false;

	cmd->ddl_kind = ddl_kind;

	/* Get target table Oid (may be InvalidOid for CREATE TABLE) */
	relid = get_ddl_target_relid(stmt);

	/*
	 * Get all publications that could cover this DDL target.
	 *
	 * A table DDL can be published through an explicit FOR TABLE publication,
	 * a FOR TABLES IN SCHEMA publication, or a FOR ALL TABLES publication.
	 * CREATE TABLE has no relid yet, so only namespace/all-tables publications
	 * can match it at capture time.
	 */
	if (relid != InvalidOid)
	{
		relpubids = GetRelationPublications(relid);
		nspid = get_rel_namespace(relid);
	}
	else
		nspid = get_ddl_target_namespace(stmt);

	if (OidIsValid(nspid))
		schemapubids = GetSchemaPublications(nspid);

	allpubids = GetAllTablesPublications();

	pubids = list_concat_unique_oid(pubids, relpubids);
	pubids = list_concat_unique_oid(pubids, schemapubids);
	pubids = list_concat_unique_oid(pubids, allpubids);

	elog(DEBUG1,
		 "logicalddl: publication lookup kind=%d relid=%u nspid=%u table_pubs=%d schema_pubs=%d all_table_pubs=%d total_pubs=%d query=\"%s\"",
		 ddl_kind, relid, nspid,
		 list_length(relpubids),
		 list_length(schemapubids),
		 list_length(allpubids),
		 list_length(pubids),
		 queryString ? queryString : "");

	/* Filter publications that have DDL enabled */
	cmd->publication_names = NIL;
	foreach(lc, pubids)
	{
		Oid			pubid = lfirst_oid(lc);
		Publication *pub = GetPublication(pubid);
		bool		ddl_matched = (pub->pubddl & ddl_kind) != 0;

		elog(DEBUG1,
			 "logicalddl: publication candidate name=\"%s\" oid=%u pubddl=%d ddl_kind=%d matched=%s",
			 pub->name, pubid, pub->pubddl, ddl_kind,
			 ddl_matched ? "true" : "false");

		if (ddl_matched)
		{
			cmd->publication_names = lappend(cmd->publication_names, pstrdup(pub->name));
		}
		pfree(pub->name);
		pfree(pub);
	}

	list_free(relpubids);
	list_free(schemapubids);
	list_free(allpubids);
	list_free(pubids);

	/* If no publications have DDL enabled for this table, skip */
	if (cmd->publication_names == NIL)
	{
		elog(DEBUG1,
			 "logicalddl: skip DDL capture, no matching ddl-enabled publication kind=%d relid=%u nspid=%u query=\"%s\"",
			 ddl_kind, relid, nspid, queryString ? queryString : "");
		return false;
	}

	/* Get command tag */
	cmd->command_tag = pstrdup(get_ddl_command_tag(stmt));

	/* Store query string */
	cmd->query_string = pstrdup(queryString);

	/* Build normalized SQL */
	cmd->normalized_sql = build_normalized_sql(stmt, queryString);

	/* Get target table name */
	cmd->target_table = get_ddl_target_table(stmt);

	/* Build object identity */
	cmd->object_identity = build_object_identity(stmt);

	/* Build ddl_kind_names list */
	cmd->ddl_kind_names = list_make1(pstrdup(get_ddl_kind_name(ddl_kind)));

	/* Store transaction info */
	cmd->xid = GetCurrentTransactionId();
	cmd->ts = GetCurrentTransactionStartTimestamp();
	cmd->lsn = GetXLogInsertRecPtr();

	/* DDL seqno - could be a sequence number within the transaction */
	cmd->ddl_seqno = 0;

	elog(DEBUG1,
		 "logicalddl: captured DDL kind=%d relid=%u publications=%d sql=\"%s\"",
		 cmd->ddl_kind,
		 relid,
		 list_length(cmd->publication_names),
		 cmd->normalized_sql ? cmd->normalized_sql : "");

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
	Datum	   *pub_datums;

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
		pub_datums = (Datum *) palloc(num_pubs * sizeof(Datum));
		i = 0;
		foreach(lc, cmd->publication_names)
		{
			pub_datums[i++] = CStringGetTextDatum((char *) lfirst(lc));
		}

		publications_arr = construct_array_builtin(pub_datums, num_pubs,
												   TEXTOID);
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

	elog(DEBUG1,
		 "logicalddl: inserted pg_publication_sync row kind=%d publications=%d sql=\"%s\"",
		 cmd->ddl_kind,
		 list_length(cmd->publication_names),
		 cmd->normalized_sql ? cmd->normalized_sql : "");

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
		Datum	   *elems;
		int			nelems;

		arr = DatumGetArrayTypeP(datum);
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
 * For now, we use a simple approach based on the oldest confirmed_flush
 * LSN across all active logical replication slots that are subscribed
 * to publications with DDL enabled.
 *
 * Note: This is a simplified implementation. A more sophisticated approach
 * would track per-subscription confirmation progress and only prune entries
 * that all relevant subscriptions have confirmed.
 */
void
PublicationSyncPrune(void)
{
	Relation	rel;
	TableScanDesc scan;
	HeapTuple	tuple;
	XLogRecPtr	oldest_confirmed_lsn = InvalidXLogRecPtr;
	XLogRecPtr	cutoff_lsn;
	MemoryContext old_ctx;
	MemoryContext prune_ctx;
	int			deleted = 0;

	/*
	 * First pass: find the oldest confirmed_flush LSN across all
	 * active logical replication slots.
	 *
	 * We iterate through the shared replication slot array to find
	 * logical slots and get their confirmed_flush positions.
	 */
	{
		int			i;

		/* Hold the replication slot lock while scanning */
		LWLockAcquire(ReplicationSlotControlLock, LW_SHARED);

		for (i = 0; i < max_replication_slots; i++)
		{
			ReplicationSlot *slot = &ReplicationSlotCtl->replication_slots[i];

			if (!slot->in_use || !SlotIsLogical(slot))
				continue;

			/*
			 * For logical slots, check the confirmed_flush LSN.
			 * This represents how far the subscriber has confirmed receipt.
			 */
			if (slot->data.confirmed_flush != InvalidXLogRecPtr)
			{
				if (oldest_confirmed_lsn == InvalidXLogRecPtr ||
					slot->data.confirmed_flush < oldest_confirmed_lsn)
					oldest_confirmed_lsn = slot->data.confirmed_flush;
			}
		}

		LWLockRelease(ReplicationSlotControlLock);
	}

	/*
	 * If we couldn't find any confirmed LSN, we can't prune safely.
	 * This could happen if there are no active subscriptions.
	 */
	if (oldest_confirmed_lsn == InvalidXLogRecPtr)
		return;

	cutoff_lsn = oldest_confirmed_lsn;

	/*
	 * Create a memory context for the prune operation to avoid
	 * memory leaks during the scan and deletion.
	 */
	prune_ctx = AllocSetContextCreate(CurrentMemoryContext,
									   "PublicationSyncPrune",
									   ALLOCSET_DEFAULT_SIZES);
	old_ctx = MemoryContextSwitchTo(prune_ctx);

	/* Open pg_publication_sync for deletion */
	rel = table_open(PublicationSyncRelationId, RowExclusiveLock);

	/* Start a scan to find entries older than cutoff_lsn */
	scan = table_beginscan(rel, NULL, 0, NULL);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Datum	lsn_datum;
		XLogRecPtr	tuple_lsn;
		bool	isnull;

		lsn_datum = heap_getattr(tuple, Anum_pg_publication_sync_psnlsn,
								 RelationGetDescr(rel), &isnull);

		if (isnull)
			continue;

		tuple_lsn = DatumGetLSN(lsn_datum);

		/* Delete entries with LSN older than cutoff */
		if (tuple_lsn < cutoff_lsn)
		{
			CatalogTupleDelete(rel, &tuple->t_self);
			deleted++;
		}
	}

	heap_endscan(scan);
	table_close(rel, RowExclusiveLock);

	MemoryContextSwitchTo(old_ctx);
	MemoryContextDelete(prune_ctx);

	elog(DEBUG1, "pruned %d entries from pg_publication_sync (cutoff LSN: %X/%X)",
		 deleted, LSN_FORMAT_ARGS(cutoff_lsn));
}

/*
 * ProcessUtility hook for DDL capture.
 *
 * This hook intercepts DDL statements and captures them for
 * logical replication to subscribers.
 */
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

static void
logicalddl_ProcessUtility_hook(PlannedStmt *pstmt,
							  const char *queryString,
							  bool readOnlyTree,
							  ProcessUtilityContext context,
							  ParamListInfo params,
							  QueryEnvironment *queryEnv,
							  DestReceiver *dest,
							   QueryCompletion *qc)
{
	LogicalDDLCommand cmd;
	bool		captured = false;

	/*
	 * Skip capture if we're already in DDL replay (circular replication prevention)
	 * or if not in a normal utility context.
	 */
	if (in_ddl_replay)
		goto next_hook;

	/* Try to capture the DDL statement */
	captured = BuildLogicalDDLCommandIfNeeded(pstmt, queryString, context, &cmd);

	/* Call the next hook or standard_ProcessUtility */
next_hook:
	if (prev_ProcessUtility_hook)
		prev_ProcessUtility_hook(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);
	else
		standard_ProcessUtility(pstmt, queryString, readOnlyTree,
								context, params, queryEnv, dest, qc);

	/*
	 * If we captured a DDL, insert it into pg_publication_sync after
	 * the statement has been executed successfully.
	 */
	if (captured && cmd.normalized_sql != NULL)
	{
		PublicationSyncInsert(&cmd);

		/* Clean up the command */
		if (cmd.command_tag)
			pfree(cmd.command_tag);
		if (cmd.normalized_sql)
			pfree(cmd.normalized_sql);
		if (cmd.object_identity)
			pfree(cmd.object_identity);
		if (cmd.target_table)
			pfree(cmd.target_table);
		if (cmd.publication_names)
			list_free_deep(cmd.publication_names);
		if (cmd.ddl_kind_names)
			list_free_deep(cmd.ddl_kind_names);
	}
}

/*
 * Register the DDL capture ProcessUtility hook.
 *
 * This should be called during startup to enable DDL capture.
 */
void
RegisterLogicalDDLCaptureHook(void)
{
	if (ProcessUtility_hook == logicalddl_ProcessUtility_hook)
		return;

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = logicalddl_ProcessUtility_hook;
}

/*
 * Read and deserialize a DDL message from the logical replication stream.
 *
 * This function expects the caller to have already read the MESSAGE protocol
 * header (xid, flags, lsn). It reads the prefix and the DDL payload.
 *
 * Returns a newly allocated string containing the SQL if this is a "pg_ddl"
 * message, or NULL if it's a different type of message.
 * The caller is responsible for freeing the returned string.
 */
char *
logicalrep_read_ddl_message(StringInfo s)
{
	const char *prefix;
	int			msg_len;
	char	   *msg;

	/* Read message prefix */
	prefix = pq_getmsgstring(s);

	/* Check if this is a DDL message */
	if (strcmp(prefix, "pg_ddl") != 0)
	{
		/* Not a DDL message, put the data back */
		pq_getmsgend(s);
		return NULL;
	}

	/* Read message content */
	msg_len = pq_getmsgint(s, 4);
	msg = palloc(msg_len + 1);
	memcpy(msg, pq_getmsgbytes(s, msg_len), msg_len);
	msg[msg_len] = '\0';

	pq_getmsgend(s);

	return msg;
}
