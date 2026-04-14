/*-------------------------------------------------------------------------
 *
 * logicalsysrel.c
 *	  helpers for explicitly allowed system relations in logical replication
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/replication/logical/logicalsysrel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_publication_sync.h"
#include "replication/logicalsysrel.h"

/*
 * Keep this whitelist intentionally narrow. This feature is about supporting
 * selected system relations, not opening all catalogs to logical replication.
 */
bool
IsLogicalRepSystemRelationOid(Oid relid)
{
	switch (relid)
	{
		case PublicationSyncRelationId:
			return true;
		default:
			return false;
	}
}
