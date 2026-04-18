/*-------------------------------------------------------------------------
 *
 * logicalsysrel.c
 *	  white-list checks for system relations in logical replication
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/backend/replication/logical/logicalsysrel.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_publication_sync.h"
#include "replication/logicalsysrel.h"

bool
IsLogicalRepSystemRelationOid(Oid relid)
{
	return relid == PublicationSyncRelationId;
}
