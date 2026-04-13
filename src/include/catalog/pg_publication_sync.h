/*-------------------------------------------------------------------------
 *
 * pg_publication_sync.h
 *	  definition of the system catalog for DDL synchronization
 *	  (pg_publication_sync)
 *
 * Portions Copyright (c) 1996-2026, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication_sync.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_SYNC_H
#define PG_PUBLICATION_SYNC_H

#include "access/xlogdefs.h"
#include "datatype/timestamp.h"
#include "catalog/genbki.h"
#include "catalog/pg_publication_sync_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_publication_sync definition.  cpp turns this into
 *		typedef struct FormData_pg_publication_sync
 * ----------------
 */
CATALOG(pg_publication_sync,6600,PublicationSyncRelationId)
{
	/* LSN position for ordering */
	XLogRecPtr	psnlsn;

	/* Timestamp of the event */
	int64 pstimestamp;

	/* Message type: 'Q'=DDL SQL, 'A'=add object, 'D'=delete object */
	char		psnmsgtype;

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	/* SQL statement or JSON data */
	text		psnmsgdata;

	/* search_path at DDL execution time */
	text		psnnamespace;

	/* publication names this DDL applies to */
	text		psnpublications[1];

	/* extra information (version, sql_mode, etc.) */
	json		psnmsgextra;
#endif
} FormData_pg_publication_sync;

/* ----------------
 *		Form_pg_publication_sync corresponds to a pointer to a tuple with
 *		the format of pg_publication_sync relation.
 * ----------------
 */
typedef FormData_pg_publication_sync *Form_pg_publication_sync;

/*
 * We don't need toast table for this catalog as the data should be small
 * and will be cleaned up regularly via prune function.
 */

/* Index on lsn for ordering */
DECLARE_UNIQUE_INDEX_PKEY(pg_publication_sync_lsn_index, 6601, PublicationSyncLsnIndexId,
    pg_publication_sync, btree(psnlsn xlog_ops));

/* Index on timestamp for time-based queries */
DECLARE_INDEX(pg_publication_sync_timestamp_index, 6602, PublicationSyncTimestampIndexId,
    pg_publication_sync, btree(pstimestamp timestamptz_ops));

/* Index on publications for filtering by publication */
DECLARE_INDEX(pg_publication_sync_publication_index, 6603, PublicationSyncPublicationIndexId,
    pg_publication_sync, gin(psnpublications name_ops));

#endif							/* PG_PUBLICATION_SYNC_H */
