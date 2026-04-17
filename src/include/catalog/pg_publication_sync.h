/*-------------------------------------------------------------------------
 *
 * pg_publication_sync.h
 *	  definition of the system catalog for publication DDL sync metadata
 *	  (pg_publication_sync)
 *
 * Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
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

#include "catalog/genbki.h"
#include "catalog/pg_publication_sync_d.h"	/* IWYU pragma: export */

/* ----------------
 *		pg_publication_sync definition.  cpp turns this into
 *		typedef struct FormData_pg_publication_sync
 * ----------------
 */
CATALOG(pg_publication_sync,9352,PublicationSyncRelationId)
{
	Oid			pfsyncpubid BKI_LOOKUP(pg_publication);	/* publication OID */
	Oid			pfsyncnspid BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_namespace);
	Oid			pfsyncrelid BKI_DEFAULT(0) BKI_LOOKUP_OPT(pg_class);
	Oid			pfsyncobjid BKI_DEFAULT(0);
	int32		pfsyncsubid BKI_DEFAULT(0);
	bool		pfsyncenabled BKI_DEFAULT(t);
	int32		pfsyncddl BKI_DEFAULT(0);
	XLogRecPtr	pfsynclsn BKI_DEFAULT(0);
	TimestampTz	pfsyncts BKI_DEFAULT(0);

#ifdef CATALOG_VARLEN
	text		pfsyncmsgtype BKI_DEFAULT(_null_) BKI_FORCE_NULL;
	text		pfsynctargettable BKI_DEFAULT(_null_) BKI_FORCE_NULL;
	text		pfsyncddlsql BKI_DEFAULT(_null_) BKI_FORCE_NULL;
	text		pfsyncpublicationlist BKI_DEFAULT(_null_) BKI_FORCE_NULL;
	text		pfsyncsearchpath BKI_DEFAULT(_null_) BKI_FORCE_NULL;
	text		pfsyncextra BKI_DEFAULT(_null_) BKI_FORCE_NULL;
#endif
} FormData_pg_publication_sync;

/* ----------------
 *		Form_pg_publication_sync corresponds to a pointer to a tuple with
 *		the format of pg_publication_sync relation.
 * ----------------
 */
typedef FormData_pg_publication_sync *Form_pg_publication_sync;

DECLARE_TOAST(pg_publication_sync, 9356, 9357);

DECLARE_UNIQUE_INDEX(pg_publication_sync_map_index, 9354, PublicationSyncMapIndexId, pg_publication_sync, btree(pfsyncpubid oid_ops, pfsyncnspid oid_ops, pfsyncrelid oid_ops, pfsyncobjid oid_ops, pfsyncsubid int4_ops));
DECLARE_INDEX(pg_publication_sync_pubid_index, 9355, PublicationSyncPubidIndexId, pg_publication_sync, btree(pfsyncpubid oid_ops));

#endif							/* PG_PUBLICATION_SYNC_H */
