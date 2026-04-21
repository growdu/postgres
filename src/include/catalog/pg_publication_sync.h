/*-------------------------------------------------------------------------
 *
 * pg_publication_sync.h
 *	  definition of the "publication sync" system catalog (pg_publication_sync)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
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
#include "catalog/genbki.h"
#include "catalog/pg_publication_sync_d.h"

/* ----------------
 *		pg_publication_sync definition. cpp turns this into
 *		typedef struct FormData_pg_publication_sync
 * ----------------
 */
CATALOG(pg_publication_sync,9352,PublicationSyncRelationId)
{
	Oid			pfsyncpubid BKI_LOOKUP(pg_publication);	/* publication oid */
	int64		pfsyncobjid;	/* publication-scoped message id */
	int64		pfsyncddl;		/* ddl class bitmask */
	bool		pfsyncenabled BKI_DEFAULT(t);
	XLogRecPtr	pfsynclsn;		/* message ordering key */
	char		pfsyncmsgtype;	/* Q/A/D */

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	timestamptz pfsyncts BKI_FORCE_NOT_NULL;	/* source timestamp */
	text		pfsynctargettable BKI_FORCE_NULL;
	text		pfsyncddlsql BKI_FORCE_NULL;
	text		pfsyncsearchpath BKI_FORCE_NULL;
	text		pfsyncpublicationlist BKI_FORCE_NULL;
	text		pfsyncextra BKI_FORCE_NULL;
#endif
} FormData_pg_publication_sync;

/* ----------------
 *		Form_pg_publication_sync corresponds to a pointer to a tuple with
 *		the format of pg_publication_sync relation.
 * ----------------
 */
typedef FormData_pg_publication_sync *Form_pg_publication_sync;

DECLARE_TOAST(pg_publication_sync, 9353, 9354);

DECLARE_UNIQUE_INDEX(pg_publication_sync_pfsyncpubid_pfsyncobjid_index, 9355, PublicationSyncPubidObjidIndexId, pg_publication_sync, btree(pfsyncpubid oid_ops, pfsyncobjid int8_ops));
DECLARE_INDEX(pg_publication_sync_pfsynclsn_index, 9356, PublicationSyncLsnIndexId, pg_publication_sync, btree(pfsynclsn pg_lsn_ops));
DECLARE_INDEX(pg_publication_sync_pfsyncmsgtype_index, 9357, PublicationSyncMsgTypeIndexId, pg_publication_sync, btree(pfsyncmsgtype char_ops));

#ifdef EXPOSE_TO_CLIENT_CODE

#define PUBLICATION_DDL_TABLE		(INT64CONST(1) << 0)
#define PUBLICATION_DDL_INDEX		(INT64CONST(1) << 1)
#define PUBLICATION_DDL_TRIGGER		(INT64CONST(1) << 2)
#define PUBLICATION_DDL_VIEW		(INT64CONST(1) << 3)
#define PUBLICATION_DDL_RULE		(INT64CONST(1) << 4)
#define PUBLICATION_DDL_SCHEMA		(INT64CONST(1) << 5)
#define PUBLICATION_DDL_FUNCTION	(INT64CONST(1) << 6)
#define PUBLICATION_DDL_TYPE		(INT64CONST(1) << 7)
#define PUBLICATION_DDL_DOMAIN		(INT64CONST(1) << 8)
#define PUBLICATION_DDL_EXTENSION	(INT64CONST(1) << 9)

#define PUBLICATION_DDL_ALL \
	(PUBLICATION_DDL_TABLE | \
	 PUBLICATION_DDL_INDEX | \
	 PUBLICATION_DDL_TRIGGER | \
	 PUBLICATION_DDL_VIEW | \
	 PUBLICATION_DDL_RULE | \
	 PUBLICATION_DDL_SCHEMA | \
	 PUBLICATION_DDL_FUNCTION | \
	 PUBLICATION_DDL_TYPE | \
	 PUBLICATION_DDL_DOMAIN | \
	 PUBLICATION_DDL_EXTENSION)

#define PUBLICATION_SYNC_MSGTYPE_QUERY	'Q'
#define PUBLICATION_SYNC_MSGTYPE_ADD	'A'
#define PUBLICATION_SYNC_MSGTYPE_DROP	'D'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_PUBLICATION_SYNC_H */
