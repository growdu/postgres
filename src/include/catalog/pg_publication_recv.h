/*-------------------------------------------------------------------------
 *
 * pg_publication_recv.h
 *	  definition of the "publication recv" system catalog (pg_publication_recv)
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_publication_recv.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_PUBLICATION_RECV_H
#define PG_PUBLICATION_RECV_H

#include "access/xlogdefs.h"
#include "catalog/genbki.h"
#include "catalog/pg_publication_recv_d.h"

/* ----------------
 *		pg_publication_recv definition. cpp turns this into
 *		typedef struct FormData_pg_publication_recv
 * ----------------
 */
CATALOG(pg_publication_recv,9358,PublicationRecvRelationId)
{
	Oid			pfrecvsubid BKI_LOOKUP(pg_subscription);	/* local subscription oid */
	int64		pfrecvremote_msgid;	/* remote publication message id */
	XLogRecPtr	pfrecvremote_lsn;	/* remote ordering key */
	int32		pfrecvddl;		/* ddl class bitmask */
	char		pfrecvmsgtype;	/* Q/A/D */
	char		pfrecvstate;	/* R/A/S/F/K/D */
	int32		pfretry_count BKI_DEFAULT(0);

#ifdef CATALOG_VARLEN			/* variable-length fields start here */
	timestamptz pfrecvremote_ts BKI_FORCE_NULL;
	timestamptz pfrecvrecvts BKI_FORCE_NOT_NULL;
	timestamptz pfrecvapplyts BKI_FORCE_NULL;
	timestamptz pfrecvfinishts BKI_FORCE_NULL;
	text		pfrecvorigin BKI_FORCE_NULL;
	text		pfrecvpubname BKI_FORCE_NULL;
	text		pfrecvtargettable BKI_FORCE_NULL;
	text		pfrecvddlsql BKI_FORCE_NULL;
	text		pfrecvsearchpath BKI_FORCE_NULL;
	text		pfrecvextra BKI_FORCE_NULL;
	text		pfrecverrcode BKI_FORCE_NULL;
	text		pfrecverrmsg BKI_FORCE_NULL;
	text		pfrecverrdetail BKI_FORCE_NULL;
	timestamptz pfnext_retry_ts BKI_FORCE_NULL;
#endif
} FormData_pg_publication_recv;

/* ----------------
 *		Form_pg_publication_recv corresponds to a pointer to a tuple with
 *		the format of pg_publication_recv relation.
 * ----------------
 */
typedef FormData_pg_publication_recv *Form_pg_publication_recv;

DECLARE_TOAST(pg_publication_recv, 9359, 9360);

DECLARE_UNIQUE_INDEX(pg_publication_recv_pfrecvsubid_pfrecvremote_lsn_pfrecvremote_msgid_index, 9361, PublicationRecvSubidRemoteLsnMsgidIndexId, pg_publication_recv, btree(pfrecvsubid oid_ops, pfrecvremote_lsn pg_lsn_ops, pfrecvremote_msgid int8_ops));
DECLARE_INDEX(pg_publication_recv_pfrecvsubid_pfrecvstate_pfrecvremote_lsn_index, 9362, PublicationRecvSubidStateRemoteLsnIndexId, pg_publication_recv, btree(pfrecvsubid oid_ops, pfrecvstate char_ops, pfrecvremote_lsn pg_lsn_ops));
DECLARE_INDEX(pg_publication_recv_pfrecvrecvts_index, 9363, PublicationRecvRecvTsIndexId, pg_publication_recv, btree(pfrecvrecvts timestamptz_ops));
DECLARE_INDEX(pg_publication_recv_pfrecvorigin_pfrecvpubname_index, 9364, PublicationRecvOriginPubnameIndexId, pg_publication_recv, btree(pfrecvorigin text_ops, pfrecvpubname text_ops));

#ifdef EXPOSE_TO_CLIENT_CODE

#define PUBLICATION_RECV_STATE_RECEIVED	'R'
#define PUBLICATION_RECV_STATE_APPLYING	'A'
#define PUBLICATION_RECV_STATE_SUCCESS	'S'
#define PUBLICATION_RECV_STATE_FAILED	'F'
#define PUBLICATION_RECV_STATE_SKIPPED	'K'
#define PUBLICATION_RECV_STATE_DEAD	'D'

#endif							/* EXPOSE_TO_CLIENT_CODE */

#endif							/* PG_PUBLICATION_RECV_H */
