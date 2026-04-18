/*-------------------------------------------------------------------------
 *
 * logicalsysrel.h
 *	  white-list checks for system relations in logical replication
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/replication/logicalsysrel.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef LOGICALSYSREL_H
#define LOGICALSYSREL_H

#include "postgres.h"

extern bool IsLogicalRepSystemRelationOid(Oid relid);

#endif							/* LOGICALSYSREL_H */
