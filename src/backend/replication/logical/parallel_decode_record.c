/*-------------------------------------------------------------------------
 *
 * parallel_decode_record.c
 *		Publisher-side parallel logical decoding: WAL record serialization.
 *
 * The walreader is the only reader of the WAL.  It hands the records it has
 * read to the decoding side as serialized copies of the decoded record, so
 * that the decoding side does not have to read the WAL itself.
 *
 * The serialization keeps the fields the logical decoding path looks at:
 * the record header, the main data of the record and the block references
 * with their data.  The tuple data of a change lives in the block data of
 * block 0, so the block data is what makes the transfer useful.
 *
 * The data is transferred as it is decoded: the record header is copied
 * verbatim, and the payloads are copied into freshly allocated memory, so
 * that they are properly aligned for the rm_decode functions that cast them
 * to the record specific structures.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 * Copyright (c) 2026, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/xlogreader.h"
#include "replication/parallel_decode.h"
#include "utils/memutils.h"

/*
 * The header of a serialized record.  It is written and read with memcpy(),
 * so the layout is the layout of this structure on the machine the session
 * runs on.  Both ends of the transfer are processes of the same server, so
 * that is fine.
 */
typedef struct PDRecordHeader
{
	uint32		version;			/* PD_RECORD_FORMAT_VERSION */
	uint32		length;				/* length of the payload that follows */
	XLogRecPtr	lsn;				/* start of the record */
	XLogRecPtr	next_lsn;			/* end of the record */
	XLogRecord	header;				/* the record header */
	RepOriginId record_origin;		/* replication origin of the record */
	TransactionId toplevel_xid;		/* top level transaction of the record */
	uint32		main_data_len;		/* length of the main data */
	int32		max_block_id;		/* highest block id in use, -1 if none */
} PDRecordHeader;

/*
 * The header of a serialized block reference.
 */
typedef struct PDBlockHeader
{
	uint8		in_use;				/* is this block ref in use? */
	uint8		forknum;			/* fork number */
	uint8		flags;				/* copy of the fork_flags field */
	uint8		pad;				/* padding, initialized to zero */
	RelFileLocator rlocator;		/* the relation the block belongs to */
	BlockNumber blkno;				/* the block number */
	uint32		data_len;			/* length of the block data */
} PDBlockHeader;

/*
 * Serialize a decoded record.
 */
void
PDRecordSerialize(DecodedXLogRecord *record, StringInfo buf)
{
	PDRecordHeader rh;
	PDBlockHeader bh;
	int			i;

	memset(&rh, 0, sizeof(rh));
	rh.version = PD_RECORD_FORMAT_VERSION;
	rh.lsn = record->lsn;
	rh.next_lsn = record->next_lsn;
	rh.header = record->header;
	rh.record_origin = record->record_origin;
	rh.toplevel_xid = record->toplevel_xid;
	rh.main_data_len = record->main_data_len;
	rh.max_block_id = record->max_block_id;

	appendBinaryStringInfo(buf, (const char *) &rh, sizeof(rh));

	if (record->main_data_len > 0)
		appendBinaryStringInfo(buf, record->main_data, record->main_data_len);

	for (i = 0; i <= record->max_block_id; i++)
	{
		DecodedBkpBlock *blk = &record->blocks[i];

		memset(&bh, 0, sizeof(bh));
		bh.in_use = blk->in_use;
		bh.forknum = (uint8) blk->forknum;
		bh.flags = blk->flags;
		bh.rlocator = blk->rlocator;
		bh.blkno = blk->blkno;
		bh.data_len = blk->has_data ? (uint32) blk->data_len : 0;

		appendBinaryStringInfo(buf, (const char *) &bh, sizeof(bh));

		if (bh.data_len > 0)
			appendBinaryStringInfo(buf, blk->data, bh.data_len);
	}
}

/*
 * Deserialize a record.
 *
 * The result is a record of the same shape as the one the reader produces,
 * so the decoding side can use it exactly like the record it would have read
 * itself.  The caller frees it with PDRecordFree().
 */
DecodedXLogRecord *
PDRecordDeserialize(const char *data, size_t len)
{
	PDRecordHeader rh;
	const char *p = data;
	const char *end = data + len;
	DecodedXLogRecord *record;
	char	   *main_data = NULL;
	int			nblocks;
	int			i;

	if (len < sizeof(PDRecordHeader))
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("serialized WAL record is too short (%zu bytes)",
						(size_t) len)));

	memcpy(&rh, p, sizeof(rh));
	p += sizeof(rh);

	if (rh.version != PD_RECORD_FORMAT_VERSION)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("serialized WAL record has an unsupported version %u",
						rh.version)));

	if (rh.max_block_id < -1)
		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("serialized WAL record has an invalid block count %d",
						rh.max_block_id)));

	nblocks = rh.max_block_id + 1;

	record = palloc0(MAXALIGN(sizeof(DecodedXLogRecord) +
							   nblocks * sizeof(DecodedBkpBlock)));

	record->lsn = rh.lsn;
	record->next_lsn = rh.next_lsn;
	record->header = rh.header;
	record->record_origin = rh.record_origin;
	record->toplevel_xid = rh.toplevel_xid;
	record->main_data_len = rh.main_data_len;
	record->max_block_id = rh.max_block_id;
	record->next = NULL;

	/* The main data of the record. */
	if (rh.main_data_len > 0)
	{
		if (p + rh.main_data_len > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("serialized WAL record is truncated")));

		main_data = palloc(rh.main_data_len);
		memcpy(main_data, p, rh.main_data_len);
		p += rh.main_data_len;
	}
	record->main_data = main_data;

	/* The block references of the record. */
	for (i = 0; i < nblocks; i++)
	{
		PDBlockHeader bh;
		DecodedBkpBlock *blk = &record->blocks[i];

		if (p + sizeof(PDBlockHeader) > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("serialized WAL record is truncated")));

		memcpy(&bh, p, sizeof(bh));
		p += sizeof(bh);

		if (bh.data_len > 0 && p + bh.data_len > end)
			ereport(ERROR,
					(errcode(ERRCODE_DATA_CORRUPTED),
					 errmsg("serialized WAL record is truncated")));

		blk->in_use = bh.in_use;
		blk->forknum = (ForkNumber) bh.forknum;
		blk->flags = bh.flags;
		blk->rlocator = bh.rlocator;
		blk->blkno = bh.blkno;
		blk->has_data = (bh.data_len > 0);
		blk->data_len = (uint16) bh.data_len;

		if (bh.data_len > 0)
		{
			blk->data = palloc(bh.data_len);
			memcpy(blk->data, p, bh.data_len);
			p += bh.data_len;
		}
		else
			blk->data = NULL;
	}

	return record;
}

/*
 * Free a record that PDRecordDeserialize() returned.
 */
void
PDRecordFree(DecodedXLogRecord *record)
{
	int			i;

	if (record == NULL)
		return;

	if (record->main_data)
		pfree(record->main_data);

	for (i = 0; i <= record->max_block_id; i++)
	{
		if (record->blocks[i].data)
			pfree(record->blocks[i].data);
	}

	pfree(record);
}
