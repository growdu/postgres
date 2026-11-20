/*-------------------------------------------------------------------------
 *
 * parallel_decode.h
 *		Shared declarations for publisher-side parallel logical decoding.
 *
 * The feature splits the publisher's logical decoding path into three
 * processes:
 *
 *   - walreader:  the only reader of WAL, drives snapbuild to consistent,
 *                 routes records to decode workers and allocates commit
 *                 sequence numbers;
 *   - decode worker: decodes tuples and produces replication protocol bytes;
 *   - walsender:  owns the replication slot and the network connection, and
 *                 emits commit/abort/cancel in commit sequence order.
 *
 * See doc/pa_decode/design.md for the full design.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARALLEL_DECODE_H
#define PARALLEL_DECODE_H

#include "postgres.h"

#include "access/transam.h"
#include "access/xlogreader.h"
#include "lib/stringinfo.h"
#include "replication/logical.h"
#include "replication/parallel_decode_stats.h"
#include "storage/shm_mq.h"
#include "storage/latch.h"
#include "storage/shm_toc.h"

/*
 * The largest size of the serialized output plugin options in the control
 * block.  The options of the output plugin are a handful of short strings,
 * so this is generous.
 */
#define PD_MAX_PLUGIN_OPTIONS		4096

/* GUCs */
extern PGDLLIMPORT int	max_parallel_decode_workers;
extern PGDLLIMPORT int	parallel_decode_work_mem;
extern PGDLLIMPORT int	parallel_decode_stream_segment_changes;
extern PGDLLIMPORT int	parallel_decode_batch_size;
extern PGDLLIMPORT int	parallel_decode_barrier_timeout;
extern PGDLLIMPORT int	parallel_decode_pending_max_bytes;
extern PGDLLIMPORT int	parallel_decode_held_max_bytes;
extern PGDLLIMPORT int	parallel_decode_fault_inject;
extern PGDLLIMPORT char *parallel_decode_trace_file;

/*
 * Am I a decode worker of the parallel decoding path?
 *
 * The flag follows the model of am_walsender: it tells the code that runs in
 * the processes of a session which one it runs in.  Only the decode worker
 * produces the output of the transactions.
 */
extern PGDLLIMPORT bool am_decode_worker;

/*
 * The failures the fault injection knob can produce.
 *
 * The knob exists to verify the failure paths of the session: the way a
 * participant reports an error, the way the walsender turns it into the end
 * of the session, and the way the subscriber recovers from it.
 */
#define PD_FAULT_NONE			0
#define PD_FAULT_WORKER			1	/* the decode workers fail when they start */
#define PD_FAULT_WALREADER		2	/* the walreader fails when it starts */

/* True when the publisher-side parallel path is available. */
extern bool ParallelDecodeEnabled(void);

/* True when the session is running on the parallel decoding path. */
extern bool ParallelDecodeActive(void);

/*
 * Tell whether the subscriber of a session applies the changes in parallel.
 *
 * The subscriber says so with the "streaming" option of the output plugin:
 * streaming='parallel' means it has a parallel apply worker, streaming='on'
 * means it applies the stream segments itself, and streaming='off' means it
 * does not want any stream segment at all.
 *
 * Parallel decoding and streaming are bound together: the parallel path can
 * only ship the changes of a transaction as stream segments, and the parallel
 * apply of the subscriber is what makes the segments worth producing.  Only
 * streaming='parallel' therefore takes the parallel path; the other two modes
 * take the serial path.
 *
 * plugin_options are the options the client gave for the output plugin.
 */
extern bool ParallelDecodeSubscriberAppliesInParallel(List *plugin_options);

/*
 * Wire trace facility.
 *
 * When parallel_decode_trace_file names a file, the walsender appends every
 * replication frame it hands to the network to that file.  The file is the
 * input of the frame level dump/diff tool, which compares the bytes produced
 * by the serial path against the bytes produced by the parallel path.
 *
 * File format: a sequence of records, each one a 4 byte big endian length
 * followed by that many bytes of payload.  A payload whose first byte is zero
 * is a session marker (the remaining eight bytes hold the session id); every
 * other payload is one replication frame, i.e. the payload of a CopyData
 * message, which starts with the frame type ('w', 'k', 'r' or 'c').
 */
extern void PDTraceInit(void);
extern void PDTraceWrite(const char *data, size_t len);
extern void PDTraceShutdown(void);

/*
 * Lifecycle, called from the walsender that owns the replication slot.
 *
 * slot is the replication slot of the session.  The workers do not own the
 * slot, so the fields of it that the decoding path looks at are copied into
 * the control block.
 *
 * plugin_options are the options the client gave for the output plugin, in
 * the serialized form of PDSerializePluginOptions().  The caller has to
 * serialize them before it creates its own decoding context: the startup
 * callback of an output plugin may modify the option values in place, and the
 * workers need the original values.
 *
 * start_lsn is the WAL position the decoding starts at, which is the
 * confirmed flush position of the slot (or the position the client asked
 * for, if that is further ahead).  The walreader starts reading at the
 * restart LSN of the slot.
 */
extern void ParallelDecodeSessionStart(ReplicationSlot *slot,
									   const char *plugin_options,
									   uint32 plugin_options_len,
									   XLogRecPtr start_lsn);
extern void ParallelDecodeSessionStop(void);

/*
 * Publish the statistics of the session for the monitoring views (11.3).
 *
 * The function copies the counters of the session out of the shared control
 * block into the shared memory of the walsenders, where the SQL functions
 * read them.  It is called by the walsender from time to time, and once more
 * when the session ends.
 *
 * force publishes the statistics even when the last publication is recent.
 */
extern void ParallelDecodePublishStats(bool force);

/*
 * Note that the session has written a frame of the given length to the
 * subscriber, and that it has flushed its output buffer to the network.
 *
 * The counters feed the monitoring views (11.3).
 */
extern void ParallelDecodeCountOutput(size_t bytes);
extern void ParallelDecodeCountSocketWrite(void);

/*
 * Entry points for the dynamically registered background workers.
 */
extern void ParallelDecodeWorkerMain(Datum main_arg);
extern void ParallelWalreaderMain(Datum main_arg);

/*
 * Shared control block.  Lives in the dynamic shared memory area created by
 * the walsender and is attached by walreader and decode workers.
 */
#define PD_WORKER_STATUS_UNUSED		0
#define PD_WORKER_STATUS_STARTING	1
#define PD_WORKER_STATUS_RUNNING	2
#define PD_WORKER_STATUS_FAILED		3
#define PD_WORKER_STATUS_EXITED		4

/*
 * The largest error text the participants of a session report.  The text is
 * kept in the control block, so that it survives the death of the process that
 * produced it.
 */
#define PD_MAX_ERROR_MESSAGE		512

typedef struct ParallelDecodeWorkerSlot
{
	pid_t		pid;			/* 0 until the worker has reported its pid */
	int			status;		/* PD_WORKER_STATUS_* */
	int			slot_index;	/* index of this worker within the session */
	uint64		error_lsn;		/* WAL position the failure was detected at */
	char		errmsg[PD_MAX_ERROR_MESSAGE];	/* error text reported by the
												 * worker */

	/*
	 * The counters of the monitoring views (11.3).
	 *
	 * Every counter has exactly one writer, so they are plain counters and
	 * not atomics: records_sent is written by the walreader, the others by
	 * the worker itself.  A reader may see a stale value, which is fine for
	 * a monitoring counter.
	 */
	volatile uint64 records_sent;		/* WAL records the walreader handed
										 * over */
	volatile uint64 records_consumed;	/* WAL records the worker decoded */
	volatile uint64 in_flight_txns;		/* transactions the worker decodes */
	volatile uint64 decoded_txns;		/* transactions the worker finished */
	volatile uint64 in_flight_bytes;	/* bytes the transactions produced */
	volatile uint64 wakeups;			/* signals the worker sent to the
										 * walsender */
} ParallelDecodeWorkerSlot;

typedef struct ParallelDecodeControl
{
	pid_t		walsender_pid;
	pid_t		walreader_pid;
	int			num_workers;
	int			walreader_status;	/* PD_WORKER_STATUS_* */

	/*
	 * The error the walreader reported, if it reported one.  The text is kept
	 * here, because the walreader is gone by the time the walsender reads the
	 * report.
	 */
	uint64		walreader_error_lsn;
	char		walreader_errmsg[PD_MAX_ERROR_MESSAGE];

	/*
	 * The WAL position the walreader has reached.  The walsender uses it to
	 * decide whether the session has caught up with the WAL that has been
	 * flushed, which is the same decision the serial path makes with the
	 * reader's EndRecPtr.
	 */
	volatile uint64 walreader_end_lsn;

	/* Set by the walsender once the slot and the plugin are known. */
	int			proto_version;
	char		plugin_name[NAMEDATALEN];

	/*
	 * The amount of memory each decode worker may use for the changes it has
	 * accumulated, in kilobytes.  Zero means the default of the decoding
	 * path.
	 */
	int			work_mem_kb;

	/*
	 * The number of changes a transaction has to accumulate before the
	 * workers send them as a stream segment of their own.  Zero means that
	 * the changes of a transaction are not cut: the transaction is streamed
	 * in a single segment when it commits.
	 */
	uint64		stream_segment_changes;

	/*
	 * The parameters of the session, as the decode workers need them.
	 *
	 * The walsender owns the replication slot, so the workers work on a copy
	 * of it that carries the fields the decoding path looks at: the name of
	 * the slot, the name of the output plugin and the database the slot
	 * belongs to.  The output plugin options are handed over in the
	 * serialized form of PD_MAX_PLUGIN_OPTIONS bytes.
	 */
	char		slot_name[NAMEDATALEN];
	Oid			database;
	bool		two_phase;
	XLogRecPtr	two_phase_at;
	uint32		plugin_options_len;
	char		plugin_options[PD_MAX_PLUGIN_OPTIONS];

	/*
	 * The WAL position the decoding starts at.  The workers use it as the
	 * start of the decoding window, which is the same value the walsender
	 * would have used for its own decoding context.
	 */
	XLogRecPtr	start_lsn;

	/* Worker status slots. */
	ParallelDecodeWorkerSlot workers[MAX_PARALLEL_DECODE_WORKERS];

	/* Set when any participant has failed; the session must be torn down. */
	volatile uint32 failed;

	/* Set by the walsender to ask all participants to wind down. */
	volatile uint32 shutdown;

	/*
	 * The session counters of the monitoring views (11.3).  Like the counters
	 * of the worker slots, every one of them has exactly one writer:
	 * cseq_allocated and walreader_wakeups are written by the walreader,
	 * cseq_released by the walsender.
	 */
	volatile uint64 cseq_allocated;		/* the highest commit sequence number
										 * the walreader has handed out */
	volatile uint64 cseq_released;		/* the highest commit sequence number
										 * the walsender has released */
	volatile uint64 walreader_wakeups;	/* signals the walreader sent to the
										 * workers and over the control
										 * queue to the walsender */
	volatile uint64 wal_records;			/* WAL records handed to the
										 * workers */

	/*
	 * The counters the walsender collects: the output of the session and the
	 * time the decoding of the transactions took.  They are written by the
	 * walsender only.
	 */
	volatile uint64 frames;			/* frames written to the subscriber */
	volatile uint64 frame_bytes;	/* bytes written to the subscriber */
	volatile uint64 socket_writes;	/* flushes of the output buffer */
	volatile uint64 txn_count;		/* transactions the session decoded */
	volatile uint64 txn_latency_us_sum;	/* sum of the decoding times */
	volatile uint64 txn_latency_us_max;	/* longest decoding time */
} ParallelDecodeControl;



/* Payload handed to the dynamic background workers through bgw_extra. */
typedef struct ParallelDecodeWorkerArg
{
	int			dsm_handle;
	int			slot_index;		/* -1 for the walreader */
	Oid			db_oid;			/* database the output plugin works on */

	/*
	 * The WAL position the walreader has to start reading at.  Only the
	 * walreader uses it; the decode workers learn their starting point from
	 * the records they receive.
	 */
	XLogRecPtr	start_lsn;
} ParallelDecodeWorkerArg;

/*
 * Magic number and table-of-contents keys of the shared segment.  The keys
 * are shared by all participants, so they live in the header.
 */
#define PARALLEL_DECODE_SHM_MAGIC		0x50445256	/* "PDRV" */
#define PARALLEL_DECODE_KEY_SHARED		1
#define PARALLEL_DECODE_KEY_CONTROL_MQ	2	/* walreader -> walsender */
#define PARALLEL_DECODE_KEY_WAL_MQ_BASE	10			/* + worker index */
#define PARALLEL_DECODE_KEY_OUT_MQ_BASE	100			/* + worker index */

/* Size of one shared message queue. */
#define PARALLEL_DECODE_QUEUE_SIZE		(256 * 1024)

/*
 * The largest payload a single chunk may carry.  A chunk has to fit into a
 * queue entry together with the message header, and it has to leave room in
 * the ring for the queue's own bookkeeping.
 */
#define PARALLEL_DECODE_CHUNK_SIZE		(64 * 1024)

/* Worker-side helpers, implemented in parallel_decode.c. */
extern ParallelDecodeControl *ParallelDecodeWorkerAttach(ParallelDecodeWorkerArg *arg,
														 int *slot_index,
														 dsm_segment **seg,
														 shm_toc **toc);
extern shm_mq *ParallelDecodeLookupMq(shm_toc *toc, int key);

/* Returns true when the given process still exists. */
extern bool ParallelDecodeProcessAlive(pid_t pid);

/*
 * Message framing over a shared memory queue.
 *
 * Every entry on a queue is a header followed by a payload.  The header is
 * fixed size, so the receiver can find the payload without copying
 * anything: the payload lives in the ring buffer of the queue and stays
 * valid until the next receive from the same queue.
 *
 * All queues are single sender / single receiver, so a message is never
 * interleaved with another sender's message.
 */
typedef enum ParallelDecodeEntryType
{
	PD_ENT_SESSION_INIT = 1,	/* walreader -> worker */
	PD_ENT_WAL = 2,				/* walreader -> worker */
	PD_ENT_SNAPSHOT = 3,		/* walreader -> worker */
	PD_ENT_GLOBAL_INVAL = 4,	/* walreader -> worker */
	PD_ENT_TXN_END = 5,		/* walreader -> worker */
	PD_ENT_SUBXACT_ABORT = 6,	/* walreader -> worker */
	PD_ENT_BLOCK = 7,			/* worker -> walsender */
	PD_ENT_EVENT = 8,			/* worker -> walsender */
	PD_ENT_ERROR = 9,			/* worker or walreader -> walsender */
	PD_ENT_READY = 10,			/* walreader -> walsender */
	PD_ENT_FRAME = 11,			/* worker -> walsender */
	PD_ENT_POSITION = 12,		/* worker -> walsender */
	PD_ENT_CSEQ = 13,			/* walreader -> walsender */
	PD_ENT_SEGMENT = 14,		/* worker -> walsender */
	PD_ENT_MARKER = 15,		/* walreader -> worker */
	PD_ENT_WATERMARK = 16,		/* walreader -> walsender */
	PD_ENT_RELATION = 17,		/* worker -> walsender */
	PD_ENT_FLUSH = 18,			/* walreader -> worker, worker -> walsender */
	PD_ENT_READ_POS = 19		/* walreader -> walsender */
} ParallelDecodeEntryType;

/*
 * The header of an error report.
 *
 * A participant that fails reports the error to the walsender, so that the
 * session ends with a message that says what went wrong instead of the
 * generic "the participant died unexpectedly".  The text of the error is in
 * the control block, so that it is still there when the participant is gone;
 * the message itself only says who failed.
 */
typedef struct PDErrorHeader
{
	int32		worker;			/* the worker that failed, -1 for the walreader */
	uint64		lsn;			/* the decoding position when the error happened */
} PDErrorHeader;

/*
 * The information about a transaction that has ended.
 *
 * The walreader is the only reader of the WAL, so it is the one that knows
 * where the record that ended the transaction is.  The walsender needs that
 * position to compute the release frontier, which is the limit the LSNs of
 * the frames it writes to the subscriber may not exceed (see the design
 * document, 7.4).
 *
 * end_lsn is the *start* of the record that ended the transaction, which is
 * the position the decoding path compares against the flush position of the
 * slot when it decides whether a transaction has to be skipped.  Using the
 * end of the record instead would let the flush position land inside the
 * record, which would make the subscriber skip the transaction.
 */
typedef struct PDCseqInfo
{
	uint64		cseq;				/* the commit sequence number */
	uint64		end_lsn;			/* the start of the record that ended the
									 * transaction */
	uint64		read_lsn;			/* the position the walreader has read up
									 * to */
	int32		owner;				/* the worker that decoded the transaction */
} PDCseqInfo;

/*
 * The header of a stream segment.
 *
 * A stream segment is the sequence of the frames of one part of a
 * transaction that is streamed before the transaction is complete: a STREAM
 * START message, the changes and a STREAM STOP message.
 *
 * The frames of a segment have to reach the subscriber without the bytes of
 * another transaction in between, because the subscriber accepts only one
 * open stream segment at a time.  The worker therefore hands the frames of a
 * segment over as one message, and the walsender writes them out one after
 * the other.
 */
typedef struct PDSegmentHeader
{
	uint64		end_lsn;			/* the decoding position after the segment */
	uint32		nframes;			/* the number of frames in the segment */
} PDSegmentHeader;

/*
 * The header that precedes every WAL record on the channel between the
 * walreader and the decode workers.
 *
 * The walreader is the only reader of the WAL, so it is the one that knows
 * which worker owns the transaction a record belongs to, and which commit
 * sequence number it gave to the record.
 */
typedef struct PDWalHeader
{
	/*
	 * The worker that owns the transaction of the record, or -1 when the
	 * record does not belong to a transaction.
	 *
	 * The workers that do not own the transaction see its records as well,
	 * so that their snapshot builders stay in step with the one of the owner,
	 * but they do not accumulate the changes: the owner is the one that
	 * produces the output of the transaction.
	 */
	int32		owner;

	/*
	 * The commit sequence number of the record, or zero when the record does
	 * not end a transaction.
	 */
	uint64		cseq;
} PDWalHeader;

/*
 * The flags of a snapshot message.
 *
 * A snapshot message tells the decode workers about the state of the snapshot
 * builder and about the snapshots the transactions are decoded with.  The
 * walreader is the only process that drives the builder, so it is the only one
 * that knows which snapshot a transaction is decoded with.
 */
#define PD_SNAPSHOT_STATE		0x0001	/* the state of the builder */
#define PD_SNAPSHOT_CONTENT	0x0002	/* the content of the snapshot follows */
#define PD_SNAPSHOT_BASE		0x0004	/* the base snapshot of a transaction */
#define PD_SNAPSHOT_SWITCH		0x0008	/* the running transactions switch over */
#define PD_SNAPSHOT_INVAL		0x0010	/* invalidation messages follow */

/*
 * The header of a snapshot message.
 *
 * The message carries the parts of the state of the snapshot builder that the
 * decode workers need, and the events that change the snapshot the running
 * transactions are decoded with.
 *
 * The layout of the payload depends on the flags:
 *
 *   PD_SNAPSHOT_CONTENT	the content of the snapshot follows the header;
 *   PD_SNAPSHOT_BASE		the base snapshot of the transaction xid is the
 *				snapshot the message carries the version of;
 *   PD_SNAPSHOT_SWITCH	the transactions that are in flight switch over to
 *				the snapshot of this message;
 *   PD_SNAPSHOT_INVAL		the invalidation messages follow the payload.
 *
 * A message that carries the content of a snapshot (PD_SNAPSHOT_CONTENT) is
 * sent to every worker: every worker holds transactions that may have to
 * switch over to the snapshot.  A message that assigns the base snapshot of a
 * transaction (PD_SNAPSHOT_BASE) is sent to the worker that owns the
 * transaction only.
 */
typedef struct PDSnapshotHeader
{
	uint64		version;		/* the version of the snapshot */
	uint64		epoch;			/* the invalidation epoch of the session */
	uint32		flags;			/* PD_SNAPSHOT_* */
	uint32		ninvalidations;	/* the number of invalidation messages that
								 * follow the payload */
	int32		state;			/* the state of the snapshot builder */
	XLogRecPtr	lsn;			/* the position the event happened at */
	XLogRecPtr	start_decoding_at;	/* the position the decoding starts at */
	TransactionId xid;		/* the transaction the event belongs to */
	TransactionId next_phase_at;	/* the transaction the next phase of the
									 * builder starts at */
} PDSnapshotHeader;

/*
 * The content of a snapshot.
 *
 * The array of the committed transactions follows the header.  It holds the
 * transactions that are treated as committed by the snapshot, which is what
 * makes the catalog rows they wrote visible to the decoding.
 */
typedef struct PDSnapshotContent
{
	TransactionId xmin;
	TransactionId xmax;
	CommandId	curcid;
	uint32		xcnt;
	/* the array of the committed transactions follows */
} PDSnapshotContent;

/*
 * The header of a message that carries invalidation messages.
 *
 * The messages follow the header.  They are the invalidation messages of a
 * record that has no transaction, which have to be executed by every worker:
 * the catalog caches of the workers are private to their process, so the
 * invalidation has to reach all of them.
 */
typedef struct PDInvalHeader
{
	uint64		epoch;			/* the invalidation epoch of the session */
	uint32		ninvalidations;
	/* the invalidation messages follow */
} PDInvalHeader;

/*
 * The watermarks of the session.
 *
 * The slot of the session has two watermarks that have to be computed from
 * the state of the transactions that are in flight: the xmin of the catalog
 * and the position the decoding may restart from.
 *
 * The walreader is the only process that sees every transaction of the
 * session, because it is the one that binds the transactions to the decode
 * workers.  It therefore computes the watermarks, in exactly the way the
 * serial path computes them, and hands them over to the walsender, which is
 * the process that owns the slot and may advance its watermarks.
 */
typedef struct PDWatermark
{
	/*
	 * The position the decoding may restart from, or zero when there is none
	 * yet.  It is the restart point of the oldest transaction that is still in
	 * flight: the decoding has to start there, so that the changes of that
	 * transaction are not lost.
	 */
	uint64		restart_lsn;

	/*
	 * The xmin of the catalog: the xmin of the snapshot of the oldest
	 * transaction that is still in flight.
	 */
	TransactionId xmin;

	/*
	 * The oldest transaction id that is running, as the running xacts record
	 * reported it.  It is the fallback of the xmin of the catalog, for the
	 * case that no transaction holds a base snapshot.
	 */
	TransactionId oldest_running;

	/*
	 * The position the watermarks are valid at.  It is the position of the
	 * running xacts record the values were computed from, and it is the
	 * position the subscriber has to confirm before the watermarks of the
	 * slot may be advanced.
	 */
	uint64		read_lsn;
} PDWatermark;

/*
 * The header of a frame message.
 *
 * The frame is the 'w' frame of the replication protocol, byte for byte the
 * frame the serial path produces: the frame type, the two LSNs, the send
 * time and the payload of the output plugin.  The send time is filled in by
 * the walsender when it writes the frame out, so that it is taken as late as
 * possible.
 *
 * end_lsn is the decoding position after the record the frame belongs to.  It
 * tells the walsender how far the decoding has come, which is what the
 * keepalive frames and the caught-up decision are based on.
 *
 * cseq is the commit sequence number of the transaction the frame belongs to.
 * It is zero for the frames that are not part of a transaction, i.e. for the
 * fragments of a streamed transaction.  The walsender holds the frames of a
 * transaction back until the commit sequence number of the transaction has
 * its turn, so that the transactions are applied in the order they were
 * committed in.
 */
typedef struct PDFrameHeader
{
	uint64		end_lsn;			/* decoding position after the record */
	uint64		cseq;				/* commit sequence number of the transaction */
	uint32		length;			/* length of the frame that follows */
} PDFrameHeader;

/*
 * The header of a frame that carries the definition of a relation.
 *
 * The decode workers each have their own cache of the relations they have
 * sent, so every worker that touches a relation produces its definition.  The
 * walsender is the one that knows in which order the transactions reach the
 * subscriber, so it is the one that decides which of the definitions are
 * needed: the definition of a relation has to reach the subscriber before the
 * first change of the relation, and the transactions reach the subscriber in
 * the order of their commit sequence numbers.
 *
 * The frame is sent as a message of its own type, so that the frames that do
 * not carry a definition do not have to carry the information about it.
 */
typedef struct PDRelationHeader
{
	uint64		end_lsn;			/* decoding position after the record */
	uint64		cseq;				/* commit sequence number of the transaction */
	uint64		epoch;				/* the epoch of the session the frame belongs to */
	uint32		relid;				/* the relation the definition belongs to */
	uint32		length;			/* length of the frame that follows */
} PDRelationHeader;

/*
 * The events a decode worker reports to the walsender.
 */
#define PD_EVENT_END_ACK		1	/* the transaction with this cseq is decoded */

/*
 * The header of an event message.
 */
typedef struct PDEventHeader
{
	uint64		cseq;				/* the commit sequence number */
	uint32		event;				/* PD_EVENT_* */
} PDEventHeader;

#define PD_MESSAGE_MAGIC			0x50444D31	/* "PDM1" */

/* The largest payload a single entry may carry. */
#define PD_MESSAGE_MAX_PAYLOAD		(1024 * 1024)

/*
 * The largest total size of a chunked transfer.
 *
 * A chunked transfer carries a WAL record or a replication frame, and both
 * of them are held in memory in the process that produces them anyway, so
 * the limit is generous.  It only exists to turn a runaway producer into an
 * error instead of an out-of-memory condition.
 */
#define PD_MAX_TRANSFER_SIZE		(512 * 1024 * 1024)

typedef struct ParallelDecodeMessageHeader
{
	uint32		magic;
	uint16		type;
	uint16		flags;
	uint32		length;
} ParallelDecodeMessageHeader;

/*
 * Send one entry.  Returns false when the queue is full and nowait was
 * requested, which is the backpressure signal of the design (5.5).
 *
 * force_flush makes the entry visible to the receiver right away.  The queue
 * only publishes what the sender has written when the sender asks for it, so
 * a sender that never flushes leaves the receiver waiting for data that is
 * already in the ring buffer.  The control channel always flushes; the data
 * channels may batch.
 */
extern bool PDQueueSend(shm_mq_handle *mqh, uint16 type, const void *payload,
						size_t len, bool nowait, bool force_flush);

/*
 * Register the counter of the wakeups of a queue.
 *
 * The queues that carry the payload of the session have a sender of their own
 * (PDQueueSender), which counts the wakeups it causes.  A queue that is
 * written with PDQueueSend() has no sender, so its counter is registered
 * here.  The counter counts the times the receiver of the queue is woken up.
 */
extern void PDQueueSetWakeupCounter(shm_mq_handle *mqh,
									volatile uint64 *wakeups);

/*
 * Did the last send fail because the queue was detached?
 *
 * A queue is detached when the process on the other end has gone away, or
 * when the shared segment of the session is going away.  During the teardown
 * of a session that is the normal way for a participant to learn that it has
 * to stop, so the participants use this flag to tell the end of the session
 * apart from a failure of the decoding: the error that the send raises is
 * not a failure of the participant, it is the end of the session.
 *
 * The flag is set by the send functions and it is never reset: a participant
 * that has seen a detached queue stops anyway.
 */
extern bool PDQueueDetached(void);

/*
 * Receive one entry.  The payload points into the ring buffer of the queue
 * and is valid only until the next receive from the same queue.  Returns
 * SHM_MQ_WOULD_BLOCK if the queue is empty and nowait was requested.
 *
 * All messages of a queue use the same framing, because a receiver uses a
 * single chunk receiver for the whole queue: a message that is sent with
 * PDQueueSend() would be mistaken for the beginning of a chunked transfer.
 */
extern shm_mq_result PDQueueRecv(shm_mq_handle *mqh, uint16 *type,
									 const void **payload, size_t *len, bool nowait);

/*
 * Transfer of a payload that does not fit into a single queue entry.
 *
 * The payload is split into chunks of at most chunk_size bytes.  The first
 * chunk carries the total length of the payload, the following ones carry
 * plain data.  The receiver keeps the partial payload in a PDChunkReceiver,
 * so it can pick the transfer up again after it has waited for more data.
 */
typedef struct PDChunkReceiver
{
	char	   *buffer;		/* the payload accumulated so far */
	size_t		len;			/* bytes accumulated so far */
	size_t		expected;		/* total length of the payload */
	bool		started;		/* a transfer is in progress */
} PDChunkReceiver;

extern void PDChunkReceiverInit(PDChunkReceiver *rcv);
extern void PDChunkReceiverReset(PDChunkReceiver *rcv);
extern shm_mq_result PDChunkReceiverRecv(PDChunkReceiver *rcv,
										 shm_mq_handle *mqh, uint16 *type,
										 const void **payload, size_t *len,
										 bool nowait);
extern bool PDQueueSendChunked(shm_mq_handle *mqh, uint16 type,
							   const void *payload, size_t len, size_t chunk_size,
							   bool nowait, bool force_flush);

/*
 * The sender of a queue that batches its messages.
 *
 * The queue publishes what the sender has written only when the sender asks
 * for it, so a sender that batches its messages has to keep track of the
 * bytes it has written since the last flush: the count decides when the
 * receiver has to be woken up.
 *
 * Every queue has exactly one sender, so the counter needs no lock.
 */
typedef struct PDQueueSender
{
	shm_mq_handle *mqh;		/* the queue */
	size_t		pending;		/* bytes written since the last flush */
	volatile uint64 *wakeups;	/* the counter of the flushes, may be NULL */
} PDQueueSender;

/* The batch size that is in effect, in bytes.  Zero means "no batching". */
extern size_t PDQueueBatchSize(void);

extern void PDQueueSenderInit(PDQueueSender *sender, shm_mq_handle *mqh,
							   volatile uint64 *wakeups);

/*
 * Write a chunked message to the queue of the sender, without batching it.
 *
 * The message is flushed on its own, so the receiver sees it right away.  The
 * messages that carry the progress of the session are written this way.
 */
extern bool PDQueueSendUnbatched(PDQueueSender *sender, uint16 type,
								 const void *payload, size_t len);

/*
 * Publish the data that was written to the queue but not flushed yet.
 *
 * The queue has no notify call of its own, so the flush is a message of its
 * own: an empty message that the receiver ignores.  The sender calls this
 * before it goes to sleep, so that the receiver does not wait for data that
 * is already in the ring buffer.
 */
extern void PDQueueSenderFlush(PDQueueSender *sender);

/*
 * Send a replication frame to the walsender.
 *
 * The frame is preceded by a PDFrameHeader and split into chunks, because a
 * single transaction can produce more data than fits into one queue entry.
 *
 * cseq is the commit sequence number of the transaction the frame belongs to.
 * It is zero for the frames that are not part of a transaction.
 */
extern void PDQueueSendFrame(PDQueueSender *sender, uint64 end_lsn, uint64 cseq,
							  const char *frame, size_t len);

/*
 * Send a frame that carries the definition of a relation to the walsender.
 *
 * The frame is preceded by a PDRelationHeader, which tells the walsender
 * which relation the definition belongs to and in which epoch of the session
 * it was produced.  The walsender uses that information to send the
 * definition of a relation to the subscriber once.
 */
extern void PDQueueSendRelation(PDQueueSender *sender, uint64 end_lsn,
								uint64 cseq, uint64 epoch, Oid relid,
								const char *frame, size_t len);

/*
 * Send a stream segment to the walsender.
 *
 * The frames of the segment follow each other, each of them preceded by its
 * length, so that the walsender can find the boundaries of the frames.
 */
extern void PDQueueSendSegment(PDQueueSender *sender, uint64 end_lsn,
							   uint32 nframes, const char *payload, size_t len);

/*
 * Report an event to the walsender.
 *
 * The message uses the chunked framing, because the receiver of a queue uses
 * a single chunk receiver for all of its messages.
 */
extern void PDQueueSendEvent(PDQueueSender *sender, uint64 cseq, uint32 event);

/*
 * Report the decoding position to the walsender.
 *
 * The position tells the walsender how far the decoding has come, even when
 * the records that were decoded produced no output at all.  The message uses
 * the chunked framing, because the receiver of a queue uses a single chunk
 * receiver for all of its messages.
 */
extern void PDQueueSendPosition(PDQueueSender *sender, uint64 end_lsn);

/*
 * The output plugin options, in the serialized form.
 *
 * The options are a list of DefElem, and every option of the plugins that
 * ship with PostgreSQL carries a plain string argument.  The serialized form
 * is therefore a list of length prefixed names and arguments.
 */
extern void PDSerializePluginOptions(List *options, char *buf, size_t buflen,
									  uint32 *len);
extern List *PDParsePluginOptions(const char *buf, uint32 len);

/*
 * Serialization of a decoded WAL record.
 *
 * The walreader reads the WAL and hands the records to the decoding side as
 * serialized copies of the decoded record, so that the decoding side does not
 * have to read the WAL itself.  Only the fields the logical decoding path
 * looks at are transferred: the record header, the main data and the block
 * references with their data.
 */
#define PD_RECORD_FORMAT_VERSION	1

extern void PDRecordSerialize(DecodedXLogRecord *record, StringInfo buf);
extern DecodedXLogRecord *PDRecordDeserialize(const char *data, size_t len);
extern void PDRecordFree(DecodedXLogRecord *record);

/*
 * Waiting for the next frame on the walsender side.
 *
 * The decode workers produce the replication frames, so the walsender has to
 * wait for them to arrive.  While it waits, it still has to service the
 * replication connection, which is what the idle callback is for.
 *
 * The callback returns true when the session has to end, which happens when
 * the client has asked to stop the streaming.  The walsender then returns to
 * its main loop, which ends the session.
 */
typedef bool (*ParallelDecodeIdleCallback) (void);

extern void ParallelDecodeSetIdleCallback(ParallelDecodeIdleCallback callback);

/*
 * A frame of the replication protocol.
 *
 * The frame is the 'w' frame of the replication protocol, byte for byte the
 * frame the serial path produces.  The bytes point into the block the frame
 * was taken from, so that the frame does not have to be copied out of it.
 *
 * The bytes belong to the block, so they must not be used after the next call
 * of ParallelDecodeNextFrame() or ParallelDecodeNextFrameNoWait(): the block
 * is released once its last frame has been handed out.
 */
typedef struct PDFrame
{
	char	   *data;			/* the bytes of the frame */
	size_t		len;			/* the number of bytes in data */
	XLogRecPtr	end_lsn;		/* the decoding position after the record */
} PDFrame;

/*
 * Wait for the next frame the decode worker produced.
 *
 * The frame is the 'w' frame of the replication protocol, byte for byte the
 * frame the serial path produces.  Its bytes point into the block it was
 * taken from, so that the caller can patch the send time into it and write
 * it out without copying it first.
 *
 * Returns false when no frame has arrived within the wait interval, which
 * is not an error: the caller is expected to return to its main loop and
 * come back later.  Returns false as well when the session is winding down.
 */
extern bool ParallelDecodeNextFrame(PDFrame *frame);

/*
 * Take the next frame that is ready to be sent, without waiting for one.
 *
 * The frame is handed out in the same way ParallelDecodeNextFrame() does it.
 * The function returns false when there is no frame at the moment, which is
 * not an error: the caller simply has nothing to write.
 *
 * The function is used to fill a batch of frames.  The first frame of a batch
 * is waited for, the frames that follow it are taken as they are there, so
 * that the batch can be written out to the client in one go.
 */
extern bool ParallelDecodeNextFrameNoWait(PDFrame *frame);

/*
 * The WAL position the decoding has come to.
 *
 * The position advances with the frames the worker produces and with the
 * position messages it sends when it runs out of work, so that records that
 * produce no output at all still advance it.
 */
extern XLogRecPtr ParallelDecodePosition(void);

/*
 * The WAL position the walreader has reached, or InvalidXLogRecPtr if the
 * walreader has not reported one yet.
 */
extern XLogRecPtr ParallelDecodeWalreaderPosition(void);

/*
 * The release frontier of the session.
 *
 * The frontier is the position of the record that ended the first transaction
 * that has not been released to the subscriber yet.  No frame the walsender
 * writes may carry a position that is beyond the frontier: the subscriber
 * uses the position of the frames it receives to advance the flush position
 * of the slot, and a position that is beyond the frontier would make the
 * subscriber skip the transactions that have not been delivered yet.
 *
 * The frontier is InvalidXLogRecPtr when the session does not know it yet.
 */
extern XLogRecPtr ParallelDecodeFrontier(void);

/*
 * Limit a position to the release frontier.
 *
 * This is used for the position of the keepalive frames, which the subscriber
 * folds into its flush position just like the position of a data frame.
 */
extern XLogRecPtr ParallelDecodeClampLSN(XLogRecPtr lsn);

/*
 * Limit the positions of a frame to the release frontier.
 *
 * The frame is the 'w' frame of the replication protocol, whose header holds
 * two positions.  The positions of the frames of a transaction that has been
 * released are always within the frontier, so this only ever changes the
 * frames of a stream segment.
 *
 * The frame is patched in place, so the bytes have to be writable.
 */
extern void ParallelDecodeClampFrame(char *data, size_t len);

/*
 * The 'prepare_write' callback of the logical decoding context.
 *
 * It builds the header of a replication frame.  The serial path and the
 * parallel path use the same code, so that the frames they produce are
 * identical.
 */
extern void PDPrepareWrite(LogicalDecodingContext *ctx, XLogRecPtr lsn,
						   TransactionId xid, bool last_write);

#endif							/* PARALLEL_DECODE_H */
