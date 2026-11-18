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

#include "storage/shm_mq.h"
#include "storage/latch.h"
#include "storage/shm_toc.h"

/*
 * Upper bound on the number of decode workers in one session.  The bound is
 * compile-time because the shared control block is a fixed-size structure.
 */
#define MAX_PARALLEL_DECODE_WORKERS		64

/*
 * Streaming aggressiveness of the publisher.
 *
 * AUTO resolves per subscriber at session start: a subscriber using
 * streaming='parallel' gets IMMEDIATE (its parallel apply only makes sense
 * when the publisher actually produces stream segments), a subscriber using
 * streaming='on' gets BUFFERED (it has no parallel apply worker, so forcing
 * streaming would push every one of its transactions onto the subscriber's
 * spool slow path).
 */
typedef enum ParallelDecodeStreamingMode
{
	PARALLEL_DECODE_STREAMING_AUTO = 0,
	PARALLEL_DECODE_STREAMING_IMMEDIATE,
	PARALLEL_DECODE_STREAMING_BUFFERED
} ParallelDecodeStreamingMode;

/* GUCs */
extern PGDLLIMPORT int	max_parallel_decode_workers;
extern PGDLLIMPORT int	parallel_decode_streaming_mode;
extern PGDLLIMPORT int	parallel_decode_stream_min_size;
extern PGDLLIMPORT int	parallel_decode_stream_segment_size;
extern PGDLLIMPORT int	parallel_decode_work_mem;
extern PGDLLIMPORT int	parallel_decode_batch_size;
extern PGDLLIMPORT char *parallel_decode_trace_file;

/* True when the publisher-side parallel path is requested. */
extern bool ParallelDecodeEnabled(void);

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
 */
extern void ParallelDecodeSessionStart(void);
extern void ParallelDecodeSessionStop(void);

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

typedef struct ParallelDecodeWorkerSlot
{
	pid_t		pid;			/* 0 until the worker has reported its pid */
	int			status;		/* PD_WORKER_STATUS_* */
	int			slot_index;	/* index of this worker within the session */
	uint64		error_lsn;		/* WAL position the failure was detected at */
	char		errmsg[512];	/* error text reported by the worker */
} ParallelDecodeWorkerSlot;

typedef struct ParallelDecodeControl
{
	pid_t		walsender_pid;
	pid_t		walreader_pid;
	int			num_workers;
	int			walreader_status;	/* PD_WORKER_STATUS_* */

	/* Set by the walsender once the slot and the plugin are known. */
	int			streaming_mode;	/* ParallelDecodeStreamingMode */
	int			proto_version;
	char		plugin_name[NAMEDATALEN];

	/* Worker status slots. */
	ParallelDecodeWorkerSlot workers[MAX_PARALLEL_DECODE_WORKERS];

	/* Set when any participant has failed; the session must be torn down. */
	volatile uint32 failed;

	/* Set by the walsender to ask all participants to wind down. */
	volatile uint32 shutdown;
} ParallelDecodeControl;

/* Payload handed to the dynamic background workers through bgw_extra. */
typedef struct ParallelDecodeWorkerArg
{
	int			dsm_handle;
	int			slot_index;		/* -1 for the walreader */
	Oid			db_oid;			/* database the output plugin works on */
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
	PD_ENT_ERROR = 9,			/* worker -> walsender */
	PD_ENT_READY = 10			/* walreader -> walsender */
} ParallelDecodeEntryType;

#define PD_MESSAGE_MAGIC			0x50444D31	/* "PDM1" */

/* The largest payload a single entry may carry. */
#define PD_MESSAGE_MAX_PAYLOAD		(1024 * 1024)

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
 * Receive one entry.  The payload points into the ring buffer of the queue
 * and is valid only until the next receive from the same queue.  Returns
 * SHM_MQ_WOULD_BLOCK if the queue is empty and nowait was requested.
 */
extern shm_mq_result PDQueueRecv(shm_mq_handle *mqh, uint16 *type,
									 const void **payload, size_t *len, bool nowait);

#endif							/* PARALLEL_DECODE_H */
