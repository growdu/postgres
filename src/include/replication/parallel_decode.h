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

/* True when the publisher-side parallel path is requested. */
extern bool ParallelDecodeEnabled(void);

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
														 int *slot_index);
extern shm_mq *ParallelDecodeLookupMq(shm_toc *toc, int key);

#endif							/* PARALLEL_DECODE_H */
