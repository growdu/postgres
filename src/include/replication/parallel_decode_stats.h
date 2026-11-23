/*-------------------------------------------------------------------------
 *
 * parallel_decode_stats.h
 *		Shared declarations for the statistics of publisher-side parallel
 *		logical decoding.
 *
 * A parallel decoding session publishes its statistics in the shared memory
 * of the walsenders, so that a session that monitors the publisher can see
 * what the session is doing.  The statistics are the counters of the
 * monitoring views (design document, 11.3).
 *
 * This header is deliberately small and free of dependencies, so that it can
 * be included by the walsender and by the SQL functions that read the
 * statistics.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PARALLEL_DECODE_STATS_H
#define PARALLEL_DECODE_STATS_H

#include "postgres.h"

#include "datatype/timestamp.h"

/*
 * Upper bound on the number of decode workers in one session.  The bound is
 * compile-time because the shared control block is a fixed-size structure.
 */
#define MAX_PARALLEL_DECODE_WORKERS		64

/*
 * The counters of one decode worker, as they are published for the
 * monitoring views.
 *
 * They are a copy of the counters of the worker slot of the control block,
 * taken by the walsender when it publishes the statistics of the session.
 */
typedef struct ParallelDecodeWorkerStats
{
	pid_t		pid;			/* the pid of the worker, 0 when it has none */
	int			status;		/* PD_WORKER_STATUS_* */
	uint64		in_flight_txns;	/* transactions the worker is decoding */
	uint64		decoded_txns;	/* transactions the worker has finished */
	uint64		in_flight_bytes;	/* bytes the transactions have produced */
	uint64		queue_depth;	/* records handed over but not decoded yet */
	uint64		wakeups;		/* signals the worker sent to the walsender */
} ParallelDecodeWorkerStats;

/*
 * The statistics of a parallel decoding session, as they are published for
 * the monitoring views.
 *
 * The session fills them in and the SQL functions read them.  They live in
 * the shared memory of the walsenders, so that the SQL functions do not need
 * access to the dynamic shared segment of the session.  The structure is
 * fixed-size, so every walsender slot carries it whether the session uses
 * the parallel path or not.
 */
typedef struct ParallelDecodeStats
{
	bool		active;		/* the session uses the parallel path */
	int			num_workers;
	TimestampTz start_time;

	/*
	 * The phase numbers of the commit_sync barrier.  The difference between
	 * them is the backlog of the ordering: the number of transactions that
	 * have ended but whose end event has not been sent to the subscriber yet.
	 */
	uint64		cseq_allocated;
	uint64		cseq_released;

	/*
	 * The age of the transaction that has been waiting for its turn the
	 * longest, in microseconds.  It is zero when no transaction is waiting.
	 */
	uint64		pending_commit_age_us;

	/* The transactions the session has decoded, and how long they took. */
	uint64		txn_count;
	uint64		txn_latency_us_sum;
	uint64		txn_latency_us_max;

	/* The output of the session. */
	uint64		frames;			/* frames written to the subscriber */
	uint64		frame_bytes;	/* bytes written to the subscriber */
	uint64		socket_writes;	/* write calls on the replication socket */

	/* The signals between the participants of the session. */
	uint64		wakeups_down;	/* walreader -> workers */
	uint64		wakeups_up;		/* workers -> walsender */

	/* The counters of the workers of the session. */
	ParallelDecodeWorkerStats workers[MAX_PARALLEL_DECODE_WORKERS];
} ParallelDecodeStats;

#endif							/* PARALLEL_DECODE_STATS_H */
