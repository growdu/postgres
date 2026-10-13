/*-------------------------------------------------------------------------
 *
 * test_parallel_decode.c
 *		SQL-callable test functions for the parallel decode infrastructure.
 *
 * Every function here maps to a public entry point in paralleldecode.c,
 * ensuring all stage-1 code paths are exercised through SQL.
 *
 * Portions Copyright (c) 2024-2025, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		src/test/modules/test_parallel_decode/test_parallel_decode.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "replication/paralleldecode.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

/* Pool handle persists across calls within this backend session. */
static ParallelDecodePool *current_pool = NULL;

PG_FUNCTION_INFO_V1(pd_create_pool);
PG_FUNCTION_INFO_V1(pd_destroy_pool);
PG_FUNCTION_INFO_V1(pd_echo);
PG_FUNCTION_INFO_V1(pd_pool_states);
PG_FUNCTION_INFO_V1(pd_pool_health);
PG_FUNCTION_INFO_V1(pd_test_error);

/*
 * pd_create_pool(nworkers) — create a parallel decode worker pool.
 */
Datum
pd_create_pool(PG_FUNCTION_ARGS)
{
	int			nworkers = PG_GETARG_INT32(0);

	if (current_pool != NULL)
		ParallelDecodePoolDestroy(current_pool);

	current_pool = ParallelDecodePoolCreate(nworkers);

	PG_RETURN_VOID();
}

/*
 * pd_destroy_pool() — destroy the current pool.
 */
Datum
pd_destroy_pool(PG_FUNCTION_ARGS)
{
	if (current_pool != NULL)
	{
		ParallelDecodePoolDestroy(current_pool);
		current_pool = NULL;
	}

	PG_RETURN_VOID();
}

/*
 * pd_echo(worker_id, payload) — send payload to worker, get it back.
 */
Datum
pd_echo(PG_FUNCTION_ARGS)
{
	int			worker_id = PG_GETARG_INT32(0);
	text	   *payload_text = PG_GETARG_TEXT_PP(1);
	const char *payload = VARDATA_ANY(payload_text);
	Size		payload_len = VARSIZE_ANY_EXHDR(payload_text);
	char	   *reply;
	Size		reply_len;
	text	   *result;

	reply = ParallelDecodePoolEcho(current_pool, worker_id,
									payload, payload_len, &reply_len);

	result = cstring_to_text_with_len(reply, reply_len);
	pfree(reply);

	PG_RETURN_TEXT_P(result);
}

/*
 * pd_pool_states() — return worker_id and state for each worker.
 */
Datum
pd_pool_states(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	int			call_cntr;
	int			max_calls;
	uint32	   *states;
	int			nworkers;
	TupleDesc	tupdesc;
	AttInMetadata *attinmeta;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		if (current_pool == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("parallel decode pool is not created")));

		nworkers = ParallelDecodePoolGetNWorkers(current_pool);
		states = (uint32 *) palloc(sizeof(uint32) * nworkers);
		ParallelDecodePoolGetWorkerStates(current_pool, states, nworkers);

		funcctx->user_fctx = states;
		funcctx->max_calls = nworkers;

		if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("function returning record called in context that cannot accept type record")));

		funcctx->attinmeta = TupleDescGetAttInMetadata(tupdesc);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();
	call_cntr = funcctx->call_cntr;
	max_calls = funcctx->max_calls;
	states = (uint32 *) funcctx->user_fctx;
	attinmeta = funcctx->attinmeta;

	if (call_cntr < max_calls)
	{
		char	   *values[2];
		HeapTuple	tuple;
		char		id_str[16];
		char		state_str[16];

		snprintf(id_str, sizeof(id_str), "%d", call_cntr);
		strlcpy(state_str, ParallelDecodeWorkerStateName(states[call_cntr]),
				sizeof(state_str));

		values[0] = id_str;
		values[1] = state_str;

		tuple = BuildTupleFromCStrings(attinmeta, values);

		SRF_RETURN_NEXT(funcctx, HeapTupleGetDatum(tuple));
	}
	else
	{
		pfree(states);
		SRF_RETURN_DONE(funcctx);
	}
}

/*
 * pd_pool_health() — check all workers are alive and heartbeating.
 */
Datum
pd_pool_health(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(ParallelDecodePoolCheckHealth(current_pool));
}

/*
 * pd_test_error(worker_id) — ask a worker to raise an error; the leader
 * should receive it through the error channel and re-raise.
 */
Datum
pd_test_error(PG_FUNCTION_ARGS)
{
	int			worker_id = PG_GETARG_INT32(0);

	/*
	 * This function always errors out — either from the worker's error
	 * message or from the pool detecting the worker died.
	 */
	ParallelDecodePoolTestError(current_pool, worker_id);

	/* Should not reach here. */
	PG_RETURN_VOID();
}
