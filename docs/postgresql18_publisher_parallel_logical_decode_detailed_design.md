# [RFC-Detail] Publisher-Side Parallel Logical Decoding — Detailed Design

## Summary

Coding-level specification for implementing publisher-side parallel logical decoding in PostgreSQL 18. Covers file layout, DSM/message wire formats, function signatures, startup sequences, main loops, concurrency rules, error handling, and build integration. All function names and API signatures are verified against the PG 18 source tree.

## Motivation

The companion RFC (`postgresql18_publisher_parallel_logical_decode_overview.md`) defines the architecture and rationale. This document provides the implementation-level detail needed to write code: every structure, message format, and function call sequence.

## Detailed Design

### API

#### New Files

| File | Content |
|---|---|
| `src/include/replication/paralleldecode.h` | Shared structures, DSM keys, message contract, pool API |
| `src/backend/replication/logical/paralleldecode.c` | Leader side: pool lifecycle, record dispatch, event publication, result replay |
| `src/backend/replication/logical/paralleldecodeworker.c` | Worker side: startup, decode pipeline, event consumption, heartbeat |
| `src/backend/replication/logical/paralleldecode_walsender.c` | Walsender integration entry points |
| `src/test/modules/test_parallel_decode/` | SQL-callable test functions |

#### Modified Files

| File | Change |
|---|---|
| `decode.c` | Dispatch hook in `LogicalDecodingProcessRecord`; COMMIT/ABORT routing; RUNNING_XACTS snapshot publication; non-transactional message ordering |
| `walsender.c` | Pool creation with eligibility check; result collection in `XLogSendLogical`; pool destruction |
| `logical.h` | `parallel_pool` field on `LogicalDecodingContext` |
| `logical.c` | `parallel_pool = NULL` initialization |
| `bgworker.c` | `InternalBGWorkers` registration |
| `guc_tables.c` | Four GUC entries |
| `Makefile`/`meson.build` | Object file additions |

#### Public Function Signatures

```c
/* Pool lifecycle */
ParallelDecodePool *ParallelDecodePoolCreate(int nworkers);
void ParallelDecodePoolDestroy(ParallelDecodePool *pool);
bool ParallelDecodePoolCheckHealth(ParallelDecodePool *pool);
int ParallelDecodePoolGetWorkerStates(ParallelDecodePool *pool,
                                      uint32 *states, int nstates);
int ParallelDecodePoolGetNWorkers(ParallelDecodePool *pool);

/* Record dispatch (called from decode.c) */
void ParallelDecodeDispatchRecord(ParallelDecodePool *pool,
                                  XLogReaderState *record,
                                  TransactionId top_xid,
                                  TransactionId xid);
void ParallelDecodeDispatchCommit(ParallelDecodePool *pool,
                                  TransactionId top_xid,
                                  XLogRecPtr commit_lsn,
                                  XLogRecPtr end_lsn,
                                  TimestampTz commit_time,
                                  RepOriginId origin_id,
                                  XLogRecPtr origin_lsn);
void ParallelDecodeDispatchAbort(ParallelDecodePool *pool,
                                 TransactionId top_xid,
                                 XLogRecPtr abort_lsn);

/* Event publication */
void ParallelDecodePublishSnapshot(ParallelDecodePool *pool,
                                   SnapBuild *builder,
                                   XLogRecPtr lsn);
void ParallelDecodePublishInvalidations(ParallelDecodePool *pool,
                                        uint32 nmsgs,
                                        SharedInvalidationMessage *msgs,
                                        XLogRecPtr commit_lsn);

/* Result handling */
void ParallelDecodeCollectResults(ParallelDecodePool *pool);
void ParallelDecodeOutputPending(ParallelDecodePool *pool,
                                 LogicalDecodingContext *ctx);
void ParallelDecodeAdvanceSlotPositions(ParallelDecodePool *pool);

/* Statistics */
void ParallelDecodePoolGetMetrics(ParallelDecodePool *pool,
                                  ParallelDecodePoolMetrics *metrics);

/* Worker entry point */
void ParallelDecodeWorkerMain(Datum main_arg);
```

### Implementation

#### DSM Layout (per Worker)

Seven `shm_toc` entries in each Worker's DSM segment:

| Key | Content | Default Size |
|---|---|---|
| 1 | `ParallelDecodeWorkerInfo` | sizeof |
| 2 | Task MQ (walsender → Worker) | 512 kB |
| 3 | Event MQ (walsender → Worker) | 256 kB |
| 4 | Result MQ (Worker → walsender) | 1 MB |
| 5 | Error MQ (Worker → walsender) | 16 kB |
| 6 | Control Request MQ (Worker → walsender) | 8 kB |
| 7 | Control Command MQ (walsender → Worker) | 8 kB |

#### Message Wire Contract

All messages begin with `PdMsgHeader`:

```c
typedef struct PdMsgHeader {
    uint32 len;   /* includes header */
    uint8  type;  /* PdMsgType */
} PdMsgHeader;
```

| Type | Direction | Payload |
|---|---|---|
| `PD_MSG_TASK_RECORD` | walsender → Worker | top_xid, xid, LSNs, raw XLogRecord bytes |
| `PD_MSG_TASK_COMMIT` | walsender → Worker | top_xid, commit_lsn, snapshot_version, generation |
| `PD_MSG_TASK_ABORT` | walsender → Worker | top_xid, abort_lsn |
| `PD_MSG_EVENT_SNAPSHOT` | walsender → Worker (broadcast) | event_id, version, SerializeSnapshot output |
| `PD_MSG_EVENT_INVAL` | walsender → Worker (broadcast) | event_id, SharedInvalidationMessage array |
| `PD_MSG_EVENT_FLUSH_ALL` | walsender → Worker (broadcast) | event_id (triggers InvalidateSystemCaches) |
| `PD_MSG_RESULT_CHANGES` | Worker → walsender | top_xid, commit_lsn, PdIntermediateChange stream |
| `PD_MSG_RESULT_FRAMES` | Worker → walsender | top_xid, commit_lsn, protocol frame bytes |
| `PD_MSG_ERROR` | Worker → walsender | Error text |
| `PD_MSG_CTRL_HEARTBEAT` | Worker → walsender | consumed_event_id, state |
| `PD_MSG_CTRL_RELEASE_ACK` | walsender → Worker | top_xid, generation |

#### Intermediate Result Format (centralized call)

```c
typedef struct PdIntermediateChange {
    uint8  op;             /* 'I'/'U'/'D'/'T' */
    Oid    relid;
    uint32 newtuple_len;   /* 0 = absent */
    uint32 oldtuple_len;   /* 0 = absent */
    /* followed by tuple bytes (TOAST-reassembled by Worker) */
} PdIntermediateChange;
```

#### Worker Startup Sequence

1. `BackgroundWorkerInitializeConnectionByOid(dboid, useroid, 0)` — establishes `MyDatabaseId`, relcache, syscache
2. `dsm_attach(dsm_handle)` — attach to DSM segment
3. `shm_toc_attach(PARALLEL_DECODE_TOC_MAGIC, ...)` — attach to TOC
4. Validate `startup_generation` and `worker_id`
5. Attach six message queues with complementary sender/receiver roles
6. Allocate private `XLogReaderState` (plan A)
7. `ReorderBufferAllocate()` with intermediate result callbacks
8. Create minimal `LogicalDecodingContext` (no slot, no plugin)
9. Set state to `PD_WORKER_IDLE`, send first heartbeat
10. Enter main loop

#### Worker Main Loop

```
while (!shutting_down):
    poll cmd_h (non-blocking) → handle SHUTDOWN/CANCEL/RELEASE_ACK
    poll event_h (non-blocking) → handle SNAPSHOT/INVAL/FLUSH_ALL
    poll task_h (non-blocking) → dispatch to decode/commit/abort handler
    if heartbeat interval elapsed → send HEARTBEAT
    WaitLatch(timeout=PD_HEARTBEAT_INTERVAL_MS)
```

#### Worker Decode Pipeline (Plan A)

```
XLogReaderSetDecodeBuffer(reader, msg->rec, msg->rec_len)
XLogBeginRead(reader, msg->orig_lsn)
record = XLogReadRecord(reader)
if top_xid != xid:
    ReorderBufferAssignChild(rb, top_xid, xid, orig_lsn)
rmgr = GetRmgr(XLogRecGetRmid(reader))
rmgr.rm_decode(decode_ctx, &buf)  /* → ReorderBufferQueueChange */
```

#### Worker Commit Processing

```
snap = lookup_snapshot(snapshot_version)
ReorderBufferSetBaseSnapshot(rb, top_xid, commit_lsn, snap)
ReorderBufferCommit(rb, top_xid, commit_lsn, ...)
  → ProcessTXN → SetupHistoricSnapshot → ToastReplace
  → apply_change callback → pdw_append_intermediate
result → Result Queue
```

#### Leader Result Replay (centralized call)

```
entry = output_queue_head (sorted by commit_lsn)
snap = RestoreSnapshot(snapshot_registry[entry->snapshot_version])
SetupHistoricSnapshot(snap, NULL)
ctx->reorder->begin(ctx->reorder, NULL)
for each PdIntermediateChange in entry->result_data:
    rel = RelationIdGetRelation(ic->relid)
    change = palloc0(sizeof(ReorderBufferChange))
    change->action = ic->op → REORDER_BUFFER_CHANGE_*
    change->data.tp.newtuple = reconstruct HeapTuple
    ctx->reorder->apply_change(ctx->reorder, NULL, rel, change)
    RelationClose(rel)
ctx->reorder->commit(ctx->reorder, NULL, entry->commit_lsn)
TeardownHistoricSnapshot(false)
```

#### Concurrency Rules

| Field | Writer | Mechanism |
|---|---|---|
| `state` | Worker | `pg_atomic_write_u32` |
| `consumed_event_id` | Worker | `pg_atomic_write_u64` (relaxed) |
| `top_xid`, `task_generation` | walsender | `SpinLockAcquire(&pd_mutex)` |
| `last_heartbeat` | Worker | `SpinLockAcquire(&pd_mutex)` |
| Compound reads (state + consumed) | walsender | Hold `pd_mutex` |

#### Timeout Constants

| Parameter | Value |
|---|---|
| Heartbeat interval | 1000 ms |
| Heartbeat timeout | 30 s |
| Worker startup timeout | 60 s |
| Pool shutdown timeout | 60 s |
| Echo/result timeout | 10 s / 30 s |

### Compatibility

**Build**: Three new `.c` files added to `src/backend/replication/logical/` (both Makefile and meson.build). No external dependencies.

**`parallel_decode = off`**: Zero code path changes. The dispatch hook in `decode.c` checks `ctx->parallel_pool != NULL` before taking any parallel action. When NULL, execution is identical to unmodified PG 18.

**Worker registration**: `ParallelDecodeWorkerMain` is added to `InternalBGWorkers` in `bgworker.c`, following the pattern of `ApplyWorkerMain`/`ParallelApplyWorkerMain`.

**Test module**: `src/test/modules/test_parallel_decode/` provides SQL functions for pool lifecycle testing without requiring a replication connection.

### Performance

**Statistics collected** via `ParallelDecodePoolMetrics`:
- Worker states (idle/busy/failed counts)
- Pending transaction count
- Task/result queue bytes
- Result spill bytes
- Transaction total and latency average
- Output lag (minimum consumed event ID)
- Plugin timing
- Fallback and restart counts

**`pg_stat_parallel_decode` view**: SQL view (schema defined) backed by C function calling `ParallelDecodePoolGetMetrics`. Returns per-slot metrics.

------------------------------------------------------------------------

## Alternatives Considered

### Worker launch: WORKERTYPE extension vs. dynamic bgworker

Preferred: extend `LogicalRepWorkerType` with `WORKERTYPE_PARALLEL_DECODE`. However, `LogicalRepWorker` array is globally fixed-size with subscription-based indexing. The initial implementation uses `RegisterDynamicBackgroundWorker` directly (detailed design §2.3 fallback path). The `bgw_extra` layout is identical, so switching to WORKERTYPE stays local to one function.

### Record transport: decoded vs. raw bytes

Initial approach sent decoded record content. Corrected to send raw `XLogRecord` bytes (`XLogRecGetTotalLen` range): Worker uses `XLogReaderSetDecodeBuffer` + `XLogReadRecord` to parse, which is the API designed for this purpose.

### Result replay: shadow transaction vs. direct callback invocation

Considered creating a shadow `ReorderBufferTXN` and calling `ReorderBufferCommit`. Simplified to direct callback invocation (`rb->begin`/`apply_change`/`commit`) to avoid the complexity of shadow transaction lifecycle management. The pgoutput wrapper callbacks handle NULL transactions correctly for basic output.

------------------------------------------------------------------------

## Open Questions

1. **Subxact mapping via ASSIGNMENT records**: Worker handles `top_xid != xid` per record, but `XLOG_XACT_ASSIGNMENT` records containing batched subxid lists are not dispatched to Workers. Full solution: extend `PD_MSG_TASK_RECORD` with subxid arrays.

2. **Result Store disk spill**: Layout defined (per-transaction files, `ReorderBufferSerialize` pattern) but not implemented. Deferred until memory-only proves insufficient under real workloads.

3. **Plan B (LSN range authorization)**: Requires Worker-side `XLogReaderState` with page-read callback for authorized ranges only. Not implemented in first pass.

4. **Worker call mode (stage 4)**: Private plugin instance loading, `OutputPluginPrepareWrite`/`Write` writer redirection to Result Queue frames, startup parameter serialization. Not implemented.

5. **`pg_stat_parallel_decode` C backing function**: SQL view schema defined; C function returning metrics not yet written.

6. **Non-transactional message ordering**: Flush mechanism implemented (`CollectResults` + `OutputPending` before message emission), but exact ordering guarantee under concurrent DDL needs verification.

7. **Error serialization format**: Stage 1 uses simple text. Stage 2 should upgrade to `pq_parse_errornotice` protocol format (matching `parallel.c:1166`).

8. **macOS ARM64 shm_mq direction**: Leader-to-Worker `shm_mq` communication times out on macOS ARM64 (Worker-to-Leader direction works). Likely platform-specific shared memory visibility issue. Linux expected to work correctly.
