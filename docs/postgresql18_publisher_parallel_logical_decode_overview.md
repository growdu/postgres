# [RFC] Publisher-Side Parallel Logical Decoding for PostgreSQL 18

## Summary

Add transaction-level parallel logical decoding to the publisher side of PostgreSQL logical replication. A fixed pool of background Decode Worker processes performs WAL record decoding in parallel, while the walsender retains exclusive control over WAL reading, transaction identification, snapshot management, and protocol output. The output byte stream is identical to serial decoding.

## Motivation

The publisher side of logical replication is single-threaded: one `walsender` process reads WAL sequentially, decodes records, invokes output plugin callbacks, and sends protocol messages in a single execution loop. When logical decoding itself becomes the bottleneck (high write volume, wide tables, many TOAST columns), this single thread cannot utilize multi-core CPUs.

PG 18's existing `streaming = parallel` option parallelizes the *subscriber* side (applying streamed transactions in parallel). It does not parallelize the *publisher* side (decoding multiple transactions concurrently). These are complementary features.

The performance bottleneck decomposes into five segments:

| Segment | Description |
|---|---|
| (a) Record parsing | `DecodeInsert`/`Update`/`Delete` + `ReorderBufferQueueChange` |
| (b) Catalog lookup | relcache/syscache access under historic snapshot |
| (c) TOAST reassembly | `ReorderBufferToastReplace` |
| (d) Plugin callback | `pgoutput` tuple-to-protocol serialization |
| (e) Protocol write | `WalSndWriteData` network send |

Segments (a)-(c) are parallelizable per-transaction. Segment (d) is parallelizable if the output plugin supports per-transaction instances. Segment (e) remains serial (single protocol output stream). With N workers, the theoretical speedup is bounded by segments (a)-(d) as a fraction of total time.

## Detailed Design

### API

**GUC Parameters** (all SIGHUP, superuser-only, read at connection establishment):

| Parameter | Type | Default | Description |
|---|---|---|---|
| `parallel_decode` | bool | `off` | Master switch |
| `max_parallel_decode_workers_per_walsender` | int | `2` | Worker pool size; bounded by `max_worker_processes` |
| `parallel_decode_input_queue_size` | int (kB) | `65536` | Pending input buffer limit; overflow triggers WAL read backpressure |
| `parallel_decode_result_queue_size` | int (kB) | `16384` | Result Store memory limit; overflow spills to disk |
| `parallel_decode_fallback` | enum (`serial`/`wait`) | `serial` | Fallback strategy when safety cannot be proven |

**Statistics View**: `pg_stat_parallel_decode` — one row per replication slot/session with worker states, pending transaction count, queue/spill bytes, invalidation catch-up wait, output lag, fallback/restart counts, plugin timing.

**Replication Protocol**: No new messages or capability bits. Output byte stream is identical to serial decoding, ordered by commit LSN.

**Output Plugin Interface**: In the baseline mode (centralized call), plugin API is unchanged. In the extended mode (Worker call), plugins must declare parallel support; undeclared plugins automatically use centralized call.

**Client Impact**: None. Subscribers of any version connect without negotiation changes.

### Implementation

#### Process Architecture

```
postmaster
├── walsender (per replication connection)
│   ├── WAL reading + transaction identification + global SnapBuild
│   ├── Scheduler (pending queue, worker assignment)
│   ├── Result collection + output ordering (Result Store)
│   └── Protocol output (sole writer)
└── Decode Worker 1..N (per session, dynamic background workers)
    ├── Private ReorderBuffer (per-transaction scoped)
    ├── Local relcache/syscache (with invalidation tracking)
    └── Decode computation (centralized: intermediate results / Worker: private plugin)
```

#### Key Mechanism: ReorderBuffer Split

The serial `ReorderBuffer` is decomposed by transaction scope:

| Responsibility | Owner |
|---|---|
| XID classification, subxact→top-level mapping | walsender |
| Transaction boundary detection (COMMIT/ABORT) | walsender |
| Per-transaction change buffering + disk spill | Worker |
| TOAST reassembly | Worker |
| Commit-time catalog invalidation + decode | Worker |
| Commit-order output | walsender |

#### Key Mechanism: Snapshot Versioning

- walsender generates snapshot versions at `RUNNING_XACTS` boundaries using `SnapBuildGetOrBuildSnapshot`
- Each version is serialized via `SerializeSnapshot` and broadcast to all Workers
- Workers cache versions in a 16-entry table and use `ReorderBufferSetBaseSnapshot` to attach the correct version per transaction
- Invariant: transaction T must use the snapshot corresponding to T's commit record LSN

#### Key Mechanism: Catalog Invalidation Catch-up

- walsender extracts `xl_xact_invals` from commit records and broadcasts as INVAL events
- Workers execute `LocalExecuteInvalidationMessage` locally, advancing `consumed_event_id`
- Over-execution of invalidations is safe (only clears caches; visibility determined by snapshot)
- Under-execution is unsafe (stale cache entries reused across versions)
- Slot position candidates (`restart_lsn`/`catalog_xmin`) are gated by the minimum `consumed_event_id` across all in-flight Workers

#### Dispatch Points in decode.c

| Record type | Parallel mode behavior |
|---|---|
| HEAP/HEAP2 (DML) | `ParallelDecodeDispatchRecord` → Worker task queue |
| COMMIT | Publish invalidations → `ParallelDecodeDispatchCommit` |
| ABORT | `ParallelDecodeDispatchAbort` |
| RUNNING_XACTS | `SnapBuildProcessRunningXacts` → `ParallelDecodePublishSnapshot` |
| Non-transactional logical message | Flush output queue to this LSN, then emit |

#### Worker Input Transport (two options)

| Aspect | Option A: per-record | Option B: LSN range |
|---|---|---|
| Worker input | Record bytes via task queue | LSN range + subxact map; Worker reads WAL |
| IPC volume | Proportional to change count | Control messages only |
| walsender CPU | Parse + copy per record | Parse record header only |
| Implementation | Simple; validated first | Complex; requires Worker-side WAL reader |

Baseline: Option A. Switch to B if benchmarks show walsender as bottleneck.

#### Output Plugin Call Position (two modes)

| Mode | Description | Plugin compatibility | Benefit |
|---|---|---|---|
| Centralized call (baseline) | Worker produces intermediate results; walsender calls plugin serially | All existing plugins unchanged | Correctness-first |
| Worker call (extension) | Worker loads private plugin instance, outputs protocol frames directly | Requires capability declaration | Parallelizes segment (d) |

Decision: Start with centralized call. Switch to Worker call if segment (d) > 30% of CPU time (measured in stage 1 benchmark).

### Compatibility

| Client | Behavior |
|---|---|
| PG subscriber, any version, `streaming = off` | Unaffected (output identical to serial) |
| PG subscriber, `streaming = on/parallel` | Entire slot falls back to serial |
| `pg_recvlogical`, Debezium, pglogical | Unaffected |
| `pgoutput` protocol v3/v4/v6 | Unchanged |
| `parallel_decode = off` | Original serial path |

**Plugin compatibility**: Centralized call mode: zero API changes. Worker call mode: requires `OutputPluginCallbacks` parallel support flag; undeclared plugins use centralized call.

**Upgrade/downgrade**: GUC default `off`; behavior unchanged after pg_upgrade. Rollback = disable GUC.

**Resource**: Workers consume `max_worker_processes` quota. Insufficient quota → fewer workers or full serial, with WARNING; connections never rejected.

### Performance

**Metrics** (per-slot via `pg_stat_parallel_decode`): worker states, pending transactions, queue/spill bytes, invalidation wait time, output lag, plugin timing, fallback count, restart count.

**Benchmark plan** (minimum):
1. Many independent small transactions
2. Few large transactions
3. Interleaved multi-transaction writes
4. High catalog access / system table changes
5. DDL interleaved with DML (triggering catalog invalidation)
6. Worker pool saturation + queue backpressure
7. Worker failure + cancel + reconnect
8. Slow plugin / slow network (observing head-of-line blocking)
9. Non-transactional logical messages interleaved
10. `two_phase` slots (serial fallback verification)
11. Serial vs parallel comparison

**Performance targets** (provisional, calibrated by benchmark):

| Metric | Target |
|---|---|
| Small transaction throughput | ≥1.5× (2 workers), ≥2× (4 workers) |
| Large transaction | ≥0.9× of serial |
| Latency P99 | ≤1.2× of serial |
| Serial path regression (`off`) | <3% deviation from unmodified PG18 |

------------------------------------------------------------------------

## Alternatives Considered

### 1. openGauss parallel decoding

openGauss 3.0.0+ implements publisher-side parallel logical decoding using a thread-based architecture (reader thread + N decoder threads + sender thread in one process, communicating via lock-free ring queues).

**Why not adopted**: openGauss's three premises relax PostgreSQL's hardest constraints:
- Thread model eliminates cross-process serialization cost (pointer passing suffices)
- Custom output protocol (text/json/binary) bypasses plugin ecosystem and byte-level equivalence
- No DDL decoding avoids catalog consistency entirely

PostgreSQL's process model, plugin ecosystem, and protocol compatibility cannot be relaxed. The design must solve cross-process serialization (§5.2), plugin call position (§5.3), and catalog consistency (§4.5) that openGauss avoids.

**What is borrowed**: The reader/decoder/sender three-stage pipeline validates the overall structure. Transaction-per-thread + LSN-ordered output confirms the scheduling model.

### 2. PolarDB (open source)

Verified: open source PolarDB-PG has **no publisher-side parallel logical decoding implementation**. Its "parallel replay" feature is standby physical WAL replay (LogIndex + page-tag dispatch), which is a different problem domain.

### 3. Subscriber-side parallel apply (existing PG 18)

PG 18's `applyparallelworker.c` has identical *mechanism* (leader + worker pool, DSM/shm_mq, one-txn-one-worker, serialize_changes fallback) but solves a *different problem* (applying DML to tables vs decoding WAL records). The catalog consistency model differs fundamentally: apply workers use current snapshots + sinval; decode workers need historic snapshots + explicit invalidation events.

### 4. Cache versioning for relcache/syscache

Rejected: relcache/syscache are globally single-version structures. Versioning them equals rewriting the cache subsystem.

### 5. Worker bypasses cache, reads heap directly

Rejected: Correct but catalog lookup is a decode hotspot; performance unacceptable.

### 6. Worker replays DDL

Rejected: Infeasible and unnecessary — invalidation semantics (clear entries) suffice.

------------------------------------------------------------------------

## Open Questions

1. **Worker call mode threshold**: What percentage of segment (d) justifies implementing Worker call? (Provisional: 30%. Needs benchmark calibration.)

2. **Snapshot version table eviction**: The 16-entry LRU with refcount-based eviction is correct but may need tuning for workloads with many concurrent transactions spanning multiple RUNNING_XACTS boundaries.

3. **Subtransaction mapping transfer**: Current approach sends `top_xid != xid` per record, which handles direct parent-child but not accumulated ASSIGNMENT record mappings. Full solution requires extending the task message with subxid lists.

4. **Result Store disk spill**: The current design describes the layout (per-transaction files, `ReorderBufferSerialize` pattern) but implementation is deferred until memory-only proves insufficient.

5. **Plan B (LSN range authorization)**: Deferred to stage 3. Requires Worker-side WAL reader lifecycle management and error recovery at range boundaries.

6. **Runtime mode switching**: Currently, parameters are fixed at connection establishment. Mid-session switching (active transaction handling, reconnection requirement) is deferred.

7. **SQL interface (`pg_logical_slot_get_changes`)**: Out of scope for the first version. Can be added later using the same pool infrastructure.

8. **Interleaved output protocol**: If the replication protocol is extended to allow cross-transaction interleaving, the design must be updated with protocol version negotiation and subscriber-side state machine changes.

------------------------------------------------------------------------

## Appendix A: Terminology

| Term | Definition |
|---|---|
| Decode Worker | Background worker process responsible for one top-level transaction's decode task |
| Worker Pool | Fixed-capacity set of Decode Workers |
| Pending Queue | Queue for transactions waiting for an idle Worker |
| Event Stream | walsender-ordered channel for snapshot versions and catalog invalidations |
| Catalog Invalidation Catch-up | Synchronization mechanism: walsender waits for Workers to consume invalidation events to a required position before advancing slot positions; semantically equivalent to Linux RCU quiescent state |
| Result Store | walsender-side result buffer: memory-first, per-transaction disk spill on overflow |
| Historic Snapshot | Historical snapshot used for transaction decoding; must correspond to the transaction's commit record LSN |
| Centralized Call / Worker Call | Two output plugin execution positions: centralized in walsender single backend (baseline) / moved to Worker private plugin instance (extension, requires plugin declaration) |
| Snapshot Version | Numbered instance of a historic snapshot generated by walsender; reused within a RUNNING_XACTS window |
