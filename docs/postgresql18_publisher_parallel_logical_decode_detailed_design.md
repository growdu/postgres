# [RFC-Detail] 发布端并行逻辑解码——详细设计

## 概述

PostgreSQL 18 发布端并行逻辑解码的编码级规格。覆盖文件布局、DSM 布局与消息格式、函数签名、启动序列、主循环、并发规则、错误处理和构建接线。所有函数名和 API 签名均在 PG 18 源码树上核实。

## 动机

配套 RFC（`postgresql18_publisher_parallel_logical_decode_overview.md`）定义了架构与动机。本文档提供编写代码所需的实现级细节：每个结构体、消息格式和函数调用序列。

## 详细设计

### 接口

#### 新增文件

| 文件 | 内容 |
|---|---|
| `src/include/replication/paralleldecode.h` | 共享结构体、DSM 键、消息契约、池 API |
| `src/backend/replication/logical/paralleldecode.c` | Leader 侧：池生命周期、记录分发、事件发布、结果回放 |
| `src/backend/replication/logical/paralleldecodeworker.c` | Worker 侧：启动、解码管线、事件消费、心跳 |
| `src/backend/replication/logical/paralleldecode_walsender.c` | walsender 集成入口 |
| `src/test/modules/test_parallel_decode/` | SQL 可调用的测试函数 |

#### 修改文件

| 文件 | 改动 |
|---|---|
| `decode.c` | `LogicalDecodingProcessRecord` 分流钩子；COMMIT/ABORT 路由；RUNNING_XACTS 快照发布；非事务消息排序 |
| `walsender.c` | 池创建（含资格检查）；`XLogSendLogical` 中收集结果；连接断开时销毁池 |
| `logical.h` | `LogicalDecodingContext` 新增 `parallel_pool` 字段 |
| `logical.c` | `parallel_pool = NULL` 初始化 |
| `bgworker.c` | `InternalBGWorkers` 注册 |
| `guc_tables.c` | 四个 GUC 条目 |
| `Makefile` / `meson.build` | 目标文件添加 |

#### 公开函数签名

```c
/* 池生命周期 */
ParallelDecodePool *ParallelDecodePoolCreate(int nworkers);
void ParallelDecodePoolDestroy(ParallelDecodePool *pool);
bool ParallelDecodePoolCheckHealth(ParallelDecodePool *pool);
int ParallelDecodePoolGetWorkerStates(ParallelDecodePool *pool,
                                      uint32 *states, int nstates);
int ParallelDecodePoolGetNWorkers(ParallelDecodePool *pool);

/* 记录分发（从 decode.c 调用） */
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

/* 事件发布 */
void ParallelDecodePublishSnapshot(ParallelDecodePool *pool,
                                   SnapBuild *builder,
                                   XLogRecPtr lsn);
void ParallelDecodePublishInvalidations(ParallelDecodePool *pool,
                                        uint32 nmsgs,
                                        SharedInvalidationMessage *msgs,
                                        XLogRecPtr commit_lsn);

/* 结果处理 */
void ParallelDecodeCollectResults(ParallelDecodePool *pool);
void ParallelDecodeOutputPending(ParallelDecodePool *pool,
                                 LogicalDecodingContext *ctx);
void ParallelDecodeAdvanceSlotPositions(ParallelDecodePool *pool);

/* 统计 */
void ParallelDecodePoolGetMetrics(ParallelDecodePool *pool,
                                  ParallelDecodePoolMetrics *metrics);

/* Worker 入口 */
void ParallelDecodeWorkerMain(Datum main_arg);
```

### 实现

#### DSM 布局（每 Worker）

每个 Worker 的 DSM 段内有七个 `shm_toc` 条目：

| 键 | 内容 | 默认大小 |
|---|---|---|
| 1 | `ParallelDecodeWorkerInfo` | sizeof |
| 2 | Task MQ（walsender → Worker） | 512 kB |
| 3 | Event MQ（walsender → Worker） | 256 kB |
| 4 | Result MQ（Worker → walsender） | 1 MB |
| 5 | Error MQ（Worker → walsender） | 16 kB |
| 6 | Control Request MQ（Worker → walsender） | 8 kB |
| 7 | Control Command MQ（walsender → Worker） | 8 kB |

#### 消息契约

所有消息以 `PdMsgHeader` 开头：

```c
typedef struct PdMsgHeader {
    uint32 len;   /* 含头部 */
    uint8  type;  /* PdMsgType */
} PdMsgHeader;
```

| 类型 | 方向 | 负载 |
|---|---|---|
| `PD_MSG_TASK_RECORD` | walsender → Worker | top_xid、xid、LSN、原始 XLogRecord 字节 |
| `PD_MSG_TASK_COMMIT` | walsender → Worker | top_xid、commit_lsn、snapshot_version、generation |
| `PD_MSG_TASK_ABORT` | walsender → Worker | top_xid、abort_lsn |
| `PD_MSG_EVENT_SNAPSHOT` | walsender → Worker（广播） | event_id、version、SerializeSnapshot 输出 |
| `PD_MSG_EVENT_INVAL` | walsender → Worker（广播） | event_id、SharedInvalidationMessage 数组 |
| `PD_MSG_EVENT_FLUSH_ALL` | walsender → Worker（广播） | event_id（触发 InvalidateSystemCaches） |
| `PD_MSG_RESULT_CHANGES` | Worker → walsender | top_xid、commit_lsn、PdIntermediateChange 流 |
| `PD_MSG_RESULT_FRAMES` | Worker → walsender | top_xid、commit_lsn、协议帧字节 |
| `PD_MSG_ERROR` | Worker → walsender | 错误文本 |
| `PD_MSG_CTRL_HEARTBEAT` | Worker → walsender | consumed_event_id、state |
| `PD_MSG_CTRL_RELEASE_ACK` | walsender → Worker | top_xid、generation |

#### 中间结果格式（集中调用模式）

```c
typedef struct PdIntermediateChange {
    uint8  op;             /* 'I'/'U'/'D'/'T' */
    Oid    relid;
    uint32 newtuple_len;   /* 0 = 无 */
    uint32 oldtuple_len;   /* 0 = 无 */
    /* 后跟元组字节（Worker 侧已完成 TOAST 重组） */
} PdIntermediateChange;
```

#### Worker 启动序列

1. `BackgroundWorkerInitializeConnectionByOid(dboid, useroid, 0)`——建立 `MyDatabaseId`、relcache、syscache
2. `dsm_attach(dsm_handle)`——附加 DSM 段
3. `shm_toc_attach(PARALLEL_DECODE_TOC_MAGIC, ...)`——附加 TOC
4. 校验 `startup_generation` 与 `worker_id`
5. 以互补的 sender/receiver 角色附加六条消息队列
6. 分配私有 `XLogReaderState`（方案 A）
7. `ReorderBufferAllocate()` 并注册中间结果回调
8. 创建最小 `LogicalDecodingContext`（无槽、无插件）
9. 置状态为 `PD_WORKER_IDLE`，发送首个心跳
10. 进入主循环

#### Worker 主循环

```
while (!shutting_down):
    轮询 cmd_h（非阻塞）→ 处理 SHUTDOWN/CANCEL/RELEASE_ACK
    轮询 event_h（非阻塞）→ 处理 SNAPSHOT/INVAL/FLUSH_ALL
    轮询 task_h（非阻塞）→ 分发给 decode/commit/abort 处理器
    若心跳间隔到达 → 发送 HEARTBEAT
    WaitLatch(timeout=PD_HEARTBEAT_INTERVAL_MS)
```

#### Worker 解码管线（方案 A）

```
XLogReaderSetDecodeBuffer(reader, msg->rec, msg->rec_len)
XLogBeginRead(reader, msg->orig_lsn)
record = XLogReadRecord(reader)
若 top_xid != xid:
    ReorderBufferAssignChild(rb, top_xid, xid, orig_lsn)
rmgr = GetRmgr(XLogRecGetRmid(reader))
rmgr.rm_decode(decode_ctx, &buf)  /* → ReorderBufferQueueChange */
```

#### Worker 提交处理

```
snap = lookup_snapshot(snapshot_version)
ReorderBufferSetBaseSnapshot(rb, top_xid, commit_lsn, snap)
ReorderBufferCommit(rb, top_xid, commit_lsn, ...)
  → ProcessTXN → SetupHistoricSnapshot → ToastReplace
  → apply_change 回调 → pdw_append_intermediate
结果 → Result Queue
```

#### Leader 结果回放（集中调用模式）

```
entry = output_queue_head（按 commit_lsn 排序）
snap = RestoreSnapshot(snapshot_registry[entry->snapshot_version])
SetupHistoricSnapshot(snap, NULL)
ctx->reorder->begin(ctx->reorder, NULL)
对 entry->result_data 中的每个 PdIntermediateChange:
    rel = RelationIdGetRelation(ic->relid)
    change = palloc0(sizeof(ReorderBufferChange))
    change->action = ic->op → REORDER_BUFFER_CHANGE_*
    change->data.tp.newtuple = 重建 HeapTuple
    ctx->reorder->apply_change(ctx->reorder, NULL, rel, change)
    RelationClose(rel)
ctx->reorder->commit(ctx->reorder, NULL, entry->commit_lsn)
TeardownHistoricSnapshot(false)
```

#### 并发规则

| 字段 | 写者 | 机制 |
|---|---|---|
| `state` | Worker | `pg_atomic_write_u32` |
| `consumed_event_id` | Worker | `pg_atomic_write_u64`（relaxed） |
| `top_xid`、`task_generation` | walsender | `SpinLockAcquire(&pd_mutex)` |
| `last_heartbeat` | Worker | `SpinLockAcquire(&pd_mutex)` |
| 复合读取（state + consumed） | walsender | 持有 `pd_mutex` |

#### 超时常量

| 参数 | 值 |
|---|---|
| 心跳间隔 | 1000 ms |
| 心跳超时 | 30 s |
| Worker 启动超时 | 60 s |
| 池关闭超时 | 60 s |
| Echo/结果超时 | 10 s / 30 s |

### 兼容性

**构建**：三个新 `.c` 文件加入 `src/backend/replication/logical/`（Makefile 与 meson.build 同步）。无外部依赖。

**`parallel_decode = off`**：零代码路径变化。`decode.c` 中的分流钩子在 `ctx->parallel_pool != NULL` 时才生效。为 NULL 时执行路径与未修改的 PG 18 完全一致。

**Worker 注册**：`ParallelDecodeWorkerMain` 加入 `bgworker.c` 的 `InternalBGWorkers` 数组，与 `ApplyWorkerMain`/`ParallelApplyWorkerMain` 同一模式。

**测试模块**：`src/test/modules/test_parallel_decode/` 提供不依赖复制连接即可测试池生命周期的 SQL 函数。

### 性能

**统计指标**（`ParallelDecodePoolMetrics` 结构体）：
- Worker 状态（idle/busy/failed 计数）
- pending 事务数
- 任务/结果队列字节数
- 结果 spill 字节数
- 事务总数与延迟均值
- 输出 lag（最小已消费事件 ID）
- 插件耗时
- 回退与重启计数

**`pg_stat_parallel_decode` 视图**：SQL 视图（schema 已定义），后端由调用 `ParallelDecodePoolGetMetrics` 的 C 函数支撑。返回按槽维度的指标。

---

## 已考虑的备选方案

### Worker 启动：WORKERTYPE 扩展 vs. 动态后台 Worker

优先扩展 `LogicalRepWorkerType` 增加 `WORKERTYPE_PARALLEL_DECODE`。但 `LogicalRepWorker` 数组全局定长、按订阅索引。初始实现直接使用 `RegisterDynamicBackgroundWorker`（详设 §2.3 回退路径）。`bgw_extra` 布局相同，后续切换 WORKERTYPE 只需修改一个函数。

### 记录传送：解码后数据 vs. 原始字节

最初方案发送解码后的记录内容。修正为发送**原始 XLogRecord 字节**（`XLogRecGetTotalLen` 覆盖范围）：Worker 用 `XLogReaderSetDecodeBuffer` + `XLogReadRecord` 解析——这正是该 API 的设计用途。

### 结果回放：影子事务 vs. 直接回调调用

考虑过创建影子 `ReorderBufferTXN` 并调用 `ReorderBufferCommit`。简化为直接调用回调（`rb->begin`/`apply_change`/`commit`），避免影子事务生命周期管理的复杂度。pgoutput 的包装回调对 NULL 事务在基本输出场景下工作正常。

---

## 开放问题

1. **子事务 ASSIGNMENT 记录的映射传递**：Worker 按 `top_xid != xid` 逐条处理，但包含批量 subxid 列表的 `XLOG_XACT_ASSIGNMENT` 记录未分发给 Worker。完整方案：在 `PD_MSG_TASK_RECORD` 中扩展 subxid 数组。

2. **Result Store 磁盘 spill**：布局已定义（按事务文件、`ReorderBufferSerialize` 模式）但未实现。推迟到内存-only 在实际负载下被证明不足时。

3. **方案 B（LSN 区间授权）**：需要 Worker 侧 `XLogReaderState` 配置只读授权区间的页面回调。首版未实现。

4. **Worker 调用模式（阶段 4）**：私有插件实例加载、`OutputPluginPrepareWrite`/`Write` writer 重定向到 Result Queue 帧、启动参数序列化。未实现。

5. **`pg_stat_parallel_decode` C 后端函数**：SQL 视图 schema 已定义；返回指标的 C 函数未编写。

6. **非事务消息排序**：冲刷机制已实现（消息输出前先 `CollectResults` + `OutputPending`），但在并发 DDL 下的精确顺序保证需验证。

7. **错误序列化格式**：阶段 1 使用简单文本。阶段 2 应升级为 `pq_parse_errornotice` 协议格式（对齐 `parallel.c:1166`）。

8. **macOS ARM64 shm_mq 方向**：Leader→Worker 方向的 `shm_mq` 通信在 macOS ARM64 上超时（Worker→Leader 方向正常）。可能是平台相关的共享内存可见性问题。Linux 上预期正常。
