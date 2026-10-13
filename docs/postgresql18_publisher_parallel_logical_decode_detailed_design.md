# PostgreSQL 18 发布端并行逻辑解码详细设计

-   **文档类型**：详细设计（编码级）
-   **上游文档**：概要设计 `postgresql18_publisher_parallel_logical_decode_overview.md`（下文引用为「概设 §x」）；机制论证、方案对比、验收标准以概设为准，本文不重复
-   **源码基线**：PostgreSQL 18（本仓库 `parallel_decode` 分支）；本文引用的函数与结构均已在基线源码核实
-   **状态**：编码输入；标注「评估」的条目允许实现时二次确认，其余条目按本文执行

------------------------------------------------------------------------

## 1. 文件清单与模块划分

新增：

| 文件 | 内容 |
|---|---|
| `src/backend/replication/logical/paralleldecode.c` | walsender 侧：Worker 池、调度、事件与任务发布、结果接收与回放、`restart_lsn`/`catalog_xmin` 推进门控 |
| `src/backend/replication/logical/paralleldecodeworker.c` | Worker main、解码循环、事件消费、结果生成 |
| `src/include/replication/paralleldecode.h` | 共享结构、DSM 键、消息与中间结果定义（§3、§6、§7） |
| `src/test/subscription/t/0xx_parallel_decode*.pl` | 概设 §12 的三类 TAP 测试 |

修改：

| 文件 | 改动 |
|---|---|
| `walsender.c` | 连接建立时的模式判定与池创建（概设 §3.2）；`WalSndLoop` 四事件源循环（概设 §4.1 图） |
| `decode.c` | `LogicalDecodingProcessRecord` 前置分流钩子（§4.2）；导出 Heap 分发表供 Worker 复用 |
| `logical.c` | 新增导出 `CreateWorkerDecodingContext`（§5.1） |
| `worker_internal.h`、`launcher.c`、`worker.c` | `WORKERTYPE_PARALLEL_DECODE` 注册路径（§2.3） |
| `pgoutput.c` | Worker 调用模式的能力声明与每事务实例审计（阶段 4） |
| `reorderbuffer.c` | 无结构性修改；仅确认 `ReorderBufferSetBaseSnapshot`/`ReorderBufferCommit` 的跨上下文可用性（§5.3） |

------------------------------------------------------------------------

## 2. Worker 进程的启动与环境

### 2.1 启动参数（bgw_extra 布局）

``` c
#define PARALLEL_DECODE_BGW_EXTRALEN  128

typedef struct ParallelDecodeWorkerStartup
{
    Oid         dboid;          /* walsender 的 MyDatabaseId */
    Oid         useroid;        /* walsender 的 MyDatabaseId 对应用户，或 InvalidOid */
    dsm_handle  dsm_handle;     /* 本 Worker 的 DSM 句柄 */
    int         worker_id;      /* 池内编号 */
    uint32      startup_generation; /* 池世代：防陈旧 Worker 误挂新会话的 DSM */
    char        slotname[NAMEDATALEN];
} ParallelDecodeWorkerStartup;

StaticAssertStmt(sizeof(ParallelDecodeWorkerStartup) <= BGW_EXTRALEN, ...);
```

`bgw_name`/`bgw_type` 固定为 "parallel logical decode worker"；`bgw_main` = `ParallelDecodeWorkerMain`；`bgw_main_arg` 不用（全部走 bgw_extra）；`bgw_notify_pid` = walsender PID（启动完成通知）。

### 2.2 启动序列（ParallelDecodeWorkerMain）

1.  `BackgroundWorkerInitializeConnectionByOid(dboid, useroid, 0)`——建立 `MyDatabaseId`、数据库路径、relcache 初始化（含共享 init 文件）、syscache，即概设 §4.4 要求的类后端环境；失效回调在该状态之后可用。
2.  创建 `CurrentResourceOwner` 与解码用内存上下文 `pd_worker_context`。
3.  `dsm_attach(dsm_handle)`，`shm_toc_lookup(PARALLEL_DECODE_KEY_WORKER_INFO)` 校验 `startup_generation` 与 bgw_extra 一致，不一致立即退出（陈旧 Worker）。
4.  attach 五条 `shm_mq`（§3.1）；task/event/result/error 由 walsender 预先 `shm_mq_set_sender`，Worker `shm_mq_attach` 后 `shm_mq_set_receiver`；control 队列拆为 Worker→walsender 的请求半区与 walsender→Worker 的命令半区（两条独立 mq，键名加后缀 `_REQ`/`_CMD`）。
5.  方案 A：创建私有 `XLogReaderState`（`XLogReaderAllocate`，页面回调为断言失败的占位——Worker 不读页）。
6.  `CreateWorkerDecodingContext`（§5.1）。
7.  原子更新共享 `state = PD_WORKER_IDLE`，发送首个心跳（含 `consumed_event_id = 0`）。
8.  进入主循环（§5.6）。
9.  `PG_CATCH`：错误序列化进 error mq（§10），转步骤 10。
10. 退出：detach 全部队列、`dsm_detach`、`proc_exit(0)`；`bgw_notify_pid` 通知由 postmaster 句柄语义承担。

### 2.3 注册路径（WORKERTYPE 扩展改动清单）

按概设 §9.5 的结论，优先扩展现有逻辑复制 Worker 框架：

1.  `LogicalRepWorkerType` 增加 `WORKERTYPE_PARALLEL_DECODE`（枚举追加在末尾，避免值漂移）。
2.  `LogicalRepWorker` 增加归属字段 `int walsender_pid;`（发布端归属）；`subid` 置 0、`relid` 置 InvalidOid、`userid` 填 useroid。
3.  `logicalrep_worker_launch`：按类型分支——该类型不查 `pg_subscription`、不解析 relation 列表、`bgw_extra` 改填 §2.1 结构。
4.  `launcher.c`：主循环对该类型跳过超时重启（发布端 Worker 由 walsender 自管生命周期；launcher 只负责槽位登记与回收）。
5.  已知约束（概设 §9.5）：`LogicalRepWorker` 数组按 `max_logical_replication_workers` 全局定长，发布端 Worker 与订阅端共享该配额——GUC 文档需注明。
6.  回退路径：若 3 的耦合超出预期，改为 `RegisterDynamicBackgroundWorker` 直启（bgw_extra 布局与本节不变，仅注册方式不同）。

------------------------------------------------------------------------

## 3. DSM 布局与消息格式

### 3.1 shm_toc 键与队列容量

| 键 | 内容 | 容量（默认） |
|---|---|---|
| `PARALLEL_DECODE_KEY_WORKER_INFO` | `ParallelDecodeWorkerInfo`（§6.1） | sizeof |
| `PARALLEL_DECODE_KEY_TASK_MQ` | 任务与事务标记 | `parallel_decode_input_queue_size / 2` |
| `PARALLEL_DECODE_KEY_EVENT_MQ` | 快照/失效事件 | `parallel_decode_input_queue_size / 2` |
| `PARALLEL_DECODE_KEY_RESULT_MQ` | 解码结果 | `parallel_decode_result_queue_size` |
| `PARALLEL_DECODE_KEY_ERROR_MQ` | 错误 | 16 kB（对齐 `DSM_ERROR_QUEUE_SIZE` 用法） |
| `PARALLEL_DECODE_KEY_CONTROL_REQ_MQ` | Worker → walsender 心跳/确认 | 8 kB |
| `PARALLEL_DECODE_KEY_CONTROL_CMD_MQ` | walsender → Worker 取消/关闭 | 8 kB |

### 3.2 通用消息头与封帧

``` c
typedef struct PdMsgHeader
{
    uint32  len;    /* 含 header 的整条消息长度 */
    uint8   type;   /* PD_MSG_* */
} PdMsgHeader;
```

所有消息 = `PdMsgHeader` + 类型体；`shm_mq_send` 以整条消息为单位（`len` 校验 + 断言）。变长数组以偏移量而非指针表达。

### 3.3 消息定义

``` c
/* 任务通道（walsender → Worker） */
PD_MSG_TASK_RECORD:                    /* 方案 A：单条 WAL 记录 */
  { PdMsgHeader; TransactionId top_xid; TransactionId xid;
    XLogRecPtr orig_lsn; XLogRecPtr end_lsn;
    uint64 latest_event_id; uint64 task_generation;
    uint32 rec_len; char rec[rec_len]; }   /* walsender 读入的原始 XLogRecord
                                             字节：XLogRecGetLen(record) 覆盖
                                             范围（记录头 + 全部 payload），
                                             不含页面填充；decode 由 Worker
                                             侧本地 reader 完成（§5.2） */

PD_MSG_TASK_COMMIT:                    /* 提交标记 */
  { PdMsgHeader; TransactionId top_xid;
    XLogRecPtr commit_lsn; XLogRecPtr end_lsn;
    TimestampTz commit_time; RepOriginId origin_id; XLogRecPtr origin_lsn;
    uint64 snapshot_version; uint64 latest_event_id; uint64 task_generation; }

PD_MSG_TASK_ABORT:
  { PdMsgHeader; TransactionId top_xid; XLogRecPtr abort_lsn; }

/* 事件通道（walsender → Worker，按 event_id 保序） */
PD_MSG_EVENT_SNAPSHOT:
  { PdMsgHeader; uint64 event_id; XLogRecPtr lsn;
    uint64 version; uint32 snap_len; char snap[snap_len]; }  /* SerializeSnapshot 输出 */

PD_MSG_EVENT_INVAL:
  { PdMsgHeader; uint64 event_id; XLogRecPtr commit_lsn;
    uint32 nmsgs; SharedInvalidationMessage msgs[nmsgs]; }   /* xl_xact_invals 原样拷贝 */

PD_MSG_EVENT_FLUSH_ALL:
  { PdMsgHeader; uint64 event_id; XLogRecPtr commit_lsn; }

/* 结果通道（Worker → walsender） */
PD_MSG_RESULT_CHANGES:                 /* 集中调用：中间结果 */
  { PdMsgHeader; TransactionId top_xid; XLogRecPtr commit_lsn;
    uint64 task_generation; uint32 nchanges;
    PdIntermediateChange changes[nchanges]; }                /* §7 */

PD_MSG_RESULT_FRAMES:                  /* Worker 调用：协议帧 */
  { PdMsgHeader; TransactionId top_xid; XLogRecPtr commit_lsn;
    uint64 task_generation; uint32 total_len;
    char frames[total_len]; }          /* 帧格式：uint32 flen + flen 字节，
                                          事务内顺序拼接 */

PD_MSG_ERROR:
  { PdMsgHeader; }                      /* 其后为 error/notice 协议消息体（§10） */

/* 控制通道 */
PD_MSG_CTRL_HEARTBEAT:  { PdMsgHeader; uint64 consumed_event_id; uint32 state; }
PD_MSG_CTRL_RELEASE_ACK:{ PdMsgHeader; TransactionId top_xid; uint64 task_generation; }
PD_MSG_CTRL_CANCEL:     { PdMsgHeader; }
PD_MSG_CTRL_SHUTDOWN:   { PdMsgHeader; }
```

约束与防御：`rec_len` 超过 1 MB 时该记录触发受控串行回退（理论不存在的防御分支）；`snapshot_version` 与 `latest_event_id` 同时出现在 TASK_COMMIT 上——TASK_RECORD 也带 `latest_event_id`，取该消息创建时 walsender 已发布的最大事件号（保守值，满足概设 §10.3.11 即可，事件量本身小）。

------------------------------------------------------------------------

## 4. walsender 侧实现

### 4.1 池与调度状态

``` c
typedef struct ParallelDecodePool
{
    int                     nworkers;
    ParallelDecodeWorkerInfo *winfo;       /* 指向各 DSM 内的共享结构 */
    HTAB                    *txn_hash;     /* ParallelDecodeTxnEntry，walsender 私有 */
    dlist_head              pending;       /* 到达序，walsender 私有 */
    uint64                  next_event_id;
    PdEventLsnRing         *event_ring;    /* event_id → LSN，定长 1024 环 */
    PdSnapshotRegistry     *snapreg;       /* §4.3 */
    ParallelDecodePoolMetrics metrics;     /* §11 指标的内存版 */
} ParallelDecodePool;
```

创建时机：复制会话建立且 `parallel_decode = on` 且通过并行资格检查（概设 §5.6：streaming/two_phase 槽不建池）。销毁时机：会话结束（概设 §3.2）。

调度按概设 §5.1 的八步执行，数据落在本结构上；pending queue 为 walsender 私有内存，无锁。

### 4.2 记录分流（decode.c 钩子）

`LogicalDecodingProcessRecord`（decode.c:88）入口处插分流：

``` c
/* 伪代码：并行模式时替换原处理体 */
ParallelDecodeProcessRecord(ctx, record):
  switch (rmgr)
    XLOG(RUNNING_XACTS):
        本地 SnapBuildProcessRunningXacts；
        若 builder->snapshot 指针变化 → snapreg 新版本 + 广播 EVENT_SNAPSHOT（§4.3）
    XACT commit:
        解析 xl_xact_commit；
        若 parsed->nmsgs > 0 → 广播 EVENT_INVAL（nmsgs 超过 64 改发 FLUSH_ALL）
        下发 TASK_COMMIT（先事件后任务，程序序保证概设 §10.3.11）
    XACT abort:   下发 TASK_ABORT + 本地销毁条目
    XACT assignment: 本地 ReorderBufferAssignChild 更新映射（随任务携带）
    HEAP/HEAP2:
        归属判定（top_xid 查 txn_hash，未登记则登记并调度）
        已绑定 → 下发 TASK_RECORD（rec = reader 解码后的整条记录字节）
        未绑定且池满 → pending 缓冲（方案 A）
        未绑定且空闲 → 绑定 + 下发
    其他 rmgr: 按概设 §5.6 资格表处理（串行回退或忽略）
```

串行模式（`parallel_decode = off`）不走本函数，代码路径与上游一致（概设 §4.2 的改造边界）。

### 4.3 快照注册表与事件发布

``` c
typedef struct PdSnapshotEntry
{
    uint64  version;
    XLogRecPtr lsn;
    char   *serialized;        /* SerializeSnapshot 输出 */
    Size    len;
    int     refcount;          /* 引用该版本且未完成任务数 */
    dlist_node lru_node;
} PdSnapshotEntry;
```

-   版本生成：仅在处理 RUNNING_XACTS 后 `builder->snapshot` 指针变化时 `version++` 并广播（`SnapBuildGetOrBuildSnapshot` 缓存行为，概设 §4.4）；
-   LRU 上限 16，淘汰条件：`refcount == 0` 且距最新版本超过 4 代；淘汰下界不得低于任一在途事务引用的版本（正确性约束，非性能参数）；
-   TASK_COMMIT 下发时对 `snapshot_version` 的 entry `refcount++`，收到 RELEASE_ACK 或事务终结时 `refcount--`；
-   Worker 侧版本表淘汰规则与此对称（保留 `[最早在途引用版本, latest]`，余量 4）；
-   `event_ring`：`{event_id, lsn}` 定长 1024 环。覆盖约束：最老槽位的事件未被全部 Worker 消费前不发布新事件（转等待，属追平等待的另一种触发）；
-   事件广播的实现：每 Worker 一条 Event Queue 各写入一份拷贝（INVAL 为定长小消息；SNAPSHOT 变长但份数等于 Worker 数）。不采用共享队列——多消费者竞争会破坏单生产者单消费者的保序前提（概设 §10.3.11）。

### 4.4 结果接收与集中调用回放

-   Result Store（walsender 私有）：`txn_hash` 条目挂结果链；就绪堆按 `commit_lsn`（binaryheap）；
-   输出路径（主循环的"输出条件满足"分支）：
    1.  取堆顶事务，确认其结果完整（RELEASE_ACK 已收）；
    2.  在 walsender 侧 rb 中构造影子事务：`ReorderBufferTxnByXid(rb, top_xid, HASH_ENTER, NULL)`，填 `commit_lsn`/`end_lsn`/`commit_time`/origin 与 `base_snapshot`（该事务 `snapshot_version` 的反序列化快照；walsender 持有注册表的反序列化副本）；
    3.  `SetupHistoricSnapshot(影子事务的 base_snapshot, NULL)`；
    4.  逐条 `ParallelDecodeBuildChange`（§7）重建 `ReorderBufferChange`，调用 `rb->begin_cb/change_cb/commit_cb`（即插件，走 `WalSndWriteData` 出口）；
    5.  `TeardownHistoricSnapshot`，`ReorderBufferReturnTXN` 释放影子事务，释放结果条目、`refcount--`、更新已发送位置。

Worker 调用模式下 3/4 替换为：按帧序写网络（帧已含完整协议消息），仍在此处单点发送。

### 4.5 槽位置推进实现

按概设 §4.7 的规则落两处：

-   已发送位置：§4.4 第 4 步更新；
-   `restart_lsn`/`catalog_xmin` 候选：候选 LSN = min(已发送位置, `event_ring[min(在途 Worker 的 consumed_event_id)]` 对应的 LSN)。在既有槽推进计算点（`LogicalIncreaseRestartDecodingForSlot` 一族的调用处）前执行该钳制；`confirmed_flush` 照旧由反馈驱动，不在此路径修改。

------------------------------------------------------------------------

## 5. Worker 侧实现

### 5.1 解码上下文构造（logical.c 新增导出）

``` c
LogicalDecodingContext *
CreateWorkerDecodingContext(const char *plugin_name,
                            OutputPluginWriterPrepareWrite prep_write,
                            OutputPluginWriterWrite do_write);
```

与 `CreateDecodingContext` 的差异：不 acquire 复制槽、不读槽状态、`output_plugin` 仅在 Worker 调用模式下按 `plugin_name` 加载（集中调用模式传 NULL）；writer 回调对由调用方注入（集中调用：忽略；Worker 调用：写 Result Queue 帧缓冲，注入方式对齐 `logicalfuncs.c:205` 的既有先例）。内部 ReorderBuffer 私有实例化，回调注册为 §5.4 的结果生成器。Worker 侧私有状态（快照版本表、结果缓冲、事件消费进度）经 `ctx->output_writer_private` 指向的 `ParallelDecodeWorkerState` 挂接——复用既有私有数据通道，不扩展 `LogicalDecodingContext` 结构。

### 5.2 方案 A 的记录解码

``` c
/* Worker 收到 PD_MSG_TASK_RECORD 后 */
XLogReaderSetDecodeBuffer(reader, msg->rec, msg->rec_len);  /* xlogreader.h:340 */
record = XLogReadRecord(reader, &errm);                     /* 从 decode buffer 解出 */
/* 组 XLogRecordBuffer{reader} 后复用 decode.c 的 Heap 分发表 */
```

需要 decode.c 导出其内部分发表（现 `LogicalDecodingProcessRecord` 内的 rmgr switch 抽成可复用函数 `DecodeDispatchRecord(ctx, buf)`，walsender 与 Worker 共用一份实现）。

### 5.3 提交处理（base_snapshot 形态）

源码事实（本仓库核实）：`ReorderBufferCommit` 签名不含 builder（reorderbuffer.c:2874），提交处理的快照来自 `txn->base_snapshot`（`ReorderBufferReplay` 内 `snapshot_now = txn->base_snapshot`；挂接 API `ReorderBufferSetBaseSnapshot`，reorderbuffer.h:742）。因此 Worker 侧：

``` c
snap = RestoreSnapshot(entry_bytes);            /* snapreg 下发的序列化快照 */
ReorderBufferSetBaseSnapshot(rb, top_xid, lsn, snap);
ReorderBufferCommit(rb, top_xid, commit_lsn, end_lsn,
                    commit_time, origin_id, origin_lsn);
```

`ReorderBufferProcessTXN` 内部照常 `SetupHistoricSnapshot(txn->base_snapshot, txn->tuplecid_hash)`，`tuplecid_hash` 在 Worker 解码中本地构建。概设 §4.2 中"串行路径在 ReorderBufferCommit 内从 builder 取快照"的表述按此修正为"快照在解码期间以 base_snapshot 挂到事务上"。

### 5.4 中间结果生成（集中调用的 rb 回调）

`begin_cb`：初始化结果缓冲（`PdResultBuffer`，上限 `parallel_decode_result_queue_size`，超限走 §5.5 之外的事务级 spill——Worker 侧先按 `logical_decoding_work_mem` spill 变更，结果缓冲只装提交处理产物）；`change_cb`：`ReorderBufferChange` → `PdIntermediateChange`（§7）追加；`commit_cb`：封 `PD_MSG_RESULT_CHANGES` 写 Result Queue。

### 5.5 事件消费（任务处理前）

按消息携带的 `latest_event_id` 排空 Event Queue：`EVENT_INVAL` → 逐条 `LocalExecuteInvalidationMessage`；`EVENT_SNAPSHOT` → `RestoreSnapshot` 入版本表（`pfree` 被淘汰版本）；`EVENT_FLUSH_ALL` → `InvalidateSystemCaches()`。排空完成后原子推进 `consumed_event_id`。

### 5.6 主循环（伪代码）

``` c
for (;;)
{
    /* 阻塞收任务/命令，1s 超时用于心跳 */
    rc = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH, 1000ms);
    if (命令队列可读) { CANCEL → 丢弃当前事务；SHUTDOWN → 退出序列; }
    if (任务队列可读)
    {
        排空事件至 latest_event_id（§5.5）;
        switch (msg type):
          TASK_RECORD → §5.2 解码入本地 rb;
          TASK_COMMIT → §5.3 提交处理 → 结果写 Result Queue
                        → 等待 RELEASE_ACK 期间仅处理命令与心跳;
          TASK_ABORT  → 丢弃本地事务，回收;
    }
    if (超时) 发送 HEARTBEAT{consumed_event_id, state};
}
```

RELEASE_ACK 的语义：walsender 从 Result Queue 取走结果并写入 Result Store 后发送。Worker 收到 ACK 前不清理事务上下文、不置 IDLE——此间 Worker 崩溃，结果已由 walsender 负责（内存或 spill），重放与否依据 walsender 侧的移交记录（概设 §8.2）；walsender 在 ACK 前不将该 Worker 视为可分配（概设 §6.1 的 `BUSY → IDLE` 唯一路径）。等待期间 Worker 不会收到新任务——walsender 未分配，因此只需处理命令与心跳。

------------------------------------------------------------------------

## 6. 并发规则与共享结构定稿

### 6.1 ParallelDecodeWorkerInfo（DSM 内）

``` c
typedef struct ParallelDecodeWorkerInfo
{
    int                 worker_id;
    pg_atomic_uint32    state;          /* PD_WORKER_{STARTING,IDLE,BUSY}，Worker 写 */
    TransactionId       top_xid;        /* 仅 walsender 写（绑定事实） */
    uint64              task_generation;/* 仅 walsender 写 */
    slock_t             pd_mutex;       /* 复合读取用 */
    pg_atomic_uint64    consumed_event_id; /* Worker 写，relaxed；walsender 读 */
    XLogRecPtr          last_input_lsn; /* 仅 walsender 写 */
    XLogRecPtr          last_result_lsn;/* 仅 walsender 写 */
    uint32              startup_generation;
    pid_t               pid;
} ParallelDecodeWorkerInfo;
```

规则：单写字段不加锁（各自只有一方写）；跨字段复合读取（追平判定读 state + consumed_event_id）持 `pd_mutex`；心跳与共享 `state` 的写方分别是 Worker（心跳、state）与 walsender（绑定字段）。

### 6.2 事务条目（walsender 私有）

结构见概设 §10.2；`txn_flags` 位定义：

| 位 | 含义 |
|---|---|
| `PD_TXN_HAS_RESULT 0x01` | 结果已入 Result Store |
| `PD_TXN_SPILLED 0x02` | 结果已 spill（walsender 侧） |
| `PD_TXN_FAILED 0x04` | 失败待处置 |
| `PD_TXN_OUTPUT_DONE 0x08` | 已输出（清理前标志，用后即清） |

------------------------------------------------------------------------

## 7. 中间结果格式（集中调用）

``` c
typedef struct PdIntermediateChange
{
    uint8   op;             /* 'I'/'U'/'D'/'T' */
    Oid     relid;
    /* I/U/D: */
    uint32  newtuple_len; char newtuple[newtuple_len];   /* TOAST 重组完成后的物理元组，可空 */
    uint32  oldtuple_len; char oldtuple[oldtuple_len];   /* 可空 */
    /* T: */
    uint32  nrelids; Oid relids[nrelids]; uint32 truncate_flags;
} PdIntermediateChange;   /* 变长，按偏移序列化 */
```

设计决定：发送 **TOAST 重组完成后**的元组——TOAST 重组在 Worker 完成（需要目录），walsender 回放时无需目录即可调插件；truncate 携带 relid 列表与 flags 对齐 `ReorderBufferTruncate` 语义。

walsender 侧重建：`ParallelDecodeBuildChange(entry) → ReorderBufferGetChange + 填 data.heap / data.truncate + ReorderBufferToastReplace 跳过（已完成）`，然后交给 `rb->change_cb`。元组反序列化产物按 `heap_copytuple` 语义构造 `HeapTuple`。

------------------------------------------------------------------------

## 8. Worker 变更 spill 路径

-   目录：`pg_replslot/<slotname>/pdecode/w<worker_id>/`，FileSet 语义（`ReorderBufferSerializeTXN` 的既有模式，阈值仍为 `logical_decoding_work_mem`）；
-   Worker 对该目录只有创建/写/删自己 `w<worker_id>` 子目录的权限边界（同槽目录，权限天然一致）；
-   清理：Worker 退出时删除自身子目录；`ReplicationSlotDrop` 的目录清理扩展扫除 `pdecode/`；孤儿目录（进程被 kill -9）由槽清理与会话重建路径负责回收（概设 §14.2）。

------------------------------------------------------------------------

## 9. 心跳、超时与关闭序列

| 参数 | 值 | 说明 |
|---|---|---|
| 心跳间隔 | 1 s | HEARTBEAT 携带 `consumed_event_id` 与 `state` |
| 失联阈值 | 30 s | 判定 Worker 失败，按概设 §8.1 处置 |
| 池关闭等待 | 60 s | 超时 WARNING + `dsm_detach` 强制（概设 §6.1 池级 closing 标志） |
| RELEASE_ACK 超时 | 不设 | ACK 只影响 walsender 侧可分配判定，不阻塞 Worker |

关闭序列：置池级 closing → CMD_SHUTDOWN 广播 → IDLE 立即退、BUSY 完成或取消后退出 → 句柄等待（60 s 上限）→ 回收 DSM 与注册项。

------------------------------------------------------------------------

## 10. 错误处理实现

-   Worker：`PG_CATCH` 内 `errstart/errfinish` 捕获 `ErrorData`，按错误/通知协议消息组帧（`pq_beginmessage(&buf, 'E')` + `pq_senderrorfield` 序列），写入 error mq，随后清理退出。
-   walsender：error mq 收到后 `pq_parse_errornotice` 反序列化（parallel.c:1166 既有用法，本仓库核实），追加上下文前缀 `("parallel decode worker %d", worker_id)` 后按现有逻辑解码错误策略 re-raise（终止连接，概设 §3.1）。

------------------------------------------------------------------------

## 11. 分阶段交付物（函数级，对概设 §14.4）

| 阶段 | 交付物 |
|---|---|
| 1 | `paralleldecode.h` 消息与 DSM 层（§2–§3）；池的创建/销毁与启动序列（§2）；echo 任务（TASK→RESULT 原样返回）验证通信与心跳链路（§9）；`WORKERTYPE` 注册（§2.3）；错误通道（§10） |
| 2 | 记录分流（§4.2）；`CreateWorkerDecodingContext`（§5.1）；方案 A 解码与提交处理（§5.2–§5.3）；集中调用中间结果与回放（§4.4、§7）；Result Store 与输出 |
| 3 | 事件层（SNAPSHOT/INVAL/FLUSH_ALL，§4.3、§5.5）；追平等待与槽位置推进（§4.5）；串行回退路径 |
| 4 | Worker 调用模式：帧 writer、startup 参数传递（概设 §5.3）、`pgoutput` 能力声明与审计 |
| 5 | `pg_stat_parallel_decode` 视图（概设 §11.2 映射 `ParallelDecodePoolMetrics`）；队列容量与心跳参数调优 |

## 12. 验收对照

验收标准全部沿用概设 §12，本文对应实现：等价性验收 → §4.4 回放路径与 §5.3；状态机/容器迁移覆盖 → §4.1/§6；流控与活性 → §3.1 容量、§5.6 心跳、§4.5 槽位置推进；故障注入 → §9 关闭序列、§10 错误通道、§8 spill 清理；槽状态等价 → §4.5。
