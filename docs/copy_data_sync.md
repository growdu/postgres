# PostgreSQL 逻辑复制初始表同步（copy_data=true）分析

## 概述

当订阅创建时使用 `copy_data=true`（默认值），PostgreSQL 会在逻辑复制开始前将发布表的现有数据复制到订阅者。这是通过专门的 **tablesync worker** 完成的独立进程。

---

## 核心概念

### 1. 表同步状态机

每个订阅表都有一个同步状态，存储在 `pg_subscription_rel` 目录表中：

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                         表同步状态转换图                                                │
│                                                                                      │
│  ┌─────────┐   启动sync worker   ┌───────────┐   COPY完成     ┌────────────┐        │
│  │   INIT  │ ─────────────────> │  DATASYNC  │ ───────────> │ FINISHEDCOPY │        │
│  │  (初始化) │                   │  (数据同步中) │              │  (COPY已完成)  │        │
│  └─────────┘                    └───────────┘              └────────────┘        │
│                                       │                           │              │
│                                       │                           │   崩溃恢复    │
│                                       │                           │   检查点      │
│                                       ▼                           ▼              │
│                              ┌────────────┐              (不重做COPY)                      │
│                              │  SYNCWAIT  │                                               │
│                              │  (等待Catchup) │                                       │
│                              └────────────┘                                               │
│                                       │                                                │
│                              apply worker                        ┌────────────┐        │
│                              设置CATCHUP                         │  SYNCDONE  │        │
│                                       │                          │ (同步完成)  │        │
│                                       ▼                          └────────────┘        │
│                              ┌────────────┐                                    │         │
│                              │  CATCHUP  │                                    │         │
│                              │ (追赶中)   │                                    │         │
│                              └────────────┘                                    │         │
│                                       │                                        │         │
│                              sync worker                                         │         │
│                              读取流                                      ┌────────────┐        │
│                              应用变更                                    │   READY    │        │
│                              设置SYNCDONE                                │  (就绪)    │        │
│                                       │                                   └────────────┘        │
│                                       ▼                                                 │
│                              apply worker                                         │
│                              确认LSN追上                                          │
│                              设置READY                                          │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 2. 状态定义

| 状态 | 值 | 含义 |
|------|-----|------|
| `INIT` | 'i' | 初始化，sync worker尚未启动 |
| `DATASYNC` | 'd' | 数据正在同步（COPY进行中）|
| `FINISHEDCOPY` | 'f' | COPY阶段已完成 |
| `SYNCWAIT` | 'w' | 等待apply worker设置CATCHUP（仅内存）|
| `CATCHUP` | 'c' | 正在追赶（仅内存）|
| `SYNCDONE` | 's' | 同步完成 |
| `READY` | 'r' | 就绪，正常复制 |
| `UNKNOWN` | '\0' | 未知状态 |

---

## 架构设计

### 3. 为什么使用独立 Tablesync Worker

```
┌───────────────────────────────────────────────────────────────────────────────────────────┐
│                              设计动机                                                      │
│                                                                                          │
│  1. 并行化 Initial Data Synchronization                                                   │
│     ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐                       │
│     │  Tablesync #1   │  │  Tablesync #2   │  │  Tablesync #3   │                       │
│     │  COPY table_a   │  │  COPY table_b   │  │  COPY table_c   │                       │
│     │  (并行)         │  │  (并行)         │  │  (并行)         │                       │
│     └─────────────────┘  └─────────────────┘  └─────────────────┘                       │
│                                                                                          │
│  2. 不阻塞 Apply Worker                                                                  │
│     ┌───────────────────────────────────────────────────────────────────────────┐      │
│     │                        Apply Worker                                         │      │
│     │  跟踪远程wal，应用发布表变更 (订阅表READY后)                                │      │
│     │  在 tablesync 期间，apply worker 可以:                                     │      │
│     │    - 启动其他 tablesync workers                                             │      │
│     │    - 处理其他已就绪表的变更                                                 │      │
│     └───────────────────────────────────────────────────────────────────────────┘      │
│                                                                                          │
│  3. 避免长事务导致膨胀                                                                   │
│     ┌───────────────────────────────────────────────────────────────────────────────┐      │
│     │  如果在单个进程中COPY整个数据库:                                           │      │
│     │    - 需要在整个COPY期间持有xid和LSN                                       │      │
│     │    - 导致更多bloat和磁盘消耗                                              │      │
│     │  独立tablesync worker:                                                    │      │
│     │    - 每个表独立事务                                                       │      │
│     │    - COPY完成后立即推进进度                                               │      │
│     └───────────────────────────────────────────────────────────────────────────────┘      │
│                                                                                          │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

### 4. 进程结构

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                         逻辑复制进程架构                                                    │
│                                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                          Apply Launcher                                              │  │
│  │  启动 Apply Worker 和 Tablesync Workers                                               │  │
│  └─────────────────────────────────────────────────────────────────────────────────────┘  │
│                    │                                    │                                    │
│                    ▼                                    ▼                                    │
│  ┌──────────────────────────────────┐   ┌──────────────────────────────────┐             │
│  │          Apply Worker            │   │      Tablesync Worker #1           │             │
│  │                                  │   │                                  │             │
│  │ • 连接到发布节点                   │   │ • 独立连接到发布节点 (独立slot)     │             │
│  │ • 使用订阅slot                     │   │ • 使用 tablesync slot             │             │
│  │ • 跟踪发布表状态                   │   │ • 执行 COPY table_a              │             │
│  │ • 应用所有READY表的变更            │   │ • 追赶apply worker的进度          │             │
│  │                                  │   │ • COPY完成后通知apply worker      │             │
│  └──────────────────────────────────┘   └──────────────────────────────────┘             │
│                    │                                    │                                    │
│                    │              ┌──────────────────────────────────┐             │
│                    │              │      Tablesync Worker #2           │             │
│                    │              │      (另一个表的同步)            │             │
│                    │              └──────────────────────────────────┘             │
│                    │                                                                  │
│                    │              ┌──────────────────────────────────┐             │
│                    │              │      Tablesync Worker #N           │             │
│                    │              └──────────────────────────────────┘             │
│                    ▼                                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────────────┐       │
│  │                              Publisher                                            │       │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐              │       │
│  │  │ 订阅slot    │  │tablesync   │  │tablesync   │  │tablesync   │              │       │
│  │  │            │  │  slot #1   │  │  slot #2   │  │  slot #N   │              │       │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘              │       │
│  └─────────────────────────────────────────────────────────────────────────────────────┘       │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 详细流程分析

### 5. Tablesync Worker 完整流程

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                    Tablesync Worker 启动和执行流程                                      │
│                                                                                      │
│  TablesyncWorkerMain()                                                               │
│       │                                                                              │
│       ▼                                                                              │
│  SetupApplyOrSyncWorker()  ──> 初始化worker，设置MyLogicalRepWorker                   │
│       │                                                                              │
│       ▼                                                                              │
│  run_tablesync_worker()                                                               │
│       │                                                                              │
│       ├─> start_table_sync()                                                          │
│       │      │                                                                       │
│       │      ├─> LogicalRepSyncTableStart()                                          │
│       │      │      │                                                                │
│       │      │      ├─> 检查表状态 (GetSubscriptionRelState)                        │
│       │      │      │      │                                                         │
│       │      │      │      ▼                                                        │
│       │      │      │      ┌──────────────────────────────┐                         │
│       │      │      │      │ relstate == SYNCDONE/READY?  │───> finish_sync_worker()│
│       │      │      │      │ relstate == INIT/DATASYNC?   │───> 继续              │
│       │      │      │      └──────────────────────────────┘                         │
│       │      │      │                                                                │
│       │      │      ├─> 创建tablesync slot                                          │
│       │      │      │      │                                                         │
│       │      │      │      ▼                                                        │
│       │      │      │  walrcv_create_slot(                                          │
│       │      │      │     slotname,                                                  │
│       │      │      │     false,  // permanent                                       │
│       │      │      │     false,  // two_phase                                      │
│       │      │      │     CRS_USE_SNAPSHOT,  // 让slot使用快照确保一致性               │
│       │      │      │     &origin_startpos)                                          │
│       │      │      │                                                                │
│       │      │      ├─> 建立复制起点跟踪                                              │
│       │      │      │      │                                                         │
│       │      │      │      ▼                                                        │
│       │      │      │  replorigin_advance(originid, origin_startpos, ...)           │
│       │      │      │  replorigin_session_setup(originid, ...)                      │
│       │      │      │                                                                │
│       │      │      └─> 执行 COPY                                                   │
│       │      │             │                                                         │
│       │      │             ▼                                                         │
│       │      │      copy_table()                                                    │
│       │      │             │                                                         │
│       │      │             ├─> fetch_remote_table_info()  // 获取远程表结构            │
│       │      │             │       │                                                   │
│       │      │             │       ▼                                                   │
│       │      │             │  向publisher执行SQL:                                   │
│       │      │             │    SELECT oid, relreplident, relkind                    │
│       │      │             │    SELECT attnum, attname, atttypid, ...               │
│       │      │             │    SELECT pg_get_expr(qual, relid)  // row filter       │
│       │      │             │                                                                │
│       │      │             ├─> 构建COPY命令                                           │
│       │      │             │       │                                                   │
│       │      │             │       ▼                                                   │
│       │      │             │  情况1: 简单表无行过滤                                   │
│       │      │             │     COPY table_name [(columns)] TO STDOUT                 │
│       │      │             │                                                                │
│       │      │             │  情况2: 有行过滤/生成列/非表                             │
│       │      │             │     COPY (SELECT cols FROM table WHERE filter1           │
│       │      │             │                OR filter2 ...) TO STDOUT              │
│       │      │             │                                                                │
│       │      │             ├─> walrcv_exec(COPY command)  // 启动COPY OUT            │
│       │      │             │                                                                │
│       │      │             └─> BeginCopyFrom() + CopyFrom()  // 本地写入              │
│       │      │                    │                                                 │
│       │      │                    │ 使用copy_read_data回调从远程读取                   │
│       │      │                    │    │                                             │
│       │      │                    │    ▼                                             │
│       │      │                    │  walrcv_receive() ──> 数据缓冲                     │
│       │      │                    │    │                                             │
│       │      │                    │    ▼                                             │
│       │      │                    │  写入本地表                                        │
│       │      │                                                                       │
│       │      ├─> 更新状态为 FINISHEDCOPY                                              │
│       │      │                                                                       │
│       │      ├─> 设置状态为 SYNCWAIT (内存)                                          │
│       │      │                                                                       │
│       │      └─> wait_for_worker_state_change(CATCHUP)  // 等待apply worker           │
│       │                                                                            │
│       ├─> walrcv_startstreaming()  // 开始流式接收                                   │
│       │                                                                            │
│       └─> start_apply()  // 追赶apply worker的进度                                   │
│                  │                                                                 │
│                  ▼                                                                 │
│           process_syncing_tables_for_sync()                                          │
│                  │                                                                 │
│                  ├─> 状态 == CATCHUP && current_lsn >= relstate_lsn                  │
│                  │       │                                                           │
│                  │       ▼                                                           │
│                  │  设置状态为 SYNCDONE                                             │
│                  │  丢弃tablesync slot                                              │
│                  │  删除复制起点跟踪                                                 │
│                  │  finish_sync_worker()                                             │
│                  │                                                                   │
│                  └─> 否则继续应用变更直到追上                                         │
│                                                                                      │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 6. Apply Worker 协同流程

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                    Apply Worker 与 Tablesync Worker 协同                            │
│                                                                                      │
│  process_syncing_tables_for_apply()                                                 │
│       │                                                                              │
│       ├─> FetchTableStates()  // 获取所有非READY表状态                                │
│       │                                                                              │
│       ├─> foreach table in table_states_not_ready                                  │
│       │      │                                                                       │
│       │      ▼                                                                       │
│       │  ┌──────────────────────────────────────────────────────────────┐           │
│       │  │ if state == SYNCDONE:                                          │           │
│       │  │      if current_lsn >= rstate->lsn:                            │           │
│       │  │          rstate->state = READY                                │           │
│       │  │          UpdateSubscriptionRelState(..., READY, ...)           │           │
│       │  │      // 等待tablesync完成并追上                                 │           │
│       │  └──────────────────────────────────────────────────────────────┘           │
│       │      │                                                                       │
│       │      ▼                                                                       │
│       │  ┌──────────────────────────────────────────────────────────────┐           │
│       │  │ else (state == INIT/DATASYNC/FINISHEDCOPY):                     │           │
│       │  │      │                                                          │           │
│       │  │      ├─> 查找是否有sync worker正在同步此表                      │           │
│       │  │      │                                                          │           │
│       │  │      │  if 没有sync worker:                                      │           │
│       │  │      │      │                                                     │           │
│       │  │      │      │  if nsyncworkers < max_sync_workers:                │           │
│       │  │      │      │      │                                              │           │
│       │  │      │      │      │  if (now - last_start_time > retry_interval)│           │
│       │  │      │      │      │      │                                         │           │
│       │  │      │      │      │      ▼                                         │           │
│       │  │      │      │      │  logicalrep_worker_launch(TABLESYNC, ...)    │           │
│       │  │      │      │      │  // 启动新的tablesync worker                  │           │
│       │  │      │      │      │                                              │           │
│       │  │      │      │      └──────                                       │           │
│       │  │      │      │                                                     │           │
│       │  │      │      └─────                                                │           │
│       │  │      │                                                            │           │
│       │  │      ▼                                                            │           │
│       │  │  ┌────────────────────────────────────────────────────────────┐   │           │
│       │  │  │ if syncworker存在 && state == SYNCWAIT:                     │   │           │
│       │  │  │      │                                                        │   │           │
│       │  │  │      ▼                                                        │   │           │
│       │  │  │  // 通知sync worker可以追赶                                  │   │           │
│       │  │  │  syncworker->relstate = CATCHUP                              │   │           │
│       │  │  │  syncworker->relstate_lsn = Max(sync_lsn, current_lsn)       │   │           │
│       │  │  │                                                            │   │           │
│       │  │  │  // 唤醒sync worker                                         │   │           │
│       │  │  │  logicalrep_worker_wakeup_ptr(syncworker)                   │   │           │
│       │  │  │                                                            │   │           │
│       │  │  │  // 等待sync worker完成                                     │   │           │
│       │  │  │  wait_for_relation_state_change(relid, SYNCDONE)            │   │           │
│       │  │  └────────────────────────────────────────────────────────────┘   │           │
│       │  └─────                                                                │           │
│       │                                                                        │           │
│       └─> foreach table in table_states_not_ready                              │
│              └─> // 如果所有表都READY，设置twophase为ENABLED                   │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 7. 数据一致性保证

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              COPY 一致性保证                                              │
│                                                                                          │
│  问题:                                                                                  │
│    COPY执行期间，远程表可能有新的DML变更                                                 │
│    如何确保COPY的数据与后续变更不冲突?                                                  │
│                                                                                          │
│  解决: 使用REPEATABLE READ + 独立复制slot                                               │
│  ┌───────────────────────────────────────────────────────────────────────────────┐      │
│  │                                                                                │      │
│  │  1. BEGIN READ ONLY ISOLATION LEVEL REPEATABLE READ                           │      │
│  │     // 在远程启动一个事务，确保后续操作看到一致的数据                           │      │
│  │                                                                                │      │
│  │  2. walrcv_create_slot(..., CRS_USE_SNAPSHOT, &startpos)                      │      │
│  │     // 在publisher创建逻辑slot，使用REPEATABLE READ快照                         │      │
│  │     // 这个快照与步骤1的事务一致                                               │      │
│  │                                                                                │      │
│  │  3. COPY TO STDOUT                                                             │      │
│  │     // 复制数据，此数据与步骤1-2创建的快照一致                                  │      │
│  │                                                                                │      │
│  │  4. COMMIT                                                                     │      │
│  │     // COPY完成，事务结束                                                      │      │
│  │                                                                                │      │
│  │  5. replorigin_advance(originid, startpos, ...)                               │      │
│  │     // 在本地推进复制起点到COPY开始位置                                        │      │
│  │     // 确保后续从startpos开始应用，不会重复COPY的数据                         │      │
│  │                                                                                │      │
│  └───────────────────────────────────────────────────────────────────────────────┘      │
│                                                                                          │
│  结果:                                                                                  │
│    ┌─────────────────────────────────────────────────────────────────────────┐        │
│  │                    Time ─────────────────────────────────────────────────►│        │
│  │                                                                           │        │
│  │  LSN:startpos ◄────────────────────────────────── │                       │        │
│  │         │                                        │                          │        │
│  │         │  COPY返回的数据                       │  后续变更(from startpos) │        │
│  │         │  (consistent with snapshot)           │  (applied later)        │        │
│  │         │                                        │                          │        │
│  │  ┌──────┴───────────────────────────────────────┴──────┐                │        │
│  │  │ 复制起点 = startpos, 从这里开始应用后续变更           │                │        │
│  │  └──────────────────────────────────────────────────────┘                │        │
│  └───────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 关键代码分析

### 8. Tablesync Slot 创建

```c
/*
 * 创建用于tablesync的永久逻辑解码slot
 * 使用CRS_USE_SNAPSHOT确保COPY与slot一致
 */
walrcv_create_slot(LogRepWorkerWalRcvConn,
                    slotname,          // "pg_%u_sync_%u_" UINT64_FORMAT
                    false,             // permanent
                    false,             // two_phase
                    MySubscription->failover,
                    CRS_USE_SNAPSHOT,   // 使用快照确保一致性
                    origin_startpos);  // 返回slot的起始LSN
```

**为什么需要独立Slot?**
1. 隔离：每个tablesync worker有自己的slot，不会影响apply worker
2. 位置追踪：通过`replorigin`追踪每个表的同步进度
3. 崩溃恢复：如果tablesync crash，可以从FINISHEDCOPY状态恢复，不重做COPY

### 9. COPY 命令构建

```c
/* 情况1: 简单表（无行过滤、无生成列）*/
if (lrel.relkind == RELKIND_RELATION && qual == NIL && !gencol_published)
{
    appendStringInfo(&cmd, "COPY %s", quote_qualified_identifier(nspname, relname));
    // 加上列名
    if (lrel.natts)
    {
        appendStringInfoChar(&cmd, '(');
        for (int i = 0; i < lrel.natts; i++)
        {
            if (i > 0) appendStringInfoString(&cmd, ", ");
            appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
        }
        appendStringInfoChar(&cmd, ')');
    }
    appendStringInfoString(&cmd, " TO STDOUT");
}

/* 情况2: 有行过滤/生成列/非表 */
else
{
    /* COPY (SELECT columns FROM table WHERE filter1 OR filter2 ...) TO STDOUT */
    appendStringInfoString(&cmd, "COPY (SELECT ");
    for (int i = 0; i < lrel.natts; i++)
        appendStringInfoString(&cmd, quote_identifier(lrel.attnames[i]));
    appendStringInfoString(&cmd, " FROM ");
    if (lrel.relkind == RELKIND_RELATION)
        appendStringInfoString(&cmd, "ONLY ");
    appendStringInfoString(&cmd, quote_qualified_identifier(nspname, relname));
    // 添加OR连接的过滤条件
    if (qual != NIL)
    {
        foreach(lc, qual)
            appendStringInfo(&cmd, " WHERE %s", strVal(lfirst(lc)));
    }
    appendStringInfoString(&cmd, ") TO STDOUT");
}
```

### 10. 数据复制回调

```c
/*
 * copy_read_data - COPY FROM的数据源回调
 * 从远程连接读取COPY数据，传递给本地COPY
 */
static int
copy_read_data(void *outbuf, int minread, int maxread)
{
    // 先检查缓冲区中是否有剩余数据
    avail = copybuf->len - copybuf->cursor;
    if (avail > 0)
    {
        memcpy(outbuf, &copybuf->data[copybuf->cursor], Min(avail, maxread));
        copybuf->cursor += avail;
        bytesread += avail;
    }

    // 循环从远程读取直到满足请求
    while (maxread > 0 && bytesread < minread)
    {
        len = walrcv_receive(LogRepWorkerWalRcvConn, &buf, &fd);

        if (len > 0)
        {
            copybuf->data = buf;
            copybuf->len = len;
            copybuf->cursor = 0;
            // 处理新数据...
        }

        // 等待更多数据或超时
        WaitLatchOrSocket(MyLatch, WL_SOCKET_READABLE | ...);
    }

    return bytesread;
}
```

---

## 状态转换时序示例

### 11. 正常流程

```
Time    Tablesync Worker #1          Apply Worker              Publisher
----    -----------------          ------------              --------

T1                              启动，状态INIT          收到 Apply启动
        启动
T2      设置状态=DATASYNC
        创建tablesync slot
        BEGIN REPEATABLE READ
T3      COPY table_a ...          检查表状态              响应COPY数据
        (发送数据)                看到state=DATASYNC
                                 等待...                   ...
T4      COPY完成                  等待...                   等待...
T5      COMMIT                    看到state=FINISHEDCOPY
        设置state=SYNCWAIT(内存)
        等待CATCHUP
                                  发现state=SYNCWAIT
T6                              设置state=CATCHUP(内存)   tablesync slot开始
        收到CATCHUP               唤醒sync worker           跟踪位置startpos
        开始流式接收
T7      应用变更直到LSN=startpos   等待SYNCDONE              发送变更
        设置state=SYNCDONE
        丢弃slot
        finish_sync_worker()
                                  看到state=SYNCDONE
T8                              设置state=READY
                                  (所有表已就绪)
```

### 12. 崩溃恢复

```
场景: Tablesync Worker在T5时刻崩溃 (COPY完成后但尚未完成catchup)

恢复流程:
1. Apply Worker检测到sync worker消失
2. 重启tablesync worker
3. 新worker检查状态:
   - relstate == FINISHEDCOPY (不重做COPY)
   - 直接跳到copy_table_done标签
4. 设置state=SYNCWAIT，继续catchup流程
```

---

## 内存管理

### 13. COPY Buffer 管理

```c
static StringInfo copybuf = NULL;  // 全局COPY缓冲区

/*
 * copy_read_data使用此缓冲区:
 * 1. 先检查缓冲区中是否有上次残留数据
 * 2. 通过walrcv_receive从远程读取新数据
 * 3. 逐步返回数据给CopyFrom()
 */
```

---

## 与 Two-Phase 提交的关系

### 14. Tablesync 与 Two-Phase

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                         Two-Phase 与 Tablesync 的关系                                       │
│                                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐   │
│  │  MySubscription->twophasestate 状态:                                               │   │
│  │                                                                                    │   │
│  │  LOGICALREP_TWOPHASE_STATE_DISABLED  ─── 用户禁用了两阶段提交                     │   │
│  │  LOGICALREP_TWOPHASE_STATE_PENDING      ─── 有tablesync未完成                      │   │
│  │  LOGICALREP_TWOPHASE_STATE_ENABLED     ─── 所有tablesync完成，两阶段已启用         │   │
│  └────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐   │
│  │  process_syncing_tables_for_apply() 中的处理:                                     │   │
│  │                                                                                    │   │
│  │  if (MySubscription->twophasestate == LOGICALREP_TWOPHASE_STATE_PENDING)        │   │
│  │  {                                                                               │   │
│  │      if (AllTablesyncsReady())  // 所有表都READY                                │   │
│  │      {                                                                           │   │
│  │          // 退出apply worker，让launcher重启它                                   │   │
│  │          // 重启后twophasestate将变为ENABLED                                     │   │
│  │          should_exit = true;                                                    │   │
│  │      }                                                                           │   │
│  │  }                                                                               │   │
│  └────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                             │
│  原因:                                                                                    │
│    两阶段提交通知只有在所有表同步完成后才能发送                                            │
│    因为tablesync期间可能有不一致的数据                                                    │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 总结

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              Initial Table Sync 要点                                        │
│                                                                                             │
│  1. 独立Tablesync Worker:每个表有独立的worker进程进行COPY                                  │
│                                                                                             │
│  2. 状态机: INIT → DATASYNC → FINISHEDCOPY → SYNCWAIT → CATCHUP → SYNCDONE → READY        │
│                                                                                             │
│  3. 一致性保证:                                                                             │
│     - 远程使用REPEATABLE READ事务                                                          │
│     - 创建独立slot使用CRU_USE_SNAPSHOT                                                     │
│     - COPY数据与后续变更基于一致的快照                                                    │
│                                                                                             │
│  4. 复制起点跟踪:                                                                          │
│     - 每个tablesync有独立的replorigin记录                                                 │
│     - COPY完成后起点设为startpos                                                          │
│     - 后续变更从startpos开始应用                                                          │
│                                                                                             │
│  5. 崩溃恢复:                                                                              │
│     - FINISHEDCOPY状态表示COPY已完成，不重做                                              │
│     - 使用replorigin恢复进度                                                              │
│                                                                                             │
│  6. 并行化:                                                                                │
│     - 多个tablesync worker并行工作                                                         │
│     - 通过max_sync_workers_per_subscription控制                                           │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 相关文件

| 文件 | 作用 |
|------|------|
| `src/backend/replication/logical/tablesync.c` | Tablesync Worker实现 |
| `src/include/catalog/pg_subscription_rel.h` | 表状态定义 |
| `src/backend/replication/logical/worker.c` | Apply Worker主循环 |
| `src/backend/replication/logical/launcher.c` | Worker启动器 |