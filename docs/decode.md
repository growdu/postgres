# PostgreSQL 逻辑解码（Logical Decoding）架构分析

## 目录
1. [概述](#1-概述)
2. [核心组件](#2-核心组件)
3. [架构图](#3-架构图)
4. [decode.c 详解](#4-decodec-详解)
5. [reorderbuffer.c 详解](#5-reorderbufferc-详解)
6. [snapbuild.c 详解](#6-snapbuildc-详解)
7. [典型流程分析](#7-典型流程分析)
8. [内存管理机制](#8-内存管理机制)
9. [关键数据结构](#9-关键数据结构)

---

## 1. 概述

逻辑解码是PostgreSQL将WAL（Write-Ahead Logging）转换为可读懂的数据变化流的核心机制。它允许外部消费者（如订阅者）以逻辑方式接收数据变化，而不是物理复制整个数据页。

```
┌─────────────┐     WAL      ┌──────────────────┐     逻辑变化流     ┌─────────────┐
│  Publisher  │ ──────>> │  Logical Decoding │ ─────────────>>  │  Subscriber │
└─────────────┘           └──────────────────┘                   └─────────────┘
```

### 核心设计目标
1. **事务重组**：将乱序的WAL记录重组成完整事务
2. **快照构建**：构建历史目录快照用于可见性判断
3. **过滤与转换**：根据发布内容过滤并转换为输出插件格式

---

## 2. 核心组件

```
┌──────────────────────────────────────────────────────────────────────┐
│                    LogicalDecodingContext                            │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────────────┐   │
│  │    XLogReader  │  │  ReorderBuffer   │  │    SnapBuild        │   │
│  │                │  │                 │  │                     │   │
│  │ 读取WAL记录    │  │ 事务重排序       │  │ 构建历史MVCC快照     │   │
│  └────────────────┘  └─────────────────┘  └──────────────────────┘   │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │                      Output Plugin                              │ │
│  │  (pgoutput, test_decoding, etc.)                               │ │
│  └────────────────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────────┘
```

| 组件 | 文件 | 职责 |
|------|------|------|
| **decode.c** | `src/backend/replication/logical/decode.c` | WAL记录解码入口，分发到不同rmgr处理器 |
| **reorderbuffer.c** | `src/backend/replication/logical/reorderbuffer.c` | 事务重排序、重组、大事务溢出到磁盘 |
| **snapbuild.c** | `src/backend/replication/logical/snapbuild.c` | 构建历史目录快照，支持可见性判断 |

---

## 3. 架构图

### 3.1 整体架构

```
                                    ┌─────────────────────────────────────────┐
                                    │          LogicalDecodingContext          │
                                    │  (逻辑解码上下文，核心数据结构)            │
                                    └─────────────────────────────────────────┘
                                                      │
                    ┌─────────────────────────────────┼─────────────────────────────────┐
                    │                                 │                                 │
                    ▼                                 ▼                                 ▼
         ┌─────────────────────┐           ┌─────────────────────┐           ┌─────────────────────┐
         │     XLogReader     │           │   ReorderBuffer     │           │     SnapBuild       │
         │                   │           │                     │           │                     │
         │ • ReadRecPtr      │           │ • by_txn (HTAB)     │           │ • committed[]      │
         │ • EndRecPtr       │           │ • toplevel_by_lsn   │           │ • xmin/xmax        │
         │ • record          │           │ • txn_heap          │           │ • state            │
         └─────────────────────┘           └─────────────────────┘           └─────────────────────┘
                    │                                 │                                 │
                    │                                 │                                 │
                    ▼                                 ▼                                 ▼
         ┌─────────────────────┐           ┌─────────────────────┐           ┌─────────────────────┐
         │    XLogRecordBuffer  │           │    ReorderBufferTXN   │           │      Snapshot       │
         │                     │           │                     │           │                     │
         │ • origptr          │◄───────────│ • xid               │           │ • xmin             │
         │ • endptr          │           │ • first_lsn          │           │ • xmax             │
         │ • record          │           │ • changes (dlist)    │           │ • xip[]            │
         └─────────────────────┘           │ • subtxns (dlist)   │           │ • subxip[]         │
                                            └─────────────────────┘           └─────────────────────┘
                                                      │
                                                      │ 根据xid哈希查找
                                                      ▼
                                            ┌─────────────────────┐
                                            │  ReorderBufferChange │
                                            │                     │
                                            │ • action (INSERT/    │
                                            │       UPDATE/DELETE) │
                                            │ • data.tp           │
                                            │   (rlocator,        │
                                            │    oldtuple,        │
                                            │    newtuple)        │
                                            └─────────────────────┘
```

### 3.2 WAL到逻辑变化的完整流程

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                     WAL Stream                                                          │
│  ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐ ┌────────┐           │
│  │ INSERT │ │ UPDATE │ │ DELETE │ │  CID   │ │ COMMIT │ │INSERT  │ │ UPDATE │ │ COMMIT │           │
│  │ (txn1) │ │ (txn1) │ │ (txn1) │ │ (txn1) │ │ (txn1) │ │ (txn2) │ │ (txn2) │ │ (txn2) │           │
│  │  LSN:1 │ │  LSN:2 │ │  LSN:3 │ │  LSN:4 │ │  LSN:5 │ │  LSN:6 │ │  LSN:7 │ │  LSN:8 │           │
│  └────┬───┘ └────┬───┘ └────┬───┘ └────┬───┘ └───┬───┘ └────┬───┘ └────┬───┘ └───┬───┘           │
│       │          │          │          │          │          │          │          │          │           │
└───────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼──────────┼───────┘
        │          │          │          │          │          │          │          │          │
        ▼          ▼          ▼          ▼          ▼          ▼          ▼          ▼          ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  LogicalDecodingProcessRecord()                                                                     │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 1. 提取 XLogRecordBuffer (origptr, endptr, record)                                             │  │
│  │ 2. 获取顶层事务 xid = XLogRecGetTopXid(record)                                                 │  │
│  │ 3. 调用 ReorderBufferAssignChild() 建立子事务关联                                              │  │
│  │ 4. 根据 rmgr 调用对应解码函数:                                                                  │  │
│  │    - heap_decode()     → INSERT/UPDATE/DELETE/TRUNCATE                                        │  │
│  │    - heap2_decode()   → MULTI_INSERT/NEW_CID                                                  │  │
│  │    - xact_decode()    → COMMIT/PREPARE/ABORT                                                  │  │
│  │    - standby_decode() → RUNNING_XACTS                                                        │  │
│  │    - logicalmsg_decode() → 逻辑消息                                                           │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        │
        │ INSERT/UPDATE/DELETE
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  ReorderBufferQueueChange()                                                                          │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │ 1. ReorderBufferTXNByXid() 查找或创建事务                                                     │  │
│  │ 2. 分配 ReorderBufferChange 并填充数据                                                        │  │
│  │ 3. dlist_push_tail(&txn->changes, &change->node)  加入事务变更列表                            │  │
│  │ 4. 调用 ReorderBufferChangeMemoryUpdate() 更新内存统计                                        │  │
│  │ 5. 调用 ReorderBufferCheckMemoryLimit() 检查是否需要溢出到磁盘                               │  │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        │
        │ COMMIT记录
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  DecodeCommit()                                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ 1. SnapBuildCommitTxn() - 更新快照构建器的已提交事务列表                                       │        │
│  │ 2. 检查 DecodeTXNNeedSkip() - 是否需要跳过此事务                                              │        │
│  │ 3. 对每个子事务调用 ReorderBufferCommitChild()                                                │        │
│  │ 4. 调用 ReorderBufferCommit() / ReorderBufferFinishPrepared()                                │        │
│  │    └─> 遍历事务+子事务的所有changes                                                         │        │
│  │    └─> 调用 output_plugin 的 begin()/apply_change()/commit() 回调                            │        │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│  Output Plugin (e.g., pgoutput)                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────────────────────────────┐        │
│  │ 生成逻辑复制协议消息:                                                                            │        │
│  │   - BEGIN                                                                                  │        │
│  │   - TABLE relation_oid: ...                                                                 │        │
│  │   - INSERT/UPDATE/DELETE tuples                                                           │        │
│  │   - COMMIT                                                                                 │        │
│  └─────────────────────────────────────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 快照状态机

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              SnapBuild 状态转换图                                              │
│                                                                                             │
│   ┌──────────────────┐                                                                      │
│   │      START        │◄─────────────────────────────┐                                        │
│   │  (初始状态)       │                              │                                        │
│   └────────┬─────────┘                              │                                        │
│            │                                        │                                        │
│            │ 收到 xl_running_xacts                 │                                        │
│            │ (无运行事务)                           │                                        │
│            ▼                                        │                                        │
│   ┌──────────────────────────┐    ┌──────────────────────────┐                              │
│   │   BUILDING_SNAPSHOT       │───►│     FULL_SNAPSHOT        │──────────────────┐          │
│   │   (正在构建初始快照)      │    │   (已构建完整快照)        │                  │          │
│   └──────────────────────────┘    └──────────────────────────┘                  │          │
│            │                                                                   │          │
│            │ 所有BUILDING期间                                              │ 收到xl_running_xacts   │
│            │ 的运行事务都已                                                │ (BUILDING期间运行的   │
│            │ 提交或中止                                                    │  事务都已完成)        │
│            ▼                                                                   │          │
│   ┌──────────────────────────┐    ┌──────────────────────────────────────────┘          │
│   │     CONSISTENT            │────│  (已达一致点，可开始逻辑解码)                                │
│   │                          │◄───┘                                                          │
│   └──────────────────────────┘                                                               │
│                                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐ │
│  │ 状态说明:                                                                                │ │
│  │ • START: 初始状态，等待足够新的xl_running_xacts记录                                       │ │
│  │ • BUILDING_SNAPSHOT: 正在收集已提交事务，构建初始目录快照                                 │ │
│  │ • FULL_SNAPSHOT: 已构建完整快照，可解码新事务，但需等待BUILDING期间事务完成                │ │
│  │ • CONSISTENT: 达到一致点，所有历史事务都已完成，可安全开始逻辑解码                         │ │
│  └────────────────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

### 3.4 事务重排序机制

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              事务重排序 (K-Way Merge Sort)                                    │
│                                                                                             │
│   WAL中的事务可能是乱序的:                                                                  │
│   ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                                          │
│   │ T1  │ │ T2  │ │ T1  │ │ T3  │ │ T2  │ │ T3  │                                          │
│   │ C   │ │ I   │ │ D   │ │ I   │ │ C   │ │ C   │                                          │
│   │ LSN │ │ LSN │ │ LSN │ │ LSN │ │ LSN │ │ LSN │                                          │
│   │ 1   │ │ 2   │ │ 3   │ │ 4   │ │ 5   │ │ 6   │                                          │
│   └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                                          │
│                                                                                             │
│   ReorderBuffer按事务ID分组:                                                                 │
│   ┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐   │
│   │    Transaction T1       │  │    Transaction T2       │  │    Transaction T3       │   │
│   │  ┌─────────────────┐   │  │  ┌─────────────────┐   │  │  ┌─────────────────┐   │   │
│   │  │ Change I (LSN:2)│   │  │  │ Change I (LSN:2)│   │  │  │ Change I (LSN:4)│   │   │
│   │  │ Change D (LSN:3)│   │  │  │                 │   │  │  │                 │   │   │
│   │  └─────────────────┘   │  │  └─────────────────┘   │  │  └─────────────────┘   │   │
│   │  Commit LSN:5         │  │  Commit LSN:5         │  │  Commit LSN:6         │   │
│   └─────────────────────────┘  └─────────────────────────┘  └─────────────────────────┘   │
│                                                                                             │
│   二叉堆 (Binary Heap) 用于K路归并:                                                          │
│                              ┌─────────┐                                                     │
│                              │  Heap   │                                                     │
│                              │    3    │  ← 最小LSN                                         │
│                            ┌─┴─────┬───┴─┐                                                   │
│                           2       │   4                                                   │
│                          [T2]    [T3] [T1]                                                  │
│                                                                                             │
│   按LSN顺序输出给Output Plugin:                                                             │
│   ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐ ┌─────┐                                          │
│   │ T2  │ │ T1  │ │ T1  │ │ T3  │ │ T2  │ │ T3  │                                          │
│   │  I  │ │  I  │ │  D  │ │  I  │ │ COMMIT│ │ COMMIT│                                    │
│   │ LSN2│ │ LSN3│ │ LSN5│ │ LSN6│ │      │ │      │                                    │
│   └─────┘ └─────┘ └─────┘ └─────┘ └─────┘ └─────┘                                          │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 4. decode.c 详解

### 4.1 核心入口函数

```c
void
LogicalDecodingProcessRecord(LogicalDecodingContext *ctx, XLogReaderState *record)
```

**执行流程:**

```
LogicalDecodingProcessRecord()
    │
    ├─> 1. 构建 XLogRecordBuffer
    │      buf.origptr = ctx->reader->ReadRecPtr
    │      buf.endptr = ctx->reader->EndRecPtr
    │      buf.record = record
    │
    ├─> 2. 获取顶层事务ID
    │      txid = XLogRecGetTopXid(record)
    │
    ├─> 3. 建立子事务关联
    │      if (TransactionIdIsValid(txid))
    │          ReorderBufferAssignChild(ctx->reorder, txid, subxid, buf.origptr)
    │
    └─> 4. 根据RMGR调用对应解码器
           rmgr = GetRmgr(XLogRecGetRmid(record))
           if (rmgr.rm_decode != NULL)
               rmgr.rm_decode(ctx, &buf)  // 调用具体解码函数
           else
               ReorderBufferProcessXid(ctx->reorder, xid, buf.origptr)
```

### 4.2 RMGR解码函数分发

| RMGR ID | 解码函数 | 处理内容 |
|---------|----------|---------|
| `XLOG_ID` | `xlog_decode()` | 检查点、日志参数变更 |
| `XACT_ID` | `xact_decode()` | COMMIT/ABORT/PREPARE/INVALIDATIONS |
| `STANDBY_ID` | `standby_decode()` | RUNNING_XACTS |
| `HEAP_ID` | `heap_decode()` | INSERT/UPDATE/DELETE/TRUNCATE |
| `HEAP2_ID` | `heap2_decode()` | MULTI_INSERT/NEW_CID |
| `LOGICALMSG_ID` | `logicalmsg_decode()` | 逻辑消息 |

### 4.3 Commit处理流程 (DecodeCommit)

```
DecodeCommit()
    │
    ├─> 1. 解析来源信息
    │      origin_lsn = parsed->origin_lsn
    │      commit_time = parsed->origin_timestamp
    │
    ├─> 2. 更新快照构建器
    │      SnapBuildCommitTxn(builder, origptr, xid,
    │                          nsubxacts, subxacts, xinfo)
    │
    ├─> 3. 检查是否跳过此事务
    │      if (DecodeTXNNeedSkip(...))
    │          // 可能是其他数据库、不感兴趣的原点、fast_forward模式
    │          ReorderBufferForget(ctx->reorder, subxacts[i], buf->origptr)
    │          return
    │
    ├─> 4. 处理子事务
    │      for (i = 0; i < nsubxacts; i++)
    │          ReorderBufferCommitChild(ctx->reorder, xid, subxacts[i],
    │                                  buf->origptr, buf->endptr)
    │
    └─> 5. 提交事务到重排序缓冲区
           if (two_phase)
               ReorderBufferFinishPrepared(...)  // 两阶段提交
           else
               ReorderBufferCommit(...)         // 普通提交
               │
               ├─> 遍历事务+子事务的所有changes
               ├─> 调用 output_plugin begin/commit 回调
               └─> 释放内存
```

---

## 5. reorderbuffer.c 详解

### 5.1 核心数据结构

**ReorderBufferTXN - 事务状态:**
```c
typedef struct ReorderBufferTXN
{
    bits32      txn_flags;           // 事务标志
    TransactionId xid;              // 事务ID
    TransactionId toplevel_xid;     // 顶层事务ID

    XLogRecPtr  first_lsn;          // 第一条记录的LSN
    XLogRecPtr  final_lsn;           // 提交/Prepare记录LSN
    XLogRecPtr  end_lsn;             // 提交记录结束位置+1

    struct ReorderBufferTXN *toptxn; // 顶层事务引用

    Snapshot   base_snapshot;        // 基础快照
    XLogRecPtr base_snapshot_lsn;    // 快照对应的LSN

    uint64     nentries;             // 总变更数
    uint64     nentries_mem;         // 内存中的变更数

    dlist_head changes;             // 变更列表
    dlist_head subtxns;              // 子事务列表
    dlist_head tuplecids;            // (ctid->cmin/cmax)映射

    Size       size;                  // 内存大小(字节)
    Size       total_size;            // 含子事务的总大小
} ReorderBufferTXN;
```

**事务标志 (txn_flags):**
```c
#define RBTXN_HAS_CATALOG_CHANGES    0x0001  // 包含目录修改
#define RBTXN_IS_SUBXACT             0x0002  // 是子事务
#define RBTXN_IS_SERIALIZED          0x0004  // 已溢出到磁盘
#define RBTXN_IS_STREAMED            0x0010  // 已流式传输
#define RBTXN_HAS_PARTIAL_CHANGE     0x0020  // 包含部分变更(TOAST/ speculation)
#define RBTXN_IS_PREPARED            0x0040  // 已Prepare
#define RBTXN_IS_COMMITTED           0x0400  // 已提交
#define RBTXN_IS_ABORTED             0x0800  // 已中止
```

### 5.2 事务查找与创建

```c
static ReorderBufferTXN *
ReorderBufferTXNByXid(ReorderBuffer *rb, TransactionId xid,
                     bool create, bool *is_new,
                     XLogRecPtr lsn, bool create_as_top)
{
    // 1. 单条目缓存查找
    if (rb->by_txn_last_xid == xid && rb->by_txn_last_txn != NULL)
        return rb->by_txn_last_txn;

    // 2. 哈希表查找
    ent = hash_search(rb->by_txn, &xid, create ? HASH_ENTER : HASH_FIND, &found);

    if (found)
        txn = ent->txn;
    else if (create)
    {
        // 3. 创建新事务
        ent->txn = ReorderBufferAllocTXN(rb);
        ent->txn->xid = xid;
        ent->txn->first_lsn = lsn;

        if (create_as_top)
            dlist_push_tail(&rb->toplevel_by_lsn, &txn->node);
    }

    // 4. 更新缓存
    rb->by_txn_last_xid = xid;
    rb->by_txn_last_txn = txn;

    return txn;
}
```

### 5.3 变更队列与内存管理

```
ReorderBufferQueueChange()
    │
    ├─> 1. 查找/创建事务
    │      txn = ReorderBufferTXNByXid(rb, xid, true, NULL, lsn, true)
    │
    ├─> 2. 检查事务是否已中止
    │      if (rbtxn_is_aborted(txn))
    │          ReorderBufferFreeChange(rb, change, false)
    │          return
    │
    ├─> 3. 标记为可流式传输
    │      if (change->action == INSERT/UPDATE/DELETE/TRUNCATE/MESSAGE)
    │          toptxn->txn_flags |= RBTXN_HAS_STREAMABLE_CHANGE
    │
    ├─> 4. 加入变更列表
    │      change->lsn = lsn
    │      dlist_push_tail(&txn->changes, &change->node)
    │      txn->nentries++
    │      txn->nentries_mem++
    │
    ├─> 5. 更新内存统计
    │      ReorderBufferChangeMemoryUpdate(rb, change, NULL, true, size)
    │
    ├─> 6. 处理部分变更 (TOAST/speculative insertion)
    │      ReorderBufferProcessPartialChange(...)
    │
    └─> 7. 检查内存限制
            ReorderBufferCheckMemoryLimit(rb)
                │
                ├─> 超过 logical_decoding_work_mem
                └─> 溢出最大事务到磁盘
```

### 5.4 K-Way归并迭代器

ReorderBuffer使用二叉堆实现K路归并，对事务及其子事务的变更按LSN排序:

```c
ReorderBufferIterTXNInit()
    │
    ├─> 1. 计算事务数量: toplevel + subtxns with nentries>0
    │
    ├─> 2. 分配迭代状态
    │      state = MemoryContextAllocZero(..., sizeof(...) + nr_txns * sizeof(...))
    │
    ├─> 3. 如果事务已溢出，从磁盘恢复
    │      if (rbtxn_is_serialized(txn))
    │          ReorderBufferSerializeTXN(rb, txn)
    │          ReorderRestoreChanges(rb, txn, &file, &segno)
    │
    └─> 4. 构建二叉堆
           binaryheap_build(state->heap)

ReorderBufferIterTXNNext()
    │
    ├─> 1. 获取最小LSN的变更
    │      off = binaryheap_first(heap)
    │      entry = &state->entries[off]
    │      change = entry->change
    │
    ├─> 2. 更新堆中下一个变更
    │      if (dlist_has_next(&entry->txn->changes, &entry->change->node))
    │          next = dlist_next(...)
    │          state->entries[off].change = next_change
    │          binaryheap_replace_first(heap, off)
    │
    ├─> 3. 如果内存中无变更，从磁盘恢复
    │      if (entry->txn->nentries != entry->txn->nentries_mem)
    │          // 删除当前变更，下次释放
    │          // 从磁盘读取下一批变更
    │          ReorderBufferRestoreChanges(...)
    │          binaryheap_replace_first(heap, off)
    │
    └─> 4. 返回变更
           return change
```

### 5.5 大事务溢出机制

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                         ReorderBuffer 内存管理                                           │
│                                                                                          │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐   │
│  │                        logical_decoding_work_mem (默认64MB)                        │   │
│  │  ┌───────────────────────────────────────────────────────────────────────────────┐  │   │
│  │  │                        ReorderBuffer->size                                  │  │   │
│  │  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │  │   │
│  │  │  │   TXN T1    │  │   TXN T2    │  │   TXN T3    │  │   TXN T4    │       │  │   │
│  │  │  │  (32MB)     │  │  (16MB)     │  │  (8MB)      │  │  (8MB)      │       │  │   │
│  │  │  └─────────────┘  └─────────────┘  └─────────────┘  └─────────────┘       │  │   │
│  │  │                                                                            │  │   │
│  │  │                      总大小: 64MB = 限制值                                  │  │   │
│  │  └───────────────────────────────────────────────────────────────────────────────┘  │   │
│  └────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                          │
│  触发溢出:                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐        │
│  │  ReorderBufferCheckMemoryLimit()                                              │        │
│  │      │                                                                     │        │
│  │      ├─> if (rb->size > logical_decoding_work_mem)                         │        │
│  │      │       │                                                             │        │
│  │      │       ├─> 从 txn_heap 中取出最大事务                                  │        │
│  │      │       │       (pairingheap, 按size比较)                               │        │
│  │      │       │                                                             │        │
│  │      │       └─> ReorderBufferSerializeTXN(rb, txn)                         │        │
│  │      │               │                                                      │        │
│  │      │               ├─> 遍历事务的所有变更                                    │        │
│  │      │               ├─> 写入磁盘: pg_logical/snapshots/                      │        │
│  │      │               └─> 更新标志: RBTXN_IS_SERIALIZED                        │        │
│  │      │                                                                     │        │
│  │      └─> 更新统计: spillTxns++, spillBytes += size                          │        │
│  └────────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                          │
│  从磁盘恢复:                                                                             │
│  ┌────────────────────────────────────────────────────────────────────────────────────┐        │
│  │  ReorderBufferRestoreChanges()                                                │        │
│  │      │                                                                     │        │
│  │      ├─> 从文件读取 ReorderBufferDiskChange                                  │        │
│  │      │                                                                     │        │
│  │      ├─> 重新构建 HeapTuple (detoast if needed)                              │        │
│  │      │                                                                     │        │
│  │      └─> 添加到 txn->changes 链表                                            │        │
│  └────────────────────────────────────────────────────────────────────────────────────┘        │
│                                                                                          │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 6. snapbuild.c 详解

### 6.1 快照状态枚举

```c
typedef enum SnapBuildState
{
    SNAPBUILD_START = -1,              // 初始状态
    SNAPBUILD_BUILDING_SNAPSHOT = 0,  // 正在构建快照
    SNAPBUILD_FULL_SNAPSHOT = 1,      // 已构建完整快照
    SNAPBUILD_CONSISTENT = 2,         // 达到一致点
} SnapBuildState;
```

### 6.2 快照构建原理

SnapBuild通过解析WAL中的`xl_running_xacts`记录来构建历史MVCC快照:

```
SnapBuildBuildSnapshot()
    │
    ├─> 1. 计算快照范围
    │      snapshot->xmin = builder->xmin
    │      snapshot->xmax = builder->xmax
    │
    ├─> 2. 填充已提交事务列表
    │      // 只包含目录修改事务，不是所有已提交事务
    │      snapshot->xip[] = builder->committed.xip
    │      snapshot->xcnt = builder->committed.xcnt
    │
    └─> 3. 设置子事务信息
           if (catalog_modifying_txn)
               snapshot->subxip[] = {toplevel_xid, subxids...}
```

### 6.3 关键函数

| 函数 | 作用 |
|------|------|
| `SnapBuildCommitTxn()` | 事务提交时更新已提交事务列表 |
| `SnapBuildProcessChange()` | 处理目录变更，更新xmin/xmax |
| `SnapBuildProcessRunningXacts()` | 处理RUNNING_XACTS记录，推进状态机 |
| `SnapBuildProcessNewCid()` | 处理NEW_CID记录，维护cmin/cmax映射 |
| `SnapBuildGetOrBuildSnapshot()` | 获取或构建当前需要的快照 |

### 6.4 状态转换示例

```
场景: 启动逻辑解码

时刻T0: 解码器启动
  └─> 状态: START
  └─> 等待足够新的 xl_running_xacts

时刻T1: 收到 xl_running_xacts (无运行事务)
  └─> 状态: START → CONSISTENT (直接跳转)
  └─> 可立即开始解码

时刻T2: 收到 xl_running_xacts (有3个运行事务: T4,T5,T6)
  └─> 状态: START → BUILDING_SNAPSHOT
  └─> 开始收集已提交事务

时刻T3: T4提交
  └─> SnapBuildCommitTxn(T4)
  └─> committed.xip = [T4]
  └─> xmin = min(committed) = T4

时刻T4: T5,T6都提交或中止
  └─> 状态: BUILDING_SNAPSHOT → FULL_SNAPSHOT
  └─> 可解码T4之后开始的事务

时刻T5: 收到新的 xl_running_xacts (无T4-T6期间的事务运行)
  └─> 状态: FULL_SNAPSHOT → CONSISTENT
  └─> 导出快照，可用于初始化订阅者
```

---

## 7. 典型流程分析

### 7.1 简单INSERT流程

```
WAL: INSERT (xid=100, lsn=0x168)
     COMMIT (xid=100, lsn=0x170)

decode.c: heap_decode()
    │
    ├─> XLOG_HEAP_INSERT
    │      │
    │      ├─> ReorderBufferProcessXid(xid=100, lsn)
    │      ├─> SnapBuildProcessChange(builder, xid, lsn)
    │      │      // builder->xmin/xmax可能更新
    │      └─> DecodeInsert()
    │             │
    │             ├─> 分配 ReorderBufferChange
    │             ├─> 填充 newtuple
    │             └─> ReorderBufferQueueChange(rb, xid, lsn, change)
    │                    │
    │                    └─> 加入 txn[xid=100]->changes 列表
    │
    └─> XLOG_XACT_COMMIT
           │
           ├─> xact_decode()
           │      │
           │      ├─> SnapBuildCommitTxn(builder, xid, ...)
           │      ├─> DecodeTXNNeedSkip() → false
           │      └─> DecodeCommit()
           │             │
           │             └─> ReorderBufferCommit()
           │                    │
           │                    ├─> output_plugin->begin()
           │                    ├─> 遍历 txn[100]->changes
           │                    │      output_plugin->apply_change()
           │                    ├─> output_plugin->commit()
           │                    └─> 释放内存
           │
           └─> 返回逻辑变化: BEGIN; TABLE ...; INSERT ...; COMMIT;
```

### 7.2 跨子事务的事务

```
WAL:
  BEGIN subtransaction 1
  INSERT (subxid=200, xid=100)
  BEGIN subtransaction 2
  UPDATE (subxid=201, xid=100)
  COMMIT subtransaction 201
  COMMIT subtransaction 200
  COMMIT toplevel 100

处理流程:
  1. INSERT (subxid=200)
     └─> ReorderBufferAssignChild(rb, xid=100, subxid=200)
     └─> ReorderBufferQueueChange(rb, subxid=200, ...)

  2. UPDATE (subxid=201)
     └─> ReorderBufferAssignChild(rb, xid=100, subxid=201)
     └─> ReorderBufferQueueChange(rb, subxid=201, ...)

  3. COMMIT subtransaction 201
     └─> ReorderBufferCommitChild(rb, xid=100, subxid=201, ...)

  4. COMMIT subtransaction 200
     └─> ReorderBufferCommitChild(rb, xid=100, subxid=200, ...)

  5. COMMIT toplevel 100
     └─> ReorderBufferCommit()
          │
          ├─> 使用K-Way归并迭代器按LSN顺序输出:
          │   Change(LSN from subxid=200)  // INSERT
          │   Change(LSN from subxid=201)  // UPDATE
          │
          └─> output_plugin看到完整的"INSERT then UPDATE"序列
```

### 7.3 大事务处理

```
场景: 一个大事务有100万行更新

1. 开始事务 T1
   └─> INSERT #1 ... LSN:1000
   └─> INSERT #2 ... LSN:1001
   └─> ...
   └─> INSERT #N ... LSN:1xxxx

2. 内存累积
   └─> ReorderBufferQueueChange() 每次调用
   └─> ReorderBufferChangeMemoryUpdate() 更新 rb->size
   └─> 达到 64MB 阈值

3. 触发溢出
   └─> ReorderBufferCheckMemoryLimit()
   └─> 找到最大事务 (就是T1)
   └─> ReorderBufferSerializeTXN(rb, txn[T1])
        │
        ├─> 打开文件: pg_logical/snapshots/<slot>/<xid>_<lsn>
        ├─> 遍历 txn[T1]->changes
        ├─> 每个change写入:
        │   ┌──────────────────────────────┐
        │   │ ReorderBufferDiskChange      │
        │   │   .size = sizeof(change)+data│
        │   │   .change = {action, lsn...} │
        │   │   data follows...            │
        │   └──────────────────────────────┘
        └─> txn[T1]->nentries_mem = 0

4. 继续处理
   └─> INSERT #N+1 ... (新变更继续加入内存)

5. COMMIT T1
   └─> ReorderBufferCommit()
        │
        ├─> ReorderBufferIterTXNInit()
        │      │
        │      ├─> txn[T1]已溢出，从磁盘恢复
        │      │   └─> ReorderBufferRestoreChanges()
        │      │          从文件读取变更
        │      │
        │      └─> 建立二叉堆: [磁盘T1] [内存subtxn1] ...
        │
        └─> 迭代输出所有变更给output_plugin
```

---

## 8. 内存管理机制

### 8.1 内存上下文结构

```
ReorderBufferAllocate()
    │
    └─> 创建三层内存上下文:
        │
        ├─> context (AllocSetContext)
        │      └─> 根上下文，计算total_size
        │
        ├─> change_context (SlabContext)
        │      └─> 固定大小块，分配 ReorderBufferChange
        │      └─> sizeof(ReorderBufferChange) = ~200 bytes
        │
        ├─> txn_context (SlabContext)
        │      └─> 固定大小块，分配 ReorderBufferTXN
        │      └─> sizeof(ReorderBufferTXN) = ~600 bytes
        │
        └─> tup_context (GenerationContext)
               └─> 生成代上下文，tuple数据
               └─> 整块分配和释放，避免碎片
```

### 8.2 内存限制检查

```c
static void
ReorderBufferCheckMemoryLimit(ReorderBuffer *rb)
{
    // 检查总内存是否超限
    if (rb->size > logical_decoding_work_mem)
    {
        // 从max-heap中取出最大事务
        txn = pairingheap_remove_first(rb->txn_heap);

        // 溢出到磁盘
        ReorderBufferSerializeTXN(rb, txn);

        // 更新统计
        rb->spillTxns++;
        rb->spillCount++;
        rb->spillBytes += txn->size;
    }
}
```

### 8.3 TOAST处理

```
场景: UPDATE一个包含大文本的tuple

WAL顺序:
  1. TOAST chunk records (多个)
  2. 主tuple record (包含TOAST指针)

ReorderBufferToastAppendChunk()
    │
    ├─> 按 chunk_id 哈希存储
    │      toast_hash[chunk_id] = {num_chunks, size, chunks list}
    │
    └─> 标记有待处理TOAST数据

ReorderBufferToastReplace()
    │
    ├─> 查找 toast_hash[chunk_id]
    │
    ├─> 按chunk_seq排序重组
    │      ┌────────┐ ┌────────┐ ┌────────┐
    │      │ seq=0  │ │ seq=1  │ │ seq=2  │ → varlena
    │      └────────┘ └────────┘ └────────┘
    │
    └─> 替换change中的tuple数据

关键标志:
  clear_toast_afterwards = true
    └─> 在此变更处理完后清除TOAST状态
```

---

## 9. 关键数据结构

### 9.1 LogicalDecodingContext

```c
struct LogicalDecodingContext
{
    XLogReaderState   *reader;          // XLog读取器
    ReplicationSlot   *slot;            // 复制槽
    ReorderBuffer     *reorder;         // 重排序缓冲区
    SnapBuild         *snapshot_builder;// 快照构建器

    // 输出插件回调
    ReorderBufferBeginCB begin;
    ReorderBufferApplyChangeCB apply_change;
    ReorderBufferCommitCB commit;
    ReorderBufferMessageCB message;
    // ... 其他回调

    // 插件数据
    void             *private_data;
    OutputPluginCallbacks callbacks;

    // 选项
    bool              fast_forward;     // 快速转发模式
    bool              twophase;         // 支持两阶段提交
};
```

### 9.2 XLogRecordBuffer

```c
typedef struct XLogRecordBuffer
{
    XLogRecPtr      origptr;      // 记录开始LSN
    XLogRecPtr      endptr;       // 记录结束LSN
    XLogReaderState *record;     // XLog记录
} XLogRecordBuffer;
```

### 9.3 ReorderBufferChange

```c
typedef struct ReorderBufferChange
{
    XLogRecPtr              lsn;          // 此变更的LSN
    ReorderBufferChangeType action;       // INSERT/UPDATE/DELETE/...

    union
    {
        struct
        {
            RelFileLocator  rlocator;    // 关系文件定位符
            bool            clear_toast_afterwards;
            HeapTuple       oldtuple;    // DELETE/UPDATE旧值
            HeapTuple       newtuple;    // INSERT/UPDATE新值
        } tp;

        struct
        {
            Size            nrelids;
            bool            cascade;
            bool            restart_seqs;
            Oid             *relids;
        } truncate;

        struct
        {
            char            *prefix;
            Size            message_size;
            char            *message;
        } msg;
        // ...
    } data;

    dlist_node         node;           // 链表节点
} ReorderBufferChange;
```

### 9.4 状态转换汇总

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                                 完整状态转换图                                                │
│                                                                                             │
│  SnapBuild状态:                                                                             │
│  ┌─────────┐   RUNNING_XACTS(无事务)   ┌─────────────┐   所有旧事务完成   ┌────────────┐    │
│  │  START  │ ───────────────────────> │   CONSISTENT │ <──────────────── │ FULL_      │    │
│  └─────────┘                          └─────────────┘   │         SNAPSHOT │   (等待中) │    │
│         │                                   ▲            └────────────┘    └────────────┘    │
│         │ RUNNING_XACTS(有事务)               │                                   │          │
│         ▼                                   │                                   │          │
│  ┌───────────────┐                         │                                   │          │
│  │BUILDING_      │ ──────────────────────────────────────────────────────────┘          │
│  │SNAPSHOT       │                                                                            │
│  └───────────────┘                                                                            │
│                                                                                              │
│  ReorderBufferTXN状态:                                                                       │
│  ┌─────────────┐  第一条记录   ┌──────────────┐  提交记录   ┌───────────┐                   │
│  │   空闲      │ ───────────> │    活动      │ ─────────> │  已提交   │                   │
│  └─────────────┘              └──────────────┘            └───────────┘                   │
│                                    │                                                     │
│                              ABORT记录                            ┌──────────────┐        │
│                                    │                              │ 已溢出到磁盘 │        │
│                                    ▼                              └──────────────┘        │
│                              ┌────────────┐                                               │
│                              │ 已中止     │                                               │
│                              └────────────┘                                               │
│                                                                                             │
│  变更状态:                                                                                  │
│  ┌──────────────┐  ReorderBufferQueueChange  ┌─────────────────┐  Commit后释放              │
│  │   空闲       │ ───────────────────────> │   已排队        │ ───────────────────> 内存  │
│  └──────────────┘                          └─────────────────┘                             │
│                                                      │                                      │
│                                                内存超限                                      │
│                                                      ▼                                      │
│                                               ┌─────────────────┐                           │
│                                               │  已溢出到磁盘    │                           │
│                                               └─────────────────┘                           │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 总结

```
┌─────────────────────────────────────────────────────────────────────────────────────────────┐
│                              逻辑解码核心要点                                               │
│                                                                                             │
│  1. decode.c 是入口模块                                                                    │
│     - 解析WAL记录类型                                                         │
│     - 调用snapbuild构建快照                                                         │
│     - 调用reorderbuffer存储变更                                                     │
│                                                                                             │
│  2. reorderbuffer 是事务重组核心                                                        │
│     - 按xid分组存储变更                                                             │
│     - K-way归并按LSN排序输出                                                       │
│     - 大事务溢出到磁盘                                                              │
│     - 子事务与顶层事务关联                                                          │
│                                                                                             │
│  3. snapbuild 是可见性判断基础                                                        │
│     - 跟踪已提交目录事务                                                            │
│     - 状态机推进: START → BUILDING → FULL → CONSISTENT                               │
│     - 构建历史MVCC快照                                                            │
│                                                                                             │
│  4. 三者协作关系                                                                             │
│     ┌─────────────────────────────────────────────────────────┐                          │
│     │                                                         │                          │
│     │   decode.c           reorderbuffer.c       snapbuild.c  │                          │
│     │       │                   │                   │         │                          │
│     │       │                   │                   │         │                          │
│     │       ▼                   ▼                   ▼         │                          │
│     │   解析WAL ──────> 存储变更 ──────> 快照信息              │                          │
│     │                   │                   │                │                          │
│     │                   │                   │                │                          │
│     │       ◀────────────────────────────────┘                │                          │
│     │                    查询快照                               │                          │
│     │                                                         │                          │
│     └─────────────────────────────────────────────────────────┘                          │
│                                                                                             │
└─────────────────────────────────────────────────────────────────────────────────────────────┘
```

本文档详细分析了PostgreSQL逻辑解码的三大核心模块，通过架构图和流程图展示了它们之间的协作关系和内部工作机制。