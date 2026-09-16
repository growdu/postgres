# pg_replication_trace 架构设计文档

## 1. 项目概述

### 1.1 项目名称

`pg_replication_trace`

简称：PRT

全称：PostgreSQL Logical Replication Trace

### 1.2 项目定位

PRT 是面向 PostgreSQL 18 的逻辑复制可观测与追踪扩展，用于提供表级、事务级、LSN 级、阶段级的逻辑复制可观测能力。

核心目标不是简单展示复制延迟，而是回答以下问题：

- 某个 Subscription 当前处于什么状态？
- 某一张表当前是否正在同步？
- 某张表的逻辑复制速率是多少？
- 当前同步到了哪个 LSN？
- 数据经过了 Logical Decode、pgoutput、Walsender、Subscriber Receive、Apply 的哪个阶段？
- 当前瓶颈位于哪一阶段？
- Walsender 是否因为网络或 Subscriber 端背压而等待？
- Apply Worker 是否因为锁、IO 或其他原因变慢？

---

## 2. 设计目标

### 2.1 Subscription 级监控

提供以下信息：

- subscription
- replication slot
- publisher
- walsender PID
- subscriber/apply worker
- current LSN
- sent LSN
- received LSN
- applied/replay LSN
- replication state
- replication lag
- throughput

### 2.2 Table 级监控

能够按照表查看：

- schema
- table
- relation OID
- INSERT 数量
- UPDATE 数量
- DELETE 数量
- TRUNCATE 数量
- change 数量
- WAL/change bytes
- rows/s
- bytes/s
- first LSN
- last LSN
- last change time

### 2.3 单表 Trace

允许用户指定一张表进行详细追踪，例如：

```sql
SELECT *
FROM pg_replication_trace_table('public.orders');
```

重点展示：

```text
public.orders

Status          APPLYING

Rows/s          12,832
Bytes/s         82.3 MB/s

Source LSN      0/8A123456
Sent LSN        0/8A120000
Received LSN    0/8A110000
Applied LSN     0/8A100000

Decode          8 ms
Output          12 ms
Apply           41 ms
```

### 2.4 瓶颈定位

将逻辑复制链路划分为：

```text
WAL
 ↓
Logical Decode
 ↓
pgoutput
 ↓
Walsender
 ↓
Network
 ↓
Walreceiver
 ↓
Apply Worker
 ↓
Subscriber Table
```

根据采集数据判断瓶颈所在阶段。

### 2.5 Wait Event 监控

重点关注 Walsender 和 Apply Worker 的等待状态，例如：

```text
WalSenderWriteData
WalSenderMain
Lock
LWLock
IO
ClientRead
ClientWrite
```

特别用于定位：

```text
walsender
  ↓
epoll_pwait()
```

这类问题。

---

## 3. 非目标

第一版本不做：

- 不改变 PostgreSQL 逻辑复制语义
- 不改变 WAL 格式
- 不改变 Logical Replication Protocol
- 不改变 pgoutput 协议
- 不替代 `pg_stat_replication`
- 不替代 `pg_stat_subscription`
- 不替代 `pg_replication_slots`
- 不额外创建 replication slot 用于监控
- 不重新执行一遍完整 Logical Decoding
- 不对每一条 change 写数据库表

---

## 4. 总体架构

```text
                         PostgreSQL 18
┌───────────────────────────────────────────────────────────┐
│                                                           │
│                         WAL                               │
│                          │                                │
│                          ▼                                │
│                  Logical Decoding                        │
│                          │                                │
│                ┌─────────┴──────────┐                     │
│                │                    │                     │
│                ▼                    ▼                     │
│          Trace Instrument        pgoutput                 │
│                │                    │                     │
│                │                    ▼                     │
│                │                Walsender                 │
│                │                    │                     │
│                │                    ▼                     │
│                │                  TCP                     │
│                │                    │                     │
│                │                    ▼                     │
│                │             Subscriber                   │
│                │             Apply Worker                 │
│                │                    │                     │
│                │                    ▼                     │
│                │             Subscriber Table             │
│                │                                          │
│                ▼                                          │
│          Shared Memory                                    │
│                │                                          │
│        ┌───────┼──────────────┐                           │
│        ▼       ▼              ▼                           │
│      Table    Txn          Pipeline                       │
│      Stats    Stats          Stats                        │
│        │       │              │                           │
│        └───────┼──────────────┘                           │
│                ▼                                          │
│        SQL Functions / Views                              │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

核心原则：

> Extension 负责统计、聚合、配置和 SQL 接口；PostgreSQL 18 核心只增加极少量 instrumentation/Trace Hook，不改变逻辑复制行为。

---

## 5. 核心设计原则

### 5.1 不重新创建 Logical Decoding

不采用：

```text
WAL
 ├── pgoutput
 │
 └── PRT output plugin
```

避免产生第二套 Logical Decoding。

推荐：

```text
Logical Decoding
       │
       ├──────► PRT Trace Hook
       │
       └──────► pgoutput
```

即：

> 旁路采集，而不是旁路复制。

### 5.2 不在 Change Callback 中写数据库

错误方式：

```text
change
 ↓
INSERT INTO trace_table
```

正确方式：

```text
change
 ↓
Shared Memory Counter
 ↓
Periodic Aggregation
 ↓
SQL View
```

### 5.3 统计优先采用聚合指标

生产环境默认记录：

- change count
- bytes
- LSN
- transaction count
- timestamp
- stage
- wait event

详细 Trace 仅在用户主动开启时启用。

---

## 6. PostgreSQL 18 埋点架构

建议分为四个阶段：

```text
Layer 1
Logical Decoding

Layer 2
pgoutput

Layer 3
Walsender

Layer 4
Subscriber Apply
```

### 6.1 Logical Decoding

重点关注：

```text
src/backend/replication/logical/
```

采集：

- transaction begin
- change
- truncate
- commit
- streaming transaction
- two-phase transaction

核心信息：

```text
relid
xid
LSN
operation
bytes
timestamp
```

### 6.2 pgoutput

重点关注：

```text
src/backend/replication/pgoutput/
```

采集：

- relation message
- begin
- change encoding
- commit
- output timing

目标：

```text
Logical Decode
      ↓
    pgoutput
```

计算：

```text
decode_latency
output_latency
```

### 6.3 Walsender

重点关注：

```text
src/backend/replication/walsender.c
```

关注：

- `WalSndLoop()`
- `XLogSendLogical()`
- `WalSndWait()`
- WAL sender wait event
- send progress

重点记录：

```text
walsender_pid
socket_fd
wait_event
wait_start
wait_end
wait_duration
sent_lsn
```

### 6.4 Subscriber Apply

重点关注：

```text
src/backend/replication/logical/
```

采集：

- received LSN
- apply start
- apply end
- relation
- operation
- commit

最终形成：

```text
Decode
 ↓
Output
 ↓
Send
 ↓
Receive
 ↓
Apply
```

---

## 7. Trace 数据模型

### 7.1 Trace Record

```c
typedef struct PRTTraceRecord
{
    uint64 trace_id;

    Oid database_oid;
    Oid relid;

    TransactionId xid;

    XLogRecPtr start_lsn;
    XLogRecPtr end_lsn;
    XLogRecPtr commit_lsn;

    uint64 change_count;
    uint64 bytes;

    uint64 insert_count;
    uint64 update_count;
    uint64 delete_count;

    TimestampTz decode_start;
    TimestampTz decode_end;

    TimestampTz output_start;
    TimestampTz output_end;

    TimestampTz send_start;
    TimestampTz send_end;

    TimestampTz receive_start;
    TimestampTz receive_end;

    TimestampTz apply_start;
    TimestampTz apply_end;

} PRTTraceRecord;
```

### 7.2 Table Statistics

```c
typedef struct PRTTableStats
{
    Oid relid;

    uint64 insert_count;
    uint64 update_count;
    uint64 delete_count;
    uint64 truncate_count;

    uint64 change_count;
    uint64 bytes;

    XLogRecPtr first_lsn;
    XLogRecPtr last_lsn;

    TimestampTz first_change_time;
    TimestampTz last_change_time;

    double rows_per_sec;
    double bytes_per_sec;

} PRTTableStats;
```

### 7.3 Transaction Statistics

```c
typedef struct PRTTransactionStats
{
    TransactionId xid;

    XLogRecPtr start_lsn;
    XLogRecPtr commit_lsn;

    uint64 change_count;
    uint64 bytes;

    TimestampTz begin_time;
    TimestampTz commit_time;

    bool streaming;

} PRTTransactionStats;
```

---

## 8. Shared Memory 设计

Extension 使用 PostgreSQL Shared Memory API。

生命周期：

```text
Postmaster
    │
    ▼
_PG_init()
    │
    ├── Define GUC
    ├── shmem_request_hook
    └── Trace Hooks
          │
          ▼
    shmem_startup_hook
          │
          ▼
      Shared Memory
```

核心结构：

```c
typedef struct PRTSharedState
{
    LWLock       *lock;

    uint32        max_tables;
    uint32        max_transactions;

    PRTTableStats *tables;
    PRTTransactionStats *transactions;

    PRTGlobalStats global;

} PRTSharedState;
```

建议使用 Hash Table 根据 `relid` 快速定位 Table Stats。

---

## 9. 锁设计

不建议所有 change 使用单一全局 LWLock。

采用：

```text
Table Hash
   │
   ├── Bucket 0
   ├── Bucket 1
   ├── Bucket 2
   └── ...
```

通过多个锁分片降低竞争：

```text
PRT_TABLE_LOCK_0
PRT_TABLE_LOCK_1
...
PRT_TABLE_LOCK_N
```

目标是在高并发、高吞吐逻辑复制场景下尽量降低监控开销。

---

## 10. 内存控制

配置：

```text
max_tables
max_transactions
```

例如：

```conf
pg_replication_trace.max_tables = 10000
pg_replication_trace.max_transactions = 1024
```

采用固定上限，避免大量表或事务导致 Extension 内存无限增长。

---

## 11. Rate 计算

Change Callback 中只维护累计值：

```text
total_rows
total_bytes
timestamp
```

采样时计算：

```text
rows_per_sec =
(delta_rows / delta_time)

bytes_per_sec =
(delta_bytes / delta_time)
```

示例：

```text
T1:
rows  = 1,000,000
bytes = 10 GB

T2:
rows  = 1,120,000
bytes = 10.8 GB

Δt = 1 sec
```

结果：

```text
rows/s  = 120,000
bytes/s = 800 MB/s
```

---

## 12. LSN 追踪

LSN 是串联各阶段的核心。

全局记录：

```text
current_lsn
sent_lsn
received_lsn
apply_lsn
```

Table 级记录：

```text
first_lsn
last_lsn
```

整体链路：

```text
Source WAL
    │
    ▼
0/8A123456
    │
    │ Decode
    ▼
0/8A120000
    │
    │ Send
    ▼
0/8A110000
    │
    │ Receive
    ▼
0/8A100000
    │
    │ Apply
    ▼
Subscriber
```

---

## 13. “当前正在同步哪张表”的设计

不能简单将：

```text
current_lsn = current_table
```

因为一个事务可以包含多张表：

```sql
BEGIN;

INSERT INTO orders;
UPDATE users;
DELETE FROM products;

COMMIT;
```

因此正确映射关系：

```text
LSN
 ↓
Transaction
 ↓
Change
 ↓
Relation
```

即：

```text
LSN Range
    │
    ▼
Transaction
    │
 ┌──┼─────────┐
 ▼  ▼         ▼
orders users products
```

UI 中的 `Current Table` 定义为：

> 当前发送或应用位置对应的最近已知 relation change。

同时增加：

```text
table_confidence
```

取值：

```text
EXACT
TRANSACTION
APPROXIMATE
```

避免将近似推断误认为绝对精确。

---

## 14. 状态机

定义：

```text
INIT
  │
  ▼
DECODING
  │
  ▼
OUTPUTTING
  │
  ▼
SENDING
  │
  ▼
RECEIVING
  │
  ▼
APPLYING
  │
  ▼
DONE
```

异常或等待状态：

```text
WAITING
BLOCKED
ERROR
DISCONNECTED
```

---

## 15. Bottleneck Diagnosis

### 15.1 WAL 瓶颈

表现：

```text
WAL generation < replication throughput
```

### 15.2 Decode 瓶颈

表现：

```text
decode latency ↑
CPU ↑
```

### 15.3 pgoutput 瓶颈

表现：

```text
output latency ↑
```

### 15.4 Network/Walsender 瓶颈

表现：

```text
send latency ↑
Send-Q ↑
WalSenderWriteData ↑
```

### 15.5 Apply 瓶颈

表现：

```text
received_lsn 快速前进
apply_lsn 增长缓慢
```

### 15.6 Lock 瓶颈

表现：

```text
wait_event = Lock
```

最终形成：

```text
BOTTLENECK =
    WAL
    DECODE
    OUTPUT
    NETWORK
    APPLY
    LOCK
```

---

## 16. SQL API

### 16.1 总体状态

```sql
SELECT *
FROM pg_replication_trace;
```

建议字段：

```text
subscription
slot_name
publisher_pid

state

current_lsn
sent_lsn
received_lsn
apply_lsn

source_rate
send_rate
receive_rate
apply_rate

lag_bytes
lag_time

current_stage
current_wait
```

### 16.2 Table 统计

```sql
SELECT *
FROM pg_replication_trace_tables;
```

建议字段：

```text
subscription
database
schema
table
relid

insert_count
update_count
delete_count
truncate_count

change_count
bytes

rows_per_second
bytes_per_second

first_lsn
last_lsn

last_change_time
```

### 16.3 Activity

```sql
SELECT *
FROM pg_replication_trace_activity;
```

建议字段：

```text
pid
backend_type

schema
table

xid

start_lsn
current_lsn

stage
state

wait_event
wait_duration
```

### 16.4 单表 Trace

```sql
SELECT *
FROM pg_replication_trace_table(
    'public.orders'
);
```

### 16.5 开启 Trace

```sql
SELECT pg_replication_trace_enable(
    'public.orders'
);
```

### 16.6 关闭 Trace

```sql
SELECT pg_replication_trace_disable(
    'public.orders'
);
```

---

## 17. 配置参数

建议提供：

```text
pg_replication_trace.enabled
pg_replication_trace.max_tables
pg_replication_trace.max_transactions
pg_replication_trace.sample_interval
pg_replication_trace.trace_level
pg_replication_trace.track_transactions
pg_replication_trace.track_wait_events
```

示例：

```conf
pg_replication_trace.enabled = on
pg_replication_trace.max_tables = 10000
pg_replication_trace.max_transactions = 1024
pg_replication_trace.sample_interval = 1s
pg_replication_trace.trace_level = normal
```

---

## 18. Trace Level

### OFF

完全关闭。

### BASIC

记录：

```text
LSN
change
bytes
rows
```

### NORMAL

增加：

```text
transaction
decode
output
send
```

### FULL

增加：

```text
wait event
apply
transaction trace
```

生产环境默认：

```text
NORMAL
```

问题排查时：

```text
FULL
```

---

## 19. PostgreSQL 原生视图集成

PRT 不替代：

```text
pg_stat_replication
pg_stat_subscription
pg_replication_slots
```

而是将这些信息与自己的 Trace 数据统一起来：

```text
                 PRT
                  │
       ┌──────────┼───────────┐
       ▼          ▼           ▼
pg_stat_     pg_stat_     pg_replication_
replication  subscription     slots
```

---

## 20. Extension 文件结构

建议：

```text
pg_replication_trace/
│
├── Makefile
├── pg_replication_trace.control
│
├── src/
│   ├── pg_replication_trace.c
│   ├── trace_shmem.c
│   ├── trace_table.c
│   ├── trace_txn.c
│   ├── trace_lsn.c
│   ├── trace_rate.c
│   ├── trace_wait.c
│   ├── trace_sql.c
│   └── trace.h
│
├── sql/
│   └── pg_replication_trace--1.0.sql
│
├── include/
│   └── pg_replication_trace.h
│
├── views/
│   ├── replication.sql
│   ├── tables.sql
│   └── activity.sql
│
├── tests/
│   ├── sql/
│   └── expected/
│
└── docs/
    ├── architecture.md
    ├── metrics.md
    └── development.md
```

---

## 21. PostgreSQL 18 核心改造边界

建议严格控制 PostgreSQL Core 改造范围：

```text
PostgreSQL 18
│
├── logical/
│   └── Trace Hook
│
├── pgoutput/
│   └── Trace Hook
│
├── walsender.c
│   └── Trace Hook
│
└── logical apply worker
    └── Trace Hook
```

原则：

> PostgreSQL 核心只提供少量 Trace Hook，Extension 负责统计、聚合和对外展示。

---

## 22. Hook 接口设计

建议统一设计：

```c
typedef struct PRTTraceHooks
{
    void (*on_decode_change)(...);

    void (*on_output_change)(...);

    void (*on_walsender_send)(...);

    void (*on_walsender_wait)(...);

    void (*on_apply_change)(...);

    void (*on_apply_commit)(...);

} PRTTraceHooks;
```

整体关系：

```text
PostgreSQL Core
       │
       ▼
   PRT Hook API
       │
       ▼
pg_replication_trace
```

这样可以减少未来 PostgreSQL 版本升级时的维护成本。

---

## 23. 性能设计

### 23.1 核心原则

监控不能成为复制性能瓶颈。

避免：

```text
change
 ↓
LWLock
 ↓
Database INSERT
 ↓
WAL
```

采用：

```text
change
 ↓
轻量内存计数
 ↓
周期性聚合
 ↓
SQL
```

### 23.2 性能目标

目标值需要通过 Benchmark 验证，设计目标为：

```text
BASIC   < 1% CPU overhead
NORMAL  < 2% CPU overhead
FULL    尽可能 < 5% CPU overhead
```

以上为工程目标，不作为未经 Benchmark 验证的硬性保证。

---

## 24. MVP 开发计划

### MVP-1：Table Statistics

首先实现 Publisher 侧：

```text
Logical Decode
      │
      ▼
Table
      │
      ├── INSERT
      ├── UPDATE
      ├── DELETE
      ├── bytes
      ├── LSN
      └── rate
```

目标：

> 验证能否准确统计每张表产生的逻辑复制数据量和速率。

### MVP-2：Output + Walsender

增加：

```text
pgoutput
walsender
```

得到：

```text
decode rate
output rate
send rate
wait event
```

重点解决：

```text
walsender
 ↓
epoll_pwait()
```

### MVP-3：Subscriber Apply

增加：

```text
receive
apply
```

最终实现：

```text
Table
 ↓
Decode
 ↓
Output
 ↓
Send
 ↓
Receive
 ↓
Apply
```

### MVP-4：自动诊断

增加：

```text
Bottleneck:
    WAL
    Decode
    Output
    Network
    Apply
    Lock
```

---

## 25. 最终用户体验

目标查询：

```sql
SELECT *
FROM pg_replication_trace_table('public.orders');
```

输出逻辑：

```text
public.orders

Status              APPLYING

Rows/s              12,832
Bytes/s             82.3 MB/s

Source LSN          0/8A123456
Sent LSN            0/8A120000
Received LSN        0/8A110000
Applied LSN         0/8A100000

Decode              120 MB/s
Output              118 MB/s
Send                117 MB/s
Receive             116 MB/s
Apply                82 MB/s

Current Stage       APPLY
Current Wait        Lock

Bottleneck           APPLY
Lag                  1.2 sec
```

Activity 查询：

```sql
SELECT *
FROM pg_replication_trace_activity
WHERE table_name = 'orders';
```

示例：

```text
PID       18273
Stage     APPLY
Wait      Lock
Duration  823 ms

XID       82931
LSN       0/8A110000
```

---

## 26. 最终架构结论

最终系统：

```text
                 PostgreSQL 18
                      │
       ┌──────────────┼───────────────┐
       │              │               │
       ▼              ▼               ▼
 Logical Decode    pgoutput       Walsender
       │              │               │
       └──────────────┼───────────────┘
                      │
                  Trace Hook
                      │
                      ▼
                Shared Memory
                      │
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
     Table           Txn          Pipeline
     Stats           Stats          Stats
       │              │              │
       └──────────────┼──────────────┘
                      ▼
                  SQL Views
                      │
             ┌────────┴────────┐
             ▼                 ▼
          DBA/SQL          Prometheus
                               │
                               ▼
                            Grafana
```

核心设计决策：

> 不额外创建 replication slot，不重新 decode WAL，不修改复制协议；在 PostgreSQL 18 的 Logical Decoding、pgoutput、Walsender、Apply 四个关键阶段增加极少量 Trace Hook，由 `pg_replication_trace` Extension 在 Shared Memory 中完成聚合，再通过 SQL 暴露。

最终实现：

```text
WAL
 ↓
Logical Decode
 ↓
pgoutput
 ↓
Walsender
 ↓
Network
 ↓
Subscriber
 ↓
Apply
 ↓
Table
```

能够围绕一张表追踪：

```text
LSN
Transaction
Change
Rate
Latency
Stage
Wait Event
Bottleneck
```

从而将 PostgreSQL Logical Replication 从黑盒复制转变为可观测的数据流水线。

---

## 27. 后续详细设计重点

概要设计完成后，详细设计应重点解决以下问题：

1. PostgreSQL 18 源码中每个 Trace Hook 的具体函数和调用位置。
2. Logical Decoding 中如何获取准确的 relation、XID、LSN 和 change bytes。
3. pgoutput 如何建立 change 与 relation 的对应关系。
4. Walsender 当前发送位置与 transaction/table 的 LSN 映射算法。
5. Subscriber Apply Worker 如何关联 publisher trace。
6. Streaming Large Transaction 如何进行跨阶段追踪。
7. Two-Phase Commit 如何进行追踪。
8. Shared Memory Hash Table 与 LWLock 的具体实现。
9. Trace Ring Buffer 与历史数据淘汰策略。
10. Rate、Latency、Lag 的精确计算方式。
11. Wait Event 与 `epoll_pwait()` 的对应关系。
12. PostgreSQL 18 源码修改点与 Extension 代码之间的接口设计。
13. Regression Test 和高并发 Benchmark 方案。
