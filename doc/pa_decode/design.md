# 逻辑复制并行解码 概要设计说明书

| 项目 | 内容 |
| --- | --- |
| 特性名称 | 逻辑复制发布端并行解码（parallel decoding） |
| 关联文档 | `need.md`（需求说明书） |
| 代码基线 | PostgreSQL 17（本仓库分支 `pa_decode`） |
| 文档状态 | 草稿 |
| 最后更新 | 2026-11-18 |

## 目录

- [1. 功能简述](#1-功能简述)
- [2. 现状与瓶颈分析](#2-现状与瓶颈分析)
  - [2.1 发布端现有流程](#21-发布端现有流程)
  - [2.2 瓶颈定位](#22-瓶颈定位)
  - [2.3 WAL 记录分类与并行切分依据](#23-wal-记录分类与并行切分依据)
  - [2.4 来自现有实现的关键事实（设计约束）](#24-来自现有实现的关键事实设计约束)
- [3. 总体架构](#3-总体架构)
  - [3.1 三进程模型](#31-三进程模型)
  - [3.2 进程关系图](#32-进程关系图)
  - [3.3 进程生命周期](#33-进程生命周期)
  - [3.4 端到端时序](#34-端到端时序)
- [4. 关键设计决策](#4-关键设计决策)
  - [4.1 决策一：解码切分点——worker 负责到哪一层](#41-决策一解码切分点worker-负责到哪一层)
  - [4.2 决策二：提交顺序保证——commit barrier 与 FIFO 队列对比](#42-决策二提交顺序保证commit-barrier-与-fifo-队列对比)
  - [4.3 决策三：worker 与事务的绑定关系](#43-决策三worker-与事务的绑定关系)
- [5. 进程间通信](#5-进程间通信)
  - [5.1 共享内存布局](#51-共享内存布局)
  - [5.2 消息定义](#52-消息定义)
  - [5.3 低拷贝策略](#53-低拷贝策略)
  - [5.4 调度与唤醒](#54-调度与唤醒)
  - [5.5 流控与背压](#55-流控与背压)
- [6. 快照与系统表变更](#6-快照与系统表变更)
  - [6.1 快照的构建](#61-快照的构建)
  - [6.2 快照的事件流（必须按序传递）](#62-快照的事件流必须按序传递)
  - [6.3 worker 侧使用历史快照](#63-worker-侧使用历史快照)
  - [6.4 relid 解析](#64-relid-解析)
  - [6.5 无事务的失效](#65-无事务的失效)
  - [6.6 xmin 水位的跨进程聚合](#66-xmin-水位的跨进程聚合)
- [7. 提交顺序保证的实现](#7-提交顺序保证的实现)
  - [7.1 提交序号（cseq）的产生](#71-提交序号cseq的产生)
  - [7.2 commit barrier 的实现](#72-commit-barrier-的实现)
  - [7.3 FIFO 方案的实现（备选/退化路径）](#73-fifo-方案的实现备选退化路径)
  - [7.4 顺序保证的边界](#74-顺序保证的边界)
- [8. 流式发送](#8-流式发送)
  - [8.1 尽早流式](#81-尽早流式)
  - [8.2 分片与整事务两种输出形态](#82-分片与整事务两种输出形态)
- [9. 异常处理](#9-异常处理)
- [10. 模块划分](#10-模块划分)
- [11. 接口说明](#11-接口说明)
  - [11.1 用户接口（新增 GUC）](#111-用户接口新增-guc)
  - [11.2 程序内部接口](#112-程序内部接口)
  - [11.3 监控指标](#113-监控指标)
- [12. 性能](#12-性能)
  - [12.1 目标与测量](#121-目标与测量)
  - [12.2 资源与内存](#122-资源与内存)
  - [12.3 性能风险点](#123-性能风险点)
- [13. 兼容性与约束](#13-兼容性与约束)
- [14. 风险与对策](#14-风险与对策)
- [15. 待决问题](#15-待决问题)
- [16. 实现计划](#16-实现计划)
- [附录 A 术语表](#附录-a-术语表)
- [附录 B 修订记录](#附录-b-修订记录)

## 1. 功能简述

逻辑复制发布端当前在单个 walsender 进程内串行完成“WAL 读取 → 快照构建 → 事务重组 → tuple 解析 → 协议字节生成 → 网络发送”的全过程，CPU 只能利用一个核。PG16 起订阅端已支持流复制协议下的并行回放，发布端的串行解码成为整条链路的瓶颈。

本方案将发布端重构为 **walreader、decode worker、walsender 三个进程**：

- **walreader**：读取 WAL，推进快照构建到 consistent，把数据类 WAL 按顶层事务分发给 decode worker，并按 WAL 顺序为事务结束事件（commit/abort/cancel）分配提交序号；
- **decode worker**：并行地把 WAL 解析为 tuple 变更并生成协议字节；
- **walsender**：主控进程，负责启动与监控上述子进程、持有复制槽与订阅端连接、按提交顺序输出、推进复制槽 LSN。

目标：tpcc 100 万 tpm 场景下逻辑复制 WAL 同步速率达到 70MB/s（以 `confirm_flush_lsn` 的速率为准）。

**设计边界：只改发布端。** 订阅端的回放机制、流复制协议格式、现有 GUC 语义与订阅端行为均不改动；本特性对订阅端完全透明。

**非目标：**

- 不改动订阅端的任何代码与机制；
- 不新增或修改流复制协议的消息类型，订阅端无需升级；
- 不改动 output plugin 的接口约定，现有插件无需适配即可工作；
- 不改变逻辑解码 SQL 接口（`pg_logical_slot_get_changes()` 等）的行为；
- 不并行化 WAL 读取与快照构建本身（WAL 只允许一个读取者，见 2.3 与 6.1）。

## 2. 现状与瓶颈分析

### 2.1 发布端现有流程

发布端的解码与发送全部在 walsender 进程内串行完成，代码路径如下：

1. `walsender.c:XLogSendLogical()` 循环调用 `logical.c:LogicalDecodingProcessRecord()`；
2. `decode.c:LogicalDecodingProcessRecord()` 取出记录头中的顶层事务 ID（`XLogRecGetTopXid`），调用 `ReorderBufferAssignChild()` 归属子事务，再按 rmgr 分派到同文件的 `xlog_decode` / `xact_decode` / `standby_decode` / `heap_decode` / `heap2_decode` / `logicalmsg_decode`（本分支已把上游 PG 的记录分发入口从 `reorderbuffer.c` 重构进 `decode.c`，树中不存在 `ReorderBufferProcessRecord()`）；
3. `reorderbuffer.c` 维护事务表（按 LSN 排序的 `toplevel_by_lsn` 与 `txns_by_base_snapshot_lsn` 两条链表），在 `ReorderBufferQueueChange()` 之后由 `ReorderBufferCheckMemoryLimit()` 按 `logical_decoding_work_mem` 阈值决定流式输出还是落盘；
4. `logical.c` 的 `OutputPluginWrite()` 把插件生成的字节交给 `walsender.c:WalSndWriteData()` 写入 socket。

### 2.2 瓶颈定位

发布端流程中 CPU 消耗最高的是第 2 步的 tuple 解析（`heap_decode` / `heap2_decode`）：它要做元组重建、系统目录查找、列格式转换。这部分与“WAL 读取顺序”“快照维护”“网络发送”在逻辑上相互独立，是可以并行拆出的主体。

### 2.3 WAL 记录分类与并行切分依据

| 记录类型 | 内容 | 归属进程 | 理由 |
| --- | --- | --- | --- |
| `standby` | `XLOG_RUNNING_XACTS` | walreader | 快照推进的唯一驱动，必须全局有序 |
| `xlog` | 检查点、恢复边界、参数变更 | walreader | 不产生数据变更 |
| `xact` | COMMIT / ABORT / PREPARE / ASSIGNMENT / INVALIDATIONS | walreader | 控制信息；提交顺序的来源 |
| `heap2` `XLOG_HEAP2_NEW_CID` | 命令 ID | walreader | 目录变更的标志，需与快照联动 |
| `heap` | INSERT / UPDATE / HOT UPDATE / DELETE / TRUNCATE / CONFIRM | decode worker | 数据变更，解码主体 |
| `heap2` `XLOG_HEAP2_MULTI_INSERT` | 批量插入 | decode worker | 数据变更，解码主体 |
| `message` | 逻辑复制消息 | decode worker（事务型）/ walreader（非事务型） | 事务型消息随事务分发 |

### 2.4 来自现有实现的关键事实（设计约束）

以下事实直接来自本仓库代码，是本设计的约束前提，方案必须成立而非重新论证。

**事实 1：顶层事务 ID 可独立获取。** `logical.c:LogicalDecodingProcessRecord()` 通过 `XLogRecGetTopXid(record)` 从 WAL 记录头直接得到顶层事务 ID，无需查询 reorder buffer。因此 walreader 可以仅凭 WAL 记录自身决定分发目标，这是并行分发的前提。

**事实 2：快照按事务分配。** `snapbuild.c:800-817` 在事务的首条记录处构建快照并调用 `ReorderBufferSetBaseSnapshot()` 绑定到该事务；`reorderbuffer.c:3162` 的 `ReorderBufferSetBaseSnapshot()` 保存 `txn->base_snapshot`。快照是**事务级**而非全局单例，因此可以随事务一起分发给 worker。

**事实 3：目录失效消息按事务累积、在事务提交时执行。** `decode.c:281-308` 处理 `XLOG_XACT_INVALIDATIONS`，调用 `ReorderBufferAddInvalidations()` 挂到事务上；`reorderbuffer.c:2577/2623/2913` 在事务结束（提交/回滚/准备）时调用 `ReorderBufferExecuteInvalidations()` 执行。失效天然属于事务，不需要额外的全局广播。

**事实 4：流式模式下事务数据本身可以是乱序的，只有 commit 需要保序。** `reorderbuffer.c:3659-3676` 的 `ReorderBufferCheckMemoryLimit()` 在 `rb->size`（reorder buffer 中所有在途变更的总字节数）超过 `logical_decoding_work_mem * 1024` 时触发流式，并通过 `ReorderBufferLargestStreamableTopTXN()` **挑选总字节数最大的事务**优先输出——可见 PG 已允许事务数据乱序下发。而 commit/abort 始终在对应的 WAL 提交记录处按 WAL 顺序发出：`decode.c:216-243` 解析 `XLOG_XACT_COMMIT` → `DecodeCommit()`（`decode.c:733` 调用 `ReorderBufferCommit()`）→ `ReorderBufferReplay()`（`reorderbuffer.c:2679`）立即输出该事务。**这意味着并行解码必须保证的只有 commit/abort/cancel 的相对顺序，事务数据不必保序。** 本设计的顺序方案全部建立在这一事实上。

**事实 5：分片的粒度不是固定字节数。** `ReorderBufferStreamTXN()`（`reorderbuffer.c:4054-4166`）输出的是“该事务自上一分片以来累积的全部变更”，触发条件是 reorder buffer 的**聚合**内存超阈值，而非单事务大小，因此一个 1GB 的事务可以被整体作为一个分片输出。流式输出后 `ReorderBufferTruncateTXN()` 回收该分片的内存（`rb->size` 回落到阈值以下），阈值被反复复用。

**事实 6：RELATION 消息的重复粒度是“每个流式事务”。** `pgoutput.c:126-140` 的 `RelationSyncEntry.streamed_txns` 记录“已为哪些顶层事务发过该关系的描述”，`maybe_send_schema()`（`pgoutput.c:704-770`）在流式模式下按 top-xid 判断是否需要重发；`cleanup_rel_sync_cache()`（`pgoutput.c:2293-2323`）在事务提交时把 xid 移出该表并置 `schema_sent = true`（会话级标志）。因此单进程下 RELATION 是“每关系每会话一次（失效后重发）”；**一旦把插件实例复制到 N 个 worker，每份缓存各自独立，最坏情况下 RELATION 消息量放大到 N 倍**。

**事实 7：非流式订阅方的大事务走落盘而非流式。** `reorderbuffer.c:3677-3691` 在无法流式时调用 `ReorderBufferSerializeTXN()` 把事务写入 `pg_logical/reorderbuffer` 下的临时文件，提交时再整体回放输出。因此“整事务字节连续”是现状既有行为，并行化后只要保持“事务数据在提交时整块写出”即可，无需新机制。

**事实 8：本分支实现了协议 v4 的并行流式。** 订阅方可用 `CREATE SUBSCRIPTION ... WITH (streaming = parallel)`（`subscriptioncmds.c:255-261`、`pg_subscription.h:175`）请求并行流式，发布方据此可以同时流式多个在途事务，订阅端由 parallel apply worker 回放（`applyparallelworker.c`）。本特性的目标场景（高吞吐 + 并行回放）正是该模式；在该模式下“多个事务的分片交错下发”已是既有行为，进一步印证**提交顺序是唯一需要严格保证的东西**。

**事实 9：复制槽绑定在 walsender 进程。** `logical.c:1693-1699` 的 `LogicalIncreaseXminForSlot()` 使用 `MyReplicationSlot`（进程局部变量），`snapbuild.c:1316` 在 `SnapBuildProcessRunningXacts()` 中调用它推进槽的 catalog_xmin。**拆分进程后，walreader 持有 snapbuild 却无法推进槽的 xmin**，必须由 walsender 代劳（见 5.2 的 `PMSG_XMIN_ADVANCE` / `PMSG_XMIN_ACK` 与 6.6）。

**事实 10：等待新 WAL 的逻辑与网络职责耦合。** `walsender.c:1054-1069` 的 `logical_read_xlog_page()` 依赖 `WalSndWaitForWal()`，后者同时负责 keepalive 与客户端反馈。walreader 需要自己的等待路径，不能复用它（见 3.1 与 5.4）。

**事实 11：是否流式由发布方决定，判据是订阅方传来的选项。** `libpqwalreceiver.c:588-590` 订阅方在 `START_REPLICATION` 中带 `streaming` 选项；发布方在 `pgoutput.c:488-510` 依据该选项与协议版本（`logicalproto.h:40-45`：v2 起支持流式，v4 起支持并行流式）决定 `ctx->streaming`。发布方因此**知道**当前订阅方是否接受分片下发。

**事实 12：output plugin 不是在读 WAL 时调用的，而是在事务回放时调用的。** 读 WAL 阶段只把变更**入队**（`decode.c:893` `DecodeInsert` → `reorderbuffer.c:788` `ReorderBufferQueueChange`）；插件回调发生在变更从 reorder buffer **回放**时——提交时（`ReorderBufferCommit()` → `ReorderBufferProcessTXN()`，`reorderbuffer.c:2109`）或流式时（`ReorderBufferStreamTXN()`，`reorderbuffer.c:4054`）。由此得到两条对本设计至关重要的推论：

- **CPU 热点不在拷贝，而在插件格式化。** 每次变更的每列都要做一次 syscache 查类型 + 一次类型输出函数调用 + 一次分配（`logical/proto.c:828-858`），每条变更还要做一次 relmapper 查表 + 一次 relcache 打开（`reorderbuffer.c:2230-2255`）。相对而言 memcpy 是 GB/s 级的廉价操作。**只把“WAL→tuple”搬到 worker 省下的是拷贝，不是热点。**
- **一个事务的插件调用不能跨进程拆分。** `txn->output_plugin_private`（pgoutput 的 `PGOutputTxnData`，`pgoutput.c:574` 创建、`pgoutput.c:610` 销毁）必须在同一个进程内走完 begin→changes→commit，且 `ReorderBufferProcessTXN()` 在调用插件前会安装历史快照并开启内部事务（`reorderbuffer.c:2127/2145-2149`）。

## 3. 总体架构

### 3.1 三进程模型

| 职责项 | walreader | decode worker | walsender |
| --- | --- | --- | --- |
| 数量 | 1 | N（`max_parallel_decode_workers`） | 1 |
| 读 WAL | ✔ 唯一读取者 | ✘ | ✘ |
| 持有 snapbuild | ✔ 唯一持有者 | ✘（持有快照副本） | ✘ |
| 维护事务表 | ✔（事务→worker 绑定） | ✔（自己负责的事务子集） | ✘ |
| 解码 tuple | ✘ | ✔ | ✘ |
| 生成协议字节 | ✘ | ✔ | ✘ |
| 持有 replication slot | ✘ | ✘ | ✔ |
| 网络收发 | ✘ | ✘ | ✔ |
| 决定提交顺序 | ✔（分配提交序号） | ✘ | ✔（按序输出） |
| 推进 `confirmed_flush` | ✘ | ✘ | ✔ |

**walreader 进程**

1. 读取 WAL 记录，驱动 snapbuild 前进，直到达到 consistent 状态；
2. 顺序读取数据类 WAL 记录，按顶层事务 ID 路由到对应的 decode worker；
3. 维护事务→worker 的绑定表与每个 worker 的负载水位；
4. 处理控制类记录（`xact`、`standby`、`xlog`、`NEW_CID`），为每个事务结束事件分配提交序号并上报 walsender；
5. 向 worker 分发事务级上下文：基础快照、目录失效消息、命令 ID；
6. 在快照推进、目录变更时向相关 worker 下发失效通知。

**decode worker 进程**

1. 从与自己的共享内存队列取出 WAL 记录批次与事务上下文；
2. 调用现有的 `heap_decode` / `heap2_decode` / `logicalmsg_decode` 完成解码，复用同一套解析函数，不改动解析语义；
3. 在本地 reorder buffer 中按事务累积变更，达到阈值即切分流式分片；
4. 通过 output plugin 生成协议字节，写入共享内存的结果区；
5. 事务结束时（收到 commit/abort/cancel）执行该事务累积的目录失效消息，释放事务资源。

**walsender 进程（主进程 / 发送进程）**

1. 启动、监控、重启 walreader 与 decode worker；子进程异常时按 9. 的策略处理；
2. 持有复制槽与订阅端的 CopyBoth 连接，负责全部网络收发；
3. 维护提交序号水位，按提交顺序输出 commit/abort/cancel；
4. 把 worker 产出的协议字节写入 socket，并负责 `confirmed_flush` 的推进与客户端反馈处理；
5. 代 walreader 执行复制槽的 xmin 推进（见事实 9）。

### 3.2 进程关系图

```text
                        ┌───────────────────────────────────────────────┐
                        │            walsender（主进程）                  │
                        │  · 持有 replication slot / TCP 连接            │
                        │  · 启动并监控 walreader 与 decode worker        │
                        │  · 按提交序号顺序输出 commit / abort / cancel   │
                        │  · 推进 confirmed_flush、处理 feedback          │
                        └───┬──────────────────────────┬─────────────────┘
       共享内存队列 A       │                          │  共享内存队列 B
   (walreader → walsender)│                          │  (worker → walsender)
   · 提交序号 cseq         │                          │  · 协议字节（可发送）
   · xmin 推进请求         │                          │  · 事务结束通知
   · keepalive 应答        │                          │  · 错误上报
                            ▼                          ▲
                ┌───────────────────────┐             │
                │     walreader 进程     │─────────────┘
                │  · 读 WAL（唯一读取者） │
                │  · 维护 snapbuild      │
                │  · 事务 → worker 绑定  │
                │  · 分配提交序号 cseq   │
                └───────┬───────────────┘
                        │ 共享内存队列（每 worker 一条 SPSC 队列）
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
   ┌────────────┐┌────────────┐┌────────────┐
   │ decode     ││ decode     ││ decode     │
   │ worker #0  ││ worker #1  ││ worker #N  │
   │ 解码+生成  ││ 解码+生成  ││ 解码+生成  │
   │ 协议字节   ││ 协议字节   ││ 协议字节   │
   └────────────┘└────────────┘└────────────┘
```

### 3.3 进程生命周期

- **启动**：walsender 收到 `START_REPLICATION ... LOGICAL` 后，创建共享内存区域，注册并启动 walreader 与 N 个 decode worker（动态后台进程，机制参照 `launcher.c:503` 的逻辑复制 apply worker 先例，用 `bgw_extra` 传递 dsm 句柄）。worker 通过 `BackgroundWorkerInitializeConnection()` 连到发布库（output plugin 需要访问系统目录）。
- **就绪**：walreader 推进 snapbuild 到 consistent 后，向 walsender 汇报就绪；walsender 在此之前不向订阅端发送任何数据（与现有 `DecodingContextFindStartpoint()` 语义一致）。
- **运行**：三进程并行推进，walsender 周期性发送 keepalive 并处理订阅端 feedback。
- **退出**：walsender 退出时通知 walreader 与 worker 收尾；复制槽释放仍由 walsender 负责。

### 3.4 端到端时序

以事务 T1（顶层事务，绑定 worker#0）为例：

```text
walreader            worker#0            walsender           订阅端
   |                    |                    |                 |
   |-- BEGIN_TXN(T1, snapshot) -------------->|  (经共享内存)   |
   |                    |                    |                 |
   |-- WAL 记录批次(T1) --------------------->|                 |
   |                    |-- 解码 -> 协议字节 -|                 |
   |                    |                    |                 |
   |-- BEGIN_TXN(T2, snapshot) -------------->|  (T2 → worker#1)|
   |-- WAL 记录批次(T2) --------------------------> worker#1      |
   |                    |                    |                 |
   |-- COMMIT(T1), cseq=1 ------------------>|                 |
   |                    |                    |-- 提交字节 ---->|  (按 cseq 放行)
   |-- COMMIT(T2), cseq=2 ------------------>|                 |
   |                    |                    |-- 提交字节 ---->|
```

要点：**T1 与 T2 的解码完全并行**，T2 的解码可以早于 T1 完成；只有 commit 的输出顺序由 cseq 强制为 T1 → T2。

## 4. 关键设计决策

### 4.1 决策一：解码切分点——worker 负责到哪一层

需要决定 decode worker 的职责边界。三种切分点：

| 对比项 | 方案 A：事务级 | 方案 B：变更级 | 方案 C：A + 共享关系表 |
| --- | --- | --- | --- |
| worker 职责 | 从 WAL 记录到协议字节，整事务在 worker 内闭环 | 只做 `heap_decode`，产出 `ReorderBufferChange` | 同 A，但关系描述符由 walreader 统一下发 |
| reorder buffer 位置 | 每个 worker 一份 | walsender 一份 | 每个 worker 一份 |
| output plugin 位置 | 每个 worker 一份 | walsender 一份 | 每个 worker 一份 |
| walsender 负载 | 仅网络发送 + 顺序控制 | 网络发送 + 协议格式化 | 同 A |
| 串行路径上的拷贝 | 共享内存 → socket（1 次） | 共享内存 → reorder buffer → 插件缓冲 → socket（3 次） | 同 A |
| 与现状差异 | reorder buffer 与插件实例化逻辑需按 worker 复制 | reorder buffer/plugin 基本不动 | 关系消息去重 |
| 风险 | 插件状态按 worker 复制；RELATION 消息重复 | **插件格式化留在单线程，热点无法并行化** | 实现复杂度最高 |

**选型：方案 A 为主，方案 C 作为关系消息去重的增强项。**

理由：

1. **CPU 热点在插件格式化，不在拷贝。** 事实 12：output plugin 的回调不是在读 WAL 时发生，而是在变更从 reorder buffer **回放**时发生（提交路径 `reorderbuffer.c:2109`、流式路径 `reorderbuffer.c:4054`），热点是插件对每列的 syscache 查类型与类型输出函数调用（`logical/proto.c:828-858`）。方案 B 只把 tuple 解码搬到 worker，**这段热点仍完整地留在单线程的 walsender 上**，无法达成 70MB/s 目标；方案 A 把“解码 + 插件格式化”整体并行化，walsender 只剩网络发送，CPU 分布最均衡。
2. **插件状态必须在同一进程内闭环。** `txn->output_plugin_private` 是 output plugin 的私有状态，跨进程无法共享。方案 A 让 begin → changes → commit 全部在同一个 worker 内走完，插件状态天然自洽；方案 B 要么在 worker 与 walsender 之间传递插件私有状态，要么把格式化退回 walsender，两者都不可取。
3. **方案 A 的串行路径拷贝最少**（共享内存 → socket 一次），符合“调度不成为新瓶颈”的要求；方案 B 多出 reorder buffer 与插件缓冲两次往返。
4. 方案 B 的优点（改动小）可以通过“把 reorder buffer 与插件实例化到 worker、walsender 只保留发送职责”来达成，而不必保留 reorder buffer 在 walsender。

方案 A 的已知代价及处理：

- **RELATION 消息重复**（事实 6）：每个 worker 有独立的 `RelationSyncCache`。按 `pgoutput.c:704-770` 的两级判定拆开看，**放大只发生在非流式事务路径**：流式事务按 `(relation, top-xid)` 粒度判断是否重发，而一个 top-xid 只会归属一个 worker，粒度与事务归属一致，**不额外放大**；非流式事务用的是会话级 `schema_sent` 标志，每个 worker 各有一份，因此同一关系最多被发 N 次。订阅端收到重复的 RELATION 会覆盖缓存，语义正确，代价是少量冗余带宽。**首版按方案 A 实现并容忍该冗余，把方案 C 的共享关系表列为后续优化**（注意其收益主要是消除上述 N 倍放大，而非消除流式路径的重复）。
- **插件实例化**：每个 worker 需要独立的 `LogicalDecodingContext` 与 output plugin 实例（`LoadOutputPlugin()` + `_PG_output_plugin_init`），各自持有 `ctx->out` 缓冲。

### 4.2 决策二：提交顺序保证——commit barrier 与 FIFO 队列对比

这是本设计的核心决策。两种候选方案：

**方案 1：commit barrier（提交水位）**

- walreader 按 WAL 顺序为每个事务结束事件（commit / abort / cancel）分配单调递增的提交序号 `cseq`；
- walsender 维护 `next_commit_seq` 水位。收到 `cseq = k` 的提交消息时，若 `k != next_commit_seq`，说明前面还有未发出的提交，将其挂入有序待提交表；只有当待提交表中 `next_commit_seq` 连续可取时，才依次写出并推进水位；
- 事务的**数据**不受 barrier 约束，随时可发（见事实 4）。

**方案 2：FIFO 队列（按 WAL 读到的提交顺序排队）**

- walreader 把事务结束事件（连同其已解码的字节块）压入一条单一 FIFO，FIFO 按 cseq 排序；
- walsender 严格按 FIFO 顺序取出发送。

**对比：**

| 维度 | commit barrier | FIFO 队列 |
| --- | --- | --- |
| 顺序保证强度 | 强：commit/abort/cancel 严格按 cseq | 强：且事务数据也严格按 cseq，强度更高 |
| 对“尽早流式”的影响 | **无影响**：数据可随时发出，只有提交消息被挂起 | **有影响**：队头事务未解码完成时，队头阻塞，其后已解码的数据全部不能发 |
| 队头阻塞 | 无 | 有：长事务会拖住其后的所有事务 |
| 内存占用 | 小：仅缓存被挂起的提交消息（每条几十字节）+ 已解码待发的数据块 | 大：需缓存乱序完成事务的全部字节，量级与乱序窗口成正比 |
| 乱序容忍度 | 高：解码完成顺序任意 | 低：解码完成顺序必须与提交顺序一致 |
| 吞吐（乱序完成场景） | 高 | 低：整体退化为“提交顺序串行” |
| 非流式订阅方（要求整事务字节连续） | 需额外机制：整事务缓冲后再整块发 | 天然满足 |
| 实现复杂度 | 需维护 cseq 水位 + 有序待提交表 + 空洞检测 | 队列本身保序，逻辑直观 |
| 与 PG 现有语义一致性 | 一致（PG 现状就是“数据可乱序、提交有序”） | 更强于 PG 现状，需额外保证订阅端能接受 |

**结论：采用 commit barrier 为主方案。**

1. 事实 4 已经表明 PG 现有的流式实现本身就不保证事务数据有序，只保证提交有序。barrier 精确复刻了这个语义，是“与现状行为等价”的最小变更；FIFO 反而引入了 PG 现状没有的更强约束。事实 8 进一步说明：在协议 v4 的并行流式模式下，多个在途事务的分片交错下发已是既有行为，barrier 是与之匹配的最小约束。
2. 用户需求明确要求“事务 stream 的时候要尽快发送来提升性能”。barrier 完全不阻碍数据提前下发；FIFO 与该要求直接冲突——只要有一个事务解码慢，FIFO 就会把后面所有已解码的数据一起堵住。
3. 内存开销差距显著。barrier 只需缓存挂起的提交消息；FIFO 需要缓存乱序窗口内所有已解码字节，在高吞吐场景下这个量级不可控。
4. FIFO 的唯一优势（非流式订阅方的整事务连续性）可以用一个退化配置实现：**当订阅方未请求流式时，切换为“整事务缓冲”模式**——此时 worker 不切分分片，walsender 在收到该事务的提交消息时才把整块字节写出。这本质上就是 FIFO 的语义，但只在低并行收益的场景下启用，不影响主路径。

因此最终设计为：**主路径 commit barrier；非流式订阅方走“整事务缓冲”退化路径。** 两种路径共用同一套 cseq 分配与水位管理，只是数据写出时机不同。

### 4.3 决策三：worker 与事务的绑定关系

| 对比项 | 方案一：worker 生命周期 = 事务生命周期 | 方案二：一个 worker 同时处理多个并发事务 |
| --- | --- | --- |
| worker 状态 | 事务结束即释放，worker 空闲 | 长期持有多个在途事务 |
| 负载均衡 | 事务粒度，天然均衡 | 需显式分配算法与再平衡 |
| 关系缓存 | 每事务重建，RELATION 消息重复率高 | 跨事务复用，重复率低 |
| 内存占用 | 与单事务大小成正比 | 与在途事务总数成正比 |
| 提交顺序 | 每个事务的提交由其 worker 产生，仍需 barrier | 同左 |

**选型：方案二（一个 worker 同时处理多个并发事务）。**

理由：方案一每次事务结束就要销毁并重建 worker 进程，进程创建开销与插件实例化开销在 tpcc 那种大量小事务的负载下会成为主要成本；且 worker 频繁重建使关系缓存无法复用，RELATION 消息重复率极高。方案二让 worker 常驻、按事务分工，插件实例与关系缓存长期有效，进程开销摊薄。

方案二带来的设计要求：

- 事务分配策略：walreader 维护“事务→worker”绑定表与各 worker 的在途事务数，分配时选择在途事务最少的 worker（最小负载优先），并保证**同一顶层事务的全部 WAL 记录始终发往同一 worker**（事务内顺序不变）；
- worker 的 reorder buffer 需支持多个并发在途事务，`rb->size` 的内存阈值需要按 worker 数量切分（`logical_decoding_work_mem` 语义从“每解码上下文”变为“每 worker”或总量均摊，见 12.2）；
- 快照：每个事务有自己的 base snapshot（事实 2），worker 需为每个在途事务保存一份快照，事务结束时释放。

## 5. 进程间通信

### 5.1 共享内存布局

采用一块由 walsender 创建的 dsa 区域（`dsa_create()`），各进程通过 `dsa_handle` 附着（`dsa_attach()`）。区域内划分：

| 区域 | 生产者 | 消费者 | 类型 | 说明 |
| --- | --- | --- | --- | --- |
| WAL 分发队列 × N | walreader | worker #i | SPSC 无锁环形队列 | 每 worker 一条，避免竞争 |
| 事务控制队列 | walreader | walsender | SPSC | cseq 分配结果、xmin 推进请求、keepalive 应答 |
| 快照/失效区 | walreader | worker | 每 worker 一块环形区 | 事务级快照与目录失效消息 |
| 解码结果区 | worker | walsender | 每 worker 一块环形区 | 协议字节块 + 事务结束/错误通知 |

选型说明：PostgreSQL 自带的 `shm_mq`（`src/backend/storage/ipc/shm_mq.c`）实现了“单发送者—单接收者”的共享内存队列，语义正好匹配“walreader → 每个 worker”的扇出模型，且提供了 `shm_mq_sendv()` 批量提交能力。`shm_mq` 只能一端发送一端接收，因此 worker → walsender 方向（N 个生产者）需要自建 MPSC 队列，或为每个 worker 建立一条 `shm_mq`（worker → walsender），由 walsender 轮询各队列——后者实现更简单且避免了 MPSC 的缓存行争用，**首版采用“每 worker 一条双向 `shm_mq`”**。

### 5.2 消息定义

**walreader → worker**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_BEGIN_TXN` | xid、拷贝后的快照（xmin/xmax/xip/subxip/curcid）、事务属性 | 事务首次出现，绑定到该 worker；携带的是 `ReorderBufferCopySnap()` 的结果而非 base snapshot（6.3） |
| `PMSG_WAL_BATCH` | 一批 WAL 记录（记录头 + 数据） | 数据类记录批量下发，减少唤醒与加锁 |
| `PMSG_SNAPSHOT_SWITCH` | 新快照（xmin/xmax/xip/subxip/curcid） | 对应 `INTERNAL_SNAPSHOT`，在变更流的特定位置生效（6.2） |
| `PMSG_COMMAND_ID` | cid + 1 | 对应 `INTERNAL_COMMAND_ID`，推进 `curcid`（6.2） |
| `PMSG_INVALIDATION` | `SharedInvalidationMessage` 数组 + 位置标记 | 对应 `INVALIDATION`，在变更流的特定位置执行（6.2） |
| `PMSG_TUPLECID` | (relfilelocator, tid) → (cmin, cmax, combocid) | 来自 `XLOG_HEAP2_NEW_CID`，用于目录行可见性判定，不可省略（6.3） |
| `PMSG_TXN_FINISH` | xid、结束类型（commit/abort/cancel） | 通知 worker 结束事务并释放资源 |

上述事件类消息必须与 WAL 变更**同序**到达 worker，因此实现上宜与 `PMSG_WAL_BATCH` 合并为同一条队列记录中的有序条目，而不是独立队列。

**worker → walsender**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_OUT_BYTES` | 协议字节块（可直接写入 socket） | 解码产出；流式分片或整事务缓冲 |
| `PMSG_TXN_DONE` | xid、cseq | 事务数据已全部产出，可提交 |
| `PMSG_ERROR` | 错误信息 | worker 内部错误，walsender 据此终止会话 |

**walreader → walsender**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_COMMIT_SEQ` | xid、cseq、结束类型 | 提交顺序的来源（见 7.1） |
| `PMSG_XMIN_ADVANCE` | lsn、新的 xmin | 复制槽 xmin 推进请求（事实 9） |
| `PMSG_READY` | — | snapbuild 达到 consistent |

**walsender → walreader**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_XMIN_ACK` | lsn、新的 xmin | walsender 代为调用 `LogicalIncreaseXminForSlot()` 后回执 |
| `PMSG_FLUSH_LSN` | confirmed_flush | 复制槽确认水位回传 |

### 5.3 低拷贝策略

先把拷贝链摆清楚。**跨进程边界必须拷贝，因此并行化后总拷贝次数是增加的**；真正要优化的是“串行路径上还剩几次拷贝”，而不是总次数。

现状（单进程，一条数据变更）：

```text
WAL 页缓冲 --memcpy--> reader decode_buffer --memcpy--> tuple(tup_context)
                --memcpy--> ctx->out --memcpy--> socket
```

方案 A（walreader + worker + walsender）：

```text
WAL 页缓冲 --memcpy--> decode_buffer --memcpy--> [共享内存 WAL 批次]
                --memcpy--> tuple --memcpy--> ctx->out --memcpy--> [共享内存 结果区]
                --memcpy--> socket
```

差别在于：现状这 4 次拷贝全部在**一个**进程里串行发生；方案 A 里除了“共享内存 → socket”这一次，其余都发生在 N 个 worker 中**并行**完成。**串行路径上只剩一次拷贝**，这是本节的目标。

在此基础上，有两处拷贝是可以真正消除的：

1. **输出侧：`ctx->out` → 共享内存结果区。** `ctx->out` 是 `LogicalDecodingContext` 上的一个 `StringInfo`（`logical.c:292`），插件通过 `pq_send*` 直接往里写。把它改为直接指向共享内存结果区（而不是先写私有缓冲再整体拷贝），即可消除这一次。这与现有机制契合：`prepare_write` / `write` 是 `LogicalDecodingContext` 上的可插拔函数指针（`logical.h:64-66`），walsender 侧实现为 `WalSndPrepareWrite` / `WalSndWriteData`（`walsender.c:1549/1576`），worker 侧改为写入共享内存即可。
2. **输入侧：WAL 记录直接解码进共享内存。** 本分支的 `XLogReaderState` 已改为基于循环解码缓冲（`xlogreader.c:446-452`，默认 64KB），并提供 `XLogReaderSetDecodeBuffer()`（`xlogreader.c:90`）允许调用方替换该缓冲。若把 walreader 的解码缓冲放在共享内存里，WAL 记录就**直接解码进共享内存**，消除“decode_buffer → 共享内存”这一次。需要注意循环缓冲会被复用，因此必须双缓冲 + 流控，保证 worker 消费完之前 walreader 不会覆盖该区域。

不可消除的拷贝：页缓冲 → decode_buffer（reader 内部）、decode_buffer → tuple（`DecodeXLogTuple`，`decode.c:1253-1256`）、tuple → `ctx->out`（插件格式化）、共享内存 → socket（socket 写入）。

两点补充：

- **WAL 记录的跨进程传递必须拷贝**：`XLogReadRecord()` 返回的记录指向 reader 私有的解码缓冲，其生命周期契约是“仅在下一次 `XLogReadRecord()` 之前有效”（`xlogreader.c:385-387`），下一次读取会复用该区域（`XLogReleasePreviousRecord`，`xlogreader.c:249-309`）。因此 walreader 必须在读取下一条记录前把当前记录拷入共享内存，或采用上面第 2 条的双缓冲方案。
- **本分支已有现成的跨进程序列化格式可借鉴**：`ReorderBufferSerializeChange()` / `ReorderBufferRestoreChange()`（`reorderbuffer.c:3805-4018` / `4400-4567`）实现了变更（含 tuple 载荷、消息、失效消息、快照、truncate）的完整序列化与还原，用于 spill 到磁盘。它是“把解码结果跨进程搬运”这件事的现成模板，方案 B 以及 worker → walsender 的结果传递都可以直接参照其格式。

### 5.4 调度与唤醒

调度本身不能成为瓶颈，因此约定：

1. **定向唤醒**：walreader 只唤醒目标 worker 的 latch（`SetLatch()`），绝不广播；worker 处理完一批后自行挂起（`WaitLatch()`），无空转。
2. **无锁队列**：SPSC 环形队列用原子变量做头尾同步，避免互斥锁竞争与缓存行争用；队列满时 walreader 阻塞等待（背压，见 5.5）。
3. **批量 + 合并唤醒**：连续多条属于同一 worker 的记录合并成一批，一次唤醒；worker 一次取走全部可用批次。
4. **避免信号风暴**：walreader 的每个批次只做一次 `SetLatch()`，即使队列中已有多个批次待取。
5. **锁粒度**：共享内存元数据（事务绑定表、cseq 水位）用 `LWLock` 或原子操作保护，持有时间极短，不在持锁状态下做 WAL 读取或解码。

### 5.5 流控与背压

- 队列积压超过阈值时，walreader 停止向该 worker 分配新事务，并优先把在途事务分配给空闲 worker；
- 队列接近满时，walreader 阻塞等待（不丢数据、不无界缓冲）；
- worker 产出速度慢于 walreader 读取速度时，天然通过队列长度把压力反传到 walreader，避免内存无界增长；
- 订阅端 TCP 窗口耗尽时，walsender 的 socket 写入阻塞，压力通过结果区反传到 worker——这是最有效的背压，无需额外机制。

## 6. 快照与系统表变更

### 6.1 快照的构建

- snapbuild 由 walreader 独占。walreader 读取 `XLOG_RUNNING_XACTS` 推进快照状态机（`decode.c:372` → `SnapBuildProcessRunningXacts()`，`snapbuild.c:1266`），达到 consistent 后向 walsender 汇报就绪；
- 快照的 `xmin` 取自 `running->oldestRunningXid`（`snapbuild.c:1297`）；`xip` 数组并非来自 running-xacts 的 `xids[]`，而是 snapbuild 累积的**已提交目录修改事务**集合（`snapbuild.c:959-980` `SnapBuildAddCommittedTxn()`）；`xmax` 只在目录修改事务提交时推进（`snapbuild.c:1184-1190`）。这套语义由 snapbuild 内部维护，拆分进程后不变。

### 6.2 快照的事件流（必须按序传递）

现状中一个事务在解码期间会收到三类**插入到变更流中的内部事件**（`reorderbuffer.c` 的 `REORDER_BUFFER_CHANGE_INTERNAL_*`）：

| 事件 | 产生位置 | 语义 |
| --- | --- | --- |
| `INTERNAL_SNAPSHOT` | `ReorderBufferAddSnapshot()`（`reorderbuffer.c:3144`），由 `SnapBuildDistributeSnapshotAndInval()`（`snapbuild.c:870-953`）在**目录修改事务提交**时，向所有已有 base snapshot 的在途事务投递 | “从此处起改用新快照”——事务进行中途发生的 DDL 由此对在途事务可见 |
| `INTERNAL_COMMAND_ID` | `ReorderBufferAddNewCommandId()`（`reorderbuffer.c:3193`），由 `SnapBuildProcessNewCid()`（`snapbuild.c:828`）在解析 `XLOG_HEAP2_NEW_CID` 时投递 | 推进 `snapshot->curcid` |
| `INVALIDATION` | `ReorderBufferAddInvalidations()`（`reorderbuffer.c:3329`），由 `decode.c:297` 在解析 `XLOG_XACT_INVALIDATIONS` 时投递 | 在变更流中的**特定位置**执行目录失效 |

关键点：**这三类事件在现状中是与数据变更混在同一条变更流里的**，其位置有语义。`reorderbuffer.c:2422-2480` 按顺序处理它们：先执行失效（`:2424`），再切换快照（`:2428`），再推进 curcid（`:2460-2480`）。

因此 walreader → worker 的传递不能只是“给一份快照”，而必须是**与 WAL 变更同序的事件流**：

1. walreader 在分发给 worker 的每一批 WAL 记录中，附带该位置上的快照切换事件、命令 ID 事件与失效消息；
2. worker 按顺序处理这些事件：切换快照时重新拷贝快照并重装（`reorderbuffer.c:2428-2458`），推进 curcid 时以新 curcid 重新拷贝快照（`:2460-2480`）；
3. 事务结束时执行累积的失效（`reorderbuffer.c:2577`），保证自己的 catcache/syscache/relcache 不残留脏条目。

### 6.3 worker 侧使用历史快照

现状中解码前会把快照**进程级**安装为历史快照：`reorderbuffer.c:2127` 的 `SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash)`，此后该进程内所有目录读取都走历史视图（`snapmgr.c:216-228` 让 `GetTransactionSnapshot()` 返回 `HistoricSnapshot`）。worker 必须复刻这一点：

- worker 为每个在途事务保存：拷贝后的快照（含 `subxip`）、该事务的 tuplecid 哈希表、当前 `curcid`；
- 解码该事务的任一变更前，先 `SetupHistoricSnapshot()`；
- 快照切换事件到达时 `TeardownHistoricSnapshot()` 后重装新快照。

**必须传递的是拷贝后的快照而非 base snapshot**：`ReorderBufferCopySnap()`（`reorderbuffer.c:1833-1886`）会在 base snapshot 基础上填入 `subxip`（该事务的 xid 及其全部子事务 xid）并设置 `curcid`。缺少 `subxip` 时，“事务自身未提交的目录行”对该事务不可见，会导致自建表并插入的表被漏掉。

**tuplecid 映射不可省略**：事务自身创建目录行时，其可见性由 `cmin < curcid` 判定（`heapam_visibility.c:1604-1644`，经 `ResolveCminCmaxDuringDecoding()` `reorderbuffer.c:5274`），依赖 `txn->tuplecid_hash`。该哈希表由 `ReorderBufferBuildTupleCidHash()`（`reorderbuffer.c:1760-1825`）从 `XLOG_HEAP2_NEW_CID` 记录构建，且**不可落盘**（`reorderbuffer.c:3235-3236` 明确跳过 `INTERNAL_TUPLECID` 的落盘记账）。因此这些记录必须以数据形式传给 worker，不能靠 worker 重新读 WAL 获得。

### 6.4 relid 解析

`reorderbuffer.c:2227-2228` 用 `RelidByRelfilenumber(spcOid, relNumber)` 把 WAL 里的 relfilenumber 映射为 relid。该映射**没有历史视图**（`reorderbuffer.c:2230-2241` 的注释），失败时对普通变更是硬错误。因此：

- relid 解析在 worker 内完成，与现状一致（现状同样在 `ReorderBufferProcessTXN()` 内解析）；
- 前提是各 worker 对 `pg_class` 的视图一致。由于解析发生在已安装的历史快照下、且历史快照不提供 relfilenumber→relid 的历史映射，各 worker 实际使用的是当前映射——这在多 worker 下与单进程等价，但依赖 6.2 的失效事件流在各 worker 侧一致生效。

### 6.5 无事务的失效

`XLOG_XACT_INVALIDATIONS` 中若 xid 无效，现状是立即执行（`decode.c:304-307` 的 `ReorderBufferImmediateInvalidation()`）。这类失效影响的是全局目录状态而非某个事务，**必须由 walreader 本地立即执行**，不能下发到 worker。`XLOG_INVALIDATIONS`（standby 记录，`storage/ipc/standby.c:1479`）逻辑解码直接忽略（`decode.c:388-394`）。

### 6.6 xmin 水位的跨进程聚合

`reorderbuffer.c:1047-1061` 通过事务的 base snapshot 计算 xmin 视野，`snapbuild.c:1311-1316` 用它调用 `LogicalIncreaseXminForSlot()`。事务分散在多个 worker 后，**walreader 必须汇总所有 worker 的快照 xmin** 才能得到全局正确的视野——这要求 worker 把自己的快照 xmin 上报给 walreader，由 walreader 汇总后再请求 walsender 推进槽的 xmin（事实 9）。这是三进程模型中必须显式设计的跨进程聚合点，也是 xmin 停滞的风险点。

## 7. 提交顺序保证的实现

### 7.1 提交序号（cseq）的产生

walreader 是 WAL 的唯一读取者，因此 WAL 顺序即提交顺序。walreader 在读到事务结束记录时分配 cseq：

| 事件 | 分配时机 | cseq |
| --- | --- | --- |
| COMMIT | 读到 `XLOG_XACT_COMMIT` 时 | 分配 |
| ABORT | 读到 `XLOG_XACT_ABORT` 时 | 分配 |
| PREPARE | 读到 `XLOG_XACT_PREPARE` 时 | 分配（两阶段提交需保持 prepare/commit prepared 的配对顺序） |
| CANCEL | 由 `XLOG_RUNNING_XACTS` 清理游离事务时 | 分配 |

cseq 单调递增且连续分配（每个事务结束事件占一个序号），因此“空洞检测”退化为简单的“水位 + 计数”，无需复杂的乱位图。

### 7.2 commit barrier 的实现

walsender 侧维护：

```c
next_commit_seq     /* 下一个允许输出的 cseq */
pending_commits[]   /* 按 cseq 排序的挂起提交表 */
```

- 收到 `cseq = k` 的提交消息时：
  - 若 `k == next_commit_seq`：立即写出，`next_commit_seq++`，随后连续检查 `pending_commits` 中是否有 `next_commit_seq`，有则一并写出并推进水位；
  - 否则：挂入 `pending_commits`（按 cseq 插入有序位置）。
- 挂起的提交消息很小（一条 commit 记录几十字节），挂起数量取决于最早未完成事务与当前事务之间的乱序窗口。

**cseq 的分配与 barrier 的配合**：cseq 由 walreader 分配，因此 walreader 天然知道事务的提交顺序；barrier 只是把这个顺序传递给 walsender 执行。两者解耦：walreader 不需要等任何 worker 就能分配 cseq。

### 7.3 FIFO 方案的实现（备选/退化路径）

若采用 FIFO：walreader 把事务结束事件按 cseq 压入一条 FIFO，walsender 顺序消费。实现简单但存在队头阻塞与内存膨胀问题（见 4.2 对比表），仅在非流式订阅方的“整事务缓冲”退化路径中隐式使用。

### 7.4 顺序保证的边界

必须明确“顺序”到底保证什么，避免实现时过度约束：

| 对象 | 是否要求保序 | 依据 |
| --- | --- | --- |
| commit / abort / cancel | **是**，严格按 cseq | 订阅端按到达顺序 apply 提交 |
| prepare / commit prepared | **是**，与 commit 同一序列 | 两阶段提交的状态机要求 |
| 事务内的变更顺序 | **是**，由 WAL 顺序与单 worker 保证 | 变更间的语义依赖 |
| 不同事务的变更顺序 | **否** | 事实 4：PG 现状已允许乱序下发 |
| 流式分片与提交消息的相对位置 | 分片可先于提交，提交不可先于本事务的全部分片 | 协议要求 |

## 8. 流式发送

### 8.1 尽早流式

用户需求明确指出“事务 stream 的时候要尽快发送来提升性能”。先把现状机制说清楚：

- 触发条件是 reorder buffer 的**聚合**内存：`reorderbuffer.c:3659` 判断 `rb->size >= logical_decoding_work_mem * 1024`，其中 `rb->size` 是所有在途变更的总字节数（`reorderbuffer.c:3253-3259`），不是单事务大小；
- 触发后由 `ReorderBufferLargestStreamableTopTXN()`（`reorderbuffer.c:3592-3621`）挑选**总字节数最大**的顶层事务来输出；
- 一个分片 = 该事务自上一分片以来累积的**全部**变更，**没有固定字节上限**（事实 5）；输出后由 `ReorderBufferTruncateTXN()` 回收内存，阈值被复用。

并行解码为尽早流式提供了三个新的条件：

1. **大事务不再阻塞其他事务**：现状下最大事务被优先选中输出，其余事务要等内存压力缓解；多 worker 下每个 worker 有独立的 reorder buffer 与内存压力，一个大事务不再拖住其他事务的分片产出；
2. **分片触发更频繁**：N 个 worker 各自持有独立的内存预算，单个 worker 的阈值更容易被触及 → 分片更小 → 发送更早；
3. **并行流式模式可用**（事实 8）：协议 v4 允许发布方同时流式多个在途事务，与多 worker 的并行解码天然契合。

为此：

- **新增 GUC 控制流式激进程度**，取值参照现有调试选项 `debug_logical_replication_streaming`（`reorderbuffer.c:3645-3661` 已实现 `buffered` / `immediate` 两种模式，`immediate` 下每条变更都立即流式输出）。生产级 GUC 建议同样提供两档：`buffered`（按内存阈值，默认）与 `immediate`（达到批次粒度即输出）；
- `immediate` 模式下，切分粒度由“共享内存批次大小”决定（建议 64KB），而非事务大小；
- 与 barrier 的关系：尽早流式与 commit barrier 正交——分片提前发，提交仍按 cseq 放行，二者不冲突。

### 8.2 分片与整事务两种输出形态

| 订阅方 | 发布方输出形态 | 说明 |
| --- | --- | --- |
| 请求流式（协议 v2+，`streaming='on'`/`'parallel'`，事实 11） | 分片下发：worker 按阈值切分，每片独立写入 socket | 首选路径，尽早发送 |
| 未请求流式 | 整事务输出：worker 不切分，事务数据在提交时整块写出 | 退化路径，保证整事务字节连续 |

判定依据是订阅方在 `START_REPLICATION` 中传来的 `streaming` 选项，发布方在 `pgoutput.c:488-510` 解析，本方案沿用该机制，**不新增协议**。

注意现状在非流式路径下用的是**落盘**（`ReorderBufferSerializeTXN()` 写临时文件，提交时回放，事实 7），不是内存缓冲。worker 化后是否继续落盘是一个取舍：落盘能复用现有代码且内存恒定，但引入磁盘 IO；改为内存整事务缓冲则省去 IO 但内存占用与事务大小成正比。**首版沿用落盘**，避免为并行场景引入新的内存上界问题。

## 9. 异常处理

| 故障 | 检测方 | 处理 |
| --- | --- | --- |
| worker 内部错误 | worker | `PMSG_ERROR` 上报；walsender 按现有逻辑复制语义终止会话并释放复制槽，**不得静默丢弃变更** |
| worker 进程崩溃 | walsender（`bgw_notify_pid` 失效） | 终止本次复制会话并报错；该会话的变更不完整，不能继续发送 |
| walreader 进程崩溃 | walsender | 同上；WAL 读取位置丢失，会话必须重建 |
| 复制槽失效 / WAL 被回收 | walreader（读 WAL 失败） | 报错终止，会话重建 |
| 快照无法达到 consistent | walreader | 报错终止（与现有 `DecodingContextFindStartpoint()` 行为一致） |
| 队列积压超阈值 | walreader | 停止向该 worker 分配新事务（背压，非故障） |
| 订阅端断开 / TCP 阻塞 | walsender | 现有 `WalSndKeepalive` 与反馈机制处理；阻塞压力自然反压 |

原则：**任何可能造成变更丢失或乱序的故障，一律以终止会话的方式暴露，不做静默降级。** 若需要“降级为串行”的容错能力，应通过 GUC 显式开启并记录告警日志。

## 10. 模块划分

| 模块 | 位置（建议） | 职责 |
| --- | --- | --- |
| parallel decode 主控 | `src/backend/replication/logical/parallel_decode.c` | 子进程生命周期管理、共享内存创建与销毁、cseq 分配、快照分发、失效通知 |
| parallel decode worker | `src/backend/replication/logical/parallel_decode_worker.c` | worker 主循环、WAL 批次消费、解码调度、结果产出 |
| walreader | `src/backend/replication/logical/parallel_walreader.c` | WAL 读取循环、snapbuild 驱动、记录分类与路由 |
| 共享内存消息 | `src/backend/replication/logical/parallel_decode_msg.c` | 消息定义、编解码、队列封装 |
| 现有代码改造 | `logical.c`、`decode.c`、`reorderbuffer.c`、`snapbuild.c`、`pgoutput/pgoutput.c` | 让解码路径支持“从共享内存缓冲读入”“向共享内存写出”“按 worker 实例化 reorder buffer 与插件” |

改造原则：**解析语义零变更**。`heap_decode` / `heap2_decode` 的行为、错误处理、边界条件完全保持，只是输入缓冲的来源和输出缓冲的去向从“进程内私有缓冲”变为“共享内存缓冲”。

## 11. 接口说明

### 11.1 用户接口（新增 GUC）

| GUC | 建议值 | 说明 |
| --- | --- | --- |
| `max_parallel_decode_workers` | 0（关闭，默认）/ N | 并行解码 worker 数量；0 表示回退到现有串行路径 |
| `parallel_decode_streaming_mode` | `buffered` / `immediate` | 流式切分激进程度（见 8.1） |
| `parallel_decode_work_mem` | 建议 `logical_decoding_work_mem` | 每 worker 的解码内存上限（总量 = worker 数 × 该值，需在文档中提示） |
| `parallel_decode_batch_size` | 建议 64KB | WAL 批次与流式分片的切分粒度 |

所有 GUC 均需提供 `sighup` 或 `user` 级可调性，并保证关闭时行为与现状完全一致（AC-8）。

### 11.2 程序内部接口

- `ParallelDecodeCreate()` / `ParallelDecodeDestroy()`：共享内存与子进程管理；
- `ParallelDecodeAssignTxn()`：事务→worker 绑定；
- `ParallelDecodeAllocCommitSeq()`：提交序号分配；
- `ParallelDecodePublishSnapshot()` / `ParallelDecodePublishInvalidations()`：快照与失效分发；
- `ParallelDecodeSubmitBytes()`：worker 产出协议字节。

### 11.3 监控指标

| 指标 | 含义 |
| --- | --- |
| `pg_parallel_decode_workers` | 各 worker 的在途事务数、已解码事务数、队列深度 |
| `pg_parallel_decode_commit_lag` | `next_commit_seq` 与已分配 cseq 的差值（顺序积压） |
| `pg_parallel_decode_txn_latency` | 单事务从首条 WAL 到提交字节产出的耗时分布 |

## 12. 性能

### 12.1 目标与测量

- 目标：tpcc 100 万 tpm 场景下 `confirm_flush_lsn` 速率 ≥ 70MB/s；
- 测量口径与 `need.md` 的 AC-1 一致；
- 需同时测量：单核场景的劣化幅度（应接近 0）、小事务延迟（进程调度不应引入显著抖动）。

### 12.2 资源与内存

- **解码内存**：现状 `logical_decoding_work_mem` 是单个解码上下文的阈值。改为多 worker 后，若每个 worker 都按该值分配，总内存上限为 N 倍。两种选择：(a) 保持 `logical_decoding_work_mem` 语义不变、总量为 N 倍，并在文档中明确；(b) 新增总量级 GUC，worker 均摊。**倾向 (b)**，避免用户无意中放大内存占用。
- **共享内存**：WAL 批次区 + 结果区，大小随并发事务数与在途字节量增长，需设定上限并纳入 `max_worker_processes` 与 `shared_buffers` 之外的独立核算。
- **进程数**：walreader 1 + decode worker N，均为动态后台进程，占用 `max_worker_processes` 名额；与并行查询、并行 apply 的名额竞争需在文档中提示。

### 12.3 性能风险点

| 风险点 | 说明 | 缓解 |
| --- | --- | --- |
| 协议格式化仍单线程 | 若最终退化为方案 B，walsender 成为新瓶颈 | 方案 A 已把格式化移入 worker |
| 共享内存带宽 | 每条数据变更要过共享内存两次（下行 WAL、上行字节） | 批量提交；必要时评估直接内存映射 |
| 唤醒开销 | 小事务高频场景下唤醒次数多 | 批量合并；每 worker 独立 latch |
| 关系缓存重复 | RELATION 消息按 worker 重复 | 首版容忍，后续用共享关系表优化 |
| worker 负载不均 | 事务大小分布不均 | 最小在途事务数优先分配 + 队列水位反馈 |

## 13. 兼容性与约束

1. **发布端行为变更**：开启并行解码后，发布端的 WAL 读取点、快照推进点发生迁移，需要保证复制槽的 `restart_lsn` / `confirmed_flush` 语义与现状等价；
2. **订阅端零改动**：协议格式、消息类型、顺序语义均不改变，订阅端无需升级或改配置；
3. **非流式订阅方**：走整事务输出路径（沿用现状的落盘回放），行为与现状一致；
4. **并行流式模式（协议 v4）**：订阅方以 `streaming='parallel'` 请求时，发布方可同时流式多个在途事务；本特性在此模式下的收益最大，且不引入新的协议语义（事实 8）；
5. **两阶段提交**：`PREPARE` / `COMMIT PREPARED` / `ABORT PREPARED` 参与同一 cseq 序列，保持状态机顺序；
6. **逻辑解码 SQL 接口**（`pg_logical_slot_get_changes()` 等）保持单进程串行实现，不受本特性影响；
7. **第三方 output plugin**：不新增必选接口；插件在 worker 中的实例化需满足“一个插件实例只处理一个数据库连接”的约束，与现状一致；
8. **崩溃恢复**：worker/walreader 崩溃后，会话重建时从复制槽的 `restart_lsn` 重新开始，语义与现状一致；
9. **复制槽的 xmin 推进**：由 walsender 代为执行（事实 9），需保证请求-回执的时序不会导致 xmin 停滞；
10. **订阅端侧文件不改动**：`src/backend/replication/logical/relation.c` 是订阅端的远端→本地关系映射，与本特性无关，不在改造范围内。

## 14. 风险与对策

| 风险 | 影响 | 对策 |
| --- | --- | --- |
| 目录变更导致 worker 读到错误的元数据 | 变更内容错误 | 失效消息随事务下发并在事务结束时执行（6.2）；以 DDL 混合负载重点验证 |
| cseq 空洞导致 barrier 卡死 | 复制停滞 | cseq 连续分配，空洞只可能是 worker 崩溃；配合超时与错误上报转为会话失败 |
| 提交顺序与订阅端 apply 顺序不一致 | 数据不一致 | 以 cseq 严格保序；与现有串行路径做逐事务比对 |
| 共享内存队列成为瓶颈 | 吞吐不升反降 | 批量提交、定向唤醒、零拷贝读写；队列深度纳入监控 |
| worker 负载不均 | 部分 worker 空闲 | 最小在途事务数优先分配；必要时按事务大小加权 |
| 内存占用放大 | OOM 风险 | 总量级 GUC 约束（12.2） |
| 进程数与并行查询/并行 apply 争抢 | 资源不足 | 文档提示；可与 `max_worker_processes` 协调 |

## 15. 待决问题

| 编号 | 问题 | 备选 | 倾向 |
| --- | --- | --- | --- |
| Q1 | 提交顺序方案 | commit barrier / FIFO | commit barrier（4.2） |
| Q2 | 解码切分点 | 事务级 A / 变更级 B / A+关系表 C | A 为主，C 为后续优化（4.1） |
| Q3 | worker 与事务绑定 | 事务生命周期 / 多事务并发 | 多事务并发（4.3） |
| Q4 | 关系消息重复 | 容忍 / 共享关系表 | 首版容忍（4.1） |
| Q5 | 解码内存语义 | 每 worker 独立 / 总量均摊 | 总量均摊（12.2） |
| Q6 | 流式切分默认档 | buffered / immediate | 默认 buffered，`immediate` 按需开启（8.1） |
| Q7 | worker 崩溃是否降级为串行 | 终止会话 / 降级 | 终止会话（9） |
| Q8 | 子进程形态 | 动态后台进程 / walsender 直接 fork | 动态后台进程，参照 `launcher.c:503`（3.3） |

## 16. 实现计划

按可独立验证的阶段推进，每个阶段结束时开关关闭的行为必须与现状完全一致（AC-8）。

| 阶段 | 目标 | 主要改动 | 阶段验收 |
| --- | --- | --- | --- |
| P0 基础设施 | 建立共享内存与子进程骨架 | `parallel_decode.c`、`parallel_decode_msg.c`：dsa 创建与附着、`shm_mq` 封装、消息编解码、动态后台进程注册 | 单 worker 串行跑通一轮复制，字节输出与现状逐字节一致 |
| P1 walreader 拆分 | 把 WAL 读取与 snapbuild 从 walsender 迁到 walreader | `parallel_walreader.c`：WAL 读取循环、snapbuild 驱动、记录分类与路由；walsender 侧改造为只做网络与顺序 | 解码结果不变，`restart_lsn` / `confirmed_flush` 语义等价（AC-2、AC-8） |
| P2 worker 解码 | worker 承担解码与插件格式化 | `parallel_decode_worker.c`：批次消费、reorder buffer 与 output plugin 实例化、结果产出；`logical.c` 支持按 worker 实例化 | 事务数据全部经 worker 产出，walsender 只剩发送（AC-2） |
| P3 顺序保证 | 引入 cseq 与 commit barrier | cseq 分配、`next_commit_seq` 水位与待提交表 | commit/abort/cancel 顺序与 WAL 一致（AC-3） |
| P4 多 worker 并行 | worker 池与事务分配 | 事务→worker 绑定表、最小负载优先分配、队列水位反馈 | 吞吐随 worker 数上升（AC-1、AC-6） |
| P5 快照与失效 | 事件流分发与 tuplecid 传递 | 快照切换 / 命令 ID / 失效消息与 WAL 变更同序传递；`ReorderBufferCopySnap()` 结果与 tuplecid 哈希跨进程传递 | DDL 混合负载下元数据与现状一致（AC-5） |
| P6 尽早流式 | 分片切分与提前下发 | 流式模式 GUC、批次粒度切分、结果区提前发送 | 大事务在解码完成前即可到达订阅端（AC-4） |

推进顺序上的三条约束：

1. **P1 之前不改解码路径。** 先把 WAL 读取搬走、解码仍留在 walsender，用现有路径验证进程拆分本身没有引入行为差异。
2. **P2 之前不引入多 worker。** 单 worker 打通后，多 worker 只是分配策略的变化；否则进程拆分、解码改造、并行调度三个变量同时出错，难以定位。
3. **P3 可以在 P1 之后独立推进。** cseq 的分配只依赖 walreader 的 WAL 顺序，与 worker 是否解码无关，可以与 P2 并行实现。

## 附录 A 术语表

| 术语 | 说明 |
| --- | --- |
| walreader | 发布端新增的 WAL 读取进程。WAL 的唯一读取者，负责推进 snapbuild、路由数据类 WAL、分配提交序号 |
| decode worker | 发布端新增的解码进程。持有独立的 reorder buffer 与 output plugin 实例，把 WAL 解析为协议字节 |
| walsender | 发布端主进程。持有复制槽与订阅端连接，负责启动子进程、网络收发、按提交顺序输出、推进 `confirmed_flush` |
| cseq（提交序号） | walreader 按 WAL 顺序为每个事务结束事件分配的序号，是提交顺序的唯一载体（见 7.1） |
| commit barrier | 提交水位机制。walsender 只在 `next_commit_seq` 连续可取时才写出提交消息（见 4.2、7.2） |
| FIFO 队列 | 按 WAL 提交顺序排队的输出方案。顺序更强，但存在队头阻塞，与“尽早流式”冲突（见 4.2） |
| reorder buffer | 事务重组缓冲区。按事务累积变更，受 `logical_decoding_work_mem` 阈值约束，超限则流式输出或落盘 |
| snapbuild | 快照构建状态机。维护解码所需的一致性快照，由 walreader 独占（见 6.1） |
| 历史快照 | `SetupHistoricSnapshot()` 安装的进程级快照视图。解码期间所有目录读取都走该视图（见 6.3） |
| 目录失效消息 | `SharedInvalidationMessage`。catalog 变更时投递给在途事务，在事务结束时执行（见 6.2） |
| 事件流 | 与数据变更同序传递的快照切换、命令 ID、失效消息序列。三者位置有语义，不可拆散（见 6.2） |
| 流式分片 | 一个事务自上一分片以来累积的全部变更，构成一次独立下发的字节块（见 8.1） |
| 整事务缓冲 | 非流式订阅方的退化路径。事务数据不切分，在提交时整块写出（见 8.2） |
| SPSC / MPSC | 单生产者单消费者 / 多生产者单消费者队列。`shm_mq` 只支持 SPSC（见 5.1） |
| dsa | 动态共享内存区域。walsender 创建、各进程通过 `dsa_handle` 附着（见 5.1） |
| xmin 水位 | 所有在途事务可见性视野的下界。决定复制槽能释放多老的事务（见 6.6） |

## 附录 B 修订记录

| 版本 | 日期 | 修订内容 |
| --- | --- | --- |
| v0.1 | 2026-11-17 | 初稿：完成 `need.md` 需求说明书 |
| v0.2 | 2026-11-18 | 重构为 walreader / decode worker / walsender 三进程模型；补充 commit barrier 与 FIFO 的对比与选型；补充快照事件流、tuplecid 传递与 xmin 跨进程聚合；补充低拷贝策略与可消除拷贝的分析；统一为标准 Markdown 格式并补充目录与术语表；新增第 16 章实现计划 |
