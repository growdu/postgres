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
  - [3.5 目标部署形态](#35-目标部署形态)
- [4. 关键设计决策](#4-关键设计决策)
  - [4.1 顺序保证模型：三层契约](#41-顺序保证模型三层契约)
    - [4.1.1 O1：事务内序](#411-o1事务内序)
    - [4.1.2 O2：流段 framing 序](#412-o2流段-framing-序)
    - [4.1.3 O3：结束事件序](#413-o3结束事件序)
    - [4.1.4 三层的关系与派生约束](#414-三层的关系与派生约束)
  - [4.2 决策一：解码切分点——worker 负责到哪一层](#42-决策一解码切分点worker-负责到哪一层)
  - [4.3 决策二：提交顺序保证——commit barrier 与 FIFO 队列对比](#43-决策二提交顺序保证commit-barrier-与-fifo-队列对比)
  - [4.4 决策三：事务与 worker 的绑定关系](#44-决策三事务与-worker-的绑定关系)
  - [4.5 事务亲和性与事务内顺序保证](#45-事务亲和性与事务内顺序保证)
    - [4.5.1 绑定规则](#451-绑定规则)
    - [4.5.2 事务内顺序的三重保证](#452-事务内顺序的三重保证)
    - [4.5.3 与流式切分的交互](#453-与流式切分的交互)
    - [4.5.4 多事务并发下的相互影响](#454-多事务并发下的相互影响)
- [5. 进程间通信](#5-进程间通信)
  - [5.1 共享内存布局](#51-共享内存布局)
  - [5.2 消息定义](#52-消息定义)
    - [5.2.1 推导：下行载荷为什么必须是原始 WAL 记录](#521-推导下行载荷为什么必须是原始-wal-记录)
    - [5.2.2 下行条目（walreader → worker）](#522-下行条目walreader-worker)
    - [5.2.3 上行条目（worker → walsender）](#523-上行条目worker-walsender)
    - [5.2.4 控制类消息](#524-控制类消息)
    - [5.2.5 一个必须显式处理的边界情形](#525-一个必须显式处理的边界情形)
  - [5.3 低拷贝策略](#53-低拷贝策略)
  - [5.4 调度与唤醒](#54-调度与唤醒)
  - [5.5 流控与背压](#55-流控与背压)
- [6. 快照与系统表变更](#6-快照与系统表变更)
  - [6.1 快照的构建](#61-快照的构建)
  - [6.2 快照的事件流（必须按序传递）](#62-快照的事件流必须按序传递)
  - [6.3 worker 侧使用历史快照](#63-worker-侧使用历史快照)
  - [6.4 relid 解析](#64-relid-解析)
  - [6.5 无事务的失效](#65-无事务的失效)
  - [6.6 槽水位的跨进程聚合](#66-槽水位的跨进程聚合)
    - [6.6.1 xmin 水位](#661-xmin-水位)
    - [6.6.2 restart_lsn 水位](#662-restart_lsn-水位)
    - [6.6.3 汇总消息](#663-汇总消息)
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
  - [15.1 收益测算：并行度并不需要很大](#151-收益测算并行度并不需要很大)
  - [15.2 待实测确认清单](#152-待实测确认清单)
- [16. 实现计划](#16-实现计划)
- [17. 故障恢复](#17-故障恢复)
  - [17.1 恢复的唯一持久化锚点是复制槽](#171-恢复的唯一持久化锚点是复制槽)
  - [17.2 故障分类与处置](#172-故障分类与处置)
  - [17.3 worker 处理到一半崩溃](#173-worker-处理到一半崩溃)
  - [17.4 walreader 崩溃](#174-walreader-崩溃)
  - [17.5 walsender 崩溃与会话断开](#175-walsender-崩溃与会话断开)
  - [17.6 数据库重启后的恢复](#176-数据库重启后的恢复)
  - [17.7 恢复语义与不变量](#177-恢复语义与不变量)
  - [17.8 实现约束清单](#178-实现约束清单)
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

1. `walsender.c:XLogSendLogical()` 循环调用 `decode.c:88` 的 `LogicalDecodingProcessRecord()`；
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
| `xact` COMMIT / ABORT / PREPARE | 事务结束 | walreader | 提交顺序的来源；结束事件再路由给持有该事务的 worker 做收尾（7.1） |
| `xact` ASSIGNMENT | 子事务归属 | **walreader 门控后转发给 worker** | worker 用它复刻 `ReorderBufferAssignChild()`（5.2.1） |
| `xact` INVALIDATIONS | 目录失效消息 | **walreader 门控后转发给 worker**（xid 有效时）；xid 无效时**广播给所有 worker**（6.5） | 失效要落到持有该事务的 worker 上 |
| `heap2` `XLOG_HEAP2_NEW_CID` | 命令 ID + tuplecid | **walreader 门控后转发给 worker** | worker 用它重建命令 ID、tuplecid 哈希与 `rbtxn_has_catalog_changes` 标志（6.3） |
| `heap` | INSERT / UPDATE / HOT UPDATE / DELETE / TRUNCATE / CONFIRM | decode worker | 数据变更，解码主体 |
| `heap` `XLOG_HEAP_INPLACE` | 目录行的就地更新 | **walreader 门控后转发给 worker** | 不解码任何数据（`decode.c:509-513`：目录元组不参与解码），但要置位 `rbtxn_has_catalog_changes`（`decode.c:527` 的 `ReorderBufferXidSetCatalogChanges()`）——tuplecid 哈希表建表的必要条件（6.3）。该标志落在 worker 的 reorder buffer 上，因此这条记录必须到 worker |
| `heap` `XLOG_HEAP_LOCK` | 行级锁 | walreader | `decode.c:535`：不关心行锁 |
| `heap2` `XLOG_HEAP2_MULTI_INSERT` | 批量插入 | decode worker | 数据变更，解码主体 |
| `heap2` `XLOG_HEAP2_REWRITE` | 仅供崩溃/归档恢复 | walreader | `decode.c:435-441`：注释明确"所有工作都在恢复时完成，这里什么都不用做" |
| `heap2` `XLOG_HEAP2_VISIBLE` / `PRUNE_*` / `LOCK_UPDATED` | 可见性位、清理、锁 | walreader | `decode.c:448-453`：纯物理操作，逻辑解码不关心 |
| `message` | 逻辑复制消息 | decode worker（事务型）/ 任意 worker（非事务型） | 事务型消息随事务分发 |

**分类的要点是"归属"不是"处理"。** `ASSIGNMENT` / `INVALIDATIONS` / `NEW_CID` 三类记录虽然数量少、看起来像控制信息，但它们携带的是**事务内部状态**，只有持有该事务的 worker 才能消费。因此 walreader 对它们只做**门控与路由**，语义解释仍在 worker 侧完成（5.2.1）。这是"下行载荷必须是原始 WAL 记录"这一结论的直接推论。

### 2.4 来自现有实现的关键事实（设计约束）

以下事实直接来自本仓库代码，是本设计的约束前提，方案必须成立而非重新论证。

**事实 1：顶层事务 ID 可独立获取。** `decode.c:88` 的 `LogicalDecodingProcessRecord()` 通过 `XLogRecGetTopXid(record)` 从 WAL 记录头直接得到顶层事务 ID，无需查询 reorder buffer。因此 walreader 可以仅凭 WAL 记录自身决定分发目标，这是并行分发的前提。

**事实 2：快照按事务分配。** `snapbuild.c:778-820` 的 `SnapBuildProcessChange()` 在事务的**首条可解码变更**处构建快照并调用 `ReorderBufferSetBaseSnapshot()` 绑定到该事务；`reorderbuffer.c:3162` 的 `ReorderBufferSetBaseSnapshot()` 保存 `txn->base_snapshot`。快照是**事务级**而非全局单例，因此可以随事务一起分发给 worker。

**事实 3：目录失效消息按事务累积、在事务结束时执行。** `decode.c:281-308` 处理 `XLOG_XACT_INVALIDATIONS`，调用 `ReorderBufferAddInvalidations()` 挂到事务上；提交路径在 `reorderbuffer.c:2577/2623/2913` 调用 `ReorderBufferExecuteInvalidations()` 执行，**回滚路径走的是 `ReorderBufferImmediateInvalidation()`**（`reorderbuffer.c:2957`，在 abort 时立即执行）。带 xid 的失效天然属于事务，随该事务分发即可；**xid 无效的失效是例外**——它影响全局目录状态，而每个 worker 有自己的目录缓存，因此必须广播（见 6.5）。

**事实 4：流式模式下事务数据本身可以是乱序的，只有 commit 需要保序。** `reorderbuffer.c:3659-3676` 的 `ReorderBufferCheckMemoryLimit()` 在 `rb->size`（reorder buffer 中所有在途变更的总字节数）超过 `logical_decoding_work_mem * 1024` 时触发流式，并通过 `ReorderBufferLargestStreamableTopTXN()` **挑选总字节数最大的事务**优先输出——可见 PG 已允许事务数据乱序下发。而 commit/abort 始终在对应的 WAL 提交记录处按 WAL 顺序发出：`decode.c:216-243` 解析 `XLOG_XACT_COMMIT` → `DecodeCommit()`（`decode.c:733` 调用 `ReorderBufferCommit()`）→ `ReorderBufferReplay()`（`reorderbuffer.c:2679`）立即输出该事务。**这意味着并行解码必须保证的只有 commit/abort/cancel 的相对顺序，事务数据不必保序。** 本设计的顺序方案全部建立在这一事实上。

**事实 5：分片的粒度不是固定字节数。** `ReorderBufferStreamTXN()`（`reorderbuffer.c:4054-4166`）输出的是“该事务自上一分片以来累积的全部变更”，触发条件是 reorder buffer 的**聚合**内存超阈值，而非单事务大小，因此一个 1GB 的事务可以被整体作为一个分片输出。流式输出后 `ReorderBufferTruncateTXN()` 回收该分片的内存（`rb->size` 回落到阈值以下），阈值被反复复用。

**事实 6：RELATION 消息的重复粒度是“每个流式事务”。** `pgoutput.c:126-140` 的 `RelationSyncEntry.streamed_txns` 记录“已为哪些顶层事务发过该关系的描述”，`maybe_send_schema()`（`pgoutput.c:704-770`）在流式模式下按 top-xid 判断是否需要重发；`cleanup_rel_sync_cache()`（`pgoutput.c:2293-2323`）在事务提交时把 xid 移出该表并置 `schema_sent = true`（会话级标志）。因此单进程下 RELATION 是“每关系每会话一次（失效后重发）”；**一旦把插件实例复制到 N 个 worker，每份缓存各自独立，最坏情况下 RELATION 消息量放大到 N 倍**。

**事实 7：非流式订阅方的大事务走落盘而非流式。** `reorderbuffer.c:3677-3691` 在无法流式时调用 `ReorderBufferSerializeTXN()` 把事务写入 `pg_logical/reorderbuffer` 下的临时文件，提交时再整体回放输出。因此“整事务字节连续”是现状既有行为。但**落盘路径在多 worker 下不可用**：它依赖进程全局的 `MyReplicationSlot`（worker 进程里为 NULL），且所有 worker 共享同一个 `pg_replslot/<slot>/` 目录，`ReorderBufferCleanupSerializedTXNs()`（`reorderbuffer.c:4598-4629`）会删掉该目录下所有 `xid*` 文件——它在每个 worker 的启动与退出时都会被调用，因此 worker 之间会互删文件。并行模式改为**内存整事务缓冲**（见 8.2）。

**事实 8：协议 v4 是订阅端的并行 apply，不是发布端的并行流式。** `logicalproto.h:26-45` 的四个版本号分别是：v1 基础协议、v2 流式（PG14）、v3 两阶段提交（PG15）、**v4 “applying large streaming transactions in parallel”（PG16）**。v4 的语义全部落在**订阅端**：`CREATE SUBSCRIPTION ... WITH (streaming = parallel)`（`subscriptioncmds.c:254-260` 解析该选项，`defGetStreamingMode()` 在 `:2389`；`pg_subscription.h:162-176` 定义 `LOGICALREP_STREAM_PARALLEL 'p'`）要求订阅端用 parallel apply worker 回放流式事务（`applyparallelworker.c`）。**发布端在这一模式下没有获得任何新能力**——它仍然是单进程串行解码、单流段顺序下发。发布端并行流式（publisher-side parallel streaming）是 PG18 的特性，本分支不具备。

**事实 8a：`streaming='parallel'` 的端到端生效条件是一条完整的链条，缺一不可。** 这条链决定了本特性的目标部署形态（见 3.5）：

| 环节 | 源码 | 行为 |
| --- | --- | --- |
| 订阅端只在发布端够新时才请求并行流式 | `worker.c:4372-4378` | `server_version >= 160000 && MySubscription->stream == LOGICALREP_STREAM_PARALLEL` 时才发 `streaming_str = "parallel"` 并置 `MyLogicalRepWorker->parallel_apply = true`；否则退化为 `"on"`（`parallel_apply = false`）或 `NULL` |
| 发布端校验协议版本 | `pgoutput.c:496-509` | 收到 `'p'` 但 `protocol_version < LOGICALREP_PROTO_STREAM_PARALLEL_VERSION_NUM`（v4）→ `ereport(ERROR, "requested proto_version=%d does not support parallel streaming")`；收到 `'t'` 但 < v2 同理 |
| 订阅端的并行 apply worker 数量 | `guc_tables.c:3190-3199` | `max_parallel_apply_workers_per_subscription`（默认 **2**，`PGC_SIGHUP`） |
| 每个流式事务分配一个 apply worker | `worker.c:1499` → `applyparallelworker.c:470-516` | `pa_allocate_worker(stream_xid)` 在 **STREAM START** 时为该事务分配一个 parallel apply worker；worker 从池中复用（`applyparallelworker.c:411-418`），但每事务都要建 dsm + 哈希项 |

**事实 8b：订阅端的并行度来自"发布端有多少事务在流式"，而不是来自 wire 的并发流段。** 事实 13 已证明 wire 上同时只有一个打开的流段。订阅端的并行是这样形成的：leader apply worker 读到 STREAM START 时为该事务分配一个 parallel apply worker 并把分片转发过去（`worker.c:1663-1670` 的 `TRANS_LEADER_SEND_TO_PARALLEL`），leader 自己继续读下一个流段。**因此"多个事务的流式分片在时间上重叠"才是订阅端并行的来源。**

而现状发布端**几乎不产生流式**：分片的触发条件是 reorder buffer 的聚合内存超限（`reorderbuffer.c:3659`），负载正常时根本不会触发。结果是：**订阅端即使配了 `streaming='parallel'` 与多个 apply worker，也拿不到可并行的流式事务**。这正是本特性必须"尽早流式"的原因（见 8.1）——不是可选的优化，而是让订阅端的并行 apply 真正能被喂饱的前提。

**事实 9：复制槽绑定在 walsender 进程。** `logical.c:1693-1699` 的 `LogicalIncreaseXminForSlot()` 使用 `MyReplicationSlot`（进程局部变量），`snapbuild.c:1316` 在 `SnapBuildProcessRunningXacts()` 中调用它推进槽的 catalog_xmin。**拆分进程后，walreader 持有 snapbuild 却无法推进槽的 xmin**，必须由 walsender 代劳（见 5.2 的 `PMSG_WATERMARK_ADVANCE` / `PMSG_WATERMARK_ACK` 与 6.6）。

**事实 10：等待新 WAL 的逻辑与网络职责耦合。** `walsender.c:1054-1069` 的 `logical_read_xlog_page()` 依赖 `WalSndWaitForWal()`，后者同时负责 keepalive 与客户端反馈。walreader 需要自己的等待路径，不能复用它（见 3.1 与 5.4）。

**事实 11：是否流式由发布方决定，判据是订阅方传来的选项。** `libpqwalreceiver.c:588-590` 订阅方在 `START_REPLICATION` 中带 `streaming` 选项；发布方在 `pgoutput.c:488-510` 依据该选项与协议版本（`logicalproto.h:26-45`：v1 基础、v2 流式、v3 两阶段提交、v4 订阅端并行 apply）决定 `ctx->streaming`。发布方因此**知道**当前订阅方是否接受分片下发，但**不知道**订阅端是否启用 parallel apply worker——这不影响发布端的输出格式，两者正交。

**事实 12：output plugin 不是在读 WAL 时调用的，而是在事务回放时调用的。** 读 WAL 阶段只把变更**入队**（`decode.c:893` `DecodeInsert` → `reorderbuffer.c:788` `ReorderBufferQueueChange`）；插件回调发生在变更从 reorder buffer **回放**时——提交时（`ReorderBufferCommit()` → `ReorderBufferProcessTXN()`，`reorderbuffer.c:2109`）或流式时（`ReorderBufferStreamTXN()`，`reorderbuffer.c:4055`）。由此得到两条对本设计至关重要的推论：

- **CPU 热点不在拷贝，而在插件格式化。** 每次变更的每列都要做一次 syscache 查类型 + 一次类型输出函数调用 + 一次分配（`logical/proto.c:828-858`），每条变更还要做一次 relmapper 查表 + 一次 relcache 打开（`reorderbuffer.c:2230-2255`）。相对而言 memcpy 是 GB/s 级的廉价操作。**只把“WAL→tuple”搬到 worker 省下的是拷贝，不是热点。**
- **一个事务的插件调用不能跨进程拆分。** `txn->output_plugin_private`（pgoutput 的 `PGOutputTxnData`，`pgoutput.c:574` 创建、`pgoutput.c:610` 销毁）必须在同一个进程内走完 begin→changes→commit，且 `ReorderBufferProcessTXN()` 在调用插件前会安装历史快照并开启内部事务（`reorderbuffer.c:2127/2145-2149`）。

**事实 13：wire 上同一时刻只允许一个打开的流段。** 订阅端用一个**进程级单值**跟踪当前流：`worker.c:302-309` 的 `in_streamed_transaction` 与 `stream_xid`。`apply_handle_stream_start()`（`worker.c:1465-1500`）收到 `STREAM_START` 时，若 `in_streamed_transaction` 已为真，直接 `ereport(ERROR, ERRCODE_PROTOCOL_VIOLATION, "duplicate STREAM START message")`——**这是运行时错误，release build 同样触发**，不是断言。发布端侧同样只有单值：`pgoutput.c:1783-1828` 的 `pgoutput_stream_start()` / `pgoutput_stream_stop()` 维护插件实例级的 `data->in_streaming` 布尔量。
更危险的是 `worker.c:996` 与 `:1053` 的 `Assert(!TransactionIdIsValid(stream_xid))` 只保护普通 `BEGIN` / `BEGIN PREPARE`，**release build 下被编译掉**；而变更的事务归属完全由全局 `stream_xid` 决定（`worker.c:566` 的 `handle_streamed_transaction()`）。因此一旦把另一个事务的字节插进一个打开的流段里，订阅端会把 T2 的变更写进 T1 的事务上下文——**静默数据损坏，不报错**。
发布端现状之所以满足该约束：整个流段由 `ReorderBufferProcessTXN()`（`reorderbuffer.c:2109`）在**一次函数调用内**产出——`rb->stream_start`（`:2181`）→ `rb->stream_change`（`:1999`）× N → `rb->stream_stop`（`:2535`）→ `rb->stream_commit`（`:1933`），单进程单调用，中间不可能插入别的字节；**不同事务的流段之间也从不交错**。换言之：流段 framing 序在现状下是“免费”的，**并行化后必须显式重建**。

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
3. 维护事务→worker 的绑定表与每个 worker 的**在途字节水位**（分配代价函数，见 4.5.1）；
4. 处理控制类记录（`xact`、`standby`、`xlog`），为每个事务结束事件分配提交序号并上报 walsender；`NEW_CID` 记录**作为数据记录下发**给 worker，由 worker 自行重建 tuplecid 与 `rbtxn_has_catalog_changes` 标志（5.2.1、6.3）；
5. 分配 base snapshot 并写入**快照版本池**，通过 `PD_ENT_SNAPSHOT` 下发给 worker（6.1）；在目录变更时下发快照切换；
6. 广播 xid-less 的目录失效消息给所有 worker（6.5）；
7. 维护 `PD_DECODE_STATE` 状态块（snapbuild 状态、`start_decoding_at`），供 worker 只读（6.1）。

**decode worker 进程**

1. 从与自己的共享内存队列取出 WAL 记录批次与事务上下文；
2. 同时承载多个在途事务，每个事务的全部 WAL 记录都由本 worker 按 WAL 顺序处理（见 4.5）；
3. 调用现有的 `heap_decode` / `heap2_decode` / `logicalmsg_decode` 完成解码，复用同一套解析函数，不改动解析语义；worker 侧没有完整的 snapbuild，只有一个**只读替身**提供 `SnapBuildCurrentState()` 与 `SnapBuildProcessChange()`（6.1）；
4. 在本地 reorder buffer 中按事务累积变更，达到阈值即切分流式分片；
5. 通过 output plugin 生成协议字节，写入共享内存的结果区；
6. 事务结束时（收到 commit/abort/cancel）执行该事务累积的目录失效消息，释放事务资源，并向 walsender 回执 `END_ACK`（7.2）；
7. 以事务为单位串行解码，逐事务 `SetupHistoricSnapshot()` / `TeardownHistoricSnapshot()`——历史快照与 tuplecid 映射是进程级单例（6.3）。

**walsender 进程（主进程 / 发送进程）**

1. 启动、监控 walreader 与 decode worker；子进程异常时按第 9 章的策略处理——**不做进程级重启**，一律终止会话并按第 17 章重建（17.3、17.4）；
2. 持有复制槽与订阅端的 CopyBoth 连接，负责全部网络收发；
3. 维护提交序号水位，按提交顺序输出 commit/abort/cancel；
4. 把 worker 产出的协议字节写入 socket，并负责 `confirmed_flush` 的推进与客户端反馈处理；
5. 代 walreader 执行复制槽的 xmin 与 restart_lsn 推进（见事实 9、6.6）。

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

**子进程句柄的持有方式**。`RegisterDynamicBackgroundWorker()`（`bgworker.h:118-119`）返回 `BackgroundWorkerHandle *`，walsender 必须持有它，用于：

| 用途 | 接口 |
| --- | --- |
| 等待 worker 完成启动（拿到 pid，用于 `SetLatch()` 定向唤醒） | `WaitForBackgroundWorkerStartup(handle, &pid)`（`bgworker.h:122-124`） |
| 查询 worker 是否已退出 | `GetBackgroundWorkerPid(handle, &pid)`（`bgworker.h:122-124`），返回 `BgwHandleStatus` |
| 主动终止 worker（会话收尾时） | `TerminateBackgroundWorker(handle)`（`bgworker.h:130`） |

**句柄只用于生命周期管理，不用于状态传递。** worker 的运行状态（RUNNING / EXITED / FAILED）放在共享内存状态字里（§5.1 的错误通道），walsender 读共享内存判定，不查句柄——因为 `GetBackgroundWorkerPid()` 只能回答"进程还在不在"，回答不了"它为什么死"。两者的分工是：句柄回答"是否还在"，状态字回答"是否健康"。§17.3 的三重检测机制中，postmaster 的 SIGUSR1 通知负责"立刻醒来"，状态字负责"为什么"。


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

### 3.5 目标部署形态

**本特性的目标场景是"发布端并行解码 + 订阅端并行 apply"，二者缺一不可。** 这个定位不是取舍，而是端到端吞吐的物理约束（15.1 的测算：解码侧 N = 3~4 个 worker 即够，真正的瓶颈在 walsender 的每帧系统调用、段粒度串行下发与订阅端 apply 速率）。

| 端 | 要求 | 源码依据 | 不满足时 |
| --- | --- | --- | --- |
| 订阅端 | `CREATE SUBSCRIPTION ... WITH (streaming = 'parallel')` | `pg_subscription.h:175` 的 `LOGICALREP_STREAM_PARALLEL 'p'`；`worker.c:4372-4378` 只在 `server_version >= 160000` 时才发 `"parallel"` | 订阅端退化为串行 apply，发布端再快也压不上去 |
| 订阅端 | `max_parallel_apply_workers_per_subscription > 0`（默认 2） | `guc_tables.c:3190-3199` | 同上 |
| 发布端 | 支持协议 v4（PG16+） | `pgoutput.c:501-505` 收到 `'p'` 且 proto_version < v4 直接报错 | 订阅端会退化为 `"on"`；发布端必须支持 v4 才能被请求 |
| 发布端 | 开启本特性 | 本设计 | 走现有串行路径 |
| 订阅端 | **不要求任何改动** | 事实 8、13 | — |

**"流尽量走 stream 发送"是这条链上的必要一环，不是可选项。** 事实 8b 已经说明：订阅端的并行 apply 靠的是"多个事务的流式分片在时间上重叠"，而现状发布端只在内存超限时才流式，正常负载下**根本不产生流段**。因此：

- 订阅端配了 `streaming='parallel'` 与多个 apply worker，如果发布端不主动流式，apply worker 是闲的；
- 反过来，发布端一旦主动流式，订阅端的 leader 就能连续为不同事务分配 apply worker，**端到端并行度才成立**；
- 这也把本设计从"发布端单方面的优化"变成"发布端与订阅端协同的方案"——发布端的流式激进程度，直接决定订阅端并行的利用率。

**由此确定三条设计取舍**：

1. **流式是主路径，整事务块是例外。** 并行模式下默认尽早流式（8.1），`PD_BLK_TXN`（整事务块）只在事务未被流式化时出现。副作用是好的：整事务字节不再需要在挂起区里等到自己的 cseq 放行，挂起区里只剩几十字节的提交消息，need.md §4.3 描述的死锁风险面因此大幅缩小。
2. **保提交序仍然必需，与订阅端是否并行无关。** 三条理由：(a) 订阅端的 flush position 按提交 LSN 追踪在途事务（`worker.c:3448-3460` 的 `lsn_mapping`），提交序乱了水位就乱；(b) S1 不变式（4.1.3）是发布端自己的性质，与订阅端行为无关；(c) AC-2 要求 apply 结果与串行路径一致，两个事务改同一行时 apply 顺序不同则最终状态不同。
3. **O2（wire 上同时只有一个打开的流段）不变。** 事实 13 是订阅端的运行时错误（release build 同样触发），且 `stream_xid` 是进程级单值。订阅端的并行 apply 并**不是**靠 wire 上多个并发流段实现的（事实 8b）。

## 4. 关键设计决策

### 4.1 顺序保证模型：三层契约

本节把“顺序”拆成三个**互不替代**的层次。每一层有独立的契约、独立的保证机制、独立的执行点。

把它们混为一谈是本设计早期版本的主要缺陷：只设计了第三层（结束事件序），漏掉了第二层（流段 framing 序）——而第二层恰恰是并行化**新引入**的约束，且一旦违反，后果是订阅端的静默数据损坏而不是报错。

| 层 | 名称 | 契约（一句话） | 保证机制 | 执行点 | 并行化后的难度变化 |
| --- | --- | --- | --- | --- | --- |
| O1 | 事务内序 | 同一顶层事务的变更在 wire 上按 WAL 顺序出现，且不被拆到多个 worker | 事务→worker 多对一绑定 + 顺序追加 + worker 单线程消费 | walreader → worker 队列 | **变难** |
| O2 | 流段 framing 序 | wire 上同时最多一个打开的流段；一个流段的字节连续出现，中间不插入任何其它字节 | 段是不可分割的转发单元 | walsender 输出调度器 | **新增约束** |
| O3 | 结束事件序 | commit / abort / cancel / prepare 按 WAL 提交序输出 | cseq + commit barrier | walsender 提交表 | 不变 |

#### 4.1.1 O1：事务内序

**契约**：订阅端看到的同一个事务，其变更顺序与 WAL 顺序逐条一致；事务不被拆分到多个 worker。

**不变式**：

| 编号 | 不变式 | 落实位置 |
| --- | --- | --- |
| INV-1.1 | 一个顶层事务的全部 WAL 记录只发往同一个 worker | 绑定表（4.5.1） |
| INV-1.2 | walreader 追加到某 worker 队列的顺序 = WAL 顺序 | 分发队列是 FIFO（5.1） |
| INV-1.3 | worker 从自己的队列按到达顺序单线程消费 | worker 主循环（10） |
| INV-1.4 | 一个事务在 worker 内只有一份 `ReorderBufferTXN` | 事务→worker 的一对一映射 |

**这四条不变式合起来给出的推论**：worker 侧收到的必须是**原始 WAL 记录**，而不是 walreader 预先消化过的语义事件。理由与展开见 5.2 的推导。

#### 4.1.2 O2：流段 framing 序

**契约**：

| 编号 | 不变式 | 含义 |
| --- | --- | --- |
| INV-2.1 | 一个流段的字节在 wire 上连续出现，段内不插入任何其它字节 | 段是原子转发单元 |
| INV-2.2 | wire 上同时最多一个未闭合的流段 | 第二个 `STREAM_START` 必须等第一个 `STREAM_STOP` |
| INV-2.3 | 段内只含该事务的变更 | 段不跨事务 |
| INV-2.4 | 同一事务的多个段按 `first_segment` 递增顺序出现，`STREAM_COMMIT` 在最后一段之后 | 段序不乱 |

**依据**（事实 13）：`worker.c:1476` 对第二个 `STREAM_START` 报运行时协议错误；`worker.c:566` 用全局 `stream_xid` 路由变更。

**违反后果的分级**：

| 违反 | 订阅端表现 | 严重性 |
| --- | --- | --- |
| INV-2.2（第二个 `STREAM_START`） | `ereport(ERROR, "duplicate STREAM START message")`，会话终止 | 显式失败，可观测 |
| INV-2.1（普通事务的字节插进流段） | 变更被路由到错误的 `stream_xid`，写进错误的事务上下文 | **静默数据损坏** |
| INV-2.1（另一个流段的字节插进流段） | 同上，并进一步触发 INV-2.2 | 静默损坏 + 显式失败 |
| INV-2.4（同一事务的段乱序） | 订阅端把后段当成首段处理，`first_segment` 语义错乱 | 静默损坏 |

**并行化后的破口**：现状下流段由 `ReorderBufferProcessTXN()` 一次调用产出（事实 13），天然连续。拆成 N 个 worker 后，字节由 N 条独立的输出流产生，**必须由 walsender 在合并点显式重建连续性**——worker 侧无法保证，因为它看不到别人的字节。

**关键推论：O2 把发送侧的并发粒度钉死为“一个流段”。** 不同事务的流段不能互相嵌入，所以 wire 上的交错单位是**整个流段**，而不是字节或消息。后果：

- 段越大，wire 阻塞越久，发送侧利用率越低；
- 因此**分片切分粒度不只是内存参数，也是发送侧的交错粒度参数**（见 8.1、11.1）；
- 并行解码提升的是“字节产出”能力，不是“下发”能力；下发仍是段粒度串行。

#### 4.1.3 O3：结束事件序

**契约**：commit / abort / cancel / prepare 按 WAL 提交序输出。

**机制**：walreader 按 WAL 顺序分配 cseq；walsender 维护 `next_commit_seq` 水位，只在水位连续时写出结束事件（见 7.1、7.2）。

**这一层在并行化前后等价**，因为现状实现本来就是“数据可乱序、结束事件有序”（事实 4）。这是本设计与现状语义等价的关键一环。

**但"数据可乱序"有一条边界，必须写进契约，而且这条边界的正确形式与直觉不同。** 协议帧的 LSN 如何影响槽水位，链路是这样的（全部可在源码中核对）：

| 环节 | 源码 | 行为 |
| --- | --- | --- |
| 帧头写 LSN | `walsender.c:1549-1560` `WalSndPrepareWrite()` | **只有 `last_write` 为真时才写入 `ctx->write_location`，否则写 0**。pgoutput 中 `last_write=true` 的只有：BEGIN、每条变更、COMMIT、PREPARE、COMMIT/ROLLBACK PREPARED、STREAM COMMIT；STREAM START / STREAM STOP 与 origin 消息写 0 |
| 订阅端跟踪 | `worker.c:3588-3594` | `last_received = max(last_received, start_lsn, end_lsn)`——**取最大值，不会倒退** |
| 订阅端回报 | `worker.c:3778-3785` `get_flush_position()` | 无待 apply 事务时 `flushpos = writepos = recvpos`，即回报 `last_received` |
| 发布端落槽 | `logical.c:1836-1850`、`:1915` `LogicalConfirmReceivedLocation()` | **无条件**把 `confirmed_flush` 设为回报值，没有"不得超过已发送位置"的上限 |
| 重启跳过判据 | `logical.c:568-591` → `snapbuild.c:358` → `decode.c:1302` → `snapbuild.c:443-446` | `start_decoding_at = confirmed_flush`；**在事务的提交/回滚记录处**用 `ptr < start_decoding_at` 判断是否整体跳过该事务 |

**由此得到真正的安全不变式**（不是"'w' 帧 LSN 不得倒退"）：

> **S1**：任何**提交记录 LSN 小于 `confirmed_flush`** 的事务，必须已经**完整送达**订阅端。
> 因为重启后 `start_decoding_at = confirmed_flush`，提交记录 LSN 更小的事务会被 `DecodeTXNNeedSkip()` **整体跳过**——它的 BEGIN 与全部变更都不会重发。（重读本身是从 `restart_lsn` 开始的，`walsender.c:1506`，所以只要事务被重发，它就是完整的；危险的不是"重发得不完整"，而是"根本不重发"。）

**现状（串行）为什么自动满足 S1**：reorder buffer 按 WAL 顺序处理记录，事务按提交顺序送达。设已完整送达的事务集合为 `D`（送达序 = 提交序），`M` = 已送达帧的最大 LSN。`M` 只可能来自某个已送达事务的变更帧，其 LSN ≤ 该事务的提交记录 LSN ≤ `D` 中最后一个事务的提交记录 LSN；而任何尚未送达的事务，其提交记录必然位于 WAL 中 `M` 之后（否则它早已被 reorder buffer 处理并输出）→ 提交记录 LSN > M → 重启时不会被跳过。

**并行模型下 S1 在哪里被破坏**：本设计让**提交消息**按 cseq 放行（= 提交序），这一半与现状等价 ✔；但**流式分片提前送达**是新的破坏源——分片里变更帧的 LSN 可以大于某个"已提交但因 barrier 尚未放行"的事务的提交记录 LSN。于是 `M` 被抬高，越过那个事务的提交记录 → 重启后该事务被跳过 → **丢变更**。

例：T1 的提交记录 LSN = 200（cseq=1，worker 慢，提交消息尚未放行）；T2（cseq=2）已提前输出一个分片，其中变更帧 LSN = 250。则 `M = 250 > 200`，重启后 `start_decoding_at = 250`，**T1 被整体跳过**。

因此 O3 的实现必须附带一条约束：

> **C9（改写）**：wire 上任何一条携带 LSN 的帧，其 LSN **不得超过"下一个待放行事务（cseq = 放行水位 + 1）的提交记录 LSN"**。
> 非流式事务的整事务字节因此必须跟随 cseq 一起放行；**流式分片必须被钳制**——walsender 在转发分片帧时把帧头两个 int64 改写为 `min(帧内 LSN, 放行前沿 LSN)`（详见 7.4）。

注意这**不是**"'w' 帧 LSN 不得倒退"：现状串行实现本身就产生倒退的帧 LSN（后提交事务的 BEGIN 帧 LSN 可能小于先提交事务的 COMMIT 帧 LSN），订阅端用取最大值的方式正是为此设计的。真正的判据是 S1。

**必须注意的一条**：不是每个事务结束事件都会产生 wire 消息。`ReorderBufferAbort()`（`reorderbuffer.c:2946`）只在 `rbtxn_is_streamed(txn)` 为真时才调用 `rb->stream_abort()`；非流式事务的 abort / cancel **不产生任何 wire 消息**。因此 cseq 水位不能靠“等待 cseq=k 的消息”来推进，必须靠“收到 cseq=k 的结束事件回执（含空消息）”来推进，否则 barrier 会在常规路径上死锁（见 7.2）。

#### 4.1.4 三层的关系与派生约束

| 维度 | O1 事务内序 | O2 流段 framing 序 | O3 结束事件序 |
| --- | --- | --- | --- |
| 作用对象 | 一个事务内部的字节 | 段与段的边界 | 事务结束消息 |
| 影响范围 | 单个事务 | 整个 wire 流 | 事务之间 |
| 保证者 | walreader（路由 / 追加）+ worker（消费） | walsender（输出调度） | walreader（分配）+ walsender（放行） |
| 并行化后的变化 | 需要跨进程保持顺序 | **新增** | 无变化 |

**由三层派生出对进程间通信的硬约束**（实现时逐条核对，5 与 16 章引用）：

| 编号 | 约束 | 派生自 |
| --- | --- | --- |
| C1 | walreader → worker 的载荷是**原始 WAL 记录**，不是语义事件；worker 复用现有的 per-record 处理逻辑 | O1 + 事实 12 |
| C2 | 分发队列每 worker 一条，严格 FIFO，追加顺序 = WAL 顺序 | INV-1.1 / INV-1.2 |
| C3 | 事务结束事件必须携带 cseq 回到 walsender；**无消息的结束事件也必须回执** | O3 + 7.2 |
| C4 | worker → walsender 的输出以**块**为单位，每块带 FIRST / MORE / LAST 边界标记 | O2 |
| C5 | walsender 在块未闭合期间不得取用其它 worker 的字节 | INV-2.1 / INV-2.2 |
| C6 | 快照事件必须与 WAL 记录同序到达 worker | O1（快照状态属于事务内状态） |
| C7 | xid 无效的失效消息必须广播到所有 worker | O1（各 worker 各自持有目录缓存） |
| C8 | walreader 只做门控与路由，不下发 snapbuild 之外的语义翻译结果 | O1（避免重复状态） |
| C9 | 已送达帧的 LSN 不得超过"下一个待放行事务的提交记录 LSN"（不变式 **S1**，见 4.1.3）；因此非流式事务的整事务字节跟随 cseq 放行，**流式分片的帧 LSN 由 walsender 钳制到放行前沿** | O3 + 7.4 |
| C10 | 复制槽的**两个水位**（xmin 与 `restart_lsn`）都必须跨进程聚合，且由 walsender 单点推进 | 6.6 |
| C11 | **受 cseq 门控的输出条目必须自带 cseq**（非流式整事务块、流式事务的 STREAM COMMIT 事件）；否则 walsender 无法把它们挂到 barrier 的正确位置 | O3 + 7.2 |
| C12 | 事务结束事件必须携带**结束记录的 LSN 与载荷**（提交时间戳、prepare 的 gid）；worker 不看提交记录本身，这些字段只能由 walreader 传下去 | O1 + 5.2.2 |

### 4.2 决策一：解码切分点——worker 负责到哪一层

需要决定 decode worker 的职责边界。三种切分点：

| 对比项 | 方案 A：事务级 | 方案 B：变更级 | 方案 C：A + 共享关系表 |
| --- | --- | --- | --- |
| worker 职责 | 从 WAL 记录到协议字节，整事务在 worker 内闭环 | 只做 `heap_decode`，产出 `ReorderBufferChange` | 同 A，但关系描述符由 walreader 统一下发 |
| reorder buffer 位置 | 每个 worker 一份 | walsender 一份 | 每个 worker 一份 |
| output plugin 位置 | 每个 worker 一份 | walsender 一份 | 每个 worker 一份 |
| walsender 负载 | 按帧转发 + 顺序控制（**每帧一次 `write()` 系统调用**，见 12.3 的帧率风险） | 网络发送 + 协议格式化 | 同 A |
| 串行路径上的拷贝 | 共享内存 → socket（1 次） | 共享内存 → reorder buffer → 插件缓冲 → socket（3 次） | 同 A |
| 与现状差异 | reorder buffer 与插件实例化逻辑需按 worker 复制 | reorder buffer/plugin 基本不动 | 关系消息去重 |
| 风险 | 插件状态按 worker 复制；RELATION 消息重复 | **插件格式化留在单线程，热点无法并行化** | 实现复杂度最高 |

**选型：方案 A 为主，方案 C 作为关系消息去重的增强项。**

理由：

1. **CPU 热点在插件格式化，不在拷贝。** 事实 12：output plugin 的回调不是在读 WAL 时发生，而是在变更从 reorder buffer **回放**时发生（提交路径 `reorderbuffer.c:2109`、流式路径 `reorderbuffer.c:4055`），热点是插件对每列的 syscache 查类型与类型输出函数调用（`logical/proto.c:828-858`）。方案 B 只把 tuple 解码搬到 worker，**这段热点仍完整地留在单线程的 walsender 上**，无法达成 70MB/s 目标；方案 A 把“解码 + 插件格式化”整体并行化，walsender 只剩**按帧转发**（每帧一次 `write()` 系统调用，见 12.3），CPU 分布最均衡。
2. **插件状态必须在同一进程内闭环。** `txn->output_plugin_private` 是 output plugin 的私有状态，跨进程无法共享。方案 A 让 begin → changes → commit 全部在同一个 worker 内走完，插件状态天然自洽；方案 B 要么在 worker 与 walsender 之间传递插件私有状态，要么把格式化退回 walsender，两者都不可取。
3. **方案 A 的串行路径拷贝最少**（共享内存 → socket 一次），符合“调度不成为新瓶颈”的要求；方案 B 多出 reorder buffer 与插件缓冲两次往返。
4. 方案 B 的优点（改动小）可以通过“把 reorder buffer 与插件实例化到 worker、walsender 只保留发送职责”来达成，而不必保留 reorder buffer 在 walsender。

方案 A 的已知代价及处理：

- **RELATION 消息重复**（事实 6）：每个 worker 有独立的 `RelationSyncCache`。按 `pgoutput.c:704-770` 的两级判定拆开看，**放大只发生在非流式事务路径**：流式事务按 `(relation, top-xid)` 粒度判断是否重发，而一个 top-xid 只会归属一个 worker，粒度与事务归属一致，**不额外放大**；非流式事务用的是会话级 `schema_sent` 标志，每个 worker 各有一份，因此同一关系最多被发 N 次。订阅端收到重复的 RELATION 会覆盖缓存，语义正确，代价是少量冗余带宽。**首版按方案 A 实现并容忍该冗余，把方案 C 的共享关系表列为后续优化**（注意其收益主要是消除上述 N 倍放大，而非消除流式路径的重复）。
- **插件实例化**：每个 worker 需要独立的 `LogicalDecodingContext` 与 output plugin 实例（`LoadOutputPlugin()` + `_PG_output_plugin_init`），各自持有 `ctx->out` 缓冲。

### 4.3 决策二：提交顺序保证——commit barrier 与 FIFO 队列对比

这是本设计的核心决策。两种候选方案：

**方案 1：commit barrier（提交水位）**

- walreader 按 WAL 顺序为每个事务结束事件（commit / abort / cancel）分配单调递增的提交序号 `cseq`；
- walsender 维护 `next_commit_seq` 水位。收到 `cseq = k` 的结束事件确认时，若 `k != next_commit_seq`，说明前面还有未发出的结束事件，将其挂入有序待提交表；只有当待提交表中 `next_commit_seq` 连续可取时，才依次写出并推进水位；
- **流式分片不受 barrier 约束，随时可发**（分片属于事务内部，见 7.4）；但**非流式事务的整事务字节与其提交消息一起按 cseq 放行**——否则 `'w'` 帧的 LSN 会倒退，`confirmed_flush` 的语义被破坏（见 7.4 末段）。

**方案 2：FIFO 队列（按 WAL 读到的提交顺序排队）**

- walreader 把事务结束事件（连同其已解码的字节块）压入一条单一 FIFO，FIFO 按 cseq 排序；
- walsender 严格按 FIFO 顺序取出发送。

**对比：**

| 维度 | commit barrier | FIFO 队列 |
| --- | --- | --- |
| 顺序保证强度 | 强：commit/abort/cancel 严格按 cseq | 强：且事务数据也严格按 cseq，强度更高 |
| 对“尽早流式”的影响 | **无影响**：流式分片可随时发出，只有提交消息被挂起 | **有影响**：队头事务未解码完成时，队头阻塞，其后已解码的数据全部不能发 |
| 队头阻塞（数据路径） | 无 | 有：长事务会拖住其后的所有事务 |
| 队头阻塞（提交路径） | **有，与现状相同**：cseq = 1 的事务慢，cseq = 2 的提交消息必须等。现状的串行实现本来就有同样的提交路径队头阻塞 | 有 |
| 内存占用 | 小：仅缓存被挂起的结束事件（每条几十字节）+ 已解码待发的数据块 | 大：需缓存乱序完成事务的全部字节，量级与乱序窗口成正比 |
| 乱序容忍度 | 高：解码完成顺序任意 | 低：解码完成顺序必须与提交顺序一致 |
| 吞吐（乱序完成场景） | 高 | 低：整体退化为“提交顺序串行” |
| 非流式订阅方（要求整事务字节连续） | 需额外机制：整事务缓冲后再整块发 | 天然满足 |
| 实现复杂度 | 需维护 cseq 水位 + 有序待提交表 + 空洞检测 | 队列本身保序，逻辑直观 |
| 与 PG 现有语义一致性 | 一致（PG 现状就是“流式分片可乱序、提交有序”） | 更强于 PG 现状，需额外保证订阅端能接受 |

**结论：采用 commit barrier 为主方案。**

1. 事实 4 已经表明 PG 现有的流式实现本身就不保证事务数据有序，只保证提交有序。barrier 精确复刻了这个语义，是“与现状行为等价”的最小变更；FIFO 反而引入了 PG 现状没有的更强约束。
2. 用户需求明确要求“事务 stream 的时候要尽快发送来提升性能”。barrier 完全不阻碍**流式分片**提前下发；FIFO 与该要求直接冲突——只要有一个事务解码慢，FIFO 就会把后面所有已解码的数据一起堵住。
3. 内存开销差距显著。barrier 只需缓存挂起的结束事件；FIFO 需要缓存乱序窗口内所有已解码字节，在高吞吐场景下这个量级不可控。
4. FIFO 的唯一优势（非流式订阅方的整事务连续性）可以用一个退化配置实现：**当订阅方未请求流式时，切换为“整事务缓冲”模式**——此时 worker 不切分分片，walsender 在收到该事务的提交消息时才把整块字节写出。这本质上就是 FIFO 的语义，但只在低并行收益的场景下启用，不影响主路径。

**必须澄清的一点**：事实 8 中"协议 v4 的并行"指的是**订阅端**用 parallel apply worker 回放，**不是**发布端可以在 wire 上同时打开多个流段。发布端在 v4 下仍然是单流段顺序下发（事实 13）。因此"多个在途事务的分片交错下发"并不是既有行为——现状是一个流段在一次函数调用内完整产出（`ReorderBufferProcessTXN()`，`reorderbuffer.c:2109`）。barrier 与流式分片的关系是 4.1.2 的 O2 与 7.4 的规则，不是"多个流段交错"。

### 4.4 决策三：事务与 worker 的绑定关系

绑定关系有两个方向，必须分清：

- **事务 → worker 是多对一**：一个 worker 可以同时承载多个并发事务；不存在“一个 worker 只服务一个事务”的限制，也不需要这样的限制。
- **事务在 worker 之间不可迁移**：一个顶层事务从第一条 WAL 记录到最后一条，全程只由同一个 worker 处理。

| 对比项 | 方案一：worker 生命周期 = 事务生命周期 | 方案二：一个 worker 同时处理多个并发事务 |
| --- | --- | --- |
| 绑定基数 | 一对一：一个 worker 同时只服务一个事务 | 多对一：一个 worker 承载多个在途事务 |
| worker 状态 | 事务结束即释放，worker 空闲 | 长期持有多个在途事务 |
| 负载均衡 | 事务粒度，天然均衡 | 需显式分配算法与再平衡 |
| 关系缓存 | 每事务重建，RELATION 消息重复率高 | 跨事务复用，重复率低 |
| 内存占用 | 与单事务大小成正比 | 与在途事务总数成正比 |
| 提交顺序 | 每个事务的提交由其 worker 产生，仍需 barrier | 同左 |

**选型：方案二（一个 worker 同时处理多个并发事务）。**

理由：方案一每次事务结束就要销毁并重建 worker 进程，进程创建开销与插件实例化开销在 tpcc 那种大量小事务的负载下会成为主要成本；且 worker 频繁重建使关系缓存无法复用，RELATION 消息重复率极高。方案二让 worker 常驻、按事务分工，插件实例与关系缓存长期有效，进程开销摊薄。

方案二带来的设计要求：

- **事务分配策略**：walreader 维护“事务→worker”绑定表与各 worker 的**累计在途字节估计**，分配时选择代价最低的 worker（见 4.5.1 的负载度量）；
- **绑定一旦建立即不可变**：同一顶层事务的全部 WAL 记录始终发往同一 worker，事务内顺序由此不变（详见 4.5）；
- worker 的 reorder buffer 需支持多个并发在途事务，`rb->size` 的内存阈值需要按 worker 数量切分（`logical_decoding_work_mem` 语义从“每解码上下文”变为“每 worker”，见 12.2）；
- 快照：每个事务有自己的 base snapshot（事实 2），worker 需为每个在途事务保存一份快照，事务结束时释放。

### 4.5 事务亲和性与事务内顺序保证

本节回答两个问题：**事务内的顺序靠什么保证？** 一个 worker 上并发多个事务会不会互相干扰？

#### 4.5.1 绑定规则

| 规则 | 说明 |
| --- | --- |
| 绑定时机 | 事务的**第一条** WAL 记录到达时分配 worker；此后不再变更 |
| 绑定粒度 | 顶层事务 xid。子事务、事务型 logical message 跟随其顶层事务 |
| 绑定唯一性 | 一个顶层事务在整个生命周期内只属于一个 worker |
| 解绑时机 | 事务收到 commit / abort / cancel 后解绑，worker 回到空闲池。**例外：prepared 事务在 PREPARE 之后不解绑**，直到 COMMIT PREPARED / ROLLBACK PREPARED（见 7.1） |
| 禁止迁移 | 事务一旦绑定，后续 WAL 记录不因负载变化而改投其他 worker |

禁止迁移的理由：事务的插件状态（`txn->output_plugin_private`）、tuplecid 哈希表、累积的变更、待执行的目录失效消息都保存在 worker 进程内。迁移需要把这些状态跨进程搬迁，代价高于短暂失衡本身。

**负载度量必须按字节，不能按事务数。** walreader 侧的分配代价函数是"**该 worker 上在途变更的总字节数**"，而不是"在途事务的个数"。理由：

- `rb->size` 统计的就是该 worker 上**所有在途变更的总字节数**（`reorderbuffer.c:3245-3260`），也是触发流式/溢出的那个量（`reorderbuffer.c:3659`）。事务数与它没有单调关系。
- tpcc 这类负载里绝大多数事务很小、少数很大。一个绑定了大事务的 worker，若按"事务数最少"判定，其在途事务数可能是 1，反而被判为**最空闲**，walreader 会继续往它身上派新事务 → 它的 `rb->size` 逼近阈值 → 触发 spill 或流式抖动，而其余 N−1 个 worker 空闲。**有效并行度退化为 1。**
- 由于 barrier 是全局按 cseq 排序的（7.2），一个 worker 上"卡着一个早提交的大事务"会把它**后面所有事务的提交一起卡住**，而它们其实在别的 worker 上早已解码完成。**负载不均因此被直接放大成全局提交停顿。**

因此分配规则为：

1. walreader 为每个 worker 维护 `pending_bytes`（该 worker 上在途事务的字节估计，由下发的 WAL 记录累加）与 `pending_txns`；
2. 新事务分配给 `pending_bytes` 最小、且 `pending_txns` 未超上限的 worker；
3. **单 worker 的在途事务数设上限**（防止事务状态——每事务一份快照——无限膨胀，见 6.3）；
4. `pending_bytes` 的估计来自 walreader 下发的 WAL 记录大小，是近似值；worker 侧的实际 `rb->size` 可以在周期性上报中校正（与 6.6.1 的水位上报合并到同一条消息）。

**大事务阻塞的是同 worker 内的事务。** `ReorderBufferProcessTXN()`（`reorderbuffer.c:2109`）是整事务原子的：一个大事务被分片输出时，同一 worker 上的其它事务要么等它输出完，要么等它整事务处理完。因此"大事务不再阻塞其它事务"这个收益**只在跨 worker 之间成立**（见 8.1 的限定）。

#### 4.5.2 事务内顺序的三重保证

事务内顺序由三个环节串联保证，**不需要任何额外的序号机制**：

1. **walreader 侧：单一来源 + 顺序追加。** walreader 是 WAL 的唯一读取者，它按 WAL 顺序读到的记录，依次追加到目标 worker 的 SPSC 队列尾部。SPSC 队列本身保序，因此队列中的记录序 = WAL 序。
2. **worker 侧：单线程顺序消费。** worker 从自己的队列按到达顺序取批次，不并发处理同一条队列。**入队可以交错，产出不得交错**：不同事务的记录在队列中交错到达，但每个事务的变更必须**整事务连续**地进入它自己的 reorder buffer 并被处理完（`ReorderBufferProcessTXN()` 是整事务原子的，`reorderbuffer.c:2109`）。这既是 O1 的要求，也是 O2 的要求——产出交错会破坏 wire 上的块连续性。
3. **事务内状态单线程。** 一个事务在 worker 内只有一份 reorder buffer 状态（`ReorderBufferTXN`），其变更链表、命令 ID、目录失效消息都追加在这一个结构上。

由此得到：**同一事务的 WAL 顺序 == 该事务在 worker 中的变更产出顺序 == 发往订阅端的字节顺序。**

顺序由“绑定 + 顺序追加 + 单线程消费”共同成立；commit barrier（4.3）解决的是**事务之间**的顺序，两者作用在不同层面，互不替代。

#### 4.5.3 与流式切分的交互

一个 worker 上并发多个事务，它们共享同一个 reorder buffer，因此 `rb->size` 统计的是该 worker 上**所有在途事务**的聚合大小（事实 4、事实 5）。这带来两点：

- 流式触发阈值变成“每 worker 聚合”，而非“每事务”。单个 worker 上的内存压力是其上事务大小之和；
- 触发流式后，`ReorderBufferLargestStreamableTopTXN()`（`reorderbuffer.c:3592-3621`）在该 worker 的在途事务中挑选总字节数最大的顶层事务输出。被选中的事务仍然是“该 worker 上的某个事务”，其分片顺序仍由该事务自身的到达顺序决定。

因此事务内顺序在流式路径上同样成立：分片是该事务变更序列的一段段连续切片，不会出现同一事务的分片乱序或交错。

#### 4.5.4 多事务并发下的相互影响

| 影响面 | 说明 | 是否影响事务内顺序 |
| --- | --- | --- |
| reorder buffer 内存 | `rb->size` 聚合统计，阈值按 worker 切分（12.2） | 否 |
| 关系缓存 | 多个事务共用一个 `RelationSyncCache`，RELATION 消息的重复粒度是每 top-xid（事实 6） | 否 |
| 目录失效 | 每个事务各自累积自己的失效消息，事务结束时各自执行（事实 3） | 否 |
| 快照 | 每个事务各自持有一份 base snapshot 的副本（事实 2、6.3） | 否，但**历史快照是进程级单例**，事务之间必须 `Setup`/`Teardown` 切换（6.3） |
| 插件状态 | 每个事务一份 `output_plugin_private`，事务之间互不影响（4.2） | 否 |
| 流式切分 | 事务级分片边界由 worker 聚合内存决定（4.5.3） | 否 |

结论：**并发只发生在事务之间，事务内始终是单线程顺序执行。** 这正是把解码切分点选在事务级（4.2 方案 A）的原因——若切分点在变更级（方案 B），事务内的顺序就会被多个 worker 的并发写入打散。

## 5. 进程间通信

### 5.1 共享内存布局

采用一块由 walsender 创建的 dsa 区域（`dsa_create()`），各进程通过 `dsa_handle` 附着（`dsa_attach()`）。区域划分**由 4.1 的三层顺序契约推导**——每一块区域的存在理由都是“维持某一层顺序所必需”：

| 区域 | 生产者 | 消费者 | 类型 | 维持的顺序 | 说明 |
| --- | --- | --- | --- | --- | --- |
| WAL 分发队列 × N | walreader | worker #i | SPSC 无锁环形队列 | O1 | 每 worker 一条；严格 FIFO，追加顺序 = WAL 顺序（INV-1.2） |
| 快照版本池 | walreader | worker | 共享数组 + 版本水位 | O1 | 快照按版本号引用，避免每事务一次 O(xcnt) 拷贝（6.3） |
| 输出队列 × N | worker #i | walsender | SPSC 无锁环形队列 | O2 + O3 | 每 worker 一条；条目分“数据块”与“结束事件”两类 |
| 控制通道 | walreader ↔ walsender | | SPSC | O3 | cseq 分配、xmin 推进请求、ready / flush 回执 |
| 错误通道 | worker #i | walsender | 共享状态字 + `bgw_notify_pid` 通知 | — | worker 内部错误（FR-11） |

选型说明：PostgreSQL 自带的 `shm_mq`（`src/backend/storage/ipc/shm_mq.c`）实现了“单发送者—单接收者”的共享内存队列，语义正好匹配“walreader → 每个 worker”的扇出模型，且提供了 `shm_mq_sendv()` 批量提交能力。`shm_mq` 的方向是固定的（`shm_mq_set_receiver()` / `shm_mq_set_sender()` 各只能设置一次，`shm_mq.c:206/224`），因此**不存在"双向 `shm_mq`"**。worker → walsender 方向（N 个生产者）需要自建 MPSC 队列，或为每个 worker 建立一条 `shm_mq`（worker → walsender），由 walsender 轮询各队列——后者实现更简单且避免了 MPSC 的缓存行争用。**首版采用“每 worker 两条 `shm_mq` 实例”**：下行（walreader → worker）与上行（worker → walsender）各一条，共 2N 条。

**下行与上行必须是两条独立的队列**，不能合并：

- O1 要求 walreader 不被 worker 的解码速度拖住。否则 WAL 读取位置会被 worker 的背压卡住，`restart_lsn` 的推进随之停滞，复制槽的 WAL 保留窗口被无谓拉长；
- O2/O3 要求 walsender 不被 worker 的产出顺序拖住。walsender 是唯一的网络出口，它必须能自由地在 N 个 worker 之间选择下一个可下发的块。

**共享内存的尺寸上界必须显式约束。** dsa 区域是按需增长的（`dsa_create_ext()` 的 `init_segment_size` / `max_segment_size`，`dsa.c:421`），若不给出上界，一个失控的 worker（或一条大事务）可以把 dsa 撑到把 postmaster 的动态共享内存吃光。因此首版约定：

| 项 | 上界来源 | 说明 |
| --- | --- | --- |
| 单条下行条目 / 单个上行块 | `MaxAllocSize`（`memutils.h:40`，1GB−1） | 单次分配的硬上限，超过即报错终止会话；不是设计目标值，只是"不会把内存吃爆"的兜底 |
| 每个 `shm_mq` 实例的容量 | `shm_mq_set_max_size()` | 队列满即触发背压（5.5），而不是无限增长 |
| dsa 区域总容量 | GUC 上限（建议 `parallel_decode_shmem_size`） | 超过即拒绝启动新事务并告警 |

**单条消息的尺寸另有协议约束**：libpq 的 `PQ_LARGE_MESSAGE_LIMIT` 就是 `MaxAllocSize - 1`（`libpq.h:31`），超过该值必须走大消息分片协议——这与本设计的 `PD_BLK_*` 分块语义天然一致（5.2.3 的 FIRST / MORE / LAST 就是分片协议），因此单块字节数上限必须显著低于 `PQ_LARGE_MESSAGE_LIMIT`。

### 5.2 消息定义

#### 5.2.1 推导：下行载荷为什么必须是原始 WAL 记录

O1 的四条不变式（4.1.1）要求 worker 能**原样重建**事务内的全部状态。现状中这些状态**全部由 WAL 记录本身承载**：

| 事务内状态 | 现状的产生方式 | 记录里已有的信息 |
| --- | --- | --- |
| 子事务归属（`txn->subtxns`） | `LogicalDecodingProcessRecord()` 对每条带 xid 的记录调用 `ReorderBufferAssignChild()`（`decode.c:99-111`） | 记录自身的 xid + 记录自带的 top xid |
| tuplecid 映射（`txn->tuplecids`） | `SnapBuildProcessNewCid()`（`snapbuild.c:828-861`）读 `xl_heap_new_cid` | `top_xid / target_locator / target_tid / cmin / cmax / combocid` |
| 命令 ID（`curcid`） | 同上，`ReorderBufferAddNewCommandId()` | `cmin` / `cmax` |
| 目录失效消息 | `ReorderBufferAddInvalidations()`（`reorderbuffer.c:3329-3386`）读 `xl_xact_invals` | 失效消息数组 |
| 变更本身 | `heap_decode` / `heap2_decode` | 记录头 + 主数据 + **block 数据** |

如果 walreader 先把这些“翻译”成语义消息再下发，就要额外维护三套**重复状态**（subxid 列表、curcid 同步、tuplecid 双向映射），且每一套都要与 WAL 位置严格对齐——这正是 O1 最容易出错的地方，而且每套重复状态都是一个新的一致性风险来源。

**结论（C1、C8）**：下行载荷是**原始 WAL 记录 + 门控结果**；walreader 只做「门控」与「路由」两件事，不做语义翻译；worker 复用现有的 per-record 处理逻辑（`ReorderBufferAssignChild` / `ReorderBufferAddNewTupleCids` / `ReorderBufferAddNewCommandId` / `ReorderBufferAddInvalidations` / `heap_decode` / `heap2_decode`），语义与现状逐条等价。

一个有利的事实：PG 17 的 WAL 记录格式里 **top xid 是记录自带的字段**（`XLR_BLOCK_ID_TOPLEVEL_XID`，`xlogreader.c:1732-1735`），不依赖 reader 侧的 xid 跟踪。因此“按记录转发 top xid”是天然可行的，worker 复刻 `ReorderBufferAssignChild()` 的输入与现状完全一致。

#### 5.2.2 下行条目（walreader → worker）

| 条目 | 何时产生 | 载荷 |
| --- | --- | --- |
| `PD_ENT_WAL` | 一批属于该 worker 的事务的 WAL 记录 | 记录数 + 每条记录（见下） |
| `PD_ENT_SNAPSHOT` | 事务首条记录之前 / 目录事务提交时 | 快照版本号 + 随附的失效消息 |
| `PD_ENT_GLOBAL_INVAL` | `XLOG_XACT_INVALIDATIONS` 且 xid 无效 | 失效消息数组（广播到所有 worker，见 6.5） |
| `PD_ENT_TXN_END` | 读到事务结束记录 / 检测到 cancel | xid、结束类型、**cseq**、**结束记录的 LSN 与载荷**（见下） |

**`PD_ENT_TXN_END` 必须携带结束记录本身的数据。** 事务结束记录（COMMIT / ABORT / PREPARE / COMMIT PREPARED）按 5.2.2 的筛选规则**不进入 `PD_ENT_WAL`**，worker 永远看不到它们。而 worker 侧的收尾工作需要这些字段：

| 字段 | 用途 | 现状来源 |
| --- | --- | --- |
| `commit_lsn`（记录起点） | `txn->final_lsn` | `decode.c` 的 `DecodeCommit()` 传 `buf->origptr` |
| `commit_end_lsn`（记录终点） | `txn->end_lsn`；**COMMIT 消息的 `'w'` 帧 LSN** | 同上，`buf->endptr` |
| `commit_time` | `logicalrep_write_commit()` 写进 COMMIT 消息的时间戳 | `xl_xact_commit.xact_time` |
| `gid` | prepare 的事务名，`logicalrep_write_commit_prepared()` 用它 | `xl_xact_prepare.twophase_gid` |
| `end_type` | commit / abort / prepare / commit-prepared / rollback-prepared / cancel | — |

现状这些值都在 worker 进程内从 WAL 记录直接解析；拆分后 walreader 读到结束记录，**只能由它解析并随事件下发**。

`PD_ENT_WAL` 中每条记录必须携带 worker 重放该记录所需的**全部**信息：

| 字段 | 说明 |
| --- | --- |
| `rmgr_id` / `info` | 记录类型，决定走哪个 rm_decode |
| `lsn` | 记录位置 |
| `xid` | 记录自身的 xid（子事务记录非 0） |
| `top_xid` | 顶层 xid；worker 用它复刻 `ReorderBufferAssignChild()` |
| `origin` | `RepOriginId`；`FilterByOrigin()` 需要（`decode.c:918`） |
| `main_data` / `main_data_len` | 记录主数据（如 `xl_heap_insert`） |
| `nblocks` + 每块 `{block_id, rlocator, forknum, blkno, data_len, data}` | **block 数据不可省**：tuple 数据位于 block 0 的 block image 中（`decode.c:913` `XLogRecGetBlockTag(r, 0, ...)`、`decode.c:930` `XLogRecGetBlockData(r, 0, &datalen)`；UPDATE/DELETE 另有一块） |

**门控**：walreader 在转发前对每条数据类记录执行 snapbuild 门控（`SnapBuildProcessChange()`，`snapbuild.c:778-820`）。未通过门控的记录**不转发**——与现状 `decode.c:420/427` 的 `if (SnapBuildProcessChange(...)) DecodeInsert(...)` 完全等价。门控还决定了事务的首条记录位置，walreader 在此之前追加一条 `PD_ENT_SNAPSHOT`（base）条目。

**记录载荷的两种实现路径**：

| 路径 | 做法 | 优点 | 代价 |
| --- | --- | --- | --- |
| (a) WAL 磁盘格式 | walreader 拷贝整条记录的原始字节，worker 用 `DecodeXLogRecord()`（`xlogreader.c:1662`，已在 `xlogreader.h:398` 导出）还原 | 复用 PG 自己的解码器，字段集天然完备 | PG 17 的 reader 用**循环解码缓冲就地解码**（`xlogreader.c:440-470`），解码后原始字节已被 `DecodedXLogRecord` 覆盖，walreader 拿不到原始字节；且 worker 需重做记录头解析与 CRC 校验 |
| (b) 并行解码自有格式 | 按 5.2.2 的字段集序列化**已解码的结构**；worker 侧反序列化后直接合成 `DecodedXLogRecord` | 紧凑；worker 不必持有完整 reader；**记录头解析与 CRC 校验只在 walreader 做一次** | 需在 worker 侧实现合成代码 |

**取 (b)**，且目标形态是**直接合成 `DecodedXLogRecord`**（而不是重新组装成 WAL 磁盘字节再调 `DecodeXLogRecord()`）。理由：

- `DecodedXLogRecord`（`xlogreader.h:156-173`）本身就是 worker 需要的最终形态：`header`（`XLogRecord`）、`record_origin`、`toplevel_xid`、`main_data` / `main_data_len`、`max_block_id`、`blocks[]`（`DecodedBkpBlock`）。
- rm_decode 侧取块数据走的是 `XLogRecGetBlockData()`（`xlogreader.c:2025-2047`），它只访问 `record->record->blocks[block_id]`（`record` 是 `DecodedXLogRecord *`，`xlogreader.h:236`）与 `record->record->max_block_id`；取块标签走 `XLogRecGetBlockTagExtended()`（同文件 `:1996-2018`），同样只访问 `blocks[]`。**两者都不需要 WAL 磁盘字节。**
- CRC 校验（`xlogreader.c:1195-1206`）在 walreader 侧完成一次即可，worker 不必重做——重做等于把每条记录的解码成本翻倍。

因此 worker 侧只需合成一个 `DecodedXLogRecord`（含 `blocks[]` 的柔性数组）并挂在一个最小 `XLogReaderState` 上（`record` 指向它，`ReadRecPtr` 等字段填当前记录的 LSN）。这样 rm_decode 函数拿到的 `XLogReaderState` 与现状同构，`heap_decode` / `heap2_decode` **一行都不用改**。

**记录筛选规则**：walreader 转发的是“现状会喂给 `LogicalDecodingProcessRecord()` 且带有效 top xid 的全部记录”，即：

- `XLOG_HEAP_INSERT` / `XLOG_HEAP_UPDATE` / `XLOG_HEAP_HOT_UPDATE` / `XLOG_HEAP_DELETE` / `XLOG_HEAP_TRUNCATE` / `XLOG_HEAP_CONFIRM`；
- `XLOG_HEAP_INPLACE`（只为传递 `rbtxn_has_catalog_changes`，见 2.3）；
- `XLOG_HEAP2_MULTI_INSERT` / `XLOG_HEAP2_NEW_CID`；
- `XLOG_XACT_ASSIGNMENT` / `XLOG_XACT_INVALIDATIONS`；
- 事务型 `XLOG_LOGICAL_MESSAGE`。

**不包括**事务结束记录（COMMIT / ABORT / PREPARE / COMMIT PREPARED），它们由 `PD_ENT_TXN_END` 取代；**不包括**无 xid 的记录（`standby` / `xlog`），它们由 walreader 自行处理；**不包括**逻辑解码不关心的物理记录（`XLOG_HEAP_LOCK` / `XLOG_HEAP2_REWRITE` / `XLOG_HEAP2_VISIBLE` / `XLOG_HEAP2_PRUNE_*` / `XLOG_HEAP2_LOCK_UPDATED`，见 2.3）。

**`XLOG_HEAP2_NEW_CID` 的额外要求**：`SnapBuildProcessNewCid()`（`snapbuild.c:828-861`）除了投递 tuplecid，还调用 `ReorderBufferXidSetCatalogChanges()` 设置 `rbtxn_has_catalog_changes` 标志——该标志是 `ReorderBufferBuildTupleCidHash()` 建表的必要条件（`reorderbuffer.c:1761-1766`）。因此这条记录下发时，worker 必须在本地事务上**同时置位该标志**（见 6.3）。

注意 `SnapBuildProcessNewCid()` 本身是 **snapbuild 的函数**，而它操作的三个目标（`ReorderBufferXidSetCatalogChanges` / `ReorderBufferAddNewTupleCids` / `ReorderBufferAddNewCommandId`）**全在 reorder buffer 上**——拆分后 reorder buffer 在 worker 侧。因此 worker 侧的 snapbuild 替身必须**再提供第三个函数** `SnapBuildProcessNewCid()`（见 6.1）。`XLOG_HEAP_INPLACE` 同理：它的 `ReorderBufferXidSetCatalogChanges()` 也落在 worker 的 reorder buffer 上。

#### 5.2.3 上行条目（worker → walsender）

上行只有两类语义：**数据块**（O2）与**结束事件**（O3）。

| 条目 | 载荷 | 说明 |
| --- | --- | --- |
| `PD_ENT_BLOCK` | 块边界标记（FIRST / MORE / LAST）、块类型、xid、**cseq（仅整事务块有）**、字节 | O2 的载体：块内字节在 wire 上必须连续 |
| `PD_ENT_EVENT` | cseq、结束类型、字节（**可为空**）、xid | O3 的载体：事务结束事件 |
| `PD_ENT_ERROR` | 错误信息 | worker 内部错误（FR-11） |

**块类型**有两种，对应订阅端的两条独立约束：

| 块类型 | 内容 | 连续性约束来源 |
| --- | --- | --- |
| `PD_BLK_SEG` | 一个流段的字节：`STREAM_START` … `STREAM_STOP` | O2 / INV-2.1、INV-2.2（事实 13） |
| `PD_BLK_TXN` | 一个**非流式**事务的完整字节：`BEGIN` … 变更 … `COMMIT` | 订阅端的 `in_remote_transaction` 状态（`worker.c:1006` / `:1035`）：`BEGIN` 与 `COMMIT` 之间不得插入别的字节，否则 `remote_final_lsn` 被覆盖，后续 `COMMIT` 会触发 `ereport(ERROR, "incorrect commit LSN ...")` |

两者都必须**整块连续**出现在 wire 上，因此统一用 FIRST / MORE / LAST 边界标记表达，walsender 的规则完全一致：**一次取一整块，块未取完不得取其它 worker 的块**（C5）。

`PD_ENT_EVENT` 的字节**可为空**：非流式事务的 abort / cancel 不产生任何 wire 消息（`reorderbuffer.c:2946`），但 cseq 水位仍必须推进——因此空事件也要回执（C3，见 7.2）。

**结束事件与数据块共用同一条上行队列**：walsender 读到事件条目时，若 barrier 未放行，则把它**挂起**在按 cseq 排序的待提交表里并继续从同一条队列取下一条。这样既保证“同一事务的所有块都先于它的结束事件下发”（同一队列 FIFO），又不会让 barrier 阻塞数据路径。若把结束事件放在独立队列，就无法保证这个先后关系。

**块与 cseq 的两种组合**：

| 事务形态 | 数据 | 结束事件 |
| --- | --- | --- |
| 被流式（分段下发） | 若干 `PD_BLK_SEG`，解码过程中陆续产出，**不带 cseq**——此时事务尚未提交，cseq 还不存在 | `PD_ENT_EVENT`，携带 cseq，载荷是 `STREAM_COMMIT` / `STREAM_ABORT` 字节；另有一条 `END_ACK` |
| 未被流式（整事务下发） | 一个 `PD_BLK_TXN`，在 `PD_ENT_TXN_END` 到达时一次性产出，**携带该 cseq** | 同样有 `END_ACK`（字节可为空） |

**只有携带 cseq 的条目受 barrier 门控**：

| 条目 | 是否受 cseq 门控 | 说明 |
| --- | --- | --- |
| `PD_BLK_SEG` | 否 | 流段内部连续即可，可以随时下发；C9 只约束它携带的帧 LSN（见 7.4） |
| `PD_BLK_TXN` | **是** | 块内的 `COMMIT` 必须与更早的 cseq 保持相对顺序 |
| `PD_ENT_EVENT`（STREAM_COMMIT / STREAM_ABORT） | **是** | 同上 |
| `PD_ENT_EVENT{END_ACK}` | **是** | 它本身就是 barrier 的推进凭据 |

因此 `PD_BLK_TXN` 必须**自带 cseq**（C11）。walsender 不能靠 `PMSG_COMMIT_SEQ` 来关联它——那是 walreader → walsender 的另一条通道，与 worker 的输出队列之间没有先后关系，两条通道的消息到达顺序不可比。

第二种形态下，`PD_BLK_TXN` 整体要等自己的 cseq 被放行才能下发。它自带 cseq，walsender 直接把它挂进按 cseq 排序的挂起表——与 7.2 的 `pending_commits` 是同一张表，不需要额外的关联机制。

#### 5.2.4 控制类消息

**walreader → walsender**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_COMMIT_SEQ` | xid、cseq、结束类型 | 提交顺序的来源（7.1）；把 cseq 与 xid 关联起来 |
| `PMSG_WATERMARK_ADVANCE` | lsn、新的 xmin、新的 restart_lsn | 复制槽的两个水位推进请求（6.6） |
| `PMSG_READY` | — | snapbuild 达到 consistent |

**walsender → walreader**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_WATERMARK_ACK` | lsn、xmin、restart_lsn | walsender 代为调用 `LogicalIncreaseXminForSlot()` / `LogicalIncreaseRestartDecodingForSlot()` 后回执 |
| `PMSG_FLUSH_LSN` | confirmed_flush | 复制槽确认水位回传 |

**worker → walreader**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PD_ENT_EVENT{ END_ACK }` | cseq | 事务结束事件已处理完毕（7.2） |
| `PD_ENT_EVENT{ WATERMARK }` | xmin、oldest_first_lsn、seq | 周期上报本 worker 的快照 xmin（6.6.1） |

**walreader → worker（状态同步）**

worker 不持有完整的 snapbuild，但它需要 snapbuild 派生出的少量状态（6.1）。这些状态放在**共享内存的状态块**里，由 walreader 独占写、worker 只读，不需要逐条消息传递：

```c
typedef struct PD_DECODE_STATE
{
    int         snapbuild_state;    /* SNAPBUILD_* 状态位 */
    XLogRecPtr  start_decoding_at;  /* builder->start_decoding_at */
    uint64      update_seq;         /* 递增序号，供 worker 检测更新 */
} PD_DECODE_STATE;
```

状态块是**单向、无锁**的：walreader 写、worker 读，中间用一个序号保证 worker 能读到一致的快照（写序号 → 写字段 → 写序号）。worker 不需要向 walreader 回传任何状态。

#### 5.2.5 一个必须显式处理的边界情形

**非事务型 logical message**（`decode.c:644-651`，`XLOG_LOGICAL_MESSAGE` 且 transactional=false）没有 xid，无法按事务路由，但插件实例在 worker 侧，必须转发给某个 worker。本设计规定转发给**当前在途字节最少的 worker**，并接受一个已知后果：**这类消息与事务数据的相对顺序在并行化后不再确定**（现状下它是按 WAL 顺序精确插入的）。由于它本身不携带事务语义、订阅端也不要求它与任何事务相对有序，这一后果可以接受，但需在文档与测试中显式记录。


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

**但这条拷贝链必须把队列本身的 memcpy 算进去。** `shm_mq` 的载荷不是零拷贝的：`shm_mq_send_bytes()`（`shm_mq.c:1043`）用 `memcpy` 把数据拷进环形缓冲；`shm_mq_sendv()` 的 iovec 只是"源端 gather"，不是 scatter/gather 零拷贝。因此实际链路是：

```text
WAL 页缓冲 --memcpy--> decode_buffer --memcpy--> shm_mq 环（memcpy）
                --memcpy--> tuple --memcpy--> ctx->out --memcpy--> shm_mq 环（memcpy）
                --memcpy--> socket
```

即**每条记录在下行多一次 memcpy，每条协议消息在上行多一次 memcpy**。这些 memcpy 发生在 worker 的并行段内（下行那次在 walreader 侧），不落在串行关键路径上，但计入"每条记录的总拷贝次数"。

在此基础上，有一处拷贝是可以真正消除的，另有一处需要修正目标形态：

1. **输入侧：WAL 记录直接解码进共享内存。** 本分支的 `XLogReaderState` 已改为基于循环解码缓冲（`xlogreader.c:446-452`，默认 64KB），并提供 `XLogReaderSetDecodeBuffer()`（`xlogreader.c:90`）允许调用方替换该缓冲。若把 walreader 的解码缓冲放在共享内存里，WAL 记录就**直接解码进共享内存**，消除“decode_buffer → 共享内存”这一次。需要注意循环缓冲会被复用，因此必须双缓冲 + 流控，保证 worker 消费完之前 walreader 不会覆盖该区域。
2. **输出侧：`ctx->out` → 共享内存结果区（需要修正目标形态）。** `ctx->out` 是 `LogicalDecodingContext` 上的一个 `StringInfo`（`logical.c:292`），插件通过 `pq_send*` 直接往里写。**它不能直接指向共享内存结果区**：`StringInfo` 的增长走 `enlargeStringInfo()`（`stringinfo.c:288-352`），内部是 `repalloc`（`:349`）——**缓冲区地址会变**，而共享内存结果区的地址是固定的。`WalSndPrepareWrite()` 每条消息还会 `resetStringInfo(ctx->out)`（`walsender.c:1553`），语义是"一条 plugin 消息一个缓冲"。

   因此正确的做法是：**worker 侧保持 `ctx->out` 为私有 `StringInfo`（插件零改动），在 `OutputPluginWrite` 回调里把 `ctx->out` 的内容 memcpy 进共享内存结果区**。这消除的是"先写共享内存缓冲、再整体拷贝"这一层冗余，而不是"完全消除拷贝"。这与现有机制契合：`prepare_write` / `write` 是 `LogicalDecodingContext` 上的可插拔函数指针（`logical.h:64-66`），walsender 侧实现为 `WalSndPrepareWrite` / `WalSndWriteData`（`walsender.c:1549/1576`），worker 侧改为写入共享内存即可。

不可消除的拷贝：页缓冲 → decode_buffer（reader 内部）、decode_buffer → tuple（`DecodeXLogTuple`，`decode.c:1253-1256`）、tuple → `ctx->out`（插件格式化）、共享内存 → socket（socket 写入）。

两点补充：

- **WAL 记录的跨进程传递必须拷贝**：`XLogReadRecord()` 返回的记录指向 reader 私有的解码缓冲，其生命周期契约是“仅在下一次 `XLogReadRecord()` 之前有效”（`xlogreader.c:385-387`），下一次读取会复用该区域（`XLogReleasePreviousRecord`，`xlogreader.c:249-309`）。因此 walreader 必须在读取下一条记录前把当前记录拷入共享内存，或采用上面第 2 条的双缓冲方案。
- **拷贝的不只是主数据，还有 block 数据**：tuple 数据位于 block 0 的 block image 中（`decode.c:930`），UPDATE/DELETE 各有两块。因此下行条目必须整体拷贝“记录头 + 主数据 + 各 block 的数据”，拷贝量与 WAL 记录的实际大小成正比，而不是与主数据大小成正比。
- **PG 17 的循环解码缓冲使“直接拷贝原始字节”不可行**：`XLogReadRecordAlloc()`（`xlogreader.c:440-470`）把解码结果**就地**放在循环缓冲里，`DecodedXLogRecord` 覆盖了记录头区域。因此 walreader 能拿到的是**已解码的结构**（`DecodedXLogRecord`：header / toplevel_xid / record_origin / main_data / blocks[]），而不是磁盘上的原始字节。下行条目按 5.2.2 的字段集从 `DecodedXLogRecord` 序列化，worker 侧再组装回标准形态（5.2.2）。
- **输出侧不需要额外拷贝**：worker 把插件产出的字节按 FIRST / MORE / LAST 边界切成块片段直接写入共享内存输出队列，walsender 取出后原样写入 socket。整块连续性由“walsender 一次只取一整块”保证，而不是靠拷贝成一个连续大块。
- **本分支已有现成的跨进程序列化格式可借鉴**：`ReorderBufferSerializeChange()` / `ReorderBufferRestoreChange()`（`reorderbuffer.c:3805-4018` / `4400-4567`）实现了变更（含 tuple 载荷、消息、失效消息、快照、truncate）的完整序列化与还原，用于 spill 到磁盘。它是“把解码结果跨进程搬运”这件事的现成模板，方案 B 以及 worker → walsender 的结果传递都可以直接参照其格式。

### 5.4 调度与唤醒

调度本身不能成为瓶颈，因此约定：

1. **定向唤醒**：walreader 只唤醒目标 worker 的 latch（`SetLatch()`），绝不广播；worker 处理完一批后自行挂起（`WaitLatch()`），无空转。
2. **无锁队列**：SPSC 环形队列用原子变量做头尾同步，避免互斥锁竞争与缓存行争用；队列满时 walreader 阻塞等待（背压，见 5.5）。
3. **批量 + 合并唤醒**：连续多条属于同一 worker 的记录合并成一批，一次唤醒；worker 一次取走全部可用批次。
4. **避免信号风暴**：walreader 的每个批次只做一次 `SetLatch()`，即使队列中已有多个批次待取。
5. **锁粒度**：共享内存元数据（事务绑定表、cseq 水位）用 `LWLock` 或原子操作保护，持有时间极短，不在持锁状态下做 WAL 读取或解码。
6. **块粒度的输出调度（O2 的执行点，C5）**：walsender 一次只取**一整块**（读到该块的 LAST 片段为止），在整块取完之前不得从其它 worker 取字节。这条规则是 O2 在 walsender 侧的唯一执行机制，必须实现为“取块”这一原子动作，而不是“取片段后再判断”。
7. **预取**：写当前块的同时，可以从另一个 worker 预取下一块到本地缓冲，但**不得提前写出**——提前写出就破坏了块连续性。预取用于掩盖“切换 worker”的空转。
8. **walsender 的等待模型要覆盖两个事件源**：socket 可写与共享内存有新数据。现有 `WalSndWaitForWal()`（`walsender.c:1054-1069`）用 `WaitLatchOrSocket()` 只等 socket；并行后 worker 的产出必须能唤醒它。注意**跨进程 `SetLatch()` 的实现是发信号**（`latch.c:695`，向 owner_pid 发 SIGURG；本分支的 `Latch` 结构没有信号量/futex 语义，`latch.h:112-121`），不是纯原子操作——每次定向唤醒的实际成本是一次 `kill()` 系统调用 + 一次信号处理（上下文切换），比一次原子操作贵一到两个数量级。因此第 3、4 条的“批量 + 合并唤醒”在这里是硬要求，不是优化项。

   **具体机制取"`WaitEventSet` 同时挂 socket 与 latch"**：`WaitLatchOrSocket()`（`latch.c:565-614`）本身就是 `WaitEventSet` 的包装——它建一个临时 `WaitEventSet`，把 `WL_LATCH_SET` 与 socket 事件一起加进去，等完就 `FreeWaitEventSet()`。但它是**一次性**的（每次调用都新建 + 释放一个 set），在高频循环里开销可观。因此并行模式的 walsender 应持有一个**长生命周期的 `WaitEventSet`**（`CreateWaitEventSet()`），一次性加入：

   | 事件 | 用途 |
   | --- | --- |
   | `WL_SOCKET_READABLE` | 订阅端的 feedback / keepalive 应答 |
   | `WL_SOCKET_WRITABLE` | TCP 窗口打开，可以继续写协议字节 |
   | `WL_LATCH_SET`（`MyLatch`） | worker 产出就绪、walreader 的控制消息就绪 |
   | `WL_EXIT_ON_PM_DEATH` | postmaster 死亡时立刻退出 |

   等待用 `WaitEventSetWait(set, timeout, &event, 1, wait_event_info)`，返回后再按 `event.events` 分派。**首版取超时值 100ms**（与 `NAPTIME_PER_CYCLE` 同量级）：正常路径由 latch 唤醒立刻返回；超时只用于"没有待写数据、也没有新产出"的空转轮询，此时必须有代价可接受——100ms 的空转轮询不影响吞吐，只影响复制延迟的下界。**注意 timeout 只是兜底**：只要 worker 侧保证"有产出就 `SetLatch()`"（第 3、4 条），正常延迟不依赖这个超时值。

   备选方案（"socket 超时 + 信号打断"）不取：`WaitLatchOrSocket()` 本来就把 socket 与 latch 放在同一个 `WaitEventSet` 里等，跨进程 `SetLatch()` 的信号会被 `WaitEventSet` 的等待机制正确接收，拆成两套机制只会多一次系统调用与一次竞争窗口。

9. **`shm_mq` 的唤醒门槛必须显式选择。** `shm_mq` 只在 `force_flush` 为真、或累计写入量达到环的 1/4 时才 `SetLatch()` 接收方（`shm_mq.c:536-541`）；反过来，接收方**每读一次就反向唤醒发送方**（`shm_mq.c:1296`）——信号总数是单向的两倍。因此唤醒策略必须显式声明，不能依赖 `shm_mq` 的默认行为：

   | 策略 | 做法 | 代价 |
   | --- | --- | --- |
   | 每批 force_flush | `shm_mq_sendv(..., force_flush = true)`，保证"每批一次唤醒" | 唤醒及时，但**失去批量合并的意义**（合并批次就不该 force_flush） |
   | 延迟唤醒 | 依赖 1/4 环的默认门槛 | 唤醒次数少，但延迟可达 1/4 环的字节量 |

   首版取**按字节阈值合并批次 + 批次满即 force_flush** 的折中：批次大小由 `parallel_decode_batch_size` 控制，批满时强制刷新并唤醒，批未满时允许合并后续批次。这样唤醒次数上界为"字节数 / 批大小"，而不是"事务数"。另需注意：`shm_mq` 没有单独的 notify API（不存在 `shm_mq_notify`），通知语义只能由 `force_flush` 参数表达。

10. **信号速率必须有预算。** 按 tpcc 100 万 tpm（约 16.7k 事务/秒）与每事务一批估算，跨进程 `kill()` 的速率为 1.3 万～6.7 万次/秒。这是"小事务场景劣化"风险（need.md 风险表）的定量来源，必须纳入监控（见 11.3 的唤醒速率指标）。

### 5.5 流控与背压

- 队列积压超过阈值时，walreader 停止向该 worker 分配新事务，并优先把在途事务分配给空闲 worker；
- 队列接近满时，walreader 阻塞等待（不丢数据、不无界缓冲）；
- worker 产出速度慢于 walreader 读取速度时，天然通过队列长度把压力反传到 walreader，避免内存无界增长；
- 订阅端 TCP 窗口耗尽时，walsender 的 socket 写入阻塞，压力通过结果区反传到 worker——这是最有效的背压，无需额外机制。
- **块粒度的背压（O2 的副作用）**：walsender 卡在一个未闭合的块上时（持有该块的 worker 迟迟不产出 LAST 片段），其它 worker 的产出无处可去——因为 O2 不允许交织。因此单个慢 worker 会阻塞整条 wire。这是 O2 的固有代价，只能靠“段不能太大”来限制单块时长（见 8.1 的分片粒度讨论）。

## 6. 快照与系统表变更

### 6.1 快照的构建

- snapbuild 由 walreader 独占。walreader 读取 `XLOG_RUNNING_XACTS` 推进快照状态机（`decode.c:372` → `SnapBuildProcessRunningXacts()`，`snapbuild.c:1266`），达到 consistent 后向 walsender 汇报就绪；
- 快照的 `xmin` 取自 `running->oldestRunningXid`（`snapbuild.c:1297`）；`xip` 数组并非来自 running-xacts 的 `xids[]`，而是 snapbuild 累积的**已提交目录修改事务**集合（`snapbuild.c:959-980` `SnapBuildAddCommittedTxn()`）；`xmax` 只在目录修改事务提交时推进（`snapbuild.c:1184-1190`）。这套语义由 snapbuild 内部维护，拆分进程后不变。

**worker 不需要完整的 snapbuild，只需要它派生出的少量状态。** 这是三进程拆分能否成立的关键前提：现状下 `ctx->snapshot_builder` 与 `ctx->reorder` 是**同一个进程的两个对象**，且 `builder->reorder == ctx->reorder`（`logical.c:213-214`）。拆分后这个等式断了——snapbuild 在 walreader、reorder buffer 在 worker——因此 worker 侧的解码路径不能再依赖 `ctx->snapshot_builder` 的完整语义。

把现有代码对 snapbuild 的依赖逐条拆开：

| 依赖点 | 源码 | 实际需要什么 | worker 侧如何获得 |
| --- | --- | --- | --- |
| `SnapBuildCurrentState(builder) < SNAPBUILD_FULL_SNAPSHOT` | `decode.c:475` | 一个**状态位** | 共享内存中的状态块（walreader 每批更新） |
| `SnapBuildProcessChange(builder, xid, lsn)` | `decode.c:477` | 判定"该事务是否已有 base snapshot"，没有就分配 | walreader 完成分配并下发 `PD_ENT_SNAPSHOT`；worker 只需回答"本事务是否已有快照" |
| `SnapBuildXactNeedsSkip(builder, lsn)` | `reorderbuffer.c:4037` | `ptr < builder->start_decoding_at`（`snapbuild.c:443-446`），**一个 LSN 比较** | 共享状态块里的 `start_decoding_at` |
| `ReorderBufferCanStartStreaming()` | `reorderbuffer.c:4028-4047` | 上述状态 + `ctx->streaming` + `ctx->reader->ReadRecPtr` | 状态块 + `ctx->streaming`（worker 侧常量）+ 本地记录 LSN |
| `SnapBuildProcessNewCid(builder, xid, lsn, xlrec)` | `snapbuild.c:828-861` | 三个 **reorder buffer** 调用：`ReorderBufferXidSetCatalogChanges` / `ReorderBufferAddNewTupleCids` / `ReorderBufferAddNewCommandId` | worker 侧替身直接调这三个 reorder buffer 函数（它们本来就在 worker 侧） |
| `ReorderBufferSetRestartPoint(rb, ptr)` | `snapbuild.c:1887`、`:2051` | 把 `rb->current_restart_decoding_lsn` 设为 `ptr` | **不跨进程**——由 walreader 维护自己的本地镜像（见下） |

**`ReorderBufferSetRestartPoint()` 是唯一一条"写 reorder buffer"的 snapbuild 依赖，必须单独处理。** 现状下它写的是 `builder->reorder->current_restart_decoding_lsn`（`reorderbuffer.c:688` 读它），而 reorder buffer 在 worker 侧。拆分后：

- walreader **自己持有一份 `current_restart_decoding_lsn` 的本地镜像**，在 snapbuild 推进时同步更新（值完全来自 snapbuild 的调用点，不需要跨进程传递）；
- walreader 在**创建事务绑定**时把当时的镜像值记为该事务的 `restart_decoding_lsn`（语义与 `reorderbuffer.c:688` 的 `txn->restart_decoding_lsn = rb->current_restart_decoding_lsn` 逐条等价）；
- worker 侧的本地 reorder buffer 上的 `current_restart_decoding_lsn` **不再被使用**——它只参与 6.6.2 的槽水位聚合，而该聚合完全在 walreader 侧完成。

这样就不需要一条"walreader → worker"的 restart point 消息，也不需要 worker 回传任何状态。

因此 worker 侧的 `ctx->snapshot_builder` 替换为一个**只读替身**：

```c
/* 共享内存中的解码状态块，由 walreader 独占写、worker 只读 */
typedef struct PD_DECODE_STATE
{
    int         snapbuild_state;    /* SNAPBUILD_* 状态位 */
    XLogRecPtr  start_decoding_at;  /* builder->start_decoding_at */
    uint64      update_seq;         /* 递增序号，供 worker 检测更新 */
} PD_DECODE_STATE;
```

worker 侧的替身只需提供三个函数：

- `SnapBuildCurrentState()` —— 直接读状态块；
- `SnapBuildProcessChange()` —— 等价行为是"本事务已收到 `PD_ENT_SNAPSHOT` 就返回 true，否则返回 false"。**它不再写 reorder buffer、不再构建快照。**
- `SnapBuildProcessNewCid()` —— 直接转调 worker 侧的 `ReorderBufferXidSetCatalogChanges()` / `ReorderBufferAddNewTupleCids()` / `ReorderBufferAddNewCommandId()`（这三个函数的操作对象全在 worker 的 reorder buffer 上，不需要 snapbuild 的任何状态）。

这样"解析语义零变更"（第 10 章）仍然成立——`heap_decode` / `heap2_decode` 的改动仅限于把 `ctx->snapshot_builder` 指向替身；`SnapBuildProcessChange` 的**判定结果**与现状一致，因为现状的判定本来就等价于"状态够 + 本事务有快照"。

**base snapshot 的分配权归 walreader。** 现状 `SnapBuildProcessChange()` 在第一次遇到某事务时执行 `SnapBuildSetBaseSnapshot(builder->reorder, xid, lsn, builder->snapshot)`——把快照设到**持有该事务的 reorder buffer**上。拆分后该事务在 worker 的 reorder buffer 里，因此这一步必须由 walreader 提前做完并下发。walreader 的下发规则是（5.2.2 的门控规则的延伸）：

1. walreader 在**门控通过**该事务的第一条数据记录时（即现状 `SnapBuildProcessChange()` 返回 true 的时刻），确定该事务的 base snapshot；
2. base snapshot 的**内容**由 snapbuild 给出：`builder->snapshot` 若为 NULL 则 `SnapBuildBuildSnapshot()` 构建一次，随后在整个目录修改纪元内复用（现状即是此语义，引用计数由 `SnapBuildSnapIncRefcount()` 管理）；
3. walreader 把快照内容写入**快照版本池**的一个槽位并分配版本号，通过 `PD_ENT_SNAPSHOT` 把版本号下发给 worker（必须**先于**该事务的第一条 WAL 记录到达）；
4. worker 收到后取用该版本作为本事务的 base snapshot，并在回放时刻自行调用 `ReorderBufferCopySnap()` 生成含 `subxip` 的最终快照（见 6.3）。

**注意这一步与 §6.2 的快照切换是同一套机制**（同一个版本池），只是触发点不同：一个是"新事务的 base snapshot"，一个是"目录变更后的快照切换"。

### 6.2 快照的事件流（必须按序传递）

现状中一个事务在解码期间会收到三类**插入到变更流中的内部事件**（`reorderbuffer.c` 的 `REORDER_BUFFER_CHANGE_INTERNAL_*`）：

| 事件 | 产生位置 | 语义 | 并行设计中的来源 |
| --- | --- | --- | --- |
| `INTERNAL_SNAPSHOT` | `ReorderBufferAddSnapshot()`（`reorderbuffer.c:3144`），由 `SnapBuildDistributeSnapshotAndInval()`（`snapbuild.c:870-953`）在**目录修改事务提交**时，向所有已有 base snapshot 的在途事务投递 | “从此处起改用新快照”——事务进行中途发生的 DDL 由此对在途事务可见 | `PD_ENT_SNAPSHOT`（快照版本号） |
| `INTERNAL_COMMAND_ID` | `ReorderBufferAddNewCommandId()`（`reorderbuffer.c:3193`），由 `SnapBuildProcessNewCid()`（`snapbuild.c:828`）在解析 `XLOG_HEAP2_NEW_CID` 时投递 | 推进 `snapshot->curcid` | **worker 从 `XLOG_HEAP2_NEW_CID` 记录自行重建** |
| `INVALIDATION` | `ReorderBufferAddInvalidations()`（`reorderbuffer.c:3329`），由 `decode.c:297` 在解析 `XLOG_XACT_INVALIDATIONS` 时投递 | 在变更流中的**特定位置**执行目录失效 | **worker 从 `XLOG_XACT_INVALIDATIONS` 记录自行重建** |

关键点：**这三类事件在现状中是与数据变更混在同一条变更流里的**，其位置有语义。`reorderbuffer.c:2422-2480` 按顺序处理它们：先执行失效（`:2424`），再切换快照（`:2428`），再推进 curcid（`:2460-2480`）。

由此得到两条设计要求：

1. **除快照外，事件不再单独下发。** 命令 ID、失效消息、tuplecid 全部由 worker 从原始 WAL 记录按现状逻辑重建（5.2.1）。这既避免了重复状态（C1），也保证了它们与数据变更的相对位置天然正确——它们就在同一条记录流里。
2. **快照仍然必须单独下发。** 快照是 walreader 独有的状态（snapbuild 只在 walreader 侧推进），worker 无法自行推导。

因此 walreader → worker 的传递是**与 WAL 变更同序的混合流**：

1. walreader 只在两处插入 `PD_ENT_SNAPSHOT`：事务的首条记录之前（base 快照），以及目录修改事务提交时（快照切换）；
2. `PD_ENT_SNAPSHOT` 同时携带随附的失效消息——对应 `SnapBuildDistributeSnapshotAndInval()` 中 `ReorderBufferAddInvalidations()` 那一半（`snapbuild.c:938-950`）：目录事务的失效消息要投递给**其它**在途事务，并在变更流的同一位置执行；
3. worker 按顺序处理这些事件：切换快照时重新拷贝快照并重装（`reorderbuffer.c:2428-2458`），推进 curcid 时以新 curcid 重新拷贝快照（`:2460-2480`）；
4. 事务结束时执行累积的失效（`reorderbuffer.c:2577`），保证自己的 catcache/syscache/relcache 不残留脏条目。

**下发范围必须与现状一致**。现状 `SnapBuildDistributeSnapshotAndInval()` 遍历 `builder->reorder->toplevel_by_lsn`，只向**已有 base snapshot** 的在途事务投递，并跳过 prepared 事务（`snapbuild.c:908/917`）。拆分后 walreader 用自己的事务绑定表替代该遍历，语义必须逐条一致：**只向“已绑定、尚未结束、已获得 base snapshot”的事务所属 worker 投递快照切换**；worker 收到广播后，对本机上**除 prepared 事务以外**的全部在途事务应用新快照（prepared 事务不应看到新的目录内容，与 `snapbuild.c:917` 的注释一致）。

**快照的传递方式**：共享内存中维护一个**快照版本池**（若干槽位，每个槽位一个 `Snapshot` 的内容 + 版本号 + 引用计数）。walreader 生成新快照时分配一个新版本号并写入一个槽位；条目里只带版本号，worker 按版本号取用并自行拷贝（6.3）。这避免了“每事务一次 O(xcnt) 的快照拷贝”（`xcnt` = 已提交的目录修改事务数，DDL 密集负载下可达数百）。

### 6.3 worker 侧使用历史快照

现状中解码前会把快照**进程级**安装为历史快照：`reorderbuffer.c:2127` 的 `SetupHistoricSnapshot(snapshot_now, txn->tuplecid_hash)`，此后该进程内所有目录读取都走历史视图（`snapmgr.c:216-228` 让 `GetTransactionSnapshot()` 返回 `HistoricSnapshot`）。worker 必须复刻这一点：

- worker 为每个在途事务保存：base snapshot 的副本、该事务的 tuplecid 哈希表、当前 `curcid`；
- 解码该事务的任一变更前，先 `SetupHistoricSnapshot()`；
- 快照切换事件到达时 `TeardownHistoricSnapshot()` 后重装新快照。

**拷贝必须发生在 worker 侧，而不是由 walreader 传递拷贝结果。** `ReorderBufferCopySnap()`（`reorderbuffer.c:1833-1886`）在 base snapshot 基础上填入 `subxip`（该事务的 xid 及其全部子事务 xid）并设置 `curcid`。它有两个决定性约束：

1. **它需要 `txn->subtxns`**——子事务链表只有持有事务的 reorder buffer 才有。子事务是通过后续的 `XACT_ASSIGNMENT` 记录确定的，在事务的第一条 WAL 记录到达时**还不知道**。因此在事务开始时做的拷贝必然缺少 `subxip`。
2. **它是 `static` 函数**（`reorderbuffer.c:269`），未导出到 `reorderbuffer.h`；其全部调用点（`reorderbuffer.c:2051/2436/2448/2470/4110/4132`）都在 worker 侧的“回放时刻”。

缺少 `subxip` 时，“事务自身未提交的目录行”对该事务不可见，会导致自建表并插入的表被漏掉。因此分工是：

| 角色 | 职责 |
| --- | --- |
| walreader | 提供 **base snapshot 的内容**（`xmin`/`xmax`/`xip`/`curcid`），通过 `PD_ENT_SNAPSHOT` 下发 |
| worker | 保存 base snapshot；在**回放时刻**（子事务已由 WAL 记录齐备）自行调用 `ReorderBufferCopySnap()` 生成含 `subxip` 的最终快照 |

这与现状代码完全一致——现状的 `ReorderBufferCopySnap()` 本来就是在回放时才调用的，worker 化不需要新增任何拷贝逻辑，只是把“拷贝的时机”从进程内直接调用变成“收到 base snapshot 后在回放时调用”。

**历史快照是进程级单例，必须按事务切换。** `HistoricSnapshot` 与 `tuplecid_data` 都是 `snapmgr.c:90-91` 的进程级静态变量，不是事务级的。worker 同时承载 K 个在途事务时，同一时刻只能有一份生效。因此 worker 必须以**事务为单位串行解码**：进入事务 A 前 `SetupHistoricSnapshot(A 的快照, A 的 tuplecid_hash)`，离开时 `TeardownHistoricSnapshot()`，再进入事务 B。绝不能两个事务的变更交错解码——否则 `ResolveCminCmaxDuringDecoding()`（`heapam_visibility.c:1703-1704`）会取到错误的 `tuplecid_data`，UPDATE/DELETE 的旧版本判定出错。

**tuplecid 映射不可省略**：事务自身创建目录行时，其可见性由 `cmin < curcid` 判定，依赖 `txn->tuplecid_hash`。该哈希表由 `ReorderBufferBuildTupleCidHash()`（`reorderbuffer.c:1761-1826`）从 `XLOG_HEAP2_NEW_CID` 记录构建，而 tuplecid 本身**不随事务落盘**：它们挂在 `txn->tuplecids` 这条独立的链表上（`reorderbuffer.c:3311`），不在 `txn->changes` 里，而 spill 只遍历 `txn->changes`（`reorderbuffer.c:3735`），因此永远不会被序列化；`ReorderBufferChangeMemoryUpdate()` 还刻意不把它们计入内存账（`reorderbuffer.c:3235-3236`），所以它们也不会触发 spill。结论是：**tuplecid 只能以内存态存在于 worker 的事务对象上**，必须以数据形式下发给 worker，不能靠 worker 重新读 WAL 获得（见 5.2.1）。

**`rbtxn_has_catalog_changes` 标志必须一并传递。** `ReorderBufferBuildTupleCidHash()` 的第一道判据是 `if (!rbtxn_has_catalog_changes(txn) || dlist_is_empty(&txn->tuplecids)) return;`（`reorderbuffer.c:1761-1766`）。该标志由 `ReorderBufferXidSetCatalogChanges()` 设置，而设置动作发生在 walreader 侧（`snapbuild.c:839`，在 `SnapBuildProcessNewCid()` 内）。若 worker 侧事务上没有这个标志，哈希表根本不会建立，`SetupHistoricSnapshot()` 传入的 `tuplecid_hash` 就是 NULL，UPDATE/DELETE 的旧版本判定随之出错。因此 `XLOG_HEAP2_NEW_CID` 的下发必须同时携带“本事务有目录变更”的语义，worker 收到后在本地事务上置位（见 5.2.1 的 `PD_ENT_WAL`）。

### 6.4 relid 解析

`reorderbuffer.c:2227-2228` 用 `RelidByRelfilenumber(spcOid, relNumber)` 把 WAL 里的 relfilenumber 映射为 relid。该映射**没有历史视图**（`reorderbuffer.c:2230-2241` 的注释），失败时对普通变更是硬错误。因此：

- relid 解析在 worker 内完成，与现状一致（现状同样在 `ReorderBufferProcessTXN()` 内解析）；
- 前提是各 worker 对 `pg_class` 的视图一致。由于解析发生在已安装的历史快照下、且历史快照不提供 relfilenumber→relid 的历史映射，各 worker 实际使用的是当前映射——这在多 worker 下与单进程等价，但依赖 6.2 的失效事件流在各 worker 侧一致生效。

### 6.5 无事务的失效

`XLOG_XACT_INVALIDATIONS` 中若 xid 无效，现状是立即执行（`decode.c:304-307` 的 `ReorderBufferImmediateInvalidation()` → `ReorderBufferExecuteInvalidations()`，`reorderbuffer.c:1606-1610`）。这类失效影响的是**全局目录状态**而非某个事务。

并行模型下，**失效必须广播给所有 worker**，不能只在 walreader 本地执行。原因是每个 worker 有自己的 catcache / syscache / relcache（进程私有，`syscache.c:86` 的静态 `SysCache[]`、`relcache.c:134` 的静态 `RelationIdCache`），它们各自缓存了目录条目；walreader 本地执行失效只能清掉 walreader 自己的缓存，worker 的缓存会继续返回**已失效的目录元数据**。后果是 DDL 之后 worker 用旧元数据解码，变更内容错误（AC-5 失败）。

因此：

| 动作 | 执行者 | 机制 |
| --- | --- | --- |
| 执行失效消息 | walreader（本地）+ **所有 worker** | `PD_ENT_GLOBAL_INVAL` 广播 |
| 送达的时点 | 立即，且与该 WAL 记录的位置对应 | walreader 读到记录后立即广播，不等事务结束 |

**广播的正确性依赖 WAL 位置，因此”广播”必须实现为”向每条下行队列各追加一个条目”，不能是独立的旁路通道。** 失效消息必须在对应 WAL 记录的位置被应用，否则 worker 可能在失效生效前就用旧元数据解码了后续变更。

具体做法：walreader 在读到该记录时，向 **N 条下行队列各追加一个 `PD_ENT_GLOBAL_INVAL` 条目**。因为 walreader 对每条队列都是**按 WAL 顺序追加**（INV-1.2），而这些追加都发生在同一时刻，所以对任意一个 worker 而言：

- 它队列中已有的条目，其 WAL 位置都在该失效记录**之前** → 先于失效生效；
- 它队列中后来追加的条目，其 WAL 位置都在该失效记录**之后** → 后于失效生效。

位置正确性因此由队列的 FIFO 追加顺序天然保证，与 5.2.1 选择”下发原始 WAL 记录、worker 自行重建失效”的理由一致——失效的**内容**由 worker 从记录重建，失效的**位置**由队列顺序保证。

代价是每条无 xid 的失效要写 N 份。这是可接受的：无 xid 的失效来自目录修改事务，频率远低于数据变更。

**同一机制也用于快照切换**（6.2）：`PD_ENT_SNAPSHOT` 的”目录修改事务提交时”那一路，同样是向每条下行队列追加一个条目，从而落在正确的 WAL 位置上。

`XLOG_INVALIDATIONS`（standby 记录，`storage/ipc/standby.c:1479`）逻辑解码直接忽略（`decode.c:388-394`），并行设计保持不变。

### 6.6 槽水位的跨进程聚合

复制槽上有两个必须由跨进程数据算出���水位。二者都源于**事务分散在多个 worker** 这一变化，都必须显式设计聚合路径。

#### 6.6.1 xmin 水位

`reorderBufferGetOldestXmin()`（`reorderbuffer.c:1047-1061`）取 `rb->txns_by_base_snapshot_lsn` 的表头作为最老事务的快照 xmin，`snapbuild.c:1311-1316` 用它调用 `LogicalIncreaseXminForSlot()`。事务分散后，walreader 的 reorder buffer 里没有事务，该值退化为 `InvalidTransactionId`，回退到 `running->oldestRunningXid`（`snapbuild.c:1313`）——比真实需要**更老**，因此安全但代价大：只要有任意长事务存在，槽的 `catalog_xmin` 就长期不前进，系统表无法回收。

**真实值与兜底值必须区分，不能混为一谈：**

| 情形 | 采用的 xmin | 理由 |
| --- | --- | --- |
| 所有 worker 均已上报且未超时 | `min(各 worker 上报值)` | 精确，槽可正常推进 |
| 任一 worker 超时未上报 | `running->oldestRunningXid` | 保守，宁可停滞也不能提前回收目录 |

上报周期与超时阈值必须相互配合：`XLOG_RUNNING_XACTS` 的周期约为 `wal_writer_delay`（默认 10s），因此超时阈值必须大于它，否则会周期性地退到保守值。

#### 6.6.2 restart_lsn 水位

`restart_lsn` 的语义是“必须从这里开始重读，才能重放所有尚未提交的事务”（`logical.c:1757-1759`）。它由**最老在途事务**的 `restart_decoding_lsn` 钉住：

```c
/* snapbuild.c:1337-1354 (SnapBuildAdjustXmin) */
txn = ReorderBufferGetOldestTXN(builder->reorder);        /* reorderbuffer.c:1021-1031 */
if (txn != NULL && txn->restart_decoding_lsn != InvalidXLogRecPtr)
    LogicalIncreaseRestartDecodingForSlot(lsn, txn->restart_decoding_lsn);
else if (txn == NULL && ...)
    LogicalIncreaseRestartDecodingForSlot(lsn, builder->last_serialized_snapshot);
```

其中 `txn->restart_decoding_lsn = rb->current_restart_decoding_lsn`（`reorderbuffer.c:688`），而 `current_restart_decoding_lsn` 由快照构建器经 `ReorderBufferSetRestartPoint()` 写入（调用点 `snapbuild.c:1887`、`snapbuild.c:2051`）。

**并行模型下这里有一个必须堵住的漏洞。** `ReorderBufferGetOldestTXN()` 取 `rb->toplevel_by_lsn` 的队头；事务全在 worker 的 reorder buffer 里 → walreader 的 `toplevel_by_lsn` 恒空 → 该函数恒返回 NULL → **必然走 else 分支**，把 `restart_lsn` 推到 `builder->last_serialized_snapshot`。而后者每次 `XLOG_RUNNING_XACTS` 都会前移（`snapbuild.c:1283` → `SnapBuildSerialize` → `snapbuild.c:1882`），大约每 10 秒一次。**于是 `restart_lsn` 会周期性地跳过所有仍在 worker 里在途的事务**——发布端重启后，这些事务的前半段变更永久丢失。

**修法：把 restart_lsn 的聚合点也放到 walreader 侧。** walreader 是在途事务的**创建者**（事务的第一条记录到达时绑定 worker），因此它在创建事务时就能记录 `first_lsn` 与当时的 `current_restart_decoding_lsn`——这正是 `reorderbuffer.c:688` 的语义。于是：

| 聚合点 | 数据来源 | 汇总者 | 请求推进者 |
| --- | --- | --- | --- |
| xmin | 各 worker 的 base snapshot xmin | walreader（`PD_ENT_EVENT` 周期性上报） | walsender → `LogicalIncreaseXminForSlot()` |
| restart_lsn | walreader 自己的事务绑定表中 `first_lsn` 最小的事务的 `restart_decoding_lsn` | walreader | walsender → `LogicalIncreaseRestartDecodingForSlot()` |

`restart_lsn` 的汇总**不需要 worker 上报**——它所需的两个值 walreader 在创建事务时就都有了。worker 侧的 `txn->restart_decoding_lsn` 是本地状态，不参与槽的推进。

**推进判据必须一并修正。** `LogicalIncreaseRestartDecodingForSlot()` 的生效条件是 `current_lsn <= slot->data.confirmed_flush`（`logical.c:1783-1790`）。这个条件假设“`confirmed_flush` 之前的数据都已发完”，在并行模型下不再成立（见 7.4 关于 `'w'` 帧 LSN 的讨论）：事务 T 的数据可能还在 worker 里没解码完，订阅端却已经把 `confirmed_flush` 推过了 T 的位置。因此判据必须增加一条：**所有 worker 上 LSN 小于 `current_lsn` 的事务都已提交并输出完毕**。

#### 6.6.3 汇总消息

两个水位共用一条周期性上报消息（见 5.2.4 的 `PMSG_WATERMARK_ADVANCE`）：

```c
typedef struct PD_WATERMARK
{
    uint32      worker_id;
    TransactionId xmin;          /* 该 worker 上最老在途事务的快照 xmin */
    XLogRecPtr  oldest_first_lsn;/* 该 worker 上最老在途事务的 first_lsn（诊断用） */
    uint64      seq;             /* 上报序号，用于检测丢失 */
} PD_WATERMARK;
```

walreader 收到全部 worker 的上报后取 `min(xmin)`，连同自己算出的 `restart_lsn`，通过控制通道请求 walsender 推进槽。**walsender 是唯一推进槽的进程**（事实 9）。

## 7. 提交顺序保证的实现

### 7.1 提交序号（cseq）的产生

walreader 是 WAL 的唯一读取者，因此 WAL 顺序即提交顺序。walreader 在读到事务结束记录时分配 cseq：

| 事件 | 分配时机 | cseq | 是否产生 wire 消息 |
| --- | --- | --- | --- |
| COMMIT | 读到 `XLOG_XACT_COMMIT` 时 | 分配 | 是（`COMMIT` 消息） |
| ABORT | 读到 `XLOG_XACT_ABORT` 时 | 分配 | **取决于是否流式**：流式事务发 `STREAM ABORT`，非流式事务不发任何消息 |
| PREPARE | 读到 `XLOG_XACT_PREPARE` 时 | 分配 | 是（`PREPARE` 消息） |
| COMMIT PREPARED / ROLLBACK PREPARED | 读到对应记录时 | 分配 | 是 |
| CANCEL | 由 `XLOG_RUNNING_XACTS` 清理游离事务时 | 分配 | **取决于是否流式**：流式事务发 `STREAM ABORT`，非流式事务不发任何消息（现状 `reorderbuffer.c:2946` 的 `ReorderBufferAbort()` 只在 `rbtxn_is_streamed(txn)` 时才调 `rb->stream_abort()`） |

**CANCEL 的收尾与其它结束事件不同，必须在 worker 侧分叉。** 事务被取消时，worker 侧的本地 `ReorderBufferTXN` 仍在（它从未提交），收尾动作是 `ReorderBufferAbort()`：

- **流式事务**：必须先发 `STREAM ABORT`（`reorderbuffer.c:2946` 的 `rb->stream_abort()`），否则订阅端会一直认为有一个打开的流段；
- **非流式事务**：不发任何 wire 消息，barrier 靠 `END_ACK` 推进（见 7.2）。

两种情况的 `END_ACK` 都必须发出。

cseq 单调递增且连续分配（每个事务结束事件占一个序号），因此“空洞检测”退化为简单的“水位 + 计数”，无需复杂的乱位图。

**cseq 分配与结束事件的处理是两件事。** walreader 分配 cseq 后，必须把结束事件（`PD_ENT_TXN_END`）路由给**持有该事务的 worker**——只有 worker 侧的本地 reorder buffer 持有该事务，才能执行收尾工作（结算流式分片、发出 commit/abort 消息、执行累积的失效、释放快照）。walreader 自己没有事务对象，不做这些事。

**PREPARE 不是事务结束。** `txn->gid` 在 PREPARE 时写入（`ReorderBufferPrepare()`，`reorderbuffer.c:2822`），`COMMIT PREPARED` 时用它写消息（`pgoutput.c:673-682` 的 `logicalrep_write_commit_prepared`）。PREPARE 与 COMMIT PREPARED 在 WAL 上可能相隔任意长时间。因此：

- **prepared 事务在 PREPARE 之后仍然保持 worker 绑定与本地 `ReorderBufferTXN`（含 `gid`），直到 COMMIT PREPARED / ROLLBACK PREPARED。** 现状佐证：`ReorderBufferReplay()`（`reorderbuffer.c:2723`）对 prepared 事务**不**调用 `ReorderBufferCleanupTXN()`，事务一直留在 reorder buffer 里。
- 因此 PREPARE 分配一个 cseq（用于保持消息顺序），但**不解绑事务**；COMMIT PREPARED 再分配一个 cseq。两者都要进 barrier。
- 代价：prepared 事务在 worker 中的驻留时间**无上界**，§12.2 的内存预算必须把它算进去（它持有快照副本与累积的变更）。

### 7.2 commit barrier 的实现

walsender 侧维护：

```c
next_commit_seq     /* 下一个允许输出的 cseq */
pending_commits[]   /* 按 cseq 排序的挂起结束事件表 */
```

**barrier 的推进依据是"结束事件被确认"，而不是"提交消息到达"。** 这是本设计中最容易被写错的一处：

> 非流式事务的 ABORT、游离事务的 CANCEL 在 wire 上**不产生任何消息**（见 7.1 的表格）。若 barrier 只在收到提交消息时推进，cseq 就会**永久空洞**，barrier 卡死。

因此结束事件的处理链是三步：

1. walreader 分配 cseq = k，把 `PD_ENT_TXN_END{xid, cseq, type}` 路由给持有该事务的 worker；
2. worker 在本地 reorder buffer 上完成该事务的收尾，把产生的协议字节（commit / abort / stream commit，可能为空）写入输出队列，并向 walsender 发送 `PD_ENT_EVENT{ type = END_ACK, cseq = k }`；
3. walsender 收到 `END_ACK(cseq = k)`：
   - 若 `k == next_commit_seq`：写出该事务的字节块（若为空则什么都不写），`next_commit_seq++`，随后连续检查 `pending_commits` 中是否有 `next_commit_seq`，有则一并写出并推进水位；
   - 否则：把 `END_ACK(k)` 挂入 `pending_commits`（按 cseq 插入有序位置）。

**END_ACK 是必需的，即使事务没有产生任何字节。** 这就是 4.1.3 中"barrier 必须靠 ack 前进、不能靠消息前进"的落地形式。

**cseq 的分配与 barrier 的配合**：cseq 由 walreader 分配，因此 walreader 天然知道事务的提交顺序；barrier 只是把这个顺序传递给 walsender 执行。两者解耦：walreader 不需要等任何 worker 就能分配 cseq。

**死锁风险的排查**：barrier 等待的最坏情形是"cseq = k 的事务迟迟不结束"。可能的原因有：(a) 该事务的 worker 死了——由 §9 的 worker 死亡检测覆盖，barrier 等待必须带超时（FR-15）；(b) 该事务的 WAL 记录还在 walreader 的下游队列里排队——由队列背压解决；(c) walreader 分配了 cseq 却没能把结束事件投递出去（如 worker 队列满）——投递失败必须视为**会话级错误**，不能重试等待，否则 barrier 永久卡死。

**挂起的量级**：挂起的是事务的**可用性**，不是字节数。非流式事务的整事务字节（`PD_BLK_TXN`）必须**整块**留在输出队列里直到它的 cseq 轮到——这意味着挂起区可能持有大量字节。若待提交区与 worker 的结果区共享容量，会形成死锁（worker 想写自己的事务字节，但空间被挂起区占满；而挂起区要等 cseq，cseq 又等那个 worker）。因此**待提交区必须与结果区分离**，或者让 cseq 队头事务的字节直通 socket（绕过挂起区）。这一条在非流式订阅方路径上是硬约束（见 8.2）。

### 7.3 FIFO 方案的实现（备选/退化路径）

若采用 FIFO：walreader 把事务结束事件按 cseq 压入一条 FIFO，walsender 顺序消费。实现简单但存在队头阻塞与内存膨胀问题（见 4.3 对比表），仅在非流式订阅方的“整事务缓冲”退化路径中隐式使用。

### 7.4 顺序保证的边界

必须明确“顺序”到底保证什么，避免实现时过度约束：

| 对象 | 是否要求保序 | 依据 |
| --- | --- | --- |
| commit / abort / cancel | **是**，严格按 cseq | 订阅端按到达顺序 apply 提交 |
| prepare / commit prepared | **是**，与 commit 同一序列 | 两阶段提交的状态机要求 |
| 事务内的变更顺序 | **是**，由事务与 worker 的绑定 + 顺序追加 + 单 worker 单线程消费保证（4.5.2） | 变更间的语义依赖 |
| 同一事务的字节在 wire 上连续 | **是**（流式：每个流段内部连续；非流式：整事务连续） | 订阅端按全局 `stream_xid` 归属变更（4.1.2 的 O2） |
| 不同事务的变更顺序 | **否** | 事实 4：PG 现状已允许乱序下发 |
| 流式分片与提交消息的相对位置 | 分片可先于提交，提交不可先于本事务的全部分片 | 协议要求 |

**”数据可乱序”有一个必须明确的边界，形式与直觉不同：不是”'w' 帧 LSN 不得倒退”，而是 4.1.3 给出的不变式 S1。**

要点回顾（推导见 4.1.3 的表）：帧头 LSN 只在 `last_write=true` 时才是真值（BEGIN、每条变更、COMMIT、PREPARE、STREAM COMMIT；STREAM START/STOP 写 0）；订阅端用**取最大值**跟踪 `last_received`；发布端把该值**无条件**写进 `confirmed_flush`；而重启时 `start_decoding_at = confirmed_flush`，**提交记录 LSN 小于它的事务会被整体跳过**。

于是安全条件是：

> **S1**：任何**提交记录 LSN < `confirmed_flush`** 的事务，必须已经**完整送达**。

串行现状自动满足它，因为事务按提交序送达、且 reorder buffer 按 WAL 序处理记录（任何变更帧的 LSN 都不会越过尚未输出的更早提交记录）。

并行模型下破坏 S1 的是**提前送达的流式分片**：分片里变更帧的 LSN 可以大于某个”已提交但因 barrier 尚未放行”的事务的提交记录 LSN，把 `confirmed_flush` 抬到那个提交记录之上，重启后该事务被跳过。

实现上有三条路：

| 方案 | 做法 | 代价 |
| --- | --- | --- |
| (a) 按 LSN 序发数据 | walsender 持有一个”下一个可发 LSN”的水位，按 LSN 排序后逐个事务整块发出 | 等价于 FIFO，放弃 4.3 的核心卖点（数据随时可发） |
| (b) 数据跟随 cseq 放行 | 非流式事务的数据与其提交消息一起按 cseq 输出 | 吞吐受最慢事务拖累，但语义最安全 |
| (c) 分片帧 LSN 钳制 | 分片照常早发，但 walsender 把分片帧头的两个 int64 改写为 `min(帧内 LSN, 放行前沿 LSN)` | 需要 walsender 改写 worker 产出的帧；订阅端 `last_received` 推进变慢，`confirmed_flush` 推进变慢，WAL 保留窗口变长 |

本设计取 **(b) + (c)**：

- **(b) 覆盖非流式事务**：它们的整事务字节（含 COMMIT）必须等自己的 cseq 被放行，因此其帧 LSN ≤ 自己的提交记录 LSN ≤ 放行前沿 ✔。
- **(c) 覆盖流式分片**：分片不属于任何已放行事务，必须显式钳制。**放行前沿**定义为”cseq = 放行水位 + 1 的那个事务的提交记录 LSN”——它由 walreader 在分配 cseq 时得知并通过控制通道告知 walsender。在该事务尚未提交（cseq 尚未分配）时，前沿取”当前 WAL 读取位置”，同样安全：此时不存在”已提交但未放行”的事务。

**”放行前沿”还有一个必须注意的边界**：它约束的是**已放行事务的提交记录 LSN**，而不是所有事务。若 walsender 已经放行到 cseq = k，则前沿 = 事务 k+1 的提交记录 LSN；任何已送达帧的 LSN 都不得越过它。这条约束由 walsender 单点执行——它是唯一同时知道”放行水位”与”正在转发的帧”的地方。

对流式事务，事务的最后一个分片与它的 `STREAM COMMIT` 之间隔着一个 cseq 屏障，会略微推迟提交可见性；这是可接受的代价，现状的串行实现本就有同样的提交路径队头阻塞。

另外，§12.1 中 G1 的度量口径必须相应调整：`confirm_flush_lsn` 速率在乱序模型下会虚高，应改为**实际发出的协议字节速率**或**订阅端确认的应用速率**。

## 8. 流式发送

### 8.1 尽早流式

用户需求明确指出“事务 stream 的时候要尽快发送来提升性能”。**在 3.5 确定了目标部署形态之后，这条需求的地位变了：它不再是可选的优化，而是让订阅端的并行 apply 真正能被喂饱的前提**（事实 8b）。先把现状机制说清楚：

- 触发条件是 reorder buffer 的**聚合**内存：`reorderbuffer.c:3659` 判断 `rb->size >= logical_decoding_work_mem * 1024`，其中 `rb->size` 是所有在途变更的总字节数（`reorderbuffer.c:3253-3259`），不是单事务大小；
- 触发后由 `ReorderBufferLargestStreamableTopTXN()`（`reorderbuffer.c:3592-3621`）挑选**总字节数最大**的顶层事务来输出；
- 一个分片 = 该事务自上一分片以来累积的**全部**变更，**没有固定字节上限**（事实 5）；输出后由 `ReorderBufferTruncateTXN()` 回收内存，阈值被复用；
- **正常负载下这段逻辑几乎不执行**：内存不超限就不流式，订阅端也就拿不到可并行的流式事务。

**现状已经有一个"立即流式"的开关，只是它是调试选项。** `debug_logical_replication_streaming`（`guc_tables.c:5126-5136`，`PGC_USERSET`，默认 `buffered`）有 `buffered` / `immediate` 两档（`guc_tables.c:429-433`）；`immediate` 下 `reorderbuffer.c:3659-3661` 的循环条件变为 `rb->size > 0`，即**每个有变更的事务都立即被流式化**。本设计的流式激进程度直接沿用这两档的语义。

并行解码为尽早流式提供了三个新的条件，**但其中只有两条真正成立**：

1. **大事务不再阻塞其他事务的解码**（成立，但需限定范围）：现状下最大事务被优先选中输出，其余事务要等内存压力缓解；多 worker 下每个 worker 有独立的 reorder buffer 与内存压力，一个大事务不再拖住其他 worker 的分片产出。**但同一 worker 内的其它事务仍被它阻塞**——`ReorderBufferProcessTXN()` 是整事务原子的（4.5.4），且分配准则若只看"在途事务数"，一个独占大事务的 worker 反而会被判为"最空闲"（见 4.5.1）。
2. **分片触发更频繁**（成立）：N 个 worker 各自持有独立的内存预算，单个 worker 的阈值更容易被触及 → 分片更小 → 发送更早。
3. **并行流式模式可用**（**不成立**）：协议 v4 的"并行"指的是**订阅端**用 parallel apply worker 回放（事实 8、事实 13）。**发布端在 wire 上依然一次只能有一个打开的流段**——订阅端的 `in_streamed_transaction` 是进程级单值，第二个 STREAM START 直接 `ereport(ERROR, "duplicate STREAM START message")`（`worker.c:1476-1479`，release build 同样触发）。这条必须写清楚，否则会得出"可以同时流式多个事务"的错误结论。

**O2 与流式的关系**（呼应 4.1.2）：O2 把 wire 的交错粒度钉死为"一个流段"。这意味着**段既是内存粒度，也是 wire 交错粒度，还是背压粒度**——三者合一，缺一不可。若段没有字节上限（事实 5），一个 1GB 的事务会占住 wire 直到整段发完，其它 worker 产出的字节全堵在共享内存结果区里。**并行解码的产出被 O2 重新压回串行**。

因此为此：

- **新增 GUC 控制流式激进程度**，取值沿用现有调试选项 `debug_logical_replication_streaming` 的两档语义（`reorderbuffer.c:3645-3661`）：`buffered`（按内存阈值）与 `immediate`（达到批次粒度即输出）。**本设计的默认值取 `immediate`**——理由见下；
- **必须给段设置字节上限**（新增 GUC `parallel_decode_stream_segment_size`）。没有上限时，O2 会把并行解码的产出全部堵死，段的独占时间无上界。`immediate` 档的 64KB 批次粒度即为这个上限的一种取值；
- **必须给小事务设一个流式门槛**（新增 GUC `parallel_decode_stream_min_size`）。`immediate` 档若无门槛，每个只有一行的事务也会被流式化，代价有两项：(a) 帧数增加——每个流段是 STREAM_START + 变更 + STREAM_STOP 三类帧，而 12.3 指出**每帧一次 `write()` 系统调用**正是串行天花板；(b) 订阅端为**每个流式事务**分配一个 parallel apply worker（事实 8a，`applyparallelworker.c:470`），小事务流式化等于让订阅端为每个事务付一次 worker 分配开销。因此小于门槛的事务走整事务块（`PD_BLK_TXN`）；
- 与 barrier 的关系：尽早流式与 commit barrier 正交——**分片提前发，提交仍按 cseq 放行**（分片属于事务内部，不受 cseq 约束）。二者不冲突。

**为什么默认值取 `immediate` 而不是 `buffered`**：

| 理由 | 说明 |
| --- | --- |
| **喂饱订阅端的并行 apply** | 事实 8b：订阅端的并行来自"多个事务的流式分片在时间上重叠"。`buffered` 档在正常负载下不产生流段 → 订阅端的 apply worker 无事可做 → 端到端并行度为 1，本特性的收益归零 |
| **缩小挂起区，消除死锁风险面** | 未流式化的事务，其整事务字节必须等到自己的 cseq 放行才能发出（`PD_BLK_TXN` 受 barrier 门控，见 5.2.3）。挂起区因此可能装下整个事务的字节，与结果区争容量 → need.md §4.3 描述的死锁。流式化后挂起区里只剩几十字节的提交消息 |
| **兑现"尽早发送"的产品目标** | 用户需求明确要求"事务 stream 的时候要尽快发送来提升性能"。`buffered` 档把发送时机交给内存压力，恰恰是最不该由它决定的事 |

代价由 `parallel_decode_stream_min_size` 与 `parallel_decode_stream_segment_size` 两个门槛控制：**小事务不流式（省帧数、省订阅端 worker 分配），大事务早流式（省内存、早送达）**。


### 8.2 分片与整事务两种输出形态

| 订阅方 | 发布方输出形态 | 说明 |
| --- | --- | --- |
| 请求流式（协议 v2+，`streaming='on'`/`'parallel'`，事实 11） | 分片下发：worker 按阈值切分，每片是一个完整的 `PD_BLK_SEG` 块 | 首选路径，尽早发送 |
| 未请求流式 | 整事务输出：worker 不切分，事务数据是一个完整的 `PD_BLK_TXN` 块，在提交时整块写出 | 退化路径，保证整事务字节连续 |

判定依据是订阅方在 `START_REPLICATION` 中传来的 `streaming` 选项，发布方在 `pgoutput.c:488-510` 解析，本方案沿用该机制，**不新增协议**。

**两种形态下的阻塞特性不同，代价也不同：**

| 形态 | wire 被占用的时长 | 内存占用 | 主要风险 |
| --- | --- | --- | --- |
| 流式（`PD_BLK_SEG`） | 一个流段（**有字节上限**，8.1） | 每 worker `rb->size` 之和 | 段切得过小 → 协议消息数膨胀 |
| 非流式（`PD_BLK_TXN`） | **整个事务**（无上限） | 整个事务的字节 | 大事务长时间独占 wire；且字节要整块挂在待提交区等 cseq（见 7.2 的死锁风险） |

**非流式路径的整事务字节与待提交区的死锁**（必须避免）：若整事务字节与 worker 的结果区共享同一块容量，会形成循环等待——worker 想写自己的事务字节，但空间被别的 worker 的挂起事务占满；而挂起事务要等自己的 cseq，cseq 又等那个 worker 写出字节。修法有两条：（a）待提交区与结果区分离，且保证 cseq 队头事务的写入空间不被挤占；（b）cseq 队头事务的字节直通 socket，不进挂起区。**推荐 (b)**，因为非流式路径本来就退化，额外增加一份整事务缓冲不划算。

**落盘路径在 worker 侧不可用。** 现状在非流式路径下用的是**落盘**（`ReorderBufferSerializeTXN()` 写临时文件，提交时回放，事实 7），但它在多 worker 下有两个硬伤：

1. 落盘路径依赖 `MyReplicationSlot`（进程全局，`slot.c:138`）：`ReorderBufferSerializedPath()`（`reorderbuffer.c:4644`）用 `MyReplicationSlot->data.name` 拼路径，而 worker 进程里 `MyReplicationSlot == NULL`（槽独占给 walsender，事实 9）；
2. 所有 worker 共享同一个 `pg_replslot/<slot>/` 目录和同一套文件名规则（`xid-%u-lsn-%X-%X.spill`），而 `ReorderBufferCleanupSerializedTXNs()`（`reorderbuffer.c:4598-4629`）会**删除该目录下所有 `xid*` 文件**——它在 `ReorderBufferAllocate()`（进程启动）和 `ReorderBufferFree()`（进程退出）时都会调用。因此 **worker B 启动时会删掉 worker A 还没恢复的 spill 文件，worker 退出时同样会删**。崩溃恢复路径上直接丢数据。

因此首版的结论是：**并行模式下禁用落盘 spill**。reorder buffer 超过阈值时，改为把该事务整体作为 `PD_BLK_TXN` 输出（内存整事务缓冲），并依靠 12.2 的内存预算来限制单个事务的上限。代价是内存占用与事务大小成正比，但这与"非流式订阅方本就退化"的定位一致。

顺带地，`ReorderBufferAllocate()` 对 `MyReplicationSlot` 的依赖（`reorderbuffer.c:315` 的 `Assert` + `:392` 对 `MyReplicationSlot->data.name` 的解引用）必须解开：worker 需要自己的 reorder buffer，但槽并不在它进程里。修法是给 `ReorderBuffer` 增加一个 `slot_name` 字段，由 walsender 在启动 worker 时通过控制通道下发只读的槽名副本，`ReorderBufferAllocate()`/`ReorderBufferSerializedPath()` 改用它而不是 `MyReplicationSlot`。同理，`StartupDecodingContext()`（`logical.c:167-185`）需要拆分成"需要槽的部分"（walsender）与"只需要插件名 + 输出 ctx 的部分"（worker）。

## 9. 异常处理

| 故障 | 检测方 | 处理 |
| --- | --- | --- |
| worker 内部错误 | worker | `PD_ENT_ERROR` 上报；walsender 按现有逻辑复制语义终止会话并释放复制槽，**不得静默丢弃变更** |
| worker 进程崩溃 | walsender（postmaster 向 `bgw_notify_pid` 发 SIGUSR1 通知 + 共享内存状态字） | 终止本次复制会话并报错；该会话的变更不完整，不能继续发送 |
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

**但"解析语义零变更"不等于"这些文件一行不改"。** 三处必须动：

| 文件 | 改动 | 原因 |
| --- | --- | --- |
| `decode.c` | `heap_decode` / `heap2_decode` 里的 `ctx->snapshot_builder` 指向 worker 侧的**只读替身**（6.1） | 现状 `ctx->snapshot_builder->reorder == ctx->reorder`（`logical.c:213-214`）的同进程耦合断了；替身只需提供 `SnapBuildCurrentState()` 与 `SnapBuildProcessChange()` |
| `reorderbuffer.c` | `ReorderBufferAllocate()` / `ReorderBufferSerializedPath()` 改用 `ReorderBuffer` 上的 `slot_name` 字段，而不是进程全局的 `MyReplicationSlot`（`reorderbuffer.c:315/392/4644`） | worker 进程里 `MyReplicationSlot == NULL`（槽独占给 walsender，事实 9） |
| `logical.c` | `StartupDecodingContext()`（`:167-185`）拆成"需要槽的部分"（walsender）与"只需要插件名 + 输出 ctx 的部分"（worker） | `LoadOutputPlugin()` 需要插件名，插件名来自槽 |

另外 `ReorderBufferCanStartStreaming()`（`reorderbuffer.c:4028-4047`）要改为读共享状态块而不是读 `ctx->snapshot_builder` 与 `ctx->reader->ReadRecPtr`——worker 既没有 snapbuild 也不读 WAL（6.1）。**若不改，它在 worker 里恒为 false，所有超阈值的事务都会掉进 spill 路径**（而 spill 路径在 worker 侧已禁用，见 8.2），等于第 8 章在 worker 里失效。

## 11. 接口说明

### 11.1 用户接口（新增 GUC）

| GUC | 建议值 | 说明 |
| --- | --- | --- |
| `max_parallel_decode_workers` | 0（关闭，默认）/ N | 并行解码 worker 数量；0 表示回退到现有串行路径 |
| `parallel_decode_streaming_mode` | `immediate`（默认）/ `buffered` | 流式切分激进程度，语义沿用 `debug_logical_replication_streaming` 的两档（见 8.1）。**默认取 `immediate`**：订阅端的并行 apply 靠发布端产生流式分片才成立（事实 8b），`buffered` 档在正常负载下不产生流段，端到端并行度会退化为 1 |
| `parallel_decode_stream_min_size` | 建议 64KB | **小于此字节数的事务不流式化**，走整事务块（`PD_BLK_TXN`）。避免小事务流式化带来的两项开销：帧数增加（每帧一次 `write()`，见 12.3）与订阅端为每个流式事务分配一个 parallel apply worker（事实 8a） |
| `parallel_decode_stream_segment_size` | 建议 1MB | **单个流段的字节上限**。O2 把 wire 的交错粒度钉死为"一个流段"，没有上限时段的独占时间无上界，并行解码的产出会被堵死（见 8.1） |
| `parallel_decode_work_mem` | 建议 `logical_decoding_work_mem` | **每 worker** 的解码内存上限（总量 = worker 数 × 该值）。它是 `logical_decoding_work_mem` 的覆盖值——后者语义不变，但首版不做跨进程总量配额（12.2） |
| `parallel_decode_batch_size` | 建议 64KB | WAL 批次与流式分片的切分粒度；同时是**跨进程唤醒的速率上限**（批次满即 force_flush，5.4） |

所有 GUC 均需提供 `sighup` 或 `user` 级可调性，并保证关闭时行为与现状完全一致（AC-8）。

**GUC 变更的生效时机必须区分。** worker 池是**会话级**的（随 walsender 一起建立与销毁），因此：

| GUC | 变更生效时机 | 理由 |
| --- | --- | --- |
| `max_parallel_decode_workers` | **下次会话**（新 walsender 会话按新值建池） | worker 数决定了共享内存的队列条数（2N 条 `shm_mq`，§5.1）与绑定表规模，在会话中途增删会改变这些结构；且正在解码的事务不能迁移（4.5） |
| `parallel_decode_work_mem` / `parallel_decode_batch_size` | `sighup` 后**下次会话** | 同上：`ReorderBufferCheckMemoryLimit()` 只读**本进程**的 GUC 值（12.2），worker 进程必须重启才能看到新值 |
| `parallel_decode_streaming_mode` / `parallel_decode_stream_segment_size` | `sighup` 后**下次会话** | 分段参数决定 worker 何时切分流段，切分点一旦不同会导致 wire 上的分片边界与现状不一致（§4.2 的"分片切点不一致"），会话中途切换会让同一事务的前后两段用不同参数切分 |

因此这些 GUC 虽然在 `sighup` 级可调，但**变更只在下一次复制会话生效**，不应给出"立即生效"的预期。这一点必须在 GUC 的说明里写清楚，否则运维会在会话中途改值并观察到不一致的行为。首版可只提供 `user` / `session` 级可调，把"会话中途变更"这个问题彻底消掉。

### 11.2 程序内部接口

- `ParallelDecodeCreate()` / `ParallelDecodeDestroy()`：共享内存与子进程管理；
- `ParallelDecodeAssignTxn()`：事务→worker 绑定（多对一，绑定后不可迁移；代价函数是**在途字节数**，见 4.5.1）；
- `ParallelDecodeAllocCommitSeq()`：提交序号分配；
- `ParallelDecodePublishSnapshot()` / `ParallelDecodePublishInvalidations()`：快照与失效分发；
- `ParallelDecodeSubmitBytes()`：worker 产出协议字节。

### 11.3 监控指标

| 指标 | 含义 |
| --- | --- |
| `pg_parallel_decode_workers` | 各 worker 的在途事务数、已解码事务数、**在途字节数**、队列深度 |
| `pg_parallel_decode_commit_lag` | `next_commit_seq` 与已分配 cseq 的差值（顺序积压） |
| `pg_parallel_decode_txn_latency` | 单事务从首条 WAL 到提交字节产出的耗时分布 |
| `pg_parallel_decode_pending_commit_age` | **最长挂起提交年龄**——cseq 队头事务已等待多久。只看"cseq 等待深度"不足以定位这类停顿（12.3 的负载不均放大） |
| `pg_parallel_decode_walsender_frames` | walsender 的**输出帧率与 socket 写次数**（12.3 的帧率风险） |
| `pg_parallel_decode_wakeups` | 跨进程唤醒（信号）的速率，区分上下行（5.4） |

## 12. 性能

### 12.1 目标与测量

- 目标：tpcc 100 万 tpm 场景下**实际发出的协议字节速率** ≥ 70MB/s（口径见下）；
- **度量口径必须修正**：`confirm_flush_lsn` 速率在并行模型下会**虚高**——订阅端用 `'w'` 帧的 LSN 推进 `last_received` 并回报 flush LSN，而事务数据允许乱序下发（7.4），于是 flush LSN 可能被后发的事务抬高，而前面事务的数据其实还在路上。因此 G1 的度量口径应为"发布端实际写出的协议字节速率"或"订阅端**确认的应用速率**"，不能直接用 `confirm_flush_lsn`；
- **达成条件（"与"关系，缺一不可）**：订阅端 `streaming='parallel'` 且 `max_parallel_apply_workers_per_subscription > 0`；发布端**尽早流式**（`parallel_decode_streaming_mode = immediate`，否则订阅端的 apply worker 无事可做——事实 8b）；发布端有段粒度上限（`parallel_decode_stream_segment_size`）；walsender 按块/按 8KB 缓冲批量刷出而非每帧一次 `write()`；发布端开启并行解码。**这几项之外的瓶颈不在解码侧**（15.1 的测算：N = 3~4 个 worker 就够解码 70MB/s）；
- 需同时测量：单核场景的劣化幅度（应接近 0）、小事务延迟（进程调度不应引入显著抖动）、**walsender 的输出帧率与 socket 写次数**（见 12.3）。

### 12.2 资源与内存

- **解码内存**：现状 `logical_decoding_work_mem` 是单个解码上下文的阈值。改为多 worker 后，若每个 worker 都按该值分配，总内存上限为 N 倍。两种选择：(a) 保持 `logical_decoding_work_mem` 语义不变、总量为 N 倍，并在文档中明确；(b) 新增总量级 GUC，worker 均摊。

  **取 (a)（每 worker 独立上限）**，理由是 (b) 需要一整块目前不存在的机制：`ReorderBufferCheckMemoryLimit()`（`reorderbuffer.c:3659-3676`）只读**本进程**的 GUC 值与**本 rb** 的大小，代码里没有全局配额概念。要实现 (b)，必须新增：一份跨进程共享的"已用内存总量"计数器、`ReorderBufferCheckMemoryLimit()` 里的一次全局查询、以及一条周期性的用量上报通道（§5.2 没有对应消息）。这是缺失的一整块设计，而不是配置改动。

  因此首版的约定是：`logical_decoding_work_mem` 语义不变，**总量 = worker 数 × 该值**，并在 GUC 的说明里显式写出这一点；同时 `parallel_decode_work_mem`（§11.1）作为**每 worker**的上限覆盖它，两者的关系必须写清楚，不能各说各话。

- **阈值的作用域**：一个 worker 承载多个在途事务（4.4），`rb->size` 是该 worker 上所有在途事务的聚合大小，因此内存阈值实际约束的是“每 worker 总量”，而不是“每事务大小”。这与现状（单进程、单解码上下文、聚合语义）一致，只是聚合的边界从“进程”变成了“worker”。
- **prepared 事务的驻留**：prepared 事务在 PREPARE 之后一直留在 worker 的 reorder buffer 里，直到 COMMIT PREPARED / ROLLBACK PREPARED（7.1），驻留时间**无上界**。它持有快照副本与累积的变更，因此内存预算必须为它单列一条——不能假定"所有在途事务都是短命的"。
- **共享内存**：WAL 批次区 + 结果区，大小随并发事务数与在途字节量增长，需设定上限并纳入 `max_worker_processes` 与 `shared_buffers` 之外的独立核算。
- **进程数**：walreader 1 + decode worker N，均为动态后台进程，占用 `max_worker_processes` 名额；与并行查询、并行 apply 的名额竞争需在文档中提示。

### 12.3 性能风险点

| 风险点 | 说明 | 缓解 |
| --- | --- | --- |
| **协议帧率 / 系统调用率** | output plugin 是**每条消息**一次 `OutputPluginWrite`（`pgoutput.c:1553/1574` 等），每个 INSERT/UPDATE/DELETE 都是一个独立的 CopyData 帧；而 `WalSndWriteData()` 在**每帧之后**调用 `pq_flush_if_writable()`（`walsender.c:1576-1598`），该函数会立刻把 `PqSendBuffer` 里的数据 `write()` 出去（`pqcomm.c:1432-1451`）。`PQ_SEND_BUFFER_SIZE` 只有 8192，缓冲复用挡不住这个粒度——**每帧一次 `write()` 系统调用**。70MB/s WAL 在 tpcc 行宽下约 30–70 万变更/秒，即 30–70 万次/秒的系统调用，单核开销约 0.5–1.5 核，且**完全串行在 walsender 一个进程里** | 并行模式的 walsender 不沿用"每帧 `pq_flush_if_writable()`"的节奏，改为**按块 / 按 8KB 缓冲刷出**（帧不能合并——订阅端 `apply_dispatch()` 一个 CopyData 只解析一条逻辑消息，`worker.c:3283-3300`——所以只能从 socket 写这一层摊薄）。系统调用率可从"帧率"降到"字节率 / 8KB"，降幅约 20–40 倍 |
| **段粒度串行下发** | O2 把 wire 的交错粒度钉死为"一个流段"；若段无字节上限（事实 5）→ 一个 1GB 的事务会占住 wire 直到整段发完，其它 worker 的产出全堵在结果区（8.1） | 给段设字节上限 GUC（`parallel_decode_stream_segment_size`） |
| **小事务被过度流式化** | `immediate` 档若无门槛，每个一行的事务也会被流式化：(a) 帧数增加——每个流段是 STREAM_START + 变更 + STREAM_STOP 三类帧，而本行的"每帧一次 `write()`"正是串行天花板；(b) 订阅端为**每个流式事务**分配一个 parallel apply worker（事实 8a，`applyparallelworker.c:470`），小事务流式化等于每事务付一次 worker 分配开销 | `parallel_decode_stream_min_size` 门槛：小事务走整事务块，不流式化（8.1） |
| **目录缓存 N 倍** | syscache / catcache / relcache 是**进程私有**的（`syscache.c:86` 的静态 `SysCache[]`、`relcache.c:134` 的静态 `RelationIdCache`），`RelationSyncCache` 也是每 worker 一份（`pgoutput.c:82/89/217`）。每个 worker 首次触达某关系时各自要走一遍发布关系判定（`GetRelationPublications` / `GetSchemaPublications`）；DDL 之后 `RelationSyncCache` 被整体作废，**每个 worker 都要把全部关系重新判定一遍**。DDL 频繁时这不是"少量冗余带宽"，而是 N 倍的目录扫描 CPU | 把"关系 → 发布列表 / schema"做成**只读共享内存表**（walreader 维护、worker 只读）。这才是 4.2 中"共享关系表"的真正收益来源——不只省带宽，还省 N 倍目录扫描 |
| **每事务内部事务与全局失效队列** | 每解码一个事务，worker 都要 `StartTransactionCommand` → `AtStart_Cache` → 在**全局 `SInvalReadLock`** 上取共享锁并排空共享失效队列（`xact.c:1163-1167` → `inval.c:807-811` → `sinvaladt.c:497`），事务结束再 `AbortCurrentTransaction`（`reorderbuffer.c:2147-2150/2574`）。N 个 worker × 每事务一次 = N×T 次对同一把全局 LWLock 的共享获取；更贵的是排空后每个 worker 都要独立作废自己的 catcache/relcache 条目 | worker 内把多个事务的解码合并到一个内部事务里做（现有代码已支持事务内做 INTERNAL_SNAPSHOT / INTERNAL_COMMAND_ID 的 Setup/Teardown，`reorderbuffer.c:2428-2478`），只要不在解码期间分配 XID 就不违反现有约束；与"目录缓存 N 倍"一起通过共享目录缓存解决 |
| **catcache miss 的扫描** | catcache miss 时的索引扫描在 historic snapshot 下进行（`genam.c:408-413`），N 个 worker 各自独立 miss、各自独立扫描，每次都要做可见性判断 | 同上，共享目录缓存 |
| 协议格式化仍单线程 | 若最终退化为方案 B，walsender 成为新瓶颈 | 方案 A 已把格式化移入 worker |
| 共享内存带宽 | 每条数据变更要过共享内存两次（下行 WAL、上行字节），且 `shm_mq` 的载荷本身是 memcpy（`shm_mq.c:1043`） | 批量提交；必要时评估固定槽 + 引用计数的自研队列（真正的零拷贝） |
| **跨进程信号速率** | 跨进程 `SetLatch()` 的实现是 `kill(pid, SIGURG)`（`latch.c:695`），本分支的 `Latch` 没有 futex 语义（`latch.h:112-121`）。按每事务一批、16.7k 事务/秒估算，信号速率约 1.3 万～6.7 万次/秒；`shm_mq` 的接收方每读一次还会反向唤醒发送方（`shm_mq.c:1296`），信号总数翻倍 | 批次按**字节阈值**（`parallel_decode_batch_size`）而非按事务切；显式选择 `force_flush` 策略（5.4）；把唤醒速率纳入监控 |
| 关系缓存重复 | RELATION 消息按 worker 重复 | 首版容忍，后续用共享关系表优化 |
| worker 负载不均 | 事务大小分布不均 | **按在途字节数**优先分配（4.5.1）+ 队列水位反馈 |

## 13. 兼容性与约束

1. **发布端行为变更**：开启并行解码后，发布端的 WAL 读取点、快照推进点发生迁移，需要保证复制槽的 `restart_lsn` / `confirmed_flush` 语义与现状等价。注意这里的"等价"是**有条件的**：并行模型下 `restart_lsn` 由两个跨进程聚合点共同决定（xmin 与 restart_lsn，6.6），`confirmed_flush` 依赖不变式 S1——已送达帧的 LSN 不得超过下一个待放行事务的提交记录 LSN（4.1.3、7.4）。这两条都必须显式实现并验证，不能假设"自然成立"。
2. **订阅端零改动**：协议格式、消息类型、顺序语义均不改变，订阅端无需升级或改配置；
3. **非流式订阅方**：走整事务输出路径。**不走现状的落盘回放**——落盘在多 worker 下会互删文件且依赖 `MyReplicationSlot`（8.2）。改为**内存整事务缓冲**：worker 不切分，事务数据是一个完整的 `PD_BLK_TXN` 块，在其 cseq 轮到时整块写出。行为语义与现状一致（整事务字节连续、提交有序），代价是内存占用与事务大小成正比。**这是兼容路径，不承诺性能收益**——本特性的目标部署形态是 `streaming='parallel'`（3.5），非流式订阅方走的是整事务缓冲 + barrier，wire 占用时间反而比现状更长；
4. **并行流式模式（协议 v4）**：协议 v4 的"并行"指的是**订阅端**用 parallel apply worker 回放（事实 8）。**发布端在 wire 上依然一次只能有一个打开的流段**（事实 13），本特性也不改变这一点。本特性在此模式下的收益在于"解码侧并行 + 分片更早"，不在于"同时打开多个流段"；**而"分片更早"恰恰是让订阅端的并行 apply 生效的前提**（事实 8b、3.5）；
5. **两阶段提交**：`PREPARE` / `COMMIT PREPARED` / `ABORT PREPARED` 参与同一 cseq 序列，保持状态机顺序。PREPARE **不解绑**事务（7.1）；
6. **逻辑解码 SQL 接口**（`pg_logical_slot_get_changes()` 等）保持单进程串行实现，不受本特性影响；
7. **第三方 output plugin**：不新增必选接口；插件在 worker 中的实例化需满足"一个插件实例只处理一个数据库连接"的约束，与现状一致；
8. **崩溃恢复**：worker/walreader 崩溃后，会话重建时从复制槽的 `restart_lsn` 重新开始，语义与现状一致；
9. **复制槽的水位推进**：由 walsender 代为执行（事实 9）。xmin 与 restart_lsn **两个水位**都需要跨进程聚合（6.6），需保证请求-回执的时序不会导致 xmin 停滞、也不会让 restart_lsn 跳过在途事务；
10. **订阅端侧文件不改动**：`src/backend/replication/logical/relation.c` 是订阅端的远端→本地关系映射，与本特性无关，不在改造范围内；
11. **目标部署形态**（3.5）：本特性的性能承诺只覆盖"发布端并行解码 + 订阅端 `streaming='parallel'` + 多个 parallel apply worker"。不满足时并行解码仍可工作（协议与语义不变），但**不承诺端到端吞吐收益**。这个限制必须写进用户可见的文档，否则会出现"发布端开了并行、订阅端没开 parallel apply、却期待 70MB/s"的落差。

## 14. 风险与对策

| 风险 | 影响 | 对策 |
| --- | --- | --- |
| 目录变更导致 worker 读到错误的元数据 | 变更内容错误 | 失效消息随事务下发并在事务结束时执行（6.2）；xid-less 的失效**广播给所有 worker**（6.5）；以 DDL 混合负载重点验证 |
| cseq 空洞导致 barrier 卡死 | 复制停滞 | cseq 连续分配；barrier **靠 END_ACK 推进**（含空事件），空洞只可能是 worker 崩溃；配合超时与错误上报转为会话失败（7.2） |
| 提交顺序与订阅端 apply 顺序不一致 | 数据不一致 | 以 cseq 严格保序；与现有串行路径做逐事务比对 |
| `'w'` 帧 LSN 把 `confirmed_flush` 抬到未送达事务之前 | 重启后该事务被整体跳过 → **静默丢数据** | 维持不变式 S1：已送达帧的 LSN 不得超过下一个待放行事务的提交记录 LSN（4.1.3）；非流式数据跟随 cseq 放行；**流式分片的帧 LSN 由 walsender 按放行前沿钳制**（7.4） |
| `restart_lsn` 越过仍在途的事务 | 发布端重启后丢变更 | walreader 侧维护事务绑定表并汇总最老在途事务的 `restart_decoding_lsn`（6.6.2） |
| wire 上出现两个打开的流段 | 订阅端 `ereport(ERROR, "duplicate STREAM START message")` | walsender 一次只取一整块，块未取完不得取其它 worker 的字节（5.4 第 6 条、O2） |
| 共享内存队列成为瓶颈 | 吞吐不升反降 | 批量提交、定向唤醒；队列深度纳入监控。注意 `shm_mq` 的载荷是 memcpy（`shm_mq.c:1043`），"零拷贝"需要自研固定槽队列，不是 `shm_mq` 的能力 |
| worker 负载不均 | 部分 worker 空闲 | **按在途字节数**优先分配（4.5.1）；周期性上报校正 |
| 内存占用放大 | OOM 风险 | 每 worker 上限 × worker 数（12.2）；prepared 事务单列 |
| 进程数与并行查询/并行 apply 争抢 | 资源不足 | 文档提示；可与 `max_worker_processes` 协调 |
| **提前流式的分片帧 LSN 抬高 `confirmed_flush`** | 订阅端确认水位越过尚未送达的事务 → 重启后该事务被跳过 → **静默丢数据** | 流式分片虽然可以早发，但其帧 LSN 必须被**放行前沿**钳制（`min(frame LSN, frontier)`，7.4）；放行前沿 = cseq = 放行水位 + 1 的事务的提交记录 LSN。**这是本设计新增的数据丢失路径，必须专门验证**（见 16 的 P6 验收） |
| walsender 每帧一次 `write()` 系统调用 | 协议帧率高时成为串行天花板 | 批量刷出（`pq_flush` 合并多个 `CopyData`）；帧率纳入监控（11.3 的 `pg_parallel_decode_walsender_frames`） |
| 段粒度串行下发阻塞整条 wire | 一个慢 worker 拖住全部产出 | `parallel_decode_stream_segment_size` 给段字节上限；预取掩盖切换开销（5.4 第 7 条） |
| 端到端吞吐上限实为订阅端 apply 速率 | 发布端优化不产生端到端收益 | 文档明确"并行解码的收益前提是订阅端 `streaming='parallel'` + 多个 parallel apply worker + 发布端尽早流式"（3.5）；在 12.1 写清达成条件 |
| **订阅端未开 parallel apply** | 期望落空：发布端已并行、订阅端仍串行 apply，端到端无收益 | 在用户可见文档与 GUC 说明中显式写出目标部署形态（3.5）；监控项暴露订阅端的 streaming 模式与 apply worker 数量 |
| 小事务被过度流式化 | 帧数上升 → 每帧一次 `write()` 的系统调用成为瓶颈；订阅端每事务一次 apply worker 分配 | `parallel_decode_stream_min_size` 门槛（8.1、12.3） |

## 15. 待决问题

| 编号 | 问题 | 备选 | 倾向 |
| --- | --- | --- | --- |
| Q1 | 提交顺序方案 | commit barrier / FIFO | commit barrier（4.3） |
| Q2 | 解码切分点 | 事务级 A / 变更级 B / A+关系表 C | A 为主，C 为后续优化（4.2） |
| Q3 | 事务→worker 绑定基数 | 一对一 / 多对一 | 多对一：一个 worker 承载多个事务，绑定后不可迁移（4.4、4.5） |
| Q4 | 关系消息重复 | 容忍 / 共享关系表 | 首版容忍（4.2） |
| Q5 | 解码内存语义 | 每 worker 独立 / 总量均摊 | **每 worker 独立**——总量均摊需要一整块尚不存在的跨进程配额机制（12.2） |
| Q6 | 流式切分默认档 | buffered / immediate | **默认 `immediate`**——订阅端的并行 apply 靠发布端产生流式分片才成立（事实 8b、3.5），`buffered` 档在正常负载下不产生流段，端到端并行度会退化为 1（8.1）。**两种档都必须有段字节上限**（`parallel_decode_stream_segment_size`）与小事务门槛（`parallel_decode_stream_min_size`） |
| Q9 | 事务数据是否跟随 cseq 放行 | 跟随（安全）/ 按 LSN 序（等价 FIFO） | 跟随 cseq（7.4 的 `'w'` 帧 LSN 约束） |
| Q7 | worker 崩溃是否降级为串行 | 终止会话 / 降级 | 终止会话（9、17.3） |
| Q8 | 子进程形态 | 动态后台进程 / walsender 直接 fork | 动态后台进程，参照 `launcher.c:503`（3.3） |
| Q10 | worker 数量与收益的匹配度 | 按解码算力配 N / 按端到端瓶颈配 N | **待定**——按源码成本结构，单核算力只差 1.5–2 倍，N = 3~4 就够达到 70MB/s 的解码能力（见 15.1）。**瓶颈不在解码**，而在 walsender 的每消息系统调用、O2 的段粒度串行下发、订阅端 apply 速率。因此"整套并行机制"的复杂度投入与解码侧的收益并不匹配，需要在实现前确认收益究竟落在哪一段 |

### 15.1 收益测算：并行度并不需要很大

按源码的真实成本结构估算 tpcc NewOrder（约 25 条行级变更、约 320 列）：

| 成本项 | 源码位置 | 每事务 |
| --- | --- | --- |
| 每列：catcache 查类型 + 类型输出函数 + 写协议缓冲 | `proto.c:828/853/854` | 约 42–112 µs |
| 每变更：relmapper + relcache + tuple 重建 | `reorderbuffer.c:2227/2251`、`decode.c:1253` | 约 8 µs |
| 每事务：内部事务 + 失效队列排空 + 快照拷贝 | `reorderbuffer.c:2150/2574/1833` | 约 3–5 µs |
| **合计** | | **约 75–125 µs / 事务** |

→ **单核解码上限约 8k–13k 事务/秒 ≈ 500k–800k tpm**。tpcc 100 万 tpm（16.7k 事务/秒）只需要约 **1.5–2.0 核**的解码算力；按每事务约 3KB WAL 折算，100 万 tpm ≈ 53MB/s WAL，G1 的 70MB/s 大致对应 130 万 tpm 量级，约 **2–2.5 核**。

**结论：N = 3~4 个 worker 就足够达到 70MB/s 的解码能力。** 更多的 worker 收益递减，而共享内存与信号开销线性上升。这一点必须写进设计的自我评估——本设计的复杂度（三进程拆分、共享内存、cseq barrier、顺序契约）远超解码侧的需求。

真正的端到端瓶颈是另外三处：

1. **walsender 的每帧一次 `write()` 系统调用**——这是并行化后最硬的串行天花板（12.3）；
2. **O2 的段粒度串行下发**——段无字节上限时 wire 的阻塞时间无上界（12.3、8.1）；
3. **订阅端 apply 速率**——发布端的吞吐上限 = 订阅端的 apply 速率；tpcc 下 42 万行/秒单个 apply worker 做不到，**整个特性的收益完全建立在订阅端已经并行回放（`streaming=parallel` + 多个 parallel apply worker）的前提上**。

因此 70MB/s 的**达成条件**必须写清：订阅端 `streaming=parallel` + 段粒度上限 + walsender 批量刷出 + 发布端并行解码。这三者是"与"的关系，缺一项则端到端达不到目标。

### 15.2 待实测确认清单

以下五项无法从源码静态推断，必须在实现期用实测数据确认。它们都直接影响上文的测算与参数取值：

| # | 事项 | 需要什么证据 | 影响的设计决策 |
| --- | --- | --- | --- |
| 1 | 订阅端是否把 `'w'` 帧的 LSN 用于 `confirmed_flush` 之外的用途 | 通读 `worker.c` 中 `last_received` / `send_feedback` 的全部使用点，以及 `pg_stat_replication` 的 `sent_lsn` / `write_lsn` 语义 | 7.4 的钳制方案是否足够（若订阅端另有用途，钳制会引入新的不一致） |
| 2 | `SnapBuildSerialize` 是否在每个 `XLOG_RUNNING_XACTS` 上真的重写 | 实测 `pg_logical/snapshots` 目录的写入频率，或读其 body 的"是否需要重写"判据 | walreader 的磁盘 I/O 是否会成为新的瓶颈 |
| 3 | `catalog_xmin` 在多 worker 下实际会被推多旧 | 一个"worker 持有长事务"的实测，看 `pg_replication_slots.catalog_xmin` 的推进行为 | 6.6 的 xmin 聚合策略（保守兜底是否够用） |
| 4 | 70MB/s 场景下协议帧率的准确值 | 一次真实负载的 `pg_stat_replication` 采样 + 协议字节数统计（帧数需从发布端埋点） | 12.3 的系统调用率估算与批量刷出策略 |
| 5 | `PQ_SEND_BUFFER_SIZE=8192` 在真实负载下的 `repalloc` 频率（`pqcomm.c:1533`） | 负载中"单条变更 > 8KB"的比例（宽表 / 大 TEXT） | 批量刷出的缓冲大小是否需要提高 |

## 16. 实现计划

按可独立验证的阶段推进，每个阶段结束时开关关闭的行为必须与现状完全一致（AC-8）。

| 阶段 | 目标 | 主要改动 | 阶段验收 |
| --- | --- | --- | --- |
| P0 基础设施 | 建立共享内存与子进程骨架 | `parallel_decode.c`、`parallel_decode_msg.c`：dsa 创建与附着、`shm_mq` 封装、消息编解码、动态后台进程注册 | 单 worker 串行跑通一轮复制，字节输出与现状逐字节一致 |
| P1 walreader 拆分 | 把 WAL 读取与 snapbuild 从 walsender 迁到 walreader | `parallel_walreader.c`：WAL 读取循环、snapbuild 驱动、记录分类与路由；walsender 侧改造为只做网络与顺序 | 解码结果不变，`restart_lsn` / `confirmed_flush` 语义等价（AC-2、AC-8） |
| P2 worker 解码 | worker 承担解码与插件格式化 | `parallel_decode_worker.c`：批次消费、reorder buffer 与 output plugin 实例化、结果产出；`logical.c` 支持按 worker 实例化 | 事务数据全部经 worker 产出，walsender 只剩发送（AC-2） |
| P3 顺序保证 | 引入 cseq 与 commit barrier | cseq 分配、`next_commit_seq` 水位与待提交表 | commit/abort/cancel 顺序与 WAL 一致（AC-3） |
| P4 多 worker 并行 | worker 池与事务分配 | 事务→worker 绑定表、**按在途字节数**优先分配、队列水位反馈 | 吞吐随 worker 数上升（AC-1、AC-6） |
| P5 快照与失效 | 事件流分发与 tuplecid 传递 | base snapshot 由 walreader 分配并通过**快照版本池**下发；命令 ID / 失效 / tuplecid 由 worker 从原始 WAL 记录**自行重建**；`rbtxn_has_catalog_changes` 标志随 NEW_CID 记录下发 | DDL 混合负载下元数据与现状一致（AC-5） |
| P6 尽早流式 | 分片切分与提前下发 | 流式模式 GUC、批次粒度切分、结果区提前发送；**walsender 按放行前沿钳制分片帧 LSN**（7.4 的方案 c） | 大事务在解码完成前即可到达订阅端（AC-4）；**且提前送达的分片不会把 `confirmed_flush` 抬到未放行事务的提交记录之上**（不变式 S1，用 kill/restart 实验验证无丢数据） |
| P7 故障恢复 | 进程生命周期与故障处置 | worker 状态字、`bgw_notify_pid` 通知、barrier 超时、退出码与 `bgw_restart_time` 约束、cseq 作废路径 | kill worker 后会话及时终止而非静默停滞；重启后可从 `restart_lsn` 续传（AC-10、AC-11） |

推进顺序上的三条约束：

1. **P1 之前不改解码路径。** 先把 WAL 读取搬走、解码仍留在 walsender，用现有路径验证进程拆分本身没有引入行为差异。
2. **P2 之前不引入多 worker。** 单 worker 打通后，多 worker 只是分配策略的变化；否则进程拆分、解码改造、并行调度三个变量同时出错，难以定位。
3. **P3 可以在 P1 之后独立推进。** cseq 的分配只依赖 walreader 的 WAL 顺序，与 worker 是否解码无关，可以与 P2 并行实现。

## 17. 故障恢复

### 17.1 恢复的唯一持久化锚点是复制槽

进程内的任何状态都不参与恢复：worker 的 reorder buffer、output plugin 私有状态、tuplecid 哈希、快照副本、共享内存队列、cseq 水位与提交水位，全部随进程消失而丢失。

**唯一跨故障存活的状态是复制槽**：

| 槽内字段 | 作用 | 写盘时机 |
| --- | --- | --- |
| `restart_lsn` | 解码的起点。它之前及之上的 WAL 被槽保留，不会被回收 | `ReplicationSlotSave()`（`slot.c:992`）、`ReplicationSlotPersist()`（`slot.c:1027`） |
| `confirmed_flush` | 订阅端已确认接收的位置。它只随订阅端反馈前进 | 同上 |
| `xmin` / `catalog_xmin` | 槽保留的事务视野，决定能回收多老的事务 | 同上 |
| `persistency` | 槽是持久化还是临时 | `ReplicationSlotPersist()` 把临时槽转为持久化槽 |

槽状态写在 `pg_replslot/<name>/state`，由 `StartupReplicationSlots()`（`slot.c:1896`）在**崩溃恢复之前**读回内存；带 CRC 校验，读回失败直接 PANIC。临时槽（`RS_EPHEMERAL` / `RS_TEMPORARY`）在崩溃后被删除而不是恢复。

由此得出恢复模型：

> **恢复 = 终止旧会话 + 重建会话 + 从槽的 `restart_lsn` 重新解码。**
> 不存在“把崩溃进程的状态捞回来”的路径，也不需要这样的路径——槽的重放语义天然覆盖了崩溃瞬间的全部未确认变更。

这条模型与现有单进程实现**完全一致**：现有实现中 walsender 崩溃后的恢复路径同样是“重建会话、从 `restart_lsn` 重放”。并行化不改变恢复语义，只增加了需要被终止的进程数量。

### 17.2 故障分类与处置

| 故障 | 退出方式 | postmaster 行为 | 本设计的处置 | 恢复路径 |
| --- | --- | --- | --- | --- |
| worker 内部错误 | `proc_exit(1)`（`bgworker.c:815`；`elog.c:590` 的 FATAL 路径同此） | 记 `rw_crashed_at`，**不触发集群重启**（`postmaster.c:2737-2739`：只有 0/1 以外的退出码才触发系统级重启） | worker 上报 `PD_ENT_ERROR`；walsender 终止会话 | 订阅端重连 → 新会话从 `restart_lsn` 重放 |
| worker 崩溃（信号） | 被信号杀死 | **触发集群级 crash recovery**（`postmaster.c:3040` 置 `FatalError`） | 集群整体恢复，槽从磁盘恢复 | 数据库重启 → 同上 |
| worker 正常退出 | `proc_exit(0)` | 记 `rw_terminate`，不重启该 worker | 视为会话终止 | 同上 |
| walreader 崩溃 | 同 worker | 同上 | 终止会话 | 同上 |
| walsender 崩溃 / 会话断开 | 正常或异常 | 异常退出触发集群级 crash recovery | `ReplicationSlotRelease()`（`walsender.c:341`）释放槽；槽状态已落盘 | 订阅端重连 → 新会话从 `restart_lsn` 重放 |
| 数据库重启 | — | `StartupReplicationSlots()` 恢复槽；所有动态后台进程消失 | 无需额外动作 | 客户端重连 → 新会话 → 重新构建快照 |

关键差别在于**谁死了**：

- **worker / walreader 死亡**（以退出码 1 结束）：这是**局部故障**。集群继续运行，复制会话终止，订阅端自动重连即可恢复，数据库本身不需要重启。
- **walsender 死亡**（异常退出）：这是**全局故障**。按 PostgreSQL 的既有语义，普通后端的异常退出会让 postmaster 终止所有其他后端并进入崩溃恢复（`postmaster.c:3040`）。这一点与现状相同，不是并行化引入的问题。

### 17.3 worker 处理到一半崩溃

这是并行化引入的唯一一个新故障点，需要单独设计。

**丢失了什么**：该 worker 上全部在途事务的解码状态——已解码但未发出的字节、reorder buffer 中累积的变更、插件私有状态、tuplecid 哈希、待执行的目录失效消息。

**为什么不尝试“部分恢复”**：

1. **流式分片可能只发了一半。** 一个事务的字节可能已经部分写入 socket，订阅端手里是一个不完整的事务。无法判断哪些字节可以保留。
2. **重发粒度是事务，不是字节。** 复制槽的重放单位是 WAL 记录，重发的最小单位是事务。要恢复一个事务，必须从它的第一条 WAL 记录重新解码——这正是“从 `restart_lsn` 重放”所做的事。
3. **迁移到其他 worker 会破坏事务亲和性**（4.5）。worker 之间的状态搬迁代价高于整个会话重建。
4. **已发出的字节可能已被订阅端 apply。** 重发会产生重复，这与逻辑复制的既有语义一致（见 17.7），不构成本设计特有的问题。

结论：**worker 崩溃一律终止会话，不做进程级或事务级的抢救。** 这与第 9 章“任何可能造成变更丢失或乱序的故障，一律以终止会话的方式暴露”的原则一致。

**故障检测**。worker 死亡后，walsender 若继续等待它持有的 cseq，barrier 会出现空洞并永久卡死——复制会静默停滞而不是失败。因此检测必须做到“确定、快速”：

| 机制 | 做法 | 覆盖的场景 |
| --- | --- | --- |
| 共享内存状态字 | 每个 worker 在共享内存中有一个状态字段（RUNNING / EXITED / FAILED）；退出前由 worker 写入 FAILED 并附错误码 | worker 主动退出或捕获到错误后正常退出 |
| postmaster 通知 | 注册 worker 时设置 `bgw_notify_pid = MyProcPid`；worker 启动（`bgworker.c:467-478`）与退出（`bgworker.c:486-515`）时 postmaster 都向该 pid 发送 SIGUSR1，walsender 被立即唤醒 | worker 异常死亡、信号杀死 |
| walreader 监控 | walreader 持有事务→worker 绑定表，能判断“某个 cseq 的持有者已经不在了”，据此判定 barrier 空洞不可恢复 | cseq 空洞的直接归因 |

三者结合：状态字给出**原因**，SIGUSR1 给出**即时唤醒**，walreader 给出**归因**。任一机制单独失效都不会造成静默停滞——最坏情况下 walsender 的 barrier 等待超时（见下）。

**barrier 等待超时**。walsender 在等待 `next_commit_seq` 时必须带超时：正常情况下 cseq 的分配不依赖任何 worker（4.3），提交消息的延迟只可能是 worker 尚未解码完成；超过阈值即判定会话失效并终止，而不是无限等待。这条兜底保证了“检测机制全部失效”时退化为“复制停滞并报错”，而不是“永久挂起”。

**不自动重启 worker**。`bgw_restart_time` 必须设为 `BGW_NEVER_RESTART`：postmaster 在该设置下会移除该 worker 并向 `bgw_notify_pid` 发送 SIGUSR1（`postmaster.c:4459-4470`）。若设置了自动重启，postmaster 会在 `bgw_restart_time` 之后拉起一个新 worker，但新 worker 没有任何前一个 worker 的在途事务状态，只会让事务在两个 worker 之间分裂，破坏 4.4 的亲和性。**worker 的重建必须以会话重建的形式进行，由 walsender 统一发起。**

### 17.4 walreader 崩溃

walreader 持有 WAL 读取位置、snapbuild 状态与事务→worker 绑定表。这些全部是进程内状态，崩溃即丢失。

处置与 worker 崩溃相同：**终止会话**。区别在于 walreader 的死亡同时意味着 WAL 读取位置丢失，会话必须从槽的 `restart_lsn` 重新开始，而不是从崩溃点继续——这同样由复制槽的重放语义保证。

一个必须避免的误判：walreader 崩溃后，walsender 仍在按 cseq 等待提交消息，而 cseq 的分配者已经死了。**cseq 的分配者与 cseq 的消费者必须同生共死**——walreader 死亡即意味着本会话的所有 cseq 作废，barrier 等待必须立即失败，不能等到超时。

### 17.5 walsender 崩溃与会话断开

walsender 持有复制槽与 CopyBoth 连接。异常退出时：

- 槽被释放（`ReplicationSlotRelease()`，`walsender.c:341`），但**槽的状态已经落盘**，`restart_lsn` / `confirmed_flush` / `xmin` 保留；
- 按 PostgreSQL 的既有语义，walsender 的异常退出会触发集群级崩溃恢复，所有其他后端被终止，共享内存被重建，槽在重启时由 `StartupReplicationSlots()` 读回；
- 订阅端的 walreceiver 检测到连接断开后按既有逻辑重连，重新发起 `START_REPLICATION`，新会话从 `restart_lsn` 开始解码。

与现状一致，无需额外机制。

### 17.6 数据库重启后的恢复

数据库重启（正常重启或崩溃恢复）后的完整过程：

| 步骤 | 发生什么 | 数据来源 |
| --- | --- | --- |
| 1. 槽恢复 | `StartupReplicationSlots()` 在崩溃恢复之前把 `pg_replslot/<name>/state` 读回内存，`restart_lsn` / `confirmed_flush` / `xmin` / `catalog_xmin` 全部还原 | 磁盘 |
| 2. 子进程清空 | walreader 与全部 decode worker 随 postmaster 一起消失；共享内存的 dsa 区域一并释放 | — |
| 3. 客户端重连 | 订阅端重连并重新发起 `START_REPLICATION`，触发一个新的 walsender 会话 | 客户端 |
| 4. 快照重建 | 新会话按既有流程 `DecodingContextFindStartpoint()`：从 `slot->data.restart_lsn` 重新读 WAL（`logical.c:655`），推进 snapbuild 直到 consistent。**并行化不改变这一步**——快照仍由单一进程构建，只是该进程从 walsender 换成了 walreader | WAL |
| 5. 重新解码 | 从 `restart_lsn`（推进到 `confirmed_flush` 之后的起点）重新解码并重发给订阅端 | WAL |
| 6. cseq 重置 | cseq 分配与 `next_commit_seq` 水位都从 1 重新开始 | 内存 |

**第 6 步是本设计特有的约束**，必须写进实现：cseq 是会话内序号，不是持久化序号。walreader 与 walsender 必须同时重建、同时重置。若 walsender 存活而 walreader 重启（理论上不应发生，因为 walreader 死亡必然终止会话），旧 cseq 与新 cseq 会冲突，导致 barrier 永久卡死。因此实现上必须保证：**walreader 的重启总是伴随整个会话的重启**。

**两阶段提交**。发布端的 prepared transaction 是真正的 prepared xact，持久化在 `pg_prepared_xacts` 中，数据库重启后依然存在。重启后重放时会再次输出 `BEGIN_PREPARE` / `PREPARE`，订阅端按 gid 处理，与现有行为一致。

**关系缓存**。worker 的 `RelationSyncCache` 随进程丢失，重建会话后 RELATION 消息会重新发送，订阅端覆盖缓存，语义正确。

**槽的 xmin 视野**。重启后 `MyReplicationSlot` 由新的 walsender 重新绑定，xmin 推进路径与现状一致（FR-12）。

### 17.7 恢复语义与不变量

| 不变量 | 说明 |
| --- | --- |
| `restart_lsn` 单调前进 | 由 walreader 侧的绑定表算出**最老在途事务**的 `restart_decoding_lsn`，再经 `PMSG_WATERMARK_ADVANCE` / `PMSG_WATERMARK_ACK` 交 walsender 单点推进（6.6.2）。**它的推进判据不是"收到订阅端反馈"**，而是"该 LSN 之前的所有事务都已提交并输出完毕"——订阅端反馈只决定 `confirmed_flush`，两者是两条独立的路径（见下） |
| `confirmed_flush` 单调前进 | 只反映订阅端已确认的位置（`LogicalConfirmReceivedLocation()` 无条件写入，无上界） |
| 重放范围 = 未确认区间 | 重发的内容由 `restart_lsn` 与 `confirmed_flush` 共同界定，即“订阅端尚未确认”的区间 |
| **跳过判据受控** | 重启时 `start_decoding_at = confirmed_flush`，**提交记录 LSN 小于它的事务会被整体跳过**（`decode.c:1302` → `snapbuild.c:443-446`）。因此必须保证不变式 S1（4.1.3）：已送达帧的 LSN 不得超过下一个待放行事务的提交记录 LSN |
| 至少一次语义 | 订阅端可能收到已 apply 但未确认的事务而重复 apply。这是逻辑复制的既有语义，本设计不改变它 |
| 绝不静默丢数据 | 任何不确定状态一律以终止会话暴露（第 9 章），不做静默降级 |
| 会话内自洽 | cseq 分配者、cseq 消费者、共享内存区域三者同生共死 |

**`restart_lsn` 与 `confirmed_flush` 的关系必须写清楚，否则实现时一定会混淆。** 两者回答的是不同问题：

| | `confirmed_flush` | `restart_lsn` |
| --- | --- | --- |
| 含义 | 订阅端确认已收到的位置 | 槽必须保留 WAL 的起点 |
| 谁推进 | 订阅端反馈 → `LogicalConfirmReceivedLocation()` | walreader 算出最老在途事务的起点 → walsender 调 `LogicalIncreaseRestartDecodingForSlot()` |
| 上界 | 无（订阅端报多少就是多少） | 受"最老在途事务"约束 |
| 对重放的作用 | 决定 `start_decoding_at`（跳过哪些事务） | 决定 WAL 从哪里开始被重读（`walsender.c:1506`） |

因此 §17.1 的表述（"槽水位只由订阅端反馈推进"）只对 `confirmed_flush` 成立；`restart_lsn` 有独立的推进路径（6.6.2），两者不能混为一谈。

### 17.8 实现约束清单

以下每一条都直接对应上面某一节的结论，实现时必须逐条满足：

1. **worker 不得以 0 / 1 以外的退出码退出，也不得死于信号。** 只有 0 与 1 不会触发集群级 crash recovery（`postmaster.c:2737-2739`）。worker 必须在事务边界用 `PG_TRY` / `PG_CATCH` 捕获错误，上报 `PD_ENT_ERROR` 后以 0 或 1 退出。
2. **`bgw_restart_time` 必须为 `BGW_NEVER_RESTART`。** worker 的重建只能以会话重建的形式进行（17.3）。
3. **`bgw_notify_pid` 必须设置为 walsender 的 pid。** 这是 worker 异常死亡时唯一的即时通知通道。
4. **cseq 与共享内存不持久化，且与 walreader 同生命周期。** 禁止跨会话复用 cseq（17.6）。
5. **barrier 等待必须带超时。** 防止检测机制全部失效时复制静默停滞（17.3）。
6. **worker 状态字必须放在共享内存中。** 使“谁持有哪个 cseq”在故障后仍可判定（17.3）。
7. **walreader 死亡必须立即使本会话的 cseq 作废。** 不等超时（17.4）。
8. **槽相关的操作仍只由 walsender 执行**（FR-12）。walreader 与 worker 都不得直接操作复制槽。

## 附录 A 术语表

| 术语 | 说明 |
| --- | --- |
| walreader | 发布端新增的 WAL 读取进程。WAL 的唯一读取者，负责推进 snapbuild、路由数据类 WAL、分配提交序号 |
| decode worker | 发布端新增的解码进程。持有独立的 reorder buffer 与 output plugin 实例，把 WAL 解析为协议字节 |
| walsender | 发布端主进程。持有复制槽与订阅端连接，负责启动子进程、网络收发、按提交顺序输出、推进 `confirmed_flush` |
| cseq（提交序号） | walreader 按 WAL 顺序为每个事务结束事件分配的序号，是提交顺序的唯一载体（见 7.1） |
| commit barrier | 提交水位机制。walsender 只在 `next_commit_seq` 连续可取时才写出提交消息（见 4.3、7.2） |
| FIFO 队列 | 按 WAL 提交顺序排队的输出方案。顺序更强，但存在队头阻塞，与“尽早流式”冲突（见 4.3） |
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
| v0.3 | 2026-11-18 | 明确事务与 worker 的绑定方向为多对一：新增 4.4 节，说明事务亲和性规则、事务内顺序的三重保证、与流式切分的交互及多事务并发的相互影响；同步更新 3.1、5.1、7.4、11.2、12.2、15 章与 `need.md` 的 FR/约束/验收 |
| v0.4 | 2026-11-18 | 新增第 17 章故障恢复：明确复制槽是唯一的持久化恢复锚点；给出故障分类与处置、worker 处理中途崩溃的处置与三级检测机制、walreader 与 walsender 崩溃的处置、数据库重启后的完整恢复流程、恢复语义不变量与实现约束清单；同步更新 `need.md` 的 FR/约束/验收/风险 |
| v0.5 | 2026-11-18 | **新增第 4.1 节"顺序保证模型：三层契约"**：把顺序要求显式拆成 O1 事务内序 / O2 流段 framing 序 / O3 结束事件序三层，各配不变式与派生约束 C1–C10。**第 5 章按三层契约重写**：下行载荷必须是原始 WAL 记录（含推导）、下行/上行条目与块类型、控制类消息、低拷贝链（含 `shm_mq` 的 memcpy）、唤醒与 `force_flush` 策略、流控。**第 6 章重写**：base snapshot 的分配权归 walreader（worker 侧用只读替身）、快照版本池、xid-less 失效广播、复制槽的 xmin 与 restart_lsn 两个跨进程聚合点。**第 7 章**：barrier 靠结束事件确认（可为空）推进，PREPARE 不解绑事务，`'w'` 帧 LSN 不倒退。**第 8 章**：修正"协议 v4 允许同时流式多个事务"的事实错误、段的字节上限、并行模式禁用落盘。**同步修订** 2.3 记录分类、3.1 职责表、4.2–4.5 选型、10 章改造范围、11 章 GUC 与监控、12 章性能风险、13/14 章兼容与风险；`need.md` 同步修订 G2/AC-2 为语义等价、FR-1/4/7/8/9/12/13、新增 FR-18/FR-19、约束 5/8/10、NFR-3、风险表 |
| v0.6 | 2026-11-18 | **修正 C9 与新增不变式 S1**：`'w'` 帧 LSN 链（`WalSndPrepareWrite()` 只在 `last_write` 时写帧 LSN → 订阅端取最大值 → 发布端无条件写 `confirmed_flush` → 重启时 `start_decoding_at = confirmed_flush` → 提交记录 LSN 小于它的事务被整体跳过）经源码逐环验证后，原 C9"'w' 帧 LSN 不得倒退"被证明**表述错误**（串行现状本身允许倒退，订阅端的取最大值跟踪正是为此设计）。改为不变式 **S1**："提交记录 LSN 小于 `confirmed_flush` 的事务必须已完整送达"，并据此把 C9 改写为"已送达帧的 LSN 不得超过下一个待放行事务的提交记录 LSN（放行前沿）"。**7.4 重写为三方案对比，取 (b) 数据跟随 cseq + (c) 分片帧 LSN 按放行前沿钳制**——后者堵住本设计引入的唯一一条新数据丢失路径。**新增约束 C11（受 cseq 门控的条目必须自带 cseq）与 C12（事务结束事件必须携带结束记录的 LSN 与载荷）**，并补齐 5.2.2 / 5.2.3 的载荷与门控表、6.1 的第三个 snapbuild 替身函数（`SnapBuildProcessNewCid()`）、6.5 的"广播=逐队列追加"语义。**第 3、5、9、11、12、14、15 章补齐**：子进程句柄（`BackgroundWorkerHandle`）的用途分工、共享内存尺寸上界（`MaxAllocSize` / `shm_mq_set_max_size()` / dsa 总容量）、walsender 的等待模型（长生命周期 `WaitEventSet` 同时挂 socket 与 latch，100ms 超时兜底）、GUC 变更只在下次会话生效、`bgw_notify_pid` 的 SIGUSR1 语义、风险表新增"提前流式抬高 `confirmed_flush`"一行。**15.1 新增收益测算**：单核解码上限约 8k–13k 事务/秒，N = 3~4 个 worker 即够 70MB/s——瓶颈不在解码而在 walsender 的每帧系统调用、段粒度串行下发与订阅端 apply 速率；15.2 列出 5 项待实测确认事项。`need.md` 同步：FR-19 改写、新增 FR-20（分片帧 LSN 钳制）与 AC-12（水位安全）、AC-1 口径与 G1 对齐、约束 5 改为三层、约束 13/14 新增、风险表改写 |
| v0.7 | 2026-11-18 | **确立目标部署形态为"发布端并行解码 + 订阅端并行 apply"**。新增事实 **8a**（`streaming='parallel'` 的端到端生效链条：订阅端只在 `server_version >= 160000` 时才请求 `"parallel"`（`worker.c:4372-4378`）、发布端校验协议 v4（`pgoutput.c:501-505`）、`max_parallel_apply_workers_per_subscription` 默认 2、每个流式事务在订阅端分配一个 apply worker（`applyparallelworker.c:470`））与事实 **8b**（订阅端的并行来自"多个事务的流式分片在时间上重叠"，而现状发布端几乎不产生流段 → `streaming='parallel'` 拿不到可并行的流式事务）。新增 **3.5 目标部署形态**一节与 need.md 的 **4.4 目标部署形态 / 4.5 尽早流式是主路径**。**8.1 重写**：`parallel_decode_streaming_mode` 默认由 `buffered` 改为 `immediate`（语义沿用现有 `debug_logical_replication_streaming` 的两档），并给出三条理由（喂饱订阅端并行 apply、缩小挂起区消除死锁风险面、兑现"尽早发送"的产品目标）；新增 `parallel_decode_stream_min_size` 门槛，避免小事务过度流式化带来的帧数上升与订阅端每事务一次的 apply worker 分配开销。**11.1 GUC 表、12.1 达成条件、12.3 风险点、13 章兼容性第 11 条、14 章风险表、15 章 Q6** 同步。`need.md` 同步：FR-9 改为"流式是主路径"、新增约束 15（目标部署形态）与 16（保提交序与订阅端是否并行无关）、新增 AC-13（目标部署形态）、G1 与 AC-1 补达成前提、风险表新增两行 |
