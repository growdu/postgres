# 逻辑复制并行解码 概要设计说明书

| 项目 | 内容 |
| --- | --- |
| 特性名称 | 逻辑复制发布端并行解码（parallel decoding） |
| 关联文档 | `need.md`（需求说明书）、`user_manual.md`（用户手册） |
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
    - [6.6.4 实现与验证](#664-实现与验证)
  - [6.7 关系描述的去重（RELATION 去重）](#67-关系描述的去重relation-去重)
- [7. 提交顺序保证的实现](#7-提交顺序保证的实现)
  - [7.1 提交序号（cseq）的产生](#71-提交序号cseq的产生)
  - [7.2 commit barrier 的实现](#72-commit-barrier-的实现)
  - [7.3 FIFO 方案的实现（备选/退化路径）](#73-fifo-方案的实现备选退化路径)
  - [7.4 顺序保证的边界](#74-顺序保证的边界)
    - [7.4.1 没有 cseq 的帧与放行前沿](#741-没有-cseq-的帧与放行前沿)
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

目标：tpcc 100 万 tpm 场景下逻辑复制的**协议字节速率**达到 70MB/s（以**发布端实际发出的协议字节速率**为准；**不能用** `confirm_flush_lsn` 速率——并行模型下事务数据可乱序下发，该指标会虚高，理由见 12.1 与 7.4）。

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
| 订阅端的并行 apply worker 数量 | `guc_tables.c:3263-3272` | `max_parallel_apply_workers_per_subscription`（默认 **2**，`PGC_SIGHUP`） |
| 每个流式事务分配一个 apply worker | `worker.c:1499` → `applyparallelworker.c:470-516` | `pa_allocate_worker(stream_xid)` 在 **STREAM START** 时为该事务分配一个 parallel apply worker；worker 从池中复用（`applyparallelworker.c:411-418`），但每事务都要建 dsm + 哈希项 |

**事实 8b：订阅端的并行度来自"发布端有多少事务在流式"，而不是来自 wire 的并发流段。** 事实 13 已证明 wire 上同时只有一个打开的流段。订阅端的并行是这样形成的：leader apply worker 读到 STREAM START 时为该事务分配一个 parallel apply worker 并把分片转发过去（`worker.c:1663-1670` 的 `TRANS_LEADER_SEND_TO_PARALLEL`），leader 自己继续读下一个流段。**因此"多个事务的流式分片在时间上重叠"才是订阅端并行的来源。**

而现状发布端**几乎不产生流式**：分片的触发条件是 reorder buffer 的聚合内存超限（`reorderbuffer.c:3659`），负载正常时根本不会触发。结果是：**订阅端即使配了 `streaming='parallel'` 与多个 apply worker，也拿不到可并行的流式事务**。这正是本特性必须"尽早流式"的原因（见 8.1）——不是可选的优化，而是让订阅端的并行 apply 真正能被喂饱的前提。

**事实 8c：`STREAM ABORT` 的载荷随订阅端的流式模式而变，worker 必须按"模式"而非"是否流式"初始化插件。** `pgoutput.c:1842` 的 `write_abort_info = (data->streaming == LOGICALREP_STREAM_PARALLEL)` 决定 `logicalrep_write_stream_abort()`（`proto.c:1166-1184`）写不写附加字段：

| 订阅端模式 | `STREAM ABORT` 载荷 | 字节数 |
| --- | --- | --- |
| `streaming='on'`（协议 v2/v3） | xid + subxid | 8 |
| `streaming='parallel'`（协议 v4） | xid + subxid + **abort_lsn** + **abort_time** | 24 |

因此 worker 侧必须拿到**订阅端的流式模式**——但**不是把它写进 `ctx->streaming`**。`LogicalDecodingContext.streaming` 的类型是 **bool**（`logical.h:86`），它只表示"插件支持流式且已启用"，参与判断的是 `ReorderBufferCanStartStreaming()`；**模式值（`'f'` / `'t'` / `'p'`）存在 `PGOutputData.streaming` 里**（`pgoutput.h:32`，类型是 **char**），它是 pgoutput 插件实例的私有状态，由插件选项列表里的 `streaming` 选项经 `defGetStreamingMode()` 解析得到（`pgoutput.c:366-374`，默认值 `LOGICALREP_STREAM_OFF`，见 `pg_subscription.h:163-175`）。`pgoutput.c:493-510` 的两处赋值也印证了这个分工：`ctx->streaming`（bool）被置为 true/false，而 `data->streaming`（char）承载模式。

由此得到 worker 侧插件实例化的硬要求：

- **worker 必须拿到原始的插件选项列表**（`LogicalDecodingContext.output_plugin_options`，`logical.h:59`、`logical.c:297`），而不是一个"模式值"或"是否流式"的布尔——因为 `data->streaming` 是从那份选项列表解析出来的，**没有办法只靠一个模式字符把它复原**（选项里还有 `binary`、`messages`、`two_phase`、`publish_no_origin` 等一并影响字节格式）；
- **worker 自己实例化插件**（`LoadOutputPlugin()` + `_PG_output_plugin_init`，`logical.c:755`），因而自己持有一份 `PGOutputData`，`data->streaming` 自然由同一份选项列表解析出正确的值；
- **walsender 侧必须与 worker 用同一份选项列表**——它是 `START_REPLICATION` 的选项（`cmd->options`）在 `CreateDecodingContext()` 时传进去的，因此"下发给 worker"就是下发**同一份列表**，不是另行计算一个模式值。

模式值与协议版本的校验同源（事实 8a 的 `pgoutput.c:496-509`），两者必须一起下发，否则会出现"允许流式但协议版本不够"的错配（见 5.2 的 `PD_ENT_SESSION_INIT` 与 6.1 的插件实例化）。

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
| 订阅端 | `max_parallel_apply_workers_per_subscription` **达到按目标速率反推的下限**（默认 2） | `guc_tables.c:3263-3272`；并发上限见 `launcher.c:412` | 订阅端的并行 apply 并发度不足，超出的流式事务退化成 spool 慢路径 `TRANS_LEADER_SERIALIZE`（8.1），端到端上不去 |
| 发布端 | 支持协议 v4（PG16+） | `pgoutput.c:501-505` 收到 `'p'` 且 proto_version < v4 直接报错 | 订阅端会退化为 `"on"`；发布端必须支持 v4 才能被请求 |
| 发布端 | 开启本特性 | 本设计 | 走现有串行路径 |
| 订阅端 | **不要求任何改动** | 事实 8、13 | — |

**"流尽量走 stream 发送"是这条链上的必要一环，不是可选项。** 事实 8b 已经说明：订阅端的并行 apply 靠的是"多个事务的流式分片在时间上重叠"，而现状发布端只在内存超限时才流式，正常负载下**根本不产生流段**。因此：

- 订阅端配了 `streaming='parallel'` 与多个 apply worker，如果发布端不主动流式，apply worker 是闲的；
- 反过来，发布端一旦主动流式，订阅端的 leader 就能连续为不同事务分配 apply worker，**端到端并行度才成立**；
- 这也把本设计从"发布端单方面的优化"变成"发布端与订阅端协同的方案"——发布端的流式激进程度，直接决定订阅端并行的利用率；
- **但流式激进程度不能一刀切**：非 parallel 订阅方没有 apply worker，流式化反而把它们推到 spool 慢路径（8.1）。因此流式档位按订阅端模式区分（`parallel_decode_streaming_mode = auto`）。

**由此确定三条设计取舍**：

1. **流式是主路径，整事务块是例外。** 对 `streaming='parallel'` 的订阅方，`auto` 解析为 `immediate`（尽早流式，8.1），`PD_BLK_TXN`（整事务块）只在事务未被流式化时出现。
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
| 帧头写 LSN | `walsender.c:1549-1560` `WalSndPrepareWrite()` | **只有 `last_write` 为真时才写入 `ctx->write_location`，否则写 0**。pgoutput 的逐消息实际取值见下方"帧头 LSN 清单"——**`STREAM START`、`STREAM STOP`、`STREAM ABORT`、`STREAM PREPARE` 写的都是真值，不是 0** |
| 订阅端跟踪 | `worker.c:3588-3594` | `last_received = max(last_received, start_lsn, end_lsn)`——**取最大值，不会倒退** |
| **keepalive 走同一条路** | `walsender.c:4064-4075` `WalSndKeepalive()`；`walsender.c:3443` | keepalive 的 end_lsn 取 `sentPtr`，而**逻辑解码路径下 `sentPtr = logical_decoding_ctx->reader->EndRecPtr`——发布端的 WAL 读取位置**，不是"已送达事务的 LSN"。keepalive 在 `WalSndWaitForWal()`（`walsender.c:3519`）等处发出 |
| 订阅端跟踪 keepalive | `worker.c:3608-3612` | `'k'` 消息分支同样执行 `last_received = max(last_received, end_lsn)`——**keepalive 与数据帧一样会抬高 `last_received`** |
| **keepalive 的第二个来源** | `walsender.c:1718-1723` `WalSndUpdateProgress()` → `WalSndKeepalive(false, lsn)` | **跳过空事务**时（`skipped_xact && SyncRepRequested() && 同步备库状态已定义`）会发一条 keepalive，其 end_lsn 取 `ctx->write_location`（`logical.c:741`），即**那个空事务自己的 LSN**——它属于一个**尚未放行**的事务，同样必须钳制 |
| 订阅端回报 | `worker.c:3778-3785` `get_flush_position()` | 无待 apply 事务时 `flushpos = writepos = recvpos`，即回报 `last_received` |
| 发布端落槽 | `logical.c:1836-1850`、`:1915` `LogicalConfirmReceivedLocation()` | **无条件**把 `confirmed_flush` 设为回报值，没有"不得超过已发送位置"的上限 |
| 重启跳过判据 | `logical.c:568-591` → `snapbuild.c:358` → `decode.c:1302` → `snapbuild.c:443-446` | `start_decoding_at = confirmed_flush`；**在事务的提交/回滚记录处**用 `ptr < start_decoding_at` 判断是否整体跳过该事务 |

**帧头 LSN 清单（逐消息核对；实现时按此钳制）**

| wire 消息 | 调用点 | `last_write` | 帧头 LSN（`ctx->write_location`） |
| --- | --- | --- | --- |
| `BEGIN` | `pgoutput.c:596` | `!send_replication_origin` | 事务首条变更的 LSN（`logical.c:872` → `txn->first_lsn`） |
| 每条变更 | `pgoutput.c:1553` | `true` | 该变更的 LSN（`logical.c:1540` → `change->lsn`） |
| `COMMIT` | `pgoutput.c:633` | `true` | 结束记录的**终点**（`logical.c:904` → `txn->end_lsn` = `buf->endptr`） |
| `BEGIN PREPARE` / `PREPARE` | `pgoutput.c:646` / `:664` | `!send_replication_origin` / `true` | `txn->first_lsn` / 结束记录终点（`logical.c:945` / `:990`） |
| `COMMIT PREPARED` / `ROLLBACK PREPARED` | `pgoutput.c:678` / `:694` | `true` | 结束记录终点（`logical.c:1035` / `:1081`） |
| `TRUNCATE` | `pgoutput.c:805`、`:810` | `false` | **0** |
| `M`（逻辑消息） | `pgoutput.c:1695` | `true` | 该消息的 LSN（`logical.c:1587` → `message_lsn`） |
| origin 消息 | `pgoutput.c:2427` | `true` | origin 的 LSN（`logical.c:1674`） |
| **`STREAM START`** | `pgoutput.c:1799` | `!send_replication_origin` | **事务首条变更的 LSN**（`logical.c:1315` → `first_lsn`）——**不是 0** |
| **`STREAM STOP`** | `pgoutput.c:1823` | **`true`（恒真）** | **该流段最后一条变更的 LSN**（`logical.c:1364` → `last_lsn`，即**段内 max**）——**不是 0** |
| **`STREAM ABORT`** | `pgoutput.c:1855` | **`true`（恒真）** | abort 记录的**终点**（`reorderbuffer.c:2948` 传入的是 `buf->record->EndRecPtr`，`logical.c:1406` → `abort_lsn`） |
| `STREAM PREPARE` | `pgoutput.c:1904` | `true` | 结束记录终点（`logical.c:1451`） |
| `STREAM COMMIT` | `pgoutput.c:1884` | `true` | 结束记录终点（`logical.c:1492`） |

**这张表里最危险的两行是 `STREAM STOP` 与 `STREAM START`**：前者是整段的 max，最容易越过放行前沿；后者是事务的首变更 LSN。v0.8 曾把两者都写成"写 0"，据此推导出的钳制范围会漏掉它们——**漏钳即丢事务**。

**由此得到真正的安全不变式**（不是"'w' 帧 LSN 不得倒退"）：

> **S1**：任何**结束记录起始 LSN 小于 `confirmed_flush`** 的事务，必须已经**完整送达**订阅端。
> "结束记录起始 LSN" = `buf->origptr`，与跳过判据 `SnapBuildXactNeedsSkip()`（`snapbuild.c:443-446`，比较 `ptr < start_decoding_at`）的**比较口径完全一致**。
> 因为重启后 `start_decoding_at = confirmed_flush`，结束记录起始 LSN 更小的事务会被 `DecodeTXNNeedSkip()` **整体跳过**——它的 BEGIN 与全部变更都不会重发。（重读本身是从 `restart_lsn` 开始的，`walsender.c:1506`，所以只要事务被重发，它就是完整的；危险的不是"重发得不完整"，而是"根本不重发"。）

**现状（串行）为什么自动满足 S1**：reorder buffer 按 WAL 顺序处理记录，事务按提交顺序送达。设已完整送达的事务集合为 `D`（送达序 = 提交序），`M` = 已送达帧的最大 LSN。`M` 只可能来自某个已送达事务的变更帧，其 LSN ≤ 该事务的提交记录 LSN ≤ `D` 中最后一个事务的提交记录 LSN；而任何尚未送达的事务，其提交记录必然位于 WAL 中 `M` 之后（否则它早已被 reorder buffer 处理并输出）→ 提交记录 LSN > M → 重启时不会被跳过。

**并行模型下 S1 在哪里被破坏**：本设计让**提交消息**按 cseq 放行（= 提交序），这一半与现状等价 ✔；但**流式分片提前送达**是新的破坏源——分片里变更帧的 LSN 可以大于某个"已提交但因 barrier 尚未放行"的事务的结束记录起始 LSN。于是 `M` 被抬高，越过那个事务的结束记录起点 → 重启后该事务被跳过 → **丢变更**。

例：T1 的结束记录起始 LSN = 200（cseq=1，worker 慢，提交消息尚未放行）；T2（cseq=2）已提前输出一个分片，其中变更帧 LSN = 250。则 `M = 250 > 200`，重启后 `start_decoding_at = 250`，**T1 被整体跳过**。

**口径的方向性必须钉死**：上例里若把 T1 的 LSN 取成结束记录的**终点**（`buf->endptr`，帧头里写的就是它），F 就会落在（起点, 终点] 区间内，`confirmed_flush` 可能大于起点而小于终点 → 重启后 T1 仍被跳过 → **丢变更**。因此 F 一律取**记录起点**（7.4）。

**还有第二个破坏源：keepalive。** 上表的两行说明 `'k'` 消息与 `'w'` 帧走的是同一条 `last_received` 通道，而 keepalive 的 end_lsn 取的是 `sentPtr` = **发布端的 WAL 读取位置**。串行下这个值是安全的——因为串行时"结束记录起始 LSN < `sentPtr` 的事务都已送达"（reorder buffer 按 WAL 序逐个输出，见上一段）。并行 + 尽早流式破坏的正是这条不变式：一个**已提交但因 barrier 尚未放行**的事务，其结束记录起始 LSN 可能远小于 WAL 读取位置。于是 keepalive 一发，`confirmed_flush` 就被抬到放行前沿之上 → 重启后该事务被整体跳过 → **丢变更**。

**keepalive 有两个 LSN 来源，两个都要钳**：主路径是 `sentPtr`（发布端 WAL 读取位置，`walsender.c:3443`、`:4071`）；次路径是**跳过空事务**时的 `ctx->write_location`（`walsender.c:1718-1723`），它是那个空事务自己的 LSN，属于一个尚未放行的事务。

**结论：钳制不能只做在数据帧上，必须覆盖所有携带 LSN 的 wire 消息**——`'w'` 帧头与 `'k'` keepalive 都要钳。

因此 O3 的实现必须附带一条约束：

> **C9（改写）**：wire 上任何一条携带 LSN 的帧，其 LSN **不得超过放行前沿 F**（定义见 7.4）。
> 非流式事务的整事务字节因此必须跟随 cseq 一起放行；**流式分片必须被钳制**——walsender 在转发分片帧时把帧头两个 int64 改写为 `min(帧内 LSN, F)`（详见 7.4），**逐帧钳制**，不能整块只钳一次；**keepalive 的 end_lsn 必须同样钳制**，它的两个来源（`sentPtr` = 发布端 WAL 读取位置；跳过空事务时的 `ctx->write_location`）都要钳。
>
> **钳制用的是 F，而 F 必须取结束记录的"起点"**（`buf->origptr`），因为跳过判据 `SnapBuildXactNeedsSkip()` 比较的就是记录起点。**帧头里写的 `txn->end_lsn` 是记录终点**（`buf->endptr`）——两者不可混用：用终点做 F 会让 `confirmed_flush` 落在（起点, 终点] 区间内，重启后该事务被整体跳过（丢变更）。

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
| C9 | 已送达帧的 LSN 不得超过"下一个待放行事务的提交记录 LSN"（不变式 **S1**，见 4.1.3）；因此非流式事务的整事务字节跟随 cseq 放行，**流式分片的帧 LSN 与 keepalive 的 end_lsn 都必须由 walsender 钳制到放行前沿** | O3 + 7.4 |
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
| walsender 负载 | 按帧转发 + 顺序控制 + **FR-20 逐帧钳制**（12.3） | 网络发送 + 协议格式化 | 同 A |
| 串行路径上的拷贝 | 共享内存 → socket（1 次） | 共享内存 → reorder buffer → 插件缓冲 → socket（3 次） | 同 A |
| 与现状差异 | reorder buffer 与插件实例化逻辑需按 worker 复制 | reorder buffer/plugin 基本不动 | 关系消息去重 |
| 风险 | 插件状态按 worker 复制；RELATION 消息重复 | **插件格式化留在单线程，热点无法并行化** | 实现复杂度最高 |

**选型：方案 A 为主；关系消息去重作为独立的增强项单独实现（6.7）。** 注意 6.7 的最终形态既不是"共享关系表"也不是"walreader 统一下发"，而是**由 walsender 按放行序做去重判定**——因为只有 walsender 掌握 cseq 序，worker 之间无法安全地互相判定（6.7.1）。

理由：

1. **CPU 热点在插件格式化，不在拷贝。** 事实 12：output plugin 的回调不是在读 WAL 时发生，而是在变更从 reorder buffer **回放**时发生（提交路径 `reorderbuffer.c:2109`、流式路径 `reorderbuffer.c:4055`），热点是插件对每列的 syscache 查类型与类型输出函数调用（`logical/proto.c:828-858`）。方案 B 只把 tuple 解码搬到 worker，**这段热点仍完整地留在单线程的 walsender 上**，无法达成 70MB/s 目标；方案 A 把“解码 + 插件格式化”整体并行化，walsender 只剩**按帧转发**（每帧一次 `write()` 系统调用 + FR-20 的逐帧钳制，见 12.3），CPU 分布最均衡。
2. **插件状态必须在同一进程内闭环。** `txn->output_plugin_private` 是 output plugin 的私有状态，跨进程无法共享。方案 A 让 begin → changes → commit 全部在同一个 worker 内走完，插件状态天然自洽；方案 B 要么在 worker 与 walsender 之间传递插件私有状态，要么把格式化退回 walsender，两者都不可取。
3. **方案 A 的串行路径拷贝最少**（共享内存 → socket 一次），符合“调度不成为新瓶颈”的要求；方案 B 多出 reorder buffer 与插件缓冲两次往返。
4. 方案 B 的优点（改动小）可以通过“把 reorder buffer 与插件实例化到 worker、walsender 只保留发送职责”来达成，而不必保留 reorder buffer 在 walsender。

方案 A 的已知代价及处理：

- **RELATION 消息重复**（事实 6）：每个 worker 有独立的 `RelationSyncCache`。按 `pgoutput.c:704-770` 的两级判定拆开看，**放大只发生在非流式事务路径**：流式事务按 `(relation, top-xid)` 粒度判断是否重发，而一个 top-xid 只会归属一个 worker，粒度与事务归属一致，**不额外放大**；非流式事务用的是会话级 `schema_sent` 标志，每个 worker 各有一份，因此同一关系最多被发 N 次。订阅端收到重复的 RELATION 会覆盖缓存，语义正确，代价是少量冗余带宽。**已解决**（6.7）：去重判定放在 walsender 侧，由它按 cseq 放行序判定——同一关系每个 epoch 恰好发一条。注意其收益主要是消除上述 N 倍放大，而非消除流式路径的重复。
- **插件实例化**：每个 worker 需要独立的 `LogicalDecodingContext` 与 output plugin 实例（`LoadOutputPlugin()` + `_PG_output_plugin_init`），各自持有 `ctx->out` 缓冲。**实例化时必须把 `ctx->streaming` 设成订阅端的模式值**（事实 8c）：它同时决定"是否允许流式"与"`STREAM ABORT` 的载荷格式"，由 `PD_ENT_SESSION_INIT` 下发（5.2.2）。

### 4.3 决策二：提交顺序保证——commit barrier 与 FIFO 队列对比

这是本设计的核心决策。两种候选方案：

**方案 1：commit barrier（提交水位）**

- walreader 按 WAL 顺序为每个事务结束事件（commit / abort / cancel）分配单调递增的提交序号 `cseq`；
- walsender 维护 `next_commit_seq` 水位。收到 `cseq = k` 的结束事件确认时，若 `k != next_commit_seq`，说明前面还有未发出的结束事件，将其挂入有序待提交表；只有当待提交表中 `next_commit_seq` 连续可取时，才依次写出并推进水位；
- **没有 cseq 的帧不占提交序号，但仍要过放行前沿**：流式分片与子事务 abort 的 `STREAM ABORT` 都属于事务内部，它们不占 cseq，但**不得越过"在它们之前就已经结束"的事务的结束事件**——否则订阅端会把两个在发布端被行锁串行化的事务并发 apply，报出发布端没有的 deadlock（见 7.4.1、7.1）；**非流式事务的整事务字节与其提交消息一起按 cseq 放行**——否则 `'w'` 帧的 LSN 会倒退，`confirmed_flush` 的语义被破坏（见 7.4 末段）。

**方案 2：FIFO 队列（按 WAL 读到的提交顺序排队）**

- walreader 把事务结束事件（连同其已解码的字节块）压入一条单一 FIFO，FIFO 按 cseq 排序；
- walsender 严格按 FIFO 顺序取出发送。

**对比：**

| 维度 | commit barrier | FIFO 队列 |
| --- | --- | --- |
| 顺序保证强度 | 强：commit/abort/cancel 严格按 cseq | 强：且事务数据也严格按 cseq，强度更高 |
| 对“尽早流式”的影响 | **近乎无影响**：流式分片只被**已经结束**的事务挡住（7.4.1），不被尚未结束的事务挡住 | **有影响**：队头事务未解码完成时，队头阻塞，其后已解码的数据全部不能发 |
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
2. 用户需求明确要求“事务 stream 的时候要尽快发送来提升性能”。barrier 几乎不阻碍**流式分片**提前下发——分片只被**已经结束**的事务挡住（7.4.1），一个还在解码的大事务挡不住别的分片；FIFO 与该要求直接冲突——只要有一个事务解码慢，FIFO 就会把后面所有已解码的数据一起堵住。
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
| 控制通道 | walreader ↔ walsender | | SPSC | O3 | cseq 分配、槽水位上报（6.6）、ready / flush 回执 |
| 错误通道 | worker #i | walsender | 共享状态字 + `bgw_notify_pid` 通知 | — | worker 内部错误（FR-11） |

**没有"关系表"这一块共享内存。** 关系描述的去重（6.7）最终落在 **walsender 进程内**的一张哈希表里：walsender 是唯一掌握放行顺序（cseq 序）的进程，只有它能安全地判定"某条关系描述是否已经送达订阅端"。放在共享内存里让 worker 自行判定是**不安全**的——worker 之间没有 cseq 序的先后关系，判定依据不成立（理由见 6.7）。

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
| `PD_ENT_SESSION_INIT` | 会话建立时（walreader 启动后、转发任何 WAL 记录之前） | **原始插件选项列表**（`LogicalDecodingContext.output_plugin_options` 的同一份拷贝，见事实 8c——worker 必须靠它解析出 `PGOutputData.streaming` 的模式值）、协商后的协议版本、复制槽名 |
| `PD_ENT_WAL` | 一批属于该 worker 的事务的 WAL 记录 | 记录数 + 每条记录（见下） |
| `PD_ENT_SNAPSHOT` | 事务首条记录之前 / 目录事务提交时 | 快照版本号 + 随附的失效消息 |
| `PD_ENT_GLOBAL_INVAL` | `XLOG_XACT_INVALIDATIONS` 且 xid 无效 | 失效消息数组（广播到所有 worker，见 6.5） |
| `PD_ENT_TXN_END` | 读到**顶层**事务结束记录 / 检测到 cancel | xid、结束类型、**cseq**、**结束记录的 LSN 与载荷**（见下） |
| `PD_ENT_SUBXACT_ABORT` | 读到**子事务**的 abort 记录 | 顶层 xid、**子 xid**、abort 记录的起始 LSN、abort 时间（见下） |
| `PD_ENT_MARKER` | walreader 每读满 `PD_POSITION_MARK_RECORDS` 条记录、或 WAL 暂时读完时 | walreader 的读取位置。记录按事务拥有者路由后，不拥有任何事务的 worker 看不到任何记录，这个位置是它"没有落下任何记录"的唯一凭据（见 16.1 的实现事实） |
| `PD_ENT_FLUSH` | 发送方在挂起之前 | **无载荷**（长度为 0）。显式刷新队列：批次未满时数据已经躺在环形缓冲里但**尚未发布**，接收方看不到它（5.4 第 9 项）。`shm_mq` 没有 notify API，刷新只能表达为一条消息，接收方读到后忽略 |

**`PD_ENT_SESSION_INIT` 必须排在所有其它条目之前。** worker 的 output plugin 实例（6.1）在实例化时就要确定两件事：**是否允许流式**（`ctx->streaming`，bool；参与 `ReorderBufferCanStartStreaming()`，`reorderbuffer.c:4028-4047`）与**`STREAM ABORT` 的载荷格式**（由 `PGOutputData.streaming` 的**模式值**决定，事实 8c）。这两项都只由订阅端的 `START_REPLICATION` 选项决定，worker 自己无从得知，因此必须由 walsender 在会话建立时统一下发，且早于任何 `PD_ENT_WAL`（否则 worker 可能已经按错误的模式产出了字节）。

**下发的必须是原始选项列表，而不是加工过的模式值。** `data->streaming` 是 pgoutput 从选项列表里解析出来的（`pgoutput.c:366-374`），只下发一个模式字符无法复原它，也无法表达同一份选项里的其它开关（`binary` / `messages` / `two_phase` / `publish_no_origin`）。因此 `PD_ENT_SESSION_INIT` 携带**与 walsender 自己那份完全相同的选项列表**，worker 用它实例化自己的插件实例。

**子事务 abort 是另一类事件，必须走单独的条目——它不是顶层事务的收尾。**

`ROLLBACK TO SAVEPOINT` 会写一条**独立的** `XLOG_XACT_ABORT` 记录，其 `xl_xid` 是**子 xid**而不是顶层 xid（`xloginsert.c:926` 的 `rechdr->xl_xid = GetCurrentTransactionIdIfAny()` 在子事务里返回子 xid；`xact.c:1725`、`:1781-1793` 的 `RecordTransactionAbort(isSubXact = true)`）。解码侧对两种 abort 记录的处理是不同的：

| abort 记录 | `xl_xid` | 记录内容 | 解码侧动作 |
| --- | --- | --- | --- |
| 顶层 abort | 顶层 xid | 带**被中止的子 xact 数组**（`xactdesc.c:141-185` 的 `parsed->subxacts[]`） | `decode.c:862-872`：先**逐个** abort 子 xact（每个都可能产出自己的 `STREAM ABORT`），**最后**才 abort 顶层 |
| 子事务 abort | 子 xid | 无子 xact 数组 | `decode.c:874-875`：只 abort 该子 xact |

**若把子事务 abort 当成顶层事务的结束事件处理，会静默丢数据。** 订阅端对 `STREAM ABORT` 的落地动作是**截断 spool 文件**：`worker.c:1791-1793` `BufFileTruncateFileSet(fd, subxact_data.subxacts[subidx].fileno, subxact_data.subxacts[subidx].offset)`，紧接着 `worker.c:1796` `subxact_data.nsubxacts = subidx;`——**连同其后已写入 spool 的子事务变更一起丢掉**。因此：

- `STREAM ABORT` **不能推迟到顶层提交时再下发**。若推迟，截断发生时，其后子事务的有效变更已经被写入 spool 并被一并丢弃 → **丢数据**；
- `STREAM ABORT` **也不能走普通流段块**——事实 13 规定 wire 上同时只能有一个打开的流段，订阅端在 `STREAM ABORT` 到达时若仍有打开的流段，会 `ereport(ERROR, "STREAM ABORT message without STREAM STOP")`（`worker.c:1824-1827`）→ 会话失败。

因此子事务 abort 的下发规则是：

1. **不分配 cseq**——它属于一个尚未提交的事务，没有提交序可言；**但它仍要过放行前沿**：它的位置不得越过"在它之前就已经结束"的事务的结束事件（见 7.4.1）。这一点与"即时下发"不冲突：放行前沿只会挡住它很短一段时间，而挡住它的时间不会影响"在后续子事务的变更到达订阅端之前"这一条；
2. **必须由持有该事务的 worker 在 WAL 顺序位置即时产出**，即在后续子事务的变更到达订阅端**之前**；
3. **独占一块**（新增块类型 `PD_BLK_ABORT`，见 5.2.3），**wire 上连续**；
4. 载荷：`toplevel_xid` / `subxid` / `abort_lsn` / `abort_time`，与 pgoutput 的 `logicalrep_write_stream_abort()`（`proto.c:1166-1184`）的字段一一对应——**worker 必须自己构造这段协议字节**，因为它不在任何一条 `PD_ENT_WAL` 记录里（结束记录按 5.2.2 的筛选规则被排除在 `PD_ENT_WAL` 之外）。

**顶层 abort 可能连带产出多条 `STREAM ABORT`。** 顶层 abort 记录里的每个子 xact 都可能被中止，且每个被中止的子 xact 若已流式化就会产出自己的 `STREAM ABORT`（`reorderbuffer.c:2945-2951` 的 `rbtxn_is_streamed(txn)`），最后才是顶层自己的那条。**这 N+1 条都属于同一个顶层 abort 的收尾**，但它们的门控归属不同：子 xact 的那些**不占 cseq、只过放行前沿**（即时产出，放行前沿放行后即下发），顶层那一条**占该顶层 abort 的 cseq、受 cseq 门控**（随 barrier 放行）。

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
| `PD_ENT_BLOCK` | 块边界标记（FIRST / MORE / LAST）、块类型、xid、**cseq（仅整事务块有）**、字节 | O2 的载体：块内字节在 wire 上必须连续。**实现没有用它**：块边界由消息边界表达（见本节末尾），枚举值保留为预留 |
| `PD_ENT_SEGMENT` | 一个流段的全部字节（帧之间用 `uint32` 长度前缀分隔） | 流段的载体，对应下表的 `PD_BLK_SEG` |
| `PD_ENT_FRAME` | 一帧的字节（头里带 cseq、end_lsn） | 非流式事务的一帧（`PD_BLK_TXN`）与子事务 abort（`PD_BLK_ABORT`）；**cseq == 0 表示这一帧没有提交序**，受放行前沿约束 |
| `PD_ENT_EVENT` | cseq、结束类型、字节（**可为空**）、xid | O3 的载体：事务结束事件 |
| `PD_ENT_ERROR` | 错误信息 | worker 内部错误（FR-11） |
| `PD_ENT_RELATION` | 关系描述帧（relid、cseq、epoch、字节） | 携带关系描述的帧，walsender 据此做去重（6.7）。**单独一种条目类型**，因此普通帧不必为它额外付出头部开销 |
| `PD_ENT_FLUSH` | **无载荷**（长度为 0） | 显式刷新队列，同 5.2.2。worker 在挂起之前发它，walsender 读到后忽略 |

**块类型**有两种，对应订阅端的两条独立约束：

| 块类型 | 内容 | 连续性约束来源 |
| --- | --- | --- |
| `PD_BLK_SEG` | 一个流段的字节：`STREAM_START` … `STREAM_STOP` | O2 / INV-2.1、INV-2.2（事实 13） |
| `PD_BLK_TXN` | 一个**非流式**事务的一帧（实现上一帧一条消息） | 订阅端的 `in_remote_transaction` 状态（`worker.c:1006` / `:1035`）：`BEGIN` 与 `COMMIT` 之间不得插入别的字节，否则 `remote_final_lsn` 被覆盖，后续 `COMMIT` 会触发 `ereport(ERROR, "incorrect commit LSN ...")` |
| `PD_BLK_ABORT` | 一条子事务 abort 的字节：`STREAM ABORT`（顶层 xid + 子 xid） | 事实 13（wire 上同时只能有一个打开的流段）与订阅端的截断语义（`worker.c:1824-1827`）：`STREAM ABORT` 到达时**不得有打开的流段**，因此它独占一块 |

两者都必须**整块连续**出现在 wire 上。实现用**消息边界**表达块边界（一条消息 = 一块，见本节末尾），walsender 的规则完全一致：**一次取一整块，块未取完不得取其它 worker 的块**（C5）。

`PD_ENT_EVENT` 的字节**可为空**：非流式事务的 abort / cancel 不产生任何 wire 消息（`reorderbuffer.c:2946`），但 cseq 水位仍必须推进——因此空事件也要回执（C3，见 7.2）。

**结束事件与数据块共用同一条上行队列**：walsender 读到事件条目时，若 barrier 未放行，则把它**挂起**在按 cseq 排序的待提交表里并继续从同一条队列取下一条。这样既保证“同一事务的所有块都先于它的结束事件下发”（同一队列 FIFO），又不会让 barrier 阻塞数据路径。若把结束事件放在独立队列，就无法保证这个先后关系。

**块与 cseq 的两种组合**：

| 事务形态 | 数据 | 结束事件 |
| --- | --- | --- |
| 被流式（分段下发） | 若干 `PD_BLK_SEG`，解码过程中陆续产出，**不带 cseq**——此时事务尚未提交，cseq 还不存在 | `PD_ENT_EVENT`，携带 cseq，载荷是 `STREAM_COMMIT` / `STREAM_ABORT` 字节；另有一条 `END_ACK` |
| 未被流式（整事务下发） | 若干条 `PD_ENT_FRAME`（一条变更一条消息），在 `PD_ENT_TXN_END` 到达时陆续产出，**携带该 cseq** | 同样有 `END_ACK`（字节可为空） |

**并行解码路径下每个事务都是流段。** `immediate` 档把 `stream_all_transactions` 打开：进行中攒够 `parallel_decode_stream_segment_size` 的事务照常切段下发，**没攒够的在提交时作为一个流段发出**（8.1）。因此上表的第一行是并行解码路径的热路径，第二行只在 `buffered` 档（`streaming='on'`）与串行路径上出现。

**携带 cseq 的条目受 cseq 门控；不携带 cseq 的条目受放行前沿约束**（见 7.4.1）：

| 条目 | 是否受 cseq 门控 | 是否受放行前沿约束 | 说明 |
| --- | --- | --- | --- |
| `PD_BLK_SEG` | 否 | **是** | 流段内部连续即可，但它**不得越过已经结束的事务的结束事件**——否则订阅端会并发 apply 两个本该被串行化的事务（7.4.1）；C9 只约束它携带的帧 LSN（见 7.4） |
| `PD_BLK_TXN` | **是** | 否（随 cseq 放行即自动满足） | 块内的 `COMMIT` 必须与更早的 cseq 保持相对顺序 |
| `PD_ENT_EVENT`（STREAM_COMMIT / STREAM_ABORT） | **是** | 否（同上） | 同上 |
| `PD_ENT_EVENT{END_ACK}` | **是** | 否 | 它本身就是 barrier 的推进凭据 |
| `PD_BLK_ABORT`（子事务 abort） | 否 | **是** | 它属于一个**尚未提交**的事务，没有提交序可言；但同样不得越过已经结束的事务的结束事件。**必须及时下发**，否则订阅端的 spool 截断会连带丢弃其后子事务的有效变更（见 5.2.2） |

因此 `PD_BLK_TXN` 必须**自带 cseq**（C11）。walsender 不能靠 `PMSG_COMMIT_SEQ` 来关联它——那是 walreader → walsender 的另一条通道，与 worker 的输出队列之间没有先后关系，两条通道的消息到达顺序不可比。实现里这个 cseq 的载体是 `PD_ENT_FRAME` 的头。

第二种形态下，`PD_BLK_TXN` 整体要等自己的 cseq 被放行才能下发。它自带 cseq，walsender 直接把它挂进按 cseq 排序的挂起表——与 7.2 的 `pending_commits` 是同一张表，不需要额外的关联机制。

**块边界由消息边界表达。** 实现不引入 FIRST / MORE / LAST 标记：**一块 = 一条队列消息**。消息载荷超过队列槽位时由 chunked 传输拆分，并在接收方由 `PDChunkReceiverRecv()` 重组完之后才交给上层——重组期间该消息独占接收缓冲，因此"块内字节在 wire 上连续"由消息边界保证，不需要额外的边界标记。

**消息类型属于整条传输，而不是某一个队列条目。** 接收方可能在传输尚未完成时就把队列读空（`ParallelDecodeNextFrameNoWait()` 会这样做），此时传输状态必须留在 `PDChunkReceiver` 里：类型在首块被接收时记下，在传输完成时交给上层。类型若留在调用方的局部变量里，完成传输的那一次调用就不会写它，上层拿到的是上一次调用留下的值——一条 8 MB 的流段会被当成别种消息处理，其载荷被静默丢弃。

#### 5.2.4 控制类消息

**walreader → walsender**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_COMMIT_SEQ` | xid、cseq、结束类型 | 提交顺序的来源（7.1）；把 cseq 与 xid 关联起来 |
| `PMSG_WATERMARK_ADVANCE` | lsn、新的 xmin、新的 restart_lsn | 复制槽的两个水位推进请求（6.6）。**已实现**：消息体是 `PDWatermark{restart_lsn, xmin, oldest_running, read_lsn}`，由 walreader 在读到 `XLOG_RUNNING_XACTS` 记录时发出 |
| `PMSG_READY` | — | snapbuild 达到 consistent |

**walsender → walreader**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PMSG_WATERMARK_ACK` | lsn、xmin、restart_lsn | walsender 代为调用 `LogicalIncreaseXminForSlot()` / `LogicalIncreaseRestartDecodingForSlot()` 后回执。**已实现：不需要回执**——"等订阅端确认"这一步由槽自己的候选机制承担（`candidate_xmin_lsn` / `candidate_restart_valid`，`logical.c:1757+`），walsender 收到水位消息即可调用那两个函数 |
| `PMSG_FLUSH_LSN` | confirmed_flush | 复制槽确认水位回传 |

**worker → walreader**

| 消息 | 载荷 | 说明 |
| --- | --- | --- |
| `PD_ENT_EVENT{ END_ACK }` | cseq | 事务结束事件已处理完毕（7.2） |
| `PD_ENT_EVENT{ WATERMARK }` | xmin、oldest_first_lsn、seq | 周期上报本 worker 的快照 xmin（6.6.1）。**已实现：worker 不上报**——walreader 自己保留了一份记录全部事务的 reorder buffer，全局的"最老在途事务"对它可见，水位由它算（6.6） |

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

**这条目标已经达成**（块化之后，见 5.2.3 与 `send_bottleneck.md`），实际链路是：

```text
worker:     ctx->out --memcpy--> pd_segment --memcpy--> shm_mq 环
walsender:  环 --memcpy--> 接收缓冲 --(接管，不拷贝)--> 块缓冲 --memcpy--> socket
```

即 **walsender 侧只剩一次拷贝**（块 → socket）。

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
- **输出侧不需要额外拷贝**：worker 把插件产出的字节拼成一段（一段 = 一条队列消息），walsender 取出后**接管**那块缓冲，帧的字节直接指向它，逐帧写进 socket 缓冲。整块连续性由“消息边界 = 块边界”保证，而不是靠拷贝成一个连续大块。
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

   **已实现（v0.21）**。`parallel_decode_batch_size` 已接线，实现要点：

   | 要点 | 实现 |
   | --- | --- |
   | 批的大小 | `parallel_decode_batch_size`（默认 64KB），并**钳制在队列的 1/4** 以内（`PARALLEL_DECODE_QUEUE_SIZE / 4`）。理由：`shm_mq` 在累计写入量达到环的 1/4 时自己会发布并唤醒接收方；批大于这个值时发送方会等待"接收方腾不出空间"，而接收方恰恰看不到尚未发布的数据，构成死锁 |
   | 发送方记账 | 每个发送方（`PDQueueSender`）记"上次刷新以来写了多少字节"。写消息时先把字节数记进去，记满一批就把这条消息的 `force_flush` 置真，于是这一批的数据连同这条消息一起被发布，接收方被唤醒**一次** |
   | 显式刷新 | `shm_mq` 没有 notify API，刷新只能表达为**一条消息**：新增 `PD_ENT_FLUSH`，一条长度为 0 的分块消息，接收方读到后忽略。发送方在挂起前发它——否则接收方会对着一条已经躺在环形缓冲里、却尚未发布的数据空等 |
   | 批量的是"数据"，不是"进度" | 承载进度的消息**不批量**，每次单独刷新：`PD_ENT_POSITION`（停滞检测依赖它）、`PD_ENT_EVENT`（事务结束事件，walsender 等它才能放行后续事务）、walreader 的广播与定向控制消息（快照、失效、水位）。这些是"多久"的信号，延迟它们会直接抬高复制延迟与停滞检测的误报率 |
   | walreader 的批是**跨队列**的 | walreader 的批次按**所有 worker 队列的字节总和**计。否则一个不持有任何事务、只收到 marker 的 worker，它的 8 字节 marker 要攒到 64KB 才被刷新一次，位置心跳会变得很稀疏 |
   | 关闭开关 | `parallel_decode_batch_size = 0` 关闭批处理：每条记录、每帧都单独刷新，行为与接线前完全一致（AC-8） |

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
| `ReorderBufferCanStartStreaming()` | `reorderbuffer.c:4028-4047` | 上述状态 + `ctx->streaming`（**bool**）+ `ctx->reader->ReadRecPtr` | 状态块 + `ctx->streaming`（worker 侧常量，**取 true**——见事实 8c：它是 bool，不承载模式）+ 本地记录 LSN |
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

复制槽上有两个必须由跨进程数据算出的水位。二者都源于**事务分散在多个 worker** 这一变化，都必须显式设计聚合路径。

**已落地的形态：walreader 侧自己保留一份 reorder buffer。** walreader 的 `pd_rb` 记录会话中**所有**事务的条目（建立、base snapshot、提交、丢弃），因此"最老在途事务"这个全局事实对 walreader 是可见的——`ReorderBufferGetOldestXmin(pd_rb)` 与 `ReorderBufferGetOldestTXN(pd_rb)` 取到的，正是串行路径在同一位置会取到的值。walreader 在读到 `XLOG_RUNNING_XACTS` 记录时算出这两个值，通过控制通道的 `PD_ENT_WATERMARK` 消息交给 walsender；**walsender 是槽的唯一拥有者**（事实 9），由它调用 `LogicalIncreaseXminForSlot()` / `LogicalIncreaseRestartDecodingForSlot()`。

这一形态的好处是：不需要给 worker 增加"周期上报 + 超时兜底"的两套规则，算出来的值与串行路径**同口径**，而且串行路径算这两个值用的就是同一对函数（`SnapBuildProcessRunningXacts()`）。

#### 6.6.1 xmin 水位

`ReorderBufferGetOldestXmin()`（`reorderbuffer.c:1047-1061`）取 `rb->txns_by_base_snapshot_lsn` 的表头作为最老事务的快照 xmin，串行路径在 `snapbuild.c:1498-1503` 用它调用 `LogicalIncreaseXminForSlot()`。

**并行路径下这个值退化的问题不成立**，前提是 walreader 自己的 reorder buffer 里也有这些事务的条目：`ReorderBufferProcessXid(pd_rb, xid, lsn)` 为每个事务建立条目（条目上带 `restart_decoding_lsn`），事务结束时 `ReorderBufferForget(pd_rb, xid, lsn)` 丢弃它，base snapshot 由快照构建器经 `ReorderBufferSetBaseSnapshot()` 设置到同一条目上。

**口径必须与串行路径一致**：串行路径的"最老在途事务"指的是**持有 base snapshot 的最老事务**，不是所有在途事务。`pd_rb` 与串行路径的 reorder buffer 承担同样的角色，因此 `ReorderBufferGetOldestXmin(pd_rb)` 与串行路径的取值口径相同。

**兜底值仍然保留**：`ReorderBufferGetOldestXmin()` 返回 `InvalidTransactionId` 时（没有任何事务持有 base snapshot），退回 `running->oldestRunningXid`——与串行路径 `snapbuild.c:1498-1501` 完全一致。

**已经排除的替代方案**：让每个 worker 周期上报自己持有的最老事务的快照 xmin，由 walreader 取最小值。那套方案必须额外处理"某个 worker 超时未上报"的兜底，而兜底值比真实值更激进还是更保守，直接决定目录元组会不会被过早回收（回收后订阅端会报 `catalog lookup failed`）——这类规则比"直接复用串行路径的取值"更难验证，因此不采用。

#### 6.6.2 restart_lsn 水位

`restart_lsn` 的语义是"必须从这里开始重读，才能重放所有尚未提交的事务"（`logical.c:1757-1759`）。它由**最老在途事务**的 `restart_decoding_lsn` 钉住，串行路径的算法是（`snapbuild.c:1524-1541`）：

```c
/* snapbuild.c:1524-1541 (串行路径) */
txn = ReorderBufferGetOldestTXN(builder->reorder);        /* reorderbuffer.c:1021-1031 */
if (txn != NULL && txn->restart_decoding_lsn != InvalidXLogRecPtr)
    LogicalIncreaseRestartDecodingForSlot(lsn, txn->restart_decoding_lsn);
else if (txn == NULL &&
         builder->reorder->current_restart_decoding_lsn != InvalidXLogRecPtr &&
         builder->last_serialized_snapshot != InvalidXLogRecPtr)
    LogicalIncreaseRestartDecodingForSlot(lsn,
                                          builder->last_serialized_snapshot);
```

其中 `txn->restart_decoding_lsn = rb->current_restart_decoding_lsn`（`reorderbuffer.c:688`），而 `current_restart_decoding_lsn` 由快照构建器经 `ReorderBufferSetRestartPoint()` 写入（`snapbuild.c:2074`、`snapbuild.c:2238`）。

**这里曾被认为有一个必须堵住的漏洞**：如果 walreader 不持有事务，`ReorderBufferGetOldestTXN()` 恒返回 NULL，就必然走 else 分支，把 `restart_lsn` 推到 `builder->last_serialized_snapshot`——而后者每次 `XLOG_RUNNING_XACTS` 都会前移（约每 10 秒一次），于是 `restart_lsn` 会周期性地跳过所有仍在 worker 里在途的事务，发布端重启后这些事务的前半段变更永久丢失。

**该漏洞的前提是"事务只在 worker 的 reorder buffer 里"，而实现否定了这个前提**：walreader 侧有自己的 `pd_rb`，它同样为每个事务建立条目、同样在 `ReorderBufferProcessXid()` 时记下当时的 `current_restart_decoding_lsn`、同样在事务结束时丢弃条目。因此 `ReorderBufferGetOldestTXN(pd_rb)` 取到的就是全局最老在途事务，与串行路径同口径；`current_restart_decoding_lsn` 只在"快照已落盘"或"快照已从盘上恢复"这两个位置被设置（`snapbuild.c:2074`、`snapbuild.c:2238`），因此它作为重启位置是安全的。

**推进判据沿用串行路径的候选机制，不做替换。** `LogicalIncreaseRestartDecodingForSlot(current_lsn, restart_lsn)` 把候选记在 `candidate_restart_valid = current_lsn` 上，等订阅端确认到 `current_lsn` 之后才生效（`logical.c:1825-1899` 的候选设置、`logical.c:1901+` 的 `LogicalConfirmReceivedLocation()` 应用）。并行路径下 `current_lsn` 取 **running-xacts 记录自身的 LSN**，这条判据仍然成立：

- 候选的生效条件是 `confirmed_flush >= current_lsn`；
- `confirmed_flush` 受放行前沿 F 钳制，而 F 在所有已报告事务放行之后等于 **walreader 认证过的读取位置**（7.4），它必然越过 running-xacts 记录的 LSN——因为该记录之前的全部事务都已放行，其帧已写给订阅端；
- 因此候选会在"running-xacts 记录之前的全部事务都已送达"之后生效，**不会**出现"候选永远挂着、WAL 无限保留"的情况。

**这条判据成立的前提是 F 随 walreader 的读取前进。** 若 walreader 停在某个位置不再读 WAL，则 F 不前进、候选不生效、`restart_lsn` 停滞——但此时本就没有新 WAL 产生，语义正确。

#### 6.6.3 汇总消息

两个水位共用一条消息，走 walreader → walsender 的控制通道：

```c
typedef struct PDWatermark
{
    uint64      restart_lsn;     /* 解码可以从哪里重启；0 表示还不知道 */
    TransactionId xmin;          /* 目录的 xmin 水位 */
    TransactionId oldest_running;/* running-xacts 记录里的最老运行事务 */
    uint64      read_lsn;        /* 这两个值成立的位置（running-xacts 记录的 LSN） */
} PDWatermark;
```

消息在 walreader 读到 `XLOG_RUNNING_XACTS` 记录时发出（`pd_walreader_send_watermark()`），walsender 在排空控制通道时取出并应用（`parallel_decode_drain_control()` 的 `PD_ENT_WATERMARK` 分支）：

```c
if (TransactionIdIsValid(wm.xmin))
    LogicalIncreaseXminForSlot((XLogRecPtr) wm.read_lsn, wm.xmin);
if (wm.restart_lsn != 0)
    LogicalIncreaseRestartDecodingForSlot((XLogRecPtr) wm.read_lsn,
                                          (XLogRecPtr) wm.restart_lsn);
```

**walsender 是槽的唯一拥有者**（事实 9），只有它能推进槽的水位；walreader 与 worker 拿到的都是槽的副本。

#### 6.6.4 实现与验证

| 事项 | 结论 |
| --- | --- |
| 聚合点 | walreader 自己的 reorder buffer `pd_rb`，不是 worker 的上报 |
| 汇总时机 | walreader 读到 `XLOG_RUNNING_XACTS` 记录时（与串行路径同一个时机、同一个周期） |
| 传输方式 | 控制通道的 `PD_ENT_WATERMARK` 消息 |
| 应用者 | walsender（槽的唯一拥有者），调用串行路径同一对 `LogicalIncrease*ForSlot()` |
| 候选的生效条件 | 沿用串行路径：`confirmed_flush >= running-xacts 记录的 LSN` |
| 实测（旧代码 → 新代码） | 并行路径下槽的 `restart_lsn` **完全不前进**（90 秒内不变）→ 前进（14 秒后从 `0/14E5430` 变为 `0/14E8728`），`catalog_xmin` 同步前进（740 → 741），与串行路径的实测结果一致 |

验证脚本：`doc/pa_decode/t/slot_watermark_smoke.sh`（建订阅、写负载、等槽的水位前进，并与串行路径对照）。

### 6.7 关系描述的去重（RELATION 去重）

**问题。** 输出插件把"这张表长什么样"（列名、列类型、replident）告诉订阅端，订阅端据此维护自己的关系缓存。串行路径只有一个解码上下文、一份缓存，因此每张表的关系描述在整个会话里只发一次（`pgoutput.c` 的 `relentry->schema_sent`）。

并行路径每个 decode worker 各有一份插件上下文、各有一份关系缓存，于是**每张表的关系描述会被每个碰过它的 worker 各发一次**。实测（6 个并发会话、48 个事务的单表负载）：串行路径发 1 条 RELATION，并行路径发 4 条（4 个 worker 各一份）。协议上这是**允许**的——订阅端收到重复的关系描述只是覆盖自己的缓存——但它是纯粹的浪费：一条关系描述约 100–200 字节，在"每事务都要发一遍"的负载下会显著放大协议字节数。

#### 6.7.1 为什么不能放在共享内存里让 worker 自行判定

最初的设计是：在 dsa 控制块里放一张"本会话已发过哪些关系"的共享哈希表，worker 在发关系描述前查表，表里有且 cseq 更小就跳过。这条路走不通，理由是**判定依据不成立**：

> 去重要成立，必须保证"被跳过的那条定义，订阅端一定已经见过了"。这要求"发定义的事务"与"用定义的事务"在订阅端的到达顺序是确定的。
> 串行路径靠的是"同一个解码上下文里先发定义、再发变更"，顺序天然确定。
> 并行路径里，worker 之间**没有 cseq 序的先后关系**：持有更大 cseq 的 worker 完全可能先到达并发出定义。于是当一个更小 cseq 的事务随后到达时，它看到"表里已经有这条关系了"，但那条定义属于**更晚**的事务——订阅端会先应用这个更小 cseq 的事务，而此时它**还没有**那条定义。

共享表 + worker 自行判定的规则（表里有且 epoch 相同就跳过）在语义上是**安全**的（最坏情况是多发一条，绝不会少发），但它**不是重复消除**：实测中出现 2 条 RELATION 而非 1 条——取决于哪个 worker 先到达。

**判定必须放在掌握全局放行序的那一端。** walsender 是唯一知道 cseq 放行顺序的进程（7.2 的 barrier），因此去重判定归它。

#### 6.7.2 实现：worker 标记，walsender 判定

| 环节 | 位置 | 内容 |
| --- | --- | --- |
| 标记"下一帧是关系描述" | decode worker | 插件钩子 `rel_definition_hook(ctx, relid)` 在 pgoutput 写关系消息**之前**被调用，worker 记下 `pd_pending_relid` / `pd_pending_epoch` |
| 走独立的消息类型 | decode worker | worker 的输出钩子发现有待标记的关系，就用 `PD_ENT_RELATION` 而不是 `PD_ENT_FRAME` 发送该帧，头部多带 `{ end_lsn, cseq, epoch, relid, length }` |
| 去重判定 | walsender | 进程内哈希表 `parallel_decode_sent_relations`：`relid → { epoch, sent }`。放行事务的帧时，若该帧是关系描述且该关系在**同一个或更新的 epoch** 里已经发过，就丢弃该帧 |

**为什么普通帧不多付代价**：去重信息走的是**另一种消息类型**（`PD_ENT_RELATION`），普通帧（`PD_ENT_FRAME`）的头部与原来完全一样。代价只落在真正携带关系描述的那少数几帧上。

**为什么恰好发一次**：walsender 按 cseq 顺序放行事务（O3，4.1.3 / 7.2），因此"最早碰到这张表的那个事务"的关系描述一定**最先**被放行并送达订阅端；此后所有事务的关系描述帧都在放行时被丢弃。于是每个关系每个 epoch 恰好发一条。

**epoch 仍然必要**：DDL 之后旧的关系描述已经过时，订阅端必须拿到新的那份。epoch 是 walreader 的"目录代次"（每次发布快照内容或广播失效就加一，随快照头与失效消息下发）。表里的条目带 epoch，判定规则是：

| 情形 | 动作 | 理由 |
| --- | --- | --- |
| 该关系在本 epoch 未发过 | **发**，并记下 `epoch` | 订阅端还没有这份定义 |
| 已发过，且表里的 epoch **≥** 本帧的 epoch | **丢弃** | 订阅端已有的定义不比本帧旧（同一个 epoch = 同一份结构；更大的 epoch = 更新鲜的结构，本帧是过时的那份） |
| 已发过，且表里的 epoch **<** 本帧的 epoch | **发**，并把表里的 epoch 更新为本帧的 | 本帧是 DDL 之后的新定义，必须送达 |

**流式事务一律照发。** pgoutput 在流式模式下按 (relation, top-xid) 追踪关系描述的发送，本来就是**每个流式事务重发一次**；且流式分片不受 cseq 顺序约束（它没有 cseq，只受放行前沿约束，见 7.4.1），订阅端应用流式事务的顺序不由此处掌握，不能假设它先到。流式事务**不会触发标记钩子**，因此不会进入去重路径。

**分区表。** `publish_via_partition_root` 场景下，祖先表的关系描述与分区表自身的关系描述**分别标记、分别去重**（各用各的 relid 作键）。

**类型消息仍然重复。** pgoutput 把用户自定义类型的消息写在关系消息**之前**（`send_relation_and_attrs()` 内部），且与关系消息分属不同的插件写，因此类型消息不在去重范围内。类型消息重复是**无害**的：订阅端的类型缓存被后到的同一条覆盖。

**钩子的语义是"标记"而非"是否发送"。** 这是与串行路径的边界：钩子为 NULL 表示"不标记，总是按普通帧发送"——串行路径就是这个语义，**串行路径一行不改**。

```c
/* replication/logical.h */
void (*rel_definition_hook) (struct LogicalDecodingContext *ctx, Oid relid);
```

pgoutput 侧只在 `!data->in_streaming` 时调用它（`send_relation_and_attrs()` 的 `announce` 参数），因此流式事务天然被排除。

#### 6.7.3 验证

`doc/pa_decode/t/rel_dedup_smoke.sh`：8 个并发会话 × 40 个事务的单表负载（事务之间保持重叠，因此会分布到 4 个 worker 上），分别用串行路径与并行路径各解一遍，统计 trace 文件里的 RELATION 消息条数。

实测（旧构建 → 新构建）：

| 路径 | RELATION 条数 |
| --- | --- |
| 串行 | 1 |
| 并行（旧：共享表 + worker 判定） | 4（有时 2，取决于 worker 到达次序） |
| 并行（新：walsender 判定） | **1**，连跑 3 次结果一致 |

DDL 场景（`ddl_diff_smoke.sh`）并行路径发 5 条 RELATION，与串行路径**逐消息一致**——说明 DDL 之后新的关系描述确实被重新送达，去重没有误伤。

## 7. 提交顺序保证的实现

### 7.1 提交序号（cseq）的产生

walreader 是 WAL 的唯一读取者，因此 WAL 顺序即提交顺序。walreader 在读到事务结束记录时分配 cseq：

| 事件 | 分配时机 | cseq | 是否产生 wire 消息 |
| --- | --- | --- | --- |
| COMMIT | 读到 `XLOG_XACT_COMMIT` 时 | 分配 | 是（`COMMIT` 消息） |
| ABORT | 读到 `XLOG_XACT_ABORT` 时 | 分配 | **取决于是否流式**：流式事务发 `STREAM ABORT`，非流式事务不发任何消息。**载荷取决于订阅端模式**（事实 8c）：`streaming='parallel'` 时多带 `abort_lsn` 与 `abort_time` |
| PREPARE | 读到 `XLOG_XACT_PREPARE` 时 | 分配 | 是（`PREPARE` 消息） |
| COMMIT PREPARED / ROLLBACK PREPARED | 读到对应记录时 | 分配 | 是 |
| CANCEL | 由 `XLOG_RUNNING_XACTS` 清理游离事务时 | 分配 | **取决于是否流式**：流式事务发 `STREAM ABORT`，非流式事务不发任何消息（现状 `reorderbuffer.c:2946` 的 `ReorderBufferAbort()` 只在 `rbtxn_is_streamed(txn)` 时才调 `rb->stream_abort()`） |
| **子事务 abort**（`ROLLBACK TO SAVEPOINT`） | 读到 `xl_xid` 为**子 xid** 的 `XLOG_XACT_ABORT` 记录时 | **不分配** | 是（`STREAM ABORT`，xid = 顶层、subxid = 子 xid）。**不受 cseq 门控，但受放行前沿约束**（不得越过已经结束的事务的结束事件，见 7.4.1），必须及时下发（见 5.2.2） |

**CANCEL 的收尾与其它结束事件不同，必须在 worker 侧分叉。** 事务被取消时，worker 侧的本地 `ReorderBufferTXN` 仍在（它从未提交），收尾动作是 `ReorderBufferAbort()`：

- **流式事务**：必须先发 `STREAM ABORT`（`reorderbuffer.c:2946` 的 `rb->stream_abort()`），否则订阅端会一直认为有一个打开的流段；
- **非流式事务**：不发任何 wire 消息，barrier 靠 `END_ACK` 推进（见 7.2）。

两种情况的 `END_ACK` 都必须发出。

cseq 单调递增且连续分配（每个**顶层**事务结束事件占一个序号），因此“空洞检测”退化为简单的“水位 + 计数”，无需复杂的乱位图。**子事务 abort 不占 cseq**——它不是提交事件；它只受放行前沿约束，且必须及时下发（5.2.2、7.4.1）。

**walreader 必须能区分顶层 abort 与子事务 abort**：判据是 abort 记录的 `xl_xid` 是否命中自己的事务绑定表——命中即为顶层 abort（占一个 cseq），否则是子 xid（不占 cseq）。

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

要点回顾（推导见 4.1.3 的表）：帧头 LSN 只在 `last_write=true` 时才是真值——**逐消息清单见 4.1.3，其中 `STREAM START`、`STREAM STOP`、`STREAM ABORT`、`STREAM PREPARE` 都写真值，不是 0**；订阅端用**取最大值**跟踪 `last_received`；发布端把该值**无条件**写进 `confirmed_flush`（`logical.c:1849`，**没有单调性检查**）；而重启时 `start_decoding_at = confirmed_flush`，**结束记录起始 LSN 小于它的事务会被整体跳过**（判据比的是**记录起点**：`decode.c:1302` 传 `buf->origptr`，`snapbuild.c:443-446` 比 `ptr < start_decoding_at`）。**keepalive 的 end_lsn 走同一条通道**（`'k'` 分支同样执行 `last_received = max(...)`），它的**两个来源**都要钳。

于是安全条件是：

> **S1**：任何**结束记录起始 LSN < `confirmed_flush`** 的事务，必须已经**完整送达**。（"结束记录起始 LSN" = `buf->origptr`，与跳过判据同口径。）

串行现状自动满足它，因为事务按提交序送达、且 reorder buffer 按 WAL 序处理记录（任何变更帧的 LSN 都不会越过尚未输出的更早提交记录）。

并行模型下破坏 S1 的是**提前送达的流式分片**：分片里变更帧的 LSN 可以大于某个”已提交但因 barrier 尚未放行”的事务的提交记录 LSN，把 `confirmed_flush` 抬到那个提交记录之上，重启后该事务被跳过。**第二个破坏源是 keepalive**：它的 end_lsn 取 `sentPtr` = 发布端的 WAL 读取位置（`walsender.c:3443`、`:4071`），订阅端照样把它并入 `last_received`（`worker.c:3608-3612`）——串行下这个值安全是因为”提交记录 LSN < `sentPtr` 的事务都已送达”，而并行 + 尽早流式正好打破这条不变式。

实现上有三条路：

| 方案 | 做法 | 代价 |
| --- | --- | --- |
| (a) 按 LSN 序发数据 | walsender 持有一个”下一个可发 LSN”的水位，按 LSN 排序后逐个事务整块发出 | 等价于 FIFO，放弃 4.3 的核心卖点（数据随时可发） |
| (b) 数据跟随 cseq 放行 | 非流式事务的数据与其提交消息一起按 cseq 输出 | 吞吐受最慢事务拖累，但语义最安全 |
| (c) 分片帧 LSN 钳制 | 分片照常早发，但 walsender 把分片帧头的两个 int64 改写为 `min(帧内 LSN, 放行前沿 LSN)`；**keepalive 的 end_lsn 同样钳制** | 需要 walsender 改写 worker 产出的帧；订阅端 `last_received` 推进变慢，`confirmed_flush` 推进变慢，WAL 保留窗口变长 |

本设计取 **(b) + (c)**：

- **(b) 覆盖非流式事务**：它们的整事务字节（含 COMMIT）必须等自己的 cseq 被放行，因此其帧 LSN ≤ 自己的提交记录 LSN ≤ 放行前沿 ✔。
- **(c) 覆盖流式分片与 keepalive**：分片不属于任何已放行事务，必须显式钳制；keepalive 同理——**它的 end_lsn 不能直接用 `sentPtr`**。

**钳制为什么是安全的（实现者必须先知道这一点，否则不敢钳）**：订阅端只把帧头 LSN 用于 `last_received`（`worker.c:3590-3594`），**不用它做任何语义判断**；而 pgoutput 的变更类消息**载荷里不含变更 LSN**（`proto.c` 的所有变更写入函数都不带 `change->lsn`）。所以把帧头 LSN 钳小只会让 `last_received` 推进慢一点，不改变任何一条变更的语义。

**但有一处不能钳**：`COMMIT` **载荷**里的 `end_lsn`（`txn->end_lsn` = 结束记录终点）经订阅端的 `store_flush_position()`（`worker.c:2280`）直接进入 flushpos → 回报给发布端 → 变成 `confirmed_flush`。这条路径**不钳**，因为它属于**已放行**事务，其终点天然 ≤ F（已放行事务的结束记录终点 ≤ 下一个未放行事务的结束记录起点，WAL 记录按序且不重叠）。这个论证必须写进实现说明，否则实现者可能"顺手"把载荷也钳了——那会破坏提交序判定。

**放行前沿的定义（唯一口径，实现时按此写）**：

> **放行前沿 F := min{ 所有"已分配 cseq 但尚未放行"的事务，其结束记录（COMMIT / ABORT / PREPARE / COMMIT PREPARED / ROLLBACK PREPARED）的**起始** LSN }。**

- **口径必须与跳过判据完全一致**：跳过判据 `SnapBuildXactNeedsSkip()`（`snapbuild.c:443-446`）比较的是 `ptr < builder->start_decoding_at`，而 `ptr` 是**结束记录的起点**（`decode.c:1302` 传的是 `buf->origptr`）。因此 F 必须取 `buf->origptr`，**不得取 `buf->endptr`**（后者才是帧头里写的那个 LSN，也是 `txn->end_lsn`）。取错方向的后果：`confirmed_flush` 落在（起点, 终点] 区间内 → 重启后 `start_decoding_at` 大于该事务的记录起点 → **整个事务被跳过 → 丢变更**。
- **abort 事务没有"提交记录"**：它用 **abort 记录的起始 LSN**（比 commit 起点更早，保守，安全）。
- **F 的来源**：walreader 在读到事务结束记录、分配 cseq 时得知该记录起始 LSN，通过控制通道按 cseq 递增的顺序把 "cseq → 结束记录起始 LSN" 的映射发给 walsender；walsender 取所有"已分配但未放行"项的最小值合成 F。**映射是有序的**，walsender 因此永远知道 F 对应哪些 cseq。
- **F 未知（集合为空）时怎么办**：集合为空意味着"walreader 已读到的结束记录全部放行"——**这不只是会话刚开始的特例，也是稳态常态**（发布端空闲、或只有低负载的非事务 WAL 时）。此时 F := **walreader 认证过的读取位置**，认证条件是"walreader 已读到的全部结束记录都已放行"，该认证必须**与映射一起下发**。
- **认证读取位置的实现载体（实现时踩过的坑，写下来免得再踩）**：“认证过的读取位置”不能只由 `PDCseqInfo.read_lsn` 承载——它**只在事务结束时**才随 cseq 映射一起下发，而集合为空恰恰意味着**没有事务结束**（发布端空闲），于是认证位置永远不前进，F 又会兜底到会话起点（正是本条禁止的兜底）。因此必须另有一条载体：控制通道消息 `PD_ENT_READ_POS`（walreader → walsender），携带 walreader 的读取位置。**认证之所以成立，是因为它与 cseq 映射走同一条 FIFO 控制通道**：walreader 先发“事务结束”再发“读取位置”，walsender 读到位置时，此前结束的事务它都已知；F 取 `min(认证读取位置, 最早未放行事务的结束记录起点)`，不会越过任何未送达的事务。
  - **下发点必须包含 walreader “等新 WAL”的那个点**。并行路径的 walreader 等新 WAL 的地方是**页读回调** `pd_walreader_read_page()`：它在回调里一直等到 WAL 到齐才返回，所以“每 N 条记录一次”与“WAL 暂时读完（`XLogReadRecord()` 返回 NULL）”这两个上报点在发布端空闲时**都到不了**。必须在下发点里加上页读回调的等待分支（“我读到这里了”），它才是串行路径 `WalSndWaitForWal()`（那里会发追平 keepalive）的并行等价物。漏掉它的后果是：表同步永远停在 `SYNCDONE`——订阅端的 `SYNCDONE → READY` 提升要求 apply worker 的 `last_received` 越过表的同步点，而 `last_received` 只由 `'w'` 帧头与 `'k'` keepalive 推进，发布端空闲时二者都没有。
  - `PD_ENT_MARKER`（给 worker 的同一位置）应**按位置去重**：等待循环每 100ms 走一次，不去重就会往每个 worker 的队列里灌重复 marker。
- **明确禁止的取值**：**不得用"未经 walreader 认证的 WAL 读取位置"作为 F 的替代**。这是本设计最容易踩的坑：cseq 是在**读到结束记录时**分配的（7.1），而放行水位滞后于 WAL 读取位置；一旦实现者按"拿不到精确值就用 WAL 读取位置"来处理，F 会越过某个已提交但未放行事务的结束记录起点，**直接破坏 S1**。
- **同样禁止的兜底**：**不得用"会话开始时的槽 `confirmed_flush`"**。集合为空是稳态常态而非例外；用会话起点的 `confirmed_flush` 兜底会把前沿**永久冻结在会话起点**——所有帧的 LSN 被钳到一个极小的常量 → 订阅端的 `last_received` 停在那里 → 它的 `lsn_mapping` 一旦被清空，`send_feedback()` 就用 `recvpos` 当 flushpos 上报（`worker.c:3753-3836`）→ 发布端 `LogicalConfirmReceivedLocation()` **无条件**写 `confirmed_flush`（`logical.c:1849`）→ **`confirmed_flush` 被拉回会话起点**，破坏 §17.7 的单调前进不变式，重连后整段区间被重发。
- **取样次序（硬约束）**：walsender 写任何一帧之前必须按 **① 更新放行水位 → ② 取样 F → ③ 写帧** 的顺序执行。否则刚被放行的事务，其 `COMMIT` 帧头 LSN（= 记录终点）会被钳到它自己记录起点之下 → 订阅端 `recvpos` 回退 → 同样造成 `confirmed_flush` 回退。
- **单调性要求**：钳制后的帧 LSN **不得小于所有已放行事务的结束记录终点 LSN**（已放行事务的终点天然 ≤ 下一个未放行事务的起点，因为 WAL 记录按序且不重叠），否则 `recvpos` 会倒退。

**放行前沿还有一个必须注意的边界**：它约束的是**尚未放行的事务**，而不是所有事务。若 walsender 已经放行到 cseq = k，则 F 取 cseq > k 的那些事务的结束记录起始 LSN 的最小值；任何已送达帧的 LSN 都不得越过它。这条约束由 walsender 单点执行——它是唯一同时知道"放行水位"与"正在转发的帧"的地方。

对流式事务，事务的最后一个分片与它的 `STREAM COMMIT` 之间隔着一个 cseq 屏障，会略微推迟提交可见性；这是可接受的代价，现状的串行实现本就有同样的提交路径队头阻塞。

**无 cseq 的帧与放行前沿的关系（7.4.1）**：最后一个分片的位置取**它的结束记录的起点**，因此它不会被"自己的"结束事件挡住——它赶在自己的 `STREAM COMMIT` 之前放行（协议要求如此），而挡它的是**别的、更早结束的事务**。换句话说：一个流式事务的最后一个分片至多被"在它之前就结束、且尚未放行"的事务挡住，不会被自己的结束事件挡住。

另外，§12.1 中 G1 的度量口径必须相应调整：`confirm_flush_lsn` 速率在乱序模型下会虚高，应改为**实际发出的协议字节速率**或**订阅端确认的应用速率**。

### 7.4.1 没有 cseq 的帧与放行前沿

**问题**：流式分片与子事务 abort 的 `STREAM ABORT` 没有 cseq，无法挂到 barrier 上。若它们一到就进就绪队列，就可以越过"在它们之前就已经结束"的事务的结束事件——发布端用行锁串行化的一对事务，在订阅端变成了并发 apply → 订阅端报出**发布端没有的 deadlock**（上游的 parallel apply 本来就有死锁风险，`applyparallelworker.c:60-130` 写明它是用 lmgr 的锁把死锁**检测**出来而不是**避免**；本条缺陷是**在发布端新增了一个本不该有的并发窗口**）。

**不变式（I-ORDER）**：

> 位置为 q 的无 cseq 帧，**不得越过**在 q 之前就已结束的事务的结束事件（COMMIT / ABORT / PREPARE）。

串行路径天然满足它：串行路径的输出顺序就是 WAL 记录的处理顺序，结束记录在位置 p 被输出、帧在位置 q 被输出，`p < q` ⇒ 结束记录一定在帧之前。并行路径不满足它，因为帧的产出时刻由"哪个 worker 先解码完"决定，与 WAL 顺序无关。

**实现（walsender 侧）**：

- **判门**：一个无 cseq 帧可以放行，当且仅当**放行前沿为空**（没有"已结束但未放行"的事务），或者**前沿队首事务的结束记录起始 LSN ≥ 该帧的位置**。
  - **两侧的位置口径不同**：队首事务的结束事件的位置是**结束记录的起点**，帧的位置是**产生它的那条记录的终点**。因此判据是 `≥` 而不是 `>`——一条记录的终点恰好等于下一条记录的起点时，两个位置相等是正常的。
  - **最后一个分片的位置另有约定**：一个事务的最后一个分片是在**它的结束记录被处理时**产出的，它的变更都早于该事务的结束事件，因此它的位置取**结束记录的起点**——这样它一定能赶在自己的结束事件之前放行（它必须先于自己的结束事件到达订阅端）。
- **held 表**：不放行的帧放进一张按位置排序的表（位置相同时按到达顺序），随放行前沿的前移逐个放行。**帧永远不丢弃**——丢弃会让订阅端拿到不完整的数据。held 表的字节数计入待放行区。
- **放行时机**：放行前沿前移（某个事务被放行）时重算一次；收到新的帧时也重算一次。
- **判门之前必须先读控制通道**：前沿是由 walreader 的消息算出来的，而控制通道与帧队列是两条通道，消息到达顺序不可比——**必须先把控制通道读空，再判门**，否则会把一个已经结束的事务漏掉。

**与"尽早流式"的关系**：挡住分片的只有**已经结束**的事务。一个还在解码的大事务不挡别的分片，因此流式的早发特性基本保留；被挡住的时间上界是"最早那个已结束事务的解码时间"。

**自检**：发布端自己检查"已经发出的分片的位置是否越过了正在放行的事务的结束位置"，越界则发一条 `WARNING`。它是**事后检测**，只报警不阻止；真正的保证来自判门。

## 8. 流式发送

### 8.1 尽早流式

用户需求明确指出“事务 stream 的时候要尽快发送来提升性能”。**在 3.5 确定了目标部署形态之后，这条需求的地位变了：它不再是可选的优化，而是让订阅端的并行 apply 真正能被喂饱的前提**（事实 8b）。先把现状机制说清楚：

- 触发条件是 reorder buffer 的**聚合**内存：`reorderbuffer.c:3659` 判断 `rb->size >= logical_decoding_work_mem * 1024`，其中 `rb->size` 是所有在途变更的总字节数（`reorderbuffer.c:3253-3259`），不是单事务大小；
- 触发后由 `ReorderBufferLargestStreamableTopTXN()`（`reorderbuffer.c:3592-3621`）挑选**总字节数最大**的顶层事务来输出；
- 一个分片 = 该事务自上一分片以来累积的**全部**变更，**没有固定字节上限**（事实 5）；输出后由 `ReorderBufferTruncateTXN()` 回收内存，阈值被复用；
- **正常负载下这段逻辑几乎不执行**：内存不超限就不流式，订阅端也就拿不到可并行的流式事务。

**现状已经有一个"立即流式"的开关，只是它是调试选项。** `debug_logical_replication_streaming`（`guc_tables.c:5228-5236`，`PGC_USERSET`，默认 `buffered`）有 `buffered` / `immediate` 两档（`guc_tables.c:430-433`）；`immediate` 下 `reorderbuffer.c:3659-3661` 的循环条件变为 `rb->size > 0`，即**每个有变更的事务都立即被流式化**。本设计的流式激进程度直接沿用这两档的语义。

并行解码为尽早流式提供了三个新的条件，**但其中只有两条真正成立**：

1. **大事务不再阻塞其他事务的解码**（成立，但需限定范围）：现状下最大事务被优先选中输出，其余事务要等内存压力缓解；多 worker 下每个 worker 有独立的 reorder buffer 与内存压力，一个大事务不再拖住其他 worker 的分片产出。**但同一 worker 内的其它事务仍被它阻塞**——`ReorderBufferProcessTXN()` 是整事务原子的（4.5.4），且分配准则若只看"在途事务数"，一个独占大事务的 worker 反而会被判为"最空闲"（见 4.5.1）。
2. **分片触发更频繁**（成立）：N 个 worker 各自持有独立的内存预算，单个 worker 的阈值更容易被触及 → 分片更小 → 发送更早。
3. **并行流式模式可用**（**不成立**）：协议 v4 的"并行"指的是**订阅端**用 parallel apply worker 回放（事实 8、事实 13）。**发布端在 wire 上依然一次只能有一个打开的流段**——订阅端的 `in_streamed_transaction` 是进程级单值，第二个 STREAM START 直接 `ereport(ERROR, "duplicate STREAM START message")`（`worker.c:1476-1479`，release build 同样触发）。这条必须写清楚，否则会得出"可以同时流式多个事务"的错误结论。

**O2 与流式的关系**（呼应 4.1.2）：O2 把 wire 的交错粒度钉死为"一个流段"。这意味着**段既是内存粒度，也是 wire 交错粒度，还是背压粒度**——三者合一，缺一不可。若段没有字节上限（事实 5），一个 1GB 的事务会占住 wire 直到整段发完，其它 worker 产出的字节全堵在共享内存结果区里。**O2 限制的是交错粒度与时延，不构成吞吐上限**——wire 本来就是单条 TCP 流，串行与并行下字节都要顺序过 socket；只要段有字节上限，段的独占时间就有界。真正的吞吐上限是 socket 与订阅端的消费能力。

因此为此：

- **新增 GUC 控制流式激进程度**，取值沿用现有调试选项 `debug_logical_replication_streaming` 的两档语义（`reorderbuffer.c:3645-3661`）：`buffered`（按内存阈值）与 `immediate`（达到批次粒度即输出），**外加一档 `auto`**。`auto` 是本设计的默认值，它**按订阅端的流式模式决定实际档位**（见下）；
- **必须给段设置字节上限**（新增 GUC `parallel_decode_stream_segment_size`）。没有上限时，O2 会把并行解码的产出全部堵死，段的独占时间无上界。`immediate` 档的批次粒度即为这个上限的一种取值；
- **流式门槛 `parallel_decode_stream_min_size` 默认取 0（不设门槛）**——理由见下"门槛的代价核算"，它与发布端的收益目标直接冲突。**0 的含义是"每个事务都被流式化"**，包括在进行中没有攒够段大小、**在提交时才被流式化**的那些事务；门槛大于 0 的含义是"低于门槛的事务**完全不被流式化**"，它们以整事务块的形式到达订阅端；
- 与 barrier 的关系：尽早流式与 commit barrier 基本正交——**分片提前发，提交仍按 cseq 放行**（分片属于事务内部，没有 cseq）。唯一的耦合是：**分片不得越过已经结束的事务的结束事件**（见 7.4.1）——挡住分片的只有**已经结束**的事务，一个还在解码的大事务挡不住别的分片。二者不冲突。

**`auto` 档：流式激进程度必须按订阅端的模式区分。**

| 订阅端模式 | `auto` 解析出的实际档位 | 理由 |
| --- | --- | --- |
| `streaming='parallel'`（协议 v4） | **`immediate`** | 事实 8b：订阅端的并行来自"多个事务的流式分片在时间上重叠"。`buffered` 档在正常负载下不产生流段 → 订阅端的 apply worker 无事可做 → 端到端并行度为 1，本特性的收益归零 |
| `streaming='on'`（协议 v2/v3） | **`buffered`**（保持现状行为） | 非 parallel 订阅方**没有 parallel apply worker**：`pa_can_start()` 因 `!MyLogicalRepWorker->parallel_apply` 直接返回 false（`applyparallelworker.c`），于是 `pa_find_worker()` 恒为 NULL，`get_transaction_apply_action()` 落入 `in_streamed_transaction` 分支 → **`TRANS_LEADER_SERIALIZE`**（把变更 spool 到文件、提交时才 apply），而不是直接 apply 的 `TRANS_LEADER_APPLY`。**强制流式化会把这类订阅方的每个事务都推到 spool 慢路径上**——对存量用户是性能回归 |
| 不请求流式 | 不流式 | 走整事务块路径（8.2） |

**"为什么 `parallel` 订阅方要用 `immediate`"的三条理由**：

| 理由 | 说明 |
| --- | --- |
| **喂饱订阅端的并行 apply** | 事实 8b：`buffered` 档在正常负载下不产生流段 → 订阅端的 apply worker 无事可做 → 端到端并行度为 1，本特性的收益归零 |
| **缩小挂起区，消除死锁风险面** | 未流式化的事务，其整事务字节必须等到自己的 cseq 放行才能发出（`PD_BLK_TXN` 受 barrier 门控，见 5.2.3）。挂起区因此可能装下整个事务的字节，与结果区争容量 → need.md §4.3 描述的死锁。流式化后挂起区里只剩几十字节的提交消息。**在 `immediate` 档 + `min_size = 0` 下这一条尤其成立**：每个事务都是流段，挂起区里只剩提交消息本身 |
| **兑现"尽早发送"的产品目标** | 用户需求明确要求"事务 stream 的时候要尽快发送来提升性能"。`buffered` 档把发送时机交给内存压力，恰恰是最不该由它决定的事 |

**`immediate` 档的实现：每个事务都是流段。** `immediate` 档把 `LogicalDecodingContext.stream_all_transactions` 打开（串行路径下它是 false，所以串行路径的行为不变）。它有两个效果：

- 进行中攒够 `parallel_decode_stream_segment_size` 的事务照常切段下发；
- **没攒够的事务在提交时作为一个流段发出**——`ReorderBufferReplay()` 在提交时判断该事务是否应当被流式化（`ReorderBufferStreamAtCommit()`），是则走 `ReorderBufferStreamCommit()`，把该事务的全部变更作为一个段发出去，订阅端收到的是一个完整的流段，可以并行 apply。

**这解决了小事务负载下"一个流段都没有"的问题。** 段大小的默认值是 1MB，而 tpcc 的典型事务只有约 3KB（15.1 的测算）——旧语义下这些事务永远攒不到段大小，因此**一个流段都不会产生**，订阅端的 apply worker 无事可做。并行解码路径现在把"提交时发一个段"作为兜底：只要 `parallel_decode_stream_min_size = 0`（默认），**每个事务都是流段**。

提交时流式化有四个前提，缺一不可：

| 前提 | 检查 | 理由 |
| --- | --- | --- |
| 路径是并行解码路径，且会话被要求立即流式 | `ctx->stream_all_transactions` | 串行路径与 `buffered` 档的行为必须不变 |
| 事务此前没有被流式化 | `rbtxn_is_streamed(txn)` | 已经发出去的事务不能重发 |
| 快照是一致的 | `ReorderBufferCanStartStreaming(rb)` | 订阅端在快照一致之前无法 apply 变更 |
| 事务不是两阶段提交的准备事务 | `rbtxn_prepared(txn)` | 两阶段提交路径保持自己的行为（`ReorderBufferPrepare()` 与 `ReorderBufferFinishPrepared()` 都在回放之前就置上了 `RBTXN_PREPARE`，所以这一条同时覆盖了 prepare 记录本身） |

低于 `parallel_decode_stream_min_size` 的事务仍然以整事务块的形式下发（8.2）。

**门槛的代价核算（这一条纠正了早期版本的错误结论）**：

早期版本把"过度流式化"的两项代价算作：(a) 帧数增加；(b) 订阅端为每个流式事务分配一个 apply worker。逐条核算后，**两条都要推翻**：

| 早前的说法 | 核算结论 |
| --- | --- |
| (a) "每个流段是 STREAM_START + 变更 + STREAM_STOP 三类帧，帧数显著上升" | **部分成立，但要修正数量级**。pgoutput 本来就是**每条变更一个独立的 CopyData 帧**（`pgoutput.c:1553/1574` 等），流式化**不改变每条变更一帧**的事实，只额外增加**每个事务 2 个**帧（STREAM_START + STREAM_STOP）。按 15.1 的 tpcc 数字（16.7k 事务/秒、约 50 万变更/秒），多出的 2 帧/事务约合 33k 帧/秒，相对 50 万帧/秒是 **约 7%** 的量级，本身不构成门槛的理由 |
| (a') **此前完全漏算的一项：`immediate` 档下 RELATION 消息按 top-xid 重发** | **成立，且是 `immediate` 档的主要代价**。pgoutput 在流式模式下按**顶层事务**追踪关系描述是否已发送：`maybe_send_schema()` 用 `get_schema_sent_in_streamed_txn(relentry, topxid)` 判断（`pgoutput.c:743`），发送后用 `set_schema_sent_in_streamed_txn()` 记录（`pgoutput.c:767`），事务提交时 `cleanup_rel_sync_cache()` 把该 xid 从 `entry->streamed_txns` 里删掉（`pgoutput.c:1887`）。因此**每个事务都要为它触及的每张表重发一次 RELATION**——tpcc NewOrder 触及约 5 张表，即每事务多出约 **5 帧 / 约 3KB**，**协议字节比非流式多 40%–60%，帧数多约 19%**。这与 2.4 的"事实 6"是同一件事，此前的核算把它漏掉了 |
| (b) "订阅端每事务分配一个 apply worker 是开销" | **算反了**。这个 apply worker **正是订阅端并行的机制本身**（事实 8a/8b）：不分配它，订阅端就没有并行回放，本特性的端到端收益归零。它的真实成本只是每事务一个哈希项（`applyparallelworker.c:499`，事务结束时 `HASH_REMOVE`）与一次 `shm_mq` 传输，而 worker 进程本身是**从池里复用**的（`applyparallelworker.c:411-418`，池保留 `max_parallel_apply_workers_per_subscription / 2` 个） |

**因此 `parallel_decode_stream_min_size` 的默认值必须是 0**（不设门槛）：门槛一旦大于典型事务大小（本设计的 15.1 测算里 tpcc 每事务约 **3KB** WAL），**低于门槛的事务完全不会被流式化** → 订阅端永远收不到 STREAM START → `pa_allocate_worker()` 不会被调用 → **订阅端并行 apply 根本不启动**，70MB/s 目标与"并行解码 + 并行 apply"这个前提同时落空。门槛只应作为**给特定负载的调优旋钮**（例如大对象写入导致的超大事务需要内存整形时调高），不能作为默认值。**注意这一条比旧语义更硬**：旧语义下 `min_size > 0` 只是"不在进行中流式化"，事务仍然可以在提交时以流段的形式发出；新语义下 `min_size > 0` 意味着"低于门槛的事务整个不流式化"。

代价改由 `parallel_decode_stream_segment_size`（段字节上限）与 `parallel_decode_stream_min_size`（用户按需调高）共同控制：**大事务早流式（省内存、早送达），段有上限（O2 的独占时间有界）**。**两个尺寸不再互相抬高**：段大小只是分段点，最小大小是"能不能被流式化"的门槛（旧实现里的 `segment_size = max(segment_size, min_size)` 已删除）。

**与订阅端 apply worker 池的耦合（必须写进实现）**：发布端的流式并发度**不能**超过订阅端能并行回放的并发度。订阅端的 apply worker 数有硬上限——`launcher.c:412` 在并发 worker 数达到 `max_parallel_apply_workers_per_subscription` 时**拒绝再启动**；此时该事务退化到 `TRANS_LEADER_SERIALIZE`（spool 慢路径，见上表）。因此：

- 订阅端能并行回放的事务数 = `max_parallel_apply_workers_per_subscription`（默认 **2**）——这就是端到端并行度的上限；
- 发布端流式并发度高于它时，多出来的事务在订阅端走慢路径，**端到端反而变慢**；
- 因此达成条件里的 apply worker 数不能只写 "> 0"，必须**按目标速率反推一个下限**（12.1）。


### 8.2 分片与整事务两种输出形态

| 订阅方 | 发布方输出形态 | 说明 |
| --- | --- | --- |
| 请求流式（协议 v2+，`streaming='on'`/`'parallel'`，事实 11） | 分片下发：worker 按阈值切分，每片是一个完整的 `PD_BLK_SEG` 块 | 首选路径，尽早发送。**并行解码路径的 `immediate` 档下每个事务都是流段**（8.1），没攒够段大小的事务在提交时作为一个段发出 |
| 未请求流式 | 整事务输出：worker 不切分，事务的每一帧各自是一条消息（`PD_ENT_FRAME`），在提交时陆续写出 | 退化路径，保证整事务字节连续 |

判定依据是订阅方在 `START_REPLICATION` 中传来的 `streaming` 选项，发布方在 `pgoutput.c:488-510` 解析，本方案沿用该机制，**不新增协议**。

**并行解码路径的热路径只有分片一种**：所有事务都被流式化之后，`PD_BLK_TXN` 形态只在 `buffered` 档（`streaming='on'`）与串行路径上出现。

**两种形态下的阻塞特性不同，代价也不同：**

| 形态 | wire 被占用的时长 | 内存占用 | 主要风险 |
| --- | --- | --- | --- |
| 流式（`PD_BLK_SEG`） | 一个流段（**有字节上限**，8.1） | 每 worker `rb->size` 之和 | 段切得过小 → 协议消息数膨胀 |
| 非流式（`PD_BLK_TXN`） | **整个事务**（无上限） | 整个事务的字节 | 大事务长时间独占 wire；且字节要整块挂在待提交区等 cseq（见 7.2 的死锁风险）。字节在 wire 上仍然是连续的，只是传输粒度是帧 |

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
| **订阅端 apply worker 崩溃** | 发布端 walsender | 订阅端 leader apply worker 同时承担 walreceiver 职责，崩溃即连接断开 → 发布端侧表现为"订阅端断开"，会话终止、槽释放；订阅端重启后从 `restart_lsn` 重放 |
| **订阅端整库重启** | 发布端 walsender | 同上 |
| **会话正常结束**（订阅端断开、表同步结束） | walsender | `ParallelDecodeSessionStop()`：置 shutdown 标志 → **先终止 walreader，再终止 decode worker** → 释放共享内存里的块。详见 17.5 的"会话的拆卸" |

**会话的拆卸必须按"生产者先停"的顺序。** walreader 是唯一的 WAL 读取者，它把记录交给 worker；先停它，worker 才不会留下来等一条永远不会到达的记录。worker 是消费者，它们把帧交给 walsender；walsender 在等待它们的时候会把输出队列排空，因此一个正卡在"交出帧"上的 worker 能够把这次交接做完，然后看到 shutdown 标志。**拆卸期间不等待 worker 退出是不安全的**——共享内存段一旦释放，worker 往里写就会踩到别人的内存；因此终止之后必须逐个等待它们真的退出，再释放共享内存段。

**订阅端侧故障在发布端的语义只有一条**：连接断开 → walsender 报错退出 → `ReplicationSlotRelease()`（`walsender.c:341`）释放槽（槽状态已落盘）→ 订阅端重连 → 新会话从 `restart_lsn` 重放。**发布端不需要为订阅端的故障做特殊处理**，但设计必须写明，因为它决定了"尽早流式"的风险边界——见 17.7 的"至少一次语义"与"会话重建后的订阅端状态复位"。

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
| `parallel_decode_streaming_mode` | `auto`（默认）/ `immediate` / `buffered` | 流式切分激进程度，语义沿用 `debug_logical_replication_streaming` 的两档（见 8.1），外加 `auto` 档。**默认取 `auto`**：按订阅端的流式模式解析——`streaming='parallel'` → `immediate`（订阅端的并行 apply 靠发布端产生流式分片才成立，事实 8b）；`streaming='on'` → `buffered`（保持现状行为；强制流式化会把非 parallel 订阅方的每个事务推到 spool 慢路径 `TRANS_LEADER_SERIALIZE`，是性能回归，8.1） |
| `parallel_decode_stream_min_size` | `0`（默认，即不设门槛） | **小于此字节数的事务完全不被流式化**，走整事务块（`PD_BLK_TXN`）。**0 的含义是"每个事务都被流式化"**——包括在进行中没有攒够段大小、在提交时才被流式化的那些事务（8.1）。**默认必须为 0**：门槛若大于典型事务大小（15.1 的 tpcc 每事务约 3KB WAL），低于门槛的事务都不流式化 → 订阅端收不到 STREAM START → 并行 apply 根本不启动（8.1 的"门槛的代价核算"）。仅作为特定负载的调优旋钮 |
| `parallel_decode_stream_segment_size` | 建议 1MB | **单个流段的字节上限**。O2 把 wire 的交错粒度钉死为"一个流段"，没有上限时段的独占时间无上界，并行解码的产出会被堵死（见 8.1）。**它只是分段点**：没攒够这个大小的事务在 `immediate` 档下仍然会在**提交时**作为一个流段发出（8.1），因此它不决定"事务是否被流式化" |
| `parallel_decode_work_mem` | 建议 `logical_decoding_work_mem` | **每 worker** 的解码内存上限（总量 = worker 数 × 该值）。它是 `logical_decoding_work_mem` 的覆盖值——后者语义不变，但首版不做跨进程总量配额（12.2） |
| `parallel_decode_batch_size` | 建议 64KB | **跨进程唤醒的速率上限**：walreader 与 worker 各把写入队列的消息按字节合并成批，批满即刷新并唤醒对端（5.4 第 9 项）。承载进度的消息（位置心跳、事务结束事件、控制消息）不批量。0 关闭批处理，每条记录、每帧都单独唤醒。有效值被钳制在共享队列的 1/4 以内 |
| `parallel_decode_barrier_timeout` | 60（秒，默认）/ 0 表示禁用 | **停滞检测的超时**：barrier 生效（有待放行的事务）且某个 worker 的解码位置落后于 walreader 的读取位置、且该 worker 在这段时间内没有送来任何消息时，判定该 worker 卡死并结束会话（17.8）。0 表示关闭该检测 |
| `parallel_decode_pending_max_bytes` | 64MB（默认）/ 0 关闭 | **待放行区（事务帧）的字节上限**：待放行区装的是"已解码但还没轮到"的事务帧。一个事务解码得慢，其后所有事务的帧都堆在发布端的内存里；超过上限时发一条 `WARNING`（每个 episode 只发一条，待放行区缩回上限以下后才会再告警）。会话继续运行，数据不受影响。0 关闭该告警 |
| `parallel_decode_held_max_bytes` | 64MB（默认）/ 0 关闭 | **无 cseq 的帧（流式分片、子事务 abort 的 `STREAM ABORT`）等待放行前沿时的字节上限**：这些帧没有 cseq，只能等"在它们之前就结束的事务"放行之后才能下发（7.4.1）。等待的帧占用发布端的内存，超过上限时发一条 `WARNING`（同样每个 episode 只发一条）。**帧永远不丢弃**——丢弃会让订阅端拿到不完整的数据。0 关闭该告警 |
| `parallel_decode_fault_inject` | 0（默认，即不注入） | **故障注入开关**，仅用于验证故障路径：`1` = decode worker 启动后立刻失败，`2` = walreader 启动后立刻失败（17.8）。生产环境必须保持 0 |

所有 GUC 均需提供 `sighup` 或 `user` 级可调性，并保证关闭时行为与现状完全一致（AC-8）。

**GUC 变更的生效时机必须区分。** worker 池是**会话级**的（随 walsender 一起建立与销毁），因此：

| GUC | 变更生效时机 | 理由 |
| --- | --- | --- |
| `max_parallel_decode_workers` | **下次会话**（新 walsender 会话按新值建池） | worker 数决定了共享内存的队列条数（2N 条 `shm_mq`，§5.1）与绑定表规模，在会话中途增删会改变这些结构；且正在解码的事务不能迁移（4.5） |
| `parallel_decode_work_mem` / `parallel_decode_batch_size` | `sighup` 后**下次会话** | 同上：`ReorderBufferCheckMemoryLimit()` 只读**本进程**的 GUC 值（12.2），worker 进程必须重启才能看到新值 |
| `parallel_decode_streaming_mode` / `parallel_decode_stream_min_size` / `parallel_decode_stream_segment_size` | `sighup` 后**下次会话** | 这三个值在**会话建立时**被解析并冻结（`auto` 档要按订阅端的流式模式解析，见 8.1；`min_size` 与 `segment_size` 要参与流段切分），会话中途改值会与已切出的流段不一致 |
| `parallel_decode_barrier_timeout` / `parallel_decode_pending_max_bytes` / `parallel_decode_held_max_bytes` | `sighup` 后**立即** | 这三个值在**每次检查时**才读，reload 之后当前会话即按新值判定（它们只影响判定阈值，不改变已经切出的流段或已经解码的帧） |

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

**已实现（FR-13）**。六个视图的取值路径：并行路径的计数器都写在 walreader / worker / walsender 三个进程各自的**动态共享内存**里，SQL 进程不能直接读，因此由 **walsender 侧**把 dsa 控制块里的计数**拷贝**到主共享内存的 `WalSnd.pd_stats`（`struct WalSnd` 新增该字段），拷贝在 `walsnd->mutex` 自旋锁下进行，**节流到 100 毫秒一次**（`ParallelDecodePublishStats()`）。两个 SRF（`pg_stat_get_parallel_decode_session()` / `pg_stat_get_parallel_decode_workers()`）遍历 `WalSndCtl->walsnds[]`，跳过 `pid == 0` 或 `pd_stats.active` 为假的槽位；视图再把 SRF 的结果与 `pg_stat_activity` / `pg_authid` 联结，补上用户名与应用名。没有 `ROLE_PG_READ_ALL_STATS` 的用户只能看到自己那一行的 pid，其余列一律置空（与 `pg_stat_get_wal_senders()` 的口径一致）。

实现要点与边界：

- **计数器是单调累计值，不是速率**。视图给出的是会话启动以来的总量（帧数、字节数、socket 写次数、事务数、唤醒次数）与两个瞬时量（当前积压的 cseq 数、最长挂起提交的年龄）。速率要由采样者自己做差分——15.2 的唤醒速率与帧率两项因此可以在性能机上直接从视图采样得到。
- **`in_flight_txns` 只有 walreader 一个写者**。worker 侧只更新 `records_consumed` / `decoded_txns` / `wakeups`；在途事务数由 walreader 在**分配**与**结束**两处增减，避免两个进程写同一个计数器造成的数据竞争。
- **唤醒次数的上下行不对称是设计使然**：下行（walreader → worker）传的是 WAL 记录，上行（worker → walsender）传的是帧。两侧都按字节合并批次（5.4 第 9 项，`parallel_decode_batch_size`），因此**唤醒次数与"字节数 / 批大小"同阶，而不是与记录数 / 帧数同阶**。计数器的口径是"**刷新次数**"——刷新才真的唤醒了对端；承载进度的消息不批量，每次都刷新，因此各计一次。批处理关闭时（`parallel_decode_batch_size = 0`）计数退回"每条记录 / 每帧一次"。**唤醒速率的具体数值待实测**（15.2）
- **没有走 pgstat 的新框架**。本树没有 `shmem_request_hook` 扩展点，引入新框架的改动面远大于收益，因此直接复用 `WalSndCtl` 这套已存在于主共享内存的结构。
- **会话不活跃时视图为空**。`pd_stats.active` 只在并行路径的会话建立之后为真，因此**串行路径的复制会话不会出现在这六个视图里**（`monitoring_smoke.sh` 对此有专门断言）。

## 12. 性能

### 12.1 目标与测量

- 目标：tpcc 100 万 tpm 场景下**实际发出的协议字节速率** ≥ 70MB/s（口径见下）；
- **度量口径必须修正**：`confirm_flush_lsn` 速率在并行模型下会**虚高**——订阅端用 `'w'` 帧的 LSN 推进 `last_received` 并回报 flush LSN，而事务数据允许乱序下发（7.4），于是 flush LSN 可能被后发的事务抬高，而前面事务的数据其实还在路上。因此 G1 的度量口径应为"发布端实际写出的协议字节速率"或"订阅端**确认的应用速率**"，不能直接用 `confirm_flush_lsn`；
- **达成条件（"与"关系，缺一不可）**：订阅端 `streaming='parallel'` 且 `max_parallel_apply_workers_per_subscription` **达到按目标速率反推的下限**——下限 = `ceil(目标变更率 / 单 apply worker 可承接的变更率)`，目标变更率 = 16.7k 事务/秒 × 25 变更/事务 = **42 万变更/秒**（这是**需求值**，不是单 worker 的容量断言）；单 worker 容量 C 未知且**待实测**（15.2 第 4 项），按 C 在 1 万–10 万变更/秒量级估算，下限落在 **4–40**，**默认 2 大概率不足**。并发上限见 `launcher.c:412`，超出的事务在订阅端退化成 spool 慢路径 `TRANS_LEADER_SERIALIZE`；发布端**尽早流式**（`parallel_decode_streaming_mode = auto` 对 `streaming='parallel'` 订阅方解析为 `immediate`，且 `parallel_decode_stream_min_size = 0` 不设门槛——否则订阅端的 apply worker 无事可做，见事实 8b 与 8.1 的"门槛的代价核算"）。**注意 `parallel_decode_stream_segment_size` 的默认值 1MB 远大于 tpcc 的典型事务（约 3KB）**：这意味着每个事务都在**提交时**才被流式化，而不是在进行中切段；端到端时延因此接近整事务下发，收益只剩"订阅端能并行 apply"（见 15.2 的待实测项）；
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
| **协议帧率**（与系统调用率是两回事，见下一行） | output plugin 是**每条消息**一次 `OutputPluginWrite`（`pgoutput.c:1553/1574` 等），每个 INSERT/UPDATE/DELETE 都是一个独立的 CopyData 帧。100 万 tpm 场景约 50 万帧/秒（15.1）。**帧数不因并行化而减少**——并行化把"生成帧"的工作搬到了 worker，但"每条变更一帧"这个协议事实没变 | 帧数无法在协议层摊薄（订阅端 `apply_dispatch()` 一个 CopyData 只解析一条逻辑消息，`worker.c:3283-3300`）；只能让每一帧的处理足够便宜。**已做到**：walsender 每帧的剩余工作只有**改帧头的发送时间戳 + FR-20 钳制 + 一次 `pq_putmessage_noblock`**，**没有额外的帧拷贝**——帧的字节直接指向它所在的块缓冲（块化之后），块本身又直接接管了 worker 消息的缓冲 |
| **walsender 每帧一次 `write()` 系统调用** | `WalSndWriteData()` 在**每帧之后**调用 `pq_flush_if_writable()`（`walsender.c:1576-1598`），该函数会立刻把 `PqSendBuffer` 里的数据 `write()` 出去（`pqcomm.c:1432-1451`）。`PQ_SEND_BUFFER_SIZE` 只有 8192，缓冲复用挡不住这个粒度——**每帧一次 `write()` 系统调用**，单核开销约 0.5–1.5 核，且完全串行在 walsender 一个进程里 | 并行模式的 walsender 不沿用"每帧 `pq_flush_if_writable()`"的节奏，改为**按块 / 按 8KB 缓冲刷出**（**已实现**，见 16.1；批次大小实际由"此刻就绪的帧"决定，8KB 只是上限）。系统调用率可从"帧率"降到"字节率 / 8KB"，按 8192 字节缓冲估算降幅约 **47 倍**（静态估算，待实测——15.2 第 8 项）。**注意：批量刷出只降 syscall，不降帧数、不降订阅端每帧解析成本、不降 FR-20 的逐帧钳制成本** |
| **段粒度串行下发** | O2 把 wire 的交错粒度钉死为"一个流段"；若段无字节上限（事实 5）→ 一个 1GB 的事务会占住 wire 直到整段发完，其它 worker 的产出全堵在结果区（8.1）。**注意：O2 限制的是交错粒度与时延，不构成吞吐上限**——wire 本就是单条 TCP 流，串行与并行下字节都要顺序过 socket | 给段设字节上限 GUC（`parallel_decode_stream_segment_size`） |
| **walreader 的每条 WAL 记录成本** | 新架构里 walreader 是**唯一**的 WAL 读取者：每条记录都要 `XLogReadRecord` + CRC32C 校验（`xlogreader.c:1195-1206`）+ 记录分类 + 路由 + 拷进共享内存。tpcc 的 WAL 记录数（含索引记录）是解码变更数的 **2–4 倍**，所以这条路径的记录数远大于变更数 | 这一项是 walreader 单进程的硬成本，无法通过增加 decode worker 摊薄。缓解方向：记录分类只做一次、CRC 校验与解码分离（walreader 只做校验与路由，tuple 重建在 worker）；成本待实测（15.2 第 7 项） |
| **walsender 的 FR-20 逐帧钳制** | FR-20 要求帧头 LSN **逐帧钳制**到放行前沿（7.4 的方案 c），不能整块只钳一次。因此 walsender 必须**逐帧定位帧边界**并改写每个 `'w'` 帧头的两个 int64。这是 O(changes) 的单进程工作量，**并行化不但没有消除，还新增了** | **已实现（前半句）**：worker 在流段里给每帧加了 `uint32` 长度前缀，walsender 据此定位帧边界，**不必自己解析 CopyData 协议**。块化之后帧也**不再从段里拷出来**——帧的字节直接指向段缓冲。剩下的是每帧改写帧头的两个 int64（发送时间戳 + FR-20 钳制）。**后半句（由 worker 直接钳制）未做**：钳制要按放行前沿逐帧判断，而放行前沿只有 walsender 知道。成本待实测（15.2 第 6 项） |
| **订阅端 leader 的每帧派发** | 订阅端 leader 每次 `PQgetCopyData` 只取一条 CopyData（`libpqwalreceiver.c:913/924`），再 `apply_dispatch`（`worker.c:3283-3300`）；流式事务里**每条变更**都要一次 `pa_send_data` → `shm_mq_send`（`worker.c:617-618`）。这是订阅端 leader 上的 **O(changes) 串行工作量** | **本设计的边界是"只改发布端"（第 106 行），无法从发布端缓解**。要么接受它为已知限制，要么在 12.1 的达成条件里显式承认端到端上限由它决定；订阅端侧的缓解方案（批量 CopyData）需要改协议，不在本设计范围内。成本待实测（15.2 第 5 项） |
| **小事务被过度流式化**（已按 8.1 重新核算） | 流式化**不改变"每条变更一帧"的事实**，只多出每事务 2 帧（STREAM_START + STREAM_STOP），约 7% 帧量；订阅端每事务一个 apply worker 是**并行的机制本身**而非开销。**但 `immediate` 档下 RELATION 消息按 top-xid 重发**（`pgoutput.c:743/767/1887`）——每事务为其触及的每张表重发一次 RELATION，tpcc NewOrder 约 5 张表即每事务多约 5 帧 / 约 3KB，**协议字节 +40%–60%、帧数 +19%** | 门槛 `parallel_decode_stream_min_size` **默认 0**（不设门槛），仅作调优旋钮；段字节上限 `parallel_decode_stream_segment_size` 才是硬约束。RELATION 重发的放大倍数待实测（15.2 第 3、16 项）。**注意这部分开销发布端消不掉**：流式模式下 `rel_definition_hook` 不触发（`pgoutput.c:827`），关系描述作为普通帧拼进流段，walsender 的去重（6.7）看不到它；而订阅端的 apply worker 正是**从流里**拿到关系描述的（`worker.c:559-643` 的 `pa_send_data`），发布端吞掉它就没有定义可用——"订阅端源码不改"是硬边界 |
| **订阅端 apply worker 池成为端到端瓶颈** | 订阅端能并行回放的事务数 = `max_parallel_apply_workers_per_subscription`（默认 **2**，`launcher.c:412` 超限即拒绝启动）；超出的事务退化成 `TRANS_LEADER_SERIALIZE`——把变更 spool 到文件、提交时才 apply。发布端流式并发度高于这个数时，端到端**反而变慢** | 达成条件里 apply worker 数按目标速率反推下限（12.1）；监控订阅端 apply worker 的占用率与 spool 事务数 |
| **非 parallel 订阅方的 spool 慢路径** | 非 parallel 订阅方没有 apply worker（`pa_can_start()` 因 `!parallel_apply` 返回 false），流式化后每个事务都走 `TRANS_LEADER_SERIALIZE` 而不是直接 apply 的 `TRANS_LEADER_APPLY` | `parallel_decode_streaming_mode = auto`：`streaming='on'` 解析为 `buffered`，保持现状行为；只有 `streaming='parallel'` 才用 `immediate`（8.1） |
| **目录缓存 N 倍** | syscache / catcache / relcache 是**进程私有**的（`syscache.c:86` 的静态 `SysCache[]`、`relcache.c:134` 的静态 `RelationIdCache`），`RelationSyncCache` 也是每 worker 一份（`pgoutput.c:82/89/217`）。每个 worker 首次触达某关系时各自要走一遍发布关系判定（`GetRelationPublications` / `GetSchemaPublications`）；DDL 之后 `RelationSyncCache` 被整体作废，**每个 worker 都要把全部关系重新判定一遍**。DDL 频繁时这不是"少量冗余带宽"，而是 N 倍的目录扫描 CPU | 把"关系 → 发布列表 / schema"做成**只读共享内存表**（walreader 维护、worker 只读）。这才是 4.2 中"共享关系表"的真正收益来源——不只省带宽，还省 N 倍目录扫描 |
| **每事务内部事务与全局失效队列** | 每解码一个事务，worker 都要 `StartTransactionCommand` → `AtStart_Cache` → 在**全局 `SInvalReadLock`** 上取共享锁并排空共享失效队列（`xact.c:1163-1167` → `inval.c:807-811` → `sinvaladt.c:497`），事务结束再 `AbortCurrentTransaction`（`reorderbuffer.c:2147-2150/2574`）。N 个 worker × 每事务一次 = N×T 次对同一把全局 LWLock 的共享获取；更贵的是排空后每个 worker 都要独立作废自己的 catcache/relcache 条目 | worker 内把多个事务的解码合并到一个内部事务里做（现有代码已支持事务内做 INTERNAL_SNAPSHOT / INTERNAL_COMMAND_ID 的 Setup/Teardown，`reorderbuffer.c:2428-2478`），只要不在解码期间分配 XID 就不违反现有约束；与"目录缓存 N 倍"一起通过共享目录缓存解决 |
| **catcache miss 的扫描** | catcache miss 时的索引扫描在 historic snapshot 下进行（`genam.c:408-413`），N 个 worker 各自独立 miss、各自独立扫描，每次都要做可见性判断 | 同上，共享目录缓存 |
| 协议格式化仍单线程 | 若最终退化为方案 B，walsender 成为新瓶颈 | 方案 A 已把格式化移入 worker |
| 共享内存带宽 | 每条数据变更要过共享内存两次（下行 WAL、上行字节），且 `shm_mq` 的载荷本身是 memcpy（`shm_mq.c:1043`） | 批量提交；必要时评估固定槽 + 引用计数的自研队列（真正的零拷贝） |
| **跨进程信号速率** | 跨进程 `SetLatch()` 的实现是 `kill(pid, SIGURG)`（`latch.c:695`），本分支的 `Latch` 没有 futex 语义（`latch.h:112-121`）。按每事务一批、16.7k 事务/秒估算，信号速率约 1.3 万～6.7 万次/秒；`shm_mq` 的接收方每读一次还会反向唤醒发送方（`shm_mq.c:1296`），信号总数翻倍 | **已实现**：批次按**字节阈值**（`parallel_decode_batch_size`）而非按事务切，唤醒次数与"字节数 / 批大小"同阶（5.4 第 9 项）；显式选择 `force_flush` 策略；唤醒速率已纳入监控（`pg_parallel_decode_wakeups`）。**具体的信号速率待实测**（15.2 第 14 项） |
| 关系缓存重复 | RELATION 消息按 worker 重复 | **已解决**（6.7）：worker 标记、walsender 按放行序去重，同一关系每个 epoch 恰好发一条 |
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
11. **目标部署形态**（3.5）：本特性的性能承诺只覆盖"发布端并行解码 + 订阅端 `streaming='parallel'` + 足够数量的 parallel apply worker + 发布端尽早流式（`auto` → `immediate`，且小事务门槛为 0）"。不满足时并行解码仍可工作（协议与语义不变），但**不承诺端到端吞吐收益**。这个限制必须写进用户可见的文档，否则会出现"发布端开了并行、订阅端没开 parallel apply、却期待 70MB/s"的落差。**非 parallel 订阅方走兼容路径**：其流式档位由 `auto` 解析为 `buffered`，行为与现状一致（8.1）。

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
| **keepalive 的 end_lsn 越过放行前沿** | keepalive 的 end_lsn 取 `sentPtr` = 发布端的 WAL 读取位置（`walsender.c:3443`、`:4071`），订阅端照样并入 `last_received`（`worker.c:3608-3612`）→ `confirmed_flush` 被抬到未放行事务的提交记录之上 → 重启后该事务被整体跳过 → **静默丢数据** | keepalive 的 end_lsn **同样钳制**到放行前沿，不得直接用 `sentPtr`（4.1.3、7.4）；与分片帧的钳制一起验证（16 的 P6） |
| **放行前沿的 fallback 被误实现** | 实现者若把“拿不到精确前沿就用当前 WAL 读取位置”当作通用规则，前沿会越过某个已提交但未放行事务的提交记录 LSN → 破坏不变式 S1 → 静默丢数据 | 前沿口径唯一化：**恒等于** cseq = 放行水位 + 1 的事务的提交记录 LSN；未知时唯一合法取值是会话开始时的槽 `confirmed_flush`；**禁止**用 WAL 读取位置（7.4） |
| **非 parallel 订阅方被强制流式化** | 存量 `streaming='on'` 订阅方的每个事务被推到 spool 慢路径 `TRANS_LEADER_SERIALIZE`，性能回归 | `parallel_decode_streaming_mode = auto` 按订阅端模式解析；`streaming='on'` 保持 `buffered`（8.1、11.1） |
| walsender 每帧一次 `write()` 系统调用 | **系统调用率**（不是帧率）在协议帧率高时成为串行天花板 | 批量刷出（`pq_flush` 合并多个 `CopyData`），按 8192 字节缓冲估算系统调用数降约 47 倍（静态估算，待实测）；**帧数与每帧处理成本不变**；帧率纳入监控（11.3 的 `pg_parallel_decode_walsender_frames`） |
| **walsender 的 FR-20 逐帧钳制** | 逐帧定位帧边界 + 改写每个 `'w'` 帧头的两个 int64，是 O(changes) 的单进程工作量，**并行化后新增而非消除** | worker 随块携带每帧 LSN 元数据（5.2），或由 worker 直接完成钳制（12.3、15.1） |
| **walreader 的每条 WAL 记录成本** | walreader 是唯一 WAL 读取者，WAL 记录数（含索引）是解码变更数的 2–4 倍，成本全部落在单进程 | 记录分类与 CRC 校验分离；成本待实测（15.2 第 7 项） |
| **订阅端 leader 的每帧派发** | 订阅端 leader 上 O(changes) 的串行工作量，**发布端无法缓解**（设计边界"只改发布端"） | 明确为已知限制，并在 12.1 的达成条件里承认端到端上限由它决定；成本待实测（15.2 第 5 项） |
| 段粒度串行下发阻塞整条 wire | 一个慢 worker 拖住全部产出 | `parallel_decode_stream_segment_size` 给段字节上限；预取掩盖切换开销（5.4 第 7 条） |
| 端到端吞吐上限实为订阅端 apply 速率 | 发布端优化不产生端到端收益 | 文档明确"并行解码的收益前提是订阅端 `streaming='parallel'` + **达到下限数量的** parallel apply worker + 发布端尽早流式"（3.5）；在 12.1 写清达成条件；apply worker 数按目标速率反推下限而非 "> 0"（3.5、8.1、12.1） |
| **订阅端未开 parallel apply** | 期望落空：发布端已并行、订阅端仍串行 apply，端到端无收益 | 在用户可见文档与 GUC 说明中显式写出目标部署形态（3.5）；监控项暴露订阅端的 streaming 模式与 apply worker 数量 |
| 小事务被过度流式化 | 帧数上升（每事务多 2 帧，约 7%）**且 `immediate` 档下 RELATION 按 top-xid 重发**（每事务约 5 帧 / 约 3KB，协议字节 +40%–60%） | `parallel_decode_stream_min_size` 默认 0，仅按需调高；段字节上限兜底；RELATION 重发放大倍数待实测（8.1、12.3、15.2 第 3 项） |

## 15. 待决问题

| 编号 | 问题 | 备选 | 倾向 |
| --- | --- | --- | --- |
| Q1 | 提交顺序方案 | commit barrier / FIFO | commit barrier（4.3） |
| Q2 | 解码切分点 | 事务级 A / 变更级 B / A+关系表 C | A 为主；关系消息去重已单独实现（6.7），形态是 walsender 侧去重而非共享关系表 |
| Q3 | 事务→worker 绑定基数 | 一对一 / 多对一 | 多对一：一个 worker 承载多个事务，绑定后不可迁移（4.4、4.5） |
| Q4 | 关系消息重复 | 容忍 / 共享关系表 / walsender 侧去重 | **walsender 侧去重**（6.7）：worker 之间的到达顺序与 cseq 序无关，共享表让 worker 自行判定不安全；只有 walsender 掌握放行序 |
| Q5 | 解码内存语义 | 每 worker 独立 / 总量均摊 | **每 worker 独立**——总量均摊需要一整块尚不存在的跨进程配额机制（12.2） |
| Q6 | 流式切分默认档 | auto / immediate / buffered | **默认 `auto`**——按订阅端模式解析：`streaming='parallel'` → `immediate`（订阅端的并行 apply 靠发布端产生流式分片才成立，事实 8b、3.5），`streaming='on'` → `buffered`（保持现状行为，否则非 parallel 订阅方的每个事务都被推到 spool 慢路径，8.1）。**两种实际档都必须有段字节上限**（`parallel_decode_stream_segment_size`）；小事务门槛 `parallel_decode_stream_min_size` **默认 0**（8.1 的"门槛的代价核算"） |
| Q9 | 事务数据是否跟随 cseq 放行 | 跟随（安全）/ 按 LSN 序（等价 FIFO） | 跟随 cseq（7.4 的 `'w'` 帧 LSN 约束） |
| Q7 | worker 崩溃是否降级为串行 | 终止会话 / 降级 | 终止会话（9、17.3） |
| Q8 | 子进程形态 | 动态后台进程 / walsender 直接 fork | 动态后台进程，参照 `launcher.c:503`（3.3） |
| Q10 | worker 数量与收益的匹配度 | 按解码算力配 N / 按端到端瓶颈配 N | **两个数必须分开回答**（15.1）。**解码侧 N = 3~4**：按修正后的成本表（53–125 µs/事务），100 万 tpm 需要约 0.9–2.1 核解码算力，N = 3~4 足够达到 70MB/s 所需的解码能力。**订阅端 apply worker 数**：下限 = `ceil(42 万变更/秒 ÷ 单 worker 容量 C)`，C 待实测（15.2 第 4 项），按 1 万–10 万变更/秒估算落在 **4–40**，默认 2 大概率不足。**瓶颈不在解码**，而在订阅端 leader 的每帧派发、订阅端 apply worker 容量、walsender 的逐帧钳制与每帧 `write()`、walreader 的每记录成本。因此"整套并行机制"的复杂度投入与解码侧的收益并不匹配，收益实际落在"撑起 26–50 万帧/秒的生产与消费路径"上 |

### 15.1 收益测算：并行度并不需要很大

**先统一三个口径**——本节早期版本正是在这里出错的：

| 口径 | 含义 | 换算式 |
| --- | --- | --- |
| WAL 字节率 | 复制槽从 WAL 里读走的字节 | `W × txn/s`，W = 每事务 WAL 字节数 |
| **协议字节率** | 发布端实际写到 socket 的字节 | `B × txn/s`，B = 每事务协议字节数 |
| 帧率 | CopyData 帧/秒 | `C × txn/s`，C = 每事务帧数 |

G1 的 70MB/s 是**协议字节率**（12.1）。早期版本用 W（每事务约 3KB WAL）去换算它，等于把 WAL 字节率当成了协议字节率，方向性误差约 `B / W ≈ 2.6` 倍。下文一律按协议字节率计算。

**tpcc NewOrder 的三个常数**（约 25 条行级变更、约 320 列、文本格式）：

| 常数 | 值 | 依据 | 性质 |
| --- | --- | --- | --- |
| W：每事务 WAL 字节 | 约 3 KB | 经验值，两份文档均无出处 | **待实测**（15.2 第 2 项） |
| B：每事务协议字节 | 约 8 KB | 静态估算，见下 | **待实测**（15.2 第 1 项） |
| C：每事务帧数 | 约 30 | 每条变更一帧（`pgoutput.c:1553/1574`）+ `immediate` 档下每事务约 5 个 RELATION 重发（8.1） | 静态估算 |

B 的静态估算（文本格式，`pgoutput.c:294` `data->binary = false`）：

- 帧头与消息头：`'w'` 帧头 25 B（`walsender.c:1573-1592`：1+8+8+8）+ CopyData 帧头 5 B + 逻辑消息头 6 B（`'I'`/`'U'` + relid 4 B + `'N'`）≈ 36 B/变更 × 25 ≈ 900 B；
- 列载荷：320 列 × (1 B 类型标记 `'t'` + 4 B 长度 + 文本平均约 8 B) ≈ 4.2 KB（`proto.c:852-854`）；
- RELATION 重发（`immediate` 档）：5 张表 × 约 600 B ≈ 3.0 KB（`proto.c` `logicalrep_write_rel` / `logicalrep_write_attrs`）；
- 合计 **B ≈ 8 KB**，约为 W（3 KB）的 **2.6 倍**。

**解码侧的成本表**（每事务小计，单位 µs；表头已写明每行都是"每事务小计"，不再混用"每列/每变更"的单位）：

| 成本项 | 源码位置 | 每事务小计 |
| --- | --- | --- |
| 每列：catcache 查类型 + 类型输出函数 + 写协议缓冲（320 列） | `proto.c:828/853/854` | 约 42–112 µs |
| 每变更：relmapper + relcache + tuple 重建（25 次变更） | `reorderbuffer.c:2227/2251`、`decode.c:1253` | 约 8 µs |
| 每事务：内部事务 + 失效队列排空 + 快照拷贝 | `reorderbuffer.c:2150/2574/1833` | 约 3–5 µs |
| **合计** | | **约 53–125 µs / 事务** |

→ **单核解码上限约 8k–19k 事务/秒 ≈ 480k–1130k tpm**。tpcc 100 万 tpm（16.7k 事务/秒）需要约 **0.9–2.1 核**的解码算力。

其中 `decode.c:1253` 的 tuple 重建成本随 `heap_decode` 迁入 worker 而**并行化**（2.4、8.1）；但 walreader 自身新增的**每条 WAL 记录**成本（`XLogReadRecord` + CRC32C 校验，`xlogreader.c:1195-1206` + 路由 + 拷进共享内存）**留在单进程**，且 tpcc 的 WAL 记录数（含索引记录）是解码变更数的 2–4 倍——这一项在 12.3 有对应的风险行，但**本表尚未给它定价**（15.2 第 7 项）。

**70MB/s 换算成事务率**（按协议字节口径）：

- 70 MB/s ÷ B（8 KB/事务）≈ **8.75k 事务/秒 ≈ 52.5 万 tpm**；
- 同一场景的**帧率** ≈ 8.75k × 30 ≈ **26 万帧/秒**；
- 作为对照，100 万 tpm 场景的帧率 ≈ 16.7k × 30 ≈ **50 万帧/秒**，协议字节率 ≈ 133 MB/s。

也就是说：**协议字节率这个目标本身并不要求高吞吐**，真正吃紧的是**帧率**——每条变更一个 CopyData 帧，而每一帧在发布端、订阅端都要被逐帧处理（12.3 的两行风险）。

**结论**：

1. **N = 3~4 个 decode worker 足够达到 70MB/s 所需的解码能力**（0.9–2.1 核）。更多的 worker 收益递减，而共享内存与信号开销线性上升。
2. 但 **N（解码 worker 数）与端到端并行度不是同一个数**。端到端并行度由订阅端的 `max_parallel_apply_workers_per_subscription` 决定（3.5、8.1、12.1）。Q10 的两个备选应当分别回答：**解码侧 N = 3~4**；**订阅端 apply worker 数按目标变更率反推**（见下）。
3. 本设计的复杂度（三进程拆分、共享内存、cseq barrier、顺序契约）远超解码侧的需求——这一点必须写进设计的自我评估。

**端到端的瓶颈按代价排序**（每一条都必须进 12.3 的风险表）：

| # | 瓶颈 | 性质 | 状态 |
| --- | --- | --- | --- |
| 1 | **订阅端 leader 的每帧派发**：`PQgetCopyData` 每次只取一条 CopyData（`libpqwalreceiver.c:913/924`）→ `apply_dispatch`（`worker.c:3283-3300`）→ 流式事务里**每条变更**一次 `pa_send_data`（`worker.c:617-618`） | 订阅端 leader 上的 **O(changes) 串行工作量**；设计边界是"只改发布端"（第 106 行），**无法从发布端缓解** | 12.3 风险表已补行 |
| 2 | **订阅端 apply worker 容量**：需求为 16.7k 事务/秒 × 25 变更 = **42 万行/秒**（这是**需求值**，不是容量断言） | 单 worker 容量 C 未知，下限 = `ceil(42万 / C)`；C 大概率在 1 万–10 万行/秒量级 → **下限很可能是 4–40，默认 2 大概率不足** | 12.3 风险表已补行；C 待实测（15.2 第 4 项） |
| 3 | **walsender 的逐帧开销**：定位帧边界 + FR-20 的**逐帧钳制**（不能整块只钳一次，7.4 的方案 c）+ memcpy 进 socket 缓冲 | O(changes) 串行；**并行化后不但没消除，还新增了钳制** | 12.3 风险表已补行 |
| 4 | **walreader 的每条 WAL 记录开销** | 唯一 WAL 读取者 + snapbuild owner + 路由器 + cseq 分配者，全在单进程 | 12.3 风险表已补行 |
| 5 | **O2 的段粒度下发** | O2 限制的是**交错粒度与时延**，**不构成吞吐上限**——wire 本就是单条 TCP 流，串行与并行下字节都要顺序过 socket；段有字节上限时段的独占时间有界 | 12.3 风险表已补行；8.1 的表述已同步 |
| 6 | **walsender 的每帧一次 `write()` 系统调用** | 系统调用数可被批量刷出降低约 `PQ_SEND_BUFFER_SIZE` 倍（`pqcomm.c:118` 为 8192），但**帧数与每帧处理成本不变**——批量刷出降的是 syscall，不是帧率 | 12.3 风险表已拆成两行（见下） |

**因此 70MB/s 的达成条件必须写清**：订阅端 `streaming=parallel` + **apply worker 数达到按目标变更率反推的下限** + 段粒度上限 + walsender 批量刷出 + 发布端并行解码 + 发布端尽早流式。这些是"与"的关系，缺一项则端到端达不到目标。

**"写一次 `write()` 是最硬的串行天花板"这句话本身是错的**（早期版本在 12.3 与 15.1 之间自相矛盾）：批量刷出把系统调用数降一个数量级，但帧数不变、订阅端每帧解析成本不变、FR-20 逐帧钳制成本不变。真正的约束是**帧率**，不是 syscall 率。12.3 已把这两条拆成独立的风险行。

### 15.2 待实测确认清单

以下十九项无法从源码静态推断，必须在实现期用实测数据确认。它们都直接影响上文的测算与参数取值。**本机开发只做功能与正确性验证，这些数值需要在性能环境实测后回填**；在回填之前，文档中的相关数字一律标注为"静态估算"或"待实测"，不得当作结论使用。

| # | 事项 | 需要什么证据 | 影响的设计决策 |
| --- | --- | --- | --- |
| 1 | **每事务协议字节数 B** 与**每事务帧数 C** | 发布端埋点统计（协议字节数可直接从帧统计，帧数从 `pg_stat_replication` 的 `sent_bytes` 与帧计数交叉验证） | 70MB/s ↔ 事务率 ↔ 帧率三者的换算枢纽；B/W 的比值决定流式化的放大代价（15.1） |
| 2 | **每事务 WAL 字节数 W** 与**每事务 WAL 记录数**（含索引记录） | 一次真实负载的 WAL 统计（`pg_waldump` 或 `pg_stat_wal`） | "3KB" 是否成立；walreader 每记录成本的规模（15.1 瓶颈 #4） |
| 3 | **`immediate` 档下 RELATION 重发的实际放大倍数** | 统计每事务的 RELATION 消息条数与字节数，与 `buffered` 档对比 | 协议字节与帧数两个维度的放大；是否需要接受这个代价（15.1、8.1） |
| 4 | **单个 parallel apply worker 的实际行/s 容量 C** | 一个"单 apply worker"的受控负载 | apply worker 数下限 = `ceil(42万 / C)` 的具体数值（默认 2 大概率不足，12.1） |
| 5 | **订阅端 leader 每帧派发的实际成本**（`PQgetCopyData` + `apply_dispatch` + `pa_send_data` 合计 µs/帧） | 订阅端 leader 的 CPU 占用 ÷ 帧率 | 15.1 瓶颈 #1 是否成立；"只改发布端"这条边界的代价（12.3） |
| 6 | **walsender 逐帧钳制 + memcpy 的 µs/帧** | 发布端 walsender 的 CPU 占用 ÷ 帧率 | 15.1 瓶颈 #3 的规模；是否需要把 LSN 元数据随块下发以避免 walsender 解析帧边界（5.2） |
| 7 | **walreader 每条 WAL 记录的 µs**（含 CRC32C 路径） | walreader 进程的 CPU 占用 ÷ WAL 记录数 | 15.1 瓶颈 #4 的规模；是否需要把 CRC 校验与记录路由拆开 |
| 8 | **批量刷出后 `write()` 系统调用数的实测下降倍数** | 发布端 socket 写次数 ÷ 帧数 | 12.3 的系统调用率风险行（静态估算约 47×，需实测修正） |
| 9 | **单帧 > 8KB 的比例**（`pqcomm.c:1533` repalloc 频率） | 负载中"单条变更 > 8KB"的比例（宽表 / 大 TEXT） | 批量刷出的缓冲大小是否需要提高 |
| 10 | 订阅端是否把 `'w'` 帧的 LSN 用于 `confirmed_flush` 之外的用途 | 通读 `worker.c` 中 `last_received` / `send_feedback` 的全部使用点，以及 `pg_stat_replication` 的 `sent_lsn` / `write_lsn` 语义 | 7.4 的钳制方案是否足够（若订阅端另有用途，钳制会引入新的不一致） |
| 11 | `SnapBuildSerialize` 是否在每个 `XLOG_RUNNING_XACTS` 上真的重写 | 实测 `pg_logical/snapshots` 目录的写入频率，或读其 body 的"是否需要重写"判据 | walreader 的磁盘 I/O 是否会成为新的瓶颈 |
| 12 | `catalog_xmin` 在多 worker 下实际会被推多旧 | 一个"worker 持有长事务"的实测，看 `pg_replication_slots.catalog_xmin` 的推进行为 | 6.6 的 xmin 聚合策略（已实现：walreader 用自己的 reorder buffer 算出，与串行路径同口径；仍需实测长事务下的推进行为） |
| 13 | 关系描述去重在真实负载下省下多少协议字节 | 一个"多表 + 多 worker"的负载，统计去重前后 RELATION 消息的条数与字节数 | 6.7 的实际收益（本机已验证条数 4 → 1，字节收益待实测） |
| 14 | **批处理后的跨进程唤醒速率**（下行 walreader → worker、上行 worker → walsender，各多少次/秒） | 性能机上对 `pg_parallel_decode_wakeups` 视图按时间采样做差分，负载为 tpcc 100 万 tpm | 5.4 第 9 项的收益是否兑现；`parallel_decode_batch_size` 的取值。本机已验证同一负载下关闭批处理与 64KB 的**唤醒次数**差异（上行 1338 → 417、下行 969 → 363），但那是次数不是速率 |
| 15 | **`parallel_decode_stream_segment_size` 的默认值 1MB 对 tpcc 的含义**（每事务约 3KB） | 统计 tpcc 负载下"事务在提交时才被流式化"的比例（即事务大小 < 段大小的比例），以及这种情形下的端到端时延 | **段大小的默认值远大于典型事务大小**，因此每个事务都在**提交时**才被流式化（而不是在进行中切段），端到端时延因此接近整事务下发，收益只剩"订阅端能并行 apply"。段大小的默认值是否需要按负载调小，见 8.1 与 12.1 |
| 16 | **`immediate` 档下 RELATION 重发占协议字节与帧数的实际比例** | 在 trace 文件（**只用于一次性采样，不能在性能跑中常开**）里统计每事务的 RELATION 条数与字节数，与 `buffered` 档对比 | 15.1 第 3 项的细化：RELATION 重发是 `immediate` 档的**固定开销**（约 37.5% 协议字节、约 17% 帧，静态估算），**发布端消不掉**（订阅端 apply worker 依赖它，订阅端源码不改）——因此 G1 的"70MB/s 协议字节"里约有 26MB 是冗余。**必须先定口径**：70MB/s 指的是协议字节率还是有效数据吞吐。见 `send_bottleneck.md` §9 |
| 17 | **50MB/s → 70MB/s 的缺口分诊**（四项检查） | ① `parallel_decode_trace_file` 是否为空（每帧两次 `write()` + 落盘，**足以单独造成这个量级的差距**）；② walsender 进程的 CPU 占用（≪100% ⇒ 限速在下游）；③ 订阅端 `pg_stat_subscription.parallel_apply_workers` 是否贴着 `max_parallel_apply_workers_per_subscription`；④ `pg_parallel_decode_commit_lag` 的 `cseq_allocated` 是否持续增长而 `cseq_released` 停滞（放行前沿卡住 ⇒ 与 CPU 无关的限速） | 决定这个缺口**到底在哪一端**。发布端逐帧路径已榨不出 20%（`send_bottleneck.md` §9.3），分诊结果决定后续是改发布端、改订阅端配置，还是承认这是协议/架构层面的限制 |
| 18 | **walreader 的单进程 WAL 记录吞吐上限** | walreader 进程的 CPU 占用 ÷ WAL 记录数（记录数 = 变更数 × 2–4）；配合把 `max_parallel_decode_workers` 固定为 1 与 4 两档对比 | 15.2 第 7 项的规模化版本：补齐 50→70MB/s 意味着多读 13–25 万条 WAL 记录/秒，这是**发布端进程内唯一还可能到顶的路径**。若 walreader 顶到单核，进一步的整改要动架构（walreader 是唯一 WAL 读取者），不是调参 |
| 19 | **walreader 等新 WAL 的方式：100 ms 轮询，而不是"WAL 刷出"事件** | 发布端 `pg_stat_activity` 里 walreader 的 `wait_event_type` / `wait_event`（应为 `Extension / paralleldecodedewalreader`）与逐进程 CPU；配合 `pg_current_wal_flush_lsn()` 的推进速率与 `pg_current_wal_lsn()`（写位置）对比 | 串行路径是被"WAL 刷出"事件唤醒的：`XLogFlush()` 末尾 `WalSndWakeupProcessRequests()`（`xlog.c:2930`、`:3105`）→ `WalSndWakeup()` → `ConditionVariableBroadcast(&wal_replay_cv)`（`walsender.c:4019`、`:4079`）→ walsender 立刻醒。并行路径的 walreader 只 sleep 在**自己的** latch 上（`parallel_walreader.c:423`，100 ms 超时），而它的 latch 只有 SIGTERM handler 会 set（`:268`）⇒ **新 WAL 最多迟 100 ms 才被发现**，流水线成波推进（波间全体饿死在 `IPC / MessageQueueReceive`）。**改法（只动发布端）**：**已实现**——walreader 改成睡在逻辑 walsender 用的那个 ConditionVariable 上（`WalSndCtl->wal_replay_cv`）：WAL 刷出时 `WalSndWakeup()` 会广播它（`walsender.c:4019`），主库每次刷出广播、备库每次重放广播；100 ms 超时保留作兜底。**没有新增共享内存，也没有新的通知点**。**收益是延迟与波纹，不是吞吐**；同时要确认刷出速率本身（walreader 等的是 `GetFlushRecPtr()`，与串行路径同一个位置），见 `send_bottleneck.md` §9.10 |
## 16. 实现计划

按可独立验证的阶段推进，每个阶段结束时开关关闭的行为必须与现状完全一致（AC-8）。

| 阶段 | 目标 | 主要改动 | 阶段验收 |
| --- | --- | --- | --- |
| P0 基础设施 | 建立共享内存与子进程骨架 | `parallel_decode.c`、`parallel_decode_msg.c`：dsa 创建与附着、`shm_mq` 封装、消息编解码、动态后台进程注册 | 单 worker 串行跑通一轮复制，字节输出与现状逐字节一致 |
| P1 walreader 拆分 | 把 WAL 读取与 snapbuild 从 walsender 迁到 walreader | `parallel_walreader.c`：WAL 读取循环、snapbuild 驱动、记录分类与路由；walsender 侧改造为只做网络与顺序 | 解码结果不变，`restart_lsn` / `confirmed_flush` 语义等价（AC-2、AC-8） |
| P2 worker 解码 | worker 承担解码与插件格式化 | `parallel_decode_worker.c`：批次消费、reorder buffer 与 output plugin 实例化、结果产出；`logical.c` 支持按 worker 实例化 | 事务数据全部经 worker 产出，walsender 只剩发送（AC-2） |
| P3 顺序保证 | 引入 cseq 与 commit barrier | cseq 分配、`next_commit_seq` 水位与待提交表 | commit/abort/cancel 顺序与 WAL 一致（AC-3） |
| P4 多 worker 并行 | worker 池与事务分配 | 事务→worker 绑定表、**按在途字节数**优先分配、队列水位反馈 | 吞吐随 worker 数上升（AC-1、AC-6） |
| P5 快照与失效 | 事件流分发与 tuplecid 传递 | **已实现**：walreader 独占 snapbuild（`SNAPBUILD_ROLE_WALREADER`），worker 用只读替身（`SNAPBUILD_ROLE_STANDIN`）；快照内容、base 快照、快照切换与失效消息按 WAL 位置下发（`PD_ENT_SNAPSHOT` / `PD_ENT_GLOBAL_INVAL`）；记录按**事务拥有者**路由（`PD_FORWARD_OWNER`），不再全量广播；命令 ID / tuplecid 由 worker 从原始 WAL 记录**自行重建**；**关系描述去重已实现**（6.7，worker 标记 + walsender 按放行序判定，关系描述每个 epoch 只发一次） | DDL 混合负载下元数据与现状一致（AC-5）；`ddl_diff_smoke.sh` 逐消息比对通过；`rel_dedup_smoke.sh` 验证关系描述只发一次 |
| P6 尽早流式 | 分片切分与提前下发 | 流式模式 GUC（`auto` / `immediate` / `buffered`，按订阅端模式解析）、批次粒度切分、结果区提前发送；**walsender 按放行前沿钳制分片帧 LSN 与 keepalive 的 end_lsn**（7.4 的方案 c） | 大事务在解码完成前即可到达订阅端（AC-4）；**且提前送达的分片与 keepalive 都不会把 `confirmed_flush` 抬到未放行事务的提交记录之上**（不变式 S1，用 kill/restart 实验验证无丢数据）；**非 parallel 订阅方的行为与现状一致**（未被强制流式化，8.1） |
| P7 故障恢复 | 进程生命周期与故障处置 | worker 状态字、`bgw_notify_pid` 通知、barrier 超时、退出码与 `bgw_restart_time` 约束、cseq 作废路径 | kill worker 后会话及时终止而非静默停滞；重启后可从 `restart_lsn` 续传（AC-10、AC-11） |

**P0 必须先有帧级比对工具与黄金文件。** 并行路径的正确性判据是"**与串行路径逐字节等价**"，没有这个工具就无法判定任何后续阶段是否引入了字节差异。因此 P0 的产出里必须包含：

- 一个**帧级 dump / diff 工具**：把串行路径与并行路径产出的 wire 字节按帧（`'w'` / `'k'` / `'r'` / `'c'`）拆开并逐帧比对，输出第一处差异的帧序号、帧类型与两个 int64 头；
- 一组**黄金文件**：在固定的 WAL 输入上，记录串行路径的帧序列，作为并行路径的比对基准。

比对时必须允许两类**有意的**差异并显式列出：帧的**分组方式**（并行路径把不同 worker 的消息混排进同一会话，帧的分组与串行路径不同）与帧头 LSN 的**钳制值**（并行路径的钳制后 LSN 可能小于串行路径的原值）。除此之外的任何差异都是缺陷——特别是**事务的先后顺序必须一致**：并行路径不得让一个事务的流式分片越过更早结束的事务的提交事件（7.4.1）。

推进顺序上的三条约束：

1. **P1 之前不改解码路径。** 先把 WAL 读取搬走、解码仍留在 walsender，用现有路径验证进程拆分本身没有引入行为差异。
2. **P2 之前不引入多 worker。** 单 worker 打通后，多 worker 只是分配策略的变化；否则进程拆分、解码改造、并行调度三个变量同时出错，难以定位。
3. **P3 可以在 P1 之后独立推进。** cseq 的分配只依赖 walreader 的 WAL 顺序，与 worker 是否解码无关，可以与 P2 并行实现。

### 16.1 实现状态（P0–P7）

P0–P7 已在 `pa_decode` 分支实现（含 P5 的**关系描述去重**与 6.6 的**槽水位跨进程聚合**）。P0–P4 的验证手段是**帧级比对**：同一个发布端、同一批 WAL、两个在同一条事务里创建的复制槽，分别用串行路径（`max_parallel_decode_workers = 0`）与并行路径（`max_parallel_decode_workers = N`）各解一遍，把两侧的 wire 字节全部记录下来逐消息比对（`doc/pa_decode/t/frame_diff_smoke.sh`、`concurrent_diff_smoke.sh`）。P5 的验证手段是**元数据失效路径的帧级比对**（`ddl_diff_smoke.sh`、`live_diff_smoke.sh`）：负载混合 DDL 与 DML，检查并行路径产出的元数据相关消息与串行路径逐消息一致——快照内容、base 快照、快照切换与失效消息的位置只要错一个，字节比对就会失败。P6 的验证手段是**订阅端实测**（`streaming_smoke.sh`）：订阅端用 `streaming = 'parallel'` 建立订阅，检查大事务的变更在解码完成前就到达订阅端，且发布端硬重启后不丢数据。P7 的验证手段是**故障注入实测**（`failure_smoke.sh`）：分别让 decode worker 与 walreader 失败，检查发布端报出的是**带原因的错误**而不是笼统的"参与者意外死亡"，且会话**及时结束**而不是静默停滞；随后恢复正常配置，订阅端重连并把数据补齐。

覆盖的负载形态（每种都有一个独立的冒烟脚本）：

| 脚本 | 负载形态 | 验证目标 |
| --- | --- | --- |
| `frame_diff_smoke.sh` | 负载写在订阅建立**之前** | 追平已有 WAL 的基准情形 |
| `concurrent_diff_smoke.sh` | 6 个并发会话 × 8 个事务，事务间交错 | 多 worker 分配与提交顺序 |
| `live_diff_smoke.sh` | 负载在订阅**正在流式时**写入 | 解码器边建快照边接增量，不丢变更 |
| `ddl_diff_smoke.sh` | 负载混合 DDL 与 DML（改列、加表、建/删索引） | 元数据失效路径：并行路径的字节与串行路径一致 |
| `keepalive_smoke.sh` | 会话空闲 20 秒 | 空闲期 keepalive 的产出 |
| `worker_death_repro.sh` | 订阅在流式中途被删除 | 参与者生命周期与关停次序 |
| `streaming_smoke.sh` | 单个 4000 行（每行约 200 字节）的大事务，订阅端 `streaming = 'parallel'` | 尽早流式：分片提前到达订阅端、分片帧不交错、发布端硬重启后不丢数据（不变式 S1） |
| `failure_smoke.sh` | decode worker / walreader 分别被注入故障，其后恢复正常 | 故障恢复：报错带**原因**、会话及时终止而非静默停滞、订阅端重连后补齐数据 |
| `rel_dedup_smoke.sh` | 8 个并发会话 × 40 个事务（事务保持重叠，分布到 4 个 worker）的单表负载 | 关系描述只发一次：并行路径发 1 条 RELATION，与串行路径一致（6.7） |
| `slot_watermark_smoke.sh` | 单会话持续流式，等待 running-xacts 记录 | 槽水位推进：并行路径下 `restart_lsn` 与 `catalog_xmin` 与串行路径一样前进（6.6） |
| `monitoring_smoke.sh` | 4 worker × 8 并发会话 × 40 事务 | 监控指标（FR-13）：六个视图存在、会话与 worker 的计数非零、worker 的唤醒次数之和等于会话的上行唤醒次数、串行会话不出现在视图里 |
| `subxact_abort_smoke.sh` | 事务内 `ROLLBACK TO SAVEPOINT`，子事务的变更已被流式下发 | 子事务 abort（AC-14）：并行路径发出 STREAM ABORT，被中止子事务的行不落地，其后子事务的行完整落地，且与串行路径逐行一致 |
| `stream_order_smoke.sh` | 两个事务**同时**在途（铺开到两个 worker），第一个事务大且解码慢，第二个事务在第一个提交之后还在写 | 无 cseq 的帧的顺序（I-ORDER，7.4.1）：发布端**持有**了等待放行前沿的分片，且**没有**报出"分片越过了更早事务的提交"；结束事件按提交序到达订阅端；行数与内容一致 |
| `stream_all_txn_smoke.sh` | 5 个小事务（每个一行），订阅端 `streaming = 'parallel'`；随后 3 个小事务，订阅端 `streaming = 'on'` | 小事务也被流式化（8.1）：并行流式模式下**每个**事务都产生一个流段（发布端的 trace 里 STREAM START / STREAM STOP / STREAM COMMIT 各 5 个）；不请求并行流式的订阅端**一个流段都没有**（只有 BEGIN / COMMIT），串行路径的行为不变 |

实现过程中确定的规则与实测到的事实：

| 事项 | 结论 |
| --- | --- |
| cseq 的分配者 | walreader。它是唯一的 WAL 读取者，按 WAL 顺序为每个事务分配 cseq，因此 cseq 的顺序就是提交的顺序（7.1） |
| 事务与 worker 的绑定 | 多对一：事务在 walreader 侧一次性绑定到一个 worker，其后的所有变更都由该 worker 解码（4.4） |
| END_ACK 的上报条件 | worker 只在**自己拥有**的事务结束时上报 END_ACK；非拥有者不上报，避免重复推进放行水位 |
| 帧头 LSN | 实测并行路径与串行路径**逐消息**的帧头 LSN 完全相同：非最终写为 0，COMMIT 帧为提交记录 LSN。"钳制"（FR-20）在 P6 引入提前流式之前不会产生任何差异，因此比对工具把"钳制值更小"作为允许差异处理 |
| 关系消息重复 | 实测证实事实 6：6 个并发会话、48 个事务的单表负载下，串行路径发 1 条 RELATION，并行路径发 4 条（每个 worker 一份关系缓存）。订阅端覆盖缓存，语义正确，但纯属浪费。**已解决**（6.7）：同一负载下并行路径发 1 条，与串行路径一致，连跑 3 次结果稳定 |
| 关系描述的去重必须带 epoch | 去重表里若只有"发过了"这一个事实，DDL 之后已发过的定义会让后来的定义被跳过，订阅端就一直用旧结构解释新数据。因此表里的条目带 **epoch**（目录变化的代次，walreader 每次发布快照或广播失效就加一），**只有同一 epoch 内的定义才可能被复用**；epoch 更大的新定义必须送达 |
| 关系描述的去重必须排除流式事务 | pgoutput 在流式模式下按 (relation, top-xid) 追踪，本来就是每个流式事务重发一次关系描述；且流式分片没有 cseq，只受放行前沿约束（7.4.1），订阅端应用流式事务的顺序不由此处掌握 → 流式事务一律照发 |
| 事务的 worker 分配决定了负载能不能铺开 | worker 的选择是"当前在途字节最少的那个"（`pd_walreader_pick_worker()`），而字节在**事务结束**（提交记录）时才释放。因此**顺序写入的多个小事务会全部落在同一个 worker 上**——去重类与多 worker 类的测试负载必须让事务**同时在途**（并发会话 + 事务内短暂停顿），否则测不出多 worker 的行为 |
| 槽水位在并行路径下不推进 | 并行路径下 walreader 用的是槽的副本，`SnapBuildProcessRunningXacts()` 在 `SNAPBUILD_ROLE_WALREADER` 下提前返回 → 串行路径里推进槽水位的那两行代码一行也不执行 → 并行路径下槽的 `restart_lsn` 与 `catalog_xmin` **完全不前进**（实测 90 秒内不变），WAL 被无限保留。修法见 6.6：walreader 用自己的 reorder buffer 算出两个值，经 `PD_ENT_WATERMARK` 交给 walsender 应用 |
| 帧分组 | 并行路径把不同 worker 的消息混排进同一会话，帧分组与串行路径不同，属允许差异；提交顺序（BEGIN/COMMIT 的相对次序）**必须**一致，实测一致 |
| 追踪设施的 fd 生命周期 | `AllocateFile()` 注册的文件在**每次事务结束时**被 `fd.c` 关闭（`AtEOXact_Files()` → `CleanupTempFiles()`），且不检查 `fflush()` 的返回值。追踪文件必须用裸 fd，且每次 `write()` 都要检查返回值 |
| DDL 不被复制 | PostgreSQL **不复制 DDL**（`doc/src/sgml/logical-replication.sgml:1662`），`is_publishable_class()`（`pg_publication.c:137`）把系统表排除在可发布之外。订阅端的表必须**事先存在**，其形状由订阅端自己维护（复制的元组按列名匹配，订阅端多出来的列留空） |
| 订阅端的表清单在创建订阅时固定 | 订阅端的 `pg_subscription_rel` 在 `CREATE SUBSCRIPTION` 时按发布端的发布清单一次性写入；之后发布端新建的表**不在清单里**，订阅端会**静默丢弃**它的变更（`should_apply_changes_for_rel()` 读 `GetSubscriptionRelState()`，无行即 `SUBREL_STATE_UNDEFINED` → 跳过，`worker.c:467-498`、`relation.c:461-464`）。把新表纳入订阅的官方手段是 `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`（`worker.c:461-465` 的注释明确指出这一点） |
| 启动竞态：worker 尚未认领槽位时被判为"已死" | worker 在 `BackgroundWorkerInitializeConnectionByOid()` **之后**才写 `ctl->workers[i].pid` 与状态位（`parallel_decode_worker.c:441-443`），而 walsender 在会话建立后立刻进入取帧循环。旧代码用 `ctl->workers[i].pid`（未认领时为 0）判断存活，`ParallelDecodeProcessAlive(0)` 返回 false → 误报"worker 意外死亡"，并连带把整个会话打断。**必须用 walsender 自己从 `WaitForBackgroundWorkerStartup()` 拿到的 pid 判断存活**，并把"状态位仍为 `PD_WORKER_STATUS_UNUSED`"解释为"还在启动中" |
| 未用的块引用必须跳过 | `XLogReaderState` 的解码缓冲（64KB）是**环形复用**的：每条记录都复用同一块 `DecodedXLogRecord`，`blocks[]` 是变长数组，其未使用槽位的 `data` / `data_len` / `has_data` 保留着**上一条记录**留下的值（只有 `in_use` 是准确的，`xlogreader.c:1744` 会把中间的块号标记为未使用）。因此 `PDRecordSerialize()` 不能把 `max_block_id` 以内的**所有**槽位都序列化出去：未使用的槽位必须置 `data_len = 0` 且不复制数据，否则会把上条记录的残留指针当成块数据传下去，导致 walreader 在 `appendBinaryStringInfo()` 里用野指针崩溃（实测 `data = 0x7878787878787878`）。这是 P1–P4 就存在的缺陷，P6 的负载把它暴露出来 |
| 流式化的粒度 | 触发点在 `ReorderBufferCheckMemoryLimit()`：除了 `logical_decoding_work_mem` 阈值外，`rb->size` 达到 `parallel_decode_stream_segment_size` 且**当前事务可以流式**（快照已一致、`SnapBuildXactNeedsSkip()` 为假）时立即切一个分片。分片在 worker 侧收集（`STREAM START` → 若干变更 → `STREAM STOP`），整块通过 `PD_ENT_SEGMENT` 交给 walsender 发送，因此**一个分片的帧在 wire 上必然连续**——订阅端同时只接受一个流段（`worker.c` 的协议约束），分片交错会被拒绝 |
| **提交时也要流式化，否则小事务负载下一个流段都没有** | 触发点有两个：`ReorderBufferCheckMemoryLimit()`（进行中切段）与 `ReorderBufferStreamAtCommit()`（提交时兜底，由 `ReorderBufferReplay()` 调用）。旧语义下段大小默认 1MB，tpcc 那种 3KB 的事务永远攒不到段大小 → **一个流段都不产生** → 订阅端看不到 STREAM START → 并行 apply 根本不启动。现在 `immediate` 档把 `LogicalDecodingContext.stream_all_transactions` 打开：**没攒够段大小的事务在提交时作为一个流段发出**。四个前提缺一不可：路径是并行解码路径且会话被要求立即流式、事务此前没有被流式化（`rbtxn_is_streamed()`）、快照一致（`ReorderBufferCanStartStreaming()`）、**不是两阶段提交的准备事务**（`rbtxn_prepared()`——`ReorderBufferPrepare()` 与 `ReorderBufferFinishPrepared()` 都在回放之前就置上了 `RBTXN_PREPARE`，所以这一条同时覆盖了 prepare 记录本身） |
| **两个尺寸不再互相抬高** | `parallel_decode_stream_min_size` 现在是"能不能被流式化"的门槛（0 = 每个事务都被流式化），`parallel_decode_stream_segment_size` 只是分段点。旧的 `if (min_size > segment_size) segment_size = min_size;` 已删除——否则 `min_size > 0` 会把段大小也抬上去，让"低于门槛的事务整个不流式化"这条新语义无法成立 |
| 分片帧的 LSN 必须钳制 | 提前送达的分片帧与 keepalive 都携带 LSN，订阅端把它并入 `last_received` → 发布端据此抬高 `confirmed_flush`。若不钳制，`confirmed_flush` 会越过尚未放行的事务的提交记录，重启后整个事务被跳过（不变式 S1 被破坏）。因此 `'w'` 帧头的两个 int64 与 `'k'` keepalive 的 `end_lsn` 都要钳到**放行前沿 F**（`ParallelDecodeClampFrame()` / `ParallelDecodeClampLSN()`）；COMMIT **载荷**里的 `end_lsn` 不钳 |
| 放行前沿 F 的取值 | F = min{ 每个已分配但未放行的 cseq 的结束记录**起点** LSN }；集合为空时唯一合法取值是 **walreader 认证过的读取位置**（`PDCseqInfo.read_lsn`）。用终点会落在（起点, 终点] 区间内 → 重启后整个事务被跳过 |
| 故障必须由**状态字**定性，不能只靠"进程没了" | worker 在捕获错误后先把原因写进共享内存的状态字（先写 `errmsg` / `error_lsn`，再 `pg_memory_barrier()`，最后写 `PD_WORKER_STATUS_FAILED`），再把错误通过 `PD_ENT_ERROR` 送给 walsender。walsender 侧 `parallel_decode_report_worker_failure()` **先看状态字**：状态已是 FAILED 就报出**带原因**的错（"the parallel decode worker failed: …"），否则报"参与者意外死亡"。只靠消息传递会有竞态——DETACHED 路径可能在错误消息被读出之前就判定进程已死，于是原因丢失，退化成笼统的"意外死亡" |
| 故障注入必须放在**循环体内** | 最初的故障注入点在 `pd_worker_decode_loop()` / `pd_walreader_read_loop()` 的**顶部**，即在 `PG_TRY` 之外 → 错误直接终止进程，`PG_CATCH` 里的错误上报根本没机会执行。注入点移进循环体、位于 `PG_TRY` 之内，错误才会走"捕获 → 上报 → 会话结束"这条路径 |
| bgworker 退出码不会触发崩溃恢复 | `bgworker.c` 的顶层处理器捕获错误后 `EmitErrorReport()` 并 `proc_exit(1)`。退出码 1 **不是**崩溃退出码，postmaster 不会做崩溃恢复。因此 worker 失败必须由 walsender 主动感知并结束会话，不能指望 postmaster 来善后 |
| 通知 walsender 的通道 | worker 侧的 `bgw_notify_pid` 指向 walsender。postmaster 收到 worker 的退出通知后向该 pid 发 SIGUSR1，walsender 的 `procsignal_sigusr1_handler` 末尾 `SetLatch(MyLatch)` → 唤醒 `ParallelDecodeNextFrame()` 里的 `WaitLatch(MyLatch, …)`。注意本树的 latch 通知用的是 **SIGURG**（`storage/ipc/latch.c` 的 `kill(owner_pid, SIGURG)`），与 SIGUSR1 是两条不同的信号，不要混淆 |
| 停滞检测的三个必要条件 | 只在**同时满足**下列三条时才判定卡死，避免误报：(a) barrier 生效——有待放行的事务；(b) 某个 worker 的解码位置**落后于** walreader 的读取位置；(c) 该 worker 在 `parallel_decode_barrier_timeout` 秒内没有送来任何消息。worker 每 256 条记录上报一次位置心跳，作为 (c) 的活性信号。空闲的发布端不满足 (a)，因此不会误触发 |
| 故障后数据不丢 | 会话失败时数据仍在 WAL 里；订阅端重连后从复制槽的 `confirmed_flush` 续传。不变式 S1 保证 `confirmed_flush` 不会越过未放行事务，因此重连后这些事务会被重新解码并送达。实测：两次故障注入之后恢复正常配置，订阅端收到全部 100 行且内容正确 |
| walsender 的批量刷出 | 串行路径每帧一次 `pq_flush_if_writable()`（`walsender.c` 的 `WalSndWriteData()`），即每帧一次 `send()` 系统调用。并行路径改为**按批刷出**：`pd_walsender_write_frames()` 先阻塞取第一帧，再用新增的 `ParallelDecodeNextFrameNoWait()` **非阻塞**取随后的帧，凑够 `PD_FRAME_BATCH_SIZE`（8KB 帧载荷）或取不到为止，然后一次 `pq_flush_if_writable()` 把整批写出去。**帧本身不合并**——每帧仍是自己的 CopyData 消息，只是写出去的动作合并成一次系统调用。串行路径不动（AC-8） |
| 批的大小由"就绪的帧"决定，不是由 8KB 决定 | 8KB 只是**上限**：非阻塞取帧只能取到"此刻已经到达"的帧，生产者慢于消费者时批次会小于 8KB。因此实测到的每次写出的字节数取决于负载的产出速率，不能把 8KB 当作稳态值 |
| 批量刷出后的系统调用数（本机计数，非吞吐性能测试） | 同一负载（50 个事务 × 20 行 = 1000 个变更，4 个 decode worker），用 `strace` 统计 walsender 在复制 socket 上的 `sendto()` 次数：**改动前 1105 次 / 平均 98.6 字节每次，改动后 52 次 / 平均 2090 字节每次**（约 21×）。两次的协议字节总量一致（108,936 与 108,670，差值来自两次构建的帧内容差异）。**这只是系统调用计数，不是吞吐性能测试**；12.3 与 15.2 的降幅倍数仍待性能环境实测回填 |

| 快照构建器有三种角色 | 串行路径的 builder 由解码进程驱动（`SNAPBUILD_ROLE_SERIAL`）；并行路径里 **walreader 独占** builder（`SNAPBUILD_ROLE_WALREADER`），worker 用的是**只读替身**（`SNAPBUILD_ROLE_STANDIN`）。替身不构建快照、不分配 base 快照、不跟踪提交，只回答"本事务是否已有快照"——而这个答案来自 walreader 下发的 base 快照事件 |
| 替身的三个"不"是保证语义等式的关键 | `SnapBuildGetOrBuildSnapshot()` 在替身角色下直接返回下发的快照；`SnapBuildProcessChange()` 在替身角色下只回答"本事务是否有 base snapshot"；`SnapBuildCommitTxn()` 在替身角色下直接返回（目录变更的跟踪由 walreader 独占）。任何一条不返回，worker 就会**自行推进**快照状态机，与 walreader 的状态分叉 |
| 快照内容随消息下发，版本号必须单调递增 | 快照的**内容**（`xmin` / `xmax` / `curcid` / 已提交目录事务数组）由 walreader 连同消息一起下发（`PD_SNAPSHOT_CONTENT`），worker 收到后装进替身 builder。版本号必须**严格递增**：worker 收到版本号不大于已见版本的消息即报"out of order"——消息乱序会让 worker 用旧快照解码 |
| base 快照只发给事务**拥有者** | 事务的第一条数据记录通过门控（`SnapBuildProcessChange()`）时确定它的 base snapshot。此时 walreader 只把 base 快照事件发给**拥有该事务的 worker**（`PD_SNAPSHOT_BASE`），不发广播——别的 worker 不持有该事务，收到也没有用 |
| 快照切换与失效消息必须**同时**下发 | 目录修改事务提交时，walreader 先下发新快照的内容，再下发"切换"事件（`PD_SNAPSHOT_SWITCH`），后者携带该事务的失效消息。**拥有者**的 worker 随事务结束执行这些失效（与现状一致）；**其它** worker 立即执行（`ReorderBufferImmediateInvalidation()`）——每个 worker 的 catcache / syscache / relcache 是进程私有的，没有别的机制会替它失效 |
| 记录按事务**拥有者**路由 | walreader 对每条记录做"影子解码"（shadow decode）判定路由：`PD_FORWARD_OWNER`（只发给拥有该事务的 worker）、`PD_FORWARD_ALL`（广播，如 running-xacts 这类每个 worker 都要的记录）、`PD_FORWARD_NONE`（不发）。这比早期"全量广播 + worker 自行跳过"的实现省掉了一个数量级的重复解码工作，也去掉了"每个 worker 都看到全部记录"带来的元数据状态分叉 |
| walreader **不拥有**复制槽 | walreader 与 worker 用的是槽的**副本**（`pd_walreader_build_slot_copy()`），因此 walreader 侧的 snapbuild 不会推进槽的水位（`SnapBuildProcessRunningXacts()` 在 `SNAPBUILD_ROLE_WALREADER` 下提前返回，不调 `LogicalIncreaseXminForSlot()` / `LogicalIncreaseRestartDecodingForSlot()`）。槽的水位由 walsender（槽的唯一拥有者）推进，**跨进程聚合已实现**（6.6）：walreader 算出数值、经 `PD_ENT_WATERMARK` 交给 walsender 应用 |
| marker 让"空闲的 worker"也能报到正确的位置 | 记录按拥有者路由后，不持有任何事务的 worker 收不到任何记录，它的解码位置会永远停在 0，停滞检测的条件 (b) 因此永远成立。`PD_ENT_MARKER` 携带 walreader 的读取位置，worker 收到后**上报该位置**（而不是自己的位置）——会话的位置因此与串行路径一致地等于 walreader 的读取位置。marker 只在 walreader 侧产生，不改变 worker 的解码位置本身 |
| 监控计数只能有**一个**写者 | 早期实现里 worker 在事务结束时也递减 `in_flight_txns`，而 walreader 在同一处也在改它——两个进程写同一个共享计数器是数据竞争。改为：**在途事务数只由 walreader 写**（分配时加、事务结束时减），worker 只写自己那侧的 `records_consumed` / `decoded_txns` / `wakeups`。这条规则适用于所有跨进程计数器 |
| 监控指标必须走**主共享内存** | 计数器都在 dsa 里，SQL 进程读不到。本树没有 `shmem_request_hook` 扩展点，把 pgstat 框架搬一套进来改动面过大；改为复用已存在于主共享内存的 `WalSndCtl`：walsender 侧节流（100ms）把 dsa 里的值拷进 `WalSnd.pd_stats`，SRF 在 `walsnd->mutex` 下读。这样**不新增共享内存段**，也不动 `pgstat.c` 的新框架 |
| 视图里的计数是**累计量**，不是速率 | 帧数、字节数、socket 写次数、事务数、唤醒次数都是会话启动以来的累计值；速率由采样者做差分。15.2 的帧率与唤醒速率两项因此可以在性能机上直接对视图采样 |
| `parallel_decode_batch_size` 已接线 | 5.4 第 9 项的"按字节阈值合并批次、批次满即 force_flush"已实现：发送方（`PDQueueSender`）记"上次刷新以来写了多少字节"，写消息时记进去，记满一批就把该消息的 `force_flush` 置真。**批次未满但发送方要睡了**靠一条长度为 0 的 `PD_ENT_FLUSH` 消息表达——`shm_mq` 没有 notify API，接收方看不到尚未发布的数据，不显式刷新就会对着已经在环形缓冲里的数据空等。承载进度的消息（`PD_ENT_POSITION` / `PD_ENT_EVENT` / walreader 的控制消息）**不批量**，每次单独刷新。walreader 的批次按**跨队列**的字节总和计，否则只收到 marker 的 worker 的位置心跳会被拖到 64KB 才刷新一次。有效批大小被钳制在队列的 1/4 以内（`shm_mq` 在累计写入量达到环的 1/4 时自己发布并唤醒，批更大则发送方会等待接收方腾不出空间，而接收方恰恰看不到未发布的数据，构成死锁）。`parallel_decode_batch_size = 0` 关闭批处理，行为与接线前完全一致 |
| 唤醒次数的口径从"每条记录 / 每帧"改为"每次刷新" | 刷新才真的唤醒了对端，因此计数必须记在刷新上。计数点下沉到发送方层（`PDQueueSender` 持有一个指向共享内存计数器的指针），worker 的上行计数与 walreader 的下行计数都由此产生，`pg_parallel_decode_wakeups` 的上下行之和因此与 `pg_parallel_decode_workers.wakeups` 的和一致 |
| 批处理确实降低了唤醒次数（本机计数，非吞吐性能测试） | 同一负载（4 worker × 8 并发会话 × 40 事务）跑两遍：一遍 `parallel_decode_batch_size = 0`（关闭批处理），一遍 64KB。**上行唤醒（worker → walsender）从 1338 次降到 417 次，下行唤醒（walreader → worker）从 969 次降到 363 次**，两次的帧数同阶（874 与 850）。断言写在 `monitoring_smoke.sh` 里。**这只是唤醒次数计数，不是吞吐性能测试**；12.3 与 15.2 的降幅倍数仍待性能环境实测回填 |
| 子事务 abort 走的是 reorder buffer 的既有机制 | 设计上曾打算为子事务 abort 单开一条通道（`PD_ENT_SUBXACT_ABORT` / `PD_BLK_ABORT`），但 worker 用的是**真实的 reorder buffer**，`ReorderBufferAbort()` 在事务已被流式下发时会调用 `rb->stream_abort()`，STREAM ABORT 由插件按正常路径产出。因此那条独立通道**没有实现，也没有必要实现**——`PD_ENT_SUBXACT_ABORT` 只是枚举里一个未使用的值。实测（`subxact_abort_smoke.sh`）：并行路径发出 1 条 STREAM ABORT，被中止子事务的行没有落地，其后子事务的行完整落地，与串行路径逐行一致 |

P5 的**关系描述去重**（6.7）与 6.6 的**槽水位跨进程聚合**均已实现：关系描述由 **worker 标记、walsender 按放行序判定**去重（判定规则见 6.7，标记机制见 `LogicalDecodingContext.rel_definition_hook` 与消息类型 `PD_ENT_RELATION`），槽水位由 walreader 用自己的 reorder buffer 算出、经 `PD_ENT_WATERMARK` 交给 walsender 应用（见 6.6）。两者的验证脚本分别是 `rel_dedup_smoke.sh` 与 `slot_watermark_smoke.sh`。

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

**会话的拆卸。** 无论是订阅端断开还是正常的会话结束，walsender 都要把并行解码会话拆掉（`ParallelDecodeSessionStop()`）。拆卸的顺序是**生产者先停**：

1. 置 shutdown 标志。它是"这个会话结束了"的唯一信号，所有进程都靠它退出循环；
2. **先终止 walreader**。它是唯一的 WAL 读取者，也是记录的生产者；先停它，worker 才不会留下来等一条永远不会到达的记录；
3. **再终止 decode worker**。它们是消费者，把帧交给 walsender；walsender 在等待它们的时候会把输出队列排空，因此一个正卡在"交出帧"上的 worker 能够把这次交接做完，然后看到 shutdown 标志；
4. **逐个等待进程真的退出**。共享内存段一旦释放，worker 往里写就会踩到别人的内存，所以终止之后必须等它们真的没了，才能释放共享内存段；
5. 释放会话期间积压的字节：待发送队列里的块、等待放行前沿的块（cseq == 0 的帧）、正在交接的那个块、以及放行前沿本身。它们都属于本会话的事务，会话结束就没有意义了。

第 2 步与第 3 步的顺序**不能颠倒**：先停 worker 的话，walreader 还会往一个已经不再消费的队列里写记录，于是 walreader 卡在队列满上，session stop 卡在等 walreader 退出上。

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
| **跳过判据受控** | 重启时 `start_decoding_at = confirmed_flush`，**结束记录起始 LSN 小于它的事务会被整体跳过**（`decode.c:1302` 传 `buf->origptr` → `snapbuild.c:443-446` 比 `ptr < start_decoding_at`）。因此必须保证不变式 S1（4.1.3）：已送达帧的 LSN 不得超过放行前沿 F，而 F 用的是**结束记录起始 LSN** |
| 至少一次语义 | **重发 = 至少一次**：已放行但未被订阅端确认（`confirmed_flush` 未覆盖）的事务会被重发。**去重靠发布端"跳过 `confirmed_flush` 之前的事务"实现（即不变式 S1），而不是让订阅端重复 apply**——PG 靠 replication origin 的 `remote_lsn`（`worker.c:2267` `replorigin_session_origin_lsn = commit_data->end_lsn`、`worker.c:4500` `origin_startpos = replorigin_session_get_progress(false)`）保证不重复 apply。**S1 一旦被破坏，重复 apply 才会真的发生** |
| 会话重建后订阅端状态复位 | 被流式化的事务在会话中断后处于"半个事务"状态，重连后必须整段重发。订阅端的 `in_streamed_transaction` / `stream_xid` 状态必须复位（`worker.c:302-309`），否则第二个 `STREAM START` 会触发 `ereport(ERROR, "duplicate STREAM START message")`（`worker.c:1476-1479`）。**"尽早流式"意味着同一个事务的多个流段可能在多次会话中被反复重发**，必须验证订阅端每次都能干净复位 |
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
9. **walreader 必须维护 `current_restart_decoding_lsn` 的本地副本**，并在创建事务绑定时把副本值写入绑定表。`ReorderBufferSetRestartPoint()` 的目标改为 walreader 自己的 reorder buffer；**worker 不得依赖 `txn->restart_decoding_lsn`**（它恒为 `InvalidXLogRecPtr`，见 6.1、6.6.2）。
10. **放行前沿 F 必须取结束记录的起始 LSN**（`buf->origptr`），且**逐帧钳制**——帧头两个 int64 一起钳，keepalive 的两个 LSN 来源都要钳（4.1.3、7.4）。
11. **子事务 abort 必须及时下发**：不占 cseq、不受 cseq 门控，但**受放行前沿约束**（不得越过已经结束的事务的结束事件，7.4.1），独占一块，且必须早于其后子事务的变更到达订阅端（5.2.2、5.2.3）。
12. **worker 必须拿到原始插件选项列表**（`PD_ENT_SESSION_INIT`），并据此实例化自己的 output plugin——模式值在 `PGOutputData.streaming` 里，`ctx->streaming` 只是 bool（事实 8c、6.1）。

P7 已落实其中的 1、2、3、5、6、7 条：worker 的错误在循环内的 `PG_TRY` / `PG_CATCH` 中被捕获，经 `PD_ENT_ERROR` 上报并写入共享内存的状态字；`bgw_restart_time = BGW_NEVER_RESTART`、`bgw_notify_pid` 指向 walsender；barrier 等待带 `parallel_decode_barrier_timeout` 超时，worker 另以每 256 条记录一次的位置心跳作为活性信号；walreader 死亡立即使本会话的 cseq 作废。第 4 条（cseq 不持久化）与第 8–12 条在 P1–P6 中已满足。

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
| v0.9 | 2026-11-18 | **修正一轮正确性评审发现的 10 条未定义路径**。(1) **帧头 LSN 清单更正**：v0.8 把 `STREAM START` / `STREAM STOP` 写成"写 0"是错的——`pgoutput.c:1799` 的 `last_write = !send_replication_origin`、`:1823` 恒为 `true`，两者都写真值（分别是事务首变更 LSN 与**段内 max**），据此推导出的钳制范围会漏掉最危险的两个帧（4.1.3 新增逐消息清单）。(2) **放行前沿 F 钉到结束记录的"起点"**：`SnapBuildXactNeedsSkip()` 比较的是 `buf->origptr`（`decode.c:1302` → `snapbuild.c:443-446`），用终点（`buf->endptr`，也是帧头里写的那个）会让 `confirmed_flush` 落在（起点, 终点] 区间内 → 重启后整个事务被跳过 → 丢变更（4.1.3、7.4）。(3) **F 集合为空时的兜底改正**：v0.8 用"会话开始时的 `confirmed_flush`"兜底，而集合为空是**稳态常态**（发布端空闲），会把前沿永久冻结在会话起点 → `confirmed_flush` 被拉回 → 破坏单调性并造成重连后的全量重投；改为"**walreader 认证过的读取位置**"，并删掉被禁止的两种兜底（7.4）。(4) **补上取样次序与单调性约束**：放行水位 → 取样 F → 写帧；钳制后的帧 LSN 不小于已放行事务的结束记录终点（7.4）。(5) **补上钳制安全性的论证**：订阅端只把帧头 LSN 用于 `last_received`，变更载荷不含变更 LSN；`COMMIT` 载荷里的 `end_lsn` 不钳且其安全性有论证（7.4）。(6) **keepalive 的第二个 LSN 来源**：跳过空事务时的 `ctx->write_location`，同样要钳（4.1.3）。(7) **新增子事务 abort 的完整路径**：`ROLLBACK TO SAVEPOINT` 写的是独立的 `XLOG_XACT_ABORT`（`xl_xid` = 子 xid），订阅端对它做 spool 截断（`worker.c:1791-1796`）并连带丢弃其后子事务的有效变更 → 必须即时下发、不占 cseq、不受门控、独占一块（5.2.2 新增 `PD_ENT_SUBXACT_ABORT` 与 `PD_BLK_ABORT`、5.2.3、7.1）。(8) **顶层 abort 与子事务 abort 的区分规则**及其"N 条子 + 1 条顶层 `STREAM ABORT`"的门控归属（7.1）。(9) **xmin 兜底改为 `min(各 worker 最后上报值, oldestRunningXid)`**：只用 `oldestRunningXid` 会让 `catalog_xmin` 比安全值更激进 → 提前回收在途事务快照仍需要的系统表元组 → 会话反复失败（6.6.1）。(10) **`restart_lsn` 的推进判据整体替换**并给出可实现的协议（绑定表 + `END_ACK`），不再以 `confirmed_flush` 为门控——否则 `restart_lsn` 长期不推进、WAL 无限保留；补上 **walreader 维护 `current_restart_decoding_lsn` 本地副本**的硬约束，worker 不得依赖 `txn->restart_decoding_lsn`（6.6.2、17.8）。(11) **补上订阅端侧故障**（断连 / apply worker 崩溃 / 整库重启）与"会话重建后订阅端流式状态复位"，并**更正"至少一次语义"的表述**：去重靠发布端跳过 `confirmed_flush` 之前的事务（S1），不是让订阅端重复 apply（9、17.7）。(12) **事实 8c 更正**：`LogicalDecodingContext.streaming` 是 **bool**，模式值在 `PGOutputData.streaming`（char）里、由插件选项列表解析而来 → worker 必须拿到**原始插件选项列表**而不是模式值（2.4、5.2.2、6.1）。(13) **§16 新增帧级 dump / diff 工具与黄金文件**作为 P0 的前置产出（16） |
| v0.10 | 2026-11-18 | **修正性能分析的量化错误与漏项**（依据一轮专门的性能审查）。(1) **统一 70MB/s 的口径**：§1 原写"以 `confirm_flush_lsn` 的速率为准"，与 G1/AC-1/§12.1 的"发布端实际发出的协议字节速率"矛盾，已统一为后者。(2) **重写 §15.1 的成本表**：原表三行相加为 53–125 µs 而文中写 75–125 µs；行标题"每变更/每列"与列标题"每事务"混用单位。已统一为"每事务小计"并写明每事务 25 变更 / 320 列的假设，单核上限修正为 8k–19k 事务/秒。(3) **引入常数 B（每事务协议字节数）与 C（每事务帧数）**：原版用"每事务约 3KB **WAL**"去换算 G1 的 **协议字节**速率，口径混用、方向性误差约 2.6 倍。已按协议字节口径重算：70MB/s ≈ 8.75k 事务/秒 ≈ 26 万帧/秒；并指出真正的约束是**帧率**而非字节率。(4) **42 万行/秒改写为需求值**，给出 apply worker 数下限公式 `ceil(42万 ÷ C)`，并注明 C 待实测、按 1 万–10 万变更/秒估算下限落在 4–40、默认 2 大概率不足（§12.1、§15.1、Q10、NFR-1）。(5) **补上 `immediate` 档的 RELATION 重发放大**：pgoutput 在流式模式下按 top-xid 追踪并重发关系描述（`pgoutput.c:743/767/1887`），每事务为其触及的每张表重发一次 RELATION，tpcc 约 5 张表即每事务多约 5 帧 / 约 3KB，**协议字节 +40%–60%、帧数 +19%**——这是此前完全漏算的 `immediate` 档主要代价（§8.1、§12.3、§14）。(6) **拆开"帧率"与"系统调用率"**：原版在 §12.3 与 §15.1 之间自相矛盾（一边说批量刷出把 `write()` 降 20–40 倍，一边说每帧 `write()` 是最硬的串行天花板）。批量刷出只降 syscall，不降帧数、不降订阅端每帧解析、不降 FR-20 逐帧钳制；已拆成两条独立风险行，降幅按 `PQ_SEND_BUFFER_SIZE=8192` 修正为约 47 倍（静态估算）。(7) **§12.3 与 §14 各补三行**：walreader 的每条 WAL 记录成本（唯一 WAL 读取者，WAL 记录数是解码变更数的 2–4 倍）、walsender 的 FR-20 逐帧钳制（O(changes)，并行化后新增而非消除）、**订阅端 leader 的每帧派发**（`PQgetCopyData` + `apply_dispatch` + `pa_send_data`，O(changes)，**发布端无法缓解**，因设计边界是"只改发布端"）。(8) **改写 §8.1 关于 O2 的表述**：O2 限制的是交错粒度与时延，**不构成吞吐上限**——wire 本就是单条 TCP 流。(9) **Q10 拆成两个数**：解码侧 N = 3~4（按修正后的成本表），订阅端 apply worker 数按目标变更率反推。(10) **§15.2 扩充为 12 项**：新增每事务协议字节数 B / WAL 记录数 / RELATION 重发放大倍数 / 单 apply worker 容量 C / 订阅端 leader 每帧成本 / walsender 逐帧钳制成本 / walreader 每记录成本 / 系统调用降幅 / 单帧 > 8KB 比例共 9 项，并注明**本机只做功能与正确性验证，这些数值须在性能环境实测后回填**，回填前不得当作结论。(11) 修正源码引用行号（`guc_tables.c:3190-3199` → `3263-3272`，`guc_tables.c:5126-5136` → `5228-5236`）。`need.md` 同步 NFR-1 与流式化代价行 |
| v0.8 | 2026-11-18 | **修正本特性自身的正确性与性能分析漏洞**。(1) **新增事实 8c**：`STREAM ABORT` 的载荷随订阅端流式模式而变（`pgoutput.c:1842`、`proto.c:1166-1184`：v2/v3 为 8 字节、v4 为 24 字节），worker 必须按**模式值**而非布尔值初始化 `ctx->streaming`——新增下行条目 `PD_ENT_SESSION_INIT`（5.2.2）并在 6.1 的插件实例化处点名。(2) **补上 keepalive 这条数据丢失路径**：`'k'` 消息的 end_lsn 取 `sentPtr` = 发布端的 WAL 读取位置（`walsender.c:3443`、`:4071`），订阅端照样并入 `last_received`（`worker.c:3608-3612`）→ `confirmed_flush` 被抬到未放行事务之上 → 重启后丢事务。因此 C9 与 §7.4 的钳制**必须同时覆盖 `'w'` 帧头与 `'k'` keepalive**。(3) **放行前沿口径唯一化**：恒等于 cseq = 放行水位 + 1 的事务的提交记录 LSN；未知时唯一合法取值是会话开始时的槽 `confirmed_flush`，**禁止**用当前 WAL 读取位置（原表述「前沿取当前 WAL 读取位置」易被误实现而破坏 S1）。(4) **修正流式门槛的默认值**：`parallel_decode_stream_min_size` 由 64KB 改为 **0**——15.1 的测算里 tpcc 每事务仅约 3KB，64KB 门槛会使**所有事务都不流式化**，订阅端永远收不到 STREAM START、并行 apply 根本不启动，70MB/s 目标与整个前提同时落空。同时**推翻了「过度流式化」的两项代价核算**：流式化不改变「每条变更一帧」，只多出每事务 2 帧（约 5%–10%）；订阅端每事务一个 apply worker 是**并行的机制本身**而非开销。(5) **流式激进程度按订阅端模式区分**：`parallel_decode_streaming_mode` 新增 `auto` 档并作为默认值——`streaming='parallel'` → `immediate`，`streaming='on'` → `buffered`（后者没有 apply worker，流式化会把它推到 `TRANS_LEADER_SERIALIZE` 的 spool 慢路径，对存量用户是性能回归）。(6) **apply worker 数不能只写「> 0」**：订阅端能并行回放的事务数 = `max_parallel_apply_workers_per_subscription`（默认 2，`launcher.c:412` 超限即拒绝启动），超出部分退化为 spool 慢路径；§3.5、§12.1、§12.3、§13、§14 同步为「按目标速率反推下限」。`need.md` 同步 || v0.11 | 2026-11-18 | **P0–P4 实现完成，§16 新增 16.1 实现状态**。记录实现过程中确定的规则（cseq 由 walreader 分配、事务与 worker 多对一绑定、END_ACK 只由事务拥有者上报）与实测事实（并行路径与串行路径的帧头 LSN 逐消息相同；关系消息按 worker 重复，实测 1 条 vs 4 条，证实事实 6；帧分组可不同但提交顺序必须一致；`AllocateFile()` 的文件在每次事务结束时被关闭且 `fflush()` 返回值未检查，追踪设施必须改用裸 fd）。帧级比对工具按 PG18 的 wire 格式重写（cstring 字符串、`'R'` 消息无长度前缀）。 |
| v0.12 | 2026-11-18 | **修复一个会话启动竞态，并澄清两条复制语义**。(1) **启动竞态**：walsender 在会话建立后立刻进入取帧循环，用控制块里的 `workers[i].pid` 判断 worker 存活；但 worker 要到 `BackgroundWorkerInitializeConnectionByOid()` 之后才写这个字段，未认领槽位时它是 0，`ParallelDecodeProcessAlive(0)` 返回 false → 误报"the parallel decode worker died unexpectedly"并打断会话。改为用 walsender 自己从 `WaitForBackgroundWorkerStartup()` 拿到的 pid 判断存活，并把状态位仍为 `PD_WORKER_STATUS_UNUSED` 解释为"仍在启动"；同时补上 walreader 的存活检查（此前 walreader 死亡会让会话静默停滞）。实测：16 个 worker 时旧代码 5 次运行里 4 次触发，修复后 8 次全部通过。(2) **DDL 不被复制**（`pg_publication.c:137` 排除系统表），订阅端的表必须事先存在。(3) **订阅端的表清单在 `CREATE SUBSCRIPTION` 时固定**，发布端之后新建的表不在 `pg_subscription_rel` 里，订阅端会静默丢弃其变更；纳入订阅的官方手段是 `ALTER SUBSCRIPTION ... REFRESH PUBLICATION`。§16.1 补入这两条事实与负载形态表；新增 `live_diff_smoke.sh`、`ddl_diff_smoke.sh`、`worker_death_repro.sh` 三个冒烟脚本 |
| v0.13 | 2026-11-18 | **P6（尽早流式与帧头钳制）实现完成，并修复一个 P1–P4 就存在的记录序列化缺陷**。(1) **尽早流式**：新增 `parallel_decode_streaming_mode`（`auto` / `immediate` / `buffered`）、`parallel_decode_stream_segment_size`、`parallel_decode_stream_min_size` 三个 GUC；流式模式在 `CreateDecodingContext()` 之前按订阅端的插件选项解析（`ParallelDecodeResolveStreamingMode()`，`auto` 按 `streaming='parallel'` → `immediate`、`streaming='on'` → `buffered`）；切分触发点放在 `ReorderBufferCheckMemoryLimit()`——`rb->size` 达到分片大小且当前事务可流式时立即切一个分片；分片在 worker 侧收集并整块通过新增的 `PD_ENT_SEGMENT` 条目交给 walsender，保证一个分片的帧在 wire 上连续（订阅端同时只接受一个流段）。(2) **帧头钳制（FR-20 / C9）**：walsender 发送 `'w'` 帧前调用 `ParallelDecodeClampFrame()`，把帧头的两个 int64 钳到放行前沿 F；`WalSndKeepalive()` 的 `end_lsn` 同样经 `ParallelDecodeClampLSN()` 钳制；COMMIT 载荷的 `end_lsn` 不钳。F = min{ 已分配未放行 cseq 的结束记录起点 LSN }，集合为空时取 walreader 认证过的读取位置（`PDCseqInfo.read_lsn`）。(3) **修复未使用块引用的序列化缺陷**：`XLogReaderState` 的解码缓冲是环形复用的，`DecodedXLogRecord.blocks[]` 中未使用槽位的 `data` / `data_len` 保留着上一条记录的残留值，`PDRecordSerialize()` 把这些槽位一并序列化后，walreader 会用野指针（实测 `data = 0x7878787878787878`）在 `appendBinaryStringInfo()` 里崩溃。改为 `!blk->in_use` 的槽位置 `data_len = 0` 且不复制数据；该缺陷在 P1–P4 就存在，用 P6 的负载复现并定位。(4) **新增 `streaming_smoke.sh`**：订阅端 `streaming = 'parallel'`，单个 4000 行大事务，检查分片真的产生（STREAM START / STREAM STOP 数量相等）、行数与内容正确、发布端硬重启（`-m immediate`）后不丢数据。(5) §16.1 实现状态扩到 P0–P6，补入上述规则与事实。**尚未实现：P5（快照与失效共享、共享关系表）、P7（故障恢复）** |
| v0.14 | 2026-11-18 | **P7（故障恢复）实现完成**。(1) **故障上报**：decode worker 与 walreader 的主循环都包在 `PG_TRY` / `PG_CATCH` 中，捕获错误后把原因写入共享内存的状态字（先写 `errmsg` 与 `error_lsn`，再 `pg_memory_barrier()`，最后写 `PD_WORKER_STATUS_FAILED`），并通过 `PD_ENT_ERROR` 条目把错误送给 walsender；walsender 侧的 `parallel_decode_report_worker_failure()` 先看状态字——状态是 FAILED 就报出**带原因**的错误（"the parallel decode worker failed: …"），否则才报"参与者意外死亡"。(2) **停滞检测**：barrier 生效（有待放行事务）+ 该 worker 的解码位置落后于 walreader 的读取位置 + 该 worker 在 `parallel_decode_barrier_timeout`（默认 60 秒）内无任何消息——三者同时成立才判定卡死并结束会话；worker 每 256 条记录上报一次位置心跳作为活性信号，空闲的发布端不满足第一个条件，因此不会误触发。(3) **通知通道**：`bgw_notify_pid` 指向 walsender，postmaster 向该 pid 发 SIGUSR1，walsender 的 `procsignal_sigusr1_handler` 末尾 `SetLatch(MyLatch)` 唤醒取帧循环（注意本树的 latch 通知用的是 SIGURG，与 SIGUSR1 是两条不同信号）。(4) **退出码约束**：`bgworker.c` 的顶层处理器捕获错误后 `EmitErrorReport()` 并 `proc_exit(1)`，退出码 1 不触发集群崩溃恢复，因此 worker 失败必须由 walsender 主动感知并结束会话；`bgw_restart_time = BGW_NEVER_RESTART`。(5) **修复一个 P1–P6 就存在的 use-after-free**：`pd_walreader_classify()` 在 `hash_search(HASH_REMOVE)` 释放 `PDTxnInfo` 之后仍解引用 `info->worker` / `info->bytes`；改为在释放前取出 worker 索引与 `ended` 标志。(6) **故障注入开关** `parallel_decode_fault_inject`（0 / 1 = worker 失败 / 2 = walreader 失败），注入点置于循环体内、`PG_TRY` 之内——最初放在循环外，错误直接终止进程，`PG_CATCH` 的上报根本没机会执行。(7) **新增 `failure_smoke.sh`**：分别注入 worker 与 walreader 故障，检查发布端报出带原因的失败、会话及时终止，随后恢复正常配置验证订阅端重连并补齐数据（100 行）。§16.1 实现状态扩到 P0–P7，§11.1 补入两个新 GUC，§17.8 补入 P7 的落实情况。**尚未实现：P5（快照与失效共享、共享关系表）** |
| v0.15 | 2026-11-18 | **walsender 批量刷出实现完成**（12.3 风险表 "walsender 每帧一次 `write()` 系统调用" 的对策落地）。(1) **实现**：`pd_walsender_write_frame()` 不再逐帧 `pq_flush_if_writable()`，帧先进连接输出缓冲；新增 `pd_walsender_write_frames()`——阻塞取第一帧，再用新增的非阻塞取帧接口 `ParallelDecodeNextFrameNoWait()` 取随后的帧，凑够 `PD_FRAME_BATCH_SIZE`（8KB 帧载荷）或取不到为止，最后一次刷出。**帧不合并**，每帧仍是自己的 CopyData 消息；串行路径不动（AC-8）。(2) **新接口**：`ParallelDecodeNextFrameNoWait()` 与阻塞版同源同语义（放行前沿、帧序、cseq 门控一致），只是不等帧；单次调用最多处理 64 条 worker 消息，避免进度消息把取帧拖长。(3) **实测（本机计数，非吞吐性能测试）**：同一负载（50 事务 × 20 行，4 worker）下，walsender 在复制 socket 上的 `sendto()` 次数由 **1105 次（平均 98.6 字节/次）** 降到 **52 次（平均 2090 字节/次）**，约 21×；协议字节总量一致。批次大小实际由"此刻就绪的帧"决定，8KB 只是上限——生产者慢时批次更小，故 12.3 的降幅倍数仍待性能环境实测回填。 |
| v0.16 | 2026-11-18 | **P5（快照与失效共享）实现完成**。(1) **快照构建器的三种角色**：`SNAPBUILD_ROLE_SERIAL` / `SNAPBUILD_ROLE_WALREADER` / `SNAPBUILD_ROLE_STANDIN`。walreader 独占真实 builder 并驱动它，worker 用只读替身；替身在 `SnapBuildGetOrBuildSnapshot()` / `SnapBuildProcessChange()` / `SnapBuildCommitTxn()` / `SnapBuildProcessRunningXacts()` / `SnapBuildDistributeSnapshotAndInval()` 五处提前返回，不自行推进快照状态机——这是"并行路径与串行路径元数据一致"的前提。(2) **快照的共享协议**：快照内容（`xmin`/`xmax`/`curcid`/已提交目录事务数组）随 `PD_SNAPSHOT_CONTENT` 消息下发并带**严格递增**的版本号（乱序即报错）；base 快照事件（`PD_SNAPSHOT_BASE`）**只发给事务的拥有者**；目录修改事务提交时先下发新快照内容、再下发切换事件（`PD_SNAPSHOT_SWITCH`，携带该事务的失效消息），拥有者的 worker 随事务结束执行失效，其它 worker 立即执行（`ReorderBufferImmediateInvalidation()`）——worker 的 catcache/syscache/relcache 是进程私有的。(3) **记录按拥有者路由**：walreader 对每条记录做"影子解码"判定路由（`PD_FORWARD_OWNER` / `PD_FORWARD_ALL` / `PD_FORWARD_NONE`），只有拥有该事务的 worker 才收到它的记录，取代了早期"全量广播 + worker 自行跳过"。(4) **新增 `PD_ENT_MARKER`**：walreader 每读满 `PD_POSITION_MARK_RECORDS` 条记录、或 WAL 暂时读完时，把自己的读取位置下发给 worker；worker 收到后**上报该位置**，使会话的位置与串行路径一致地等于 walreader 的读取位置（记录按拥有者路由后，不持有事务的 worker 否则会永远停在 0，停滞检测的条件 (b) 会一直成立）。(5) **修复三处崩溃/缺陷**：`resetStringInfo()` 用在未初始化的局部 `StringInfoData` 上（三个 publish 函数），改为 `initStringInfo()` + `pfree()`；`ParallelDecodeClampLSN()` / `ParallelDecodeClampFrame()` 的前沿判据写反（从前沿无效时不钳制变成从不钳制），且帧头 LSN 是**网络字节序**（`pq_sendint64()`），钳制时必须 `pg_ntoh64()` / `pg_hton64()`——写反时会把 202 个帧头的 LSN 写成乱码。(6) **本机功能与正确性验证**（非性能测试）：`frame_diff_smoke.sh` / `ddl_diff_smoke.sh` / `live_diff_smoke.sh` / `concurrent_diff_smoke.sh` / `streaming_smoke.sh` / `keepalive_smoke.sh` / `failure_smoke.sh` 七个脚本全部通过。**尚未实现：P5 的共享关系表**（每 worker 一份关系缓存，实测 6 会话负载下串行 1 条 RELATION / 并行 4 条），以及 6.6 的槽水位跨进程聚合 |
| v0.17 | 2026-11-18 | **P5 的共享关系表与 6.6 的槽水位跨进程聚合实现完成**。(1) **共享关系表（RELATION 去重，6.7）**：并行路径下每个 decode worker 各有一份 pgoutput 的关系缓存，同一张表的关系描述被重复下发（实测：8 并发会话 × 40 事务、4 worker 的单表负载下，串行路径发 1 条 RELATION、并行路径发 4 条）。修法是在 walsender 创建的 dsa 控制块里放一张固定 2048 槽位、由自旋锁保护的开放式定址哈希表（`ParallelDecodeControl.rels`），条目为 `{relid, cseq, epoch}`。判定规则：表里没有该关系 → 发并记下；同一 epoch 且表里的提交序号更小 → **不发**（事务按 cseq 顺序到达订阅端，更早的定义必然先到）；epoch 不同 → 发；自己的 cseq 为 0（不可排序）或属于流式事务 → 一律发（pgoutput 在流式模式下本来就每个流式事务重发一次关系描述，且流式分片绕开 cseq 顺序）。**epoch 是目录变化的代次**，walreader 每次发布快照内容或广播失效就加一并随消息下发——没有它，DDL 之后先前的 worker 会让后一个 worker 跳过关系描述，订阅端就一直用旧结构解释新数据。查表做成通用钩子 `LogicalDecodingContext.rel_announce_hook`（NULL = 总是发），pgoutput 的 `maybe_send_schema()` 在非流式时调用它，因此**串行路径一行不改**。(2) **槽水位跨进程聚合（6.6）**：并行路径下 walreader 用的是槽的副本，`SnapBuildProcessRunningXacts()` 在 `SNAPBUILD_ROLE_WALREADER` 下提前返回 → 串行路径里推进槽水位的那两行代码一行也不执行 → 槽的 `restart_lsn` 与 `catalog_xmin` **完全不前进**（实测 90 秒内不变），WAL 被无限保留。修法不是 worker 周期上报再取最小值，而是**让 walreader 侧保留一份记录全部事务的 reorder buffer**（`pd_rb`）：`ReorderBufferGetOldestXmin(pd_rb)` 与 `ReorderBufferGetOldestTXN(pd_rb)` 取到的正是串行路径在同一位置会取到的值。walreader 在读到 `XLOG_RUNNING_XACTS` 记录时算出两个值，经控制通道的新消息 `PD_ENT_WATERMARK` 交给 **walsender**（槽的唯一拥有者）调用串行路径同一对 `LogicalIncreaseXminForSlot()` / `LogicalIncreaseRestartDecodingForSlot()`。**推进判据沿用串行路径的候选机制、不做替换**：候选挂在 running-xacts 记录的 LSN 上，等订阅端确认越过它才生效；放行前沿 F 在全部事务放行之后等于 walreader 认证过的读取位置，必然越过该 LSN，因此不会出现候选永远挂着、WAL 无限保留。(3) **本机功能与正确性验证**（非性能测试）：新增两个冒烟脚本——`rel_dedup_smoke.sh`（8 并发会话 × 40 事务、事务保持重叠以铺开到 4 个 worker，统计 trace 里的 RELATION 条数）与 `slot_watermark_smoke.sh`（等槽的水位前进，并与串行路径对照）。**A/B 实测**（同一负载，改动前 → 改动后）：RELATION 条数 **4 → 1**；槽的 `restart_lsn` **90 秒内不变 → 14 秒后前进**（`0/14E5430` → `0/14E8728`，与串行路径的实测值一致），`catalog_xmin` 同步前进（740 → 741）。原有七个冒烟脚本（`frame_diff_smoke.sh` / `ddl_diff_smoke.sh` / `live_diff_smoke.sh` / `concurrent_diff_smoke.sh` / `streaming_smoke.sh` / `keepalive_smoke.sh` / `failure_smoke.sh`）连同新加的两个**全部通过**。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.18 | 2026-11-18 | **RELATION 去重的方案重做：由"共享表 + worker 自行判定"改为"worker 标记 + walsender 按放行序判定"**。(1) **旧方案不成立**：v0.17 把去重表放在 dsa 控制块里、让 worker 在发关系描述前查表。问题在于**判定依据不成立**——worker 之间没有 cseq 序的先后关系，持有更大 cseq 的 worker 完全可能先到达并发出定义；此时一个更小 cseq 的事务若按"表里已有就跳过"处理，订阅端会先应用这个小 cseq 的事务，而它**还没有**那份定义。旧规则在语义上安全（最坏是多发一条，绝不会少发），但**不是重复消除**：实测出现 2 条 RELATION 而非 1 条，取决于哪个 worker 先到达。(2) **新方案**：判定移到 walsender——它是唯一掌握 cseq 放行序的进程。实现分三步：pgoutput 在写关系消息**之前**调用解码上下文上的标记钩子 `rel_definition_hook(ctx, relid)`（pgoutput 侧新增 `send_relation_and_attrs()` 的 `announce` 参数，只在非流式时调用）；worker 记下待标记的 relid 与 epoch，把该帧用**新消息类型 `PD_ENT_RELATION`**（头部多带 `{end_lsn, cseq, epoch, relid, length}`）而非 `PD_ENT_FRAME` 发出——因此普通帧的头部与原来完全一样，不为去重付额外代价；walsender 放行事务的帧时，若该帧是关系描述且该关系在**同一或更新的 epoch** 里已发过，就丢弃该帧。由于放行序就是 cseq 序，最早碰到该表的事务的定义一定最先送达，此后全部丢弃 ⇒ 同一关系每个 epoch **恰好发一条**。(3) **共享内存里不再有关系表**：`PDSharedRelation` / `PD_SHARED_REL_SLOTS` / `rel_lock` 全部删除，walsender 侧改为进程内的 `HTAB`（`parallel_decode_sent_relations`，`relid → {epoch, sent}`），无锁、无共享。(4) **epoch 语义保持**：条目带 epoch（目录代次），已发过的 epoch ≥ 本帧 epoch 则丢弃，更大的 epoch 则发送并更新条目——DDL 之后的新定义必达，旧定义被丢弃。流式事务不触发标记钩子（pgoutput 本来就每个流式事务重发一次关系描述，且流式分片绕开 cseq 序），分区表的祖先定义与自身定义分别标记、分别去重，类型消息仍然重复（无害：订阅端类型缓存被覆盖）。(5) **钩子语义是"标记"而非"是否发送"**：为 NULL 表示不标记、总是按普通帧发送——串行路径即此语义，**串行路径一行不改**。(6) **验证**：`rel_dedup_smoke.sh`（8 并发会话 × 40 事务、事务保持重叠以铺开到 4 个 worker）实测并行路径 **4 条 → 1 条**，连跑 3 次结果一致；旧构建（`/tmp/pdoldpg/bin`）作 A/B 基线为 4 条。`ddl_diff_smoke.sh` 并行路径发 5 条 RELATION，与串行路径**逐消息一致**，说明 DDL 之后的新定义确实被重新送达。原有七个冒烟脚本加新加的两个**全部通过**。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.19 | 2026-11-19 | **FR-13 监控指标实现完成**。(1) **六个视图**：`pg_parallel_decode_workers` / `pg_parallel_decode_commit_lag` / `pg_parallel_decode_txn_latency` / `pg_parallel_decode_pending_commit_age` / `pg_parallel_decode_walsender_frames` / `pg_parallel_decode_wakeups`，对应 11.3 的六项指标。(2) **取值路径**：并行路径的计数器都在 dsa 里、SQL 进程读不到；本树没有 `shmem_request_hook` 扩展点，引入 pgstat 新框架的改动面过大，因此**复用已存在于主共享内存的 `WalSndCtl`**——walsender 侧把 dsa 控制块里的计数**节流（100ms）拷贝**进 `struct WalSnd` 新增的 `pd_stats` 字段，拷贝在 `walsnd->mutex` 自旋锁下进行；两个新 SRF（`pg_stat_get_parallel_decode_session()` / `pg_stat_get_parallel_decode_workers()`）遍历 `WalSndCtl->walsnds[]`，跳过 `pid == 0` 或 `pd_stats.active` 为假的槽位；视图再与 `pg_stat_activity` / `pg_authid` 联结补上用户名与应用名。没有 `ROLE_PG_READ_ALL_STATS` 的用户只能看到自己那一行的 pid，其余列置空（与 `pg_stat_get_wal_senders()` 口径一致）。**不新增共享内存段，不动 pgstat 框架**。(3) **新增计数器**：帧数、帧字节数、socket 写次数、事务数、事务延迟（累计值与最大值）、cseq 已分配 / 已放行数、最长挂起提交年龄、上下行唤醒次数、每个 worker 的在途事务数 / 在途字节数 / 已解码事务数 / 已收发记录数。(4) **一处数据竞争被修掉**：早期实现里 worker 与 walreader 都写 `in_flight_txns`——跨进程计数器必须**只有一个写者**，改为该计数只由 walreader 在分配与事务结束两处增减，worker 只写自己那侧的 `records_consumed` / `decoded_txns` / `wakeups`。(5) **两处精度问题被修掉**：事务延迟原先被除以 `USECS_PER_SEC`，而 `TimestampTz` 本来就是微秒；且 `parallel_decode_record_cseq()` 原先在「已放行事务」的提前返回**之前**就建待放行条目，会给早已放行的事务留下永不回收的条目——条目创建移到提前返回**之后**。(6) **验证**：新增 `monitoring_smoke.sh`（4 worker × 8 并发会话 × 40 事务），检查六个视图存在、会话与 worker 的计数非零、**worker 的唤醒次数之和等于会话的上行唤醒次数**、以及「串行会话不出现在视图里」（`pd_stats.active` 只在并行路径会话建立后为真）。本机 11 个冒烟脚本全部通过。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.20 | 2026-11-19 | **AC-14（子事务 abort）验证通过，并补上上游回归测试**。(1) **AC-14 的缺口查清**：`PD_ENT_SUBXACT_ABORT` / `PD_BLK_ABORT` 这条独立通道**没有实现，也不需要实现**——worker 用的是真实的 reorder buffer，`ReorderBufferAbort()` 在事务已被流式下发时会调用 `rb->stream_abort()`，STREAM ABORT 由插件按正常路径产出。新增 `subxact_abort_smoke.sh`：事务内先写入 2000 行（足以切出 17 个流段）、`SAVEPOINT` 后再写入 800 行、`ROLLBACK TO SAVEPOINT`、之后再写入 500 行；订阅端 `streaming='parallel'`。实测并行路径发出 **1 条 STREAM ABORT**，被中止子事务的行没有落地，其后子事务的 500 行完整落地，最终行集与**串行路径逐行一致**（AC-14 的核心判据"并行路径与串行路径的 apply 结果一致"成立）。(2) **上游回归测试**：`publication` 与 `subscription` 两个 pg_regress 用例通过；`src/test/subscription/t/` 的 34 个 TAP 用例（015_stream / 016_stream_subxact / 018_stream_subxact_abort / 019_stream_subxact_ddl_abort 等流式与子事务用例，共 457 个断言）全部通过——串行路径未受影响（AC-7）。(3) **冒烟脚本的运行环境**：所有脚本现在把 `PDTEST_ROOT` 与端口**导出**给 `setup_env.sh`，此前它们只是 shell 变量，导致脚本与被调用的 harness 用了不同的端口——本机曾因此出现"端口已被占用"的假失败。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.21 | 2026-11-19 | **`parallel_decode_batch_size` 接线完成（5.4 第 9 项）**。(1) **批处理的实现**：发送方（`PDQueueSender`）记"上次刷新以来写了多少字节"，写消息时把字节数记进去，记满一批就把该消息的 `force_flush` 置真；批次未满但发送方要睡了，靠一条**长度为 0 的 `PD_ENT_FLUSH` 消息**显式刷新——`shm_mq` 没有 notify API（不存在 `shm_mq_notify`），接收方看不到尚未发布的数据，不显式刷新就会对着已经在环形缓冲里的数据空等。(2) **承载进度的消息不批量**：`PD_ENT_POSITION`（停滞检测依赖它）、`PD_ENT_EVENT`（事务结束事件）、walreader 的广播与定向控制消息每次都单独刷新，否则复制延迟与停滞检测的误报率会直接被抬高。(3) **批大小的上界**：有效批大小被钳制在共享队列的 1/4（`PARALLEL_DECODE_QUEUE_SIZE / 4`）以内——`shm_mq` 在累计写入量达到环的 1/4 时自己会发布并唤醒接收方，批更大则发送方会等待接收方腾不出空间，而接收方恰恰看不到未发布的数据，构成死锁。(4) **walreader 的批按跨队列计**：否则不持有任何事务、只收到 8 字节 marker 的 worker，其位置心跳要攒到 64KB 才刷新一次。(5) **唤醒计数器的口径改为"每次刷新"**：计数点下沉到发送方层，worker 的上行计数与 walreader 的下行计数都由刷新产生，`pg_parallel_decode_wakeups` 的上下行之和与 `pg_parallel_decode_workers.wakeups` 的和因此一致。`monitoring_smoke.sh` 改为跑两遍同一负载（一遍关闭批处理、一遍 64KB）并断言唤醒次数下降：上行 1338 → 417、下行 969 → 363（帧数同阶，874 / 850）。**本机只做功能与正确性验证，唤醒次数不是吞吐性能测试，12.3 与 15.2 的降幅倍数仍待性能环境实测回填** |
| v0.23 | 2026-11-19 | **无 cseq 的帧（流式分片、子事务 abort 的 `STREAM ABORT`）不再越过已经结束的事务的结束事件——新增放行前沿的判门与 held 表**。(1) **缺陷**：并行路径把 cseq = 0 的帧一到就直接放进就绪队列，既不看待放行区也不看放行前沿，于是它们可以越过"在它们之前就已经结束"的事务的结束事件上 wire。发布端用行锁串行化的一对事务，在订阅端变成并发 apply → **订阅端报出发布端没有的 deadlock**（上游 parallel apply 本就有死锁风险，`applyparallelworker.c:60-130` 写明它检测而不避免；本条是发布端新增的并发窗口）。串行路径没有这个问题：它的输出顺序就是 WAL 记录的处理顺序。(2) **不变式 I-ORDER**：位置为 q 的无 cseq 帧，不得越过在 q 之前就已结束的事务的结束事件。(3) **判门**：无 cseq 帧可以放行，当且仅当放行前沿为空，或**前沿队首事务的结束记录起始 LSN ≥ 帧的位置**。两侧口径不同（队首取结束记录**起点**，帧取产生它的那条记录的**终点**），故判据是 `≥` 而非 `>`。**最后一个分片的位置取它的结束记录的起点**——它的变更都早于该事务的结束事件，这样它一定赶在自己的结束事件之前放行（协议要求如此），也顺带免掉了"按位置匹配去放行最后一个分片"这种脆弱做法（另一个事务的分片可能落在同一位置）。(4) **held 表**：不放行的帧按位置排序挂起，随放行前沿前移逐个放行；**帧永远不丢弃**（丢弃会让订阅端拿到不完整的数据）；held 表的字节计入待放行区。**判门之前必须先把控制通道读空**——前沿由 walreader's 消息算出，而控制通道与帧队列是两条通道，消息到达顺序不可比。(5) **新增 GUC `parallel_decode_held_max_bytes`**（默认 64MB，0 关闭）：等待放行前沿的帧占用发布端内存，超限时发一条 `WARNING`（每个 episode 只发一条）。同时把三个"每次检查时才读"的 GUC（`parallel_decode_barrier_timeout` / `parallel_decode_pending_max_bytes` / `parallel_decode_held_max_bytes`）的帮助文本从"下次复制会话生效"改为"reload 后立即生效"——它们不是会话建立时冻结的值。(6) **验证**：新增 `stream_order_smoke.sh`——两个事务**同时**跑（否则它们落在同一个 worker 上，一个 worker 内部天然串行，缺陷不会显现），第一个事务大（它的 worker 解码得慢，结束事件到得晚），第二个事务在第一个事务提交之后还在写，因此它产出的分片的位置在第一个事务的结束事件之后。断言：发布端**持有**了等待放行前沿的分片（告警出现）、发布端**没有**报出"分片越过了更早事务的提交"、结束事件按提交序到达订阅端、行数与内容一致。**负控制**（临时把判门改成恒真、重跑）必须失败：实测发布端报出 `the parallel decoding session sent a stream segment before the commit of an older transaction`（"分片位置 0/4D16A88 越过了在 0/4C40B00 结束的事务的提交"）。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.22 | 2026-11-19 | **修复 `copy_data = true` 的表同步卡在 `srsubstate = 's'`；待放行区加字节上限与告警；停滞检测收窄到队头事务的拥有者**。(1) **卡死的根因是三处缺陷叠加**（分析见 `copy_data_hang.md`）：**D1**——walsender 的 `sentPtr` 在 `XLogSendLogical()` 里被冻结，发布端空闲时不再前进，订阅端的 `last_received` 因此永远越不过表的同步点（`SYNCDONE → READY` 的提升条件）；**D2**——承载“认证读取位置”的 `PD_ENT_READ_POS` 消息虽已存在，但只有两个下发点，发布端空闲时**一个都到不了**：walreader 阻塞在页读回调 `pd_walreader_read_page()` 里等新 WAL，“每 N 条记录一次”与“WAL 暂时读完”这两个上报点都在那之后；**D3**——追平 keepalive 的节流判据 `TimestampDifferenceExceeds()` 的两个参数**写反了**（第一个参数必须是较早的时间），于是 keepalive 永远不发。三处缺一不可：只修 D2 会让 keepalive 仍然不发，只修 D3 会让帧头 LSN 仍然被钳在一个常量上。(2) **修法**：walsender 的空闲回调 `pd_walsender_idle()` 刷新 `sentPtr = ParallelDecodePosition()`；新增 `pd_walreader_publish_position()`，在**页读回调的等待分支**也下发认证读取位置与 marker（marker 按位置去重，等待循环每 100ms 走一次，不去重会往每个 worker 的队列里灌重复 marker）；keepalive 的节流判据改正参数顺序。(3) **待放行区的字节上限与告警（8.2 的“至少加个上限”方案）**：新增 GUC `parallel_decode_pending_max_bytes`（默认 64MB，0 关闭），walsender 统计待放行帧的字节数，超过上限时发一条 `WARNING`（每个 episode 只发一次，待放行区缩回上限以下后才会再告警）；会话继续运行，数据不受影响。顺带修掉放行事务时丢弃关系定义帧的**内存泄漏**（丢弃时没有 `pfree()`）。(4) **停滞检测收窄**：`parallel_decode_check_stall()` 原先遍历**全部** worker，而空闲的 worker 天然落后于 walreader 的读取位置（落后量约一个 marker 的距离）且在发布端空闲时不再上报，因此“待放行事务 + 空闲发布端”会误报卡死；现在只检查**待放行队列队头事务的拥有者**——会话就是在等它，检测能力不变而误报消失。(5) **删掉一个死变量** `parallel_decode_certified_cseq`：它被赋值但从未被读取，认证读取位置由 `PD_ENT_READ_POS` 与 `parallel_decode_certified_lsn` 承载。(6) **验证**：新增 `pending_area_smoke.sh`——订阅端 `streaming = 'off'`（整事务形态），发布端 `parallel_decode_barrier_timeout = 5`、`parallel_decode_pending_max_bytes = 1MB`，写一个 20 万行的事务；断言**待放行区的告警出现**、**停滞检测没有误杀会话**、**事务耗时超过停滞检测的超时**（否则检测没有理由触发）、行数与校验和一致。本机 14 个冒烟脚本**全部通过**（含新增的这个）。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
| v0.24 | 2026-11-19 | **并行解码路径把每个事务都流式化：小事务也在提交时作为一个流段发出**。(1) **问题**：`immediate` 档的流式触发点是 `ReorderBufferCheckMemoryLimit()`——事务要在**进行中**攒够 `parallel_decode_stream_segment_size`（默认 1MB）才被切段发出。tpcc 的典型事务约 3KB，**永远攒不到**→ 一个流段都不产生 → 订阅端收不到 STREAM START → **并行 apply 根本不启动**，端到端退化成串行 apply（`send_bottleneck.md` §6 记录了这次测量的现象）。(2) **修法**：新增 `LogicalDecodingContext.stream_all_transactions`（串行路径恒为 false，串行路径一行不改），由 worker 从会话的流式模式接过来（`PD_STREAMING_IMMEDIATE` 才打开）。reorder buffer 侧新增 `ReorderBufferStreamAtCommit()`：提交时若该事务应当被流式化，就走 `ReorderBufferStreamCommit()`，把它的全部变更**作为一个流段**发出去。(3) **四个前提**（缺一不可）：路径是并行解码路径且会话被要求立即流式；事务此前没有被流式化（`rbtxn_is_streamed()`）；快照一致（`ReorderBufferCanStartStreaming()`）；**不是两阶段提交的准备事务**（`rbtxn_prepared()`——`ReorderBufferPrepare()` 与 `ReorderBufferFinishPrepared()` 都在回放之前就置上了 `RBTXN_PREPARE`，所以这一条同时覆盖了 prepare 记录本身，两阶段提交的路径因此保持原样）。低于 `parallel_decode_stream_min_size` 的事务仍然以整事务块下发。(4) **两个尺寸的语义重新分开**：`parallel_decode_stream_segment_size` 只是**分段点**，`parallel_decode_stream_min_size` 是"能不能被流式化"的门槛（0 = 每个事务都被流式化）。旧的 `if (min_size > segment_size) segment_size = min_size;`（两者互相抬高）已删除——否则 `min_size > 0` 会把段大小也抬上去，新语义无法成立。GUC 的帮助文本随之改写。(5) **文档同步**：8.1 新增"`immediate` 档的实现：每个事务都是流段"一节与四个前提的表；5.2.3 补上"块边界由消息边界表达"与实际的条目类型；5.3 补上已达成的拷贝链路；8.2 补上"并行解码路径的热路径只有分片一种"；9 与 17.5 补上会话拆卸的次序（生产者先停：先 walreader 后 worker，然后逐个等待退出再释放共享内存）；11.1 / 12.1 / 12.3 / 15.2 按 peer 会话的 `design_changelist.md` 同步；`send_bottleneck.md` §6 改为"已解决"；`user_manual.md` 的两个 GUC 说明同步。(6) **验证**：新增 `stream_all_txn_smoke.sh`——订阅端 `streaming='parallel'`，5 个小事务，断言发布端的 trace 里 **STREAM START / STREAM STOP / STREAM COMMIT 各 5 个**且行数与内容正确；随后换一个订阅端 `streaming='on'`，断言**一个流段都没有**（只有 BEGIN / COMMIT 各 3 个）——串行路径的行为不变。本机 17 个冒烟脚本**全部通过**。**本机只做功能与正确性验证，吞吐数字仍待性能环境实测回填** |
