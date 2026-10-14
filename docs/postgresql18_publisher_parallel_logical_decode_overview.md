# [RFC] PostgreSQL 18 发布端并行逻辑解码

## 概述

在 PostgreSQL 逻辑复制的发布端增加事务级并行逻辑解码能力。由固定容量的后台 Decode Worker 进程池并行执行 WAL 记录解码，walsender 独占 WAL 读取、事务识别、快照管理和协议输出。输出字节流与串行解码完全一致。

## 动机

逻辑复制的发布端是单线程的：一个 `walsender` 进程顺序读取 WAL、解码记录、调用输出插件回调、发送协议消息，全部在单个执行循环里完成。当逻辑解码本身成为瓶颈时（高写入量、宽表、大量 TOAST 列），该单线程无法利用多核 CPU。

PG 18 已有的 `streaming = parallel` 选项解决的是**订阅端**的并行（并行应用流式事务），不是**发布端**的并行（并行解码多个事务）。两者互补，不是同一件事。

解码瓶颈按 CPU 消耗分解为五段：

| 段 | 描述 |
|---|---|
| (a) 记录解析 | `DecodeInsert`/`Update`/`Delete` + `ReorderBufferQueueChange` |
| (b) 目录查找 | 历史快照下的 relcache/syscache 访问 |
| (c) TOAST 重组 | `ReorderBufferToastReplace` |
| (d) 插件回调 | `pgoutput` 的 tuple→协议消息序列化 |
| (e) 协议写出 | `WalSndWriteData` 网络发送 |

段 (a)-(c) 可以按事务并行。段 (d) 在输出插件支持每事务独立实例的前提下可以并行。段 (e) 保持串行（单一协议输出流）。N 个 Worker 的理论加速比受段 (a)-(d) 占总时间比例的约束。

## 详细设计

### 接口

**GUC 参数**（全部 SIGHUP、仅超级用户、连接建立时读取）：

| 参数 | 类型 | 默认值 | 语义 |
|---|---|---|---|
| `parallel_decode` | bool | `off` | 总开关 |
| `max_parallel_decode_workers_per_walsender` | int | `2` | Worker 池容量；受 `max_worker_processes` 约束 |
| `parallel_decode_input_queue_size` | int (kB) | `65536` | pending 输入缓存上限，超限触发 WAL 读取流控 |
| `parallel_decode_result_queue_size` | int (kB) | `16384` | Result Store 内存上限，超限按事务 spill 到磁盘 |
| `parallel_decode_fallback` | enum (`serial`/`wait`) | `serial` | 安全性无法证明时的回退策略 |

**统计视图**：`pg_stat_parallel_decode`——按复制槽/会话一行，包含 Worker 状态数、pending 事务数、队列与 spill 字节、失效追平等待、输出 lag、回退/重建计数、插件耗时。

**复制协议**：不新增消息或能力位。输出字节流与串行解码按提交序一致。

**输出插件接口**：基线模式（集中调用）下插件 API 不变。扩展模式（Worker 调用）需要插件声明并行支持；未声明的插件自动使用集中调用。

**客户端影响**：无。任意版本的订阅端无需协商即可连接。

### 实现

#### 进程架构

```
postmaster
├── walsender（每个复制连接一个）
│   ├── WAL 读取 + 事务识别 + 全局 SnapBuild
│   ├── 调度（pending queue、Worker 分配）
│   ├── 结果收集与输出排序（Result Store）
│   └── 协议输出（唯一写者）
└── Decode Worker 1..N（每会话，动态后台 Worker）
    ├── 私有 ReorderBuffer（仅含被授权事务）
    ├── 本地 relcache/syscache（含失效跟踪）
    └── 解码计算（集中调用：产出中间结果 / Worker 调用：持有私有插件实例）
```

#### 关键机制：ReorderBuffer 职责拆分

串行 `ReorderBuffer` 按事务作用域拆分：

| 职责 | 归属 |
|---|---|
| XID 分类、子事务→顶层映射 | walsender |
| 事务边界识别（COMMIT/ABORT） | walsender |
| 每事务变更缓冲 + 磁盘 spill | Worker |
| TOAST 重组 | Worker |
| 提交时目录失效应用与解码 | Worker |
| 提交序输出 | walsender |

#### 关键机制：快照版本化

- walsender 在 RUNNING_XACTS 边界调用 `SnapBuildGetOrBuildSnapshot` 生成快照版本
- 每个版本经 `SerializeSnapshot` 序列化后广播到全部 Worker
- Worker 在 16 条版本表中缓存，解码时用 `ReorderBufferSetBaseSnapshot` 挂接正确版本
- 不变量：事务 T 必须使用 T 提交记录 LSN 对应的快照版本

#### 关键机制：目录失效追平

- walsender 从提交记录提取 `xl_xact_invals`，作为 INVAL 事件广播
- Worker 本地执行 `LocalExecuteInvalidationMessage`，推进 `consumed_event_id`
- 失效**提前执行安全**（只清缓存，可见性由快照决定）；**欠执行不安全**（过期缓存条目被跨版本复用）
- 槽位置候选（`restart_lsn`/`catalog_xmin`）受全部在途 Worker 的最小 `consumed_event_id` 门控

#### decode.c 分流点

| 记录类型 | 并行模式行为 |
|---|---|
| HEAP/HEAP2（DML） | `ParallelDecodeDispatchRecord` → Worker task 队列 |
| COMMIT | 先发布失效事件 → `ParallelDecodeDispatchCommit` |
| ABORT | `ParallelDecodeDispatchAbort` |
| RUNNING_XACTS | `SnapBuildProcessRunningXacts` → `ParallelDecodePublishSnapshot` |
| 非事务逻辑消息 | 先冲刷输出队列到当前 LSN，再输出消息 |

#### Worker 输入获取方式（两个候选）

| 维度 | 方案 A：逐条记录下发 | 方案 B：LSN 区间授权 |
|---|---|---|
| Worker 输入 | 记录字节经 task 队列发送 | 只发 LSN 区间；Worker 自行读 WAL |
| IPC 流量 | 与变更数成正比 | 只有控制消息 |
| walsender CPU | 解析 + 拷贝，每条记录一次 | 只读记录头，负担最低 |
| 实现复杂度 | 低，先验证正确性 | 高，需要 Worker 侧 WAL reader 生命周期管理 |

基线：方案 A。若基准显示 walsender 为瓶颈，切换方案 B。

#### 插件调用位置（两个候选）

| 模式 | 描述 | 插件兼容性 | 收益 |
|---|---|---|---|
| 集中调用（基线） | Worker 产出中间结果；walsender 单一后端按序调用插件 | 所有现有插件不变 | 正确性优先 |
| Worker 调用（扩展） | Worker 加载私有插件实例，直接产出协议帧 | 需要插件声明并行支持 | 并行化段 (d) |

决策：先做集中调用。阶段 1 基准中段 (d) 占比超过 30%（阈值暂定）时推进 Worker 调用。

### 兼容性

| 使用方 | 行为 |
|---|---|
| PG 订阅端，任意版本，`streaming = off` | 不受影响（输出与串行一致） |
| PG 订阅端，`streaming = on/parallel` | 整槽回退串行 |
| `pg_recvlogical`、Debezium、pglogical | 不受影响 |
| `pgoutput` 协议 v3/v4/v6 | 不变 |
| `parallel_decode = off` | 恢复原串行路径 |

**插件兼容性**：集中调用模式零 API 变化。Worker 调用模式需要 `OutputPluginCallbacks` 的并行支持标志；未声明的插件自动使用集中调用。

**升级/降级**：GUC 默认 `off`，升级后行为不变。回退 = 关闭 GUC。

**资源约束**：Worker 占用 `max_worker_processes` 配额。不足时降级为更少 Worker 或完全串行，记 WARNING，不拒绝连接。

### 性能

**指标**（按槽维度，`pg_stat_parallel_decode`）：Worker 状态、pending 事务数、队列/spill 字节、失效追平等待、输出 lag、插件耗时、回退/重建计数。

**基准计划**（最少覆盖）：
1. 大量互不相关的小事务
2. 少量大事务
3. 多事务交错写入
4. 高目录访问与系统表变化
5. DDL 与 DML 交错（触发目录失效追平）
6. Worker 池饱和 + 队列流控
7. Worker 故障 + 取消 + 重连
8. 慢插件/慢网络（观察 head-of-line 阻塞）
9. 非事务逻辑消息交错
10. `two_phase` 槽（验证串行回退）
11. 串行 vs 并行对照

**性能目标**（暂定，基准校准后确认）：

| 指标 | 目标 |
|---|---|
| 小事务吞吐 | ≥1.5×（2 Worker）、≥2×（4 Worker） |
| 大事务 | ≥串行的 0.9× |
| 延迟 P99 | ≤串行的 1.2× |
| 串行路径回归（`off`） | 与不含本特性的 PG18 偏差 <3% |

---

## 已考虑的备选方案

### 1. openGauss 并行解码

openGauss 3.0.0+ 在发布端实现了并行逻辑解码，使用线程架构（同一进程内的 reader 线程 + N 个 decoder 线程 + sender 线程，经无锁环形队列通信）。

**不采纳的原因**：openGauss 的三个前提放宽了 PostgreSQL 最难约束——线程模型消除了跨进程序列化成本（传指针即可）；自有输出协议（text/json/二进制）绕开了插件生态与字节级等价；不支持 DDL 解码回避了目录一致性问题。PostgreSQL 的进程模型、插件生态和协议兼容要求不可放宽，必须正面解决跨进程序列化、插件调用位置和目录一致性这三件 openGauss 绕开的事。

**借鉴点**：reader/decoder/sender 三级流水线验证了总体结构；事务固定线程 + LSN 序输出印证了调度模型。

### 2. PolarDB（开源）

已核实：开源 PolarDB-PG **没有**发布端并行逻辑解码实现。其"并行回放"特性是备库物理 WAL 回放（LogIndex + 页级分发），属于不同问题域。

### 3. 订阅端并行 Apply（PG 18 已有）

PG 18 的 `applyparallelworker.c` 有完全相同的**机制**（leader + worker 池、DSM/shm_mq、一事务一 Worker、serialize_changes 回退），但解决的是**不同问题**（向表应用 DML vs 解码 WAL 记录）。目录一致性模型根本不同：apply Worker 用当前快照 + sinval；decode Worker 需要历史快照 + 显式失效事件。

### 4. 给 relcache/syscache 做快照版本化

否决：relcache/syscache 是全局单版本结构。版本化等于重写缓存子系统。

### 5. Worker 绕过缓存直接读堆表

否决：正确但目录查找是解码热点，性能不可接受。

### 6. Worker 内重放 DDL

否决：不可行且无必要——失效语义（清条目）已足够。

---

## 开放问题

1. **Worker 调用模式阈值**：段 (d) 占比多少才值得实现 Worker 调用？（暂定 30%。需基准校准。）

2. **快照版本表淘汰**：16 条 LRU + refcount 淘汰正确但可能需要针对跨多个 RUNNING_XACTS 边界的并发事务做调优。

3. **子事务映射传递**：当前按 `top_xid != xid` 逐条发送，处理直接父子关系，但不处理 ASSIGNMENT 记录中累积的映射。完整方案需要在任务消息中扩展 subxid 列表。

4. **Result Store 磁盘 spill**：布局已定义（按事务文件、`ReorderBufferSerialize` 模式），实现推迟到内存-only 被证明不足时。

5. **方案 B（LSN 区间授权）**：推迟到阶段 3。需要 Worker 侧 WAL reader 的生命周期管理和区间边界错误恢复。

6. **运行时模式切换**：参数在连接建立时固定。会话中切换（活跃事务处理、是否需重连）推迟。

7. **SQL 接口（`pg_logical_slot_get_changes`）**：首版范围外。可后续用同一池基础设施添加。

8. **交错输出协议**：若复制协议扩展为允许跨事务交错，需更新协议版本协商和订阅端状态机设计。

---

## 附录 A：术语表

| 术语 | 定义 |
|---|---|
| Decode Worker | 后台 Worker 进程，负责一个顶层事务的解码任务 |
| Worker Pool | 固定容量的 Decode Worker 集合 |
| Pending Queue | 等待空闲 Worker 的事务队列 |
| 事件流 | walsender 按序发布快照版本和目录失效的通道 |
| 目录失效追平 | 同步机制：walsender 等待 Worker 将失效事件消费到指定位点后才推进槽位置；语义对应 Linux RCU 静默点 |
| Result Store | walsender 侧结果缓冲：内存为主，超限按事务 spill 到磁盘 |
| 历史快照 | 事务解码使用的快照；必须对应事务提交记录的 LSN |
| 集中调用 / Worker 调用 | 插件回调的两种执行位置：集中在 walsender 单一后端（基线）/ 移入 Worker 私有插件实例（扩展） |
| 快照版本 | walsender 生成的历史快照编号实例；RUNNING_XACTS 窗口内复用同一版本 |
