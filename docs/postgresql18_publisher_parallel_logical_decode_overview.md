# PostgreSQL 18 发布端并行逻辑解码概要设计

-   **文档类型**：概要设计
-   **目标版本**：PostgreSQL 18
-   **设计范围**：发布端（Publisher）事务级并行逻辑解码
-   **状态**：设计稿，可作为详细设计与原型实现的基线；配套文档：详细设计 `postgresql18_publisher_parallel_logical_decode_detailed_design.md`（编码级，含消息格式、接口与并发规则）
-   **文档结构**：总分式。§1–§3 为总述：背景、总体架构（walsender 与 Decode Worker 的进程关系与端到端时序，§2）、用户接口与兼容性（§3）。§4–§14 为分述，其中三个重点：一致性机制——ReorderBuffer 与快照（§4）、方案对比——openGauss 与 PolarDB（§9）、测试与验证（§12）。附录 A 为术语表。

------------------------------------------------------------------------

## 1. 背景与目标

### 1.1 背景

PostgreSQL 原生逻辑复制的发布端由一个 `walsender` 会话顺序读取 WAL，并在该会话内完成逻辑解码、输出插件回调及协议发送。PG18 已有的 `streaming = parallel` 用于订阅端对流式事务进行并行应用，与发布端对多个事务并行解码是两件事。

当发布端逻辑解码本身成为瓶颈时，单一解码执行流无法利用多核 CPU。本设计在发布端增加事务级并行解码调度：walsender 按 WAL 顺序识别和归属事务记录，将不同顶层事务分配给预先创建的 Decode Worker 并行处理，结果最后由 walsender 统一收集并输出。

### 1.2 设计目标

1.  跨事务并行：多个符合条件的顶层事务可以由不同 Worker 同时解码。
2.  事务内单 Worker：一个顶层事务在一次解码期间只归属于一个 Worker；同一 Worker 同时最多处理一个顶层事务。
3.  固定 Worker Pool：启动或按配置预热一组 Worker；池中没有空闲 Worker 时事务进入等待队列，不为每个新事务创建 Worker。
4.  全局状态单一权威：walsender 独占 WAL 顺序推进、事务归属、全局 `SnapBuild`、提交/中止识别及槽位置（`confirmed_flush`/`restart_lsn`/`catalog_xmin`）推进。
5.  快照与目录安全：按顺序分发快照和目录失效事件；无法证明目录可见性安全时等待失效追平，必要时回退到受控串行解码。
6.  单一协议出口：Worker 只生产解码结果，不直接发送协议消息，不并发修改协议状态。
7.  复用 PostgreSQL 既有机制：参考 PG18 `applyparallelworker.c` 的 Worker 生命周期、DSM、`shm_mq`、Latch 和错误传递模式，不复用订阅端的 DML Apply 状态机。
8.  兼容与可回退：保留现有串行逻辑解码路径；新模式可通过配置启停，不满足并行安全条件时回退。
9.  选型有依据：三项关键选型（Worker 输入获取方式 §5.2、插件调用位置 §5.3、结果暂存与流控 §5.5）以 CPU 成本拆解基准和串行对照为依据，不以主观判断定案。

### 1.3 非目标

-   不实现订阅端跨事务并行 Apply 调度。
-   不覆盖 SQL 接口（`pg_logical_slot_get_changes` 等函数）的并行化；本设计只针对 `walsender` 复制连接。
-   不改变事务的 ACID 语义，也不允许同一事务的变更被多个 Worker 分别执行逻辑解码。
-   不允许 Worker 自行决定 WAL 读取范围或独立推导全局状态。在被显式授权的 LSN 区间内做只读重放（§5.2 方案 B）不违反本约束，属于开放选型。
-   当前不实现 `two_phase` 槽上的并行解码，相关槽回退串行（§5.6）。
-   不允许多个 Worker 直接并发写同一个 `walsender` 的复制协议输出流。
-   不要求所有事务始终并行执行；Worker 不足、快照未就绪、目录失效未追平或安全性未知时，事务可以等待或走串行路径。

### 1.4 核心原则

-   顺序识别、并行计算、集中输出：WAL 读取和全局状态推进由 walsender 串行控制；事务级解码任务并行；协议输出统一收口。
-   事务归属稳定：顶层事务首次分配 Worker 后，直到事务解码任务完成并完成资源清理之前，不迁移到其他 Worker。
-   固定容量、显式流控：Worker Pool 达到容量上限时事务进入 pending queue；不以无限缓存替代流控。
-   安全优先于并行度：不能证明正确性时，等待失效追平或回退串行，不猜测目录状态。
-   全局与本地状态分离：walsender 持有权威状态；Worker 只持有被授权的事务上下文和可重建的本地执行状态。
-   结果与提交顺序分离：事务可以先完成解码，结果何时输出由 walsender 根据协议约束、输出位置与事务状态统一决定。

------------------------------------------------------------------------

## 2. 总体架构：walsender 与 Decode Worker

### 2.1 串行解码的进程结构（现状）

发布端现状是单进程架构。每个复制连接由 postmaster fork 一个 `walsender` 后端进程，逻辑解码的全部逻辑角色都在这一个进程的单个执行流里串行完成：

``` mermaid
flowchart LR
    PM[postmaster] -->|fork 每个复制连接| WS[walsender 进程]
    subgraph WSInside[walsender 进程内 - 单执行流]
        XR[WAL 读取<br/>XLogReaderState + logical_read_xlog_page] --> DEC[decode.c<br/>记录归属与事务识别]
        DEC --> RB[ReorderBuffer<br/>事务重组-缓冲-spill]
        DEC --> SB[SnapBuild<br/>快照构建]
        RB --> OP[输出插件 pgoutput<br/>进程内 LoadOutputPlugin]
        OP --> PW[协议写出<br/>WalSndPrepareWrite / WalSndWriteData]
    end
    SLOT[(ReplicationSlot 共享内存)] --- WS
    WALDISK[(pg_wal)] --> XR
    PW --> NET[(复制协议 / 订阅端)]
```

WAL 读取（`walsender.c` 的 `logical_read_xlog_page`，主库直接读本地 `pg_wal`）、事务识别与重组（`decode.c` + `ReorderBuffer`）、快照构建（`snapbuild.c`）、输出插件（进程内 `LoadOutputPlugin()`）、协议写出（`WalSndPrepareWrite`/`WalSndWriteData` 回调）全部在 `WalSndLoop` 的同一次循环里串行执行。解码路径上没有进程间通信，唯一的跨进程交互是到订阅端的网络；复制槽在共享内存中，解码会话是其唯一写者。瓶颈即该单执行流：CPU 密集段（变更解析、目录查找、TOAST 重组、插件回调）与等待段（等 WAL 刷新、等网络发送）相互排队。

### 2.2 并行解码的进程结构

目标态是 1 个 walsender 进程 + 每会话固定 N 个 Decode Worker 进程。walsender 仍是唯一推进全局 WAL 状态的执行流、复制协议消息的唯一出口；新增的 Worker 进程只做被授权事务的解码计算：

``` mermaid
flowchart LR
    PM[postmaster] -->|fork 复制连接| WS[walsender 进程]
    PM -.->|动态后台 Worker 启动与回收| W1[Decode Worker 1]
    PM -.-> W2[Decode Worker 2..N]
    subgraph WSInside[walsender 进程内]
        C[WAL 读取 - 事务识别 - 全局 SnapBuild]
        SCH[调度逻辑 / Pending Queue]
        INV[目录失效追平判定]
        COL[结果收集与输出调度<br/>Result Store 与槽位置推进]
        PS[协议出口 - 唯一写者]
    end
    subgraph WInside[每个 Worker 进程内 - 图示一份]
        RBp[事务私有 ReorderBuffer]
        REL[本地 relcache 与目录缓存]
        PLG[解码计算<br/>集中调用：中间结果生产<br/>Worker 调用：私有输出插件实例]
    end
    C --- SLOT[(ReplicationSlot 共享内存<br/>仍单写者)]
    WALD[(pg_wal)] --> C
    WALD -.->|仅方案 B - 授权区间只读| W1
    W1 <-->|DSM + shm_mq<br/>task - event - result - error - control| WS
    PS --> NET[(复制协议 / 订阅端)]
```

各进程/模块职责及其相对串行架构的来源：

| 进程/位置 | 模块 | 职责 | 相对串行架构的变化 |
|---|---|---|---|
| walsender | WAL 读取 + 事务识别 + 全局 `SnapBuild` | 顺序读 WAL、归属分类、子事务映射、快照版本发布、槽位置推进（§4.7） | 从"读 WAL 并顺带完成全部解码"收缩为"只做分类、调度与全局状态" |
| | 调度 / 失效追平判定 / 结果收集与输出调度 | 任务分配与 pending queue、目录安全判定、结果整理与 Result Store（§5.5） | 新增；均为 walsender 进程内逻辑，不引入新进程 |
| | 协议出口 | 生成并发送复制协议消息、keepalive（§4.6）、槽状态推进 | 不变，仍是唯一写者 |
| Decode Worker（每会话 N 个） | 事务私有 ReorderBuffer + 本地 relcache + 解码计算（集中调用产出中间结果 / Worker 调用持有私有插件实例，§5.3） | 被授权顶层事务的重组与解码（§4.2） | 新增进程；承接原 walsender 中的每事务重组与解码段 |
| postmaster | 后台 Worker 生命周期 | Worker 进程启动、监控、崩溃回收 | 新增交互；复用动态后台 Worker 框架 |

第一阶段采用每个逻辑解码会话（对应一个 `walsender`/复制连接）拥有一个独立的 Worker Pool：事务上下文、输出插件状态、复制槽和输出推进的生命周期容易管理。全局共享池可以作为后续优化，但会引入跨连接配额、公平性、Worker 归属和故障隔离问题，不作为当前设计的前提。

### 2.3 进程间通信

新增的进程边界只有一处：walsender 与 Decode Worker 之间。

| 通信路径 | 方向 | 机制 | 顺序约束 |
|---|---|---|---|
| Task Queue | walsender → Worker | 每 Worker 独立 DSM + `shm_mq` | 单生产者单消费者，保序 |
| Event Queue | walsender → Worker | 同上 | `event_id` 单调递增；Worker 消费位置显式确认（§4.4） |
| Result Queue | Worker → walsender | 同上 | 结果按事务分帧，携带排序元数据与 generation（§8.2） |
| Error Queue | Worker → walsender | 同上 | 错误即时上报，不等结果通道 |
| Control Channel | 双向 | `shm_mq` + Latch | 心跳/进度、取消、释放确认（§4.6、§7.2） |
| Worker 启动/回收 | walsender ↔ postmaster | 动态后台 Worker API（注册、等待启动、句柄监控） | 仅生命周期语义，不参与解码 |
| 复制槽共享状态 | walsender ↔ 共享内存 | 既有 `ReplicationSlotCtl` 锁协议 | 不变：仍单写者 |
| 订阅端网络 | walsender ↔ 订阅端 | 既有复制协议 | 不变：Worker 不访问网络 |

通信不变量：Worker 之间不直接通信，任何协同都经 walsender 中转；Worker 不写网络，不写复制槽的 `confirmed_flush`/`restart_lsn`；postmaster 只管进程生命周期，不参与解码语义；所有等待都走带超时的 Latch 循环，并按 §4.6 处理会话活性。

### 2.4 端到端时序

一次典型的并行解码交互（两个事务跨一个 DDL，覆盖事件、任务、结果三类消息的先后关系）：

``` mermaid
sequenceDiagram
    participant W as WAL
    participant S as walsender（读取/调度）
    participant E as Event Queue（每 Worker）
    participant T as Task Queue（每 Worker）
    participant K as Decode Worker
    participant R as Result Queue

    W->>S: RUNNING_XACTS
    S->>E: SNAPSHOT 事件（新快照版本）
    W->>S: T1 首条变更
    S->>T: T1 任务（记录/区间，required_event_id）
    T->>K: 收到任务，先排空事件至 required_event_id
    W->>S: DDL 事务提交（提交记录内嵌失效消息）
    S->>E: INVAL 事件（广播全部 Worker）
    W->>S: T2 首条变更（DDL 之后）
    S->>T: T2 任务（required_event_id 已越过 INVAL 位点）
    W->>S: T1 提交记录
    S->>T: T1 提交标记（含 snapshot_version）
    K->>K: 本地 ReorderBufferCommit（历史快照下解码）
    K->>R: T1 结果（提交 LSN + generation）
    R->>S: 结果入 Result Store，Worker 回 IDLE
    S->>S: 按提交序判定输出；必要时等待失效追平（§4.5）
    S->>S: 协议输出，推进已发送位置
```

时序中的关键顺序，全部由 walsender 单生产者的程序序保证：

1.  SNAPSHOT/INVAL 事件先于引用它的任务入队（§10.3 不变量 11），Worker 到位后先排空事件再解码，不发生等待；
2.  提交标记在 walsender 读到提交记录之后下发，因此提交识别必然先于 Worker 解码（§4.4 使用流程的顺序依据）；
3.  Worker 移交结果后立即回 IDLE，不等待输出；输出顺序上的等待由 walsender 侧的 Result Store 承担（§5.5）；
4.  条件等待只发生在 walsender 侧：失效追平（§4.5）与输出流控（§5.5）。

### 2.5 与串行架构的差异

| 维度 | 串行（现状，§2.1） | 并行解码（目标态，§2.2） |
|---|---|---|
| 进程数 | 1 个 walsender | 1 个 walsender + 每会话 N 个 Worker |
| WAL 读取 | walsender 独占 | walsender 独占推进；方案 B 下 Worker 只读授权区间（§5.2） |
| 解码计算 | walsender 内串行 | Worker 按事务并行 |
| ReorderBuffer | 单实例承担全部重组职责 | 按事务作用域拆分（§4.2）：全局分类留 walsender，事务内重组进 Worker |
| 输出插件 | walsender 内单实例 | 集中调用下不变；Worker 调用下每 Worker 私有实例（§5.3） |
| 协议写出与槽状态机 | walsender | 不变，输出字节流一致（§3.3） |
| 发布端内部 IPC | 无 | 每 Worker DSM + `shm_mq` + Latch（§2.3） |
| 失败域 | 连接级 | 仍连接级：Worker 故障按 §8 处理，当前实现传导为会话错误并按槽恢复语义重放（§8.2） |
| 系统资源 | 无额外进程 | 占用 `max_worker_processes` 配额，不足时降级不拒连（§14.2） |

对外行为不变：复制协议字节流、复制槽状态机、故障恢复语义与串行实现一致；变化全部在发布端进程内部。

------------------------------------------------------------------------

## 3. 用户接口与兼容性

并行解码对外的接口面很小：一组 GUC、一个统计视图；复制协议与输出插件 API 维持现状（仅 Worker 调用模式需要插件声明支持）。本节给出接口定义和各类客户端的行为；插件细则、升级降级与分阶段实施见 §14。

### 3.1 接口定义

新增接口分两部分：GUC 与统计视图。复制协议不新增消息和能力位；输出插件接口仅在 Worker 调用模式下增加能力声明。订阅端无需修改。

GUC（暂定名与默认值，最终以社区评审为准）：

| 参数 | 类型 | 默认（暂定） | context | 语义 |
|---|---|---|---|---|
| `parallel_decode` | bool | `off` | SIGHUP | 总开关；仅新建复制连接时读取，在途会话不受影响 |
| `max_parallel_decode_workers_per_walsender` | int | `2` | SIGHUP | 每会话 Worker 上限；受 `max_worker_processes` 约束，资源不足时降级运行（§14.2）；命名风格对齐 `max_sync_workers_per_subscription` |
| `parallel_decode_input_queue_size` | int（kB） | `65536` | SIGHUP | pending 输入/任务缓存上限，超限流控；默认量级对齐 `logical_decoding_work_mem`；方案 B 下仅约束控制类缓冲 |
| `parallel_decode_result_queue_size` | int（kB） | `16384` | SIGHUP | Result Store 内存上限，超限按事务 spill（§5.5）；内存 + 磁盘达硬上限触发输出流控 |
| `parallel_decode_fallback` | enum（`serial`/`wait`） | `serial` | SIGHUP | 安全性未知时的回退策略，默认保守串行回退或等待 |

以上 GUC 均为 superuser-only。备选接口：槽级选项（`CREATE_REPLICATION_SLOT ... (parallel_decode)`，与 `two_phase` 槽选项同风格），按槽固定语义、便于运维工具声明意图。当前先实现 GUC，槽级选项留待后续完善；两者并存时槽级优先。

统计视图：新增 `pg_stat_parallel_decode`（暂定名），按复制槽/会话一行，列与 §11.2 指标一一对应（Worker 各状态数、pending 事务数、队列与 spill 字节、追平等待、输出 lag、回退/重建计数、输出插件耗时）。现有 `pg_replication_slots`、`pg_stat_replication` 不加列、不改语义。

复制协议：不新增消息和能力位；输出字节流与串行解码按提交序一致（§5.4）。

输出插件接口：集中调用下插件 API 不变，现有插件无需重编译；Worker 调用增加能力声明，未声明的插件所在会话使用集中调用（§14.1）。

错误报告：Worker 错误最终以现有 walsender 逻辑解码错误形式上报（终止连接）；不新增客户端可见错误码，服务端日志携带 Worker 标识。

### 3.2 参数生效时点

-   `parallel_decode = off`：走原有串行路径。
-   `parallel_decode = on`：启用固定 Worker Pool 和并行调度。
-   任务不能安全并行时：等待所需事件或进入受控串行路径。
-   关闭并行模式或会话退出时：停止接收新任务，按安全顺序处理/取消在途任务，再释放 Worker。

§3.1 中的参数仅在建立复制会话时读取并固定；SIGHUP 只影响后续连接，在途会话不迁移模式、不中途启停 Worker。该做法作为当前基线；运行中切换（活跃事务的处理、切换点、是否需要重连）留待后续完善。

### 3.3 客户端行为

| 使用方 | 行为 | 说明 |
|---|---|---|
| PG 订阅端（任意版本），`streaming = off` | 不受影响 | 输出与串行解码一致（§5.4），无需协商 |
| PG 订阅端，`streaming = on/parallel` | 整槽回退串行 | 连接建立时检测 streaming 请求（§5.6），并行不生效，连接行为正常 |
| `pg_recvlogical`、Debezium、pglogical 等第三方消费端 | 不受影响 | 输出与串行一致；配套输出插件按 §14.1 使用集中调用 |
| `pgoutput` 协议 v3/v4/v6 | 不变 | 不新增、不修改协议消息 |
| 旧版本订阅端 + 新发布端 | 不受影响 | 并行是发布端内部机制，对协议不可见 |
| 关闭并行（`parallel_decode = off`） | 恢复串行路径 | 行为与不含本特性的 PG18 相同 |
| 双向复制 / origin 过滤（发布集 `ORIGIN`、`pgoutput` 的 `publish_no_origin`） | 语义不变 | origin 随事务结果一并移交，过滤行为与串行一致；纳入 §11.3 对照测试 |

------------------------------------------------------------------------

## 4. 一致性机制：ReorderBuffer 拆分与快照

本章是正确性的核心：解码计算移入 Worker 后，事务重组的职责如何拆分，快照与目录失效如何跨进程传递并保证与串行解码等价。

### 4.1 串行调用链与并行改造点

并行改造落在现有调用链的具体位置，以当前 PG18 源码的串行链路为基线（文件对应 §13 衔接表）：

```
WalSndLoop(XLogSendLogical)                     -- walsender.c
 └─ XLogSendLogical: XLogReadRecord(reader) 取下一条记录
     └─ LogicalDecodingProcessRecord(ctx, record)   -- decode.c
         ├─ HEAP/HEAP2: DecodeInsert/Update/Delete/...
         │    └─ ReorderBufferQueueChange                -- reorderbuffer.c
         ├─ 子事务: ReorderBufferAssignChild             -- reorderbuffer.c
         ├─ RUNNING_XACTS: SnapBuildProcessRunningXacts  -- snapbuild.c
         └─ DecodeCommit / DecodeAbort                   -- decode.c
              └─ ReorderBufferCommit / ReorderBufferAbort
                   └─ ReorderBufferProcessTXN
                        ├─ SetupHistoricSnapshot(snapshot, txn->tuplecid_hash)
                        ├─ ReorderBufferToastReplace
                        └─ rb->begin/change/commit_cb（pgoutput）
                             └─ OutputPluginWrite → ctx->write
                                = WalSndPrepareWrite/WalSndWriteData（walsender.c）
```

一个对实现有用的事实：输出插件的 writer 回调在 `CreateDecodingContext` 时按上下文注入——walsender 注入 `WalSndPrepareWrite`/`WalSndWriteData`，SQL 接口 `pg_logical_slot_get_changes` 注入 `LogicalOutputPrepareWrite`/`LogicalOutputWrite`（logicalfuncs.c）。writer 不是全局固定的，这为 Worker 重定向插件输出提供了现成挂点（§5.3）。

各调用点在并行模式下的去向：

| 串行调用点 | 并行模式下的去向 |
|---|---|
| `XLogSendLogical` 读记录 | 不变，仍在 walsender |
| `LogicalDecodingProcessRecord` 的记录分发 | 拆分：快照与事务边界记录仍在 walsender 处理；DML 记录经调度逻辑分发（新文件提案 `paralleldecode.c`，入口函数提案 `ParallelDecodeDispatchChange`） |
| `ReorderBufferQueueChange` | 事务已绑定 Worker：改投 Task Queue；无空闲 Worker：方案 A 进 walsender 有界缓存，方案 B 只登记 LSN 区间（§5.2） |
| `ReorderBufferAssignChild` | 不变，仍在 walsender；归属结论随任务移交（§4.2） |
| `DecodeCommit` → `ReorderBufferCommit` | walsender 只识别 COMMIT 并发送事务完成标记；`ReorderBufferCommit` 的执行体（`ProcessTXN`、`ToastReplace`、插件回调）移到 Worker |
| `SetupHistoricSnapshot` | Worker 侧执行，快照版本由 walsender 序列化移交（§4.3、§4.4） |
| `ctx->write = WalSndWriteData` | Worker 侧上下文的 writer 替换为写 Result Queue 的实现（注入方式与 SQL 接口相同） |

walsender 主循环改造：`WalSndLoop` 成为统一事件循环，事件源四类——WAL 可读（推进解码与分发）、Result Queue 可收（收 Worker 结果进 Result Store）、输出条件满足（按提交序输出事务）、超时（keepalive 与 feedback，§4.6）。每个等待点带超时 Latch，四类事件在同一个循环里交替处理，不引入独立输出线程。

``` mermaid
flowchart TB
    LOOP[WaitLatch 带超时的事件循环]
    subgraph WALSIDE[WAL 可读 - 读取与分流]
        R1[XLogReadRecord 取记录] --> R2{记录类型}
        R2 -->|RUNNING_XACTS| R3[SnapBuild 推进<br/>快照变化则广播新版本 SNAPSHOT 事件]
        R2 -->|DML| R4{事务已绑定 Worker?}
        R4 -->|是| R5[记录投递 Task Queue<br/>required_event_id 覆盖已发事件]
        R4 -->|否 且池满| R6[进入 pending queue 有界缓冲]
        R4 -->|否 且有空闲| R7[绑定 Worker 建任务]
        R2 -->|COMMIT| R8[置 commit_lsn<br/>下发提交标记 含 snapshot_version]
        R2 -->|COMMIT 内嵌失效| R9[提取失效消息<br/>广播 INVAL 或 FLUSH_ALL<br/>记录追平要求位点]
        R2 -->|ABORT| R10[销毁事务条目<br/>通知 Worker 丢弃缓冲]
    end
    subgraph RESULTSIDE[Result Queue 可收]
        C1[结果入 Result Store<br/>确认移交 Worker 回 IDLE]
    end
    subgraph OUTSIDE[输出条件检查]
        O1{提交序最小的 commit_lsn<br/>结果已就绪?}
        O1 -->|是| O2[协议输出 释放条目<br/>推进已输出位置]
        O1 -->|否| O3[等待队头事务<br/>Result Store 硬上限则流控]
    end
    subgraph TIMESIDE[超时]
        K1[keepalive 处理订阅端 feedback<br/>检查会话取消与连接断开]
    end
    LOOP --> WALSIDE
    LOOP --> RESULTSIDE
    LOOP --> OUTSIDE
    LOOP --> TIMESIDE
```

输出与槽位置推进的门控：O2 只释放已完成输出的事务，推进的是已发送位置；`restart_lsn`/`catalog_xmin` 的候选推进还要等所有在途 Worker 的 `consumed_event_id` 越过对应位点（§4.5 的追平等待）；`confirmed_flush` 由订阅端反馈驱动（§4.6）。三者合起来保证"槽位置不超过依赖状态均已处理的位置"（§4.7、§10.3.9）。

### 4.2 ReorderBuffer 的职责拆分

调度单位是顶层事务。子事务不能被独立分配给其他 Worker；walsender 维护子事务到顶层事务的映射（`ReorderBufferAssignChild` 不变），属于该顶层事务的变更、子事务提交/中止信息和事务结束状态遵守统一的事务上下文。对于无法可靠归属的特殊记录，当前采用串行回退或阻塞等待，不猜测归属。

串行逻辑解码中，`ReorderBuffer` 是事务重组的唯一执行者：把按 WAL 顺序交错到达的变更重新组织成按事务聚合的变更序列，并在提交时按提交顺序输出。并行解码不另造一套重组逻辑，而是把 ReorderBuffer 的职责按事务作用域拆开：全局分类留给 walsender，事务内重组进入 Worker，提交序输出由 walsender 统一判定。

| ReorderBuffer 现有职责 | 代表机制 | 并行后归属 |
|---|---|---|
| 按 XID 归类记录、子事务挂接到顶层事务 | `ReorderBufferAssignChild`、`by_txn` 事务哈希 | walsender（全局唯一） |
| 事务边界（BEGIN/COMMIT/ABORT）追踪与输出触发 | `ReorderBufferCommit`、`ReorderBufferAbort` | walsender 识别边界；输出时机由 walsender 判定 |
| 每事务变更缓存与磁盘 spill | `ReorderBufferQueueChange`、`ReorderBufferSerializeTXN` | Worker（本事务私有）；pending 事务在方案 A 下仍由 walsender 有界缓存 |
| TOAST 重组 | `ReorderBufferToastReplace` | Worker |
| 提交时目录失效应用与解码计算 | `ReorderBufferProcessTXN` | Worker；插件调用位置取决于 §5.3 选型 |
| 大事务流式分段输出 | `ReorderBufferStreamTXN` | 当前不与并行叠加：请求 streaming 的槽整槽串行（§5.6） |
| 全局事务集合追踪与内存回收 | `ReorderBuffer` 全局字段 | walsender |

对象级交互：

1.  walsender 侧 ReorderBuffer 退化为元数据容器：保留 `by_txn` 事务哈希、`ReorderBufferTXN` 生命周期与 `AssignChild` 挂接，停用变更队列——`QueueChange` 的调用点被任务分发替换。改造在 decode.c 的调用点分流（并行模式走分发，串行模式原样），不侵入 reorderbuffer.c 的内部结构，保证 `parallel_decode = off` 时代码路径不变。
2.  Worker 侧持有功能完整的 ReorderBuffer 实例，仅包含被授权的事务：`QueueChange`、`ToastReplace`、`ProcessTXN`、按 `logical_decoding_work_mem` 阈值 spill 的逻辑原样复用；`ReorderBufferCommit` 的触发改为收到 walsender 的事务完成标记后在本地调用。
3.  事务移交接口（提案）：`ParallelDecodeHandoffTxn(top_xid, first_lsn..commit_lsn, 子事务映射, 快照版本标识, required_event_id)`。walsender 打包 `ReorderBufferTXN` 中属于元数据的字段（xid、lsn 区间、子事务列表、origin），Worker 据此重建本地 txn；变更数据本体走 §5.2 的两种获取方式之一。
4.  `ProcessTXN` 的快照来源：串行路径中快照在解码期间以 `base_snapshot` 挂到事务上（`ReorderBufferSetBaseSnapshot`），提交处理直接取用（`snapshot_now = txn->base_snapshot`）；Worker 侧把移交的快照版本反序列化后同样经 `ReorderBufferSetBaseSnapshot` 挂到本地事务，后续复用既有提交处理。`txn->tuplecid_hash`（cmin/cmax 解析所需）在 Worker 解码过程中本地构建、随本地 txn 生命周期管理，不需要移交。
5.  两种 spill 语义不同，不能合并：输入侧变更 spill（Worker 内、解码前，重组的原料）与输出侧结果 spill（Result Store，§5.5、解码后、重组的产物）服务于不同的推进条件与生命周期，分开管理。

### 4.3 解码快照的正确性不变量

事件流（§4.4）只是传输机制，约束正确性的是以下不变量：

1.  事务 T 必须使用 T 的提交记录 LSN 对应的 historic snapshot 解码，该快照包含提交时刻仍在进行中的事务集合。Worker 不得因为"手上正好有"而复用相邻 LSN 时刻的快照版本。
2.  walsender 发布的每个快照版本携带其生效的 LSN 区间和构造它所需的事务状态；Worker 绑定事务时记录所用快照版本的标识，解码过程中不切换。
3.  快照跨进程序列化传输（与 §5.2 的指针禁令一致）；`SnapBuild` 内部结构不能直接按字节复制到 Worker 后假定有效。序列化表示参照 `SnapBuildSerialize` 的磁盘格式裁剪。
4.  当前不与订阅端流式输出叠加：输出仍是事务级（BEGIN ... COMMIT 完整事务），快照语义锚定在提交点。若后续支持发布端并行下的流式输出，需重新定义事务内逐条变更的可见性判断点，另行设计。

### 4.4 快照版本与失效事件的实现

walsender 为事件分配单调递增的 `event_id`，经 Event Queue 分发。事件类型：

| 事件 | 内容 | Worker 处理 |
|---|---|---|
| SNAPSHOT | 快照版本号 + `SerializeSnapshot` 序列化的快照字节 | `RestoreSnapshot` 反序列化后存入本地版本表 |
| INVAL | 事务提交 LSN + WAL 提交记录内嵌的失效消息数组（`xl_xact_invals`，`SharedInvalidationMessage` 原样拷贝） | 逐条 `LocalExecuteInvalidationMessage` 在本地执行 |
| FLUSH_ALL | 触发标记 | `InvalidateSystemCaches()` 全量清本地缓存 |
| 控制类 | 失效追平的发起/解除、事务边界等 | 更新本地等待条件 |

失效信号的具体内容就是 `SharedInvalidationMessage`（sinval.h）：定长小记录，形如"OID 为 X 的 relcache 条目作废"（`SHAREDINVALRELCACHE_ID`）、"`pg_class` 的 syscache 中主键为 Y 的条目作废"（`SHAREDINVALCATALOG_ID`）。这些消息本来就逐条内嵌在提交 WAL 记录里（`xl_xact_invals`），walsender 解码提交记录时原样拷出即可，不需要新的编码格式。某次提交的失效条数超过阈值时改发 FLUSH_ALL，对应串行路径失效溢出时全量清缓存的行为（reorderbuffer.c）。

快照版本的生成与存储：

1.  生成：串行路径在提交处理时调用 `SnapBuildGetOrBuildSnapshot(builder)` 取快照；builder 缓存已构建的快照，仅在处理 RUNNING_XACTS 记录时重建——快照内容只在 running-xacts 边界变化。walsender 按此边界生成新的快照版本号，窗口内的所有提交复用同一份序列化结果。
2.  序列化：`SerializeSnapshot`（snapmgr.c）输出 `SnapshotData` 及 xip 数组的紧凑表示。并行查询已有同一用法的先例：`InitializeParallelDSM`（parallel.c）把事务快照与活动快照序列化进 DSM 传给并行 worker，worker 侧 `RestoreSnapshot` 重建——本设计对 Decode Worker 做同样的事。
3.  walsender 侧存储：最近 K 个版本的序列化结果（LRU），随 SNAPSHOT 事件广播；任务移交时只携带版本号。
4.  Worker 侧存储：`version → Snapshot` 哈希表（保留最近 K 个），解码事务时按任务携带的 `snapshot_version` 取用。

Worker 的使用流程（单次事务解码）：

1.  收到任务：`{top_xid, 输入数据或 LSN 区间, commit_lsn, snapshot_version, required_event_id}`。
2.  先消费事件至 `required_event_id`：INVAL 逐条本地执行、SNAPSHOT 入版本表、FLUSH_ALL 全量清缓存——保证缓存内容不落后于自己的解码历史（§4.5 的进度边界）。
3.  解码变更，进入本地 ReorderBuffer；收到提交标记后在本地执行 `ReorderBufferCommit`。
4.  从版本表取出 `snapshot_version` 对应的快照，`SetupHistoricSnapshot(snapshot, txn->tuplecid_hash)`；目录查询经本地 relcache/syscache 进行（缓存时序已由第 2 步对齐，可见性由该快照决定）；TOAST 重组与插件回调（或生成中间结果）与串行路径相同。
5.  `TeardownHistoricSnapshot`，结果写入 Result Queue，清理本地事务。
6.  更新 `consumed_event_id` 并确认，等待下一个任务。

``` mermaid
flowchart TB
    A[收到任务: top_xid 输入 commit_lsn<br/>snapshot_version required_event_id] --> B{事件消费进度<br/>落后于 required_event_id?}
    B -->|是| C[消费事件:<br/>INVAL 逐条本地执行<br/>SNAPSHOT 入版本表<br/>FLUSH_ALL 全量清缓存]
    C --> B
    B -->|否| D[解码变更<br/>进入本地 ReorderBuffer]
    D --> E{收到提交标记?}
    E -->|未收到| D
    E -->|已收到| F[从版本表取出 snapshot_version 快照<br/>SetupHistoricSnapshot]
    F --> G[本地 ReorderBufferCommit:<br/>目录查询 TOAST 重组<br/>插件回调或生成中间结果]
    G --> H{出错?}
    H -->|是| I[错误写入 Error Queue<br/>walsender 按 generation 处置]
    H -->|否| J[TeardownHistoricSnapshot<br/>结果写入 Result Queue]
    J --> K[得到 walsender 移交确认<br/>本地清理 回 IDLE]
```

图中两个回环承担正确性保证：C → B 保证"缓存时序不落后于自己的解码历史"（§4.5 的进度边界，欠执行才不安全）；E → D 保证"提交标记先于解码"（§4.4 的顺序依据）。

上述机制的前提是 Worker 的执行环境：Decode Worker 是后台 Worker 进程，做历史快照下的目录读取前必须具备类后端环境——`MyDatabaseId` 与数据库路径、relcache 初始化（含共享 init 文件的加载）、syscache 建立、`pg_catalog` 搜索路径；且失效回调（relcache/syscache/relmap）在该环境下必须可用，本地失效执行才能生效。启动序列在 `ParallelDecodeWorkerMain`（提案）中完成，参照并行查询 worker 的环境建立方式，属于详细设计的固定工作项。

事件消息只包含上述可序列化内容，不跨进程传递后端私有指针。事件合并、广播的批量粒度、版本表的淘汰时机在详细设计中确定。

### 4.5 目录失效追平

先说明要保护的东西。解码一条变更时，Worker 通过 relcache/syscache 查系统目录：表的列定义、类型、replica identity。缓存条目按 OID 组织、不携带版本信息，一个条目"属于哪个时刻的目录状态"完全取决于它在什么时序下被填入和被清除——目录一致性的全部内容就是维持这个时序。

串行路径的做法（源码基线）：

1.  DDL 事务在 WAL 中登记失效消息，`DecodeCommit` 将其随事务一并交给 `ReorderBufferCommit`（decode.c）。
2.  `ReorderBufferProcessTXN` 在处理该事务时于本进程执行这批失效：`ReorderBufferExecuteInvalidations` 逐条调用 `LocalExecuteInvalidationMessage`；失效消息超出内嵌上限时直接 `InvalidateSystemCaches()` 全量清缓存（reorderbuffer.c）。
3.  提交记录在 WAL 中构成全序，单进程内失效的本地执行严格按提交序穿插在各事务的解码之间：一个缓存条目要么已按新状态重建、要么尚未被污染，不存在跨版本复用。

并行破坏了其中两点：

1.  缓存不共享也不自动失效。Worker 的 relcache/syscache 是进程私有的，且解码使用历史快照，不在 sinval 失效框架的覆盖范围内——发布端实例上的 DDL 不会让另一个进程的缓存条目失效，失效只能从 WAL 推导后显式分发。§4.4 的事件流就是为此存在。
2.  分派顺序不等于提交顺序。事务按首次出现的顺序分配 Worker，提交按提交记录的 WAL 全序到达；同一个 Worker 可能先解提交晚的事务、再解提交早的事务，"新状态"的缓存条目会被旧事务的解码命中。跨 Worker 之间反而没有这个问题（缓存不共享），危害只存在于每个 Worker 自己的解码历史里。

要做的事：

1.  分发：walsender 解码时提取失效消息，作为有序事件发布到 Event Queue。
2.  本地执行：Worker 收到失效事件后在本地执行，语义与串行路径的本地执行相同，只是驱动源从"本进程的提交处理"换成事件流。
3.  进度边界：Worker 开始解码事务 T 前，失效执行进度（`consumed_event_id`）必须覆盖此前已处理事务的最大提交位点。提前执行失效是安全的——失效只清缓存，清多了下次在历史快照下重新读取即可，目录行的可见性由快照决定、不由缓存决定（串行路径的 `InvalidateSystemCaches()` 全量清就是同样的道理）；欠执行才不安全，那会跨版本复用缓存条目。

等待发生在两个位置：任务下发时，事务携带 `required_event_id`，Worker 消费进度不满足则先消费事件再开工；walsender 推进 `restart_lsn`/`catalog_xmin`（即让 WAL 段与旧目录行可被回收）之前，等待所有在途 Worker 的消费进度越过对应位点。语义上等价于 Linux RCU 的静默点等待：旧目录状态的读者全部越过后，新状态才允许被依赖。

``` mermaid
flowchart TB
    subgraph WS[walsender 侧]
        W1[解码提交记录] --> W2{内嵌失效消息?}
        W2 -->|有| W3[广播 INVAL 或 FLUSH_ALL<br/>记录追平要求的位点]
        W2 -->|无| W4[常规提交处理]
        W3 --> W6{推进 restart_lsn 或 catalog_xmin<br/>让 WAL 与旧目录行可回收} -->|在途 Worker 未全部越过| W7[等待消费进度<br/>keepalive 照常 §4.6]
        W6 -->|全部越过| W8[推进 restart_lsn 与 catalog_xmin]
        W7 --> W6
    end
    subgraph WK[Worker 侧]
        K1[收到任务<br/>携带 required_event_id] --> K2{consumed_event_id<br/>落后?}
        K2 -->|是| K3[消费并本地执行失效事件<br/>推进 consumed_event_id 并确认]
        K2 -->|否| K5[直接开始解码]
        K3 --> K5
        K5 --> K6[在历史快照下解码<br/>目录读取经已对齐的本地缓存]
    end
    W3 -. 事件分发 .-> K3
```

读图要点：追平位点有两种用途——随任务下发为 `required_event_id`，约束 Worker 开工前的消费进度（Worker 侧路径）；作为全局推进的门控，约束 `restart_lsn`/`catalog_xmin`（walsender 侧的等待回环）。Worker 侧"落后则先消费"是排空队列而非等待，因此不存在 Worker 侧的等待状态（§6.1）；walsender 侧的等待带 keepalive（§4.6）。

不采用的替代做法：给 relcache/syscache 条目做快照版本化——两者是全局单版本结构，改造等于重写缓存子系统；Worker 绕开缓存直接扫堆表——正确，但目录查找是解码热点，性能不可接受；在 Worker 内重放 DDL——不可行也无必要，失效语义（清条目）已经足够；openGauss 直接排除 DDL 解码的做法在 PostgreSQL 生态不可行（§9.3）。影响范围或可见性无法可靠分析时，转入受控串行解码路径——串行回退不是异常路径，而是处理未知语义、逐步扩大并行覆盖面的安全边界。

### 4.6 长时间等待期间的会话活性

walsender 的多个环节可能长时间不让出执行流：失效追平等待、pending queue/Result Store 流控暂停、等待 Worker 结果等。等待期间会话活性职责必须继续履行：

1.  walsender 的所有等待实现为带超时的 `WaitLatch` 循环，不允许无超时阻塞；超时唤醒点执行 keepalive 处理（满足 `wal_sender_timeout` 约束）、读取并处理订阅端 feedback（standby status update，推进确认状态）、检查会话取消与连接断开。
2.  流控暂停读取 WAL 期间同理：可以暂停解码推进，但不暂停协议层的活性应答。
3.  Worker 通过 Control Channel 定期上报心跳/进度；walsender 据此检测 Worker 失联并按 §8.1 处理。

若等待期间不处理 keepalive 与 feedback，等待超过 `wal_sender_timeout` 时订阅端会断连；这一要求在所有等待路径上都必须成立。

### 4.7 槽位置的推进规则

串行路径的槽字段分工：`confirmed_flush` 由订阅端反馈驱动——`ProcessStandbyReplyMessage` 收到 standby status update 后经 `LogicalConfirmReceivedLocation` 生效；`restart_lsn` 与 `catalog_xmin` 由 walsender 在解码推进时计算候选（`LogicalIncreaseRestartDecodingForSlot` 一族），反馈确认后应用——这两个字段决定 WAL 段与旧目录行能否被回收，是槽管理里最微妙的部位。

并行模式保持同一分工（槽字段全部只由 walsender 写入，§10.3.4），并增加两条约束：

1.  已发送位置（walsender 本地）只覆盖已完成输出的事务，是反馈确认的上界——`confirmed_flush` 不可能超过它；
2.  `restart_lsn`/`catalog_xmin` 的候选计算受追平门控（§4.5）：候选只能取到"所有在途 Worker 的失效消费进度均已越过"的位点。这是"没有 Worker 还依赖旧目录状态"的形式化——防止某个 Worker 仍在旧快照版本上解码时，VACUUM 依据已推进的 `catalog_xmin` 回收它还需要读取的目录行。

------------------------------------------------------------------------

## 5. 任务分发与结果输出

### 5.1 调度算法

每次为事务分配任务时，按以下顺序执行：

1.  根据顶层 XID 查询 `ParallelDecodeTxnHash`。
2.  事务已绑定 Worker：新到达的事务记录/控制事件继续投递给原 Worker。
3.  尚未绑定：查找状态为 `IDLE` 的 Worker。
4.  找到后，在持有必要同步保护的情况下将 Worker 标记为 `BUSY`，建立 `XID -> Worker` 映射，提交任务。
5.  没有空闲 Worker：事务加入 pending queue。不得因为池满而为该事务额外启动 Worker。
6.  Worker 释放时，调度逻辑从 pending queue 中选择事务并分配空闲 Worker；任务所需的失效与快照事件先于任务入队（§10.3）。
7.  等待期间不得持有会阻止 Worker 完成或 walsender 推进的池锁；唤醒后重新检查条件，不把一次唤醒视为资源已被保留。
8.  Result Store 达到硬上限时的输出流控（§5.5）同样通过本算法实施：暂停新任务分配，等待输出推进，等待循环按 §4.6 处理会话活性。

等待和唤醒采用 Latch/条件通知加状态重检的方式实现。具体锁与原子状态设计在详细设计阶段确定。

### 5.2 Worker 输入的获取方式

无论选择哪种获取方式，都需要定义：WAL 位置和记录顺序标识；顶层 XID、子事务 XID 与事务阶段；记录负载的所有权及生命周期；Worker 是否需要原始 WAL 字节、解析后数据或两者；任务取消、重试、Worker 崩溃后的重放边界。不把 walsender 的进程私有指针、`MemoryContext`、`Relation`、`TupleDesc`、`ResourceOwner` 或普通 `SnapshotData` 指针通过共享内存传递给 Worker；跨进程传递的数据采用明确的序列化表示、共享内存偏移量或可重建的标识。

两种获取方式：

-   方案 A（逐条记录下发）：walsender 解析 WAL、判定归属后，把属于该事务的记录经 Task Queue 发给 Worker。
-   方案 B（LSN 区间授权）：walsender 只发送 `[first_lsn, end_lsn]` 区间与子事务映射，Worker 用私有 `XLogReaderState` 只读重放该区间。

| 维度 | 方案 A：逐条记录下发 | 方案 B：LSN 区间授权 |
|---|---|---|
| Worker 输入 | 记录字节/解析结果经 Task Queue 逐条发送 | 区间与子事务映射；Worker 只读重放区间，跳过不属于本事务的记录 |
| IPC 流量 | 与变更数量成正比，每条记录一次拷贝、一次消息 | 只有控制消息，与事务大小无关 |
| walsender CPU | 解析 + 拷贝双重负担，容易仍成为瓶颈 | 只做归属识别（读记录头），负担最低 |
| pending 事务的内存 | 等待 Worker 的事务变更仍需 walsender 有界缓存（等效于今天的 ReorderBuffer 缓冲） | 不存在该问题：未分配事务只占 LSN 区间元数据，WAL 本身就是缓冲 |
| WAL 保留 | 无额外要求 | 依赖复制槽 `restart_lsn` 防止段回收；在途区间必须落在保留范围内（槽语义已覆盖） |
| 实现复杂度 | 低；进程与通信骨架简单 | 较高；需要 Worker 侧 WAL 读取的错误恢复、区间边界与私有 reader 生命周期管理 |
| 正确性边界 | 归属完全由 walsender 决定，最保守 | Worker 仍不建立全局快照/事务状态、不自行决定读取范围，不违反 §1.3 非目标 |

先实现方案 A，阶段 2 结束前测量 walsender CPU 占比与拷贝开销（§11.1）。walsender 成为可测量的瓶颈时，切换到方案 B，或按事务大小混合使用（大事务区间授权、小事务逐条下发）。openGauss 采用的同样是集中读取加队列分发（§9.1），但它的队列免拷贝依赖线程共享内存，PostgreSQL 进程模型下这一点不成立。方案 B 的 WAL 读取参考入口：`src/backend/access/transam/xlogreader.c` 与 `src/backend/replication/walsender.c` 的 `logical_read_xlog_page()`。

### 5.3 输出插件调用位置

解码流水线在 Worker 与 walsender 之间切断：切断点之前的段在 Worker 并行执行，之后的段收回到 walsender。Worker 不发送协议消息，段 (e)（协议写出与网络）固定在 walsender；可变的只有插件回调段 (d) 的执行位置。

基线做法是插件回调集中在 walsender 单一后端执行，称集中调用；扩展做法是每个 Worker 持有私有插件实例执行回调、直接产出协议帧，称 Worker 调用。集中调用是默认实现，现有插件不用修改；Worker 调用是否实现由基准数据决定，未声明能力的插件回退到集中调用。

单事务解码成本分五段：(a) 记录解析与缓存、(b) 目录查找、(c) TOAST 重组、(d) 插件回调（pgoutput 的 tuple 到协议消息序列化）、(e) 协议写出与网络。两种做法下 (a)–(c) 都在 Worker 并行，(e) 都在 walsender，差别只在 (d)。据此：

1.  (d) 段占比决定收益上限。占比高时（阈值暂定 30%），(d) 在集中调用下是串行段，加速比受限于 (a)–(c) 的可并行比例。
2.  插件兼容。集中调用不动插件 API；Worker 调用要求插件每事务实例可重入、声明能力，`pgoutput` 的跨事务共享状态（relation 元数据缓存、`in_streaming`）要逐项审计。
3.  中间表示。集中调用要定义可序列化的中间结果（tuple 字节、relation 标识、操作类型），编解码有开销，也是新增代码路径；Worker 调用的输出即协议帧，不需要中间表示。
4.  协议。两种做法都保持单一出口，跨事务的协议帧（keepalive、status）都由 walsender 生成，差异只在 (d) 段消息的产生位置。

集中调用（基线实现）：Worker 产出可序列化的中间结果（解析后的变更、TOAST 重组结果等），walsender 在单一后端上下文中按序调用输出插件。插件被调用的方式与串行解码相同，仅时机推迟到结果回放时。优点：输出插件 API 不变；单一插件实例，协议状态机风险小；对第三方插件透明。局限：(d) 段串行，加速比受 (d) 占比限制；中间结果的序列化表示需要定义且有成本。

Worker 调用（扩展实现，需插件声明支持）：每个 Worker `LoadOutputPlugin()` 加载私有插件实例，直接产出该事务的协议消息字节流；插件经 `OutputPluginPrepareWrite`/`OutputPluginWrite` 的 writer 回调被重定向为向 Result Queue 写长度前缀帧；walsender 只按提交序拼接帧、写网络。插件的 `startup_cb` 依赖连接参数（`protocol_version`、publication 列表、binary 标志等构造 `PGOutputData` 的输入），这些参数随 Worker 初始化一次性序列化传入——连接参数在会话建立时确定、期间不变，无需重复传递。收益：(d) 段并行化；Worker 的输出即协议字节，Result Store 不需要自定义中间表示，spill/重载按帧存取即可。代价：要求插件满足每事务独立实例的可重入约束；需要新的插件能力声明与协商（未声明支持的插件使用集中调用）；`pgoutput` 的跨事务共享状态需要逐项审计；事务间共享的协议帧仍由出口统一生成。

插件输出的重定向有现成机制：`CreateDecodingContext` 的 writer 回调按上下文注入，walsender 注入 `WalSndPrepareWrite`/`WalSndWriteData`，SQL 接口注入 `LogicalOutputPrepareWrite`/`LogicalOutputWrite`。Worker 调用插件时以同样方式注入写 Result Queue 的 writer，插件代码不感知差异；集中调用下 walsender 在结果回放时沿用原有 `WalSndWriteData` 路径。不能假设同一个 `PGOutputData` 实例可被多个 Worker 并发重入；Worker 调用下每个 Worker 持有私有实例，跨实例共享的状态收敛为只读配置或显式同步。若现有插件 API 无法表达事务结果的延迟生成或多事务并行完成，需要在详细设计阶段明确增加的接口与兼容策略；协议行为未定义前，不认定该设计对现有客户端可用。

参照：订阅端并行 Apply 中每个 Worker 进程独立维护应用上下文与缓存，发布端 Worker 调用需要对 `pgoutput` 完成同样的审计；openGauss 的解码线程直接产出最终输出格式（§9.1），验证了该形态可行。

阶段 1 的成本拆解基准给出 (d) 的实测占比，低于阈值维持集中调用，超过则把 Worker 调用列为阶段 4 的实现目标（分阶段见 §14.4）。

### 5.4 协议消息只由 walsender 发送

每个 Worker 生成带有事务标识和排序元数据的结果，交给 walsender。输出前检查：Worker 是否完成该事务的所有输入；提交/中止状态；所需快照与失效事件是否已消费追平；结果是否符合当前输出插件和逻辑复制协议的消息约束；是否可以推进已发送位置。之后由原有输出插件和 `walsender` 协议发送路径生成、发送消息。这样避免多个 Worker 并发修改 `pgoutput` 的状态，或在网络流中交错写入不完整的协议消息。

不同事务按不同速度完成解码，Worker 最先完成不等于事务最先输出。需要区分四种顺序：WAL 记录顺序（walsender 读取和识别记录的顺序）、事务提交顺序（由提交记录 LSN 确定）、Worker 完成顺序（由运行时调度和事务复杂度决定）、协议输出顺序（由协议语义、事务边界、插件契约与提交顺序共同决定）。

输出按事务提交顺序。这会引入 head-of-line blocking：一个解码慢的大事务阻塞其后所有已解码事务的输出。这是按提交序输出的直接结果，处理手段是 Result Store 的缓存与 spill（§5.5），而不是打乱输出顺序。若协议扩展允许多事务流交错，必须同时设计并验证发布端与订阅端的协议状态机，不能仅凭发布端内部并行完成就认定订阅端可用。

### 5.5 结果暂存与输出流控

Worker 完成的先后与事务提交顺序不一致，输出又必须按提交序，结果在输出前必然经过缓冲；要确定的只是缓冲的位置、容量和溢出行为：

1.  位置：walsender 侧的 Result Store。缓冲不能放在 Worker 里，否则输出顺序上的等待会占用 Worker，并行度退化到输出顺序；Worker 移交结果后立即回收。
2.  容量：有界。队头慢事务会让后续已解码事务持续堆积，无界缓冲意味着内存随复制延迟增长。
3.  溢出：按事务 spill 到磁盘，文件布局与清理复用 `ReorderBufferSerialize` 的既有模式。
4.  流控：Result Store 达到硬上限时暂停 WAL 读取推进，代价是 WAL 保留时间变长，期间 keepalive 照常（§4.6）；不丢结果，也不让 Worker 停在原地占用池子。

具体行为：Worker 把结果写入 Result Queue 并得到 walsender 确认、完成本地清理后回到 `IDLE`（§6.1）；Result Store 以内存缓冲为主，内存占用超过 `parallel_decode_result_queue_size` 后按事务粒度 spill 到磁盘文件；解码完成后 walsender 才识别 ABORT 的事务，其条目（含已 spill 的文件）直接删除；事务到达输出条件时从内存或 spill 文件装载，经协议出口写出，随后推进已输出位置并释放该事务条目；结果按完整协议帧/事务段组织，spill 与重载不破坏消息边界。

### 5.6 并行资格与特殊记录处理

以下记录/事务当前按保守规则处理，全部或部分排除在并行之外：

1.  非事务性 logical message（`pg_logical_emit_message()` 的 `transactional => false`）：不属于任何顶层事务，但必须在精确的 WAL 位置上输出。并行场景下它与前后事务的输出相对顺序必须与 WAL 顺序一致。处理规则：遇到非事务消息时建立输出排序点，walsender 先输出该 LSN 之前的全部事务结果，再输出消息本身，然后放行后续事务；该规则是输出判定条件之一，不回退串行解码。
2.  `two_phase` / prepared transaction：PREPARE 与 COMMIT PREPARED 可能相距很远，且 `two_phase = on` 的槽在 PREPARE 点就有输出语义，排序约束复杂。处理规则：`two_phase` 槽禁用并行解码，整槽回退串行；仅对 2PC 事务回退、其余事务并行的可行性留待后续完善。
3.  无 XID 事务 / 仅含失效消息的事务：归属与输出语义需要单独定义。当前由 walsender 按串行路径就地处理，不进入并行调度。
4.  包含目录变更（DDL）的事务：正常进入并行调度，但默认触发目录失效追平（§4.5），受影响 Worker 等待事件应用确认后再继续。
5.  请求流式输出的槽（订阅端 `streaming = on/parallel`）：并行解码锚定提交点（§4.3），不与发布端流式输出叠加；连接建立时检测到 streaming 请求的槽整槽回退串行（理由见 §4.2 的 ReorderBufferStreamTXN 行，客户端行为见 §3.3）。

------------------------------------------------------------------------

## 6. Worker 状态机与事务簿记

### 6.1 Worker 状态机

``` mermaid
stateDiagram-v2
    [*] --> STARTING
    STARTING --> IDLE: 就绪（首个空闲心跳）
    IDLE --> BUSY: 分配事务
    BUSY --> IDLE: 结果移交确认，本地清理完成
    IDLE --> [*]: 池关闭，立即退出
    BUSY --> [*]: 池关闭，完成/取消在途后退出
```

Worker 的共享状态只需要回答一个问题：这个 Worker 能不能被分配。回答它需要"进程就绪了吗"（STARTING → IDLE）与"有没有事务绑定"（IDLE ↔ BUSY），因此状态机只有三个状态。早期草稿中的 FINISHING（结果在途、等待移交确认）并入 BUSY——它对外的事实就是"仍有绑定、不可再分配"；此间崩溃是否重放，由 walsender 侧的 generation 与移交记录判定（§8.2），不依赖 Worker 当时处于哪个子阶段。

两个正交的维度曾被视为状态，实际不需要共享枚举：

| 维度 | 回答的问题 | 表达方式 |
|---|---|---|
| 绑定（本状态机） | 能否被分配 | STARTING/IDLE/BUSY 共享枚举 |
| 进程生命周期 | 进程活着吗、在退出吗、要不要重建 | 进程句柄存活性 + 池级 closing 标志 + 管理器簿记 |

生命周期的处理：池关闭是池级标志，closing 置位后 IDLE 的 Worker 立即退出、BUSY 的 Worker 完成或取消在途事务并确认移交后退出，管理器经后台 Worker 句柄等待全部退出（`WaitForBackgroundWorkerStartup` 一族机制的对应用法）；崩溃由句柄存活性与心跳（§4.6）检测，slot 回收与是否重建是管理器的策略记录。退出与崩溃不进状态机。

状态机的关键不变量：`BUSY → IDLE` 的唯一路径是结果写入 Result Queue、得到 walsender 确认且本地清理完成——移交确认前不回到 IDLE，保证不会被再分配（§5.5）。Worker 在绑定期间不存在需要异步等待的条件：事件与任务由 walsender 单生产者写入，引用 `required_event_id` 的任务入队之前，事件队列已包含至该位点的全部事件（§10.3 不变量 11），Worker 收到任务后先排空事件再解码（§4.4）。Worker 的阻塞点只有队列接收本身；真正的条件等待在 walsender 侧——失效追平等待（§4.5）与输出流控（§5.5）。

### 6.2 事务的簿记：位置与标志

原 ReorderBuffer 不为事务维护生命周期状态机：事务的属性用标志位表达，"阶段"由所在的容器表达——`by_txn` 哈希中即"已知"，`toplevel_by_lsn` 链表上即"按提交序可迭代"，spill 后置 `RBTXN_IS_SERIALIZED`；提交与中止同样是标志位（`RBTXN_IS_COMMITTED`、`RBTXN_IS_ABORTED`）。十三个 `RBTXN_*` 标志里没有一个叫"状态"的字段——串行解码没有调度问题，一个执行流按提交序逐个处理，不需要状态机。

并行解码引入了两个新的"位置"（等待分配、结果等待输出），但不需要因此引入状态枚举。Worker 状态机（§6.1）已经回答"这个 Worker 能否被分配"；事务侧要回答的问题由容器成员关系和字段回答：

| 问题 | 表达方式 | 串行 ReorderBuffer 中的对应 |
|---|---|---|
| 等待分配吗 | pending queue 的成员 | 无（串行无池） |
| 被谁处理 | Worker 的 `top_xid` 绑定；事务条目的 `worker_id` 仅为查找缓存，权威在 Worker 侧 | `by_txn` 哈希的存在性 |
| 结果可输出吗 | Result Store 中携带 `commit_lsn` 的条目，按提交序取最小者 | `toplevel_by_lsn` 的可迭代性 |
| 失败了吗 | 失败标志；处置依据 generation 与移交记录（§8.2） | 无 |

事务的生命周期即容器迁移路径：

```
登记入 ParallelDecodeTxnHash
  →（无空闲 Worker）pending queue
  → 绑定 Worker（接收/缓冲/解码/结果在途，均为 Worker 的 BUSY）
  → 结果移交入 Result Store（Worker 回 IDLE）
  → 按提交序输出，清理销毁
```

中止在任何位置都直接销毁条目、解除绑定并通知 Worker 丢弃缓冲，无输出；失败置标志待 §8 处置。沿用"位置即状态"而不引入第二套簿记，避免状态字段与事务实际所在容器失同步——这也是既有 ReorderBuffer 代码的风格。

------------------------------------------------------------------------

## 7. Worker Pool 与流控

### 7.1 队列满与流控行为

-   事务排入 pending queue。
-   walsender 仍在安全和资源允许范围内推进全局 WAL 状态，不因某个事务等待 Worker 而丢失其他事务的归属信息。
-   对等待任务的输入缓存必须有界。达到队列或缓存阈值后，walsender 进入流控，暂停额外任务派发或降低读取速度；不无限积累 WAL 记录的内存副本。若采用 §5.2 方案 B，未分配事务只占 LSN 区间元数据，该压力大幅降低。
-   Worker 完成并清理后，调度逻辑唤醒等待任务，重新检查事务是否仍可分配（未中止、未取消）。
-   输出侧的对称流控（Result Store 硬上限）见 §5.5。
-   内存总量核算：方案 A 下总占用 = walsender pending 缓冲 + 各 Worker 的 `logical_decoding_work_mem` 阈值（N 份）。`parallel_decode_input_queue_size` 只约束 pending 部分，与 `logical_decoding_work_mem` 管的是同一类缓冲（未完成事务的变更），调优按总量考虑；两者的边界重叠在详细设计中统一（候选：pending 缓冲直接复用 `logical_decoding_work_mem` 语义）。

### 7.2 Worker 生命周期

Worker 在以下步骤全部完成后回到 `IDLE`：收到该事务的全部输入及终止标记；完成事务解码或完成可恢复错误/取消处理；将结果或错误可靠地交给 walsender（结果进入 Result Store，移交即算完成，不等输出）；清理事务级内存上下文、快照引用、子事务状态、TOAST/重排序相关临时状态及其他事务私有资源；从 `ParallelDecodeTxnHash` 中移除事务绑定；更新共享状态为 `IDLE`；通知调度逻辑有 Worker 可用。

如果 Worker 进程崩溃，管理器把该 Worker 标记为 `FAILED`，取消或隔离其未完成任务，并按明确的重放规则恢复。在未确认结果是否已移交前，不把同一事务分配给另一个 Worker，否则可能重复产生输出。

------------------------------------------------------------------------

## 8. 错误处理、取消与资源回收

### 8.1 错误分类

| 错误类别 | 处理 |
|---|---|
| 事务级、可定位的解码错误 | 将错误连同 XID/LSN 交给 walsender，由会话按现有逻辑解码错误策略处理；不静默丢弃事务 |
| 目录状态不确定 | 等待失效追平，无法解决时回退串行 |
| Worker 进程崩溃 | 标记 Worker 失败，隔离任务与结果，按任务 generation 和结果移交状态决定是否重放 |
| Worker 失联（心跳超时，§4.6） | 等同进程崩溃处理：标记失败并隔离其在途任务 |
| DSM/消息队列损坏或断开 | 视为 Worker/任务失败；不继续使用不完整结果 |
| 会话取消或复制连接断开 | 停止新任务分配，取消或排空在途任务，清理 DSM、共享队列、Result Store 及 spill 文件 |
| 输出插件错误 | 由单一输出端统一处理，避免多个 Worker 各自报告引发不一致状态 |

### 8.2 重放与幂等边界

Worker 的解码计算可以在明确的事务边界内重做，但结果不能重复输出。每个任务具有 generation/attempt 标识；walsender 只接受当前有效 generation 的结果，并记录结果是否已提交给协议发送路径。

复制连接异常时，恢复依赖复制槽及逻辑解码的既有恢复语义。并行解码新增的任务状态不取代 WAL/复制槽中可恢复的权威位置。

并行模式会放大错误的重放代价：任一在途任务失败终结会话后，从 confirmed_flush 起的全部解码（包括已解码未输出的事务）都要重做，在途任务越多放大越明显。当前接受该代价，以 `decode_worker_restart_total`、`decode_output_lag` 监控；若基准表明重放代价不可接受，再评估对已解码未输出事务做结果级 checkpoint（受已输出位置约束），留待后续完善。

### 8.3 资源清理

需要覆盖正常结束、ABORT、错误、取消、Worker 崩溃、复制连接断开及服务器关闭等路径。所有路径都必须释放或隔离：DSM 和 `shm_mq`；事务映射和 Worker 绑定；事务私有内存上下文与快照引用；子事务和解码临时状态；Result Store 中已缓冲的事务结果与 spill 文件（按 generation 失效的条目一并清除）；队列中属于失效 generation 的任务/结果；Worker 进程及其注册信息。

------------------------------------------------------------------------

## 9. 方案对比：openGauss 与 PolarDB

本章调研三个参照：openGauss 的发布端并行逻辑解码（与本设计最接近的先行实现）、PolarDB 开源版的调研结论、PG18 订阅端并行 Apply（进程模型与生命周期的复用参考）。

### 9.1 openGauss 并行逻辑解码

openGauss 3.0.0 引入发布端并行逻辑解码，经连接选项 `parallel-decode-num`（大于 1 且不超过 20）开启，官方描述其结构为"一个读取线程、多个解码线程以及一个发送线程协同"。以下基于 openGauss master 分支源码（`src/gausskernel/storage/replication/logical/` 下的 `parallel_decode.cpp`、`parallel_decode_worker.cpp`、`parallel_reorderbuffer.cpp`、`logical_queue.cpp`）与官方《并行逻辑解码》文档整理，细节以对应版本为准。

执行模型与流水线。openGauss 是线程架构，三类线程运行在同一进程地址空间内：

1.  reader 线程：读 WAL 并解析记录（`ParseProcessRecord`），生成 `ParallelReorderBufferChange` 放入各解码线程的 changeQueue；
2.  N 个 decoder 线程：从自己的 changeQueue 取 change，解码为 `logicalLog`（tuple 序列化、TOAST 重组），放入各自的 LogicalLogQueue；
3.  sender（walsender 线程）：按序汇总各 LogicalLogQueue 的结果发送给客户端。

队列 `LogicalQueue` 是进程内无锁环形队列，由原子 `writeHead`/`readTail` 推进，传递指针而非数据拷贝；`logicalLog` 内存池由 reader 线程申请、decoder 线程经 CAS free 链释放。

事务分发与顺序。同一事务的变更定向到同一解码线程，子事务 XID 数组随逻辑日志传递（ASSIGNMENT 记录）；发送侧的 `ParallelReorderBuffer` 以 `toplevel_by_lsn` 组织事务并取最老事务汇总输出，源码注释明确要求 walsender 获得的逻辑日志保持有序。即按事务提交序输出、事务内按 LSN 序，与本设计的输出顺序约束相同。

输出与生命周期。解码线程直接产出最终输出格式，源码提供 text/json/二进制三种序列化回调（`parallel_decode_change_to_text/_json/_bin`），配合白名单过滤（`FilterWhiteList`）与批量发送选项，面向 openGauss 自有客户端（JDBC、扩展的 pg_recvlogical），不经过 PostgreSQL 复制协议。线程组由 dispatcher 槽位管理（上限为 `min(max_replication_slots, 20)`），`StartLogicalLogWorkers`/`StopParallelDecodeWorkers` 负责起停。

性能与配置。官方数据：解码速度从 3~5 MB/s 提升到标准场景（16 核 CPU、128G 内存、网络带宽大于 200MBps、单行 0.1~1KB、以 insert 为主、单事务语句数小于 4096）下的 100 MB/s；建议预留"并发数 + 2"个 CPU 核，每个备机只建立一条并行解码连接。

限制。官方约束包括：不支持 DDL 解码（事务中执行 DDL 后，该 DDL 及其后的语句均不解码）；不支持列存、压缩表、数据页复制、分布式事务的解码；支持的数据类型集合明显小于 PostgreSQL；依赖备机解码能力。

### 9.2 PolarDB 调研结论

对开源 PolarDB for PostgreSQL（Gitee `polardb/PolarDB-for-PostgreSQL`，POLARDB_11_STABLE 分支）核实的结果：`src/backend/replication/logical/` 目录只包含上游 PostgreSQL 的标准文件（decode.c、reorderbuffer.c、snapbuild.c 等），没有并行解码专有实现；开源文档站（polardb.github.io/polardb-pg-docs）的特性文档中也没有并行逻辑解码条目。即当前开源 PolarDB-PG 没有发布端并行逻辑解码可供对比。

容易混淆的是其可用性特性"并行回放"（`avail-parallel-replay`）：Startup 进程只解析 WAL 并构建 LogIndex，后台 Dispatcher 按 LSN 检索并把"某条 WAL 作用于某个数据块"拆成回放子任务，以 PageTag 为序号保证同一数据块上回放的先后顺序，分发给并行回放进程组。它是备库物理 WAL 回放的并行化，与逻辑解码无关；但其"解析与执行分离、按子任务分发、以 Tag 保序"的结构与本设计"读取与解码分离、按事务分发、以提交序输出"同构，可作为独立问题域里的旁证。商业版 PolarDB 的公开资料未能核实到发布端并行逻辑解码特性；若后续获得其内部资料，可补充进对比表。

### 9.3 对比分析

| 维度 | openGauss 并行解码 | PolarDB（开源） | 本设计 |
|---|---|---|---|
| 是否有发布端并行逻辑解码 | 有（3.0.0 起） | 无（核实结论见 §9.2） | 本设计的目标 |
| 执行模型 | 线程，同进程共享地址空间，队列传指针 | —（物理并行回放为多进程 + LogIndex 分发） | 进程，DSM + `shm_mq`，跨进程序列化 |
| 并行单位 | 顶层事务固定到同一解码线程 | — | 顶层事务绑定唯一 Worker（一致） |
| WAL 读取 | 独立 reader 线程读取并解析，change 分发进各线程队列 | — | walsender 读取与归属；方案 B 下 Worker 只读授权区间 |
| 全局状态 | 快照/事务状态在同进程内共享，加锁访问 | — | walsender 唯一权威，快照与失效经事件流分发（§4） |
| 输出协议 | 自有 text/json/二进制格式 + 批量发送，不保持 PG 协议兼容 | — | 严格保持 PG 复制协议字节流与插件 API（§3） |
| 语义范围 | 不支持 DDL、类型与表类型受限，正确性范围收窄 | — | 与串行解码完全等价，DDL 经失效追平处理（§4.5） |
| 输出顺序 | 发送侧按 `toplevel_by_lsn` 取最老事务，提交序输出 | — | 按提交序输出 + Result Store（§5.5），机制对应 |
| 缓冲与流控 | 有界环形队列 + logicalLog 内存池 | — | Pending Queue / Result Store 有界 + 磁盘 spill |
| 失败域 | 线程崩溃即进程（实例级）故障 | — | Worker 进程隔离；当前实现传导为会话错误后按槽恢复语义重放 |
| 性能参照 | 官方 3~5 → 100 MB/s（限定场景） | — | 以 §12.5 目标值与基准为准 |

与 openGauss 差异的根源在于前提不同，而不是工程偏好不同。openGauss 的三个前提放宽了本题最难的约束：线程模型消除了跨进程序列化成本，队列传指针即可；自有输出协议绕开了插件生态与字节级等价的要求，解码线程直接产出最终格式；不支持 DDL 解码则回避了目录一致性问题。PostgreSQL 的进程模型、既有插件生态与协议兼容要求都无法放弃，因此本设计必须正面解决 openGauss 得以绕开的三件事：跨进程序列化（§5.2）、插件调用位置（§5.3）、目录一致性（§4.5）。

订阅端并行 Apply 也没有目录一致性意义上的对应机制——apply worker 是普通后端，用普通快照应用 DML，本地 DDL 经标准 sinval 失效框架自动传播，且核心逻辑复制不复制 DDL；它拥有的是一组排序同步（`serialize_changes` 落盘回放、`PARALLEL_TRANS_*` 事务边界等待、`pa_lock_stream` 流式块锁），解决的是事务间顺序，不是目录状态。

### 9.4 可借鉴的点

1.  reader/decoder/sender 三级流水线验证了"集中读取 + 并行解码 + 单点发送"的总体结构，与本设计同构。
2.  事务固定解码线程、发送侧按 LSN 重排，与本设计"事务绑定 Worker + 按提交序输出"互相印证。
3.  openGauss 解码线程直接产出最终输出格式，验证了插件回调段 (d) 并行化的可行形态（对应 §5.3 的 Worker 调用）。
4.  "并发数 + 2"的 CPU 预留与每连接独占资源的口径，与本设计 `max_worker_processes` 配额约束（§14.2）同类。
5.  openGauss 以排除 DDL 解码回避目录一致性，反证了 PostgreSQL 生态下失效追平的必要性——PG 不能把 DDL 排除在解码范围之外。
6.  PolarDB 并行回放"解析与执行分离、按 Tag 保序的子任务分发"，从物理回放这个独立问题域旁证了本设计的分发/保序结构。

openGauss 的 100 MB/s 在其声明的限定场景（insert 为主、无 spill、类型受限）取得，不能直接外推到 PostgreSQL 负载，仅作数量级参照。

### 9.5 PG18 订阅端并行 Apply 的复用参考

主要参考源码：`applyparallelworker.c`、`worker.c`、`launcher.c`（`src/backend/replication/logical/`）、`worker_internal.h`。

Decode Worker 与并行 Apply Worker 的运行机制应当一致，机制级对照如下：

| 机制 | 并行 Apply Worker（源码核实） | Decode Worker（本设计） | 一致性 |
|---|---|---|---|
| 进程模型 | leader apply worker + Worker 进程池，每订阅一个池 | walsender + Worker 进程池，每复制会话一个池（§2.2） | 一致 |
| 池管理 | `ParallelApplyWorkerPool`，受 `max_parallel_apply_workers_per_subscription` 上限，池满复用等待 | Worker Pool，受 `max_parallel_decode_workers_per_walsender` 上限，池满排队（§5.1） | 一致 |
| 事务绑定 | `ParallelApplyTxnHash`（xid → winfo），一事务一 worker，`pa_allocate_worker` | `ParallelDecodeTxnHash`（xid → worker），一事务一 Worker | 一致 |
| 输入通道 | 每 Worker DSM + `shm_mq`（`pa_setup_dsm`，`shm_toc` 组织） | 每 Worker DSM + `shm_mq`（Task Queue），同样按 `shm_toc` 组织 | 一致（多一条 Event Queue，见差异 1） |
| 错误通道 | 独立 error queue（`error_mq_handle`） | Error Queue | 一致 |
| 事务结束 | `pa_xact_finish` → `pa_free_worker` 归还池复用，进程不退出 | 结果移交确认后回 `IDLE` 复用，进程不退出 | 一致 |
| 顺序责任 | leader 按提交序调度，必要时 `pa_wait_for_xact_finish` 在事务边界等待 | walsender 按提交序输出，必要时追平等待/输出流控（§4.5、§5.5） | 一致（等待点不同，模式相同） |
| 不安全时回退 | `serialize_changes` 落盘串行应用 | 串行回退路径（§4.5） | 一致 |
| generation | `LogicalRepWorker.generation`（uint16，worker 复用时递增） | `task_generation` | 一致 |
| 状态簿记 | `in_use` 标志 + 启动就绪等待 + 事务哈希，无显式状态机 | `STARTING/IDLE/BUSY` 三态（§6.1） | 一致：三态即 `in_use` 加就绪门控的形式化 |
| 崩溃处理 | leader/launcher 检测后按需重启 | 句柄存活性 + 心跳检测，重建策略（§8.1） | 一致 |

差异只有三处，都源于"算的东西不同"而不是机制选择不同：

1.  管道里流的内容：apply worker 收到的是已解码的协议消息（insert/update/stream/commit），它直接应用；Decode Worker 收到的是 WAL 记录或 LSN 区间加快照/失效事件——解码本身就是它的产物。多出的 Event Queue 是这个差异的体现（§4.4）。
2.  输出去向：apply worker 把结果写本地表，各事务独立提交，顺序靠锁与 leader 等待维持；Decode Worker 的结果要汇入单一协议流，必须全局按提交序，因此 Result Store 与输出调度在 apply 侧没有对应物（§5.4、§5.5）。
3.  目录上下文：apply worker 是普通后端，当前快照加 sinval 失效框架即可；Decode Worker 用历史快照，需要事件流分发快照版本与失效（§4.3–§4.5）。

基于机制一致的原则，注册框架优先扩展现有框架：`LogicalRepWorkerType`（WORKERTYPE_TABLESYNC/APPLY/PARALLEL_APPLY）就是扩展点，评估新增 `WORKERTYPE_PARALLEL_DECODE` 复用 `logicalrep_worker_launch` 与 `LogicalRepWorker` 共享注册表，需要补齐的是发布端归属字段（walsender 会话/复制槽标识，替代订阅端的 subscription OID/用户/关系）。要纳入预期的是改造量：`LogicalRepWorker` 数组按 `max_logical_replication_workers` 全局定长、哈希以 subid 为键——发布端两者都没有。若详细设计发现订阅端语义无法干净剥离（launcher 的重启策略等），再退回发布端专用注册表。

必须调整的语义：`pa_allocate_worker()` 先调用 `pa_launch_parallel_worker()`（找空闲，找不到且 `pa_can_start()` 允许再启动新 Worker），本设计池容量固定、池满只排队；Worker 不自行扫描整个 WAL；全局 `SnapBuild` 只由 walsender 推进；Worker 执行逻辑解码工作，不执行 `apply_dispatch()` 的 DML 应用；Worker 不直接写复制协议；Worker 释放时机由解码结果移交、事务私有资源清理及状态注销共同决定。

------------------------------------------------------------------------

## 10. 关键数据结构与不变量

以下是设计层面的逻辑结构，不是 PostgreSQL 18 当前已有类型；最终字段和锁设计在详细设计阶段结合源码确认。

### 10.1 Worker Pool

``` c
/* 设计示意：字段名及类型需在详细设计中确认。 */
typedef enum ParallelDecodeWorkerState
{
    PD_WORKER_STARTING, /* 就绪门控：不可分配 */
    PD_WORKER_IDLE,     /* 无事务绑定，可分配 */
    PD_WORKER_BUSY      /* 有事务绑定：接收/缓冲/解码/结果在途 */
} ParallelDecodeWorkerState;

typedef struct ParallelDecodeWorkerInfo
{
    int                 worker_id;
    ParallelDecodeWorkerState state;
    TransactionId       top_xid;
    uint64              task_generation;
    uint64              consumed_event_id;
    XLogRecPtr          last_input_lsn;
    XLogRecPtr          last_result_lsn;
    /* DSM / shm_mq 标识、PID、Latch/通知信息等 */
} ParallelDecodeWorkerInfo;
```

### 10.2 事务调度项

事务不加状态枚举（§6.2）：阶段由容器成员关系（哈希、pending queue、Result Store）与 `txn_flags` 标志表达，风格对齐 `RBTXN_*`。

``` c
typedef struct ParallelDecodeTxnEntry
{
    TransactionId          top_xid;
    int                    worker_id;   /* 查找缓存；绑定的权威在 Worker.top_xid */
    XLogRecPtr             first_lsn;
    XLogRecPtr             last_lsn;
    XLogRecPtr             commit_lsn;  /* 有效即"已提交"；方案 B 的分配前提 */
    uint64                 required_event_id;
    uint64                 snapshot_version; /* 本事务解码必须使用的快照版本（§4.3） */
    uint64                 task_generation;
    uint32                 txn_flags;   /* 失败、结果已移交、已输出等标志（仿 RBTXN_*） */
    /* 子事务映射、错误状态及资源引用 */
} ParallelDecodeTxnEntry;
```

### 10.3 需要保持的不变量

1.  一个 `top_xid` 最多只有一个有效 Worker 绑定。
2.  一个 Worker 在任意时刻最多绑定一个顶层事务。
3.  `IDLE` Worker 不保留上一事务的有效上下文。
4.  walsender 是全局 `SnapBuild` 和事务边界状态的唯一写入者。
5.  事务 T 的解码必须使用 T 提交记录 LSN 对应的 historic snapshot 版本；Worker 不复用其他 LSN 时刻的快照（§4.3）。
6.  Worker 消费事件的序号不能越过它实际处理完成的事件。
7.  非事务性消息与其前后事务的输出相对顺序必须与 WAL 顺序一致（§5.6）。
8.  输出顺序不依据 Worker 的完成时间判定。
9.  复制槽的 `restart_lsn` 与 `catalog_xmin` 只能推进到"已完成输出、且所有在途 Worker 的失效消费进度均已越过"的位置（§4.7）；`confirmed_flush` 以订阅端反馈为准，不超过已发送位置。
10. 任务队列、输入缓存和 Result Store 均有界；达到上限走 spill 或流控，不允许无界增长（§5.5、§7.1）。
11. 引用 `required_event_id` 的任务写入 Worker 任务队列之前，该 Worker 的事件队列必须已包含至该位点的全部事件；由 walsender 单生产者的程序序保证。

------------------------------------------------------------------------

## 11. 性能设计与可观测性

### 11.1 性能目标

性能目标不是让所有 Worker 永远处于忙碌状态，而是在保证正确性的前提下减少单线程解码瓶颈。应分别测量：

-   解码 CPU 成本拆解（§5.3 选型依据）：变更解析与缓存 (a)、目录查找 (b)、TOAST 重组 (c)、输出插件回调 (d)、协议写出与网络 (e) 各自的耗时占比；
-   WAL 读取与 walsender 事务识别耗时（含 §5.2 方案 A 的拷贝/IPC 开销）；
-   任务分配等待时间；
-   Worker 实际解码 CPU 时间；
-   目录失效追平等待时间（walsender 侧）；
-   结果队列等待、Result Store spill 和结果整理时间；
-   输出 head-of-line 阻塞时间（最早未输出事务的等待时长）；
-   协议输出及网络等待时间；
-   spill、DSM/队列流控和 Worker 重建次数。

### 11.2 建议的统计指标

| 指标 | 说明 |
|---|---|
| `decode_worker_pool_size` | 当前会话 Worker Pool 容量 |
| `decode_worker_idle/busy/failed` | Worker 各状态数量 |
| `decode_pending_txns` | 等待分配的事务数 |
| `decode_worker_busy_time` | Worker 处于解码任务状态的时间 |
| `decode_catalog_inval_wait_time` | 目录失效追平等待时间 |
| `decode_task_queue_bytes` | 任务/输入缓存占用 |
| `decode_result_queue_bytes` | Result Store 内存占用 |
| `decode_result_spill_bytes` | Result Store 磁盘 spill 量 |
| `decode_txn_latency` | 从事务首次被识别到结果就绪的耗时 |
| `decode_output_lag` | 最早未完成的必要输出位置与当前解码位置之间的距离 |
| `decode_output_plugin_time` | 输出插件回调/协议消息生成耗时（§5.3 决策依据） |
| `decode_fallback_total` | 因安全性未知等原因回退串行的次数 |
| `decode_worker_restart_total` | Worker 异常退出和重建次数 |

指标按复制会话/复制槽维度可查询，避免只暴露全局汇总而无法定位单个复制流的瓶颈。

### 11.3 基准测试建议

至少覆盖以下负载：大量互不相关的小事务；少量大事务；多事务交错写入；高比例目录访问和系统目录变化；DDL 与 DML 交错；Worker Pool 饱和、pending queue 与 Result Store 流控（含 spill 触发）；Worker 故障、取消与复制连接重连；输出插件处理慢或网络发送慢（观察 head-of-line 阻塞与 Result Store 行为）；非事务 logical message 与事务交错（验证 §5.6 输出排序点）；`two_phase` 槽与 2PC 事务（验证整槽串行回退）；订阅端请求 streaming（验证整槽串行回退，§3.3）；串行模式与并行模式对照。

对照指标包括吞吐量、端到端延迟、CPU 利用率、内存峰值、磁盘 spill 量、WAL 保留量、输出延迟及错误恢复正确性。先证明结果与串行逻辑解码一致，再评估性能增益。

------------------------------------------------------------------------

## 12. 测试与验证

测试以输出等价性为前提：并行解码的输出必须与串行解码逐字节一致，等价性验收通过后才进行性能测试。本章给出各验收域和通过标准，作为 §14.4 各实施阶段的出口条件。

### 12.1 正确性等价验收

| 验收项 | 方法 | 通过标准 |
|---|---|---|
| 输出流等价（基础项） | `test_decoding` 与 `pgoutput` 双工具：同一负载、同一槽分别以串行/并行解码运行，对输出流做逐字节 diff | 零差异 |
| 多事务交错 + 子事务 + ABORT | 并发会话随机写、随机回滚、子事务嵌套 | 零差异 |
| 大事务 + TOAST + spill | 超过 `logical_decoding_work_mem` 与 Result Store 阈值的事务 | 零差异，spill/重载不破坏消息边界 |
| DDL/DML 交错 + 目录失效 | 高频 DDL 与 DML 混合，触发目录失效追平 | 零差异，无追平死锁/饿死 |
| 非事务消息排序 | `pg_logical_emit_message` 非事务消息与事务交错注入 | 消息输出位置与 WAL 顺序一致（§5.6） |
| streaming / two_phase 槽回退 | 以 `streaming=on`、`two_phase=on` 的槽连接并行发布端 | 整槽串行回退，输出与纯串行一致（§5.6、§3.3） |
| 快照可见性 | 长事务、prepared 事务、并发写入并存 | 零差异（可见性错误通常表现为 diff） |
| 错误路径输出前缀 | 解码错误发生时对已输出前缀做串行对照 diff | 错误事务及其后的事务不输出，前缀与串行一致 |
| 槽状态等价 | 同一负载串行/并行各跑一遍，对照 `confirmed_flush`/`restart_lsn`/`catalog_xmin` 的推进轨迹 | 终值一致；允许并行模式阶段性滞后，不允许超过串行值或回退（§4.7） |

### 12.2 调度与状态机验收

-   Worker 状态机（§6.1）的每个转移至少被触发一次，含取消、池关闭与崩溃路径（崩溃路径验证句柄存活性与心跳检测，§4.6）；事务的容器迁移路径（登记 → pending → 绑定 → Result Store → 输出销毁，以及中止与失败）逐条覆盖；通过标准：覆盖矩阵完整。
-   固定池约束运行时断言：任意时刻活跃 Worker 数 ≤ 配置上限；同一 `top_xid` 全程绑定唯一 Worker；池满时事务进入 pending queue 且不新建 Worker。
-   Worker 崩溃重放场景下，一事务一 Worker 与 generation 去重不变量仍成立（§8.2）。
-   生产者顺序不变量断言：引用 `required_event_id` 的任务入队时，事件队列已包含至该位点的事件（§10.3.11）。

### 12.3 流控与资源验收

-   输入队列与 Result Store 分别打满：触发对应流控、内存峰值有上界、输出零丢失零重复。
-   Result Store spill → 重载 → 输出全链路：消息边界完整，与纯内存路径输出一致。
-   等待期间会话活性（§4.6）：人为构造超过订阅端 `wal_sender_timeout` 的追平/流控等待，订阅端不断连、feedback 正常处理。

### 12.4 故障注入验收

| 场景 | 注入方式 | 通过标准 |
|---|---|---|
| Worker 崩溃 | `kill -9` 单个 Decode Worker | 按 §8.1/§8.2 处理；恢复重放后无重复、无丢失输出 |
| Worker 失联 | `SIGSTOP` Worker（心跳超时） | 判定失败并隔离在途任务 |
| DSM/队列损坏 | 模拟 `shm_mq` detach/损坏 | 视为任务失败，不输出不完整结果 |
| 会话取消/连接断开 | 解码中途 CANCEL、断开重连 | DSM/spill/Worker 进程全部回收，无泄漏 |
| 服务器重启 | 立即关机后重启 | 从 `confirmed_flush` 重解码，行为与串行恢复一致 |

### 12.5 性能验收目标（暂定值，以 §11.3 基准校准）

| 指标 | 目标（暂定） |
|---|---|
| 互不相关小事务混合负载吞吐 | ≥1.5×（2 Worker）、≥2×（4 Worker）；walsender CPU 占比 < 50% |
| 大事务负载 | 不低于串行的 0.9× |
| 事务端到端延迟 P99 | 不劣于串行的 1.2× |
| 串行路径回归（`parallel_decode = off`） | 与不含本特性的 PG18 偏差 <3% |
| 回退路径开销（streaming/two_phase 槽） | 相对纯串行额外开销 <5% |

### 12.6 持续测试与阶段出口

-   回归集成：在 `src/test/subscription`（或新目录）编写三类 TAP 测试——等价性 diff、回退行为、故障注入，纳入 `make check-world`。
-   长稳测试：72 小时混合负载 + 随机故障注入：无内存增长、无死锁、输出零差异。
-   阶段出口（对照 §14.4）：阶段 1 出口 = §12.1 全过；阶段 2 出口 = §12.2 + §12.3；阶段 3 出口 = §12.1 的 DDL 项 + §12.3 活性项；阶段 4 出口 = §12.1 全过 + §12.5 初值达成；阶段 5 出口 = §12.5 全部目标 + 长稳通过。

------------------------------------------------------------------------

## 13. 源码衔接点

以下是建议重点阅读/修改的源码位置。具体函数调用链和修改点以目标 PG18 源码树为准，在详细设计阶段逐项确认。

| 文件 | 现有职责 / 参考点 | 本设计中的用途 |
|---|---|---|
| `src/backend/replication/logical/decode.c` | WAL 逻辑解码主流程 | walsender 侧记录归属与分流的改造点（§4.1） |
| `src/backend/replication/logical/reorderbuffer.c` | 事务变更缓存、子事务、事务边界和 spill（`ReorderBufferSerializeTXN`、`logical_decoding_work_mem`、`ReorderBufferExecuteInvalidations`） | 职责拆分结论见 §4.2；失效本地执行的串行基线见 §4.5；Result Store 的 spill 布局参考 |
| `src/backend/replication/logical/snapbuild.c` | 逻辑解码快照构建（`SnapBuildGetOrBuildSnapshot`、`SnapBuildSerialize`） | 快照版本生成与序列化的基础（§4.4） |
| `src/backend/replication/logical/logical.c` | 逻辑解码上下文及相关接口 | `CreateDecodingContext` 的 writer 注入机制是 Worker 重定向输出的挂点（§5.3） |
| `src/backend/replication/logical/logicalfuncs.c` | SQL 接口 `pg_logical_slot_get_changes` | writer 回调替换的现成先例（`LogicalOutputPrepareWrite`/`LogicalOutputWrite`），Worker 侧 Result Queue writer 的实现参照（§4.1、§5.3） |
| `src/backend/replication/logical/applyparallelworker.c` | 并行 Apply Worker Pool、DSM、消息队列和 Worker 主循环 | 最重要的并行 Worker 机制参考（§9.5 机制对照） |
| `src/backend/replication/logical/worker.c`、`launcher.c` | 逻辑复制 Worker 公共实现与启动框架 | `logicalrep_worker_launch` 扩展 `WORKERTYPE_PARALLEL_DECODE` 的评估入口（§9.5） |
| `src/include/replication/worker_internal.h` | 逻辑复制 Worker 类型及内部定义（`LogicalRepWorkerType`、`LogicalRepWorker.generation`） | 机制对照基线（§9.5）；优先评估复用现有注册表 |
| `src/backend/access/transam/xlogreader.c`、`src/backend/access/transam/parallel.c` | WAL 读取器；并行查询 DSM 初始化 | 方案 B 的 Worker 侧私有 `XLogReaderState`；`SerializeSnapshot` 进 DSM 的先例（§4.4） |
| `src/backend/replication/walsender.c`（含 `logical_read_xlog_page`） | 发布端复制会话和输出链路 | 调度、结果收集与单一协议出口在 walsender 内的集成位置；§4.6 活性职责的挂接点；方案 B 的页面读取参考 |
| `src/backend/replication/pgoutput/pgoutput.c`、`src/include/replication/pgoutput.h`（`PGOutputData`） | `pgoutput` 插件和复制协议消息 | Worker 调用的每事务可重入审计对象（§5.3） |

关键边界：`applyparallelworker.c` 是订阅端并行应用实现，不是发布端并行解码的现成实现；`apply_dispatch()` 不作为 Decode Worker 的主要处理函数；`logicalrep_worker_launch()` 及其注册框架可以参考，但发布端 Worker 的会话归属、复制槽和退出生命周期需要单独设计；`pgoutput` 不默认被多个进程共享或并发重入；`SnapBuild`、`ReorderBuffer` 的内部状态与指针不不加分析地跨进程共享，设计上明确 walsender/Worker 的状态所有权，不把现有串行对象复制到 Worker 后就视为正确。

------------------------------------------------------------------------

## 14. 演进与风险

### 14.1 输出插件兼容性

-   集中调用（基线）：插件 API 不变，无需重编译。`test_decoding`、wal2json 等现有插件直接可用，插件不感知并行。
-   Worker 调用（扩展）：需要能力声明（如 `OutputPluginCallbacks` 的并行支持标志）；未声明的插件所在会话使用集中调用——只损失 (d) 段并行，(a)–(c) 段并行保留（§5.3），不是整槽禁用。
-   `test_decoding` 与 `pgoutput` 作为等价性验证工具：串行/并行输出逐字节对照（§11.3）。
-   插件适配 Worker 调用的约束见 §5.3：每事务独立实例、无跨事务可变共享状态、经 writer 回调产出完整消息。

### 14.2 升级、降级与故障切换

-   无新增持久化状态：任务、队列、Result Store 与 spill 文件全部是会话级瞬态；崩溃或重启后按复制槽既有语义从 `confirmed_flush` 重新解码，恢复行为与串行一致（重放代价分析见 §8.2）。
-   孤儿 spill 文件：进程异常终止后由启动/会话清理路径回收（§8.3）。
-   Worker 资源不足：`max_worker_processes` 无余量时连接仍被接受，降级为更少 Worker 或完全串行，记录 WARNING 并计入 `decode_fallback_total`；不因资源不足拒绝复制连接。
-   pg_upgrade / 回滚：GUC 默认 `off`，升级后行为不变；回滚即关闭 GUC。旧内核配新 GUC 会因未知参数启动失败，卸载前先清理配置（标准流程）。
-   PG18 failover slot / 槽同步：并行解码是会话内部机制，不改变槽状态机与同步语义；晋升后新建的解码会话按新节点配置决定模式。
-   备库解码、`hot_standby_feedback` 等：本设计不依赖也不改变这些机制；备库逻辑解码不在当前范围。

### 14.3 与现有流式协议的关系

发布端内部对多个事务并行解码，不改变现有复制协议的约束：协议不允许多个事务的消息任意交错。当前保持按提交序输出（§5.4）。若要支持跨事务交错输出，需要单独定义协议版本能力协商、事务标识、Stream Start/Stop/Commit/Abort 语义以及订阅端状态机，并测试旧订阅端兼容性。

### 14.4 分阶段实施

阶段 1：固定池与串行等价验证——引入 Worker Pool、任务/结果通道、基本生命周期；先用受控任务验证进程通信、错误传递、取消和资源清理；输出仍由 walsender 单点控制（集中调用形态）；完成 §11.1 的解码 CPU 成本拆解基准，据此确定插件调用位置（§5.3）与输入获取方式（§5.2）的最终取舍；对比串行解码结果，验证无丢失、重复或顺序错误。

阶段 2：事务级任务调度——walsender 识别顶层事务和子事务归属；同一顶层事务绑定单个 Worker；实现 pending queue、固定池等待、流控和 Worker 复用；加入提交/中止状态和输出推进管理；若基准显示方案 A 的逐条下发成为瓶颈，启动方案 B（LSN 区间授权）改造。

阶段 3：快照与目录安全——建立有序事件流，实现 §4.3 快照不变量；实现 Worker 事件消费确认；实现目录失效追平与串行回退；覆盖目录变化、DDL/DML 交错及目录缓存失效测试。

阶段 4：输出插件和协议集成——将 Worker 结果收回到 walsender 单点输出，实现 Result Store 与输出流控（§5.5）；按阶段 1 基准的决策执行：维持集中调用，或实现 Worker 调用（Worker 私有插件实例、长度前缀帧输出与插件能力协商）；明确 `pgoutput` 的事务结果接口和协议约束；若需要跨事务交错输出，再单独设计协议扩展及能力协商。

阶段 5：性能调优与可观测性——增加统计视图/指标；优化队列、任务大小、池容量和事件广播；通过多类负载基准确定默认值和并行适用边界。

### 14.5 主要风险

| 风险 | 影响 | 应对 |
|---|---|---|
| 全局快照状态被多个进程修改 | 逻辑解码可见性错误 | 仅 walsender 推进全局 `SnapBuild`；Worker 使用授权版本（§4.3） |
| Worker 使用了错误 LSN 时刻的快照版本 | 可见性错误、结果不一致 | 快照版本绑定事务，不变量 §10.3.5 |
| Worker 的本地目录缓存过期 | 解码结果错误或崩溃 | 有序失效分发、事件确认、失效追平、串行回退（§4.5） |
| WAL 记录归属错误 | 事务内容缺失/重复 | walsender 按 WAL 顺序识别 XID、子事务和顶层事务 |
| Worker 完成顺序不同 | 输出顺序不合法或确认位置错误 | walsender 统一判定输出就绪与槽位置推进（§4.7） |
| Worker 直接调用共享 `pgoutput` | 并发状态损坏、协议消息交错 | Worker 只生成结果，单一出口调用插件/协议；Worker 调用使用私有实例 + 能力协商（§5.3） |
| 集中调用下输出插件成本占比过高 | 并行收益不足 | 阶段 1 成本拆解基准；(d) 段占比超阈值即实现 Worker 调用（§5.3） |
| 输出 head-of-line 阻塞导致结果积压 | 内存膨胀、复制延迟 | 有界 Result Store + 按事务磁盘 spill + 输出流控（§5.5） |
| Worker 池满后无界缓存 | 内存增长、复制延迟 | 固定池、pending queue、队列上限与流控（§7.1） |
| 等待追平/流控期间缺失 keepalive | 订阅端 `wal_sender_timeout` 断连 | 所有等待走带超时 WaitLatch 循环并处理活性职责（§4.6） |
| 非事务消息与并行事务交错输出 | 协议语义破坏、订阅端状态错乱 | 非事务消息形成输出排序点（§5.6） |
| 请求 streaming 的槽误入并行路径 | 流式协议语义与提交点锚定冲突 | 连接建立时检测 streaming 参数，整槽串行回退（§5.6、§3.3） |
| Worker 崩溃后盲目重试 | 重复输出或丢失结果 | generation 标识、结果移交确认和明确重放边界（§8.2） |
| 错误后整体重放代价放大 | 故障恢复期间复制长时间停滞 | 接受当前限制并监控；必要时评估结果级 checkpoint（§8.2） |
| 直接复用订阅端 Worker 注册语义 | 生命周期/归属混乱 | 复用通用机制，发布端注册表与会话归属单独评估（§9.5） |
| 把所有事务强制并行 | 目录/协议/事务语义风险 | 并行资格判断（§5.6），未知时等待或串行回退 |
| 只测试吞吐不验证等价性 | 隐蔽数据错误 | 先做串行对照、故障注入和长时间一致性测试（§12） |

------------------------------------------------------------------------

## 15. 结论

本设计的总体方案：walsender 串行维护全局状态（WAL 顺序、事务边界、全局 `SnapBuild`、槽位置），固定 Worker Pool 并行处理顶层事务，快照与目录失效经有序事件流分发，目录安全由失效追平保证，解码结果经有界 Result Store 暂存后由单一协议出口按提交序输出。

实现上借鉴 PG18 `applyparallelworker.c` 的 Worker Pool、DSM、`shm_mq`、Latch、错误传递和生命周期管理机制，但与订阅端并行 Apply 不同：发布端 Worker 处理的是 walsender 归属后的事务解码任务，不是 `apply_dispatch()` 的 DML 应用工作。与 openGauss 的差异同样源于前提：它以线程模型、自有输出协议、排除 DDL 三个前提绕开了本题最难的三件事，本设计必须正面解决跨进程序列化（§5.2）、插件调用位置（§5.3）、目录一致性（§4.5）。

主要设计约束：一个顶层事务始终只绑定一个 Worker；Worker 池满时排队等待，不临时创建 Worker；walsender 是全局 WAL、事务边界与 `SnapBuild` 的唯一权威，事务解码使用其提交 LSN 对应的快照版本；快照和失效事件有序消费，无法证明安全时等待追平或串行回退；Worker 不直接发送协议消息，输出条件、结果暂存与槽位置推进由 walsender 统一管理（§4.7）；扩展现有 `pgoutput`/协议前，先明确事务结果的输出顺序和兼容性边界。

详细设计启动前需要完成三项选型（判据见 §5.2、§5.3、§5.5）：Worker 输入的获取方式、插件回调的执行位置、结果暂存与流控。其中结果暂存与流控已定为基线，其余两项以阶段 1–2 的基准数据裁决。

进入详细设计前需要通过源码验证的三个问题：现有逻辑解码/ReorderBuffer 状态中哪些能按事务隔离；目录失效与快照事件应从哪些位置发布；现有输出插件 API 如何承接延迟完成的事务结果。这三项决定了核心改动范围与正确性边界。

------------------------------------------------------------------------

## 附录 A：术语表

| 术语 | 含义 |
|---|---|
| Decode Worker | 后台 Worker 进程，负责一个顶层事务的逻辑解码任务 |
| Worker Pool | 固定容量的 Decode Worker 集合 |
| Pending Queue | 等待空闲 Worker 或等待安全条件满足的事务队列 |
| Transaction Context | 某个顶层事务的变更、子事务关系、快照引用及解码状态 |
| Event Stream / Event Queue | walsender 按序发布快照版本、目录失效及控制事件的通道（§4.4） |
| 目录失效追平 | 目录变更后，walsender 等待受影响 Worker 把失效事件消费到指定位点（`consumed_event_id` 追平发布位点）的同步机制；语义对应 Linux RCU 的静默点等待（§4.5） |
| 结果收集与输出调度 | walsender 进程内的逻辑：接收 Worker 结果、维护 Result Store、判定输出条件与槽位置推进（§4.7）、按提交序交给协议出口 |
| Result Store | walsender 侧的事务结果暂存：内存为主，超过上限后按事务 spill 到磁盘（§5.5） |
| 已输出位置 | 已按提交序完成输出的事务的最大提交 LSN；Result Store 条目的回收以它为界限，不等于任意 Worker 的最大已完成 LSN，也不等于 `confirmed_flush`（后者由订阅端反馈驱动，§4.7） |
| Historic Snapshot | 事务解码使用的历史快照；必须取该事务提交记录 LSN 对应的版本（§4.3） |
| 集中调用 / Worker 调用 | 插件回调的两种执行位置：集中在 walsender 单一后端（基线）/ 移入 Worker 私有插件实例（扩展，需插件声明支持）（§5.3） |
| 快照版本 | walsender 生成的 historic snapshot 的编号；跨进程传递后按编号取用，窗口内的提交复用同一份（§4.4） |
