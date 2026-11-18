# 逻辑复制并行解码 需求说明书

| 项目 | 内容 |
| --- | --- |
| 特性名称 | 逻辑复制发布端并行解码（parallel decoding） |
| 目标版本 | PostgreSQL 17（本仓库分支 `pa_decode`） |
| 关联文档 | `design.md`（概要设计说明书） |
| 文档状态 | 草稿 |

## 1. 背景

逻辑复制基于 WAL 逻辑解码，把源库的 INSERT、UPDATE、DELETE 等变更转换为逻辑变更事件，经发布（Publication）/ 订阅（Subscription）机制同步到目标库。它是异构数据库同步、多主库数据同步、数据库滚动升级等场景的底层能力，异构迁移工具 exbase 的底层机制即为逻辑复制。

逻辑复制链路分为两端：

- **发布端**：读取 WAL，维护一致性快照，重组事务，将 WAL 解析为 tuple 变更，通过 TCP 流复制协议发送给订阅端。
- **订阅端**：接收变更，在 apply worker 中并行回放并落盘。

自 PG16 起订阅端支持流复制协议（streaming protocol），可启动多个 apply parallel worker 并行回放；发布端始终是单进程串行解码。发布端因而成为整条链路的瓶颈：它消耗 CPU 最多，却只能用到一个核。

## 2. 现状与问题

### 2.1 现状

发布端的全部工作都在一个 walsender 进程内串行完成（`src/backend/replication/logical/decode.c:88` 的 `LogicalDecodingProcessRecord` → 各 rmgr 解析函数 → `reorderbuffer.c:788` 的 `ReorderBufferQueueChange` 路径，以及 `src/backend/replication/walsender.c` 的 `XLogSendLogical` 循环）：

1. 顺序读取 WAL；
2. 解析 `XLOG_RUNNING_XACTS` 等记录，构建并推进一致性快照（snapbuild）；
3. 按 WAL 顺序重组事务（reorder buffer）；
4. 将每条 WAL 解析为 tuple 变更（CPU 密集）；
5. 按事务提交顺序经流复制协议发送。

### 2.2 问题

- **单核瓶颈**：第 4 步是全流程中 CPU 消耗最高的部分，且与其他步骤耦合在同一进程中，无法利用多核 CPU。
- **订阅端已并行，发布端未跟进**：PG16+ 订阅端可并行 apply，发布端的串行解码无法跟上，形成不对称瓶颈，订阅端的并行能力得不到兑现。
- **解码与发送耦合**：解码结果与网络发送在同一进程串行进行，发送的抖动会直接拖慢解码。
- **输出插件调用时机受限**：插件回调发生在变更从 reorder buffer **回放**时（`reorderbuffer.c:2109` 提交路径、`reorderbuffer.c:4054` 流式路径），不在读 WAL 时。只把“WAL → tuple”搬走并不能并行化真正的热点（每列的 syscache 查类型与类型输出函数调用）。

### 2.3 可并行化的部分

把发布端流程拆成两类：

| 类别 | 内容 | 特性 |
| --- | --- | --- |
| 控制信息 | `standby`（RUNNING_XACTS）、`xlog`、`xact`（COMMIT/ABORT/PREPARE/ASSIGNMENT/INVALIDATIONS）、`heap2` 的 `XLOG_HEAP2_NEW_CID` | 数量少、需全局顺序、必须由单一进程维护 |
| 数据信息 | `heap`（INSERT/UPDATE/DELETE/TRUNCATE/CONFIRM）、`heap2` 的 `XLOG_HEAP2_MULTI_INSERT`、事务型 `message` | 数量大、CPU 密集、可按事务独立并行 |

数据信息占解码开销的绝大部分，且天然可按顶层事务切分——这是本特性的并行切分依据。

### 2.4 目标架构

发布端拆成三个进程（详见 `design.md` 第 3 章）：

| 进程 | 职责 |
| --- | --- |
| **walsender（主进程 / 发送进程）** | 启动并监控 walreader 与 decode worker；持有复制槽与 CopyBoth 连接；负责全部网络收发；按提交序号输出 commit/abort/cancel；推进 `confirmed_flush`；代 walreader 执行复制槽的 xmin 推进 |
| **walreader** | 唯一的 WAL 读取者；驱动 snapbuild 前进至 consistent；按顶层事务把数据类 WAL 路由到对应 worker；解析控制类 WAL；为每个事务结束事件分配提交序号并上报 walsender；向 worker 分发快照与目录失效通知 |
| **decode worker** | 从共享内存队列取 WAL 批次与事务上下文；**同时承载多个在途事务**，每个事务的全部 WAL 记录由本 worker 按 WAL 顺序处理；复用现有解析函数完成解码；在本地 reorder buffer 中累积变更并切分流式分片；通过 output plugin 生成协议字节写入共享内存；解码完成即通知 walsender 可以发送 |

三者通过共享内存队列通信。**改动只落在发布端，订阅端机制与代码零改动。**

## 3. 目标

| 编号 | 目标 | 度量方式 |
| --- | --- | --- |
| G1 | 提升发布端解码吞吐 | tpcc 100 万 tpm 场景下，逻辑复制 WAL 同步速率达到 **70MB/s**（以 `confirm_flush_lsn` 速率为准） |
| G2 | 解码结果与原串行实现等价 | 开启/关闭并行解码，同一套负载产生的逻辑变更内容与顺序完全一致 |
| G3 | 保证提交顺序 | 订阅端收到的 commit/abort/cancel 顺序与 WAL 中的提交顺序完全一致；事务数据本身允许乱序（与现有串行语义一致） |
| G4 | 事务完整 | 子事务与顶层事务合并为一个完整事务，不拆分、不丢变更 |
| G5 | 流式尽早发送 | 大事务边解码边发送，不等整个事务解码完成才开始传输 |
| G6 | 特性可控、可回退 | 通过 GUC 控制开关；关闭时行为与现有串行路径完全一致 |
| G7 | 不破坏既有部署 | 现有订阅端、现有发布端配置无需修改即可继续工作 |

## 4. 范围

### 4.1 范围内

- 发布端拆分为 walreader、decode worker、walsender 三进程，walsender 作为主进程；
- walreader 与 worker 之间、worker 与 walsender 之间的共享内存通信与调度；
- 按顶层事务分配 worker、维护事务→worker 绑定（一个事务只交给一个 worker，一个 worker 可承载多个事务）；
- 提交序号（cseq）的分配与基于 cseq 的 commit/abort/cancel 顺序输出；
- 一致性快照在 worker 间的分发，以及目录失效消息随 WAL 变化及时送达 worker；
- 并行模式下的错误传播、worker 异常退出处理；
- 相关 GUC、监控指标与文档。

### 4.2 范围外

- **订阅端**：并行 apply 已由 PG16+ 的流复制协议提供，本特性不改动订阅端任何代码与机制。
- **非流复制协议场景**：未开启 streaming 的订阅方，其事务仍走原有串行解码路径。发布端可以同时支持两类订阅方，但只有流复制订阅方能受益于并行解码。
- **物理复制**、逻辑复制插件（output plugin）自身的接口与行为。
- **快照构建（snapbuild）本身的并行化**：快照仍由 walreader 独占维护，worker 只持有快照副本与事件流。
- **WAL 读取的并行化**：WAL 只能有一个读取者（walreader），不引入 WAL 读取的多进程拆分。

## 5. 功能需求

| 编号 | 需求 |
| --- | --- |
| FR-1 | walsender 作为主进程，负责启动、监控与重启 walreader 和 decode worker，持有复制槽与订阅端 CopyBoth 连接，负责全部网络收发与 `confirmed_flush` 的推进。 |
| FR-2 | walreader 是唯一的 WAL 读取者，负责驱动 snapbuild 前进至 consistent 状态，并解析全部控制类 WAL（`xact`、`standby`、`xlog`、`XLOG_HEAP2_NEW_CID`）。 |
| FR-3 | walreader 按顶层事务 ID 把数据类 WAL 路由到绑定的 decode worker，并维护事务→worker 绑定表与各 worker 的在途事务数；新事务分配给在途事务最少的 worker。 |
| FR-4 | **事务与 worker 的绑定是多对一的**：一个顶层事务的全部 WAL 必须由同一个 worker 处理，事务内 WAL 顺序不得改变；一个 worker 可以同时承载多个并发事务。绑定在事务的第一条 WAL 记录到达时建立，直到事务 COMMIT/ABORT/CANCEL 才解除，期间不得迁移到其他 worker。 |
| FR-5 | walreader 为每个事务结束事件（commit/abort/cancel）按 WAL 提交顺序分配提交序号 cseq，并上报 walsender。 |
| FR-6 | decode worker 复用现有 `heap_decode` / `heap2_decode` / `logicalmsg_decode` 完成解码，经 output plugin 生成协议字节写入共享内存，完成后通知 walsender 可以发送。 |
| FR-7 | walsender 按 cseq 顺序输出 commit/abort/cancel；事务数据允许乱序发送，与现有串行实现的语义一致（现有实现中事务数据本就不保证按提交顺序输出）。 |
| FR-8 | 每个 worker 在解码事务前获得该事务对应的基础快照；解码过程中遇到目录失效、命令 ID 等事件时，walreader 须按 WAL 顺序及时通知 worker；无 xid 的失效消息由 walreader 本地执行。 |
| FR-9 | 事务在 worker 内部累积到阈值即切分流式分片，边解码边发送，不等待整个事务解码完成。 |
| FR-10 | `XLOG_RUNNING_XACTS` 清理的游离事务（无 COMMIT/ABORT）须通知对应 worker 取消该事务的解码并回收资源（对应 `PMSG_TXN_FINISH` 的结束类型 `cancel`）。 |
| FR-11 | worker 内部报错须上报 walsender（对应 `PMSG_ERROR`），由 walsender 按现有逻辑复制错误语义终止本次复制会话；walsender 不得因单个 worker 故障而静默丢弃变更。 |
| FR-12 | 复制槽的 xmin 推进由 walsender 代 walreader 执行，保证复制槽状态由单一进程维护。 |
| FR-13 | 提供 GUC 控制并行解码的启停与 worker 数量，并提供相关监控项（至少含 worker 数量、事务平均解码耗时、队列积压深度、cseq 等待深度）。 |

## 6. 非功能需求

| 编号 | 需求 |
| --- | --- |
| NFR-1 | **性能**：达到 G1 的 70MB/s 目标；并行度提升应带来可观测的吞吐提升，单核场景下不显著劣化。 |
| NFR-2 | **兼容性**：订阅端零改动；不新增 output plugin 的必选接口；现有插件（pgoutput 等）在并行模式下行为不变。 |
| NFR-3 | **资源占用**：worker 为常驻进程，同时承载多个在途事务，内存占用以单个 worker 上全部在途事务解码所需为上限；空闲时不应常驻占用 `max_worker_processes` 名额之外的资源。 |
| NFR-4 | **稳定性**：并行路径需覆盖 crash recovery、worker 异常退出、事务回滚、catalog 变更等既有回归用例，不引入新的 crash 或数据不一致。 |
| NFR-5 | **可维护性**：并行解码逻辑与既有串行解码逻辑共用同一套解析函数（`heap_decode`、`heap2_decode` 等），差异集中在“谁来调用”而非“怎么解析”。 |
| NFR-6 | **调度开销**：进程间通信以共享内存队列为主，调度本身不得成为新的性能瓶颈；串行发送路径上的内存拷贝应保持最少。 |

## 7. 约束与假设

1. **只改发布端**：改动限定在发布端，订阅端机制与代码不改动；并行走的是既有流复制协议。
2. **WAL 单读者**：WAL 读取、snapbuild 推进、提交序号分配集中于 walreader。
3. **插件状态单进程闭环**：output plugin 的私有状态（`txn->output_plugin_private`）不可跨进程共享，begin → changes → commit 必须在同一个 worker 内走完。
4. **快照一致性是前提**：解码必须基于一致性快照读取系统表元数据。快照由 walreader 独占维护，worker 持有快照副本并接收事件流。
5. **顺序保证只针对事务结束事件**：commit/abort/cancel 的顺序必须严格保证；事务数据本身的乱序容忍度与现有串行实现保持一致。
6. **子事务归属先于并行**：WAL 中出现 `XACT_ASSIGNMENT` 时，子事务已归属于某个顶层事务；数据类 WAL 到达时其顶层事务 ID 已可确定。
7. **流复制协议为并行前提**：并行解码依赖 streaming 协议分片发送，非流复制订阅方保持原路径。
8. **快照副本可序列化**：worker 所需的快照状态必须能在进程间传递，必要时复用 `ReorderBufferCopySnap` 的子事务集拷贝机制。
9. **事务与 worker 多对一绑定**：一个顶层事务只交给一个 worker，期间不迁移；一个 worker 可同时承载多个事务。事务内顺序由“绑定 + 顺序追加 + 单 worker 单线程消费”保证（FR-4）。
10. **并发只发生在事务之间**：事务内的解码与插件调用始终单线程顺序执行；这是把切分点选在事务级的前提。

## 8. 验收标准

| 编号 | 验收项 | 通过条件 |
| --- | --- | --- |
| AC-1 | 吞吐 | tpcc 100 万 tpm 场景下 `confirm_flush_lsn` 速率 ≥ 70MB/s。 |
| AC-2 | 功能等价 | 同一负载在并行开关 ON/OFF 两种状态下，订阅端收到的逻辑变更内容与顺序完全一致；其中**每个事务内部的变更顺序必须与 WAL 顺序逐条一致**（FR-4）。 |
| AC-3 | 提交顺序 | 大并发 + 频繁提交/回滚 + 系统表变更（DDL）混合负载下，commit/abort/cancel 的输出顺序与 WAL 提交顺序逐条一致，无乱序、无事务拆分、无丢变更。 |
| AC-4 | 流式及时性 | 大事务在解码未完成时即可向订阅端输出分片，订阅端提前收到首片数据。 |
| AC-5 | 快照正确性 | 并发 DDL 场景下，worker 读到的元数据与串行实现一致，无“元数据已失效却仍被使用”导致的变更内容错误。 |
| AC-6 | 异常处理 | 人为制造 worker 内部错误、worker 被 kill，walsender 能正确上报错误并终止会话，不产生脏数据。 |
| AC-7 | 回归 | 现有逻辑复制回归用例（`logical_replication`、`publication`、DDL 复制等）全部通过。 |
| AC-8 | 可回退 | 关闭开关后行为与当前主线实现完全一致，性能无显著劣化。 |
| AC-9 | 订阅端零改动 | 特性开启期间订阅端不需要升级、不需要改配置，行为与升级前一致。 |

## 9. 风险

| 风险 | 影响 | 应对 |
| --- | --- | --- |
| 系统表变更导致快照失效，worker 读到错误元数据 | 变更内容错误 | 快照与目录失效信息随 WAL 变化及时通知 worker（FR-8），并以 AC-5 重点验证。 |
| 并行完成顺序与提交顺序不一致 | 订阅端 apply 乱序 | 提交顺序由 walreader 分配 cseq、walsender 单点按序输出（FR-5、FR-7）。 |
| worker 崩溃导致变更丢失 | 数据不一致 | worker 错误与退出必须上报，walsender 终止会话而非静默继续（FR-11）。 |
| 事务在 worker 之间迁移导致事务内乱序 | 变更顺序错误、数据不一致 | 绑定建立后禁止迁移；事务内顺序由“绑定 + 顺序追加 + 单 worker 单线程消费”保证（FR-4、约束 9） |
| 共享内存队列成为新瓶颈 | 吞吐不升反降 | 队列深度与吞吐纳入监控（NFR-1、NFR-6），必要时按实测调整队列与批量策略。 |
| 进程间通信引入额外延迟 | 小事务场景劣化 | 需实测小事务延迟，与串行路径做对比（AC-8）。 |
| 插件状态按 worker 复制，RELATION 消息重复 | 冗余带宽 | 首版容忍该冗余（重复 RELATION 语义正确），把共享关系表列为后续优化。 |
| 复制槽 xmin 推进跨进程归属不清 | 槽保留 xmin 漂移或过旧 | 由 walsender 代 walreader 执行槽相关操作（FR-12）。 |

## 10. 参考

- `design.md`：概要设计说明书（三进程模型、消息协议、顺序保证方案、快照与失效分发）。
- PostgreSQL 源码：`src/backend/replication/logical/logical.c`、`src/backend/replication/logical/decode.c`、`src/backend/replication/logical/reorderbuffer.c`、`src/backend/replication/logical/snapbuild.c`、`src/backend/replication/logical/worker.c`、`src/backend/replication/walsender.c`。
