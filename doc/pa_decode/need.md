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
- **输出插件调用时机受限**：插件回调发生在变更从 reorder buffer **回放**时（`reorderbuffer.c:2109` 提交路径、`reorderbuffer.c:4055` 流式路径），不在读 WAL 时。只把“WAL → tuple”搬走并不能并行化真正的热点（每列的 syscache 查类型与类型输出函数调用）。

### 2.3 可并行化的部分

把发布端流程拆成两类：

| 类别 | 内容 | 特性 |
| --- | --- | --- |
| walreader 独占处理 | `standby`（RUNNING_XACTS）、`xlog`、`xact` 的 COMMIT/ABORT/PREPARE | 数量少、需全局顺序、必须由单一进程维护 |
| 门控后转发给 worker | `xact` 的 ASSIGNMENT / INVALIDATIONS、`heap2` 的 `XLOG_HEAP2_NEW_CID`、全部 `heap` / `heap2` 数据记录、事务型 `message` | 携带**事务内部状态**（子事务归属、tuplecid、命令 ID、失效消息），只有持有该事务的 worker 才能消费 |
| 数据信息 | `heap`（INSERT/UPDATE/DELETE/TRUNCATE/CONFIRM）、`heap2` 的 `XLOG_HEAP2_MULTI_INSERT`、事务型 `message` | 数量大、CPU 密集、可按事务独立并行 |

数据信息占解码开销的绝大部分，且天然可按顶层事务切分——这是本特性的并行切分依据。

**下发给 worker 的是原始 WAL 记录，而不是翻译后的语义事件。** 命令 ID、失效消息、tuplecid 映射都能从记录本身重建；翻译成语义消息就要额外维护三套重复状态，且每一套都要与 WAL 位置严格对齐——这正是并行化最容易出错的地方。

### 2.4 目标架构

发布端拆成三个进程（详见 `design.md` 第 3 章）：

| 进程 | 职责 |
| --- | --- |
| **walsender（主进程 / 发送进程）** | 启动并监控 walreader 与 decode worker；持有复制槽与 CopyBoth 连接；负责全部网络收发；按提交序号输出 commit/abort/cancel；推进 `confirmed_flush`；代 walreader 执行复制槽的 **xmin 与 restart_lsn** 推进 |
| **walreader** | 唯一的 WAL 读取者；驱动 snapbuild 前进至 consistent；按顶层事务把 WAL 记录路由到对应 worker；分配 base snapshot 并通过快照版本池下发；广播无 xid 的目录失效；为每个事务结束事件分配提交序号并上报 walsender；维护事务→worker 绑定表与在途字节水位 |
| **decode worker** | 从共享内存队列取 WAL 批次与事务上下文；**同时承载多个在途事务**，每个事务的全部 WAL 记录由本 worker 按 WAL 顺序处理；复用现有解析函数完成解码；在本地 reorder buffer 中累积变更并切分流式分片；通过 output plugin 生成协议字节写入共享内存；事务结束事件处理完毕后向 walsender 回执 |

三者通过共享内存队列通信。**改动只落在发布端，订阅端机制与代码零改动。**

## 3. 目标

| 编号 | 目标 | 度量方式 |
| --- | --- | --- |
| G1 | 提升发布端解码吞吐 | tpcc 100 万 tpm 场景下，逻辑复制 WAL 同步速率达到 **70MB/s**（以**发布端实际发出的协议字节速率**为准；不能用 `confirm_flush_lsn` 速率——并行模型下事务数据可乱序下发，该指标会虚高，理由见 `design.md` 7.4） |
| G2 | 解码结果与原串行实现**语义等价** | 开启/关闭并行解码，同一套负载下：每个事务内部的变更内容与顺序与 WAL 顺序**逐条一致**；事务结束事件（commit/abort/cancel）的顺序一致；订阅端最终 apply 的结果一致。**允许**的差异：RELATION 消息冗余（每 worker 一份关系缓存）、事务之间的数据下发顺序不同、流式分片的切点不同 |
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
- 提交序号（cseq）的分配与基于 cseq 的 commit/abort/cancel 顺序输出，**barrier 靠 worker 的结束事件确认推进（含空事件）**；
- **wire 上的分帧顺序保证**：同一时刻只允许一个打开的流段，同一事务的字节整块连续（FR-18）；
- **复制槽两个水位（xmin 与 `restart_lsn`）的跨进程聚合**（FR-12）；
- **复制槽 `confirmed_flush` 的安全约束**：非流式数据随 cseq 放行，流式分片的帧 LSN 按放行前沿钳制（FR-19、FR-20）；
- 一致性快照在 worker 间的分发（base snapshot 由 walreader 分配并下发），以及目录失效消息随 WAL 变化及时送达 worker；
- 并行模式下的错误传播、worker 异常退出处理；
- 故障恢复：worker / walreader / walsender 崩溃与数据库重启后的会话重建；
- 相关 GUC、监控指标与文档。

### 4.2 范围外

- **订阅端**：并行 apply 已由 PG16+ 的流复制协议提供，本特性不改动订阅端任何代码与机制。
- **物理复制**、逻辑复制插件（output plugin）自身的接口与行为。
- **快照构建（snapbuild）本身的并行化**：快照仍由 walreader 独占维护，worker 只持有 base snapshot 的副本，并从原始 WAL 记录自行重建命令 ID、失效消息与 tuplecid。
- **WAL 读取的并行化**：WAL 只能有一个读取者（walreader），不引入 WAL 读取的多进程拆分。

### 4.3 非流式订阅方（退化路径）

未开启 streaming 的订阅方**仍走并行路径**，但输出形态退化为"整事务缓冲"：worker 不切分分片，整个事务的协议字节构成一个完整的块，在该事务的提交序号轮到时整块写出。

与串行路径的差别有两处，都必须显式处理：

- **不再落盘**：现状非流式路径用 `ReorderBufferSerializeTXN()` 写临时文件、提交时回放。但落盘路径在多 worker 下会**互删文件**（所有 worker 共享 `pg_replslot/<slot>/` 目录，`ReorderBufferCleanupSerializedTXNs()` 会删掉该目录下所有 `xid*` 文件，而它在每个 worker 的启动与退出时都会被调用），且依赖进程全局的 `MyReplicationSlot`（worker 进程里为 NULL）。因此并行模式下禁用落盘，改为**内存整事务缓冲**，代价是内存占用与事务大小成正比。
- **待提交区必须与结果区分离**：整事务字节要整块挂在待提交区等自己的提交序号，若待提交区与 worker 的结果区共享容量，会形成死锁（worker 想写自己的事务字节但空间被挂起区占满；挂起区等提交序号，提交序号又等那个 worker 写出字节）。因此待提交区与结果区分离，或让提交序号队头事务的字节直通 socket。

只有流式订阅方能真正受益于并行解码；非流式订阅方的收益仅限"解码算力并行"，而其 wire 占用时间反而变长。

## 5. 功能需求

| 编号 | 需求 |
| --- | --- |
| FR-1 | walsender 作为主进程，负责启动、监控 walreader 和 decode worker，持有复制槽与订阅端 CopyBoth 连接，负责全部网络收发与 `confirmed_flush` 的推进。**子进程异常一律终止本次复制会话并按第 17 章重建会话，不做进程级重启**——worker 注册为 `bgw_restart_time = BGW_NEVER_RESTART` 的后台进程，自动重启会让同一事务被拆到两个 worker。 |
| FR-2 | walreader 是唯一的 WAL 读取者，负责驱动 snapbuild 前进至 consistent 状态，并解析全部控制类 WAL（`xact`、`standby`、`xlog`）。 |
| FR-3 | walreader 按顶层事务 ID 把数据类 WAL 路由到绑定的 decode worker，并维护事务→worker 绑定表与各 worker 的**在途字节数**；新事务分配给在途字节最少的 worker（按事务数分配会被大事务误导，见 `design.md` 4.5.1）。 |
| FR-4 | **事务与 worker 的绑定是多对一的**：一个顶层事务的全部 WAL 必须由同一个 worker 处理，事务内 WAL 顺序不得改变；一个 worker 可以同时承载多个并发事务。绑定在事务的第一条 WAL 记录到达时建立，直到事务 COMMIT/ABORT/CANCEL 才解除，期间不得迁移到其他 worker。**例外：prepared 事务在 PREPARE 之后仍保持绑定与事务上下文（含 gid），直到 COMMIT PREPARED / ROLLBACK PREPARED**。 |
| FR-5 | walreader 为每个事务结束事件（commit/abort/cancel/prepare）按 WAL 提交顺序分配提交序号 cseq，并上报 walsender。 |
| FR-6 | decode worker 复用现有 `heap_decode` / `heap2_decode` / `logicalmsg_decode` 完成解码，经 output plugin 生成协议字节写入共享内存，完成后通知 walsender 可以发送。 |
| FR-7 | walsender 按 cseq 顺序输出 commit/abort/cancel；**提交序号的推进依据是"结束事件被 worker 确认"而不是"提交消息到达"**——非流式事务的 abort 与游离事务的 cancel 在 wire 上不产生任何消息，若只在收到消息时推进，水位会永久空洞。事务数据允许乱序发送，与现有串行实现的语义一致（现有实现中事务数据本就不保证按提交顺序输出）。 |
| FR-8 | 每个 worker 在解码事务前获得该事务对应的 **base snapshot**（由 walreader 分配并下发，worker 在回放时刻自行补齐子事务集合）；**无 xid 的失效消息必须广播给所有 worker**——每个 worker 有自己的 catcache/syscache/relcache，walreader 本地执行只能清掉自己的缓存，worker 会继续使用已失效的元数据。命令 ID、失效消息与 tuplecid 映射由 worker 从原始 WAL 记录自行重建。 |
| FR-9 | 事务在 worker 内部累积到阈值即切分流式分片，边解码边发送，不等待整个事务解码完成。**单个流段必须有字节上限**——wire 上同一时刻只允许一个打开的流段，没有上限时段的独占时间无上界，并行解码的产出会被堵死。 |
| FR-10 | `XLOG_RUNNING_XACTS` 清理的游离事务（无 COMMIT/ABORT）须通知对应 worker 取消该事务的解码并回收资源（对应 `PD_ENT_TXN_END` 的结束类型 `cancel`）。 |
| FR-11 | worker 内部报错须上报 walsender（对应 `PD_ENT_ERROR`），由 walsender 按现有逻辑复制错误语义终止本次复制会话；walsender 不得因单个 worker 故障而静默丢弃变更。 |
| FR-12 | 复制槽的水位推进由 walsender 代 walreader 执行，保证复制槽状态由单一进程维护。**两个水位都要跨进程聚合**：`xmin`（各 worker 上报的最小快照 xmin）与 `restart_lsn`（全局最老在途事务的 `restart_decoding_lsn`）——后者若不聚合，槽会周期性地跳过仍在 worker 中在途的事务，发布端重启后丢变更。 |
| FR-13 | 提供 GUC 控制并行解码的启停与 worker 数量，并提供相关监控项（至少含 worker 数量、在途字节数、事务平均解码耗时、队列积压深度、cseq 等待深度、**最长挂起提交年龄**、walsender 输出帧率与 socket 写次数、跨进程唤醒速率）。 |
| FR-14 | **worker 处理到一半崩溃时，walsender 必须终止本次复制会话，不得静默丢弃或续跑该 worker 上的在途事务**；恢复只能以整个会话重建的方式进行，从复制槽的 `restart_lsn` 重新解码。 |
| FR-15 | worker 死亡必须被**确定且及时**地检测到，不得让复制静默停滞：共享内存记录 worker 状态、worker 退出时通知 walsender、barrier 等待带超时，三者至少其一生效即可判定会话失效。 |
| FR-16 | 提交序号 cseq 与共享内存状态**不持久化**，与 walreader 同生命周期；walreader 死亡即作废本会话的全部 cseq，walsender 必须立即失败而非等待超时。 |
| FR-17 | 数据库重启后，复制槽由既有机制恢复，新会话按既有流程从 `restart_lsn` 重新读 WAL 并重建快照；并行化不得改变该恢复语义。 |
| FR-18 | **wire 上的分帧顺序必须保证**：同一时刻只允许一个打开的流段（`STREAM_START` … `STREAM_STOP` 必须成对且不交错），非流式事务的 `BEGIN` … `COMMIT` 字节必须整块连续。订阅端用进程级单值跟踪当前流，第二个 `STREAM START` 会触发 `ereport(ERROR, "duplicate STREAM START message")`；流段打开期间插入其它事务的字节则会被静默归错事务，造成数据损坏。 |
| FR-19 | **复制槽的 `confirmed_flush` 语义必须守住**：流复制协议每条逻辑消息都带一个 `'w'` 帧头，其 LSN 被订阅端用来推进 `last_received` 并回报 flush LSN。订阅端用**取最大值**跟踪它，发布端把该值**无条件**写进 `confirmed_flush`；而重启时 `start_decoding_at = confirmed_flush`，**提交记录 LSN 小于它的事务会被整体跳过**。因此安全条件是：**任何提交记录 LSN 小于 `confirmed_flush` 的事务，必须已经完整送达订阅端**（即"已送达帧的 LSN 不得超过下一个待放行事务的提交记录 LSN"）。**注意：这不是"'w' 帧 LSN 不得倒退"**——串行现状本身就允许倒退，订阅端的取最大值跟踪正是为倒退设计的。 |
| FR-20 | **提前流式的分片帧 LSN 必须钳制**：流式分片可以早于其提交消息发出，其中的变更帧 LSN 可能大于某个"已提交但因 barrier 尚未放行"的事务的提交记录 LSN，把 `confirmed_flush` 抬到那个提交记录之上——重启后该事务被整体跳过，**静默丢数据**。因此 walsender 必须按**放行前沿**（= cseq = 放行水位 + 1 的那个事务的提交记录 LSN）钳制分片帧的 LSN：`min(帧内 LSN, 放行前沿)`。非流式事务的数据随其 cseq 一起放行，天然满足 FR-19，无需额外钳制。 |

## 6. 非功能需求

| 编号 | 需求 |
| --- | --- |
| NFR-1 | **性能**：达到 G1 的 70MB/s 目标；并行度提升应带来可观测的吞吐提升，单核场景下不显著劣化。**达成条件**（"与"关系）：订阅端 `streaming=parallel` + 多个 parallel apply worker；发布端流段有字节上限；walsender 批量刷出；发布端开启并行解码。**注意解码侧本身不是瓶颈**——按源码成本结构，单核算力只差 1.5–2 倍，N = 3~4 个 worker 即够（`design.md` 15.1）。 |
| NFR-2 | **兼容性**：订阅端零改动；不新增 output plugin 的必选接口；现有插件（pgoutput 等）在并行模式下行为不变。 |
| NFR-3 | **资源占用**：worker 为常驻进程，同时承载多个在途事务，内存占用以**单个 worker** 上全部在途事务解码所需为上限（`parallel_decode_work_mem` 是每 worker 的上限，总量 = worker 数 × 该值；首版不做跨进程总量配额）。**prepared 事务的驻留时间无上界**（PREPARE 之后直到 COMMIT PREPARED / ROLLBACK PREPARED），必须单列计入预算。空闲时不应常驻占用 `max_worker_processes` 名额之外的资源。 |
| NFR-4 | **稳定性**：并行路径需覆盖 crash recovery、worker 异常退出、事务回滚、catalog 变更等既有回归用例，不引入新的 crash 或数据不一致。 |
| NFR-5 | **可维护性**：并行解码逻辑与既有串行解码逻辑共用同一套解析函数（`heap_decode`、`heap2_decode` 等），差异集中在“谁来调用”而非“怎么解析”。 |
| NFR-6 | **调度开销**：进程间通信以共享内存队列为主，调度本身不得成为新的性能瓶颈；串行发送路径上的内存拷贝应保持最少。 |

## 7. 约束与假设

1. **只改发布端**：改动限定在发布端，订阅端机制与代码不改动；并行走的是既有流复制协议。
2. **WAL 单读者**：WAL 读取、snapbuild 推进、提交序号分配集中于 walreader。
3. **插件状态单进程闭环**：output plugin 的私有状态（`txn->output_plugin_private`）不可跨进程共享，begin → changes → commit 必须在同一个 worker 内走完。
4. **快照一致性是前提**：解码必须基于一致性快照读取系统表元数据。快照由 walreader 独占维护，worker 持有快照副本并接收事件流。
5. **顺序保证有三层**：**事务内序**（同一事务的变更按 WAL 顺序、且不拆到多个 worker）；**分帧顺序**——wire 上同一时刻只允许一个打开的流段，同一事务的字节必须整块连续（FR-18），这是订阅端的状态机要求，不是本特性引入的；**结束事件序**（commit/abort/cancel 的顺序必须严格保证）。事务数据本身的乱序容忍度与现有串行实现保持一致。
6. **子事务归属先于并行**：WAL 中出现 `XACT_ASSIGNMENT` 时，子事务已归属于某个顶层事务；数据类 WAL 到达时其顶层事务 ID 已可确定。
7. **流复制协议为并行前提**：并行解码依赖 streaming 协议分片发送。非流复制订阅方走"整事务缓冲"退化路径（见 4.3），收益有限。
8. **快照的传递方向**：walreader 下发的是 **base snapshot 的内容**（`xmin`/`xmax`/`xip`/`curcid`）；**含子事务集合的最终快照由 worker 自行生成**——`ReorderBufferCopySnap()` 需要事务的子事务链表，那只有持有事务的 worker 才有，且子事务在事务的第一条记录到达时还不知道。它是 `static` 函数，未导出到头文件，因此 walreader 想调也调不到。
9. **事务与 worker 多对一绑定**：一个顶层事务只交给一个 worker，期间不迁移；一个 worker 可同时承载多个事务。事务内顺序由“绑定 + 顺序追加 + 单 worker 单线程消费”保证（FR-4）。
10. **并发只发生在事务之间**：事务内的解码与插件调用始终单线程顺序执行；这是把切分点选在事务级的前提。worker 承载多个事务时，各事务的解码**不得交错**——历史快照与 tuplecid 映射是进程级单例，交错会导致旧版本可见性判定出错。
11. **恢复以会话为单位**：复制槽是唯一的持久化恢复锚点，进程内状态（reorder buffer、插件私有状态、快照副本、cseq、共享内存）全部不参与恢复；worker 的重建只能以整个会话重建的形式进行。
12. **worker 不得自动重启**：worker 注册为 `bgw_restart_time = BGW_NEVER_RESTART` 的后台进程，且退出码只能是 0 或 1——其他退出码或死于信号会触发集群级 crash recovery。
13. **GUC 的变更只在下次会话生效**：`max_parallel_decode_workers` 决定共享内存的队列条数与绑定表规模，正在解码的事务不能迁移；`parallel_decode_work_mem` 由 `ReorderBufferCheckMemoryLimit()` 从**本进程** GUC 读取，worker 必须重启才能看到新值；分段参数中途变更会让同一事务的前后两段用不同参数切分。因此这些 GUC 虽可 `sighup` 级调整，实际生效点是**下一次复制会话**，不得给出"立即生效"的预期。
14. **共享内存有显式上界**：dsa 区域按需增长，若不设上界，一个失控的 worker 可把动态共享内存吃光。必须有 dsa 总容量上限、单队列容量上限（`shm_mq_set_max_size()`）与单次分配上限（`MaxAllocSize`）；单条消息的字节数还必须显著低于 `PQ_LARGE_MESSAGE_LIMIT`（= `MaxAllocSize - 1`），否则必须走大消息分片协议。

## 8. 验收标准

| 编号 | 验收项 | 通过条件 |
| --- | --- | --- |
| AC-1 | 吞吐 | tpcc 100 万 tpm 场景下**发布端实际发出的协议字节速率** ≥ 70MB/s（口径与 G1 一致）。**不能用 `confirm_flush_lsn` 速率**——并行模型下事务数据可乱序下发，该指标会被后发事务抬高而虚高。**达成前提**：订阅端 `streaming=parallel` + 多个 parallel apply worker；发布端有流段字节上限；walsender 按块/按 8KB 缓冲批量刷出。 |
| AC-2 | 语义等价 | 同一负载在并行开关 ON/OFF 两种状态下，**每个事务内部的变更内容与顺序与 WAL 顺序逐条一致**（FR-4）；**事务结束事件的顺序一致**；**订阅端最终 apply 的结果一致**。允许的差异：RELATION 消息冗余（每 worker 一份关系缓存）、事务之间的数据下发顺序不同、流式分片的切点不同（G2）。 |
| AC-3 | 提交顺序 | 大并发 + 频繁提交/回滚 + 系统表变更（DDL）混合负载下，commit/abort/cancel 的输出顺序与 WAL 提交顺序逐条一致，无乱序、无事务拆分、无丢变更。 |
| AC-4 | 流式及时性 | 大事务在解码未完成时即可向订阅端输出分片，订阅端提前收到首片数据。 |
| AC-5 | 快照正确性 | 并发 DDL 场景下，worker 读到的元数据与串行实现一致，无“元数据已失效却仍被使用”导致的变更内容错误。 |
| AC-6 | 异常处理 | 人为制造 worker 内部错误、worker 被 kill，walsender 能正确上报错误并终止会话，不产生脏数据。 |
| AC-7 | 回归 | 现有逻辑复制回归用例（`logical_replication`、`publication`、DDL 复制等）全部通过。 |
| AC-8 | 可回退 | 关闭开关后行为与当前主线实现完全一致，性能无显著劣化。 |
| AC-9 | 订阅端零改动 | 特性开启期间订阅端不需要升级、不需要改配置，行为与升级前一致。 |
| AC-10 | worker 崩溃恢复 | 在 worker 处理事务中途 kill 掉该进程，walsender 能及时发现并终止会话（不静默停滞、不静默丢数据）；订阅端重连后从复制槽 `restart_lsn` 重放，变更内容与顺序与崩溃前一致。 |
| AC-11 | 数据库重启恢复 | 数据库正常重启与崩溃恢复后，复制槽正确恢复，订阅端重连后能继续同步，无丢变更、无重复的事务内乱序。 |
| AC-12 | **水位安全** | 大事务提前流式下发期间 kill 掉发布端并重启，**已提交但当时尚未放行的事务不得丢失**（FR-19、FR-20）。这是本特性引入的唯一一条新的数据丢失路径，必须专项验证：构造"大事务正在流式下发、其提交消息尚未放行"的时刻，重启后该事务仍能被完整重放。 |

## 9. 风险

| 风险 | 影响 | 应对 |
| --- | --- | --- |
| 系统表变更导致快照失效，worker 读到错误元数据 | 变更内容错误 | base snapshot 由 walreader 分配并下发；无 xid 的失效**广播给所有 worker**（每个 worker 有自己的目录缓存，walreader 本地执行不够）；命令 ID / 失效 / tuplecid 由 worker 从原始 WAL 记录自行重建（FR-8），并以 AC-5 重点验证。 |
| 并行完成顺序与提交顺序不一致 | 订阅端 apply 乱序 | 提交顺序由 walreader 分配 cseq、walsender 单点按序输出（FR-5、FR-7）。 |
| **wire 上出现两个打开的流段** | 订阅端 `ereport(ERROR, "duplicate STREAM START message")`；流段打开期间插入其它事务的字节则被静默归错事务，数据损坏 | walsender 一次只取一整块，块未取完不得取其它 worker 的字节（FR-18）。 |
| **`confirmed_flush` 越过尚未送达的事务** | 发布端重启后该事务被整体跳过 → **静默丢数据**（这不是"'w' 帧 LSN 倒退"的问题——串行现状本身允许倒退，订阅端用取最大值跟踪正是为此设计） | 维持不变式：**提交记录 LSN 小于 `confirmed_flush` 的事务必须已完整送达**。非流式事务数据跟随 cseq 放行（FR-19）；**流式分片的帧 LSN 由 walsender 按放行前沿钳制**（FR-20）。用 kill/restart 实验专项验证。 |
| **`restart_lsn` 越过仍在途的事务** | 发布端重启后，在途事务的前半段变更永久丢失 | walreader 侧维护事务绑定表并汇总全局最老在途事务的 `restart_decoding_lsn`，两个水位都由 walsender 单点推进（FR-12）。 |
| worker 崩溃导致变更丢失 | 数据不一致 | worker 错误与退出必须上报，walsender 终止会话而非静默继续（FR-11）。 |
| 事务在 worker 之间迁移导致事务内乱序 | 变更顺序错误、数据不一致 | 绑定建立后禁止迁移；事务内顺序由“绑定 + 顺序追加 + 单 worker 单线程消费”保证（FR-4、约束 9） |
| **分配准则只看事务数** | 大事务独占的 worker 被判为“最空闲”，有效并行度退化为 1；早提交的大事务把其后所有提交一起卡住 | 分配代价函数改为**在途字节数**，并设单 worker 在途事务数上限（FR-3）。 |
| 共享内存队列成为新瓶颈 | 吞吐不升反降 | 队列深度与吞吐纳入监控（NFR-1、NFR-6），必要时按实测调整队列与批量策略。 |
| **协议帧率 / 系统调用率** | 每条协议消息一次 `write()` 系统调用，30–70 万次/秒量级，串行卡在 walsender | 并行模式改为按块 / 按 8KB 缓冲刷出（帧不能合并）；帧率纳入监控。 |
| **目录缓存 N 倍** | 每个 worker 各自做发布关系判定与 DDL 后的全量重扫，DDL 密集时是 N 倍目录扫描 CPU | 把“关系 → 发布列表 / schema”做成只读共享内存表（walreader 维护、worker 只读）。 |
| 进程间通信引入额外延迟 | 小事务场景劣化 | 需实测小事务延迟，与串行路径做对比（AC-8）；批次按字节阈值而非按事务切。 |
| 插件状态按 worker 复制，RELATION 消息重复 | 冗余带宽 | 首版容忍该冗余（重复 RELATION 语义正确），把共享关系表列为后续优化。 |
| 复制槽水位推进跨进程归属不清 | 槽保留 xmin 漂移或过旧；`restart_lsn` 推进过早 | 由 walsender 代 walreader 执行槽相关操作，两个水位都需跨进程聚合（FR-12）。 |
| worker 崩溃未被检测，复制静默停滞 | 复制链路悄悄挂死 | 三级检测机制：共享内存状态字 + worker 退出通知 + walreader 归因；barrier 等待带超时兜底（FR-15）。 |
| cseq 分配者与消费者不同步 | barrier 永久卡死 | cseq 与 walreader 同生命周期，walreader 死亡即作废全部 cseq 并立即失败（FR-16）。**并且 barrier 靠 worker 的结束事件确认推进（含空事件）**，否则非流式 abort / cancel 会造成永久空洞（FR-7）。 |
| worker 被 postmaster 自动重启 | 同一事务被拆到两个 worker，事务内乱序 | `bgw_restart_time = BGW_NEVER_RESTART`，worker 只随会话重建（约束 11、12、FR-1）。 |

## 10. 参考

- `design.md`：概要设计说明书（三进程模型、消息协议、顺序保证方案、快照与失效分发、故障恢复）。
- PostgreSQL 源码：`src/backend/replication/logical/logical.c`、`src/backend/replication/logical/decode.c`、`src/backend/replication/logical/reorderbuffer.c`、`src/backend/replication/logical/snapbuild.c`、`src/backend/replication/logical/worker.c`、`src/backend/replication/walsender.c`。
