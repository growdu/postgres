# 用 commit_sync 屏障替换 cseq 严格放行（发布端）

状态：已评审，待实现。本文档的 §4 安全决定与 §5 待定项均已由 growdu 于 2026-11-22 确认落地。
边界：只动发布端（walreader / decode worker / walsender）。订阅端一行不改，wire 格式不变。

## 1. 意图与动机

design2.md 描述了 `commit_sync` 屏障机制：不相交事务可以乱序下发，提交/中止按相位保序。
当前实现用的是 `cseq` 严格放行——事务 k 的 commit 必须等事务 k-1 的 commit 放行后才放行，
是一种更保守的简化。本次改动把当前 `cseq + frontier` 的严格序换成 design2 的 `commit_sync`
相位屏障，让发布端能真正乱序下发，回到设计文档描述的形态。

不改吞吐目标，不改订阅端，不改 wire 协议。这是发布端内部放行机制的替换。

## 2. 当前机制（两道闸门）

`cseq` 由 walreader 按 WAL 提交序分配（`parallel_walreader.c:874` `pd_next_cseq++`），既给
worker 的帧打标，又经 `PD_ENT_CSEQ` 控制消息发给 walsender。walsender 侧两道闸门共同保证顺序：

1. **cseq 闸门**（`parallel_decode_release_txns`，`parallel_decode.c:1052`）：只放行
   `parallel_decode_next_cseq`，且要 `acked`。严格 WAL 提交序。
2. **frontier 闸门**（`parallel_decode_frame_may_go`，`parallel_decode.c:964`）：无 cseq 的流式
   分片只有在 `frontier[0]->end_lsn >= block->end_lsn` 时才放行，即不得越过最老那个已结束未放行
   事务。这道闸门是当前真正阻止"t2 的段在 t1 commit 之前发出"的东西。

`:1095` 的 WARNING 检测的就是这道闸门被打破后的场景（"sent a stream segment before the commit
of an older transaction"），其提示是订阅端会并发 apply 发布端行锁串行化过的两个事务，报出
发布端没有的 deadlock。

## 3. 目标机制（commit_sync 相位屏障）

- 每个事务结束事件（commit / abort / cancel / prepare）产生一个 `commit_sync_k` 标记，
  `k` 是该事务的相位号（沿用 cseq 的分配，WAL 提交序；或改为结束事件到达序——见 §5 待定项）。
- worker 把事务数据产出完毕后，发出 `commit_sync_k`（替代/补充当前的 `PD_EVENT_END_ACK`）。
- walsender 维护"已 settle"事务集合（数据收齐 + 见到 commit_sync_k）。
- **数据帧（流式分片，无 cseq）一旦其 worker 产出即放行下发，不再被更早事务的 commit 挡住**
  ——这就是"乱序"的来源。
- commit/abort 标记按相位保序下发。

## 4. 关键安全决定（本次评审的核心）

去掉 cseq 严格序的同时，frontier 闸门是保留、放宽还是删除，直接决定安全性：

- **保留 frontier**：数据仍不越过已结束事务 → 行为与现状等价，没有真正的乱序（达不到目标）。
- **删除 frontier**：数据可在已结束事务之前发 → 实现 design2 的乱序，但 `:1095` 的场景变成常态。

**本次决定（已由 growdu 于 2026-11-22 确认）：删除 frontier 闸门，数据按"所在事务已 settle"
放行，commit/abort 由 commit_sync 相位保序。** 理由：design2 明确允许 t2 delete 在 t1 commit
之前发（`:85`），并声明不相交时安全（`:100-101`、`:103`）。

**确认的安全模型（本次评审锁定）：** 发布端不再计算行锁交集，安全性完全依赖订阅端单
stream 串行 apply（`worker.c` 单值 `stream_xid`，leader 等 PA worker 做完才读下一帧）保证
t1/t2 不相交时不会并发 apply。**若该前提在某些路径（如 leader 未等待、prepared 事务）不成立，
删除 frontier 会让订阅端报出发布端没有的 deadlock。** 这是本次改动唯一可能引入正确性回归的
点，smoke 必须覆盖"相交事务乱序下发 + 订阅端不报 deadlock"。

## 5. 待定项的决策（已由 growdu 于 2026-11-22 确认）

- 相位号 k 的来源：**沿用 cseq（WAL 提交序）作为相位号**，最小改动，与 commit_sync 的相位
  语义一致。
- `commit_sync_k` 与当前 `PD_EVENT_END_ACK` 的关系：**不新增独立事件类型，改为在帧上打标记**。
  实现细化（2026-11-22 写实现计划时定）：commit_sync 由「worker 给 commit/abort/prepare
  标记帧打 `PD_FRAME_MARKER` 标志 + walsender 用相位屏障下发该标记帧」共同实现，而不是新增
  `PD_ENT_COMMIT_SYNC` 事件。理由：`PD_EVENT_END_ACK` 已承担「数据 settle」通知，相位屏障所需的
  「这是 commit/abort 标记帧」信息直接由帧标志携带即可，避免一条与 END_ACK 冗余的信号。相位号
  仍沿用 cseq。原 §5 的「新增事件类型」调用以此细化取代。
- frontier 删除后，`parallel_decode_sent_stream_max` 的 WARNING：**删除**，因为乱序下发现在是
  预期行为，不再是异常。
- 监控视图（`pg_parallel_decode_commit_lag` 的 `cseq_allocated`/`cseq_released`）：**字段名保留，
  语义改为相位号**，文档同步。

## 6. 不改的东西

- 订阅端源码、wire 协议（一条变更一个 CopyData）、帧字节与顺序（除"可跨事务乱序"外）。
- RELATION 去重、块化、接管缓冲、二叉堆 held 区。
- 快照广播、catalog_xmin/restart_lsn 推进（与本次无关）。

## 7. 验证

- `doc/pa_decode/t/` 全部冒烟通过，重点新增/加强：相交事务乱序下发后订阅端无 deadlock、
  帧字节与顺序在"单事务内"仍严格一致、commit/abort 顺序正确。
- 本机只做功能与正确性验证，不跑吞吐测试。
