# pg_lrstat — 逻辑复制 LSN 与速率统计扩展 设计文档

| 项 | 内容 |
| --- | --- |
| 文档版本 | v1.0（设计评审稿） |
| 基线代码 | PostgreSQL 18.3 开发分支（本仓库，branch `cluster_rate`） |
| 状态 | 待评审 |

---

## 1. 背景与目标

### 1.1 背景

逻辑复制现状下，要回答"发布端生成了多少 WAL、发出去多少、订阅端收了多少、应用了多少、还差多少、大概多久追平"这些问题，需要手工在 5 个系统视图之间做拼图：

- `pg_stat_replication`（发布端：sent/write/flush/replay_lsn 与三个 lag）
- `pg_replication_slots`（发布端：confirmed_flush_lsn、restart_lsn、wal_status）
- `pg_stat_replication_slots`（发布端解码压力：spill/stream 字节数）
- `pg_stat_subscription`（订阅端：received_lsn、latest_end_lsn、消息时间戳）
- `pg_replication_origin_status`（订阅端应用位点：remote_lsn/local_lsn）

这些视图只提供**瞬时快照**，没有任何一处分母是时间——速率、积压、追平预估（ETA）全部需要外部工具定时采样后自行差分。而逻辑复制的典型运维问题恰恰全是速率问题：迁移窗口还有多久追平、瓶颈在发布端发送还是订阅端应用、大事务回放期间 WAL 会不会被槽拖爆。本扩展把这条链路的观测收敛为"一查即得"：定频采样 + 共享内存历史 + 速率/积压/ETA 计算，并在发布端与订阅端分别落地。

### 1.2 目标

1. 提供一个 C 扩展 `pg_lrstat`，部署在**发布端和订阅端**均可（同一扩展，按本实例上存在的角色自动产出对应视图数据）。
2. 发布端输出：WAL 生成速率、发送速率、对端应用速率（经由反馈）、各段积压（未发送 / 在途 / 对端未应用 / 槽保水 WAL）、解码压力（spill/stream 速率）、时间延迟、ETA。
3. 订阅端输出：接收速率、本地 WAL 生成速率、应用速率、待应用积压（backlog）、ETA。
4. 内置定频采样（后台 worker + 共享内存环形历史），视图直接给出速率与历史，无需外部采样器。
5. 提供端到端同步链路的**阶段分解视图**，一眼看清"卡在哪一段"，并配套基于指标的瓶颈定位与性能分析方法（§6.4 起）。
6. **整体视图（订阅端）**：订阅端凭 `subconninfo` 反向轮询发布端系统视图，把两端检查点、速率、积压合成为一张视图 `pg_lrstat_overall`——**仅部署订阅端即可获得全链路统计**，发布端无需安装任何组件。
7. 纯只读观测：不修改复制行为、不占用复制槽、不写系统表。

### 1.4 远端轮询对发布端的运行时副作用（重要）

虽然发布端**无需安装扩展**，但订阅端的远端轮询（§5.7）会在发布端额外维持**普通 libpq 连接**，**每个订阅一条**。这意味着：

- 发布端 `max_connections` 实际占用 = 原有 + N（N = 该发布端承载的订阅数）。
- 发布端 `pg_stat_activity` 会新增 `application_name='pg_lrstat'` 的连接。
- 发布端 `pg_hba.conf` 与角色权限（建议授予 `pg_read_all_stats`，§5.9）需为订阅连接用户额外放开。

因此"发布端零依赖"指的是**无需安装扩展**，并非"无任何运行时影响"。运维拓扑设计时要把这部分连接计入发布端容量评估。可通过 `pg_lrstat.remote_poll = off`（§5.8）整体关闭远端轮询、仅保留本地视图。

### 1.3 非目标（v1 不做）

- 跨集群数据只做**"订阅端 → 发布端"单向拉取**，且复用订阅自身的连接串（§5.7）；发布端不反向拉取订阅端。多订阅/多发布大型拓扑的集中汇总仍交给监控端按 `slot_name ↔ subscription` join（P3 提供参考实现）。
- 不做表级 / publication 级粒度统计（P3）。
- 不做物理流复制（walreceiver→startup）统计；本扩展只针对逻辑复制链路。
- 不替代告警系统，只提供 `*_stalled` 等可被外部消费的状态位。

---

## 2. 逻辑复制数据链路与观测点

### 2.1 链路总图

```
发布端                                    订阅端
┌─────────────────────────────┐          ┌──────────────────────────────┐
│ WAL 生成(current_lsn)        │          │                              │
│   │                          │          │                              │
│   ▼                          │          │                              │
│ 解码(XLogReader/输出插件)     │  TCP     │  接收(received_lsn)           │
│   │                          ├─────────►│   │  (libpq 消息,不落本地WAL)   │
│   ▼                          │          │   ▼                          │
│ 发送(sent_lsn)               │  反馈     │  应用(origin remote_lsn)      │
│   │                          │◄─────────┤   │  (apply worker 写本地WAL)  │
│   │                          │ write/   │   ▼                          │
│   ▼                          │ flush/   │  反馈发送(send_feedback)       │
│ 确认(confirmed_flush_lsn)     │ apply    │                              │
│ 保水(restart_lsn)            │          │                              │
└─────────────────────────────┘          └──────────────────────────────┘
```

### 2.2 观测点与数据来源（本仓库已核实）

| # | 检查点 | 发布端来源 | 订阅端来源 | 语义 |
| --- | --- | --- | --- | --- |
| C0 | WAL 生成位点 | `pg_current_wal_lsn()` | —（订阅端另有本地 `pg_current_wal_lsn()`，含义不同，见 §3.3） | 已写入 WAL 的最大位置 |
| C1 | 已解码位点 | 不可见（解码与发送在 walsender 单进程内串行耦合） | — | 已完成输出插件序列化的位置 |
| C2 | 已发送位点 | `pg_stat_replication.sent_lsn`（`WalSnd.sentPtr`） | — | 已写入 libpq 发送缓冲并 flush 的位置 |
| C3 | 对端已接收 | `pg_stat_replication.write_lsn`（反馈报文 `recvpos`，见 `src/backend/replication/logical/worker.c` `send_feedback()` 注释 `/* write */`） | `pg_stat_subscription.received_lsn` / `latest_end_lsn`（实时、无反馈延迟） | 订阅端最近收到的流消息对应的 LSN（**非字节计数**） |
| C4 | 对端已提交且本地 WAL 已 flush | `pg_stat_replication.flush_lsn`（反馈报文 `flushpos`，对应 `get_flush_position()` 中已 commit 且 local WAL 已 flush 的事务 `remote_end`） | — | 已 commit 且本地 WAL 已落盘的事务位点；与 C5 的差别见下 |
| C5 | 对端 leader 最近 commit 位点 | `pg_stat_replication.replay_lsn`（反馈报文 `writepos`，对应 `get_flush_position()` 中最近一个 commit 事务的 `remote_end`） | `pg_replication_origin_status.remote_lsn`（实时） | **不是"每条消息都已应用"位点**——它反映最近一次 commit 的 LSN；大事务处理期间会显著领先于真实应用进度。leader 串行回放下近似"已应用位点"；parallel apply 下 `lsn_mapping` 由 leader 写、parallel worker 不参与（`worker.c` 中 `am_parallel_apply_worker()` 跳过 `store_flush_position()`），所以 C5 等于 "leader 已 commit 速率"，不分解到每个 worker |
| C6 | 安全水位（确认） | `pg_replication_slots.confirmed_flush_lsn`（**不是反馈位，是槽内部状态**——walsender 依据反馈 flush 即 C4 推进） | — | 槽安全水位；与 C4 大小关系通常 `C6 ≥ C4`，决定 `restart_lsn` 与 WAL 回收 |
| C7 | 槽保水位 | `pg_replication_slots.restart_lsn` | — | 为本槽保留的最早 WAL 位点；`C0 - C7` 即 retained WAL |
| D1 | 解码压力 | `pg_stat_replication_slots`：`spill_bytes` / `stream_bytes` / `total_txns` / `total_bytes` | — | ReorderBuffer 溢写/流式落盘的字节计数，反映大事务下解码端内存压力 |

### 2.3 三个必须写进设计的关键事实

1. **反馈报文是发布端了解订阅端的唯一通道，且滞后一个反馈周期**（`wal_receiver_status_interval`，默认 10s）。发布端视图中的 C3~C5 天然存在最多一个反馈周期的延迟；订阅端本地视图无此延迟。这是"两端都部署本扩展"的核心价值之一。
2. **逻辑流不写入订阅端本地 WAL**。订阅端 `pg_wal` 中只有 apply worker 回放产生的记录（带 origin），因此订阅端不存在"接收→写 WAL→flush"这一物理复制的中间阶段；`pg_stat_wal_receiver` 视图只覆盖物理 walreceiver，对逻辑订阅为空（已在本仓库核实：`logical/worker.c` 不写 `WalRcvData`）。
3. **时钟边界**。`write_lag/flush_lag/replay_lag` 由内核基于反馈时间戳计算（commit 时间戳差分），可安全跨机使用；本扩展**绝不**用 `now()` 直接跨机比较。同机速率差分使用 `clock_timestamp()` 单调采样序号，对 NTP 小幅回拨不敏感（§5.3）。

---

## 3. 指标体系

### 3.1 指标总表

速率列单位统一为 **MB/s**（内部按字节差分计算，输出时换算，double precision），积压单位为**字节**（bigint，可直接套 `pg_size_pretty()`）。
速率定义：窗口差分 `rate(x) = Δx / Δt`，窗口 `pg_lrstat.rate_window`（默认 2min），算法见 §5.3。

#### 发布端（按逻辑槽，一槽一行）

| 指标 | 公式 | 说明 |
| --- | --- | --- |
| `gen_rate` | ΔC0/Δt | WAL 生成速率（发布库全体写入，非单槽） |
| `send_rate` | ΔC2/Δt | 发送速率 |
| `apply_rate` | ΔC5/Δt | 对端应用速率（经反馈，含反馈周期延迟）。与 `pg_lrstat_sub_rate.apply_rate` 含义相同，**但来源不同步**——发布端从反馈报文获得、滞后约一个 `wal_receiver_status_interval`；订阅端从 origin `remote_lsn` 实时获得。命名统一为 `apply_rate` 以便跨视图 join；数据新鲜度以视图的 `rate_time` 与 `last_remote_poll_time` 为准。 |
| `confirm_rate` | ΔC6/Δt | 确认水位推进速率 |
| `spill_rate` / `stream_rate` | ΔD1/Δt | 解码溢写/流式落盘速率（解码压力） |
| `backlog_unsent` | C0 − C2 | 未发送积压 = 未解码 + 已解码未发送 |
| `backlog_inflight` | C2 − C3 | 已发送、对端尚未确认接收（网络在途 + 反馈滞后；整体视图改用本地 C3′ 消除反馈延迟，见 §3.1 整体表） |
| `backlog_peer_unapplied` | C3 − C5 | 对端已收未应用（≈ 订阅端 backlog，但滞后反馈周期） |
| `backlog_total` | C0 − C5 | 端到端总积压 |
| `retained_wal` | C0 − C7 | 槽保水 WAL（含上述各段 + 已确认但尚未回收的部分） |
| `write_lag/flush_lag/replay_lag` | 透传 `pg_stat_replication` | 时间延迟（interval） |
| `eta_unsent` / `eta_total` | backlog / 对应速率 | 追平预估秒数；速率 ≤ `eta_min_rate` 时为 NULL |
| —（不再提供 `progress_pct`） | — | 原设计 100×C5/C0 在初次同步开始时近 0，语义误导。如需"相对推进率"请订阅端自行计算 `(C5 - restart_lsn) / (C0 - restart_lsn)` 命名为 `catchup_pct`。 |

#### 订阅端（按订阅 worker，一 worker 一行）

| 指标 | 公式 | 说明 |
| --- | --- | --- |
| `recv_rate` | ΔC3′/Δt | 接收速率（C3′ = `received_lsn`，由 `LogicalRepUpdateProgress` 在每条流消息到达时推进；附注见下） |
| `apply_rate` | ΔC5′/Δt | 应用速率（C5′ = origin `remote_lsn`，PG18 下由 leader apply worker 推进，parallel apply 时等于 "leader 已 commit 速率"） |
| `local_wal_rate` | Δ `pg_current_wal_lsn()` / Δt | 订阅端本地 WAL 插入速率（含未 commit 的 WAL，应用回放产生 + 本库其他写入的噪声，见 §3.3） |
| `backlog_apply` | C3′ − C5′ | 已接收未应用积压（订阅端实时口径，无反馈延迟） |
| `eta_apply` | backlog_apply / apply_rate | 追平预估秒数；速率 < `eta_min_rate` 时为 NULL |

> **关于 C3′ 的取值**：`pg_stat_subscription.received_lsn` 是"最近一个已接收流消息的 LSN"，`latest_end_lsn` 是"最近一个已完成事务窗口的结束 LSN"。**稳态下 `received_lsn ≥ latest_end_lsn`**（流式接收持续推进，apply worker commit 时更新 `latest_end`），故直接取 `received_lsn` 即可；仅当 apply worker 重连/重启后存在 `latest_end_lsn > received_lsn` 的瞬态（保留事务窗口边界）。当前设计固定取 `received_lsn` 以保持口径简单，**不**采用"取较大"的隐式口径——后者在数据流极端场景下会被 `latest_end_lsn` 的瞬态拉偏，导致 `backlog_apply` 出现负值风险。如未来 PG 改动二者关系，须在 §5.5 的 SRF 内显式处理。

#### 整体（订阅端合成，按订阅）

由订阅端的远端轮询（§5.7）与本地采样拼装：发布端检查点来自轮询，订阅端检查点用本地实时值，**全部速率在同一条本地时钟轴上差分**：

| 指标 | 公式 | 说明 |
| --- | --- | --- |
| `gen_rate` / `send_rate` | ΔC0/Δt、ΔC2/Δt | 发布端速率（远端轮询差分） |
| `recv_rate` / `apply_rate` | ΔC3′/Δt、ΔC5′/Δt | 订阅端速率（本地采样） |
| `backlog_unsent` | C0 − C2 | 发布端未发送 |
| `backlog_inflight` | C2 − C3′ | 已发送 → 本地已接收（真·网络在途，无反馈延迟失真） |
| `backlog_unapplied` | C3′ − C5′ | 本地已收未应用 |
| `backlog_total` | C0 − C5′ | 端到端总积压（各段之和） |
| `retained_wal` | C0 − C7 | 发布端槽保水 |
| `feedback_lag_bytes` | C3′ − C3 | 本地实收 − 反馈报文中的接收位：反馈滞后量的直观度量（纯观测列） |
| `eta_total` | unsent/send_rate + (inflight + unapplied)/apply_rate | 分段求和；任一段速率不足为 NULL |

### 3.2 指标语义澄清

- **"发送速率"测的是协议字节流的 LSN 等效推进**，即按发布端 WAL 位点差分（sent_lsn 是 WAL 位点），不是 socket 字节数；解码膨胀（输出插件序列化后的字节量）由 `pg_stat_replication_slots.total_bytes` 与 `spill/stream` 计数补充观测。
- **`backlog_unsent` 包含解码耗时**：单进程 walsender 中"读 WAL+解码"与"发送"串行，无法区分，解码压力只能经由 D1 的 spill/stream 计数间接观测。若未来内核暴露"已解码位点"，可在不改变对外接口的前提下细分为 `backlog_decode` 与 `backlog_sendq`。
- **`confirmed_flush_lsn`（C6）与 `replay_lsn`（C5）的差别（重要）**：
  - **来源不同**：`replay_lsn`（C5）来自反馈报文的 `apply` 字段（`worker.c` `pq_sendint64(reply_message, writepos);	/* apply */`，最终落 `pg_stat_replication.replay_lsn`）；`confirmed_flush_lsn`（C6）**不是反馈位**，而是 `pg_replication_slots` 中 walsender 依据反馈 flush（C4）推进的槽内部状态。视图层不要把 `confirmed_flush_lsn` 当作"反馈位的另一份"。
  - **大小关系**：通常 `C6 ≥ C4`（confirmed 总是追上或超过反馈 flush），`C6` 与 `C5` 没有强制大小关系（独立 commit 路径）。文档不强行统一两者视图，分别暴露。
  - **应用上的视角——C5 是"已 commit 位点"而非"已应用位点"**：见 §2.2 C5 行的说明。文档提到 "已应用" 仅在 leader 串行回放下近似成立，parallel apply 视图使用 C5 时必须按 commit 位点理解。
- **整体视图的取值优先级**：订阅端检查点一律取本地采样值（无反馈延迟），发布端检查点取远端轮询值；`pg_stat_replication` 反馈来的 write/flush/replay 位在整体视图中仅用于 `feedback_lag_bytes` 交叉观测，不作为主数据源。

### 3.3 订阅端"生成速率"的定义

订阅端没有"接收流落 WAL"阶段（§2.3-2），因此本设计把"生成速率"定义为：**应用逻辑复制数据所引发的本地 WAL 写入速率**，以订阅库 `pg_current_wal_lsn()` 差分近似。已知噪声：订阅库自身的其他写入（analyze、vacuum、用户写）会计入。若订阅库为专用库（常见部署），该值即应用产生的 WAL 速率。此定义已列入"开放问题"（§11），实现时保持独立列 `local_wal_rate`，不与 `apply_rate` 混算。

---

## 4. 总体架构

### 4.1 部署形态

```
        ┌────────────── 发布集群（每库 CREATE EXTENSION）──────────────┐
        │  shared_preload_libraries = 'pg_lrstat'                      │
        │  ┌─────────────┐   SPI    ┌───────────────────────────────┐  │
        │  │ lrstat      │ ───────► │ pg_stat_replication /          │  │
        │  │ sampler     │          │ pg_replication_slots /         │  │
        │  │ (bgworker)  │ ◄─────── │ pg_stat_replication_slots ...  │  │
        │  └──────┬──────┘  写入    └───────────────────────────────┘  │
        │         ▼                                                     │
        │  ┌─────────────────── 共享内存（全集群一份）────────────────┐  │
        │  │ 目标表 × 环形历史（raw 30s 环 + coarse 5min 环）          │  │
        │  └──────┬──────────────────────────────────────────────────┘  │
        │         ▼  SRF 读取                                          │
        │  pg_lrstat_pub_sample / pub_rate / pipeline / pub_history     │
        └───────────────────────────────────────────────────────────────┘
        ┌────────────── 订阅集群（同扩展同构）────────────────────────┐
        │  本地采样: pg_stat_subscription / pg_replication_origin_status│
        │            / pg_subscription / pg_current_wal_lsn()          │
        │  远端轮询: sampler 以 subconninfo ──libpq──► 发布端系统视图   │
        │  视图:   pg_lrstat_sub_sample / sub_rate / sub_history        │
        │          pg_lrstat_overall（两端合成整体视图，§5.5/§5.7）     │
        └───────────────────────────┬───────────────────────────────────┘
                                    │ 只读 SELECT（普通连接，发布端免安装）
                                    ▼
                     发布端: pg_current_wal_lsn() / pg_stat_replication /
                     pg_replication_slots / pg_stat_replication_slots
        远端轮询仅订阅端单向拉取；多订阅/多发布拓扑的集中汇总仍由监控端对位 JOIN。
```

- 同一扩展二进制在两端通用：采样器逐库轮询时，发布库产出槽样本，订阅库产出订阅样本，互不冲突；发布+订阅同库（级联场景）自然共存。
- 视图可在任意库创建；数据在共享内存中全集群共享（与 `pg_stat_replication` 本身一样是集群级事实）。
- 订阅端额外承担远端轮询与整体视图合成（§5.7）；发布端零依赖，什么都不用装。

### 4.2 采样实现路线的选型

| 方案 | 做法 | 结论 |
| --- | --- | --- |
| A（选定）SPI 采样 | bgworker 连接各数据库，SPI 查询 §附录 B 的只读 SQL | 版本面稳定（只依赖稳定视图/函数）、权限语义与用户一致、实现量小；采样开销为每轮几次索引扫描，默认 30s 周期下可忽略 |
| B 直读共享内存 | 直接 `#include` `walsender_private.h` / `slot.h` / `worker_internal.h` 读 `WalSndCtl`、`ReplicationSlotCtlData`、`LogicalRepCtx`、replorigin 哈希 | 免 SPI、无数据库连接依赖；但绑定私有结构（跨版本脆弱），且订阅列表/origin 仍要查 catalog。列为 P2 优化项（发布端热点计数器直读） |

选 A 的决定性理由：`WalSnd`、槽控制块等私有共享结构属于后端内部接口，跨版本变动频繁，PGXS 外部构建下包含 `*_private.h` 也不可靠；而系统视图/系统函数是受治理的稳定接口，扩展只需随版本核对列增减即可。

### 4.3 组件清单

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| 入口/引导 | `pg_lrstat.c` | `_PG_init`：注册 shmem hook、bgworker；`pg_lrstat--1.0.sql` 建视图与函数 |
| 采样器 | `lrstat_worker.c` | bgworker 主循环：逐库 SPI 采样 → 写环 |
| 远端轮询 | `lrstat_remote.c` | 订阅端对发布端的只读轮询：连接缓存、异步查询、超时/退避（§5.7） |
| 共享内存 | `lrstat_shmem.c` | 目标分配/回收、环形缓冲、锁 |
| SRF | `lrstat_sql.c` | 快照/历史/注入函数，速率计算在读取时完成 |

---

## 5. 详细设计

### 5.1 采样器（bgworker）

- **注册**：`shared_preload_libraries` 加载时注册（`BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION`），postmaster 崩溃重启自动拉起。
- **数据库遍历**：参照 autovacuum 的库列表模式。每轮从 `pg_database` 取所有 `datallowconn` 且非模板库，逐库连接采样；库删除/不可连时跳过并限频记 WARNING。发布槽与订阅都是库级对象，逐库遍历保证不漏。
- **节拍**：`WaitLatch` 超时驱动，`pg_lrstat.sample_interval`（默认 30s）；单轮耗时超过间隔则本轮顺延，不叠加补偿采样。
- **时钟**：每轮开始取一次 `clock_timestamp()`，同轮所有目标共用该时间戳，保证速率分子分母对齐。
- **远端轮询（仅订阅库）**：本地采样完成后，对每个启用中的订阅用其 `subconninfo` 异步轮询发布端系统视图（§5.7），写入 `LR_TARGET_RPUB` 目标；整轮受 `pg_lrstat.remote_poll_budget` 约束，超时/失败不阻塞本地采样，置 `remote_state` 并退避重试。
- **异常**：SPI 出错按 `PG_TRY` 捕获、abort 事务、限频日志，下轮重试；worker 自身 FATAL 交给 postmaster 重启。
- **无任何写路径**：只 SELECT；不建临时表、不写 catalog。

### 5.2 共享内存与数据结构

```c
/* 单个发布端样本（一槽一样本） */
typedef struct LRPubSample
{
    TimestampTz ts;                 /* 采样时刻（本轮统一时钟） */
    XLogRecPtr  current_lsn;        /* C0  pg_current_wal_lsn()      */
    XLogRecPtr  sent_lsn;           /* C2                            */
    XLogRecPtr  peer_recv_lsn;      /* C3  反馈 write                */
    XLogRecPtr  peer_flush_lsn;     /* C4  反馈 flush                */
    XLogRecPtr  peer_applied_lsn;   /* C5  反馈 apply                */
    XLogRecPtr  confirmed_lsn;      /* C6                            */
    XLogRecPtr  restart_lsn;        /* C7                            */
    uint64      spill_bytes;        /* D1                            */
    uint64      stream_bytes;       /* D1                            */
    uint64      total_bytes;        /* D1                            */
} LRPubSample;                      /* 96 B */

/* 单个订阅端样本（一 worker 一样本） */
typedef struct LRSubSample
{
    TimestampTz ts;
    XLogRecPtr  received_lsn;       /* C3' received_lsn              */
    XLogRecPtr  latest_end_lsn;     /* C3' latest_end_lsn            */
    XLogRecPtr  applied_lsn;        /* C5' origin remote_lsn         */
    XLogRecPtr  origin_local_lsn;   /* origin local_lsn              */
    XLogRecPtr  local_wal_lsn;      /* 订阅库 pg_current_wal_lsn()   */
} LRSubSample;                      /* 56 B */

typedef enum { LR_TARGET_PUB = 1, LR_TARGET_SUB = 2, LR_TARGET_RPUB = 3 } LRTargetKind;

typedef struct LRTargetCtl
{
    LRTargetKind kind;
    char        name[NAMEDATALEN];  /* pub: 槽名；sub/rpub: 订阅名     */
    /* sub 专用 */
    Oid         relid;              /* tablesync worker 的表，0=apply */
    bool        is_tablesync;
    /* 生命周期 */
    bool        in_use;
    uint32      generation;         /* 槽位复用计数，防陈旧数据冒名   */
    TimestampTz last_sample_ts;     /* 老化与断档判定                 */
    /* 并发控制：单写者(sampler)多读者(backend)，spinlock 足够 */
    slock_t     mutex;
    uint32      raw_head;
    uint32      raw_count;
    uint32      coarse_head;
    uint32      coarse_count;
    /* 变长环（raw_len/coarse_len 由 GUC 定）按 kind 解释为两种样本 */
    FlexibleArray samples;          /* raw[raw_len] + coarse[coarse_len] */
} LRTargetCtl;

typedef struct LRStatShared
{
    int32       magic;
    LWLock      map_lock;           /* 目标槽位的分配/回收           */
    uint32      ntargets;           /* = pg_lrstat.max_targets       */
    LRTargetCtl targets[FLEXIBLE_ARRAY_MEMBER];
} LRStatShared;
```

- **空间估算**：每目标 ≈ 头部 128B + raw 1800×96B + coarse 1440×96B ≈ 311KB（pub 样本口径）；默认 `max_targets=32` → ≈ 10MB（通过 `RequestAddinShmemSpace` 在启动期计算，含对齐）。GUC 与公式见 §5.8。
- **目标分配**：采样器每轮以 `(kind, name, relid)` 为键查找；未命中则在 `map_lock` 下取一个空闲或最久未更新的槽位，`generation++`。**老化回收**：`last_sample_ts` 超过 `stale_target_ttl`（默认 10min）的目标允许被新目标复用（保证订阅重建、槽删除后不占位）。
- **远端目标**：`kind = LR_TARGET_RPUB`，一订阅一个（键 `subname`），样本复用 `LRPubSample` 结构（C0/C2/C6/C7 + spill/stream 计数 + 反馈位用于 `feedback_lag_bytes` 交叉观测）；时间戳取**轮询完成时的本地时钟**，与本地样本同轴。
- **写路径**：sampler 持目标 spinlock 写一个样本（memcpy 级，纳秒）；**读路径**：SRF 拷贝出所需窗口后再计算，锁内只做 memcpy，不做算术。

### 5.3 速率与积压算法

速率**在查询时**基于环形历史计算（不在采样时预计算），便于更换窗口而不丢历史：

```
输入：目标 t，字段 f（LSN/计数器），窗口 W（默认 2min）
1. 从 raw 环取最新有效样本 s1（ts 最新且 generation 匹配）
2. 向前找最旧样本 s0 满足 ts(s0) >= ts(s1) - W；至少要有 K=2 个样本
3. 有效跨度 dt = ts(s1) - ts(s0)；若 dt < 0.5*W（数据不足/刚启动）→ NULL
4. 断档检测：若 ts(s1) 距 now > 3*sample_interval → 视为采样中断，返回 NULL
   （防 bgworker 重启/库不可连期间的旧数据被当成当前值）
5. d = f(s1) - f(s0)；LSN 单调递增（64bit 无回绕），d = max(d, 0)
6. rate = d / dt（字节/秒，double）；视图输出统一换算为 MB/s
```

- **NTP 回拨**：同一目标内时间戳若非单调（`ts(s1) < ts(s0)`），直接判 NULL，不产出负速率。
- **停滞判定**：`rate` 非 NULL 且 < 1 B/s 且 backlog > 0 → 视图置 `*_stalled = true`（发送停滞/应用停滞，供外部告警）。
- **ETA**：`backlog / rate`，当 rate < `eta_min_rate`（默认 1KB/s）时返回 NULL，避免"速率≈0 时 ETA=∞"式的噪声输出。
- 积压为**读取时刻两点位差**，用最新样本；不参与窗口差分（差分才有除零问题，两点位差没有）。
- 可选平滑：EWMA（α=0.3）仅作为 `*_rate_smooth` 附加列（P1 实现、默认不透出，避免列爆炸）。

### 5.4 多分辨率历史与降采样

- raw 环：`sample_interval`（默认 30s）粒度，默认保留 1800 点（约 15 小时）。
- coarse 环：每满 10 个 raw 样本（5 分钟边界）归档一个（取该区间最后一个样本），默认 1440 点（约 5 天）。
- 读取函数 `pg_lrstat_history_*` 可指定 `granularity := 'raw' | 'coarse'`；速率窗口超出 raw 覆盖（约 15 小时）时自动落 coarse 环。
- 用途：raw 支撑速率与近期毛刺；coarse 支撑长趋势（如迁移期间的积压曲线）。

### 5.5 对外 SQL 接口

#### 视图族总览

观测面按"**采样表**（瞬时事实）"与"**实时速率表**（窗口差分结果）"两类组织，两者都带时间列；整体视图与阶段分解表为合成层：

| 视图 | 端 | 一行粒度 | 内容 | 时间列 |
| --- | --- | --- | --- | --- |
| `pg_lrstat_pub_sample` | 发布 | 一逻辑槽 | 最新采样事实：LSN 检查点、状态、积压、解码计数器 | `sample_time` |
| `pg_lrstat_pub_rate` | 发布 | 一逻辑槽 | 实时速率、ETA、停滞位 | `rate_time`、`window_start_time`、`window_end_time` |
| `pg_lrstat_sub_sample` | 订阅 | 一 worker | 最新采样事实 | `sample_time` |
| `pg_lrstat_sub_rate` | 订阅 | 一 worker | 实时速率、ETA、停滞位 | `rate_time`、`window_start_time`、`window_end_time` |
| `pg_lrstat_overall` | 订阅 | 一订阅 | 两端合成整体视图 | `sample_time`、`last_remote_poll_time`、`rate_time` + 窗口 |
| `pg_lrstat_pipeline` | 发布 | 一槽一阶段 | 阶段分解（速率 + 积压 + lag） | `rate_time` + 窗口 |
| `pg_lrstat_pub_history` / `pg_lrstat_sub_history` | 两端 | 时间序列 | 原始样本回放（画图用） | `ts` |

#### `pg_lrstat_pub_sample`（发布端采样表，一逻辑槽一行）

最近一次采样的瞬时事实，`sample_time` 为该样本所属轮次的统一采样时钟：

| 列 | 类型 | 来源 |
| --- | --- | --- |
| `slot_name` | name | 槽名 |
| `sample_time` | timestamptz | 本行样本的采样时刻 |
| `database` / `plugin` / `temporary` | name / name / bool | 槽属性（temporary=true 为初始同步临时槽） |
| `active` / `sender_pid` | bool / int | 槽激活态 / walsender pid |
| `application_name` / `client_addr` | text / inet | 订阅侧标识（权限裁剪同 §5.9） |
| `state` / `sync_state` | text | 透传 |
| `current_lsn` / `sent_lsn` / `peer_recv_lsn` / `peer_flush_lsn` / `peer_applied_lsn` / `confirmed_flush_lsn` / `restart_lsn` | pg_lsn | C0, C2, C3, C4, C5, C6, C7 |
| `wal_status` / `safe_wal_size` | text / bigint | 透传（lost/extended 等风险态） |
| `spill_bytes` / `stream_bytes` / `total_bytes` | bigint | 解码计数器原值（D1） |
| `backlog_unsent` / `backlog_inflight` / `backlog_peer_unapplied` / `backlog_total` / `retained_wal` | bigint | §3.1（单样本点位差） |
| `write_lag` / `flush_lag` / `replay_lag` | interval | 反馈时间延迟透传 |
| `reply_time` | timestamptz | 最近反馈时刻 |

#### `pg_lrstat_pub_rate`（发布端实时速率表，一逻辑槽一行）

由采样历史窗口差分计算（§5.3）；`rate_time` 为本行计算时刻，`window_*` 标出实际参与差分的样本区间，保证速率可追溯：

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `slot_name` | name | 槽名 |
| `rate_time` | timestamptz | 速率计算时刻 |
| `window_start_time` / `window_end_time` / `window_secs` | timestamptz / timestamptz / double | 实际差分窗口 |
| `gen_rate` / `send_rate` / `apply_rate` / `confirm_rate` | double precision（MB/s） | §3.1；`apply_rate` 来源是反馈报文，含一个反馈周期延迟 |
| `spill_rate` / `stream_rate` | double precision（MB/s） | 解码溢写/流式落盘速率 |
| `eta_unsent` / `eta_total` | double precision（秒） | 速率不足为 NULL |
| `send_stalled` | bool | §5.3 |

> **注意**：原设计草稿曾提供 `progress_pct = 100×C5/C0`，但 C0 是当前 WAL 位点（持续前进），C5/C0 在初次同步时近 0，看起来"进度很慢"实则已快完成，已不输出该列。如需展示同步进度，请订阅端自行计算 `(C5 - restart_lsn) / (C0 - restart_lsn)` 并赋语义名 `catchup_pct`。

#### `pg_lrstat_sub_sample`（订阅端采样表，一 worker 一行）

| 列 | 类型 | 来源 |
| --- | --- | --- |
| `sub_name` / `subid` | name / oid | 订阅 |
| `sample_time` | timestamptz | 本行样本的采样时刻 |
| `worker_type` | text | 'apply' / 'tablesync'（透传 PG18 列） |
| `worker_pid` / `leader_pid` / `relid` | int / int / oid | worker 拓扑（parallel apply 时 leader_pid 非空） |
| `received_lsn` / `latest_end_lsn` / `applied_lsn` / `origin_local_lsn` / `local_wal_lsn` | pg_lsn | C3′、C5′ 等 |
| `last_msg_send_time` / `last_msg_receipt_time` / `latest_end_time` | timestamptz | 透传 |
| `backlog_apply` | bigint | C3′ − C5′ |
| `apply_error_count` / `sync_error_count` | bigint | 透传 `pg_stat_subscription_stats`（P1） |

#### `pg_lrstat_sub_rate`（订阅端实时速率表，一 worker 一行）

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `sub_name` / `worker_type` / `relid` | name / text / oid | 目标标识 |
| `rate_time` | timestamptz | 速率计算时刻 |
| `window_start_time` / `window_end_time` / `window_secs` | timestamptz / timestamptz / double | 实际差分窗口 |
| `recv_rate` / `apply_rate` / `local_wal_rate` | double precision（MB/s） | §3.1 |
| `eta_apply` | double precision（秒） | §3.1；`apply_lag_time` 与 `eta_apply` 公式重复，已合并删除——权威时间延迟请用发布端 `replay_lag` |
| `apply_stalled` | bool | backlog>0 且 apply_rate≈0 |

#### `pg_lrstat_pipeline`（发布端端到端阶段分解，一槽一阶段一行）

一行 =（slot, stage），便于直接画 stacked bar / per-stage 监控；每行携带 `rate_time` 与 `window_start_time`/`window_end_time`（与速率表同一差分窗口）：

| stage | bytes 列 | rate 列 | lag 列 |
| --- | --- | --- | --- |
| `unsent`（生成→已发送） | backlog_unsent | send_rate | — |
| `inflight`（已发送→对端已收） | backlog_inflight | send_rate | `flush_lag - write_lag`（更贴近"接收 vs 本地落盘"时延；改自原设计的 `write_lag`，理由见 §6.5.2 与 §2.2 C4） |
| `peer_unapplied`（对端已收→已应用） | backlog_peer_unapplied | apply_rate | `replay_lag - flush_lag`（commit 时延，**不是事务级 apply 耗时**；可为负 → NULL） |
| `retained`（槽保水） | retained_wal | — | — |

#### `pg_lrstat_overall`（订阅端整体视图，一订阅一行）

发布端检查点来自远端轮询（§5.7）、订阅端检查点用本地采样，速率统一在本地时钟轴计算；**仅部署订阅端即可得到两端全链路统计**：

| 列 | 类型 | 说明 |
| --- | --- | --- |
| `sub_name` / `subslotname` | name | 订阅名 / 对应发布端槽名 |
| `remote_state` | text | 'ok' / 'unreachable' / 'stale'（轮询失败或结果超期） |
| `last_remote_poll_time` | timestamptz | 最近一次成功轮询时刻 |
| `sample_time` | timestamptz | 本地样本时刻（与上一列共同标识两端数据新鲜度） |
| `rate_time` / `window_start_time` / `window_end_time` / `window_secs` | timestamptz / double precision | 速率差分窗口（同速率表） |
| `pub_current_lsn` / `sent_lsn` | pg_lsn | C0、C2（轮询） |
| `received_lsn` / `applied_lsn` | pg_lsn | C3′、C5′（本地） |
| `confirmed_flush_lsn` / `restart_lsn` | pg_lsn | C6、C7（轮询） |
| `gen_rate` / `send_rate` | double precision | 发布端速率（轮询差分） |
| `recv_rate` / `apply_rate` | double precision | 订阅端速率（本地） |
| `backlog_unsent` / `backlog_inflight` / `backlog_unapplied` / `backlog_total` / `retained_wal` | bigint | §3.1 整体指标表 |
| `feedback_lag_bytes` | bigint | C3′ − C3，反馈滞后观测 |
| `write_lag` / `flush_lag` / `replay_lag` | interval | 发布端反馈 lag 透传 |
| `spill_rate` / `stream_rate` | double precision | 发布端解码压力（轮询差分） |
| `eta_unsent` / `eta_total` | double precision | 分段追平预估 |
| `send_stalled` / `apply_stalled` | bool | 停滞位 |

#### 历史 / 维护函数

- `pg_lrstat_pub_history(slot_name, since, until, granularity)` / `pg_lrstat_sub_history(...)`：返回原始时间序列（监控画图用）。
- `pg_lrstat_reset()`：清空所有目标与历史（superuser）。
- `pg_lrstat_inject_sample(...)`：**仅测试**，手工注入样本驱动速率算法做确定性单测；由 `pg_lrstat.allow_inject`（默认 off，SUSET）开关控制。

### 5.6 采样 SQL（方案 A 的数据面）

发布端（逐库执行；全文见附录 B）：

```sql
SELECT $1::timestamptz,                       /* 本轮统一时钟 */
       s.slot_name, d.datname, s.plugin, s.temporary, s.active, s.active_pid,
       r.pid AS sender_pid, r.application_name, r.client_addr,
       r.state, r.sync_state,
       pg_current_wal_lsn(),
       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn,
       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status,
       rs.spill_bytes, rs.stream_bytes, rs.total_bytes
FROM pg_replication_slots s
LEFT JOIN pg_database d ON d.oid = s.datoid
LEFT JOIN pg_stat_replication r
       ON r.pid = s.active_pid
LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name
WHERE s.slot_type = 'logical';
```

订阅端：

```sql
SELECT $1::timestamptz,
       su.oid, su.subname, st.worker_type, st.pid, st.leader_pid, st.relid,
       st.received_lsn, st.latest_end_lsn,
       o.remote_lsn, o.local_lsn AS origin_local_lsn,
       pg_current_wal_lsn()
FROM pg_subscription su
LEFT JOIN pg_stat_subscription st ON st.subid = su.oid
LEFT JOIN pg_replication_origin_status o ON o.external_id = su.subname
WHERE su.subdbid = $2;                        /* 当前采样库 */
```

（origin 与订阅的对应关系：apply worker 以订阅名创建/使用 origin，`rname = subname`。）

### 5.7 发布端远端轮询（整体视图数据面）

订阅端 sampler 为每个启用中的订阅维护一条到发布端的**普通 libpq 连接**（非复制协议），按采样节拍执行只读查询，结果写入 `LR_TARGET_RPUB` 环：

```sql
-- 在发布端执行（$1 = subslotname）
SELECT pg_current_wal_lsn(),
       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status,
       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn,
       rs.spill_bytes, rs.stream_bytes, rs.total_bytes
FROM pg_replication_slots s
LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid
LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name
WHERE s.slot_name = $1;
```

- **连接管理**：连接缓存于 worker 私有内存（每订阅至多一条，订阅删除/禁用即断开）；连接串复用 `subconninfo`，worker 仅追加 `application_name='pg_lrstat'` 与 `connect_timeout`；凭证只在内存中使用，日志与视图不落 conninfo。
- **异步与预算**：基于 `src/include/libpq/libpq-be-fe-helpers.h` 提供的 bgworker 异步 libpq helper 实现——`libpqsrv_connect()` / `libpqsrv_connect_params()` 建立连接（保留 FD、注册中断处理），`libpqsrv_exec_params()` 提交参数化查询、`libpqsrv_get_result()` 异步取结果（自动注册 `wait_event`，`pg_stat_activity` 可见等待原因）。手写 `PQconnectStart`+`PQsendQuery`+latch 轮询会漏掉 `wait_event` 注册与中断处理，不采用。单轮远端总预算 `pg_lrstat.remote_poll_budget`（默认 500ms），超时即放弃本轮（该订阅远端样本缺失，`remote_state` 置 'unreachable'/'stale'），**绝不拖延本地采样节拍**。
- **退避**：连续失败指数退避（1s→2s→…→60s 封顶），日志限频；发布端恢复后自动重连。
- **时钟统一**：远端样本时间戳取轮询完成时的本地时钟——发布端速率实为"本地时钟轴上的轮询差分"，两端 NTP 偏差不进入任何计算；轮询 RTT（亚秒级）相对采样间隔可忽略。**注意**：远端窗口的实际时间跨度 = 两次成功轮询之间的本地时钟差 + 各自 poll 完成时的 RTT，单 RTT 远小于 30s 采样间隔时可忽略；跨地域部署（RTT > 1s）会让 `window_secs` 反映的远端速率系统性偏低，监控告警阈值应放宽（具体见 §1.4）。
- **发布端权限**：远端用户即订阅连接用户。建议在发布端授予 `pg_monitor`（至少 `pg_read_all_stats`）以看到完整 walsender 行与槽详情；未授权时行通常仍可见（walsender 以同一用户运行），但 `client_addr` 等列为 NULL，视图对缺失列一律 NULL 容忍。
- **发布端运行时影响**：每订阅一条空闲普通连接 + 每采样周期一次轻量 SELECT。**部署侧容量评估必须把 N 条 pg_lrstat 计入发布端 `max_connections`**，并允许这些连接出现在 `pg_stat_activity`（`application_name='pg_lrstat'`）；`pg_hba.conf` 与角色权限按上述推荐配置。`pg_lrstat.remote_poll = off` 可整体关闭远端轮询、仅保留本地视图。详见 §1.4。

### 5.8 GUC 参数

| GUC | 默认 | 说明 |
| --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | 采样周期（下限 1s；远端轮询同节拍） |
| `pg_lrstat.rate_window` | `2min` | 速率窗口（需 ≥ 2×采样间隔） |
| `pg_lrstat.max_targets` | 32 | 共享内存目标数上限（重启生效） |
| `pg_lrstat.raw_history_samples` | 1800 | raw 环长度（重启生效） |
| `pg_lrstat.coarse_history_samples` | 1440 | coarse 环长度（重启生效） |
| `pg_lrstat.stale_target_ttl` | `10min` | 目标老化回收阈值 |
| `pg_lrstat.eta_min_rate` | `1kB/s` | ETA 有效性下限 |
| `pg_lrstat.remote_poll` | on | 订阅端是否远端轮询发布端（整体视图数据面） |
| `pg_lrstat.remote_connect_timeout` | `5s` | 远端连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | 单轮远端轮询总预算 |
| `pg_lrstat.allow_inject` | off | 测试注入口开关（SUSET） |

`sample_interval` / `rate_window` / `eta_min_rate` / `remote_poll` / `remote_connect_timeout` / `remote_poll_budget` 支持运行时 `pg_reload_conf()`（sampler 下一轮生效）；其余影响 shmem 布局，需重启。

### 5.9 权限模型

- 所有视图 `REVOKE ALL FROM PUBLIC; GRANT SELECT ON ... TO pg_monitor;`；底层 SRF `REVOKE EXECUTE FROM PUBLIC`。
- 采样 worker 以 bgworker 身份连接（等价 superuser 会话），因此可读到完整统计；对外视图重新收紧。
- `client_addr` 等标识列沿用 `pg_read_all_stats` 的语义：非特权用户查询时置 NULL（在 SRF 内用 `has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS) || superuser()` 判定），与 `pg_stat_replication` 行为一致。
- 整体视图把发布端统计带入订阅端可见范围：视图仍只授予 `pg_monitor`，且不暴露 conninfo/密码；远端轮询复用订阅自身凭证，不引入新的凭证面。

---

## 6. 端到端视图拼装与链路性能分析

### 6.1 分阶段结论表（本插件的核心交付）

任一时刻，对每个（发布槽 ↔ 订阅）对，用户应能直接回答（订阅端开启远端轮询时，`pg_lrstat_overall` 一张视图即给出全部答案；下表同时标出仅单侧部署时的替代查询）：

| 问题 | 发布端查询 | 订阅端查询 |
| --- | --- | --- |
| WAL 产出多快？ | `gen_rate` | — |
| 发得出去吗？ | `backlog_unsent` + `send_rate` + `send_stalled` | — |
| 网络是瓶颈吗？ | `backlog_inflight`（需结合反馈延迟解释） | `recv_rate` |
| 解码是瓶颈吗？ | `spill_rate`/`stream_rate`（间接） | — |
| 订阅端应用是瓶颈吗？ | `backlog_peer_unapplied` + `apply_rate`（经反馈，滞后约 `wal_receiver_status_interval`） | `backlog_apply` + `apply_rate`（实时、权威；命名同源但来源不同步） |
| 还有多久追平？ | `eta_total` | `eta_apply` |
| WAL 会不会被拖爆？ | `retained_wal` + `wal_status` + `safe_wal_size` | — |

### 6.2 时钟与口径纪律

- 跨机时间延迟一律使用内核反馈算法产物（`*_lag` 列），本扩展不自造跨机时间差。
- 两端各自的速率/积压口径独立成立；合并展示时以**订阅端实时值**为准，发布端反馈值用于单侧部署场景的近似。
- `backlog_inflight` 在逻辑复制里天然包含反馈周期造成的"假在途"（默认 10s），文档与视图注释中显式说明；整体视图改用本地 C3′ 计算该段（§3.1 整体表），消除了这一失真。

### 6.3 并发形态的正确性

- `streaming = parallel`：leader/parallel worker 共享 origin，`remote_lsn` 由 leader 提交推进；订阅视图按 worker 行展示（`leader_pid` 列区分），backlog 以 leader 行为准。
- `two_phase`：prepared 事务在 prepare 与 commit 各推进一次 remote_lsn，不影响 LSN 差分正确性。
- 多表初始同步：每张表一个 tablesync worker + 临时槽（`temporary=true`），发布/订阅两侧均单列一行，不与主链路混算。

### 6.4 瓶颈定位：三步决策树

第一步看总量，第二步看分段占比，第三步用速率关系定性。全部基于 `pg_lrstat_overall`（双侧分别部署时用 pub/sub 视图组合）：

```
① backlog_total 是否超阈值且持续增长？
├─ 否 → 稳态链路：转到 ④ 做容量余量评估
└─ 是 → ② 积压集中在哪一段（各段 / backlog_total 占比）？
    ├─ unsent（C0−C2）占大头     → 发布端问题 …… ③-A，下钻 §6.5.1
    ├─ inflight（C2−C3′）占大头  → 网络/反馈 …… ③-B，下钻 §6.5.2
    └─ unapplied（C3′−C5′）占大头 → 订阅端应用 … ③-C，下钻 §6.5.3
③ 用速率关系表定性（下表；判据须在 2min 窗口持续成立，避免毛刺误判）
④ 余量 = 稳态下 gen_rate − min(send_rate, apply_rate)，趋势见 §6.7
```

| 速率关系（持续成立） | 结论 | 下钻 |
| --- | --- | --- |
| `send_rate` < `gen_rate` 且 unsent 增长 | 发布端发送/解码吞吐不足 | §6.5.1 |
| `send_rate` ≈ `gen_rate` 且 total 稳定非零 | 发送已尽力，短板在网络或对端 | 看 inflight / unapplied 占比 |
| `recv_rate` ≈ `send_rate` 且 inflight 小 | 网络不是瓶颈 | — |
| `apply_rate` < `recv_rate` 且 unapplied 增长 | 订阅端应用是短板 | §6.5.3 |
| 三者接近且 backlog_total → 0 | 全链路健康 | 转 §6.7 容量评估 |

### 6.5 分段下钻手册

#### 6.5.1 发布端（unsent 段）

- **解码压力**：`spill_rate`/`stream_rate` > 0 说明 ReorderBuffer 在溢写/流式落盘——大事务场景，解码被磁盘 IO 拖累。`pg_stat_replication_slots.total_bytes` 增速与 `gen_rate` 之比即**解码膨胀率**（输出协议字节 / 输入 WAL 字节），它同时近似实际线上字节速率，用于与链路带宽对照（注意 `send_rate` 是 WAL 位点等效速率，不是线上字节数）。
- **发送停滞**：`send_stalled`=t 时核对采样表 `state`（streaming/catchup）、OS 侧 walsender 进程 CPU 与 WAL 读 IO（`pg_stat_io` / iostat）。
- **同步复制干扰**：`sync_state` 非 async、state 显示等待 → walsender 被同步复制拖住，属于等待而非吞吐问题。
- **多订阅共担**：多个逻辑槽共享发布端资源，WAL 读取与解码按槽重复发生；各槽 unsent 同步增长而 `gen_rate` 正常，即发布端 CPU/IO 是共享瓶颈。

#### 6.5.2 网络与反馈（inflight 段）

- **先排除反馈假象**：inflight 与 `feedback_lag_bytes` 同量级时，"在途"主要是反馈周期（内核默认 10s）造成的观测滞后，不是网络问题。
- **真在途且持续增长**：把 `send_rate` 按解码膨胀率换算为线上字节速率，与链路带宽对照；高 RTT 链路的特征是 inflight 稳定在带宽×RTT（BDP）量级后不再增长——此时扩带宽无效，需提高并发流或压缩。

#### 6.5.3 订阅端（unapplied 段）

- **停滞**（`apply_stalled`=t）：查 `pg_stat_activity` 中 apply worker 的 `wait_event`（锁等待最常见：订阅端长事务/DDL 与回放冲突）；查 `pg_stat_subscription_stats` 的 `apply_error_count` 增速（冲突重试）。
- **吞吐不足但无停滞**：单 apply worker 串行回放是常态瓶颈——大事务、行级触发器、索引维护、外键检查都会拉低单位回放效率；核对 `streaming` 设置与并行 worker 是否实际启动（采样表 `leader_pid` 非空）。
- **存储瓶颈**：`local_wal_rate` 与 `apply_rate` 严重不成比例（本地写放大异常），或伴随订阅端磁盘写饱和（`pg_stat_io`）。
- **初始同步期**：存在 `worker_type='tablesync'` 行时 unapplied 增长属正常，按对应 relid 行单独评估。

### 6.6 常见故障模式 → 指标签名速查

| 模式 | 签名（组合特征） |
| --- | --- |
| 大事务回放 | `spill_rate`/`stream_rate` 飙升；unsent 先增后骤降；事务回放期间 apply_rate 短暂归零 |
| 网络抖动 | inflight 尖峰 + `feedback_lag_bytes` 波动；send_rate 突降后恢复 |
| 订阅端锁冲突 | `apply_stalled`=t 且 wait_event 为锁；unapplied 阶梯式增长 |
| 订阅暂停/断开 | 槽 active=f 或 `send_stalled`=t；`retained_wal` 线性增长；`wal_status` 沿 reserved→unreserved→lost 恶化 |
| 发布端写入洪峰 | `gen_rate` 突增，积压沿 unsent→inflight→unapplied 依次传导（各段到达时间差即该段传输延迟） |
| 反馈滞后误报 | inflight ≈ `feedback_lag_bytes` 且订阅端 applied_lsn 持续推进 |

### 6.7 容量规划与预测用法

- **追平窗口**：持续写入场景下用**净追平速率** = min(send_rate, apply_rate) − gen_rate 重估：eta = backlog_total / 净追平速率；净速率为负则永远追不平，应先扩容短板侧再谈追平。
- **余量评估**：稳态（backlog→0）下余量 = gen_rate − min(send_rate, apply_rate)；用 coarse 历史（5min 粒度、约 5 天覆盖）回归余量随负载的关系，按峰值负载外推安全余量。
- **WAL 拖爆预测**：`retained_wal` 增速 × 可用 `pg_wal` 空间 = 槽拖垮磁盘的剩余时间；结合 `wal_status`/`safe_wal_size` 与 `max_slot_wal_keep_size` 设置告警阈值。
- **粒度纪律**：容量结论一律用 coarse/长窗口数据；毛刺定位才临时调小 `sample_interval` 与 `rate_window`（窗口 ≥ 2×采样间隔）。

### 6.8 与内核观测的联动

扩展指标回答"**哪一段、多严重**"，根因确认回到内核与 OS：`pg_stat_activity`（walsender / apply worker 的 wait_event 与 state）、`pg_stat_io`（walsender 的 WAL 读、溢写 IO）、`pg_stat_subscription_stats`（错误与冲突计数）、订阅端日志（冲突详情）、OS 层（进程 CPU、网卡利用率、磁盘 util）。时间维度一律使用反馈 lag 列与各视图自带时间列，不做跨机 `now()` 差。

---

## 7. 性能与开销评估

- 采样成本：每轮每库 2 条 SPI 查询（发布/订阅各一），全部命中系统视图的轻量路径（`WalSndCtl` spinlock 快照、catalog 扫描），微秒~毫秒级；默认 30s 周期、32 目标下 CPU 开销可忽略不计，内存 ≈ 10MB（§5.2 公式）。
- 查询成本：SRF 只拷贝窗口内样本（≤ rate_window/sample_interval 条），速率计算 O(窗口长)。
- 无锁热点：采样器单写者；读侧锁内仅 memcpy。
- 不影响复制路径本身：不注册任何 hook 到 walsender/apply 热路径。

---

## 8. 文件布局与构建

```
contrib/pg_lrstat/              （亦可独立仓库走 PGXS，二选一，优先 contrib 内建）
├── Makefile / meson.build
├── pg_lrstat.control           (superuser, relocatable=no)
├── pg_lrstat--1.0.sql          （视图 + GRANT）
├── pg_lrstat.c                 （_PG_init / shmem hook / worker 注册）
├── lrstat_shmem.c/.h           （目标表 + 环形历史 + 锁）
├── lrstat_worker.c             （bgworker：逐库 SPI 采样、降采样、老化）
├── lrstat_remote.c             （订阅端对发布端的远端轮询：连接缓存/异步/退避）
├── lrstat_sql.c                （SRF：快照/历史/注入/重置）
├── sql/pg_lrstat.sql           （regress：注入样本 → 速率/ETA/积压断言）
├── expected/pg_lrstat.out
└── t/001_lrstat_basic.pl       （TAP 端到端）
```

依赖头文件均为公开接口（`postmaster/bgworker.h`、`storage/shmem.h`、`funcapi.h`、`executor/spi.h`），不 include 任何 `*_private.h`，保证 PGXS 可移植（P2 直读方案除外，将用 `#ifdef` 按版本保护）。

---

## 9. 测试方案

| 层 | 内容 |
| --- | --- |
| 单元（regress） | `pg_lrstat_inject_sample` 注入确定性序列：常数速率、零速率、时间回拨、断档、单样本（NULL）、大数值；断言速率/ETA/backlog 精确值 |
| 对拍 | TAP 中两次手工查询 `pg_stat_replication` 等差分近似视图速率（容差 20%，因窗口不同） |
| 场景（TAP，参照 `src/test/subscription` 基建） | ① 稳态流复制：各速率>0、backlog 有界；② `pg_subscript_pause`/断开订阅：`backlog_unsent`、`retained_wal` 持续增长、`send_stalled`；恢复后 ETA 递减至 0；③ 大事务（触发 spill/stream）：`spill_rate`>0；④ 初始同步：temporary 槽行 + tablesync 行出现并消失；⑤ `streaming=parallel`、`two_phase`；⑥ 槽失效（wal_receiver_status_interval 内 wal_status 变化）；⑦ 采样 worker kill & 重启：速率短暂 NULL 后恢复；⑧ 权限：非特权用户标识列为 NULL；⑨ 远端轮询正常合成：各检查点单调、四段积压之和 = backlog_total；⑩ 发布端停机：`remote_state` 转 unreachable、远端列为 NULL/stale，恢复后自动重连；⑪ 轮询超时不拖慢本地采样（人为延迟发布端应答，本地样本节拍不变）；⑫ 发布端权限不足：整体视图优雅降级，仅本地列有效 |
| 性能 | pgbench 高写入下对比开/关扩展的 tps 与 walsender 吞吐，验证开销 <1% |

---

## 10. 实施计划

| 阶段 | 交付 | 验收 |
| --- | --- | --- |
| P0（骨架） | shmem + sampler + 两端快照列（无速率） | TAP ①② 通过 |
| P1（完整 v1.0） | 采样表/速率表双视图族、pipeline、远端轮询与 `pg_lrstat_overall` 整体视图、history、权限、单元+场景测试 | §9 全绿，文档化指标口径 |
| P2（优化） | 发布端共享结构直读优化（`WalSndCtl`/槽控制块）、lag 时间列进入历史环 | 采样开销进一步降低，周期性抖动可回溯 |
| P3（扩展） | per-publication 粒度、JSON 导出、监控端合并工具（订阅端↔发布端对位 JOIN 的参考实现） | — |

---

## 11. 开放问题

1. 订阅端"生成速率"的口径（§3.3 现定义为应用引发的本地 WAL 速率，含噪声）是否满足需求，还是需要后续版本通过 apply 事务级统计精确归因？
2. 默认采样间隔 30s、速率窗口 2min 偏平滑，适合容量评估与趋势观测；实时排障需要更灵敏读数时需同时调小两者（窗口 ≥ 2×间隔），是否需要视图级参数化 `window`（当前：GUC 全局 + 历史函数参数，速率表不参数化以保简单）？
3. 是否在 P2 之外提供 `pg_lrstat_pipeline` 的订阅端对偶视图（receive→apply 分解已有，是否再拆 parallel apply 的各 worker 贡献）。
4. 远端轮询为每订阅在发布端增加一条普通连接；订阅数很大的发布端是否需要共享轮询连接或轮询频率上限约束（当前设计：一订阅一连接、随订阅增减、可整体关闭）。

---

## 附录 A：术语

- **LSN**：WAL Log Sequence Number（`pg_lsn`，64bit 单调）。
- **反馈报文**：订阅端 apply worker 周期性发往 walsender 的 standby status，逻辑复制中 write=最近接收位、flush=已 commit 且本地 WAL 已 flush 的事务位、apply=最近一次 commit 的事务位（`src/backend/replication/logical/worker.c` `send_feedback()`；与 §2.2 C3/C4/C5 的语义映射见正文）。注意 feedback 中标注的 `/* apply */` 实际值来自 `get_flush_position()` 的 `*write`，是 commit 位点而非逐消息应用位点。
- **confirmed_flush_lsn（C6）**：`pg_replication_slots` 视图中的槽安全水位，**不是反馈位**，而是 walsender 依据反馈 flush（C4）推进的槽内部状态，决定 `restart_lsn` 推进与 WAL 回收（详见 §2.2 与 §3.2）。
- **retained WAL / 保水**：因槽存在而不能回收的 WAL，`pg_current_wal_lsn() - restart_lsn`。
- **spill / stream（解码）**：ReorderBuffer 事务超限后溢写磁盘（spill）与流式发送未提交大事务（stream），对应 `pg_stat_replication_slots` 字节计数。

## 附录 B：采样 SQL 全文

见 §5.6 与 §5.7（发布端本地采样、订阅端本地采样、订阅端对发布端的远端轮询共三条查询；worker 逐库以 `subdbid = 当前库` 过滤订阅，槽按库归属自然过滤）。

## 附录 C：示例输出（目标形态）

速率列单位 **MB/s**（double precision 直接输出）；积压列 bigint，用 `pg_size_pretty()` 展示；所有视图均带时间列。

```sql
-- 发布端·采样表：最新样本的瞬时事实
SELECT slot_name, sample_time, state, current_lsn, sent_lsn, peer_recv_lsn,
       peer_applied_lsn, confirmed_flush_lsn, restart_lsn, wal_status,
       pg_size_pretty(backlog_unsent) AS unsent,
       pg_size_pretty(backlog_total)  AS total,
       pg_size_pretty(retained_wal)   AS retained
FROM pg_lrstat_pub_sample;

 slot_name |     sample_time     |   state   | current_lsn | sent_lsn   | peer_recv_lsn | peer_applied_lsn | confirmed_flush_lsn | restart_lsn | wal_status | unsent | total  | retained
-----------+---------------------+-----------+-------------+------------+---------------+------------------+---------------------+-------------+------------+--------+--------+----------
 sub_a     | 2026-09-22 10:31:00 | streaming | 0/3000D240  | 0/2FFFC880 | 0/2FFFC100    | 0/2FFF41C0       | 0/2FFF4640          | 0/2FFEA000  | reserved   | 43 kB  | 214 kB | 148 kB
```

```sql
-- 发布端·实时速率表：窗口差分结果（MB/s）
SELECT slot_name, rate_time, window_start_time, window_secs,
       gen_rate, send_rate, apply_rate, spill_rate, stream_rate,
       eta_unsent, eta_total, send_stalled
FROM pg_lrstat_pub_rate;

 slot_name |     rate_time      |  window_start_time  | window_secs | gen_rate | send_rate | apply_rate | spill_rate | stream_rate | eta_unsent | eta_total | send_stalled
-----------+--------------------+---------------------+-------------+----------+-----------+------------+------------+-------------+------------+-----------+--------------
 sub_a     | 2026-09-22 10:31:02 | 2026-09-22 10:29:00 |       120.0 |    58.35 |     41.20 |      39.87 |       0.00 |        2.10 |       0.02 |      5.37 | f
```

```sql
-- 订阅端·整体视图：两端合成（发布端免安装）
SELECT sub_name, remote_state, sample_time, last_remote_poll_time, window_secs,
       gen_rate, send_rate, recv_rate, apply_rate,
       pg_size_pretty(backlog_unsent)     AS unsent,
       pg_size_pretty(backlog_inflight)   AS inflight,
       pg_size_pretty(backlog_unapplied)  AS unapplied,
       pg_size_pretty(backlog_total)      AS total,
       pg_size_pretty(feedback_lag_bytes) AS fb_lag,
       write_lag, replay_lag, eta_total, send_stalled, apply_stalled
FROM pg_lrstat_overall;

 sub_name | remote_state |     sample_time     | last_remote_poll_time | window_secs | gen_rate | send_rate | recv_rate | apply_rate | unsent | inflight | unapplied | total | fb_lag | write_lag  | replay_lag | eta_total | send_stalled | apply_stalled
----------+--------------+---------------------+----------------------+-------------+----------+-----------+-----------+------------+--------+----------+-----------+-------+--------+------------+------------+-----------+--------------+---------------
 sub_a    | ok           | 2026-09-22 10:31:00 | 2026-09-22 10:31:01  |       120.0 |    58.35 |     41.20 |     41.18 |      39.87 | 12 MB  | 38 kB    | 85 MB     | 97 MB | 6 MB   | 00:00:01.2 | 00:00:04.8 |    2480.3 | f            | f
```

## 附录 D：v1.0 实现说明（contrib/pg_lrstat/）

代码已按本文档落地并验证（编译零警告、注入断言、真实逻辑槽采样、worker 稳定运行）。与设计稿的差异与实现要点：

1. **单库采样而非逐库遍历**（对 §5.1 的简化）：槽、订阅、walsender 统计、origin 进度均为集群级共享目录/共享内存，采样 worker 只连一个库（GUC `pg_lrstat.database`，默认 postgres）即可看到全部目标，无需 autovacuum 式逐库轮转。
2. **raw 单环实现**：v1 只实现 raw 环与 `*_history` 函数（`since` 参数过滤）；设计中的 coarse 降采样环与 `granularity` 参数列为后续版本。
3. **远端轮询为同步实现**：`PQexecParams` + 连接级 `statement_timeout` + 单轮总预算 + 指数退避，语义与设计的异步方案一致，30s 采样节拍下更简单安全。发布端侧只注入 `application_name='pg_lrstat'`、`connect_timeout`、`options=-c statement_timeout=…`，凭证只在内存中。
4. **视图列类型**：标识列用 `text`（非 `name`）；速率 MB/s、ETA 秒、积压 bigint，与 §3.1/§5.5 一致。
5. **测试注入口**：`pg_lrstat_inject_pub/rpub/sub`，受 `pg_lrstat.allow_inject`（SUSET）+ superuser 双重门控；TAP 001 用注入做速率算术的确定性断言（注意两次注入间隔需小于速率窗口，如 110s < 2min，否则旧样本出窗、速率为 NULL——这是窗口差分的语义而非缺陷），002 起真实发布/订阅对做端到端断言。
6. **PG18 适配点**（升级参考）：`pg_noreturn` 替代 `pg_attribute_noreturn`；`ProcessMainLoopInterrupts`；`AbortTransactionCommand` 已移除（`CommitTransactionCommand` 统一处理 aborted 状态）；bgworker 中 SPI 需 `SetCurrentStatementStartTimestamp + StartTransactionCommand + PushActiveSnapshot(GetTransactionSnapshot())` 三件套；`.so` 以 `-fvisibility=hidden` 编译，worker 入口必须 `PGDLLEXPORT` 导出；SPI 读 `name` 类型列必须先 `::text`，否则按 varlena 解引用会段错误。
7. **并行 apply 行合并**：SUB 目标以（订阅名, relid, apply/tablesync）为键，parallel apply worker 行与 leader 合并到同一目标，meta 的 pid 取最后采样值（§5.2 的已知简化）。
8. **构建与测试接线**：`Makefile`（REGRESS + `--temp-config pg_lrstat.conf`，见 pg_stat_statements 惯用法）与 `meson.build`（shared_module + libpq 依赖 + regress/tap 注册，`runningcheck: false`）双构建系统均验证通过；确定性回归测试 `sql/pg_lrstat.sql` + `expected/pg_lrstat.out` 覆盖速率算术、积压、阶段分解、整体合成、单样本 NULL、重置。测试验证报告见 `contrib/pg_lrstat/TEST_REPORT.md`。
9. **Review 修复轮（2026-09-23）**：新增 `pg_lrstat_info` 诊断视图（loaded/layout_version/配置/轮次健康/`dropped_samples` 丢弃计数）；shmem header 加布局版本与轮次诊断；EXEC_BACKEND（Windows）后端惰性附加；`WaitEventExtensionNew` 必须惰性注册（pre-load 阶段调用会段错误，已按 postgres_fdw 模式修复）；速率视图只拷贝窗口内样本并以 scratch 复用，overall 改用预构建 RPUB 快照；conninfo 全量持有不再截断；注入函数拒绝 NULL 参数；worker 轮次日志状态翻转 + 半小时限频；全部视图补 `COMMENT ON COLUMN`，pipeline 速率列更名为 `rate`。

