# pg_lrstat — 逻辑复制 LSN 与速率统计扩展 设计文档（v2.0 会话模型）

| 项 | 内容 |
| --- | --- |
| 文档版本 | v2.0（会话模型重构稿，替代 v1.x 环形历史设计） |
| 基线代码 | PostgreSQL 18 开发分支（本仓库，branch `cluster_rate`） |
| 状态 | 待评审；实现为 v2.0（不兼容 v1.x 视图，升级需 drop/recreate extension） |

---

## 1. 背景与目标

### 1.1 v1.x 的问题

v1.x 是**连续监控**模型：后台采样写入每目标 1800 点的环形缓冲，查询时在 `rate_window`（默认 2min）上做窗口差分。实践中暴露三类问题：

1. "2 分钟窗口平均"既不是瞬时也不是全程——排障时要的是"刚刚 30 秒怎么样"，压测要的是"这轮跑完平均多少"，窗口值两头不靠；
2. 环形历史带来复杂的环管理（回绕、新鲜度、半窗有效性、拷贝缩减），也使共享内存占用与查询成本线性于环长；
3. 观测目标常是**一段明确的测量期**（压测窗口、迁移演练、变更前后对比），连续历史的大部分时间并不被消费。

### 1.2 v2.0 模型：显式测量会话

```
lrstat_start()          采样中(30s/轮)  随时查询             lrstat_stop()
     │ ──── 会话开始 ────►│──────────────►│──── 会话结束 ────►│
     │                    │               │                   │
  记录锚点样本          逐轮采样        live 视图输出        冻结全部数据
  会话日志清零          增量入会话日志   瞬时速率 + 平均速率   report 视图输出
                                                          完整统计报告
```

- **`lrstat_start()`**：开启一个会话。清空会话存储，触发一轮即时采样作为**锚点**（anchor），开始计时。
- **采样期间随时查询**：每个目标输出**瞬时速率**（最近两个样本的差分，即一个采样间隔——默认 30 秒——内的速率）与**平均速率**（锚点到最新样本的全程差分）。没有窗口概念。
- **`lrstat_stop()`**：结束会话，冻结 start→stop 期间的全部逐间隔数据与统计，形成**会话报告**；报告保留到下一次 `lrstat_start()` 覆盖为止。

### 1.3 目标 / 非目标

- 目标：一次会话即一份完整的复制链路测试报告（总量、平均、峰值、逐间隔序列）；瞬时/平均双速率语义清晰；共享内存与查询成本显著低于环形模型；发布端/订阅端双侧视角与订阅端合成两端的整体视图（v1 的数据面全部保留）。
- 非目标：7×24 连续历史趋势（由监控端周期采 `live` 视图落库实现，见 §9）；跨集群拉取订阅端之外的数据；表级粒度。

## 2. 逻辑复制数据链路与观测点（自 v1 保持不变）

### 2.1 链路总图

```
发布端                                    订阅端
┌─────────────────────────────┐          ┌──────────────────────────────┐
│ WAL 生成(current_lsn)        │          │                              │
│   ▼                          │  TCP     │  接收(received_lsn)           │
│ 解码(XLogReader/输出插件)     ├─────────►│   │  (libpq 消息,不落本地WAL)   │
│   ▼                          │  反馈     │  应用(applied_lsn)            │
│ 发送(sent_lsn)               │◄─────────┤   │  (apply worker 写本地WAL)  │
│   │                          │ write/   │   ▼                          │
│   ▼                          │ flush/   │  反馈发送(send_feedback)       │
│ 确认(confirmed_flush_lsn)     │ apply    │                              │
│ 保水(restart_lsn)            │          │                              │
└─────────────────────────────┘          └──────────────────────────────┘
```

### 2.2 观测点与数据来源（本仓库已核实）

| # | 检查点 | 发布端来源 | 订阅端来源 | 语义 |
| --- | --- | --- | --- | --- |
| C0 | WAL 生成位点 | `pg_current_wal_lsn()` | —（订阅端本地另有 `pg_current_wal_lsn()`，含义不同） | 已写入 WAL 的最大位置 |
| C1 | 已解码位点 | 不可见（解码与发送在 walsender 单进程内串行耦合） | — | 已完成输出插件序列化的位置 |
| C2 | 已发送位点 | `pg_stat_replication.sent_lsn` | — | 已写入 libpq 发送缓冲并 flush 的位置 |
| C3 | 对端已接收 | `pg_stat_replication.write_lsn`（反馈 write=recvpos） | `pg_stat_subscription.received_lsn` / `latest_end_lsn`（实时） | 订阅端收到的流位置 |
| C4 | 对端已提交落盘 | `pg_stat_replication.flush_lsn`（反馈 flush） | — | 已 apply 且本地落盘的提交位点 |
| C5 | 对端已应用 | `pg_stat_replication.replay_lsn`（反馈 apply） | origin `remote_lsn` ∪ 反馈 flush 位（v18 origin 仅提交边界推进，取 max 合并） | 已应用的事务提交位点 |
| C6 | 安全水位（确认） | `pg_replication_slots.confirmed_flush_lsn` | — | WAL 回收决策位 |
| C7 | 槽保水位 | `pg_replication_slots.restart_lsn` | — | 为本槽保留的最早 WAL 位点 |
| D1 | 解码压力 | `pg_stat_replication_slots`：spill/stream/total_bytes | — | ReorderBuffer 溢写/流式计数 |

v1 迭代中固化的四条内核事实全部继续有效：反馈报文 write/flush/apply 三槽位语义（§附录 A）；逻辑流不落订阅端 WAL、`pg_stat_wal_receiver` 不覆盖逻辑订阅；反馈周期（默认 10s）造成的"假在途"用 `feedback_lag_bytes` 区分；v18 的 origin 命名 `pg_<subid>` 与提交边界推进特性、`worker_type` 的 `table synchronization`/`parallel apply` 拼写。

## 3. 指标体系（会话语义）

### 3.1 双速率定义

对任一速率字段（gen/send/recv/apply/spill/stream 等，分母均为字节、显示 MB/s）：

```
瞬时速率 instant = (S_last − S_prev) / (t_last − t_prev)
                  最近两个样本，即一个采样间隔（默认 30s）内的速率

平均速率 avg     = (S_last − S_anchor) / (t_last − t_anchor)
                  会话锚点（start 后首轮采样）到最新样本的全程速率
```

- 无窗口、无环回绕、无半窗有效性判定；瞬时速率的粒度完全由 `sample_interval` 决定（要 1s 粒度就调 1s）；
- 平均速率随会话推进自动"积分"，stop 时即为该字段的会话总结；
- `S_prev/S_last` 每轮滚动更新；`S_anchor` 会话内不变（目标中途加入时，锚点取它的首个样本，报告标注 `partial=true`）。

### 3.2 指标清单

**位点与积压（瞬时，同 v1）**：七个检查点原值；五段积压 unsent/inflight/unapplied/total/retained（最新样本两点位差）；`feedback_lag_bytes`。语义、公式、发布端/订阅端口径与 v1 完全一致（见 §附录 C 术语）。

**速率（双份）**：每个 v1 速率字段在 live 视图中同时给出 `*_instant` 与 `*_avg` 两列。

**会话报告专有（stop 后，由冻结的逐间隔日志计算）**：

| 指标 | 计算 | 说明 |
| --- | --- | --- |
| `duration_secs` | stop_ts − start_ts | 会话时长 |
| `total_*_bytes` | last − anchor | 各字段会话总量 |
| `avg_*` | total / duration | 会话平均（与 stop 时刻 live 的 avg 一致） |
| `min/max/avg(瞬时)` | 逐间隔日志统计 | 瞬时速率的分布：峰值、谷值、均值 |
| `peak_backlog_*` | 锚点起累积重建各水位，取逐间隔最大 | 峰值积压（水位可由 anchor+增量前缀和重建） |
| `stalled_intervals` | 瞬时速率为 0 且积压>0 的间隔数 | 停滞时长占比 |
| `first_seen_ts` | 目标首个样本时刻 | 目标晚于 start 加入时 < start_ts，`partial=true` |
| `truncated` | 会话日志超上限 | 超出 `session_max_samples` 后丢弃并标记 |

**ETA/停滞（live）**：`eta_*` 基于平均速率（比瞬时稳，符合"还要多久"的问法）；`*_stalled` 判定改用瞬时速率（最近 30s 没动）。

### 3.3 两端整体（overall，会话版）

v1 的双环合成（订阅端本地 + 轮询发布端）结构不变；`pg_lrstat_overall_live` 输出双端位点、双端双速率（instant/avg）、三段积压与总积压、`remote_state`；报告期 `pg_lrstat_overall_report` 给两端会话总结。

## 4. 总体架构

### 4.1 部署形态（同 v1）

发布端装 → 槽级视图；订阅端装 → worker 级视图 + 凭 `subconninfo` 轮询发布端合成两端视图（发布端免安装）；两端时钟偏差不进入任何计算（远端样本打订阅端本地时钟）。单 worker 连单库即可见全集群目标（槽/订阅/origin/统计均为集群级）。

### 4.2 共享内存布局（核心变化：目标槽 + 会话状态 + 会话日志）

```c
/* 会话状态（全局唯一） */
typedef struct LRSessionState
{
    int32       magic;
    int32       layout_version;
    uint64      session_id;         /* 每次 start 递增                    */
    bool        running;            /* start 后 true，stop 后 false        */
    TimestampTz start_ts;           /* start() 执行时刻                    */
    TimestampTz stop_ts;            /* stop() 执行时刻（冻结后有效）        */
    bool        truncated;          /* 会话日志超上限                      */
    slock_t     mutex;
} LRSessionState;

/* 每目标：锚点 + 前一样本 + 最新样本 + 元数据（环被三个槽位取代） */
typedef struct LRTargetCtl
{
    LRTargetKind kind;              /* PUB / SUB / RPUB                    */
    char        name[NAMEDATALEN];
    char        worker_char;        /* 'a' apply / 't' tablesync           */
    Oid         relid;
    bool        in_use;
    TimestampTz last_sample_ts;
    slock_t     mutex;
    LRSample    anchor;             /* 会话锚点（running 时不变）           */
    LRSample    prev, last;         /* 滚动窗口：最近两个样本               */
    LRTargetMeta meta;
} LRTargetCtl;

/* 会话日志：逐间隔增量，start 清零、stop 冻结（非环形，append-only） */
typedef struct LRInterval
{
    TimestampTz ts;                 /* 间隔结束时刻                         */
    int32       d_current, d_sent, d_received, d_applied,
                d_spill, d_stream;  /* 本间隔各字段增量（字节）             */
    /* 按目标交错存放，报告期按目标分组扫描 */
} LRInterval;
```

- **空间对比 v1**：每目标从 `头 + 1800×96B(≈173KB)` 降到 `头 + 3×96B + meta(≈1KB)`；新增全局会话日志 `session_max_samples × 目标数 × ~40B`（默认 2880×32×40B ≈ 3.7MB 上限）。默认配置总占用约为 v1 的 1/4，且可通过 `session_max_samples=0` 关闭日志（只留 start/stop 汇总所需的 anchor/last 与在线聚合）。
- 采样 worker、SPI 采样 SQL、远端轮询（libpqsrv + 反馈位回填 `bump_applied`、parallel 行去重、v18 origin 双命名 join 等 v1 修复）全部保持不变。

### 4.3 会话生命周期与状态机

```
idle ──lrstat_start()──► running ──lrstat_stop()──► stopped ──lrstat_start()──► running(...)
                           │  ▲                                              │
                           │  └──── 每采样轮：prev=last; last=新样本；        │
                           │         running 时计算增量 append 会话日志        │
                           └ postmaster 重启 → 回 idle（会话数据丢弃，报告清空）
```

- **start()**：superuser；`running=true` 则报错（一次一会话）。`session_id++`，清空会话日志与所有目标的 anchor/prev/last 与 `truncated`，记录 `start_ts`，**唤醒 worker 立即执行一轮采样**——该轮样本即锚点（保证 start 后一个 `sample_interval` 内即可查到双速率，早期查询 avg=瞬时段值并随会话增长）。
- **采样轮**（worker 主循环，每 `sample_interval`）：若 `running`，每目标 `prev=last, last=新样本`，并计算增量 append 会话日志（满则 `truncated=true` 并丢弃，限频 WARNING）；若 `stopped/idle` 只更新 `last`（供下次 start 前的 live 查看，速率列为 NULL）。
- **stop()**：superuser；`running=false`，记录 `stop_ts`，唤醒 worker 立即采一轮**收尾样本**（捕捉 stop 前最后一段增量），冻结会话日志；`pg_lrstat_report` 从冻结数据生成报告，保留至下次 start。
- **竞态**：start/stop 与采样轮通过 `LRSessionState.mutex` 与 worker latch 协调——start/stop 先置状态再唤醒，采样轮读写目标槽时持各自 spinlock，增量计算以"该轮看到的 running 状态"为准，最多造成首/末间隔并入或剔除一个采样周期，报告标注 `duration` 以实际样本区间为准。

## 5. 对外 SQL 接口

### 5.1 命令

```sql
lrstat_start() → (session_id, started_at)          -- superuser；重复 start 报错
lrstat_stop()  → (session_id, started_at, stopped_at, report_digest)
                                                    -- 冻结并返回摘要；无会话报错
```

### 5.2 视图

| 视图 | 何时有效 | 内容 |
| --- | --- | --- |
| `pg_lrstat_live` | 任意时刻；running 时速率列有效 | 每目标一行：位点、积压（瞬时）、`*_instant`/`*_avg` 双速率、`elapsed`、`*_stalled`、`session_id`。stopped 后显示冻结的末状态 |
| `pg_lrstat_overall_live` | 同上 | 两端合成（v1 overall 的会话版）：双端位点/积压 + 双端双速率 + `remote_state` |
| `pg_lrstat_pipeline_live` | 同上 | 四段分解（unsent/inflight/unapplied/retained），速率为双份 |
| `pg_lrstat_report` | stop 之后 | 每目标一行会话总结：§3.2 报告专有列全部；`pg_lrstat_overall_report` 为两端合成版 |
| `pg_lrstat_report_intervals` | stop 之后 | 冻结的逐间隔序列（目标名、ts、六字段增量、由前缀和重建的水位/积压）——v1 history 的会话版替代，画会话内曲线用 |
| `pg_lrstat_info` | 任意时刻 | loaded/layout_version/session 状态(started/running/stopped/session_id/truncated)/配置/轮次健康/丢弃计数 |

**v1 → v2 视图映射**：`pub_sample`+`pub_rate` → `live`（位点与双速率合并）；`*_history` → `report_intervals`（仅会话期内）；`pipeline`/`overall` → `*_live`；新增 `report`/`overall_report`。`pg_lrstat_reset()` 语义改为"强制回 idle 并清报告"。

### 5.3 典型用法

```sql
SELECT lrstat_start();                      -- 开始压测窗口
-- …运行业务负载，期间随时：
SELECT sub_name,
       round(send_instant::numeric,1) AS 发送_瞬时,
       round(send_avg::numeric,1)     AS 发送_平均,
       round(apply_avg::numeric,1)    AS 应用_平均,
       pg_size_pretty(backlog_total)  AS 总积压
FROM pg_lrstat_overall_live;
SELECT lrstat_stop();

-- 完整报告（会话期间所有统计）：
SELECT * FROM pg_lrstat_report WHERE name = 'sub_a';
SELECT * FROM pg_lrstat_overall_report;
-- 会话内曲线：
SELECT ts, pg_size_pretty(d_current) AS 每段生成 FROM pg_lrstat_report_intervals
WHERE name = 'sub_a' ORDER BY ts;
```

## 6. 详细设计要点

### 6.1 采样与差分

```
每轮（sample_interval，默认 30s，SIGHUP 可调，下限 1s）：
  1. SPI 快照发布端/订阅端（SQL 与 v1 相同，含 v1 全部修复）
  2. 对每目标 T：
     new = 本轮样本
     若 !in_use: 首次出现 → 若 running: anchor = prev = new（partial 标记）
     否则:
        若 running && last 有效: 增量 d = new − last → append 会话日志
        prev = last; last = new
  3. 远端轮询（预算/退避同 v1）+ 反馈位回填 applied（bump_applied 作用于 last）
```

- 瞬时速率 = 最后一行日志增量 ÷ 其时间跨度（查询时算，不预存）；
- 平均速率 = last − anchor ÷ 时间跨度（同上）；
- stop 报告扫描冻结日志一次性聚合（O(间隔数)，≤2880×目标数，毫秒级）。

### 6.2 GUC

| GUC | 默认 | 说明 |
| --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | 采样周期 = **瞬时速率的积分区间**（下限 1s） |
| `pg_lrstat.session_max_samples` | `2880` | 会话日志容量（30s 间隔 ≈ 24h；0=关闭逐间隔日志，仅保留汇总） |
| `pg_lrstat.max_targets` | `32` | 目标槽数（重启生效） |
| `pg_lrstat.stale_target_ttl` | `10min` | 目标老化回收 |
| `pg_lrstat.eta_min_rate` | `1kB/s` | ETA 有效性下限（作用于 avg） |
| `pg_lrstat.remote_poll / remote_connect_timeout / remote_poll_budget` | on / `5s` / `500ms` | 远端轮询（同 v1） |
| `pg_lrstat.database` | `postgres` | 采样 worker 连接库（重启生效） |
| `pg_lrstat.allow_inject` | off | 测试注入口（superuser + 前缀约束，注入要求 running 会话） |

**移除**：`rate_window`、`raw_history_samples`（模型不再需要）。

### 6.3 内存安全与并发（承接 v1 修复清单）

单写者 worker + 每目标 spinlock；字符串一律 `SPI_getvalue()`（detoast+副本）；分配用 `palloc` 首分配、`repalloc` 仅对已分配块；会话日志 append 在 `LRSessionState.mutex` 下做越界判定（满则 truncated）；EXEC_BACKEND 惰性附加与 `pg_noreturn`/wait event 惰性注册等 v1 全部适配点保持。

### 6.4 权限

视图授予 `pg_monitor`；`lrstat_start/stop/reset` 与 inject 仅 superuser。

## 7. 测试方案

| 层 | 内容 |
| --- | --- |
| 回归（pg_regress，temp-config preload） | 注入驱动：start 后注入两样本 → 瞬时=末间隔差分、avg=全程差分的精确断言；单间隔时瞬时=avg；stop 后报告 total/min/max/峰值与手算一致；重复 start/无会话 stop 报错；报告保留至下次 start |
| TAP 001 | 同上注入算术（会话版） |
| TAP 002 / scripts/logical_rep_test.sh | 真实发布订阅：start → pgbench -T N → 期间断言 live 双速率非零且 instant 波动、avg 单调收敛 → stop → 断言 report 的 duration/total/avg 与 pgbench 产出的 WAL 量级一致、intervals 行数 ≈ N/30 |
| 会话边界 | 目标中途加入（会话中建订阅）partial 标记；stop 恰逢采样轮；重启回 idle 清报告 |

## 8. 实施计划

| 阶段 | 交付 |
| --- | --- |
| P0 | 会话状态机 + start/stop + 三槽位采样 + live 视图（双速率） |
| P1 | 会话日志 + report/intervals 视图 + overall_live/report + 回归/TAP 重写 |
| P2 | 迁移工具：v1 部署的视图对照说明；文档（USER_MANUAL/BEST_PRACTICES 会话化改写） |

## 9. 与连续监控的关系（趋势怎么办）

会话模型不保存跨会话历史；7×24 趋势的推荐做法：监控端周期采集 `pg_lrstat_live`（其 `*_avg` 在长会话中即滚动均值；或周期性 start/stop 形成固定窗口序列）落外部时序库。`session_max_samples=0` + 常驻长会话 + 只读 live，等价于"极低开销的持续均值探针"。

---

## 附录 A：术语（沿袭 v1）

LSN、反馈报文三槽位（write=接收位/flush=已提交落盘/apply=已应用）、confirmed_flush_lsn、retained WAL、spill/stream、解码膨胀率——定义与来源见 v1 文档附录 A，本版不再重复。

## 附录 B：采样 SQL

与 v1 完全相同（发布端/订阅端/远端轮询三条，含全部 v1 修复：`::text` 转换、origin 双命名、`pg_stat_wal_receiver` 不适用于逻辑订阅等），见实现文件 `lrstat_worker.c` / `lrstat_remote.c` 与 USER_MANUAL §6 血缘表。

## 附录 C：v1 → v2 指标映射

| v1 | v2 |
| --- | --- |
| `*_rate`（rate_window 窗口差分） | `*_instant`（末间隔）+ `*_avg`（会话全程） |
| `*_sample` 视图 | 并入 `live`（位点与积压不变） |
| `*_history`（环形，15h） | `report_intervals`（会话期内逐间隔，stop 冻结） |
| 无 | `report`：total/avg/min/max/峰值积压/停滞间隔数等会话总结 |
| `pg_lrstat_info` | 增加 session 状态；移除 ring_len，改列 session_max_samples |
