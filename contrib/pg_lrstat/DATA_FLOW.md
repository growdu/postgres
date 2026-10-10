# pg_lrstat 数据流说明（表记录内容 · 计算公式 · 生命周期 · 导出）

本文从代码逐行梳理：每张表记录什么、数字怎么算出来、数据何时写入/冻结/清理、导出报告包含什么。所有公式与 `lrstat_sql.c` / `lrstat_export.c` 的实现一一对应。

## 1. 数据流全景

```
发布端系统视图                     订阅端系统视图
(pg_replication_slots 等)         (pg_subscription 等)
        │                                │
        │ ②远端轮询 REMOTE_SQL            │ ①本地采样 RECV_SQL / SEND_SQL
        │ (libpq 连回发布端,              │ (worker SPI, 每 sample_interval)
        │  按槽名查一行)                  │
        ▼                                ▼
   ┌────────────── RSEND 镜像目标 ──────────────┐
   │  反馈 apply 位(replay_lsn) ──折入──► RECV 目标 │
   └──────────────────┬─────────────────────────┘
                      ▼  ③历史记录（唯一数据源, 环形）
              LRHistoryEntry[] = { ts, kind, session_id,
                10 个 LSN + spill/stream + 每轮状态 }
                      │
        ┌─────────────┼──────────────┐
        ▼             ▼              ▼
   6 个 SQL 视图   lrstat_export   (冻结在共享内存,
  (查询时现算)     (写报告文件)     下一次 start 清空)
```

## 2. 每张表记录什么、公式是什么

### pg_lrstat_send_history / recv_history —— 原始样本（唯一数据源）

每轮每目标一行**直接采样值**，无任何计算。来源列映射：

| history 列 | 来源（系统视图列） |
| --- | --- |
| `current_lsn` | 发布端 `pg_current_wal_lsn()`（RSEND 轮询） |
| `sent_lsn` | 发布端 `pg_stat_replication.sent_lsn` |
| `peer_recv/flush/applied_lsn` | 发布端 walsender 的 `write/flush/replay_lsn`（= 接收端反馈三水位） |
| `confirmed_flush_lsn` | `pg_replication_slots.confirmed_flush_lsn` |
| `restart_lsn` | `pg_replication_slots.restart_lsn` |
| `spill/stream_bytes` | `pg_stat_replication_slots` 解码计数（累计值） |
| `received_lsn` | 订阅端 `pg_stat_subscription.received_lsn` |
| `applied_lsn` | **单调融合**: GREATEST(origin.remote_lsn, 反馈 apply 位) —— 只在提交边界推进 |
| `local_wal_lsn` | 订阅端 `pg_current_wal_lsn()` |
| state/wal_status/worker_type/pid/lag 等 | 同轮采样直存 |

### pg_lrstat_send_stat / recv_stat —— 逐轮时序（查询时现算）

原始行直出 + 相邻同目标样本差分：

```
*_mbps(i) = lsn_diff(LSN_i, LSN_{i-1}) / 1048576 / (ts_i − ts_{i-1})
```
- 首行无前样本 → NULL；位置不推进 → 0.000
- `spill_mb` 同式但不除时间（增量 MB）

积压（每行按当轮水位差）：
```
backlog_unsent     = current_lsn − sent_lsn
backlog_inflight   = sent_lsn − peer_recv_lsn        (send 侧)
                   = sent_lsn − received_lsn          (cluster 侧)
backlog_peer_unapplied = peer_recv_lsn − peer_applied_lsn
backlog_total      = current_lsn − peer_applied_lsn   (send 侧)
                   = current_lsn − applied_lsn        (cluster 侧)
retained_wal       = current_lsn − restart_lsn
```
- send_stat 的 `apply_mbps` = 反馈 apply 位（peer_applied_lsn）的差分速率
- `*_blocked` = 有对应积压 > 0 且该速率 < 0.001

### pg_lrstat_cluster_stat —— 两端合成逐轮

每个 recv 行配对**时间最近的** RSEND 行（双游标单调前进），输出两侧水位 +
上方全部积压公式（用 cluster 侧变体）+ 四个 `*_mbps`（两侧各自的逐轮差分）+：

```
catchup_send_secs  = backlog_unsent / send_mbps
catchup_total_secs = backlog_unsent / send_mbps
                     + (inflight + unapplied) / apply_mbps
bottleneck = 'send'       若 send_mbps < gen_mbps 且 unsent > total/2
           | 'recv_apply' 若 unapplied > total/2
           | 'network'    若 inflight > total/2
           | 'none'
```

### pg_lrstat_info —— 会话与 worker 健康

会话状态（start/stop 写）、`nrounds`（会话内轮数：start 清零、每采样轮 +1、stop 冻结）、
`last_round_ts/ok`（worker 存活信号，空闲也推进）、`exported_report_names`（exports 目录文件）。

## 3. 数据生命周期

| 数据 | 写入 | 更新 | 冻结 | 清理 |
| --- | --- | --- | --- | --- |
| 历史环形 | 每采样轮 append（仅 start→stop 间） | 不改（append-only） | stop 后不再写入 | ① 环满覆盖最旧（truncated=true）② 下一次 start 清空 ③ 实例重启归零 |
| 目标三槽 | 每采样轮轮转 | 反馈 apply 位折入 last | stop 后不动 | start 清空 / 重启 |
| 会话状态 | start/stop 各写一次 | — | — | start 覆盖 |
| nrounds | start 清零 | 每采样轮 +1 | stop 冻结 | start 清零 |
| last_round_* | worker 每轮 | 总是推进 | 永不（健康信号） | 重启归零 |
| 视图 | — | — | — | 随历史环形 |
| **报告文件** | lrstat_export 调用时 | 每次 export 覆盖 | — | **不随 start/stop 清理**——手动删除；重启后仍在 |

## 4. 导出报告包含什么

`lrstat_export(name, format)` → 写 `$PGDATA/pg_lrstat/exports/<name>.<format>`。

数据源：当前会话（不传 name）或环形内的历史会话（传 sess_N）；被清空的会话报错。
**全部数字从历史现算，与视图同源同公式**：

| 报告区块 | 内容 | 公式 |
| --- | --- | --- |
| analysis | 瓶颈 + 四速率 avg + 三段积压 | `avg = lsn_diff(末有效LSN, 首有效LSN) / 1048576 / (末ts − 首ts)`（首末均取含有效位置的样本） |
| capacity | 追平 + 50/100/200G | 两段式追平（同 cluster）；`sync_50g_secs = 51200 / apply_avg`，100G/200G 严格 2×/4× |
| send_stat/recv_stat (JSON) | 每目标会话平均 + 最新 LSN | 同 analysis 公式，按目标 |
| Targets (HTML) | 同上（可视化行） | 同上 |
| Evidence（四表） | send/recv 原始样本 + send/recv 逐间隔速率 | 速率 = 相邻样本差分（与 stat 视图同式）——报告头部的每个数字都能从此手工重算 |
