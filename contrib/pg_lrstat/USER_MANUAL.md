# pg_lrstat 用户使用手册

版本：v1.0（对应扩展 1.0）｜配套文档：[DESIGN.md](DESIGN.md)（设计）、[BEST_PRACTICES.md](BEST_PRACTICES.md)（性能分析实践）、[TEST_REPORT.md](TEST_REPORT.md)（测试验证）

---

## 1. 这是什么

pg_lrstat 是一个逻辑复制 LSN 与速率统计扩展：一个后台采样 worker 周期性快照发布端与订阅端的系统视图，写入共享内存环形历史；SQL 视图在查询时做窗口差分，直接给出**速率（MB/s）、积压（bytes）、追平预估（秒）**，无需外部采样器。

部署形态（同一扩展，装哪端就有哪端的数据）：

```
发布端安装  →  pg_lrstat_pub_sample / pub_rate / pipeline / pub_history
订阅端安装  →  pg_lrstat_sub_sample / sub_rate / sub_history
                pg_lrstat_overall（凭订阅 conninfo 轮询发布端，两端合成，
                发布端无需安装任何东西）
两端都装    →  发布端精确视图 + 订阅端实时视图，互为补充
```

链路与检查点（手册通用的编号，来自设计文档 §2.2）：

```
WAL生成 C0 ─► 发送 C2 ─► 网络 ─► 订阅端接收 C3' ─► 应用 C5' ─► 反馈 ─► 确认 C6
                    └─► 对端已收 C3(反馈) ─► 对端已应用 C5(反馈)      保水 C7
```

## 2. 安装

### 2.1 构建

```sh
# 方式一：随 PG 源码树（本仓库）
./configure ... && make -j8
make -C contrib/pg_lrstat install

# 方式二：meson
meson setup bm && ninja -C bm contrib/pg_lrstat/pg_lrstat.dylib
ninja -C bm install        # 或指定 target
```

### 2.2 启用

```ini
# postgresql.conf（发布端、订阅端各自加）
shared_preload_libraries = 'pg_lrstat'
```

重启后：

```sql
CREATE EXTENSION pg_lrstat;

-- 第一步永远是看它：
SELECT * FROM pg_lrstat_info;
```

`pg_lrstat_info.loaded` 为 `t` 即就绪；为 `f` 说明没走 preload（所有数据视图为空），先检查 `shared_preload_libraries` 与重启。

### 2.3 权限

视图授予 `pg_monitor`（其余角色默认无权限）；`pg_lrstat_reset()` 与 `pg_lrstat_inject_*` 仅超级用户。远端轮询复用订阅自身的连接串，不引入新凭证面，`subconninfo` 绝不出现在任何视图或日志中。

## 3. 配置（GUC）

| GUC | 默认 | 作用域 | 说明 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期（下限 1s）；远端轮询同节拍 |
| `pg_lrstat.rate_window` | `2min` | SIGHUP | 速率差分窗口，需 ≥ 2×采样间隔 |
| `pg_lrstat.max_targets` | `32` | 重启 | 目标槽位数（槽×端 + 订阅 worker 数），见 `pg_lrstat_info.dropped_samples` |
| `pg_lrstat.raw_history_samples` | `1800` | 重启 | 每目标环长（30s 采样≈15 小时） |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标多久不更新才允许被新目标复用 |
| `pg_lrstat.eta_min_rate` | `1kB/s` | SIGHUP | ETA 有效性下限（低于此速率 ETA 置 NULL） |
| `pg_lrstat.remote_poll` | `on` | SIGHUP | 订阅端是否轮询发布端（off 则 overall 只有本地列） |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 远端连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮远端轮询总预算（超时置 stale，不拖慢本地采样） |
| `pg_lrstat.database` | `postgres` | 重启 | 采样 worker 连接的库（任意库皆可：底层目录/统计是集群级） |
| `pg_lrstat.allow_inject` | `off` | SUSET | 测试注入口开关（配合 `inject_name_prefix`） |
| `pg_lrstat.inject_name_prefix` | `''` | SUSET | 注入目标名前缀约束（建议测试环境设 `pg_lrstat_test_`） |

改 SIGHUP 项后 `SELECT pg_reload_conf();` 即生效（下一采样轮起）。

## 4. 快速开始

**发布端：这个槽健康吗？**

```sql
SELECT slot_name, state, wal_status,
       round(gen_rate::numeric,1)  AS gen_mb,
       round(send_rate::numeric,1) AS send_mb,
       pg_size_pretty(backlog_unsent) AS unsent,
       pg_size_pretty(retained_wal)  AS retained,
       eta_unsent, send_stalled
FROM pg_lrstat_pub_sample s JOIN pg_lrstat_pub_rate r USING (slot_name);
```

**订阅端：两端合成一张图（发布端零安装）**

```sql
SELECT sub_name, remote_state,
       round(gen_rate::numeric,1)  AS gen_mb,
       round(send_rate::numeric,1) AS send_mb,
       round(apply_rate::numeric,1) AS apply_mb,
       pg_size_pretty(backlog_unsent)    AS unsent,
       pg_size_pretty(backlog_inflight)  AS inflight,
       pg_size_pretty(backlog_unapplied) AS unapplied,
       pg_size_pretty(backlog_total)     AS total,
       round(eta_total::numeric)         AS eta_s
FROM pg_lrstat_overall;
```

**画趋势曲线（喂给监控）**

```sql
SELECT ts, current_lsn, sent_lsn, peer_applied_lsn
FROM pg_lrstat_pub_history('mysub', now() - interval '1 hour');
```

## 5. 视图详解

单位约定：**速率 MB/s（1MB=1048576B）**，**积压/字节类列 bytes**（可直接 `pg_size_pretty`），**eta_* 秒**。每个视图的每一列都有 `COMMENT ON COLUMN`，psql 里 `\d+ pg_lrstat_overall` 可看全部释义。

### 5.1 `pg_lrstat_info` — 诊断（先看这个）

一行：`loaded / layout_version / ntargets / ring_len / sample_interval_ms / rate_window_ms / max_targets / remote_poll / last_round_ts / last_round_ok / last_round_error / nrounds / dropped_samples`。

### 5.2 `pg_lrstat_pub_sample` — 发布端采样表（一逻辑槽一行）

最新样本的瞬时事实：32 列 = 标识（slot/database/plugin/temporary/active/sender_pid/application_name/client_addr/state/sync_state）+ 7 个 LSN 检查点（current/sent/peer_recv/peer_flush/peer_applied/confirmed_flush/restart）+ wal_status/safe_wal_size + 解码计数器（spill/stream/total_bytes）+ 5 个积压 + 3 个反馈 lag + reply_time。

### 5.3 `pg_lrstat_pub_rate` — 发布端速率表

`rate_time` + 实际差分窗口三件套（`window_start_time/window_end_time/window_secs`）+ gen/send/peer_apply/confirm/spill/stream 六速率 + `eta_unsent/eta_total` + `send_stalled`。

### 5.4 `pg_lrstat_sub_sample` / `pg_lrstat_sub_rate` — 订阅端

按 worker 一行（apply/tablesync，`leader_pid` 区分并行）：接收/应用/本地 WAL 检查点、`backlog_apply`；速率表给 recv/apply/local_wal 三速率与 `eta_apply`、`apply_stalled`。

### 5.5 `pg_lrstat_pipeline` — 发布端阶段分解（一槽四行）

`stage ∈ {unsent, inflight, peer_unapplied, retained}`，每行带该段积压、排空速率与反馈时间 lag——画堆叠柱状图直接用。

### 5.6 `pg_lrstat_overall` — 订阅端整体视图（一订阅一行，34 列）

发布端列来自远端轮询、订阅端列来自本地采样；`remote_state ∈ {ok, unreachable, stale, n/a}`；含 `feedback_lag_bytes`（本地实收 − 反馈接收位，量化反馈滞后）。

### 5.7 `pg_lrstat_pub_history / sub_history` — 原始时间序列

`(name, since)` 两个可选过滤参数；按时间升序返回环内原始样本。

## 6. 运维操作

```sql
-- 清空所有目标与历史（superuser）
SELECT pg_lrstat_reset();

-- 目标不够用（dropped_samples 增长 / 新槽不出现）：
--   postgresql.conf: pg_lrstat.max_targets = 64 → 重启

-- 测试注入（superuser + allow_inject=on，生产勿开）
SET pg_lrstat.allow_inject = on;
SELECT pg_lrstat_inject_pub('pg_lrstat_test_t', now(), '0/1000000', '0/800000',
                            '0/400000', '0/400000', '0/200000');
```

## 7. 故障排查

| 现象 | 看 | 结论/处理 |
| --- | --- | --- |
| 所有视图空 | `pg_lrstat_info.loaded` | `f` → 未 preload；`t` → 看 `last_round_ok` |
| `loaded=t` 但无数据 | `last_round_ok/last_round_error` | 采样轮失败（如权限/视图变更），看 error 文本；失败日志仅状态翻转时记一条 |
| `dropped_samples > 0` | `pg_lrstat_info` | 目标槽满，调大 `max_targets` 重启 |
| `remote_state='unreachable'` | `pg_lrstat_overall` | 发布端连不上：连接串/防火墙/发布端 down（指数退避重连） |
| `remote_state='stale'` | 同上 | 槽不存在（订阅新建未同步）或单轮预算耗尽（订阅数多时调大 `remote_poll_budget`） |
| `remote_state='n/a'` | 同上 | 无远端数据（`remote_poll=off` 或首轮未完成） |
| 速率列全 NULL | `window_secs` | 采样中断/刚启动：最新样本距 now > 3×interval，或窗口跨度不足半程 |

## 8. 已知限制（读前必知）

1. **单库采样**：worker 只连一个库；因槽/订阅/walsender 统计均为集群级，通常无感。跨库特殊场景见设计文档附录 D。
2. **反馈延迟**：发布端视图的对端位置（C3~C5）滞后一个 `wal_receiver_status_interval`（默认 10s）；订阅端视图无此延迟——这是两端都装的价值。
3. `backlog_inflight`（发布端口径）含反馈滞后造成的"假在途"；`pg_lrstat_overall` 用本地接收位计算，无此失真。
4. `local_wal_rate` 含订阅库自身其他写入的噪声。
5. 大事务回放期间 `apply_rate` 可能短暂为 0（单事务内无 commit 位点推进），`apply_stalled` 辅助区分。
6. `pg_lrstat_pipeline` 的 progress 为 LSN 比值近似，不是精确百分比。
