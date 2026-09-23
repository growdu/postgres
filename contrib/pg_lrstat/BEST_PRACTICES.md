# 逻辑复制性能分析最佳实践（pg_lrstat）

配套文档：[USER_MANUAL.md](USER_MANUAL.md)（视图与配置）、[DESIGN.md](DESIGN.md) §6（方法论来源）。
本文是操作手册：所有 SQL 可直接粘贴，按"巡检 → 定位 → 下钻 → 定论"组织。

---

## 0. 黄金法则

1. **先定位段，再谈原因**：`backlog_total` 在 unsent / inflight / unapplied 哪一段，答案完全不同。
2. **速率判据要持续成立**：所有结论基于"2min 窗口持续"的读数，单点毛刺不下结论。
3. **时间只用反馈 lag 列**：`write_lag/flush_lag/replay_lag` 由内核按反馈时间戳计算，可跨机；绝不拿两端 `now()` 相减。
4. **字节换算走 LSN 位点**：速率列是 WAL 位点等效速率；要对比网络带宽时用解码膨胀率换算（见 §3.1）。

## 1. 三步定位法

```
① backlog_total 超阈值且持续增长？
├─ 否 → 稳态：转 §5 容量评估（余量 = gen − min(send, apply)）
└─ 是 → ② 积压集中在哪段（overall 视图三列占比）？
    ├─ unsent 占大头     → 发布端问题     → §3.1
    ├─ inflight 占大头   → 网络或反馈     → §3.2
    └─ unapplied 占大头  → 订阅端应用     → §3.3
```

速率关系判定表（判据须持续成立）：

| 速率关系（持续） | 结论 |
| --- | --- |
| `send_rate < gen_rate` 且 unsent 增长 | 发布端发送/解码吞吐不足 |
| `send_rate ≈ gen_rate` 且 total 稳定非零 | 发送已尽力，短板在网络或对端 |
| `recv_rate ≈ send_rate` 且 inflight 小 | 网络不是瓶颈 |
| `apply_rate < recv_rate` 且 unapplied 增长 | 订阅端应用是短板 |
| 三者接近且 total → 0 | 全链路健康 |

## 2. 每日巡检（5 条 SQL）

```sql
-- ① 健康自检：一次看完扩展状态
SELECT loaded, last_round_ok, dropped_samples, nrounds FROM pg_lrstat_info;

-- ② 订阅端全景（两端合成；发布端零安装也可用）
SELECT sub_name, remote_state,
       round(gen_rate::numeric,1)   AS gen_mb,
       round(send_rate::numeric,1)  AS send_mb,
       round(apply_rate::numeric,1) AS apply_mb,
       pg_size_pretty(backlog_total) AS total,
       round(eta_total::numeric)     AS eta_s,
       send_stalled, apply_stalled
FROM pg_lrstat_overall
ORDER BY backlog_total DESC;

-- ③ 停滞告警源（接监控直接用）
SELECT sub_name FROM pg_lrstat_overall
WHERE send_stalled OR apply_stalled;

-- ④ 发布端 WAL 风险（槽拖爆磁盘倒计时）
SELECT slot_name, wal_status,
       pg_size_pretty(retained_wal)      AS retained,
       pg_size_pretty(safe_wal_size)     AS safe_margin,
       replay_lag
FROM pg_lrstat_pub_sample
ORDER BY retained_wal DESC;

-- ⑤ 解码压力（大事务特征）
SELECT slot_name, spill_bytes, stream_bytes, total_bytes
FROM pg_lrstat_pub_sample
WHERE spill_bytes > 0 OR stream_bytes > 0;
```

## 3. 分段下钻

### 3.1 发布端（unsent 段）

```sql
SELECT slot_name, state, sync_state,
       round(gen_rate::numeric,1)  AS gen_mb,
       round(send_rate::numeric,1) AS send_mb,
       round(spill_rate::numeric,2) AS spill_mb,
       round(stream_rate::numeric,2) AS stream_mb,
       pg_size_pretty(backlog_unsent) AS unsent
FROM pg_lrstat_pub_sample s JOIN pg_lrstat_pub_rate r USING (slot_name);
```

- **解码压力**：`spill_rate/stream_rate > 0` = ReorderBuffer 溢写/流式落盘，大事务解码被磁盘拖累。长期高 spill → 控制发布端事务大小（批量提交）或增大逻辑解码内存（`logical_decoding_work_mem`）。
- **解码膨胀率**（输出协议字节 ÷ 输入 WAL 字节）。history 视图暂未含 `total_bytes`，用两次快照计算（监控端可周期抓取同一查询）：

```sql
-- 抓两次（间隔 ≥ 一个采样周期），膨胀率 = Δtotal_bytes / Δcurrent_lsn：
SELECT now(), slot_name, current_lsn, total_bytes
FROM pg_lrstat_pub_sample;

-- 得到的是"每字节 WAL 产生的协议字节数"。它同时用于把速率换算成
-- 线上字节速率：线上速率 ≈ gen_rate × 膨胀率，才能与 NIC 带宽对照
-- （send_rate 本身是 LSN 等效速率，不能直接比带宽）。
```

- **同步复制干扰**：`sync_state` 非 async 且 state 显示等待 → walsender 被同步复制拖住，是等待不是吞吐问题。
- **多订阅共担**：多个槽的 unsent 同步增长而 `gen_rate` 正常 → 发布端 CPU/IO 是共享瓶颈（WAL 读取与解码按槽重复发生）。

### 3.2 网络与反馈（inflight 段）

```sql
SELECT sub_name,
       pg_size_pretty(backlog_inflight)   AS inflight,
       pg_size_pretty(feedback_lag_bytes) AS fb_lag,
       write_lag, replay_lag
FROM pg_lrstat_overall;
```

- **先排除假在途**：`inflight ≈ feedback_lag_bytes` → 是反馈周期（内核默认 10s）的观测滞后，不是网络问题。
- **真在途持续增长**：用解码膨胀率把 `send_rate` 换算成线上字节速率与带宽对照；高 RTT 链路特征是 inflight 稳定在 **带宽×RTT（BDP）** 量级后不再增长——此时扩带宽无效，需提高并发流或压缩。

### 3.3 订阅端（unapplied 段）

```sql
SELECT sub_name, worker_type, worker_pid, leader_pid,
       round(recv_rate::numeric,1)  AS recv_mb,
       round(apply_rate::numeric,1) AS apply_mb,
       round(local_wal_rate::numeric,1) AS local_wal_mb,
       pg_size_pretty(backlog_apply) AS unapplied,
       apply_stalled
FROM pg_lrstat_sub_rate;
```

- **apply_stalled = t**：查 `pg_stat_activity` 中该 worker_pid 的 `wait_event`（锁等待最常见：订阅端长事务/DDL 与回放冲突）；查 `pg_stat_subscription_stats.apply_error_count` 增速（冲突重试）。
- **吞吐不足但无停滞**：单 apply worker 串行回放是常态瓶颈——大事务、行级触发器、索引维护、外键检查。核对 `streaming` 设置与并行 worker 是否真启动（`leader_pid` 非空）。
- **存储瓶颈**：`local_wal_rate` 与 `apply_rate` 严重不成比例，或订阅端磁盘写饱和（`pg_stat_io`）。
- **初始同步期**：存在 `worker_type='tablesync'` 行时 unapplied 增长属正常，按 relid 行单独评估。

## 4. 故障模式 → 指标签名速查

| 模式 | 签名（组合特征） | 第一动作 |
| --- | --- | --- |
| 大事务回放 | spill/stream 飙升；unsent 先增后骤降；apply_rate 短暂归零 | 等待 + 溯源大事务，评估拆批 |
| 网络抖动 | inflight 尖峰 + feedback_lag_bytes 波动；send_rate 突降恢复 | 抓包/看链路，勿动数据库 |
| 订阅端锁冲突 | apply_stalled 且 wait_event 为锁；unapplied 阶梯增长 | `pg_blocking_pids()` 找阻塞源 |
| 订阅暂停/断开 | 槽 active=f 或 send_stalled；retained_wal 线性增长；wal_status 沿 reserved→unreserved→lost 恶化 | 恢复订阅或drop槽，重点盯 wal_status=unreserved |
| 发布端写入洪峰 | gen_rate 突增，积压沿 unsent→inflight→unapplied 依次传导 | 各段到达时间差即各段延迟，评估是否限流 |
| 反馈滞后误报 | inflight ≈ feedback_lag_bytes 且 applied_lsn 持续推进 | 无需处理，读 overall 而非发布端视图 |
| 目标槽满 | pg_lrstat_info.dropped_samples 增长 + 新槽/订阅不出现在视图 | 调大 max_targets 重启 |

## 5. 容量规划

```sql
-- 净追平速率：为负则永远追不平，先扩容短板侧
SELECT sub_name,
       round(least(send_rate, apply_rate)::numeric,1) AS drain_mb,
       round(gen_rate::numeric,1)                      AS gen_mb,
       round((least(send_rate, apply_rate) - gen_rate)::numeric,1) AS net_mb,
       round((backlog_total / nullif(least(send_rate,apply_rate)*1048576,0))::numeric) AS eta_if_gen_stops_s
FROM pg_lrstat_overall;

-- 槽无推进期间，retained WAL 增速 ≈ WAL 生成速率。取 history 最近 10 分钟
-- 相邻样本的增量，磁盘可用空间 ÷ 平均增速 = 拖爆倒计时：
WITH d AS (
  SELECT current_lsn - lag(current_lsn) OVER (ORDER BY ts) AS wal_bytes,
         extract(epoch FROM (ts - lag(ts) OVER (ORDER BY ts))) AS secs
  FROM pg_lrstat_pub_history('mysub', now() - interval '10 minutes')
)
SELECT round(avg(wal_bytes / nullif(secs,0))::numeric) AS retained_bps
FROM d;
```

实用做法：取 `pg_lrstat_pub_history` 中 retained_wal 两个时刻的差 ÷ 时间得增速，`磁盘可用 / 增速` = 剩余小时数；同时用 `wal_status`/`safe_wal_size` 与 `max_slot_wal_keep_size` 设告警。

**稳态余量**：backlog→0 时的 `gen_rate − min(send_rate, apply_rate)` 即当前余量；用 history 长序列回归"余量 vs 负载"，按峰值外推。

## 6. 场景剧本

**A. 迁移追平监控**：割接前持续看 `overall.eta_total`；用净追平速率重估（持续写入下 eta_total 会骗人，见 §5）；backlog_total 归零且三速率同频后，再叠加一个静默窗口验证零漂移，方可割接。

**B. 订阅暂停/恢复**：暂停期间盯发布端 `retained_wal` 增速与 `wal_status`（unreserved 即亮红灯）；恢复后看 `send_stalled→f`、eta_total 递减至 0。

**C. 大事务窗口**：事前预估 `spill_rate` 将转正；事中允许 apply_rate 短暂为 0（区分 stalled：`apply_stalled` 为 f）；事后确认 unsent 骤降且 history 无持久台阶。

**D. 发布端瓶颈确认**：多槽 unsent 同步增长 + gen_rate 正常 + OS 侧 walsender CPU 饱和 → 解码 CPU 是共享瓶颈；单槽场景对比 send_rate 与解码膨胀率换算的线上速率定位 CPU/网络谁是短板。

## 7. 窗口与粒度选择

- 默认（30s 采样 / 2min 窗口）：容量评估、趋势、常规巡检——**不要**用它看秒级毛刺。
- 实时排障：临时调小 `sample_interval=1s, rate_window=4s`（窗口 ≥ 2×间隔；SIGHUP 生效），用完调回，避免放大采样开销。
- 历史回溯：`*_history` 按 `since` 过滤；30s 粒度 1800 点 ≈ 15 小时，更长的趋势落监控端存储。

## 8. 陷阱清单

1. `backlog_inflight` 在发布端视图里天然含反馈"假在途"，判断网络问题先看 `feedback_lag_bytes`。
2. 单 apply worker 回放大事务期间速率归零 ≠ 故障；以 `apply_stalled` 与 wait_event 为准。
3. `local_wal_rate` 含订阅库其他写入（analyze/vacuum/用户写），专用订阅库才可当"应用产生的 WAL"读。
4. progress 类百分比是 LSN 比值近似，FSFP 重放等场景偏差大，只作参考。
5. `eta_total` 假设速率持续；持续写入场景必须用净追平速率重估。
6. 跨机时间比较只用反馈 lag 列；`*_time` 列只用于各自端内的时序。
