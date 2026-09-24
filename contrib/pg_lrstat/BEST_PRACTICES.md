# 逻辑复制排障手册（运维版）

> 面向不熟悉内核的运维，所有 SQL 可直接粘贴。
> 视图列的完整定义见 [USER_MANUAL.md](USER_MANUAL.md)。

---

## 0. 一张图看懂

把发送端想成**总仓**、接收端想成**分仓**，数据变更是**包裹**：

```
总仓生产 ──► 打包发出 ──► 干线运输 ──► 分仓签收 ──► 上架(写进表) ──► 回执
(WAL生成)    (发送)       (网络)       (接收)        (应用)
 gen_rate   send_rate                  recv_rate    apply_rate
     |←── 没发货 ──┐|←── 在路上 ──┐|←── 到了没上架 ──┐
     └──── backlog_unsent ── backlog_inflight ── backlog_unapplied ──┘
                         （三段之和 = backlog_total）
```

排障只需要回答：**货压在哪段？谁慢？从什么时候开始？还要多久？**

## 1. 名词速查

| 术语 | 人话 | 对应视图列 |
| --- | --- | --- |
| WAL | 数据库的记账流水，复制就是把流水发给对方重演 | `gen_rate` = 记账速度 |
| LSN | 流水账的页码，只增不减 | 各 `*_lsn` 列 |
| 发送/接收/应用 | 打包发出去 / 分仓收到 / 真正写进表 | `send` / `recv` / `apply` |
| 积压(backlog) | 还没走完的量，单位 MB | `backlog_*` |
| 回执 | 分仓每 10 秒回一句"我用到第几页" | `feedback_lag_mb` |
| 堵住(blocked) | 有活干但干不动 | `send_blocked` / `apply_blocked` |
| 追平(catchup) | 按当前速度清完积压要几秒 | `catchup_total_secs` |
| 瓶颈(bottleneck) | 自动判定最慢的环节 | `bottleneck` |

## 2. 日常巡检：三条 SQL

### ① 插件本身好吗

```sql
SELECT loaded, session_state, last_round_ok, dropped_samples FROM pg_lrstat_info;
```
```
 loaded | session_state | last_round_ok | dropped_samples
--------+---------------+---------------+-----------------
 t      | idle          | t             |               0    ← 正常
```
异常：`loaded=f` 查 preload；`last_round_ok=f` 看 `last_round_error`；`dropped>0` 调大 max_targets。

### ② 复制整体健康吗（核心巡检，只盯这一个）

```sql
SELECT lrstat_start('daily_check');
-- 等 30~60 秒后：
SELECT recv_name,
       round(gen_avg::numeric,1)   AS 生成,
       round(send_avg::numeric,1)  AS 发送,
       round(apply_avg::numeric,1) AS 应用,
       round(backlog_total,1)      AS 总积压MB,
       round(catchup_total_secs)   AS 追平秒,
       bottleneck                  AS 瓶颈,
       send_blocked, apply_blocked
FROM pg_lrstat_cluster_stat;
SELECT lrstat_stop('daily_check');
```

**三条读法**：
1. **四速率相等** = 一切正常
2. **从左到右第一个掉队的数字就是瓶颈**：发送 < 生成 → 总仓发货慢；应用 < 接收 → 分仓上架慢
3. **bottleneck 列**直接告诉你答案（`send` / `recv_apply` / `network` / `none`）

### ③ 发布端磁盘会被拖爆吗

```sql
SELECT slot_name, wal_status,
       round(retained_wal,0)  AS 扣住MB,
       round(safe_wal_size,0) AS 余量MB
FROM pg_lrstat_send_stat;
```
`wal_status` 变 **`unreserved`** = 警戒线；变 **`lost`** = 数据已缺，必须重建订阅。

## 3. 定位流程（四步）

```
第1步 看快照定段     cluster_stat：哪段积压大 + 哪个速率掉队
第2步 查历史定时     send/recv_history：从什么时候开始坏的、越来越差还是好转
第3步 按段下钻       下面 A/B/C 的具体动作
第4步 处置后验证     cluster_stat 积压归零 + blocked 全 false = 闭环
```

### A. 没发货（unsent 大）

1. `send_blocked = t` → 订阅端断开了，恢复连接后自愈
2. `spill_avg > 0` → 大事务在翻译打包，等它或推动业务拆事务
3. `sync_state` 不是 async → 在等同步备库确认，不是真慢

### B. 在路上（inflight 大）

```sql
-- 先排除"假在路上"（10秒回执造成的观测假象）：
SELECT round(backlog_inflight,0) AS 在路上MB,
       round(feedback_lag_mb,0)  AS 回执滞后MB
FROM pg_lrstat_cluster_stat;
```
两个数差不多大 → 假象，不用处理。真在路上持续增长才报修网络。

### C. 到了没上架（unapplied 大）

1. `apply_blocked = t` → **多半是锁**，查谁堵的：
```sql
-- 把 <PID> 换成 recv_stat 里的 worker_pid
SELECT pid, wait_event, pg_blocking_pids(pid) AS 被谁堵
FROM pg_stat_activity WHERE pid = <PID>;
```
2. 不停但持续慢 → 单线程上架是设计如此（索引多/触发器/大事务都拖慢），治理在订阅端表结构
3. 有 `worker_type = 'table synchronization'` 行 → 初始同步进行中，正常

## 4. 故障卡片

**卡片 1：复制彻底不动**
`send_blocked` 或 `apply_blocked` 为 t。按"锁→连接断开→订阅报错"三选一处理。

**卡片 2：越追越远**
发送 < 生成持续成立。净追平 `min(send,apply) − gen` 为负则调参没用，必须扩容或拆负载。

**卡片 3：磁盘要撑爆**
`retained_wal` 持续增长 + `wal_status = unreserved`。找到收不下货的订阅 → 恢复消费；`lost` = 必须重建。

**卡片 4：一阵慢一阵快**
`spill_avg` 间歇 > 0、积压锯齿形 → 大事务，属正常现象。

**卡片 5：割接前估时**
```sql
SELECT round(catchup_total_secs) AS 追平秒,
       round((least(send_avg,apply_avg) - gen_avg)::numeric,1) AS 净追平MB
FROM pg_lrstat_cluster_stat;
```
净追平必须为正。例：积压 100GB、净 2MB/s ≈ 14 小时。

**卡片 6：看起来落后其实没事**
`backlog_inflight ≈ feedback_lag_mb` 且应用速率正常 → 回执假象，别派网络工单。

## 5. 常见误读

1. 四速率相等**不是 bug**，是健康
2. 大事务期间"应用"读 0 属正常，看 `apply_blocked` 区分真假停
3. 速率 **NULL 不是 0**，等一个采样周期
4. `catchup_total_secs` 在持续写入下偏乐观，用净追平重估
5. 会话重启后（persist=false）数据消失是设计
6. `sync_state` 非 async 的慢是在等同步备库，不是复制故障

## 6. 什么时候找谁

| 现象 | 找谁 |
| --- | --- |
| info 异常 / 视图全空 / 采样失败 | 扩展维护者（带 last_round_error） |
| wal_status = lost | DBA：按预案重建订阅 |
| 订阅端冲突报错 | 业务方对齐 + DBA |
| 速率都正常但业务说慢 | 应用侧排查（复制链路已证清白） |
