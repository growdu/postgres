# 逻辑复制性能排障手册（运维版）

> 面向不熟悉内核的运维同学。所有 SQL 可直接粘贴；每个输出都给了"正常长什么样、异常怎么办"。
> 视图列的完整定义见 [USER_MANUAL.md](USER_MANUAL.md) §5，字段来源见 §6。

---

## 0. 先建一张图：逻辑复制就是一条快递流水线

把发布端想成**总仓**、订阅端想成**分仓**，数据变更是**包裹**：

```
总仓生产包裹 ──► 打包发出 ──► 干线运输 ──► 分仓签收 ──► 上架(写进表) ──► 回执给总仓
 (WAL生成)      (发送)        (网络)       (接收)        (应用)          (反馈)
   gen_rate     send_rate                   recv_rate    apply_rate
        |←─ 没发货 ─┐|←── 在路上 ──┐|←── 到了没上架 ─┐
        └── backlog_unsent ── backlog_inflight ── backlog_unapplied ──┘
                              （三段加起来 = backlog_total 总积压）
```

**排障只需要回答三个问题**：货压在哪一段？哪个人（环节）干得慢？还要多久清完？本手册全部内容就是这三问。

## 1. 五分钟名词速查（人话版）

| 术语 | 人话 | 对应视图列 |
| --- | --- | --- |
| WAL | 数据库的记账流水：所有增删改先记流水再落盘，复制就是把这些流水发给对方重演一遍 | `gen_rate` = 记流水的速度 |
| LSN | 流水账的页码，只增不减。"进行到哪了"都用页码表示 | 各 `*_lsn` 列 |
| 发送 / 接收 / 应用 | 打包发出去 / 分仓收到了 / 真正写进表里 | `send_rate` / `recv_rate` / `apply_rate` |
| 积压（backlog） | 还没走完的量，**单位是字节**，用 `pg_size_pretty()` 看着舒服 | `backlog_*` |
| 回执（反馈） | 分仓每 10 秒回一句"我用到第几页了"。**总仓眼里的分仓进度最多滞后 10 秒**，这是设计如此不是故障 | `feedback_lag_bytes` |
| 槽（slot） | 总仓的客户档案：只要分仓没确认，流水就不许删 | `retained_wal`（为这个客户扣住的流水总量） |
| 解码（decoding） | 把流水翻译成分仓能执行的语句 | `spill_rate`（翻译不过来堆到磁盘的速度） |
| ETA | 按当前速度清完积压需要的秒数 | `eta_total` |

## 2. 日常巡检：三条 SQL

### ① 插件本身健康吗（每天一次 / 告警缺失时）

```sql
SELECT loaded, last_round_ok, dropped_samples FROM pg_lrstat_info;
```

```
 loaded | last_round_ok | dropped_samples
--------+---------------+-----------------
 t      | t             |               0     ← 正常：已加载/采样正常/没丢数据
```

| 异常 | 含义 | 动作 |
| --- | --- | --- |
| `loaded = f` | 没预加载，所有视图都是空的 | 检查 `shared_preload_libraries` 并重启 |
| `last_round_ok = f` | 采样进程上一轮失败了 | 看 `last_round_error` 列的错误信息 |
| `dropped_samples > 0` | 监控目标太多装不下了 | 调大 `pg_lrstat.max_targets`（需重启） |

### ② 复制整体健康吗（核心巡检）

```sql
SELECT sub_name, remote_state,
       round(gen_rate::numeric,1)   AS 生成,
       round(send_rate::numeric,1)  AS 发送,
       round(recv_rate::numeric,1)  AS 接收,
       round(apply_rate::numeric,1) AS 应用,
       pg_size_pretty(backlog_total) AS 总积压,
       round(eta_total::numeric)     AS 追平秒,
       send_stalled, apply_stalled
FROM pg_lrstat_overall;
```

```
 sub_name | remote_state | 生成 | 发送 | 接收 | 应用 | 总积压 | 追平秒 | send_stalled | apply_stalled
----------+--------------+------+------+------+------+--------+--------+--------------+--------------
 sub_a    | ok           |  7.6 |  7.6 |  7.6 |  7.6 | 8 kB   |      1 | f            | f
```

**读法（就三条）**：
1. **四个速率相等** = 一切正常，货流顺畅（都等于"生成"是因为上游产多少下游就跟多少）；
2. **从左到右第一个掉队的数字就是瓶颈**：发送 < 生成 → 总仓发货慢（§4-A）；应用 < 接收 → 分仓上架慢（§4-C）；
3. **两个 stalled 任一为 t** = 对应环节停摆了，优先处理（§5 卡片 1）。

`remote_state`：`ok` 正常；`unreachable` 连不上总仓（网络/总仓宕机，插件在自动重连）；`n/a` 还没轮询到（刚启动 30 秒内正常）。

### ③ 发布端磁盘会被拖爆吗

```sql
SELECT slot_name, wal_status,
       pg_size_pretty(retained_wal)  AS 扣住的流水,
       pg_size_pretty(safe_wal_size) AS 距离上限余量
FROM pg_lrstat_pub_sample;
```

- `retained_wal` **持续增长** = 有分仓没收货，总仓在替它囤流水；
- `wal_status` 从 `reserved` 变成 **`unreserved`** = 囤到警戒线了，立即处理（§5 卡片 3）；变 `lost` = 流水已被删，这个订阅**缺数据了，必须重建**。

## 3. 第一问：货压在哪一段？

```sql
SELECT sub_name,
       pg_size_pretty(backlog_unsent)    AS 没发货,
       pg_size_pretty(backlog_inflight)  AS 在路上,
       pg_size_pretty(backlog_unapplied) AS 到了没上架,
       pg_size_pretty(backlog_total)     AS 总积压
FROM pg_lrstat_overall;
```

哪个列大就去对应章节：**没发货 → §4-A（总仓问题）**；**在路上 → §4-B（网络）**；**到了没上架 → §4-C（分仓问题）**。

## 4. 分段处理手册

### A. 没发货（unsent 大，总仓的问题）

1. `send_stalled = t`？→ 总仓发送停了，最常见是**订阅端断开**（分仓重启/网络断）。确认订阅端进程在不在，恢复连接后这里自己会动起来。
2. `spill_rate > 0`？→ **大事务**：业务一次改了几十上百万行，翻译打包要时间。属正常现象，等它发完；频繁出现就找业务方把大事务拆小（分批提交）。
3. 发布端有**好几个订阅**、大家同时变慢、但 `gen_rate` 正常？→ 总仓 CPU 被多个翻译任务挤占，考虑拆分发布端或限制订阅数。
4. `sync_state` 不是 `async` 且发布端在等同步复制？→ 这不是慢，是**在等另一个同步备库确认**，找 DBA 确认同步复制配置。

### B. 在路上（inflight 大，先别急着报修网络）

**关键一步：先排除"假在路上"**——回执每 10 秒才发一次，所以总仓眼里总有一点"在路上"的假象：

```sql
SELECT pg_size_pretty(backlog_inflight)   AS 在路上,
       pg_size_pretty(feedback_lag_bytes) AS 回执滞后
FROM pg_lrstat_overall;
```

两个数**差不多大** → 是回执延迟的假象，**不是网络问题**，收工。只有"在路上"**持续增长**且远大于回执滞后，才是真网络瓶颈——找网络组，给他们"发送速率"（MB/s）和链路带宽对比即可。

### C. 到了没上架（unapplied 大，分仓的问题）

1. `apply_stalled = t`？→ **多半是锁**：分仓往表里写数据时被人堵住了。一条 SQL 找到堵人的：

```sql
-- 在订阅端执行，把 <PID> 换成 sub_sample 里查到的 worker_pid
SELECT pid, wait_event_type, wait_event,
       pg_blocking_pids(pid) AS 被谁堵
FROM pg_stat_activity WHERE pid = <PID>;
```

常见堵因：有人在订阅库跑长事务/改表。处理：等它结束或与业务协调 kill。

2. 不停但一直慢（应用 < 接收）？→ 分仓**单线程上架**是设计如此，下面任何一条都会拖慢：目标表索引太多、有触发器、外键检查、单条事务太大。治理方向在订阅端表结构，不在复制配置。
3. 看到 `worker_type = 'table synchronization'` 的行？→ **初始同步进行中**，"到了没上架"增长是正常过程，等同步完成（行消失）即可。

## 5. 故障处理卡片

**卡片 1：复制彻底不动了**
一眼确认：`send_stalled=t` 或 `apply_stalled=t`。
常见原因：订阅端被锁堵住（§4-C 的 SQL 查）；发布端订阅断开；订阅报错反复重试（`pg_stat_subscription_stats` 里错误计数在涨 → 看订阅端日志的具体报错，多是约束冲突）。
处理：按原因三选一解锁/重连/修数据。

**卡片 2：越追越远（发送 < 生成，持续）**
含义：总仓产货快过发货，差距只会越来越大。
处理：先按 §4-A 排查；确认是纯吞吐不够的话，用"净追平速度"判断是否要扩容：`min(发送,应用) − 生成` 是**正数**才追得平，负数必须扩容/拆负载，调参数没用。

**卡片 3：磁盘要被流水撑爆（retained_wal 增长 / wal_status=unreserved）**
处理顺序：找到收不下货的订阅（`pg_lrstat_pub_sample` 里 retained 最大的槽）→ 恢复它的消费；短期救急可以调大 `max_slot_wal_keep_size`；**`wal_status` 一旦 `lost`，这个订阅数据已缺，只能重建**（先留好业务数据再操作）。

**卡片 4：一阵慢一阵快**
签名：`spill_rate` 间歇 > 0，总积压锯齿形。
原因：大事务。见 §4-A-2，属可解释的正常现象；要平滑就拆事务。

**卡片 5：割接前问"还要多久"**
```sql
SELECT sub_name, pg_size_pretty(backlog_total) AS 总积压,
       round(eta_total::numeric) AS 追平秒,
       round((least(send_rate,apply_rate) - gen_rate)::numeric,1) AS 净追平MB
FROM pg_lrstat_overall;
```
`eta_total` 假设业务停写；**业务继续在写时看"净追平"**：必须为正。例：积压 100GB、净追平 2MB/s ≈ 14 小时。割接窗口按这个留余量，积压归零后再静默观察一个窗口无增长才动手。

**卡片 6：看起来落后其实没事**
签名：`backlog_inflight ≈ feedback_lag_bytes`，且"应用"速率正常。
原因：回执 10 秒一发造成的观测假象。无需处理，别误派网络工单。

## 6. 常见误读十条

1. 四个速率相等**不是抄数 bug**，是健康的表现（都被"生成"定节奏）。
2. "应用"在单个大事务回放期间读到 0 属正常（事务提交才记账），看 `apply_stalled` 区分真假停。
3. 速率列是 **NULL 不是 0**：刚启动、采样中断恢复、或窗口数据不足——等一个采样周期再看。
4. `local_wal_rate` 包含订阅库自己的其他写入，只有专用订阅库才能当"应用产生的量"读。
5. `backlog_inflight` 天然含 10 秒回执假象（见卡片 6）。
6. 实例**重启后历史清零**（数据在内存），速率要等约 1 分钟才恢复显示；长期趋势靠监控采集 history 视图。
7. `eta_total` 在持续写入下偏乐观，用净追平速度重估（卡片 5）。
8. 发布端视图里的对端进度**最多滞后 10 秒**（回执周期），订阅端视图是实时的——判断分仓状态优先看订阅端。
9. 查 history 视图**一定带过滤条件**（订阅名/时间），否则全量数据量很大。
10. `sync_state` 非 async 时的"慢"是等同步备库确认，不是复制故障（§4-A-4）。

## 7. 什么时候升级处理

| 现象 | 找谁 |
| --- | --- |
| `pg_lrstat_info` 异常 / 视图全空 / 采样进程反复失败 | 扩展维护者（带 `last_round_error` 内容） |
| `wal_status = lost` | DBA：订阅已缺数据，需按预案重建订阅 |
| 订阅端冲突报错（错误计数在涨） | 业务方对齐数据 + DBA 处理冲突 |
| 各环节速率都正常、业务仍反馈延迟 | 应用侧排查（复制链路已证清白） |
