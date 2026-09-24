# 逻辑复制性能排障手册（运维版）

> 面向不熟悉内核的运维同学。所有 SQL 可直接粘贴；关键步骤都配了带数字的样例输出和下一步动作。
> 视图列的完整定义见 [USER_MANUAL.md](USER_MANUAL.md) §5，字段来源见 §6，速率公式见 §7。

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

**排障只需要回答四个问题**：货压在哪一段？谁干得慢？**从什么时候开始、越来越差还是正在好转？**还要多久清完？前两问看快照视图（§2-③），第三问必须靠 history 视图（§3），最后一问用净追平速度（§6 卡片 5）。

## 1. 五分钟名词速查（人话版）

| 术语 | 人话 | 对应视图列 |
| --- | --- | --- |
| WAL | 数据库的记账流水：所有增删改先记流水再落盘，复制就是把流水发给对方重演一遍 | `gen_rate` = 记流水的速度 |
| LSN | 流水账的页码，只增不减。"进行到哪了"都用页码表示，两页相减就是字节数 | 各 `*_lsn` 列 |
| 发送 / 接收 / 应用 | 打包发出去 / 分仓收到了 / 真正写进表里 | `send_rate` / `recv_rate` / `apply_rate` |
| 积压（backlog） | 还没走完的量，单位字节，用 `pg_size_pretty()` 看着舒服 | `backlog_*` |
| 回执（反馈） | 分仓每 10 秒回一句"我用到第几页了"。总仓眼里的分仓进度**最多滞后 10 秒**，设计如此不是故障 | `feedback_lag_bytes` |
| 槽（slot） | 总仓的客户档案：只要分仓没确认，流水就不许删 | `retained_wal` |
| 解码（decoding） | 把流水翻译成分仓能执行的语句；翻译不过来会堆到磁盘 | `spill_rate` |
| history 视图 | 每 30 秒一行的**原始流水快照**，默认存 15 小时——它是"回看过程"的唯一手段 | `pg_lrstat_pub/sub_history` |

## 2. 日常巡检：三条 SQL

### ① 插件本身健康吗

```sql
SELECT loaded, last_round_ok, dropped_samples FROM pg_lrstat_info;
```

```
 loaded | last_round_ok | dropped_samples
--------+---------------+-----------------
 t      | t             |               0     ← 正常：已加载/采样正常/没丢数据
```

异常动作：`loaded=f` → 查 `shared_preload_libraries` 并重启；`last_round_ok=f` → 看 `last_round_error` 的报错文本；`dropped_samples>0` → 调大 `pg_lrstat.max_targets`（重启）。

### ② 复制整体健康吗（核心巡检，订阅端执行）

```sql
SELECT sub_name, remote_state,
       round(gen_rate::numeric,1)   AS 生成,
       round(send_rate::numeric,1)  AS 发送,
       round(recv_rate::numeric,1)  AS 接收,
       round(apply_rate::numeric,1) AS 应用,
       pg_size_pretty(backlog_unsent)    AS 没发货,
       pg_size_pretty(backlog_inflight)  AS 在路上,
       pg_size_pretty(backlog_unapplied) AS 到了没上架,
       pg_size_pretty(backlog_total)     AS 总积压,
       send_stalled, apply_stalled
FROM pg_lrstat_overall;
```

```
 sub_name | remote_state | 生成 | 发送 | 接收 | 应用 | 没发货 | 在路上 | 到了没上架 | 总积压 | send_stalled | apply_stalled
----------+--------------+------+------+------+------+--------+--------+------------+--------+--------------+--------------
 sub_a    | ok           |  6.0 |  3.2 |  3.2 |  3.1 | 800 MB | 12 MB  | 40 MB      | 852 MB | f            | f
```

**三条读法**：
1. 四个速率相等 = 健康（下游跟得上，都被"生成"定节奏）；
2. **从左到右第一个掉队的数字就是瓶颈**：上例发送 3.2 < 生成 6.0 → 总仓发货慢，且"没发货" 800MB 最大，互相印证 → 去 §5 案例 A；
3. 两个 stalled 任一为 t = 停摆，最高优先级（§7 卡片 1）。

### ③ 发布端磁盘会被拖爆吗（发布端执行）

```sql
SELECT slot_name, wal_status,
       pg_size_pretty(retained_wal)  AS 扣住的流水,
       pg_size_pretty(safe_wal_size) AS 距离上限余量
FROM pg_lrstat_pub_sample;
```

`retained_wal` 持续增长 = 有人囤货；`wal_status` 变 `unreserved` = 到警戒线；变 **`lost` = 流水已删、订阅缺数据、必须重建**。

## 3. history 视图：把"现在"变成"过程"

快照只告诉你"现在压了 852MB"，**不知道是刚出事还是快好了**。history 视图存的是每 30 秒一行的原始样本（默认 15 小时，实例重启清零），四个用法：

> 位置要记牢：`pub_history` 在**发布端**查，`sub_history` 在**订阅端**查。查询**永远带过滤**（订阅名 + 时间范围），否则返回全部目标的全环数据。

### 用法 1：确认趋势和起点（从什么时候开始坏的）

```sql
-- 发布端：看"没发货"积压是怎么长起来的
SELECT ts,
       pg_size_pretty(current_lsn - sent_lsn) AS 没发货
FROM pg_lrstat_pub_history('sub_a', now() - interval '1 hour')
ORDER BY ts;
```

```
         ts          | 没发货
---------------------+---------
 2026-09-24 10:20:00 | 0 bytes        ← 一小时前还是好的
 2026-09-24 10:25:00 | 96 MB
 2026-09-24 10:30:00 | 234 MB
 2026-09-24 10:35:00 | 371 MB          ← 每 5 分钟涨 ~140MB，匀速恶化
 2026-09-24 10:40:00 | 512 MB
```

判读：**匀速增长** = 供需差稳定（发送能力固定、生成变大了），起点在 10:20–10:25 之间；**台阶式跳变** = 单次事件（大事务/锁）；**增长放缓趋平** = 正在追上，可以只观察不动手。

### 用法 2：分辨"谁变了"——是产得多了，还是发得慢了？

这是定位最关键的一步。把每 30 秒的增量算出来对比：

```sql
SELECT ts,
       pg_size_pretty(current_lsn - lag(current_lsn) OVER w) AS 本段新增,
       pg_size_pretty(sent_lsn   - lag(sent_lsn)   OVER w) AS 本段发货
FROM pg_lrstat_pub_history('sub_a', now() - interval '1 hour')
WINDOW w AS (ORDER BY ts);
```

```
         ts          | 本段新增 | 本段发货
---------------------+----------+----------
 2026-09-24 10:19:30 | 87 MB    | 87 MB      ← 出事前：产多少发多少
 2026-09-24 10:20:00 | 88 MB    | 87 MB
 2026-09-24 10:25:30 | 179 MB   | 96 MB      ← 出事后：新增翻倍、发货原地踏步
 2026-09-24 10:26:00 | 180 MB   | 96 MB
```

两种结论完全不同的处理方向：
- **新增变大、发货不变**（上例）→ 上游来了新负载（批量任务/大促），发货能力本来就是 3.2MB/s 没退化为 → 找业务方协调负载或扩容链路；
- **新增不变、发货变小** → 发货能力退化了（网络劣化/发布端资源被抢）→ 找网络组/查发布端主机。

### 用法 3：订阅端"到了没上架"的时间线（积压列不在 history 里，两列相减即得）

```sql
-- 订阅端：待上架 = 收到 − 已应用，看它是不是"台阶式"增长（锁的典型形态）
SELECT ts,
       pg_size_pretty(received_lsn - applied_lsn) AS 到了没上架
FROM pg_lrstat_sub_history('sub_a', now() - interval '30 minutes')
ORDER BY ts;
```

```
 10:00 | 0 bytes      10:04 | 698 MB
 10:01 | 175 MB       10:05 | 872 MB     ← 匀速涨：应用停了
 10:02 | 349 MB       10:08 | 96 MB      ← 突然清空：堵点放开了
```

台阶的**起点 = 被堵时刻**，**拐点 = 放开时刻**——拿这两个时间点去对订阅库的日志/审计（谁在那时候跑了长事务/DDL），比盲查锁表快得多。

### 用法 4：手工复核/任意窗口平滑 + 恢复验证

对任何两个时刻的样本，速率 =（新 LSN − 旧 LSN）÷ 秒数 ÷ 1048576。想看 1 小时平均而不是默认 2 分钟窗口，取首尾两行算即可；处置完成后，用用法 1 的同款查询看"没发货"曲线是否归零走平——**曲线归零 + 快照积压 0** 才算闭环。

**两个限制**：粒度 = `sample_interval`（默认 30s），更细的毛刺看不见（临时调 1s 采样可抓）；数据在内存，**重启即清零**——长期趋势请让监控定期采集 history 落库。

## 4. 定位流程总纲（四步走）

```
第1步 看快照定段     §2-②：哪段积压大 + 第一个掉队的速率   → 得出"压在哪、谁慢"
第2步 查历史定时     §3 用法1/2：起点、匀速还是台阶、"谁变了"  → 得出"什么性质的问题"
第3步 按段下钻       §5（发布端/网络/订阅端的具体动作和确认 SQL）
第4步 处置后验证     §3 用法4：曲线归零 + stalled 复位        → 闭环
```

## 5. 分段下钻手册（第 3 步的展开）

### A. 没发货（unsent 大，发布端的问题）

1. `send_stalled=t` → 订阅端断开/订阅被禁用，恢复连接后自愈；
2. `spill_rate>0` → **大事务**在翻译打包（§7 卡片 4）：等，或推动业务拆事务；配合 history 确认是"偶发锯齿"还是"常态"；
3. 多个订阅同时变慢、`gen_rate` 正常 → 发布端 CPU 被多个翻译任务挤占（每个订阅都要独立翻译一遍流水）；
4. `sync_state` 非 async 且发布端在等 → 在等同步备库确认，不是复制慢，找 DBA。

### B. 在路上（inflight 大，先排除假象再报网络）

```sql
SELECT pg_size_pretty(backlog_inflight)   AS 在路上,
       pg_size_pretty(feedback_lag_bytes) AS 回执滞后
FROM pg_lrstat_overall;
```

两个数**差不多大** → 10 秒回执造成的假象，收工。只有"在路上"**持续增长**且远超回执滞后才是真网络问题——给网络组"发送速率 MB/s"和链路带宽即可（§8 陷阱 5 的换算如需对比带宽）。

### C. 到了没上架（unapplied 大，订阅端的问题）

1. `apply_stalled=t` → 多半是锁。先从 history（§3 用法 3）拿到被堵/放开的时间点，再查现场：

```sql
-- 订阅端执行，<PID> = sub_sample 里的 worker_pid
SELECT pid, wait_event_type, wait_event, pg_blocking_pids(pid) AS 被谁堵
FROM pg_stat_activity WHERE pid = <PID>;
```

2. 不停但持续慢（应用<接收）→ 单线程上架是设计如此：目标表索引多/触发器/外键/大事务都会拖慢，治理在订阅端表结构；
3. 有 `worker_type='table synchronization'` 行 → 初始同步进行中，属正常，等行消失。

## 6. 两个完整实战案例（四步全走一遍）

### 案例 A："总积压 852MB" 报警（发布端负载突增）

1. **快照定段**（§2-② 输出即本例）：发送 3.2 < 生成 6.0，没发货 800MB 占 94% → 发布端发货段；
2. **历史定时**（§3 用法 1/2）：起点 10:20–10:25；匀速恶化；**新增翻倍、发货不变** → 结论：新负载涌入，发货能力没退化；
3. **下钻**：`spill_rate=0` 排除大事务；回执滞后 11MB ≈ 在路上 12MB 排除网络；`sync_state=async` 排除同步等待 → 锁定"上游批量任务"；
4. **处置**：与业务确认 10:20 启动了批处理 → 错峰/限流；**验证**：history"没发货"曲线 10:55 开始下降、11:10 归零走平，快照积压 0 → 闭环。

### 案例 B："应用停了 8 分钟"（订阅端锁堵塞）

1. **快照定段**：应用 0.0 < 接收 2.9，到了没上架 870MB，`apply_stalled=t` → 订阅端；
2. **历史定时**（§3 用法 3）：10:00 起匀速涨、10:08 陡降 → 被堵 10:00–10:08，已自愈；
3. **溯源**：拿 10:00 时间点查订阅库审计/日志 → 发现有人 10:00 跑了 8 分钟的报表大事务（锁住了目标表）；
4. **处置**：无需现场动作（已放开）；**预防**：报表改走只读副本/加超时。验证：曲线走平、stalled=f。

## 7. 故障处理卡片

**卡片 1：复制彻底不动**：`stalled=t`。按"锁（§5-C SQL）→ 连接断开 → 订阅报错（`pg_stat_subscription_stats` 错误计数在涨则看订阅日志，多为约束冲突）"三选一。

**卡片 2：越追越远**：发送<生成持续。净追平 `min(发送,应用)−生成` 为**负**则调参无用，必须扩容/拆负载（正数才追得平）。

**卡片 3：磁盘要被撑爆**：retained 增长 + `wal_status=unreserved`。找到囤货订阅 → 恢复消费；救急调大 `max_slot_wal_keep_size`；**`lost` = 数据已缺，按预案重建订阅**。

**卡片 4：一阵慢一阵快**：`spill_rate` 间歇>0、积压锯齿 → 大事务，属可解释现象，频繁则拆事务。

**卡片 5：割接前估时**：

```sql
SELECT sub_name, pg_size_pretty(backlog_total) AS 总积压,
       round(eta_total::numeric) AS 追平秒,
       round((least(send_rate,apply_rate) - gen_rate)::numeric,1) AS 净追平MB
FROM pg_lrstat_overall;
```

业务停写看"追平秒"；**业务继续写必须看"净追平"**且为正。例：积压 100GB、净追平 2MB/s ≈ 14 小时。积压归零后再静默观察一个窗口无增长才割。

**卡片 6：看起来落后其实没事**：在路上 ≈ 回执滞后且应用正常 → 观测假象，别派网络工单。

## 8. 常见误读十条

1. 四速率相等**不是 bug**，是健康（都被"生成"定节奏）。
2. 大事务回放期间"应用"读 0 属正常（提交才记账），用 `apply_stalled` 区分真假停。
3. 速率列 **NULL 不是 0**：刚启动/采样中断/窗口数据不足，等一个采样周期。
4. `local_wal_rate` 含订阅库自身写入，专用订阅库才能当"应用产生的量"。
5. 速率是**页码差不是网线上的字节**：对比带宽要先乘"解码膨胀率"（`total_bytes` 增量÷`current_lsn` 增量）。
6. `backlog_inflight` 天然含 10 秒回执假象（卡片 6）。
7. 实例**重启后 history 清零**，速率约 1 分钟后恢复显示；长期趋势靠监控落库。
8. 发布端视图里的分仓进度**最多滞后 10 秒**；判断分仓状态优先看订阅端视图。
9. 查 history **必带过滤**且**查对节点**（pub 在发布端、sub 在订阅端）。
10. `sync_state` 非 async 时的慢是在等同步备库，不是复制故障。

## 9. 什么时候升级处理

| 现象 | 找谁 |
| --- | --- |
| `pg_lrstat_info` 异常 / 视图全空 / 采样进程反复失败 | 扩展维护者（带 `last_round_error`） |
| `wal_status = lost` | DBA：订阅缺数据，按预案重建 |
| 订阅端冲突报错（错误计数在涨） | 业务方对齐数据 + DBA 处理冲突 |
| 各环节速率正常、业务仍反馈延迟 | 应用侧排查（复制链路已证清白） |
