# pg_lrstat 用户手册

pg_lrstat 是一个测量**逻辑/物理复制性能**的扩展。部署在复制链路的**接收端**即可工作：本地采样订阅/恢复进度，同时按订阅连接串轮询发送端系统视图，把两端合成一条链路视图——发送端无需安装任何东西。

## 1. 快速开始

```sql
-- 0. postgresql.conf: shared_preload_libraries = 'pg_lrstat'，重启后
CREATE EXTENSION pg_lrstat;

-- 1. 开始测量
SELECT lrstat_start();

-- 2. 迁移/压测期间随时看链路健康（日常只需要这一个视图）
SELECT recv_name, bottleneck,
       round(gen_avg::numeric,1)  AS 生成,
       round(send_avg::numeric,1) AS 发送,
       round(apply_avg::numeric,1) AS 应用,
       round(backlog_total::numeric,1) AS 总积压MB
FROM pg_lrstat_cluster_stat;

-- 3. 结束测量
SELECT lrstat_stop();

-- 4. 导出报告（直接写文件 = 持久化，返回路径，浏览器打开）
SELECT lrstat_export();
-- -> /var/lib/pgsql/data/pg_lrstat/exports/session.html
-- 环形内仍可按名补导: SELECT lrstat_export('sess_3');
--   （会话名查 info.session_name / exported_report_names）
```

单位约定：**所有字节量输出为 MB（1MB = 1048576 字节），所有速度为 MB/s，时间为秒**。

## 2. 命令（共 4 个，均要求 superuser）

| 命令 | 用法 | 说明 |
| --- | --- | --- |
| `lrstat_start()` | `SELECT lrstat_start()` | 开始采样（无参数，全局唯一会话）。自动名 sess_N 仅用于报告与按名导出 |
| `lrstat_stop()` | `SELECT lrstat_stop()` | 停止采样（无参数） |
| `lrstat_export(name, format)` | `SELECT lrstat_export()` | 导出报告并**直接写文件**，返回绝对路径；format='html'（默认）或 'json'。不传 name 导出当前/最近会话；传 name（如 `sess_3`，见 info 的 archived_session_names）导出指定归档。名字只在这里出现——用于标识报告或本次测试 |
| `pg_lrstat_reset()` | `SELECT pg_lrstat_reset()` | 强制清除当前内存数据（不影响归档文件） |

报告内容（HTML）：分析结论卡（瓶颈判定、四速率、积压构成、追平预估、50/100/200GB 容量推算）、Session 卡、**Targets 卡**（每个目标的状态/平均速率/最新 LSN）、速率折线图（gen/send/apply 三线）、原始样本表。

## 3. 视图总览（6 个）

| 视图 | 粒度 | 用途 |
| --- | --- | --- |
| `pg_lrstat_info` | 1 行 | 健康自检 + 当前会话状态 + 归档会话名列表 |
| `pg_lrstat_send_stat` | 发送端一目标**一轮**一行 | 发送端逐轮时序：状态/水位/积压/本轮速率 |
| `pg_lrstat_recv_stat` | 接收端一 worker**一轮**一行 | 接收端逐轮时序 |
| `pg_lrstat_cluster_stat` | 一复制对**一轮**一行 | **两端合成逐轮时序，日常巡检只看这个** |
| `pg_lrstat_send_history` | 一目标一轮一行 | 发送端原始 LSN 样本（始终记录） |
| `pg_lrstat_recv_history` | 一目标一轮一行 | 接收端原始 LSN 样本（始终记录） |

前四张 stat/info 视图可随时查（每轮一行，含会话外空闲轮，速率空闲轮为 0）；逐间隔速率与原始样本也进导出报告的 Evidence 区（§6）。

## 4. 视图字段详解

### 4.1 `pg_lrstat_info` — 健康自检（1 行，15 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `loaded` | bool | 扩展是否已加载（shared_preload_libraries）；f 时其他视图全空 |
| `session_name` | text | 当前/最近一次会话名；从未 start 过为 NULL |
| `session_state` | text | idle（从未 start）/ running / stopped |
| `session_truncated` | bool | t = 历史环形写满，覆盖过最旧样本 |
| `session_degraded` | bool | t = 历史环形曾有写异常（保留位，正常恒 f） |
| `session_start_ts` | timestamptz | 当前会话开始时间 |
| `session_stop_ts` | timestamptz | 会话结束时间；运行中为 NULL |
| `sample_interval_ms` | int8 | 采样周期（毫秒）——瞬时速率的时间粒度 |
| `last_round_ts` | timestamptz | 采样 worker 上一轮时间 |
| `last_round_ok` | bool | 上一轮采样是否成功 |
| `last_round_error` | text | 上一轮失败时的错误信息 |
| `nrounds` | int8 | 累计采样轮数 |
| `dropped_samples` | int8 | 因目标槽满被丢弃的目标数；>0 需调大 max_targets |
| `remote_poll` | bool | 是否启用接收端→发送端轮询 |
| `exported_report_names` | text[] | 已导出的报告名列表（exports 目录）——报告即持久化产物 |

### 4.2 `pg_lrstat_send_stat` — 发送端时序（一目标一轮一行，27 列）

每个采样轮、每个发送端目标一行：水位、当轮积压、本轮速率、当轮状态。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `slot_name` `ts` | text, timestamptz | 目标（槽名）与采样时间 |
| `plugin` `temporary` `application_name` `client_addr` | — | 直通属性（不随轮存储，取自当前槽位） |
| `active` | bool | 本轮 walsender 是否在线 |
| `sender_pid` | int4 | 本轮 walsender PID |
| `state` `sync_state` `wal_status` | text | 本轮 walsender 状态 / 同步角色 / 槽 WAL 保留状态（lost=订阅会断） |
| `current_lsn` `sent_lsn` `confirmed_flush_lsn` | pg_lsn | 本轮生成/发送/确认水位 |
| `backlog_unsent` | float8 | 本轮未发送积压（MB）——解码/发送慢 |
| `backlog_inflight` | float8 | 已发送未收到（MB）——网络在途 |
| `backlog_peer_unapplied` | float8 | 对端收到未应用（MB）——对端应用慢 |
| `backlog_total` `retained_wal` | float8 | 总积压 / 槽扣住 WAL（MB） |
| `gen_mbps` `send_mbps` `apply_mbps` | float8 | 本轮间隔速率（MB/s，与前一轮同目标作差）；首轮为 NULL |
| `spill_mb` | float8 | 本轮解码溢写增量（MB）——突增=解码压力大 |
| `write_lag` `flush_lag` `replay_lag` | interval | 本轮发送端观测的三段延迟 |
| `send_blocked` | bool | 本轮有未发送积压且发送速率≈0 |

### 4.3 `pg_lrstat_recv_stat` — 接收端时序（一 worker 一轮一行，17 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `recv_name` `ts` | text, timestamptz | 订阅名与采样时间 |
| `worker_type` | text | apply / table synchronization |
| `worker_pid` `leader_pid` `relid` | — | 本轮 worker PID / 并行 leader / tablesync 目标表 OID |
| `received_lsn` `applied_lsn` | pg_lsn | 本轮收到/应用水位（applied 为提交边界） |
| `last_msg_send_time` `last_msg_receipt_time` | timestamptz | 本轮心跳收发时间——差过大=链路断 |
| `backlog_apply` | float8 | 本轮收到未应用（MB） |
| `recv_mbps` `apply_mbps` `local_wal_mbps` | float8 | 本轮间隔速率（MB/s）；首轮为 NULL |
| `apply_error_count` `sync_error_count` | int8 | 本轮累计错误数 |
| `apply_blocked` | bool | 本轮有未应用积压且应用速率≈0——典型是锁冲突 |

### 4.4 `pg_lrstat_cluster_stat` — 两端合成时序（一对一轮一行，26 列，**日常只看这个**）

每个采样轮把接收端行与**时间上最近的**发送端轮询样本配对成一行。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `recv_name` `ts` | text, timestamptz | 订阅名与接收端采样时间 |
| `remote_state` | text | ok / stale / unreachable / n/a（当前轮询状态） |
| `send_current_lsn` `sent_lsn` `confirmed_flush_lsn` | pg_lsn | 发送端水位（配对样本） |
| `received_lsn` `applied_lsn` | pg_lsn | 接收端水位 |
| `gen_mbps` `send_mbps` | float8 | 发送端本轮生成/发送速率（MB/s） |
| `recv_mbps` `apply_mbps` | float8 | 接收端本轮接收/应用速率（MB/s） |
| `backlog_unsent` `backlog_inflight` `backlog_unapplied` `backlog_total` | float8 | 本轮三段积压与总量（MB） |
| `retained_wal` `feedback_lag_mb` | float8 | 槽扣住 WAL / 反馈滞后（MB） |
| `write_lag` `flush_lag` `replay_lag` | interval | 发送端观测三段延迟（配对样本） |
| `catchup_send_secs` `catchup_total_secs` | float8 | 按本轮速率的追平预估（秒） |
| `send_blocked` `apply_blocked` | bool | 本轮发送/应用堵住 |
| `bottleneck` | text | 本轮瓶颈判定：send / recv_apply / network / none |

**可验证性**：`*_mbps` 列 = 对 `*_history` 视图相邻两行作差÷间隔，可用窗口函数手工重算，逐位一致。

### 4.5 `pg_lrstat_send_history` — 发送端原始样本（一目标一轮一行，11 列）

**始终记录**。这是所有派生统计的最原始数据来源；报告中的每条速率都可从这两张表手工重算。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `name` | text | 目标名（槽名） |
| `ts` | timestamptz | 采样时间 |
| `current_lsn` | pg_lsn | 发送端当前 WAL 生成位置 |
| `sent_lsn` | pg_lsn | 已发送位置——相邻两行作差即该轮发送量 |
| `peer_recv_lsn` | pg_lsn | 对端反馈：已收到位置 |
| `peer_flush_lsn` | pg_lsn | 对端反馈：已刷盘位置 |
| `peer_applied_lsn` | pg_lsn | 对端反馈：已应用位置 |
| `confirmed_flush_lsn` | pg_lsn | 槽确认边界 |
| `restart_lsn` | pg_lsn | 槽保水起点——current−restart 即 retained_wal |
| `spill_bytes` | int8 | 解码溢写累计字节 |
| `stream_bytes` | int8 | 解码流式（streaming large txn）累计字节 |

### 4.6 `pg_lrstat_recv_history` — 接收端原始样本（5 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `name` | text | 订阅名 |
| `ts` | timestamptz | 采样时间 |
| `received_lsn` | pg_lsn | 已收到位置 |
| `applied_lsn` | pg_lsn | 已应用位置（提交边界） |
| `local_wal_lsn` | pg_lsn | 接收端本地 WAL 写入位置 |

> 环形写满覆盖最旧（`info.session_truncated = true`）。**数据生命周期**：新会话 start 只重置测量锚点，不清历史——已结束的会话在环形内仍可 `lrstat_export(name)` 补导，直到被更新的采样自然挤出环形（默认约 2880 样本/目标）；实例重启后内存清空。**要留档就在 stop 后立即 export——写出的报告文件就是持久化**。

## 5. 速率是怎么算的（公式与算例）

所有速率都从 `pg_lrstat_send_history` / `pg_lrstat_recv_history` 的**原始 LSN 样本**推导，没有任何预存派生量——这意味着每个数字都能手工复算。单位约定：LSN 差为字节，**1 MB = 1,048,576 字节**，速率 MB/s。

### 5.1 两个基本公式

**公式一：逐轮速率**（stat 视图每行的 `*_mbps` 列）——同一目标相邻两个样本作差：

```
rate(第i轮) = pg_wal_lsn_diff(LSN_i, LSN_{i-1}) ÷ 1048576 ÷ (ts_i − ts_{i-1})
```

- LSN 十六进制差直接用 `pg_wal_lsn_diff()`（= 高 32 位×2^32 + 低 32 位之差，字节）
- 首行没有前一样本 → `*_mbps` 为 NULL
- 样本轮转空闲（位置不变）→ 0.000

**公式二：会话平均速率**（导出报告 analysis 的 `*_avg`）：

```
avg = pg_wal_lsn_diff(LSN_末, LSN_锚点) ÷ 1048576 ÷ (ts_末 − ts_锚点)
```

- 锚点 = 会话内**首个含有效位置的样本**，末样本 = 会话内最后一条（全零样本两头都不参与——否则会把整个 WAL 历史位置当增量，得出荒谬大速率）
- 分母取 min(末样本时间, stop_ts)：采样在 stop 后仍继续（空闲轮位置不动），不截断的话均值会随导出时间推迟被不断稀释

### 5.2 每条速率用哪个 LSN

| 速率列 | 差分字段 | 含义 |
| --- | --- | --- |
| `gen_mbps` | send_history.`current_lsn` | 发送端 WAL **生成**速率 |
| `send_mbps` | send_history.`sent_lsn` | 发送端**发送**速率 |
| `apply_mbps`（send 侧） | send_history.`peer_applied_lsn` | 发送端反馈观测的对端应用速率 |
| `recv_mbps` | recv_history.`received_lsn` | 接收端**接收**速率 |
| `apply_mbps`（recv 侧） | recv_history.`applied_lsn` | 接收端**应用**速率 |
| `local_wal_mbps` | recv_history.`local_wal_lsn` | 接收端本地 WAL 写入速率 |
| `spill_mb`（增量列） | send_history.`spill_bytes` | 本间隔解码溢写量（MB，不除时间） |

### 5.3 积压、追平与容量公式

```
backlog_unsent     = current_lsn − sent_lsn          （发送端没发出去的）
backlog_inflight   = sent_lsn − received_lsn         （网络上在途的）
backlog_unapplied  = received_lsn − applied_lsn      （收到但没应用的）
backlog_total      = current_lsn − applied_lsn       （端到端总积压）
retained_wal       = current_lsn − restart_lsn       （槽扣住的磁盘空间）

catchup_send_secs  = backlog_unsent ÷ send 速率        （发送段追平秒数）
catchup_total_secs = backlog_unsent ÷ send 速率
                     + (inflight + unapplied) ÷ apply 速率   （全链路追平）

sync_50g_secs      = 50 × 1024 ÷ apply_avg           （容量外推；100G/200G 严格 2×/4×）
```

### 5.4 完整算例（真实数据逐步走）

一次真实测量（1s 采样，两笔小事务），`pg_lrstat_recv_history` 原始样本：

| ts | applied_lsn |
| --- | --- |
| 14:12:29.307 | 0/8FE9D88 |
| 14:12:30.316 ~ 33.328 | 0/8FE9D88（4 行不变，空闲轮） |
| **14:12:34.332** | **0/9001E10**（第二笔事务提交，跳变） |
| 14:12:35.336 ~ 37.344 | 0/9001E10（3 行不变） |

**第一步：逐轮速率**（recv_stat 中 14:12:34.332 行的 `apply_mbps`）：

```
分子 = pg_wal_lsn_diff('0/9001E10', '0/8FE9D88')
     = 0x9001E10 − 0x8FE9D88 = 0x18088 = 98,440 字节 = 0.0939 MB
分母 = 34.332 − 33.328 = 1.004 s
apply_mbps = 0.0939 ÷ 1.004 = 0.0935 MB/s          ← 视图输出 0.0935 ✓
```

空闲轮（LSN 不变）差分为 0 → 视图输出 0.0000；提交轮把**整个事务的量**归到提交那一轮。

**第二步：会话平均**（报告 analysis 的 `apply_avg`）：

```
锚点 = 首个有效样本 29.307 的 0/8FE9D88；末样本 = 37.344 的 0/9001E10
分子 = 同上 0.0939 MB
分母 = 37.344 − 29.307 = 8.037 s（首末有效样本，非会话墙上时长）
apply_avg = 0.0939 ÷ 8.037 = 0.0117 MB/s           ← 报告输出 0.0117 ✓
```

注意分子分母同源（都来自 history 的有效行）：如果分母用"stop 时刻 − 开始时刻"（8.16s）会得到 0.0115——**错**，因为会话首尾各有一段没有数据的空窗。

**第三步：容量外推**（报告 capacity）：

```
sync_50g_secs = 50 × 1024 ÷ 0.0117 = 4,382,793 s ≈ 1,217 小时   ← 报告输出 ✓
sync_100g_secs / sync_200g_secs = 严格 2 × / 4 × 上述值          ✓
```

（本例速率极小是因为负载只有 0.09MB；真实迁移压测下数字同公式。）

**第四步：端到端闭合**：接收端 applied 推进 0.0939 MB == 发布端该窗口 `pg_current_wal_lsn()` 实测推进量（读者可用两端 history 互验）。

### 5.5 手工复算 SQL（可验证性）

```sql
-- 复算某轮的 apply 速率（与 recv_stat.apply_mbps 对齐 ts 逐位一致）
WITH h AS (SELECT ts, applied_lsn,
                  lag(applied_lsn) OVER w AS pl, lag(ts) OVER w AS pt
             FROM pg_lrstat_recv_history WHERE name='asub'
           WINDOW w AS (ORDER BY ts))
SELECT ts, round((pg_wal_lsn_diff(applied_lsn, pl)/1048576
                  / extract(epoch FROM ts-pt))::numeric, 4) AS apply_mbps
  FROM h WHERE pl IS NOT NULL ORDER BY ts;

-- 复算会话平均（与报告 analysis.apply_avg 逐位一致）
WITH h AS (SELECT ts, applied_lsn FROM pg_lrstat_recv_history
            WHERE name='asub' AND applied_lsn > '0/0'
              AND ts BETWEEN (SELECT session_start_ts FROM pg_lrstat_info)
                         AND (SELECT session_stop_ts  FROM pg_lrstat_info))
SELECT round((pg_wal_lsn_diff(max(applied_lsn), min(applied_lsn))/1048576
              / extract(epoch FROM max(ts) - min(ts)))::numeric, 4) AS apply_avg
  FROM h;
```

export 报告底部的 **Evidence** 区直接给出四张佐证表（send/recv 原始样本 + 逐间隔速率），头部结论即由此推导。

### 5.6 特殊情形（都不是 bug）

| 情形 | 表现 | 原因 |
| --- | --- | --- |
| 会话首行速率 | NULL | 无前一样本可差分 |
| 负载结束后的行 | 0.000 | 位置不推进，差分为 0 |
| 大事务期间 applied 不动，提交时一次跳变 | 该轮 apply_mbps 突然很大 | 应用位置只在提交边界推进（内核语义）；跳变量 = 整个事务的量，归属于提交那一轮 |
| 会话开始前已有积压 | 首轮 apply 速率即很高 | 追赶存量属于会话期间的真实工作；锚点从会话首样本起算，不会计入会话前的量 |
| applied 与 origin 不完全同步 | 微小偏差 | applied 是 origin 与发送端反馈 apply 位的单调融合（取更及时者），两者都是提交边界 |

## 6. 输出报告内容（lrstat_export）

报告直接写文件到 `$PGDATA/pg_lrstat/exports/<name>.<format>` 并返回绝对路径；HTML 浏览器双击即看，JSON 给机器。**export 就是持久化**：数据源是当前会话或内存环形内的指定会话（按名），找不到则明确报错。

### 6.1 HTML 报告（五个区块，自上而下）

| 区块 | 内容 | 怎么用 |
| --- | --- | --- |
| **Analysis**（最顶部） | ① 瓶颈判定徽章：`send`（发送慢，红）/ `recv_apply`（应用慢，黄）/ `network`（在途过半，蓝）/ `none`（健康，绿）② 四个平均速率 gen/send/recv/apply（MB/s）③ 三段积压 unsent/inflight/unapplied 与总量（MB）④ 追平预估（秒）与净追平速率（负值=追不上，标注 CANNOT catch up）⑤ **容量外推表**：当前积压、50G、100G、200G 按平均应用速率的耗时 | 运维第一眼看徽章；容量表回答“这次迁移要多久/再压 100G 行不行” |
| **Chart**（紧随其后） | gen/send/apply 三条逐间隔速率折线（内嵌 SVG） | 看速率何时掉下来、瓶颈何时开始 |
| **Session** | 会话名、起止时间、目标数、样本数；`[truncated]`（环形覆盖过最旧）/`[degraded]`（文件写失败）标记 | 确认测量窗口与数据完整性 |
| **Targets** | 每目标一行：侧别（send/recv）、状态（walsender state 或 worker 类型）、平均速率（`gen x / send y` 或 `recv x / apply y`）、最新 LSN | 多订阅/多槽时逐目标对比 |
| **Evidence**（底部佐证区，四张表） | ① send history samples：ts、目标、7 个 LSN 水位、spill/stream MB ② recv history samples：ts、目标、received/applied/local_wal ③ send rate history：每间隔的 gen/send/反馈apply MB/s 与 spill MB ④ recv rate history：每间隔的 recv/apply MB/s | **报告头部的每个数字都能从这里手工重算**——审计/质疑结论时用 |

### 6.2 JSON 报告（速率 4 位小数，可精确互算）

```json
{
  "session":  { "name", "running", "start", "stop", "truncated", "degraded" },
  "analysis": { "bottleneck", "gen_avg", "send_avg", "recv_avg", "apply_avg",
                "backlog_unsent_mb", "backlog_inflight_mb",
                "backlog_unapplied_mb", "backlog_total_mb" },
  "capacity": { "catchup_secs", "net_catchup_mbps",
                "sync_50g_secs", "sync_100g_secs", "sync_200g_secs" },
  "send_stat": [ 每目标摘要 ],
  "recv_stat": [ 每目标摘要 ],
  "history":   { "total_samples", "exported_samples",
                 "samples": [ 最近 500 条原始样本（kind 区分 send/recv）] }
}
```

### 6.3 数字口径（全部可手工复核）

- **平均速率**（analysis 的 `*_avg`）：会话内**首个含有效位置的样本** → 末样本，时间分母截断到 `stop_ts`（stop 后的空闲采样不稀释均值）。
- **容量外推**：`sync_50g_secs = 50×1024 MB ÷ apply_avg`；100G/200G 严格 2 倍/4 倍。用 JSON 的 4 位小数 `apply_avg` 复算应与 `sync_*_secs` 一致。
- **逐间隔速率**（Evidence 表）：相邻同目标样本作差 ÷ 间隔，与 SQL 里 `pg_wal_lsn_diff(x, lag(x)) ÷ extract(epoch ...)` 完全同式。
- `applied` 位置是 origin 与发送端反馈 apply 位的单调融合——大事务期间停在提交边界、提交时一次跳变属正常。

## 7. 配置（GUC）

| GUC | 默认 | 生效 | 含义 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期 = 瞬时速率粒度（最小 1s） |
| `pg_lrstat.session_max_samples` | `2880` | 重启 | 历史环形容量（每目标保留样本数；写满覆盖最旧） |
| `pg_lrstat.max_targets` | `32` | 重启 | 最多同时监测的目标数（订阅+槽+worker） |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标消失后多少秒从视图剔除（如 tablesync 结束） |
| `pg_lrstat.remote_poll` | `true` | SIGHUP | 是否从接收端轮询发送端（关闭则 cluster_stat 无远端数据） |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 轮询连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮轮询总预算；超时标 stale 不阻塞采样 |
| `pg_lrstat.catchup_min_rate` | `0.001` | SIGHUP | 追平预估输出的最低平均速率（MB/s） |

改运行时参数用 `SELECT pg_reload_conf();` 即可，不用重启。

## 8. 典型工作流

```sql
-- 迁移前评估：起会话，跑一轮试迁移，看应用速率和容量推算
SELECT lrstat_start();
-- ... 跑 10 分钟代表性负载 ...
SELECT lrstat_stop();
SELECT lrstat_export(NULL, 'json');   -- 容量表：50/100/200GB 需要多久

-- 日常巡检：瓶颈在哪、还要多久追平
SELECT recv_name, bottleneck, round(backlog_total::numeric,1) 积压MB,
       round(catchup_total_secs) 追平秒
FROM pg_lrstat_cluster_stat;

-- 事后分析：导出那次会话的报告（含佐证数据；名字查 info 的归档列表）
SELECT lrstat_export('sess_3');
-- 报告 Evidence 区四张表：send/recv 原始样本 + send/recv 逐间隔速率，
-- 与 Analysis 的结论一一对应，可手工重算核对

-- 空间占用风险：槽扣住了多少 WAL
SELECT slot_name, round(retained_wal::numeric,0) MB, wal_status
FROM pg_lrstat_send_stat ORDER BY retained_wal DESC NULLS LAST;
```

## 9. 常见问题

| 问题 | 答案 |
| --- | --- |
| 视图全空 | `info.loaded = f`：没预加载，检查 shared_preload_libraries 并重启 |
| 速率全是 NULL | 会话刚开始还没有两个有效样本，等一个采样周期 |
| cluster_stat 没数据 | 没有活跃订阅，或 remote_state = unreachable（检查订阅连接串可达性） |
| apply 速率 = 0 但积压在涨 | 应用被堵（锁冲突最常见）：拿 recv_stat 的 worker_pid 查 pg_stat_activity 的 wait_event |
| applied 长时间不动然后突然跳 | 大事务：应用位置只在提交边界推进，正常 |
| 想看已结束的会话 | 环形内 `lrstat_export(name)` 补导；已 export 过的直接开 exports 目录下的报告文件 |
| 四个速率相等 | 健康！下游跟得上，都被"生成"定节奏 |
| round(x, 1) 报错 | 视图速率/积压列是 float8，需 `round(x::numeric, 1)` |
