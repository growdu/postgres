# pg_lrstat 用户手册

pg_lrstat 是一个测量**逻辑/物理复制性能**的扩展。部署在复制链路的**接收端**即可工作：本地采样订阅/恢复进度，同时按订阅连接串轮询发送端系统视图，把两端合成一条链路视图——发送端无需安装任何东西。

## 1. 快速开始

```sql
-- 0. postgresql.conf: shared_preload_libraries = 'pg_lrstat'，重启后
CREATE EXTENSION pg_lrstat;

-- 1. 开始一次命名测量（persist=true 时数据落文件，可长期追溯）
SELECT lrstat_start('mig_20260924', true);

-- 2. 迁移/压测期间随时看链路健康（日常只需要这一个视图）
SELECT recv_name, bottleneck,
       round(gen_avg::numeric,1)  AS 生成,
       round(send_avg::numeric,1) AS 发送,
       round(apply_avg::numeric,1) AS 应用,
       round(backlog_total::numeric,1) AS 总积压MB
FROM pg_lrstat_cluster_stat;

-- 3. 结束测量
SELECT lrstat_stop('mig_20260924');

-- 4. 导出报告（直接写文件，返回路径，浏览器打开）
SELECT lrstat_export('mig_20260924');
-- -> /var/lib/pgsql/data/pg_lrstat/exports/mig_20260924.html
```

单位约定：**所有字节量输出为 MB（1MB = 1048576 字节），所有速度为 MB/s，时间为秒**。

## 2. 命令（共 5 个，均要求 superuser）

| 命令 | 用法 | 说明 |
| --- | --- | --- |
| `lrstat_start(name, persist)` | `SELECT lrstat_start('压测A', true)` | 开始采样；persist 默认 false。只重置测量锚点，不清历史——旧会话在环形内仍按名可导 |
| `lrstat_stop(name)` | `SELECT lrstat_stop()` | 停止采样。名字可省略（停当前会话）；传了则必须与 start 的一致 |
| `lrstat_export(name, format)` | `SELECT lrstat_export('压测A')` | 导出报告并**直接写文件**到 `$PGDATA/pg_lrstat/exports/<name>.<format>`，返回绝对路径；format='html'（默认）或 'json'。查找顺序：归档文件（persist 会话，重启后可用）→ 内存环形（任意最近会话，含 persist=false）；都没有则报错 |
| `lrstat_delete(name)` | `SELECT lrstat_delete('压测A')` | 删除归档会话文件（persist 会话）；运行中的会话拒绝删除 |
| `pg_lrstat_reset()` | `SELECT pg_lrstat_reset()` | 强制清除当前内存数据（不影响归档文件） |

报告内容（HTML）：分析结论卡（瓶颈判定、四速率、积压构成、追平预估、50/100/200GB 容量推算）、Session 卡、**Targets 卡**（每个目标的状态/平均速率/最新 LSN）、速率折线图（gen/send/apply 三线）、原始样本表。

## 3. 视图总览（9 个）

| 视图 | 粒度 | 用途 |
| --- | --- | --- |
| `pg_lrstat_info` | 1 行 | 健康自检 + 当前会话状态 |
| `pg_lrstat_send_stat` | 发送端一目标**一轮**一行 | 发送端逐轮时序：状态/水位/积压/本轮速率 |
| `pg_lrstat_recv_stat` | 接收端一 worker**一轮**一行 | 接收端逐轮时序 |
| `pg_lrstat_cluster_stat` | 一复制对**一轮**一行 | **两端合成逐轮时序，日常巡检只看这个** |
| `pg_lrstat_send_history` | 一目标一轮一行 | 发送端原始 LSN 样本（始终记录） |
| `pg_lrstat_recv_history` | 一目标一轮一行 | 接收端原始 LSN 样本（始终记录） |
| `pg_lrstat_session_stat` | 一会话×目标一行 | 按会话名查历史统计（总量/平均速率） |
| `pg_lrstat_send_rate_history` | 一目标一间隔一行 | 发送端逐间隔速率（窗口函数现算） |
| `pg_lrstat_recv_rate_history` | 一目标一间隔一行 | 接收端逐间隔速率（窗口函数现算） |

stat 三视图反映**当前会话窗口**；会话结束后冻结显示最终值，新会话 start 后重新锚定。历史会话用 `lrstat_export(name)`（文件或内存），报告自带佐证数据。

## 4. 视图字段详解

### 4.1 `pg_lrstat_info` — 健康自检（1 行）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `loaded` | bool | 扩展是否已加载（shared_preload_libraries）；f 时其他视图全空 |
| `session_name` | text | 当前/最近一次会话名；从未 start 过为 NULL |
| `session_state` | text | idle（从未 start）/ running / stopped |
| `session_truncated` | bool | t = 历史环形写满，覆盖过最旧样本 |
| `session_degraded` | bool | t = persist 会话文件写入失败，落盘不完整 |
| `session_start_ts` | timestamptz | 当前会话开始时间 |
| `session_stop_ts` | timestamptz | 会话结束时间；运行中为 NULL |
| `sample_interval_ms` | int8 | 采样周期（毫秒）——瞬时速率的时间粒度 |
| `last_round_ts` | timestamptz | 采样 worker 上一轮时间 |
| `last_round_ok` | bool | 上一轮采样是否成功 |
| `last_round_error` | text | 上一轮失败时的错误信息 |
| `nrounds` | int8 | 累计采样轮数 |
| `dropped_samples` | int8 | 因目标槽满被丢弃的目标数；>0 需调大 max_targets |
| `remote_poll` | bool | 是否启用接收端→发送端轮询 |
| `archived_session_names` | text[] | 归档会话名列表（persist 会话），供 export/delete 按名使用 |

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

> 环形写满覆盖最旧（`info.session_truncated = true`）。**数据生命周期**：新会话 start 只重置测量锚点，不清历史——已结束的会话（含 persist=false）在环形内仍可 `lrstat_export(name)` 导出，直到被更新的采样自然挤出环形（默认约 2880 样本/目标）。要**永久**追溯的会话用 `persist=true`（落文件，重启后也能导出）。

## 5. 速率是怎么算的

```
stat 视图的 *_mbps = 相邻两个原始样本作差 ÷ 间隔（每轮一行）
报告的 avg        = （末样本 − 会话锚点样本）÷ 会话时长（含窗口截断）
```
stat 视图自 v2.1 起是**逐轮时序**：每个采样周期、每个目标一行。

- 会话锚点 = 会话内**首个含有效位置的样本**（全零样本不锚定，避免把 WAL 历史位置当增量）
- 应用位置只在事务提交边界推进——大事务期间 `applied` 不动、提交时跳变，属正常内核行为
- 瞬时回答"刚过去一个间隔怎么样"，平均回答"整场会话怎么样"
- 速率是 LSN（页码）差，不是网线字节；与带宽对比需考虑解码膨胀率

**可验证性**：报告/视图中的每条速率都能从 `pg_lrstat_recv_history` / `pg_lrstat_send_history` 手工重算：

```sql
-- 例：手工重算 apply 平均速率（与 cluster_stat.apply_avg 一致）
WITH h AS (SELECT ts, applied_lsn FROM pg_lrstat_recv_history
            WHERE name='asub' AND applied_lsn>'0/0' ORDER BY ts)
SELECT pg_wal_lsn_diff(max(applied_lsn), min(applied_lsn))/1048576
       / extract(epoch FROM max(ts)-min(ts)) AS apply_mbps
  FROM h;
```

export 报告底部的 **Evidence** 区直接给出四张佐证表（send/recv 原始样本、send/recv 逐间隔速率），报告头部的分析结论即由此推导。

## 6. 配置（GUC）

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

## 7. 典型工作流

```sql
-- 迁移前评估：起会话，跑一轮试迁移，看应用速率和容量推算
SELECT lrstat_start('mig_eval', true);
-- ... 跑 10 分钟代表性负载 ...
SELECT lrstat_stop('mig_eval');
SELECT lrstat_export('mig_eval', 'json');   -- 容量表：50/100/200GB 需要多久

-- 日常巡检：瓶颈在哪、还要多久追平
SELECT recv_name, bottleneck, round(backlog_total::numeric,1) 积压MB,
       round(catchup_total_secs) 追平秒
FROM pg_lrstat_cluster_stat;

-- 事后分析：导出那次会话的报告（含佐证数据）
SELECT lrstat_export('mig_eval');
-- 报告 Evidence 区四张表：send/recv 原始样本 + send/recv 逐间隔速率，
-- 与 Analysis 的结论一一对应，可手工重算核对

-- 空间占用风险：槽扣住了多少 WAL
SELECT slot_name, round(retained_wal::numeric,0) MB, wal_status
FROM pg_lrstat_send_stat ORDER BY retained_wal DESC NULLS LAST;
```

## 8. 常见问题

| 问题 | 答案 |
| --- | --- |
| 视图全空 | `info.loaded = f`：没预加载，检查 shared_preload_libraries 并重启 |
| 速率全是 NULL | 会话刚开始还没有两个有效样本，等一个采样周期 |
| cluster_stat 没数据 | 没有活跃订阅，或 remote_state = unreachable（检查订阅连接串可达性） |
| apply 速率 = 0 但积压在涨 | 应用被堵（锁冲突最常见）：拿 recv_stat 的 worker_pid 查 pg_stat_activity 的 wait_event |
| applied 长时间不动然后突然跳 | 大事务：应用位置只在提交边界推进，正常 |
| 想看已结束的会话 | `lrstat_export(name)` 即可：环形内直接内存导出（含 persist=false）；被挤出环形或重启后仅 persist 会话可从文件导出 |
| 四个速率相等 | 健康！下游跟得上，都被"生成"定节奏 |
| round(x, 1) 报错 | 视图速率/积压列是 float8，需 `round(x::numeric, 1)` |
