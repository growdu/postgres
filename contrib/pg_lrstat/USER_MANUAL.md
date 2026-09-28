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
| `lrstat_start(name, persist)` | `SELECT lrstat_start('压测A', true)` | 开始会话；persist 默认 false（不写文件）。会清空上一会话的内存数据 |
| `lrstat_stop(name)` | `SELECT lrstat_stop('压测A')` | 结束会话；名字必须与 start 一致否则报错 |
| `lrstat_export(name, format)` | `SELECT lrstat_export('压测A')` | 导出报告并**直接写文件**到 `$PGDATA/pg_lrstat/exports/<name>.<format>`，返回绝对路径；format='html'（默认）或 'json'。传归档会话名时直接读会话文件，重启后也能导 |
| `lrstat_delete(name)` | `SELECT lrstat_delete('压测A')` | 删除归档会话文件（persist 会话）；运行中的会话拒绝删除 |
| `pg_lrstat_reset()` | `SELECT pg_lrstat_reset()` | 强制清除当前内存数据（不影响归档文件） |

报告内容（HTML）：分析结论卡（瓶颈判定、四速率、积压构成、追平预估、50/100/200GB 容量推算）、Session 卡、**Targets 卡**（每个目标的状态/平均速率/最新 LSN）、速率折线图（gen/send/apply 三线）、原始样本表。

## 3. 视图总览（9 个）

| 视图 | 粒度 | 用途 |
| --- | --- | --- |
| `pg_lrstat_info` | 1 行 | 健康自检 + 当前会话状态 |
| `pg_lrstat_send_stat` | 发送端一连接一行 | 发送端全量状态（在发送端部署时看） |
| `pg_lrstat_recv_stat` | 接收端一 worker 一行 | 接收端全量状态 |
| `pg_lrstat_cluster_stat` | 一复制对一行 | **两端合成，日常巡检只看这个** |
| `pg_lrstat_send_history` | 一目标一轮一行 | 发送端原始 LSN 样本（始终记录） |
| `pg_lrstat_recv_history` | 一目标一轮一行 | 接收端原始 LSN 样本（始终记录） |
| `pg_lrstat_session_stat` | 一会话×目标一行 | 按会话名查历史统计（总量/平均速率） |
| `pg_lrstat_send_rate_history` | 一目标一间隔一行 | 发送端逐间隔速率（窗口函数现算） |
| `pg_lrstat_recv_rate_history` | 一目标一间隔一行 | 接收端逐间隔速率（窗口函数现算） |

stat 三视图反映**当前会话窗口**；会话结束后冻结显示最终值，下一次 start 清零。要看历史会话用 `session_stat` / `*_rate_history`（内存环形内）或 `lrstat_export(name)`（persist 会话文件）。

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

### 4.2 `pg_lrstat_send_stat` — 发送端（一连接一行，33 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 本行数据所属的当前会话名 |
| `slot_name` | text | 复制槽名（逻辑复制槽或物理复制连接名） |
| `plugin` | text | 逻辑解码插件名（pgoutput 等）；物理复制为空 |
| `temporary` | bool | 是否临时槽 |
| `active` | bool | 槽是否活跃（walsender 在用） |
| `sender_pid` | int4 | walsender 进程 PID；无连接为 NULL |
| `application_name` | text | 对端连接的应用名（订阅端为订阅名） |
| `client_addr` | text | 对端 IP 地址 |
| `state` | text | walsender 状态：streaming（稳态）/ catchup（追赶）/ startup 等 |
| `sync_state` | text | 同步复制角色：async / sync / quorum；async 时不影响吞吐判定 |
| `wal_status` | text | 槽的 WAL 保留状态：reserved（正常）/ extended / unreserved / **lost（对端太久没确认，WAL 已被回收，订阅会断）** |
| `safe_wal_size` | float8 | 距离 wal_status 变 lost 还能保留的 WAL 量（MB）；仅扩展态有值 |
| `sample_time` | timestamptz | 本行样本的采样时间 |
| `current_lsn` | pg_lsn | 发送端当前 WAL **生成**位置 |
| `sent_lsn` | pg_lsn | 已**发送**到对端的位置 |
| `confirmed_flush_lsn` | pg_lsn | 对端已**确认**的位置——发送端可安全回收的边界 |
| `backlog_unsent` | float8 | 未发送积压（current−sent，MB）——**解码/发送慢** |
| `backlog_inflight` | float8 | 已发送未收到（sent−对端反馈写位置，MB）——**网络在途** |
| `backlog_peer_unapplied` | float8 | 对端收到未应用（反馈写位置−反馈应用位置，MB）——**对端应用慢** |
| `backlog_total` | float8 | 总积压（current−对端反馈应用位置，MB） |
| `retained_wal` | float8 | 槽扣住的 WAL（current−restart_lsn，MB）——**磁盘占用风险** |
| `gen_instant` / `gen_avg` | float8 | WAL 生成速率：上一采样间隔 / 会话至今平均（MB/s） |
| `send_instant` / `send_avg` | float8 | 发送速率（MB/s） |
| `apply_instant` / `apply_avg` | float8 | 对端应用速率（按反馈水位计算，MB/s） |
| `spill_instant` / `spill_avg` | float8 | 解码溢写速率（MB/s）——大事务溢出到磁盘的速度 |
| `write_lag` | interval | 发送端观测的对端写延迟 |
| `flush_lag` | interval | 对端刷盘延迟 |
| `replay_lag` | interval | 对端回放延迟（物理复制语义） |
| `send_blocked` | bool | t = 有未发送积压且发送平均速率≈0——发送端堵住 |

### 4.3 `pg_lrstat_recv_stat` — 接收端（一 worker 一行，21 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 本行数据所属的当前会话名 |
| `recv_name` | text | 订阅名 |
| `worker_type` | text | apply（主应用 worker）/ table synchronization（初始同步 COPY） |
| `worker_pid` | int4 | worker 进程 PID |
| `leader_pid` | int4 | 并行 apply 的 leader PID；普通 worker 为 NULL |
| `relid` | oid | tablesync 正在同步的目标表 OID；apply worker 为 NULL |
| `sample_time` | timestamptz | 采样时间 |
| `received_lsn` | pg_lsn | 已从网络**收到**的位置 |
| `applied_lsn` | pg_lsn | 已**应用**的位置（提交边界——大事务提交时一次跳变是正常的） |
| `last_msg_send_time` | timestamptz | 对端最后一次发心跳的时间 |
| `last_msg_receipt_time` | timestamptz | 本端最后一次收到心跳的时间——两差过大说明链路断 |
| `backlog_apply` | float8 | 收到未应用（received−applied，MB） |
| `recv_instant` / `recv_avg` | float8 | 接收速率（MB/s） |
| `apply_instant` / `apply_avg` | float8 | 应用速率（MB/s）——**追平能力的关键数字** |
| `local_wal_instant` / `local_wal_avg` | float8 | 本地 WAL 写入速率（MB/s，接收端自身写放大观察） |
| `apply_error_count` | int8 | 应用累计错误数（pg_stat_subscription_stats） |
| `sync_error_count` | int8 | 初始同步累计错误数 |
| `apply_blocked` | bool | t = 有未应用积压且应用速率≈0——典型原因是锁冲突 |

### 4.4 `pg_lrstat_cluster_stat` — 两端合成（一复制对一行，32 列，**日常只看这个**）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 本行数据所属的当前会话名 |
| `recv_name` | text | 订阅名（链路标识） |
| `remote_state` | text | 发送端轮询状态：ok / stale（本轮预算内未完成）/ unreachable（连接失败退避中）/ n/a（未轮询） |
| `last_remote_poll_time` | timestamptz | 上次成功轮询发送端的时间 |
| `sample_time` | timestamptz | 接收端样本时间 |
| `send_current_lsn` | pg_lsn | 发送端当前 WAL 生成位置（远端轮询值） |
| `sent_lsn` | pg_lsn | 发送端已发送位置（远端轮询值） |
| `received_lsn` | pg_lsn | 接收端已收到位置 |
| `applied_lsn` | pg_lsn | 接收端已应用位置 |
| `confirmed_flush_lsn` | pg_lsn | 发送端槽的确认边界（远端轮询值） |
| `gen_instant` / `gen_avg` | float8 | 发送端 WAL 生成速率（MB/s） |
| `send_instant` / `send_avg` | float8 | 发送速率（MB/s） |
| `recv_instant` / `recv_avg` | float8 | 接收速率（MB/s） |
| `apply_instant` / `apply_avg` | float8 | 应用速率（MB/s） |
| `backlog_unsent` | float8 | 未发送积压（MB）——积压在发送端 |
| `backlog_inflight` | float8 | 网络在途（MB）——积压在网络 |
| `backlog_unapplied` | float8 | 收到未应用（MB）——积压在接收端 |
| `backlog_total` | float8 | 总积压（MB） |
| `retained_wal` | float8 | 发送端槽扣住的 WAL（MB）——磁盘风险 |
| `feedback_lag_mb` | float8 | 反馈滞后（received−发送端反馈写位置，MB）——反馈延迟的量纲 |
| `write_lag` / `flush_lag` / `replay_lag` | interval | 发送端观测的三段延迟 |
| `catchup_send_secs` | float8 | 仅未发送积压按发送速率追平所需秒数 |
| `catchup_total_secs` | float8 | 全部积压追平所需秒数（发送段+应用段）——**追平预估看这个** |
| `send_blocked` | bool | 发送端堵住（有未发送积压且速率≈0） |
| `apply_blocked` | bool | 接收端应用堵住 |
| `bottleneck` | text | **自动瓶颈判定**：send（发送慢）/ recv_apply（应用慢）/ network（在途占比过半）/ none（健康） |

判定规则：`send` = 发送速率落后于生成速率且未发送积压过半；`recv_apply` = 未应用积压过半；`network` = 在途积压过半；否则 none（四速率相等=下游跟得上，正常）。

### 4.5 `pg_lrstat_send_history` — 发送端原始样本（一目标一轮一行，12 列）

**始终记录**（会话外样本 `session_name` 为 NULL）。这是所有派生统计的最原始数据来源。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 样本所属会话名；NULL = 会话之外的空闲期样本 |
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

### 4.6 `pg_lrstat_recv_history` — 接收端原始样本（6 列）

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 同上 |
| `name` | text | 订阅名 |
| `ts` | timestamptz | 采样时间 |
| `received_lsn` | pg_lsn | 已收到位置 |
| `applied_lsn` | pg_lsn | 已应用位置（提交边界） |
| `local_wal_lsn` | pg_lsn | 接收端本地 WAL 写入位置 |

> 环形写满覆盖最旧（`info.session_truncated = true`）。`lrstat_start` 清空内存历史——旧会话只有 persist 会话留了文件，用 `lrstat_export(name)` 取回。

### 4.7 `pg_lrstat_session_stat` — 按会话汇总（13 列）

回答"**那次**会话跑得怎么样"。对 history 按会话×目标聚合，会话结束后仍可查（在内存环形内）。

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` | text | 会话名 |
| `name` | text | 目标名 |
| `n_samples` | int8 | 会话内样本数 |
| `first_ts` / `last_ts` | timestamptz | 会话内首/末样本时间（即实际测量区间） |
| `gen_mb` | numeric | 会话期间累计 WAL 生成量（MB） |
| `sent_mb` | numeric | 累计发送量（MB）；接收侧行为 NULL |
| `received_mb` | numeric | 累计接收量（MB）；发送侧行为 NULL |
| `applied_mb` | numeric | 累计应用量（MB）；发送侧行为 NULL |
| `gen_mbps` | numeric | 会话平均生成速率（MB/s）；区间不足为 NULL |
| `send_mbps` | numeric | 平均发送速率（MB/s） |
| `recv_mbps` | numeric | 平均接收速率（MB/s） |
| `apply_mbps` | numeric | 平均应用速率（MB/s）——**容量规划的依据** |

### 4.8 `pg_lrstat_send_rate_history` / `pg_lrstat_recv_rate_history` — 逐间隔速率

对 history 用窗口函数现算的**每两次采样之间**的速率（不额外存储）。画速率曲线直接查。

send_rate_history（9 列）：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `session_name` / `name` / `ts` | — | 本间隔结束时刻的样本 |
| `interval_secs` | numeric | 与上一样本的间隔秒数 |
| `gen_mbps` | numeric | 本间隔平均生成速率（MB/s） |
| `send_mbps` | numeric | 本间隔平均发送速率（MB/s） |
| `peer_apply_mbps` | numeric | 对端反馈应用速率（MB/s） |
| `spill_mb` | numeric | 本间隔解码溢写增量（MB）——突增=该轮解码压力大 |
| `stream_mb` | numeric | 本间隔流式解码增量（MB） |

recv_rate_history（7 列）：`session_name / name / ts / interval_secs` 同上，加：

| 列 | 类型 | 含义 |
| --- | --- | --- |
| `recv_mbps` | numeric | 本间隔平均接收速率（MB/s） |
| `apply_mbps` | numeric | 本间隔平均应用速率（MB/s） |
| `local_wal_mbps` | numeric | 本地 WAL 写入速率（MB/s） |

会话首行（无前一样本）rate 为 NULL；LSN 为 0 的样本（目标尚未产生有效位置）自动跳过。

## 5. 速率是怎么算的

```
瞬时速率 instant = （最新样本 − 前一样本）÷ 采样间隔
平均速率 avg     = （最新样本 − 会话锚点样本）÷ 已历时
逐间隔速率 rate_history = 相邻两个原始样本作差 ÷ 间隔（视图现算）
```

- 会话锚点 = 会话内**首个含有效位置的样本**（全零样本不锚定，避免把 WAL 历史位置当增量）
- 应用位置只在事务提交边界推进——大事务期间 `applied` 不动、提交时跳变，属正常内核行为
- 瞬时回答"刚过去一个间隔怎么样"，平均回答"整场会话怎么样"
- 速率是 LSN（页码）差，不是网线字节；与带宽对比需考虑解码膨胀率

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

-- 事后分析：那次会话到底跑成什么样
SELECT * FROM pg_lrstat_session_stat WHERE session_name = 'mig_eval';
SELECT to_char(ts,'HH24:MI:SS'), apply_mbps
FROM pg_lrstat_recv_rate_history
WHERE session_name = 'mig_eval' ORDER BY ts;

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
| history 里 session_name 是 NULL | 会话之外的空闲期采样，正常；过滤掉即可 |
| 想看已结束多时的会话 | 当时 persist=true 了吗？是则 `lrstat_export(name)` 从文件重建；否则内存环形已覆盖 |
| 四个速率相等 | 健康！下游跟得上，都被"生成"定节奏 |
| round(x, 1) 报错 | 视图速率/积压列是 float8，需 `round(x::numeric, 1)` |
