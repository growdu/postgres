# pg_lrstat 用户手册

版本 2.0（会话模型）｜设计文档：[DESIGN.md](DESIGN.md)

---

## 1. 这是干什么的

pg_lrstat 用来回答复制链路的四个运维问题：**货压在哪？谁慢？从什么时候开始坏的？还要多久清完？**

工作方式很简单：

```sql
SELECT lrstat_start('mig_20260924', persist := true);  -- 开始测量
-- ……运行业务负载，期间随时查视图看速率
SELECT lrstat_stop('mig_20260924');                     -- 结束测量
SELECT lrstat_export('mig_20260924');                   -- 导出报告
```

一次 start 到 stop 就是一个**会话**，会话期间和结束后都能查数据。

## 2. 安装

### 2.1 构建

```sh
# 随 PG 源码树
make -C contrib/pg_lrstat install

# 或 meson
ninja -C build contrib/pg_lrstat/pg_lrstat.dylib && ninja -C build install
```

### 2.2 启用

```ini
# postgresql.conf（发送端和接收端都加）
shared_preload_libraries = 'pg_lrstat'
```

重启后：

```sql
CREATE EXTENSION pg_lrstat;
SELECT loaded FROM pg_lrstat_info;  -- 必须为 t
```

`loaded = f` 说明没预加载，检查配置后重启。

### 2.3 装在哪端

- **只装接收端**：推荐。凭订阅连接串自动轮询发送端，`cluster_stat` 合成两端，发送端什么都不用装
- **只装发送端**：只能看发送端单侧（send_stat）
- **两端都装**：最全，但通常不必要

## 3. 命令（共 5 个）

| 命令 | 用法 | 说明 |
| --- | --- | --- |
| `lrstat_start(name, persist)` | `SELECT lrstat_start('压测A', true)` | 开始会话。persist 默认 false（不写文件） |
| `lrstat_stop(name)` | `SELECT lrstat_stop('压测A')` | 结束会话。名字必须与 start 一致 |
| `lrstat_export(name, format)` | `SELECT lrstat_export('压测A')` | 导出报告。format='html'（默认）或 'json' |
| `lrstat_delete(name)` | `SELECT lrstat_delete('压测A')` | 删除已归档会话文件（persist 会话） |
| `pg_lrstat_reset()` | `SELECT pg_lrstat_reset()` | 强制清除当前数据（不影响归档文件） |

全部要求 superuser。

## 4. 视图（只有 6 个）

日常巡检**只需要盯 `cluster_stat` 一个**。

### 4.1 `pg_lrstat_info` — 健康自检（1 行）

| 列 | 含义 |
| --- | --- |
| `loaded` | 是否已加载；f = 其他视图全空 |
| `session_name` | 当前/最近会话名 |
| `session_state` | idle / running / stopped |
| `session_truncated` | t = 日志超上限有丢失 |
| `session_degraded` | t = 文件写失败 |
| `session_start_ts` / `session_stop_ts` | 会话起止时间 |
| `sample_interval_ms` | 采样周期（瞬时速率的粒度） |
| `last_round_ok` / `last_round_error` | 采样 worker 上一轮状态 |
| `dropped_samples` | >0 说明目标槽满了，需调大 max_targets |
| `remote_poll` | 是否启用远端轮询 |
| `archived_session_names` | 已归档会话名列表（export/delete 用） |

### 4.2 `pg_lrstat_send_stat` — 发送端（一连接一行）

关键列（完整 37 列见 `\d pg_lrstat_send_stat`）：

| 列组 | 列 | 看什么 |
| --- | --- | --- |
| 标识 | `slot_name` `state` `active` `wal_status` | 哪个槽、walsender 在不在线、WAL 保留状态 |
| 位点 | `current_lsn` `sent_lsn` `confirmed_flush_lsn` | WAL 写到哪、发到哪 |
| 积压(MB) | `backlog_unsent` `backlog_total` `retained_wal` | 没发货多少、总积压、槽扣住多少 |
| 速率(MB/s) | `gen_instant/avg` `send_instant/avg` | 生成快不快、发送跟不跟得上 |
| 判定 | `send_blocked` | t = 发送堵住了 |

### 4.3 `pg_lrstat_recv_stat` — 接收端（一 worker 一行）

| 列组 | 列 | 看什么 |
| --- | --- | --- |
| 标识 | `recv_name` `worker_type` `worker_pid` | 哪个订阅、什么类型的 worker |
| 位点 | `received_lsn` `applied_lsn` | 收到哪、应用到哪 |
| 积压 | `backlog_apply` | 已收未应用多少 MB |
| 速率(MB/s) | `recv_instant/avg` `apply_instant/avg` | 收货多快、上架多快 |
| 判定 | `apply_blocked` | t = 应用堵住了（查锁） |

### 4.4 `pg_lrstat_cluster_stat` — **两端合成，日常只看这个**

| 列组 | 列 | 看什么 |
| --- | --- | --- |
| 标识 | `recv_name` `remote_state` | 哪个订阅、能否连上发送端 |
| 速率(MB/s) | `gen` `send` `recv` `apply` 各 instant+avg | **四速率相等=健康；第一个掉队的=瓶颈** |
| 积压(MB) | `backlog_unsent/inflight/unapplied/total/retained_wal` | 没发货/在路上/到了没上架/总量/槽扣住 |
| 追平 | `catchup_send_secs` `catchup_total_secs` | 还要多久追平（秒） |
| 判定 | `send_blocked` `apply_blocked` `bottleneck` | 哪堵了 + 自动瓶颈判定（send/recv_apply/network/none） |

### 4.5 `pg_lrstat_send_history` — 发送端逐间隔序列（stop 后可查）

| 列 | 含义 |
| --- | --- |
| `name` `ts` | 哪个目标、什么时间 |
| `d_current` `d_sent` | 本间隔生成了多少 MB、发送了多少 MB |
| `d_spill` `d_stream` | 解码溢写/流式多少 MB |

### 4.6 `pg_lrstat_recv_history` — 接收端逐间隔序列（stop 后可查）

| 列 | 含义 |
| --- | --- |
| `name` `ts` | 同上 |
| `d_received` `d_applied` | 本间隔收了多少 MB、应用了多少 MB |

## 5. 速率是怎么算的

```
瞬时速率 instant = （最新样本 − 前一样本）÷ 采样间隔（默认 30 秒）
平均速率 avg     = （最新样本 − 会话起点样本）÷ 已历时
```

- 瞬时回答"刚刚 30 秒怎么样"
- 平均回答"这轮从头到现在平均多少"
- 速率单位 **MB/s**，积压单位 **MB**，追平预估单位**秒**
- 速率列是 NULL 不是 0：刚启动或采样中断，等一个采样周期再看

## 6. GUC 配置

| GUC | 默认 | 说明 |
| --- | --- | --- |
| `sample_interval` | `30s` | 采样周期 = 瞬时速率粒度（可调 1s） |
| `session_max_samples` | `2880` | 会话日志容量（30s≈24h；0=关） |
| `max_targets` | `32` | 目标槽数量 |
| `remote_poll` | `on` | 接收端是否轮询发送端 |
| `remote_poll_budget` | `500ms` | 远端轮询时间预算 |
| `database` | `postgres` | 采样 worker 连接的库 |

改 `sample_interval` 等运行时参数用 `SELECT pg_reload_conf();` 即可，不用重启。

## 7. 完整使用示例

```sql
-- 1. 开始测量
SELECT lrstat_start('mig_20260924', persist := true);

-- 2. 负载期间随时看两端合成视图
SELECT recv_name,
       round(gen_avg::numeric,1)   AS 生成,
       round(send_avg::numeric,1)  AS 发送,
       round(apply_avg::numeric,1) AS 应用,
       round(backlog_total,1)      AS 总积压MB,
       round(catchup_total_secs)   AS 追平秒,
       bottleneck                  AS 瓶颈
FROM pg_lrstat_cluster_stat;

-- 3. 结束测量
SELECT lrstat_stop('mig_20260924');

-- 4. 看逐间隔趋势
SELECT ts, round(d_sent,1) AS 每段发送MB
FROM pg_lrstat_send_history ORDER BY ts;

-- 5. 导出报告（HTML 包含图表+分析结论）
SELECT lrstat_export('mig_20260924');
-- 返回 HTML 文本，用 \o report.html 保存后浏览器打开
```

## 8. 常见问题

| 问题 | 答案 |
| --- | --- |
| 视图全空 | `info.loaded = f`，没预加载，检查 shared_preload_libraries |
| 速率全是 NULL | 刚 start 还没两个样本，等一个采样周期 |
| cluster_stat 没数据 | 没有活跃的订阅/复制连接，或 remote_state = unreachable |
| apply_rate = 0 但积压在涨 | 应用堵住了（锁冲突），查 worker_pid 的 wait_event |
| history 视图空 | 会话还没 stop，或 session_max_samples = 0 |
| 四个速率相等 | 正常！下游跟得上，都被"生成"定节奏 |
| 会重启后数据丢了 | persist=false 的会话随重启消失，这是设计；要保留就用 persist=true |
| 磁盘上有 pg_lrstat 目录 | persist 会话的文件，用 lrstat_delete(name) 清理 |
