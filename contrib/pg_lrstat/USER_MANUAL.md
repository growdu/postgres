# pg_lrstat 用户手册

pg_lrstat 测量**复制链路性能**：四个速率（生成/发送/接收/应用）、三段积压、瓶颈自动判定、容量外推。部署在**订阅端**即可（发布端零安装）。每个数字都可从原始样本手工复算。

## 1. 30 秒上手

```sql
-- 0. postgresql.conf: shared_preload_libraries = 'pg_lrstat'，重启后
CREATE EXTENSION pg_lrstat;

-- 1. 开始采样（无参数；自动清空上一会话数据）
SELECT lrstat_start();

-- 2. 迁移/压测期间随时看链路健康（日常只看这一个视图）
SELECT ts, recv_name, bottleneck,
       round(gen_mbps::numeric,2)   AS 生成,
       round(send_mbps::numeric,2)  AS 发送,
       round(apply_mbps::numeric,2) AS 应用,
       round(backlog_total::numeric,1) AS 总积压MB
FROM pg_lrstat_cluster_stat ORDER BY ts DESC LIMIT 10;

-- 3. 停止采样（无参数；数据冻结可查）
SELECT lrstat_stop();

-- 4. 导出报告 = 持久化（写文件，返回路径，浏览器直接打开）
SELECT lrstat_export();
-- → /var/lib/pgsql/data/pg_lrstat/exports/session.html
```

单位约定：**字节量 = MB（1MB = 1,048,576 字节），速率 = MB/s**。

### 部署与升级（重要）

更换版本后按此顺序操作，避免新旧混跑：

```sh
# 1. 替换 .so 后必须重启实例——运行中的 postmaster 仍持有旧代码在内存
pg_ctl restart -D $PGDATA

# 2. 已有 2.0 目录的实例升级到 2.1（函数签名变了）：
psql -c "ALTER EXTENSION pg_lrstat UPDATE;"
#    或彻底重建：
psql -c "DROP EXTENSION pg_lrstat CASCADE; CREATE EXTENSION pg_lrstat;"
```

内置防护：安装/升级脚本引用 `pg_lrstat_layout_version()` 哨兵——若实例未重启（内存中还是旧库），`CREATE/ALTER EXTENSION` 会直接报 "could not find function" 而不是静默混版本；`CREATE EXTENSION` 会清空共享内存中的残留数据。

## 2. 数据生命周期（一张图）

```
 lrstat_start()              lrstat_stop()           下一次 lrstat_start()
─────┬──────────────────────────┬──────────────────────────┬─────────►
     │ ◄── 采样进行中 ──►       │ ◄── 数据冻结 ──►        │
     清空上一会话数据            可查询 / 可按名 export      又清空重来
     视图逐轮增长                                          (旧数据没了)
                                │
                                └─ 要留档? 在这里 export ──► 报告文件(永久)
```

- **没有 start 就没有数据**——视图为空是正常的。
- **stop 后立即 export**——报告文件是唯一持久产物；环形内也可按名补导（`lrstat_export('sess_3')`，名字查 `info` 视图）。
- 实例重启后内存清空，已导出的报告文件仍在。

## 3. 命令（4 个，均要求 superuser）

| 命令 | 用法 | 说明 |
| --- | --- | --- |
| `lrstat_start()` | `SELECT lrstat_start()` | 开始采样；清空上一会话全部数据；返回自动名 `sess_<n>` |
| `lrstat_stop()` | `SELECT lrstat_stop()` | 停止采样；数据冻结在内存 |
| `lrstat_export(name, format)` | `SELECT lrstat_export()` | 写报告文件到 `$PGDATA/pg_lrstat/exports/`，返回绝对路径。不传 name 导当前/最近会话；传 `sess_N` 按名补导（环形内）。format='html'（默认）/'json' |
| `pg_lrstat_reset()` | `SELECT pg_lrstat_reset()` | 强制清内存数据（不动报告文件；删报告直接 rm） |

## 4. 六个视图

```
                    ┌────────────────────────────────┐
                    │  LRHistoryEntry[] (唯一数据源)  │
                    └───┬──────┬──────┬──────┬──────┬┘
                        ▼      ▼      ▼      ▼      ▼
                   send_   recv_  cluster send_  recv_
                   stat    stat   _stat   history history
                   (逐轮时序, 相邻样本差分)  (原始样本直出)
                                                info (会话/健康, 1行)
```

### 4.1 `pg_lrstat_info` — 健康自检（1 行，15 列）

| 列 | 含义 |
| --- | --- |
| `loaded` | f = 没预加载（其他视图全空） |
| `session_name` / `session_state` | 当前/最近会话名；idle / running / stopped |
| `session_truncated` | t = 历史环形写满，覆盖过最旧样本 |
| `session_degraded` | 保留位（正常恒 f） |
| `session_start_ts` / `session_stop_ts` | 会话起止时间 |
| `sample_interval_ms` | 采样周期（= 时序粒度） |
| `last_round_ts` / `last_round_ok` / `last_round_error` | 上一轮采样状态 |
| `nrounds` / `dropped_samples` | 累计轮数 / 目标槽满丢弃数（>0 调大 max_targets） |
| `remote_poll` | 是否启用发送端轮询 |
| `exported_report_names` | **已导出的报告名列表**（exports 目录） |

### 4.2 `pg_lrstat_send_stat` — 发送端时序（一目标一轮一行，27 列）

| 列 | 含义 |
| --- | --- |
| `slot_name` `ts` | 目标（槽/订阅名）与本轮时间 |
| `plugin` `temporary` `application_name` `client_addr` | 直通属性（取自当前槽位，不随轮存储） |
| `active` `sender_pid` | 本轮 walsender 是否在线 / PID |
| `state` `sync_state` `wal_status` | 本轮 walsender 状态 / 同步角色 / 槽保留状态（**lost = 订阅会断**） |
| `current_lsn` `sent_lsn` `confirmed_flush_lsn` | 本轮生成/发送/确认水位 |
| `backlog_unsent` | 本轮未发送积压（MB）——解码/发送慢 |
| `backlog_inflight` | 已发送未收到（MB）——网络在途 |
| `backlog_peer_unapplied` | 对端收到未应用（MB）——对端应用慢 |
| `backlog_total` `retained_wal` | 总积压 / 槽扣住 WAL（MB，磁盘风险） |
| `gen_mbps` `send_mbps` `apply_mbps` | 本轮速率（首行 NULL，空闲轮 0） |
| `spill_mb` | 本轮解码溢写增量——突增=解码压力大 |
| `write_lag` `flush_lag` `replay_lag` | 本轮发送端观测三段延迟 |
| `send_blocked` | 本轮有未发送积压且发送速率≈0 |

### 4.3 `pg_lrstat_recv_stat` — 接收端时序（一 worker 一轮一行，17 列）

| 列 | 含义 |
| --- | --- |
| `recv_name` `ts` | 订阅名与本轮时间 |
| `worker_type` | apply / table synchronization |
| `worker_pid` `leader_pid` `relid` | 本轮 worker PID / 并行 leader / tablesync 目标表 OID |
| `received_lsn` `applied_lsn` | 本轮收到/应用水位（applied = origin∪反馈的单调融合，提交边界） |
| `last_msg_send_time` `last_msg_receipt_time` | 本轮心跳——差过大=链路断 |
| `backlog_apply` | 本轮收到未应用（MB） |
| `recv_mbps` `apply_mbps` `local_wal_mbps` | 本轮速率（首行 NULL） |
| `apply_error_count` `sync_error_count` | 本轮累计错误数 |
| `apply_blocked` | 本轮有未应用积压且速率≈0——典型是锁冲突 |

### 4.4 `pg_lrstat_cluster_stat` — 两端合成时序（一链路一轮一行，26 列，**日常只看这个**）

每行 = 接收端一轮 × 时间上最近的发送端轮询样本配对。

| 列 | 含义 |
| --- | --- |
| `recv_name` `ts` | 链路标识与本轮时间 |
| `remote_state` | ok / stale（本轮预算内未完成）/ unreachable（连接失败退避中）/ n/a |
| `send_current_lsn` `sent_lsn` `confirmed_flush_lsn` | 发送端水位（配对样本） |
| `received_lsn` `applied_lsn` | 接收端水位 |
| `gen_mbps` `send_mbps` | 发送端本轮生成/发送速率 |
| `recv_mbps` `apply_mbps` | 接收端本轮接收/应用速率 |
| `backlog_unsent` `backlog_inflight` `backlog_unapplied` `backlog_total` | 本轮三段积压与总量（MB） |
| `retained_wal` `feedback_lag_mb` | 槽扣住 WAL / 反馈滞后（MB） |
| `write_lag` `flush_lag` `replay_lag` | 发送端观测三段延迟 |
| `catchup_send_secs` `catchup_total_secs` | 按本轮速率的追平预估（秒） |
| `send_blocked` `apply_blocked` | 本轮发送/应用堵住 |
| `bottleneck` | **本轮瓶颈判定**：send / recv_apply / network / none |

### 4.5 `pg_lrstat_send_history` — 发送端原始样本（一目标一轮一行，11 列）

| 列 | 含义 |
| --- | --- |
| `name` `ts` | 目标与采样时间 |
| `current_lsn` | 发送端当前 WAL 生成位置 |
| `sent_lsn` | 已发送位置——相邻两行作差即该轮发送量 |
| `peer_recv_lsn` `peer_flush_lsn` `peer_applied_lsn` | 接收端反馈的收到/落盘/应用三水位 |
| `confirmed_flush_lsn` | 槽确认边界（发送端可回收的边界） |
| `restart_lsn` | 槽保水起点——`current − restart` 即 retained_wal |
| `spill_bytes` `stream_bytes` | 解码溢写/流式累计字节 |

### 4.6 `pg_lrstat_recv_history` — 接收端原始样本（5 列）

| 列 | 含义 |
| --- | --- |
| `name` `ts` | 订阅名与采样时间 |
| `received_lsn` | 已收到位置 |
| `applied_lsn` | 已应用位置（提交边界） |
| `local_wal_lsn` | 接收端本地 WAL 写入位置 |

## 5. 速率是怎么算的（公式与算例）

### 5.1 两个基本公式

```
逐轮速率 (stat 视图 *_mbps):
    rate(i) = pg_wal_lsn_diff(LSN_i, LSN_{i-1}) / 1048576 / (ts_i − ts_{i-1})
    同一目标相邻两个样本; 首行 NULL; 空闲轮 0

会话平均 (报告 analysis *_avg):
    avg = pg_wal_lsn_diff(LSN_末, LSN_锚点) / 1048576 / (ts_末 − ts_锚点)
    锚点/末样本 = 会话内首个/末个含有效位置的样本 (全零样本两头都不参与)
```

### 5.2 每条速率用哪个 LSN

| 速率列 | 差分字段 | 含义 |
| --- | --- | --- |
| `gen_mbps` | send_history.`current_lsn` | 发送端 WAL 生成速率 |
| `send_mbps` | send_history.`sent_lsn` | 发送速率 |
| `apply_mbps`（send 侧） | send_history.`peer_applied_lsn` | 反馈观测的对端应用速率 |
| `recv_mbps` | recv_history.`received_lsn` | 接收速率 |
| `apply_mbps`（recv 侧） | recv_history.`applied_lsn` | 应用速率 |
| `local_wal_mbps` | recv_history.`local_wal_lsn` | 接收端本地 WAL 写入速率 |
| `spill_mb`（增量列） | send_history.`spill_bytes` | 本间隔解码溢写量（MB，不除时间） |

### 5.3 积压、追平与容量公式

```
backlog_unsent     = current_lsn − sent_lsn          （发送端没发出去的）
backlog_inflight   = sent_lsn − received_lsn         （网络上在途的）
backlog_unapplied  = received_lsn − applied_lsn      （收到但没应用的）
backlog_total      = current_lsn − applied_lsn       （端到端总积压）
retained_wal       = current_lsn − restart_lsn       （槽扣住的磁盘空间）

catchup_send_secs  = backlog_unsent ÷ send 速率
catchup_total_secs = backlog_unsent ÷ send 速率
                     + (inflight + unapplied) ÷ apply 速率

sync_50g_secs      = 50 × 1024 ÷ apply_avg           （100G/200G 严格 2×/4×）
```

### 5.4 完整算例（真实数据）

一次真实测量（1s 采样，两笔小事务）的 recv_history：

| ts | applied_lsn |
| --- | --- |
| 14:12:29.307 | 0/8FE9D88 |
| 14:12:30~33 | 0/8FE9D88（4 行不变，空闲轮） |
| **14:12:34.332** | **0/9001E10**（事务提交，跳变） |
| 14:12:35~37 | 0/9001E10（3 行不变） |

**逐轮速率**（recv_stat 中 34.332 行）：

```
分子 = pg_wal_lsn_diff('0/9001E10','0/8FE9D88') = 0x18088 = 98,440 B = 0.0939 MB
分母 = 34.332 − 33.328 = 1.004 s
apply_mbps = 0.0939 ÷ 1.004 = 0.0935 MB/s     ← 视图输出 0.0935 ✓
```

**会话平均**（报告 analysis）：

```
分子 = 同上 0.0939 MB
分母 = 37.344 − 29.307 = 8.037 s（首末有效样本）
apply_avg = 0.0939 ÷ 8.037 = 0.0117 MB/s      ← 报告输出 0.0117 ✓
```

**容量外推**：

```
sync_50g_secs = 50 × 1024 ÷ 0.0117 = 4,382,793 s ≈ 1,217 小时   ← 报告 ✓
```

### 5.5 手工复算 SQL（可验证性）

```sql
-- 复算逐轮 apply 速率（与 recv_stat.apply_mbps 逐位一致）
WITH h AS (SELECT ts, applied_lsn, lag(applied_lsn) OVER w pl, lag(ts) OVER w pt
             FROM pg_lrstat_recv_history WHERE name='asub' WINDOW w AS (ORDER BY ts))
SELECT ts, round((pg_wal_lsn_diff(applied_lsn, pl)/1048576
                  / extract(epoch FROM ts-pt))::numeric, 4)
  FROM h WHERE pl IS NOT NULL ORDER BY ts;

-- 复算会话平均（与报告 analysis.apply_avg 一致）
WITH h AS (SELECT ts, applied_lsn FROM pg_lrstat_recv_history
            WHERE name='asub' AND applied_lsn > '0/0'
              AND ts BETWEEN (SELECT session_start_ts FROM pg_lrstat_info)
                         AND (SELECT session_stop_ts  FROM pg_lrstat_info))
SELECT round((pg_wal_lsn_diff(max(applied_lsn), min(applied_lsn))/1048576
              / extract(epoch FROM max(ts)-min(ts)))::numeric, 4) FROM h;
```

### 5.6 特殊情形（都不是 bug）

| 情形 | 表现 | 原因 |
| --- | --- | --- |
| 会话首行速率 | NULL | 无前样本可差分 |
| 负载结束后的行 | 0.000 | 位置不推进 |
| 大事务期间 applied 不动，提交时跳变 | 该轮 apply_mbps 突大 | 应用位只在提交边界推进（内核语义） |
| 会话开始前已有积压 | 首轮 apply 速率即很高 | 追赶存量属于会话期间的真实工作 |
| applied 与 origin 微小偏差 | 融合了更及时的反馈 apply 位 | 两者都是提交边界 |

## 6. 输出报告内容（lrstat_export）

报告写文件到 `$PGDATA/pg_lrstat/exports/<name>.<format>`；HTML 双击即看，JSON 给机器。

### 6.1 HTML 五个区块（自上而下）

```
┌────────────────────────────────────────────────────┐
│ ① Analysis（最顶部）                                │
│    瓶颈徽章: send(红)/recv_apply(黄)/network(蓝)/    │
│              none(绿)                               │
│    四速率 avg: gen/send/recv/apply                  │
│    三段积压: unsent/inflight/unapplied + total      │
│    追平预估 + 净追平速率(负值=CANNOT catch up)        │
│    容量表: 当前积压 / 50G / 100G / 200G 耗时          │
├────────────────────────────────────────────────────┤
│ ② Chart  gen/send/apply 逐轮折线 (内嵌 SVG)         │
├────────────────────────────────────────────────────┤
│ ③ Session 会话名/起止/样本数/truncated/degraded      │
├────────────────────────────────────────────────────┤
│ ④ Targets 每目标: 侧别/状态/平均速率/最新LSN          │
├────────────────────────────────────────────────────┤
│ ⑤ Evidence（底部佐证区, 四张表）                     │
│    send history / recv history (原始样本)           │
│    send rate / recv rate (逐间隔速率)                │
│    ── 头部每个数字都能从这里手工重算 ──               │
└────────────────────────────────────────────────────┘
```

### 6.2 JSON 结构（速率 4 位小数）

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
                 "samples": [ 最近 500 条原始样本 ] }
}
```

### 6.3 数字口径

- 平均速率窗口 = **首个有效样本 → 末个有效样本**（stop 后无采样，天然不稀释）。
- 容量 = `50×1024 ÷ apply_avg`；用 JSON 的 4 位小数复算应逐位一致。
- 逐间隔速率与 `pg_wal_lsn_diff(x, lag(x)) ÷ extract(epoch ...)` 完全同式。

## 7. 配置（GUC）

| GUC | 默认 | 生效 | 含义 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期 = 时序粒度（最小 1s） |
| `pg_lrstat.session_max_samples` | `2880` | 重启 | 历史环形容量（每目标样本数；写满覆盖最旧） |
| `pg_lrstat.max_targets` | `32` | 重启 | 最多同时监测目标数 |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标消失后从视图剔除（如 tablesync 结束） |
| `pg_lrstat.remote_poll` | `true` | SIGHUP | 接收端轮询发送端（关闭则 cluster_stat 无远端数据） |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 轮询连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮轮询总预算；超时标 stale |
| `pg_lrstat.catchup_min_rate` | `0.001` | SIGHUP | 追平预估输出的最低平均速率（MB/s），低于此值不给出预估 |

运行时参数用 `SELECT pg_reload_conf();` 即可，不用重启。

## 8. 典型工作流

```sql
-- 迁移前评估：试迁移一轮，看应用速率和容量
SELECT lrstat_start();
-- ... 10 分钟代表性负载 ...
SELECT lrstat_stop();
SELECT lrstat_export(NULL, 'json');   -- 容量表: 50/100/200GB 需要多久

-- 日常巡检：瓶颈在哪、还要多久追平
SELECT ts, bottleneck, round(backlog_total::numeric,1) 积压MB,
       round(catchup_total_secs) 追平秒
FROM pg_lrstat_cluster_stat ORDER BY ts DESC LIMIT 5;

-- 空间风险：槽扣住了多少 WAL
SELECT slot_name, round(retained_wal::numeric,0) MB, wal_status
FROM pg_lrstat_send_stat ORDER BY retained_wal DESC NULLS LAST LIMIT 5;
```

## 9. 常见问题

| 问题 | 答案 |
| --- | --- |
| 视图全空 | 没预加载（`info.loaded=f`），或还没 `lrstat_start()`——只有 start 后才采样 |
| 速率全是 NULL | 首行没有前样本可差分（正常）；第二行起有值，等一个采样周期（默认 30s——测试时建议 `ALTER SYSTEM SET pg_lrstat.sample_interval='1s'; SELECT pg_reload_conf();`） |
| stat 表只有一行 | start 后立即有一行（首行速率 NULL）；此后每个采样周期追加一行，默认 30s 一行。要更快出数据就调小 sample_interval |
| apply 速率一直是 0 | 该间隔内没有事务提交（applied 只在提交边界推进）；跑持续负载后再看，或看报告 Evidence 表的提交轮跳变 |
| cluster_stat 没数据 | 没有活跃订阅，或 remote_state = unreachable（检查订阅连接串） |
| apply 速率 = 0 但积压在涨 | 应用被堵（锁冲突最常见）：拿 recv_stat 的 worker_pid 查 pg_stat_activity 的 wait_event |
| applied 长时间不动然后跳 | 大事务：应用位只在提交边界推进，正常 |
| 想看已结束的会话 | stop 后立即 `lrstat_export()`；环形内可 `lrstat_export('sess_N')` 补导；已被新 start 清掉的会话只剩报告文件 |
| 四个速率相等 | 健康！下游跟得上，都被"生成"定节奏 |
| round(x, 1) 报错 | 速率/积压列是 float8，需 `round(x::numeric, 1)` |
