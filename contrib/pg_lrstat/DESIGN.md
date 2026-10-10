# pg_lrstat 设计文档

pg_lrstat 是一个 PostgreSQL 扩展，用于**精确测量逻辑/物理复制链路的性能**：生成速率、发送速率、接收速率、应用速率，三段积压，瓶颈自动判定与容量外推。所有数字可从原始样本手工复算。

## 1. 总体架构

### 1.1 一张图看懂

```
        发布端 (publisher)                     订阅端 (subscriber, 可独立部署)
┌──────────────────────┐                ┌─────────────────────────────────────┐
│  WAL 生成 ── walsender ──────────────► walreceiver ── apply worker          │
│    C0        发送 C2        网络      接收 C3'         应用 C5'              │
│                      │                │                                     │
│  系统视图(只读):      │   ①本地采样     │  系统视图(只读):                      │
│  pg_replication_slots│   ◄────────────┤  pg_subscription / pg_stat_         │
│  pg_stat_replication │                │  subscription / replication_origin  │
│                      │   ②远端轮询     │                                     │
│  pg_current_wal_lsn()│   ◄────────────┤         (libpq 连回发布端查询)       │
└──────────────────────┘                │                                     │
                                        │  ┌───────────────────────────┐     │
                                        │  │ pg_lrstat 采样 bgworker   │     │
                                        │  │  (每 sample_interval 一轮) │     │
                                        │  └────────────┬──────────────┘     │
                                        │               ▼                    │
                                        │  ┌───────────────────────────┐     │
                                        │  │ 共享内存                  │     │
                                        │  │  · 会话状态 + 会话注册表   │     │
                                        │  │  · 目标表 (三槽样本)      │     │
                                        │  │  · 历史环形 (唯一数据源)  │     │
                                        │  └────────────┬──────────────┘     │
                                        │        ┌──────┴──────┐             │
                                        │        ▼             ▼             │
                                        │   6 个 SQL 视图   lrstat_export    │
                                        │   (逐轮时序)      (报告文件)       │
                                        └─────────────────────────────────────┘
```

**核心设计**：部署在**订阅端**即可工作——本地采样接收进度，同时通过订阅连接串**轮询**发布端系统视图（RSEND 镜像目标），两端合成一条链路视图。发布端零安装。

### 1.2 组件

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| 入口 | `pg_lrstat.c` | GUC 定义、共享内存 hook、worker 注册 |
| 采样器 | `lrstat_worker.c` | bgworker 主循环：SPI 采样 → 远端轮询 → 历史记录 |
| 远端轮询 | `lrstat_remote.c` | libpq 连回发布端查询（预算/退避/反馈折入） |
| 共享内存 | `lrstat_shmem.c` | 会话状态、目标表、历史环形、会话注册表 |
| SQL 层 | `lrstat_sql.c` | start/stop/reset 命令、6 个视图 SRF |
| 导出 | `lrstat_export.c` | HTML/JSON 报告生成与落盘 |

### 1.3 会话模型（极简）

```
          lrstat_start()                lrstat_stop()
 idle ────────────────────► running ────────────────────► stopped
  ▲                          │  ▲                          │
  │      清空上一会话数据      │  └── 每个采样轮：            │
  └──────────────────────────┘       采样→轮询→记录          │
       (下一次 start)            stop 后数据冻结可查/可导出     │
                                                             │
                     要留档？── stop 后、下一次 start 前 export ─┘
                              (报告文件 = 唯一持久产物)
```

- **start()**：无参数。清空历史环形与全部目标（干净起点），登记会话，唤醒 worker 立即采一轮。
- **stop()**：无参数。停止采样；数据冻结在内存。
- **采样只发生在 start→stop 之间**：无 start 不采样，视图为空。
- **export()**：写报告文件到 `$PGDATA/pg_lrstat/exports/`——这就是持久化。

## 2. 复制链路与观测点

### 2.1 LSN 检查点（逻辑复制）

```
 发布端                网络                 订阅端
─────────────   ────────────────   ─────────────────────
current_lsn  ─► sent_lsn ─►(在途)─► received_lsn ─► applied_lsn
   C0            C2                   C3'            C5'
                                                ▲
反馈通道 (apply→send, 默认10s心跳):               │
  peer_recv_lsn / peer_flush_lsn / peer_applied_lsn (发布端观测)
  ──────────────────────────────────────────────┘
```

### 2.2 三段积压

```
|◄── backlog_unsent ──►|◄── backlog_inflight ─►|◄─ backlog_unapplied ─►|
current_lsn            sent_lsn              received_lsn         applied_lsn
   └── 未发送(解码/发送慢)──┘└── 网络在途 ──┘└── 收到未应用(应用慢) ──┘
                        backlog_total = current − applied
```

### 2.3 设计依赖的内核事实（PG18，已核实）

- 逻辑复制 origin 的 `remote_lsn` 只在**事务提交边界**推进——大事务期间恒为旧值；
- walsender 视角 `write_lsn/flush_lsn/replay_lsn` = 接收端反馈的 write/flush/apply 三槽；
- 反馈心跳默认 10s（`wal_receiver_status_interval`）——发送端观测最多滞后一个心跳周期；
- `pg_stat_subscription.worker_type`：`apply` / `table synchronization`（19 字符）；
- 订阅无 worker 运行时（启动窗口）相关行全 NULL——全零样本必须跳过。

## 3. 采样与数据流

### 3.1 采样轮（每 sample_interval，仅 running）

```
┌─ 1. 采样 (SPI, worker_spi 式单事务) ─────────────────────────┐
│   RECV_SQL ─► RECV 目标 (订阅/worker 状态)                    │
│   SEND_SQL ─► SEND 目标 (本机槽; 订阅端通常为空)              │
│   · 三槽轮转: prev=last; last=新样本                          │
│   · applied_lsn 单调融合: GREATEST(origin, 反馈apply)          │
│   · 锚点: 仅首个"含有效位置"的样本 (全零样本不锚定)             │
├─ 2. 远端轮询 (libpq 连回发布端, 仅 running) ─────────────────┤
│   · 查询发布端槽/水位 ─► RSEND 镜像目标                       │
│   · 反馈 apply 位 (replay_lsn) 折入 last ──┐                  │
│                                           └─ 在记录之前执行   │
├─ 3. 历史记录 ────────────────────────────────────────────────┤
│   · 每目标本轮快照 (LSN + kind + 每轮状态) ─► 历史环形         │
│   · 时间戳未前进的目标跳过 (防重复)                            │
├─ 4. 错误恢复 ────────────────────────────────────────────────┤
│   PG_CATCH: SPI_finish + AbortOutOfAnyTransaction             │
│   (吞错不回卷会卡死下一轮)                                     │
└──────────────────────────────────────────────────────────────┘
```

**为什么轮询在记录之前**：反馈折入先于落盘，记录值与视图/报告同源——速率可验证的基础。

### 3.2 applied_lsn 的融合（可验证性的关键）

```
采样时:   applied = GREATEST(origin.remote_lsn, 上一轮融合值)   ← push 单调
每轮轮询: 折入反馈 apply 位 (walsender.replay_lsn)               ← bump
                 ⚠ 必须用 apply 位而非 flush 位: flush 领先于应用会虚高速率
视图/报告/history ──► 同一条单调融合序列
```

### 3.3 共享内存布局（布局版本 6）

```
┌─────────────────────────────────────────────────────────┐
│ LRSessionState  (magic/版本/会话名/id/起止/truncated)     │
├─────────────────────────────────────────────────────────┤
│ LRTargetCtl[max_targets]                                │
│   · kind (SEND/RECV/RSEND) + name + relid               │
│   · 三槽: anchor / prev / last (测量窗口)                │
│   · meta: 直通属性 (plugin/state/lag/worker_type/...)    │
├─────────────────────────────────────────────────────────┤
│ LRHistoryEntry[max_targets × session_max_samples]  ◄── 唯一数据源
│   · ts + target_idx + kind + session_id                 │
│   · 发送侧 7 LSN + spill/stream                          │
│   · 接收侧 3 LSN                                         │
│   · 每轮状态: state/wal_status/worker_type/pid/lag/错误数 │
│   (环形: 写满覆盖最旧, truncated=true)                    │
├─────────────────────────────────────────────────────────┤
│ LRSessionRegEntry[16]  (session_id → 名字/起止)          │
└─────────────────────────────────────────────────────────┘
```

默认（32 目标 / 2880 样本）约 20MB，postmaster 启动期预留。

### 3.4 视图如何从历史推导（零预存派生量）

```
LRHistoryEntry[] (环形)
      │
      ├─► send_stat     发送侧条目 + 相邻同目标差分 = 本轮速率 + 条目自带状态
      ├─► recv_stat     接收侧条目, 同上
      ├─► cluster_stat  recv 行 × 时间最近的 RSEND 行配对 (双游标单调前进)
      ├─► send_history  发送侧原始行 (直出)
      ├─► recv_history  接收侧原始行 (直出)
      └─► export        首末有效样本重建锚点 ─► 会话平均/瓶颈/容量
```

## 4. 速率体系

### 4.1 两个公式

```
逐轮速率 (stat 视图 *_mbps):
    rate(i) = lsn_diff(LSN_i, LSN_{i-1}) / 1048576 / (ts_i − ts_{i-1})
    同一目标相邻两个样本; 首行 NULL; 空闲轮 0

会话平均 (报告 analysis *_avg):
    avg = lsn_diff(LSN_末, LSN_锚点) / 1048576 / (ts_末 − ts_锚点)
    锚点/末样本 = 会话内首个/末个含有效位置的样本
    分母 = min(末样本时间, stop_ts) —— stop 后无采样, 天然不稀释
```

### 4.2 容量外推

```
sync_50g_secs  = 50 × 1024 / apply_avg        (MB 按 1048576 字节)
sync_100g_secs = 2 × sync_50g_secs            (严格成倍)
sync_200g_secs = 4 × sync_50g_secs
```

### 4.3 瓶颈判定（逐轮 + 会话两级）

```
send       : 发送速率 < 生成速率  且 未发送积压 > 总积压/2
recv_apply : 未应用积压 > 总积压/2
network    : 在途积压 > 总积压/2
none       : 其他 (健康: 四速率相等)
```

## 5. 用户接口

### 5.1 命令（4 个，均 superuser）

```sql
lrstat_start()                → text   -- 开始采样; 清空旧数据; 返回自动名 sess_<n>
lrstat_stop()                 → text   -- 停止采样; 数据冻结
lrstat_export(name DEFAULT NULL,
              format DEFAULT 'html') → text
                                      -- 写报告文件 = 持久化; 返回绝对路径
pg_lrstat_reset()             → void   -- 强制清内存 (不动报告文件)
```

### 5.2 视图（6 个）

| 视图 | 粒度 | 用途 |
| --- | --- | --- |
| `pg_lrstat_info` | 1 行 / 15 列 | 健康自检 + 会话状态 + 已导出报告列表 |
| `pg_lrstat_send_stat` | 一目标一轮 / 27 列 | 发送端逐轮时序 |
| `pg_lrstat_recv_stat` | 一 worker 一轮 / 17 列 | 接收端逐轮时序 |
| `pg_lrstat_cluster_stat` | 一链路一轮 / 26 列 | **两端合成, 日常巡检只看这个** |
| `pg_lrstat_send_history` | 一目标一轮 / 11 列 | 发送端原始样本 |
| `pg_lrstat_recv_history` | 一目标一轮 / 5 列 | 接收端原始样本 |

字段含义详见用户手册（逐列文档）。

### 5.3 GUC

| GUC | 默认 | 生效 | 含义 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期 = 时序粒度 (最小 1s) |
| `pg_lrstat.session_max_samples` | `2880` | 重启 | 历史环形容量 (每目标样本数) |
| `pg_lrstat.max_targets` | `32` | 重启 | 最多同时监测目标数 |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标消失后从视图剔除的秒数 |
| `pg_lrstat.remote_poll` | `true` | SIGHUP | 是否从接收端轮询发送端 |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 轮询连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮轮询总预算 |
| `pg_lrstat.catchup_min_rate` | `0.001` | SIGHUP | 追平预估输出的最低平均速率 (MB/s) |
| `pg_lrstat.database` | `postgres` | 重启 | 采样 worker 连接的库（视图数据集群级，任意库可查） |
| `pg_lrstat.allow_inject` | `false` | SUSET | 测试注入函数开关（内部） |

### 5.4 导出报告

```
$PGDATA/pg_lrstat/exports/<name>.{html,json}

HTML (自上而下):
┌──────────────────────────────────────────────┐
│ Analysis   瓶颈徽章/四速率/三段积压/容量表      │ ◄─ 运维第一眼
│ Chart      gen/send/apply 逐轮折线 (内嵌SVG)  │
│ Session    会话元信息                          │
│ Targets    每目标: 侧别/状态/平均速率/最新LSN   │
│ Evidence   ①send history ②recv history       │ ◄─ 每个数字可手工复算
│            ③send rate  ④recv rate (逐间隔)    │
└──────────────────────────────────────────────┘

JSON: session/analysis/capacity/send_stat/recv_stat/history
      速率 4 位小数 (容量推算可精确互算)
```

## 6. 关键机制细节

### 6.1 目标键与生命周期

- 键 = `(kind, name, relid, worker_char)`；`parallel apply` 行跳过（leader 是规范源）。
- 视图数据生命周期与 start/stop 严格对齐：目标中途消失（如 tablesync 结束）其历史行保留到会话结束——历史是会话的完整记录。
- `stale_target_ttl` 仅管槽位复用：目标消失超过阈值后其槽位可让给新目标（`max_targets` 不够时）；满载丢弃计数入 `info.dropped_samples`。

### 6.2 远端轮询（RSEND）

- 连接串来自 `pg_subscription.subconninfo`；注入 `application_name='pg_lrstat'`、connect_timeout、statement_timeout。
- 单轮总预算 `remote_poll_budget`；失败指数退避（1s→60s），状态 `ok/stale/unreachable`。
- 结果写入 RSEND 目标三槽；**反馈 apply 位折入 RECV 的 last**。

### 6.3 并发与内存安全

- 单写者（worker）+ spinlock 每目标；持锁只做 memcpy 级操作。
- 视图遍历环形时按时间升序（`truncated` 时游标从写位置绕回）。
- 全零样本三重防护：无 worker 行跳过、锚点需非零 LSN、视图 SQL `nullif(x,'0/0')`。

### 6.4 测试

TAP `t/001_tablesync.pl`（`meson test pg_lrstat/001_tablesync`，需 `-Dtap_tests=enabled`），22 项断言覆盖：table sync worker 可见性、订阅端不崩溃、逐轮时序、会话清空、按名导出、Evidence 表、未知会话报错。

人工验证记录见 `TEST_REPORT.md`（三方速率一致性、容量精确成倍、端到端 WAL 闭合）。
