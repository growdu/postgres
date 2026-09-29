# pg_lrstat 设计文档（v2.0）

复制（逻辑/物理）测量会话扩展：命名会话、双速率实时观测、可视化报告导出

| 项 | 内容 |
| --- | --- |
| 文档版本 | 2.0 |
| 基线代码 | PostgreSQL 18 开发分支（本仓库，branch `cluster_rate`） |
| 状态 | 待评审 |

---

## 1. 概述

### 1.1 要解决的问题

复制链路（逻辑或物理）出问题时，运维要回答的永远是四个问题：**货压在哪一段？谁干得慢？从什么时候开始、越来越差还是正在好转？还要多久清完？** PostgreSQL 自带的系统视图只提供瞬时快照（LSN 位点、状态），没有速率、没有趋势、没有"这一轮压测跑完到底什么水平"。本扩展用显式的**测量会话**补齐这一层：

```
lrstat_start(true)      lrstat_stop()      lrstat_export()
        │                                              │
        ▼                                              ▼
   ┌──────────────── 会话进行中 ────────────────────────┐
   │  采样 worker 每 30s 采集一次全链路位点               │
   │  随时查询：瞬时速率（最近一个采样间隔）＋平均速率（自 start） │
   └──────────────────────────────────────────────────┘
        │
        ▼
   会话报告（总量/平均/峰值/逐间隔序列）→ 可视化导出（HTML/JSON）
```

### 1.2 核心概念

| 概念 | 定义 |
| --- | --- |
| **会话（session）** | 一次 `lrstat_start` 到 `lrstat_stop` 的测量窗口，有唯一名字、编号、起止时间 |
| **目标（target）** | 被监测的对象：发送端一条复制连接/一个复制槽（SEND）、接收端一个 worker 或恢复进程（RECV）、接收端远端轮询到的发送端镜像（RSEND） |
| **锚点样本（anchor）** | 会话首轮采到的样本，平均速率的起算基准 |
| **逐轮速率（*_mbps）** | 相邻两个样本的差分 ÷ 间隔——stat 视图每轮一行输出 |
| **平均速率（avg）** | 最新样本 − 锚点 ÷ 经过时间——"这轮从头到现在平均多少" |
| **历史数组** | 每轮每目标一条**全量原始 LSN 样本 + 逐轮状态**的环形记录（始终在记）——唯一数据源 |
| **会话报告** | stop 后由历史数组聚合出的完整统计（总量、平均、峰值、堵住等） |

### 1.3 目标与非目标

**目标**：一次会话一份完整报告；瞬时/平均双速率语义清晰；会话可命名、可按需持久化（默认零文件写入）、崩溃自动恢复；发送端/接收端双侧视角，接收端可凭复制连接串（逻辑订阅 conninfo 或物理 walreceiver conninfo）合成两端整体视图（发送端免安装）；纯只读观测，不修改复制行为、不占用复制槽、默认不写数据库数据（无 WAL、无表）。

**非目标**：7×24 连续趋势（监控端周期采 live 视图落库实现）；跨集群拉取数据；表级粒度；自动故障处理。

---

## 2. 复制链路与观测点

### 2.1 逻辑复制链路图

```
发送端                                     接收端
┌──────────────────────────────┐          ┌───────────────────────────────┐
│ WAL 生成(current_lsn)  C0     │          │                               │
│   ▼                          │  TCP     │  接收(received_lsn)  C3'       │
│ 解码(输出插件翻译)  D1        ├─────────►│   ▼                           │
│   ▼                          │  反馈     │  应用(applied_lsn)  C5'        │
│ 发送(sent_lsn)  C2           │◄─────────┤   ▼                           │
│   ▼                          │ write/   │  反馈(每10s)                   │
│ 确认(confirmed_flush) C6      │ flush/   │                               │
│ 保水(restart_lsn)  C7         │ apply    │                               │
└──────────────────────────────┘          └───────────────────────────────┘
     对端已收 C3 / 已提交落盘 C4 / 已应用 C5 ←来自反馈报文
```

### 2.2 检查点与数据来源

| # | 检查点 | 发送端来源 | 接收端来源 | 语义 |
| --- | --- | --- | --- | --- |
| C0 | WAL 生成位点 | `pg_current_wal_lsn()` | —（接收端本地另有 `pg_current_wal_lsn()`，含义为接收库自身写入，两码事） | 已写入 WAL 的最大位置 |
| C2 | 已发送位点 | `pg_stat_replication.sent_lsn` | — | walsender 已写出发送缓冲并 flush 的位置 |
| C3 | 对端已接收 | `pg_stat_replication.write_lsn`（反馈 write 槽位） | 逻辑：`pg_stat_subscription.received_lsn`；物理：`pg_stat_wal_receiver.flushed_lsn` | 接收端收到的流位置 |
| C4 | 对端已提交落盘 | `pg_stat_replication.flush_lsn`（反馈 flush 槽位） | — | 已应用且本地落盘的提交位点 |
| C5 | 对端已应用/回放 | `pg_stat_replication.replay_lsn`（反馈 apply/replay 槽位） | 逻辑：origin `remote_lsn` 与反馈 flush 位取大（§4.6）；物理：`pg_last_wal_replay_lsn()` | 逻辑=已应用的事务位；物理=已回放的 WAL 位 |
| C6 | 安全水位 | `pg_replication_slots.confirmed_flush_lsn` | — | WAL 回收决策位 |
| C7 | 槽保水位 | `pg_replication_slots.restart_lsn` | — | 为该槽保留的最早 WAL；C0−C7 即扣住的 WAL 量 |
| D1 | 解码压力 | `pg_stat_replication_slots` 的 `spill_bytes/stream_bytes/total_bytes` | — | ReorderBuffer 溢写/流式/总输出字节计数 |

### 2.3 设计依赖的内核事实（PG18，均已在仓库核实）

1. **逻辑反馈三槽位**：逻辑接收端 `send_feedback()` 把**接收位 recvpos** 填在报文 write 槽、**已提交落盘位** 填在 flush 槽、**已应用位** 填在 apply 槽——这是接收端状态进入发送端视图的唯一通道，也决定了发送端观测最多滞后一个反馈周期（`wal_receiver_status_interval`，默认 10s）。
2. **origin 命名与推进**：逻辑订阅的复制源名为 `pg_<订阅oid>`；其 `remote_lsn` **只在事务提交边界跳变**（`replorigin_session_advance` 由提交路径调用），单事务回放期间不推进。因此逻辑接收端应用位点需用反馈 flush 位融合（§4.6）。
3. **worker_type 取值**：`apply` / `parallel apply` / `table synchronization`（注意拼写与长度）。
4. **逻辑流不落接收端 WAL**：逻辑订阅的流只存在于 apply worker 内存，接收端 WAL 仅含回放产生的记录；`pg_stat_wal_receiver` 只覆盖物理流复制。
5. **物理复制的接收端视图**：standby 上 `pg_stat_wal_receiver` 暴露 `written_lsn/flushed_lsn/latest_end_lsn/last_msg_send_time/last_msg_receipt_time/sender_host/sender_port/conninfo`；`pg_last_wal_receive_lsn()` / `pg_last_wal_replay_lsn()` 给接收/回放位点；物理反馈的 write/flush/replay 位是 walreceiver 直接上报的**真实值**（无逻辑复制那种 origin 语义问题）。
6. **`pg_stat_replication.kind`**：PG18 区分 `physical`/`logical` walsender，发送端同一视图可按 kind 过滤，一条采样 SQL 同时覆盖两种复制。
7. **时钟纪律**：任何跨机时间延迟只能用内核按反馈时间戳计算的 lag 列（`write_lag/flush_lag/replay_lag`）；本扩展所有样本（含远端轮询）统一打**采样方本地时钟**，两端 NTP 偏差不进入任何计算。

---

## 3. 指标体系

### 3.1 位点与积压（瞬时量）

位点即 §2.2 的 C0~C7 原值。积压为最新样本上两点位相减（负值截 0），**单位 MB**（字节 ÷ 1048576，float8）：

| 积压 | 公式 | 含义 |
| --- | --- | --- |
| `backlog_unsent` | C0 − C2 | 已生成未发送（含未解码） |
| `backlog_inflight` | C2 − C3′ | 发出后接收端尚未收到——真网络在途（接收端本地实收口径，无反馈假象） |
| `backlog_unapplied` | C3′ − C5′ | 已收到未应用 |
| `backlog_total` | C0 − C5′ | 端到端总积压（三段之和） |
| `backlog_apply` | C3′ − C5′（接收端视图） | 同 unapplied，接收端独立视图口径 |
| `retained_wal` | C0 − C7 | 槽扣住的 WAL 总量（磁盘风险） |
| `feedback_lag_mb` | C3′ − 反馈 write 槽位 | 回执滞后量：区分"真在途"与"10 秒回执假象"的交叉观测量 |

发送端视图另提供 `backlog_peer_unapplied`（C3−C5，反馈口径，最多滞后 10s）。

### 3.2 速率（逐轮 + 会话平均）

对任一速率字段 f ∈ {gen(C0), send(C2), recv(C3′), apply(C5′), spill(D1), stream(D1), confirm(C6)}——apply 在逻辑复制=应用速率、在物理复制=回放速率，列名统一为 apply：

```
逐轮速率（stat 视图 *_mbps）= (f(S_i) − f(S_{i-1})) / (ts(S_i) − ts(S_{i-1}))
    ——同一目标相邻两个样本；首行无前值输出 NULL
会话平均（报告 analysis 的 *_avg）= (f(S_last) − f(S_anchor)) / (stop_ts − ts(S_anchor))
    ——锚点=会话内首个含有效位置的样本；分母截断到 stop_ts
```

- 单位：**MB/s**（内部按 LSN 差分得到字节/秒，输出前 ÷ 1048576 转为 MB/s）；
- 逐轮速率的粒度 = `sample_interval`（默认 30s，可调至 1s）；
- 两种速率都可从 history 手工重算（§5.5.2 容量口径）；
- 数值示例：两样本相隔 110s，`sent_lsn` 从 `0/0` 到 `0/800000`（8 MB）→ `send` 瞬时 = 平均 = 8 ÷ 110 ≈ 0.0727 MB/s；
- 有效性规则：最新样本距查询超过 3×采样间隔（采样中断）→ 速率列 NULL；`S_prev` 缺失（会话首轮）→ 瞬时列 NULL，平均列=首段值；
- **速率是页码（LSN）差不是网线字节**：与带宽对比需乘解码膨胀率（`total_bytes` 增量 ÷ C0 增量）。

### 3.3 会话报告指标（stop 后聚合）

| 指标 | 计算 | 说明 |
| --- | --- | --- |
| `duration_secs` | 实际首末样本区间 | 以样本为准，非命令时刻 |
| `total_f_mb` | f(S_last) − f(S_anchor)（÷ 1048576） | 各字段会话总量（MB） |
| `avg_f` | total ÷ duration | 会话平均（= stop 时刻 live 的 avg） |
| 逐轮速率分布 | stat 时序的 min/max/avg（SQL 聚合即可得） | 速率波动与稳定性 |
| `peak_backlog_*` | 锚点起增量前缀和重建各水位，取逐间隔最大 | 峰值积压 |
| `blocked_intervals` | 瞬时 apply/send ≈0 且对应积压 >0 的间隔数 | 堵住时长占比 |
| `polls_ok / polls_fail` | 远端轮询成功/失败次数 | 整体视图专有 |
| `partial` | 目标首个样本晚于 start_ts | 会话中途加入的目标 |
| `truncated` / `degraded` | 历史环形覆盖最旧 / 文件写失败 | 数据完整性标记 |

### 3.4 追平预估（catchup）与堵住（live 视图）

```
catchup_send_secs  = backlog_unsent_mb / avg_send_mbps
catchup_total_secs = backlog_unsent_mb / avg_send_mbps + (backlog_inflight_mb + backlog_unapplied_mb) / avg_apply_mbps
```

平均速率低于 `catchup_min_rate`（默认 0.001 MB/s）或积压为 0 时为 NULL；持续写入场景需用**净追平速率** `min(avg send, avg apply) − avg gen` 重估（为负则追不平）。`*_blocked` = 对应积压>0 且**瞬时**速率有效但 < 0.001 MB/s（"最近一个采样间隔没动"）。

---

## 4. 架构与机制原理

### 4.1 部署形态

```
发送端实例                                接收端实例
shared_preload_libraries=pg_lrstat        shared_preload_libraries=pg_lrstat
┌──────────────────────┐                 ┌─────────────────────────────┐
│ pg_lrstat 采样 worker │                 │ pg_lrstat 采样 worker        │
│  SPI 采样发送端视图    │   普通 libpq     │  SPI 采样接收端视图           │
│  → SEND 目标          │ ◄────────────── │  凭连接串轮询发送端            │
│  (逻辑+物理)           │  (远端轮询)      │  → RSEND 目标 + 两端合成       │
└─────────┬────────────┘                 └──────────┬──────────────────┘
          ▼ 共享内存（会话状态+目标+历史环形+会话注册表）   ▼
          └──────────────► SQL 视图 / 会话文件 / 报告导出 ◄┘
```

- 同一扩展两端通用：发送端产出 SEND 目标（逻辑槽+物理连接），接收端产出 RECV 目标（逻辑 worker 或物理恢复进程）与 RSEND 目标（远端轮询镜像）；
- **接收端可独立部署**：凭连接串轮询发送端系统视图（逻辑复制的连接串来自 `pg_subscription.subconninfo`，物理复制的来自 `pg_stat_wal_receiver.conninfo`），`overall` 系列视图合成两端，发送端零安装；
- 采样 worker 只连一个库（`pg_lrstat.database`，默认 postgres）——槽、订阅、复制源、walsender/walreceiver 统计均为集群级，单连接看全集群。物理复制发送端的逻辑槽与物理连接都出现在 `pg_replication_slots`/`pg_stat_replication`，按 `kind` 列区分；接收端 standby 的 `pg_stat_wal_receiver` 同样集群级可见。

### 4.2 组件

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| 入口 | `pg_lrstat.c` | GUC 定义、共享内存请求/启动 hook、worker 注册、wait event 惰性注册点 |
| 采样器 | `lrstat_worker.c` | bgworker 主循环：采样三槽位轮转、全量样本入历史 |
| 远端轮询 | `lrstat_remote.c` | 接收端→发送端只读轮询（libpqsrv、预算/退避、反馈位回填） |
| 共享内存 | `lrstat_shmem.c` | 会话状态、目标表（anchor/prev/last）、历史环形数组（覆盖最旧） |
| SQL 层 | `lrstat_sql.c` | start/stop/export/delete 命令、全部视图 SRF、报告聚合 |
| 导出渲染 | `lrstat_export.c` | HTML（内嵌 SVG+JS）+ JSON 两格式 + 分析结论计算 |

### 4.3 共享内存布局

```c
/* 会话状态（全局唯一） */
typedef struct LRSessionState
{
    int32       magic;
    int32       layout_version;
    uint64      session_id;         /* 每次 start 递增                          */
    bool        running;            /* start 后 true，stop 后 false              */
    char        name[NAMEDATALEN];  /* 会话名（start 传入或自动生成）             */
    TimestampTz start_ts;
    TimestampTz stop_ts;
    bool        truncated;          /* 历史环形已覆盖最旧                        */
    bool        degraded;           /* 会话文件写失败，持久化不完整               */
    slock_t     mutex;              /* 会话状态转换锁                            */
} LRSessionState;

/* 发送端样本（SEND 与 RSEND 共用，按 kind 解释；逻辑/物理通用） */
typedef struct LRPubSample
{
    TimestampTz ts;                 /* 采样方本地时钟                            */
    XLogRecPtr  current_lsn;        /* C0                                        */
    XLogRecPtr  sent_lsn;           /* C2                                        */
    XLogRecPtr  peer_recv_lsn;      /* C3  反馈 write                            */
    XLogRecPtr  peer_flush_lsn;     /* C4  反馈 flush                            */
    XLogRecPtr  peer_applied_lsn;   /* C5  反馈 apply                            */
    XLogRecPtr  confirmed_lsn;      /* C6                                        */
    XLogRecPtr  restart_lsn;        /* C7                                        */
    uint64      spill_bytes;        /* D1                                        */
    uint64      stream_bytes;       /* D1                                        */
    uint64      total_bytes;        /* D1                                        */
} LRPubSample;

/* 接收端样本（逻辑=apply worker，物理=恢复进程） */
typedef struct LRRecvSample
{
    TimestampTz ts;
    XLogRecPtr  received_lsn;       /* C3' 逻辑:received_lsn 物理:receive_lsn   */
    XLogRecPtr  applied_lsn;        /* C5' 逻辑:origin∪反馈 物理:replay_lsn     */
    XLogRecPtr  local_wal_lsn;      /* 接收库 pg_current_wal_lsn()              */
} LRRecvSample;

typedef union LRSample { LRPubSample pub; LRRecvSample sub; } LRSample;

/* 透传属性（不参与速率），每轮整块重写 */
typedef struct LRTargetMeta
{
    bool  active, temporary, safe_wal_size_valid, reply_time_valid;
    pid_t sender_pid;
    char  state[16], sync_state[16], wal_status[16];
    char  database[64], plugin[64], application_name[64], client_addr[64];
    int64 safe_wal_size, write_lag_us, flush_lag_us, replay_lag_us;   /* lag: -1 未知 */
    TimestampTz reply_time;
    char  remote_state[16];          /* RSEND: ok/unreachable/stale/n/a          */
    TimestampTz last_remote_poll;
    /* slot_name 即目标键 name，meta 不重复存储 */
    char  worker_type[24];           /* apply / parallel apply / table sync / recovery */
    pid_t worker_pid, leader_pid;
    TimestampTz last_msg_send_time, last_msg_receipt_time, latest_end_time;
    int64 apply_error_count, sync_error_count;
    /* 以下为内部字段：不参与速率差分，不暴露到视图 */
    XLogRecPtr origin_lsn;          /* 逻辑:origin remote_lsn（bump_applied 比较用） */
    XLogRecPtr latest_end_lsn;      /* received_lsn 为空时的回退源                  */
} LRTargetMeta;

/* 每目标：三槽位（锚点/前一/最新），无历史环 */
typedef struct LRTargetCtl
{
    LRTargetKind kind;               /* SEND / RECV / RSEND                       */
    char     name[NAMEDATALEN];
    char     worker_char;            /* SUB: 'a' apply / 't' tablesync            */
    Oid      relid;                  /* SUB tablesync 目标表，否则 0              */
    bool     in_use;
    TimestampTz first_seen_ts;       /* 会话内首见（partial 判定）                 */
    TimestampTz last_sample_ts;
    slock_t  mutex;                  /* 保护本块与三槽位                          */
    LRSample anchor, prev, last;
    LRTargetMeta meta;
} LRTargetCtl;

/* 历史条目：每轮每目标一条全量原始样本（内存环形数组与文件记录共用此形） */
typedef struct LRHistoryEntry
{
    TimestampTz ts;                  /* 本轮采样时刻                              */
    int32     target_idx;           /* 目标表下标                                */
    /* 发送端 LSN（SEND/RSEND 目标）                                         */
    XLogRecPtr current_lsn, sent_lsn;
    XLogRecPtr peer_recv_lsn, peer_flush_lsn, peer_applied_lsn;
    XLogRecPtr confirmed_lsn, restart_lsn;
    uint64    spill_bytes, stream_bytes;
    /* 接收端 LSN（RECV 目标）                                               */
    XLogRecPtr received_lsn, applied_lsn, local_wal_lsn;
} LRHistoryEntry;                   /* 不存差值：差值由视图/导出按相邻样本现算    */
```

**空间公式**：`MAXALIGN(头) + max_targets × MAXALIGN(sizeof(LRTargetCtl)+3×sizeof(LRSample)) + session_max_samples × max_targets × sizeof(LRHistoryEntry) + 16 × sizeof(LRSessionRegEntry)`。**历史条目（布局 v4）**是完整的每轮快照：目标 kind（读写按侧、防 union 别名）、session_id 盖章（0=会话外）、两侧全量 LSN、以及逐轮状态（walsender state/sync/wal_status、PID、三段 lag、worker_type、心跳、错误计数）——stat 视图的时序输出全部来自条目现算。会话注册表（16 槽环形）把 session_id 映射回会话名。默认（32 目标 / 2880 样本）约 **20MB**，postmaster 启动期一次预留。

### 4.4 会话机制

**状态机**：

```
idle ──start()──► running ──stop()──► stopped ──start()──► running(...)
  │                             │  ▲
  │                             │  └─ 采样轮：prev=last; last=新样本;
  │                             │     每轮把全量样本 append 历史
  │                             └ 实例重启：内存清空；已 export 的报告文件仍在
  └ 历史（环形）始终在记（跨会话保留至被覆盖）；要留档的会话在 stop 后 export
```

**start()**：superuser，无参（全局唯一会话；重复 start 报错）。自动名 `sess_<n>` 仅标识报告与按名导出。动作：`session_id++`、记录 `start_ts`、**清空历史环形与全部目标**（每个会话干净起点——上一会话数据作废，要留档须在其 stop 后 export）、**唤醒 worker 立即执行一轮采样**——该轮样本即锚点。

**采样轮**（每 `sample_interval`）：

```
0. 仅 running 时执行整轮（无 start 不采样；stop 后冻结直到下一次 start）
1. 采样（SPI，worker_spi 式单会话：StartTransaction+Push 快照+SPI_connect）：
   SEND_SQL/RECV_SQL → 目标三槽位轮转（prev=last; last=新样本；
   首个"含有效位置"的样本才入 anchor——全零样本污染锚点会把整个
   WAL 历史当增量）；recv.applied_lsn 单调融合（GREATEST(origin, 反馈apply)）
2. 远端轮询（§4.5，仅 running）+ 反馈 apply 位折入 last（§4.6）
   ——放在历史记录之前，保证记录值与视图/报告同源（速率可验证）
3. 历史 append：每目标本轮 last 的全量快照（LSN + kind + 每轮状态字段）
   → 内存环形数组（环满覆盖最旧置 truncated）；样本时间戳未前进则跳过（防重复刷屏）
4. 错误恢复：PG_CATCH 内 SPI_finish + AbortOutOfAnyTransaction，
   仅成功路径 Commit——吞错的残留事务会卡死下一轮
```

**stop()**：superuser，无参。置 `running=false`、记录 `stop_ts`。停止后内存历史与视图继续可查。**`lrstat_export(name)` 查找**：当前会话（name 省略或等于当前）→ 内存环形中该会话的盖章条目（目标名取自当前槽位、槽位已复用时按条目 kind 推断侧别）→ 明确报错并提示 stop 后立即 export。

**竞态规则**：状态转换与采样轮经 `LRSessionState.mutex` + worker latch 协调——先置状态再唤醒；增量以该轮醒来时读到的状态为准，首/末间隔并入或剔除一个采样周期属可接受误差，`duration` 以实际样本区间为准。

### 4.5 远端轮询机制（接收端 → 发送端）

- **连接**：每个被监测的复制对一条**普通 libpq 连接**（libpqsrv 助手函数：FD 记账、wait event、信号中断安全）。连接串来源：
  - 逻辑复制：`pg_subscription.subconninfo`；
  - 物理复制：`pg_stat_wal_receiver.conninfo`（standby 侧解析后反向连主库）；
  仅注入 `application_name='pg_lrstat'`、`connect_timeout`、`options=-c statement_timeout=…`；凭证只在内存；
- **查询**：按槽名/连接名过滤发送端（与发送端本地采样同一 SQL 形态，见 §6）；结果写 RSEND 目标三槽位，**时间戳取轮询完成时的接收端本地时钟**；
- **预算与退避**：单轮总预算 `remote_poll_budget`（默认 500ms），超时标 `remote_state='stale'` 不阻塞本地采样；连接/查询失败指数退避（1s→2s→…→60s）并标 `unreachable`；
- **发送端可达但目标不存在**（初始建槽间隙/standby 断连）标 `stale`；
- wait event 在首次轮询时惰性注册（`WaitEventExtensionNew` 在 postmaster pre-load 阶段调用会段错误，必须惰性）。

### 4.6 接收端应用位点的融合（逻辑专用，物理天然准确）

**逻辑复制**：origin 的 `remote_lsn` 只在事务提交边界推进（§2.3-2），单大事务/初始同步期间恒为 0 会让应用速率与积压失真。融合规则：

```
采样时：applied = GREATEST(origin.remote_lsn, 上一轮融合值)   ← push 单调融合
每轮远端轮询成功后（在历史记录之前执行）：
    lrstat_bump_applied(recv_name, 反馈 apply 位)   ← 折入 last 槽位
    ——必须用反馈 apply 位（walsender 的 replay_lsn），不是 flush 位：
      flush 领先于应用，会把应用进度报高、速率虚快
视图/报告/history 使用同一条单调融合序列（速率可验证的基础）
```

**物理复制**：`pg_last_wal_replay_lsn()` 由恢复进程实时推进，物理反馈的 replay 位也是 walreceiver 直接上报的真实值——**无需融合**，直接采样即准确。

效果：速率与积压共用同一条既本地又连续的序列，行内自洽。逻辑的 `origin_local_lsn` 列保留 origin 原值（重启续传位点，排障对照用；物理复制此列恒 NULL）。

### 4.7 并发、锁与内存安全规则

- **单写者**：采样 worker 是共享内存唯一写者（bump_applied 的只增更新除外）；每目标 spinlock 保护本块，持锁只做 memcpy 级操作；
- **锁序**：会话状态 mutex → 目标 mutex，单向，无嵌套反转；
- **字符串提取**：一律 `SPI_getvalue()`（detoast + 独立副本 + NULL 安全），杜绝 toast 指针/零拷贝悬垂；
- **分配**：数组首配 `palloc`，仅对已分配块 `repalloc`（`repalloc(NULL)` 非法）；worker 每轮在独立内存上下文运行，轮末重置整体回收；
- **EXEC_BACKEND**：后端惰性 `ShmemInitStruct` 附加（以"曾 preload"标记门控，绝不误创建段）；worker 入口 `PGDLLEXPORT` 导出；
- **目标管理**：键 (kind, name, relid, worker_char)；逻辑的 `parallel apply` 行跳过（leader 行是规范源）；物理的 worker_char='p'（recovery 进程，每 standby 一个）；目标超过 `stale_target_ttl` 未再被采样（如 table sync worker 结束）即从视图过期剔除、槽位可被复用；满载丢弃计数入 `dropped_samples` 并限频 WARNING。

---

## 5. 用户接口

### 5.1 命令

```sql
lrstat_start() → text
    -- superuser。开始采样（全局唯一会话；已有 running 会话则报错）。
    -- 返回自动名 sess_<n>——仅用于报告文件名与按名补导。

lrstat_stop() → text
    -- superuser。停止采样（无参数）。返回会话名。

lrstat_export(name text DEFAULT NULL, format text DEFAULT 'html') → text
    -- 导出报告并直接写文件到 $PGDATA/pg_lrstat/exports/<name>.<format>，
    -- 返回绝对路径。**export 即持久化**——写出的报告文件是会话唯一的
    -- 持久产物。不传 name 导当前/最近会话；传 name（如 sess_3，见
    -- info 的 exported_report_names）补导环形内的指定会话；找不到报错
    -- 并提示 stop 后立即 export。

pg_lrstat_reset() → void
    -- superuser。会话回 idle 并清内存数据
```

### 5.2 视图总览（6 个）

| 视图 | 粒度 | 用途 |
| --- | --- | --- |
| `pg_lrstat_info` | 1 行 | 健康自检 + 当前会话状态 + 归档会话名列表 |
| `pg_lrstat_send_stat` | 发送端一目标**一轮**一行 | 发送端逐轮时序：状态/水位/积压/本轮速率/本轮判定 |
| `pg_lrstat_recv_stat` | 接收端一 worker**一轮**一行 | 接收端逐轮时序 |
| `pg_lrstat_cluster_stat` | 一复制对**一轮**一行 | 两端合成逐轮时序（recv 行 × 时间最近的发送端轮询样本配对）——日常巡检只看这个 |
| `pg_lrstat_send_history` | 一目标一轮一行 | 发送端全量原始样本（含每轮状态），**唯一数据源** |
| `pg_lrstat_recv_history` | 一目标一轮一行 | 接收端全量原始样本 |

三个 stat 视图完全从 history 现算（相邻同目标样本作差 = 本轮速率；条目自带每轮状态与水位），不预存任何派生量；会话平均速率只在导出报告里聚合。

### 5.3 视图字段详解（每列注明"为什么需要它"）

#### `pg_lrstat_info`（一行，15 列）

| 列 | 类型 | 为什么需要 |
| --- | --- | --- |
| `loaded` | bool | 第一步自检：false = 没预加载，其余视图全空，先修配置 |
| `session_name` | text | 当前/最近会话名——确认你在看的确实是刚跑的那轮 |
| `session_state` | text | idle/running/stopped/interrupted——是还在跑、已结束、还是崩溃了 |
| `session_start_ts` / `session_stop_ts` | timestamptz | 会话起止时间——和其他系统日志对时间线 |
| `session_truncated` | bool | true = 日志超上限有丢失——报告数据不完整，需扩容 |
| `session_degraded` | bool | true = 文件写失败——持久化不完整，导出可能缺数据 |
| `exported_report_names` | text[] | 已导出报告名列表（exports 目录）——报告即持久化产物 |
| `sample_interval_ms` | int8 | 瞬时速率的粒度——读数前先知道"30 秒内的平均"是什么概念 |
| `last_round_ts` | timestamptz | 采样 worker 最近一次成功——长期不更新 = worker 挂了 |
| `last_round_ok` | bool | 上一轮采样成功与否 |
| `last_round_error` | text | 失败时的错误信息——定位采样问题 |
| `nrounds` | int8 | 启动以来完成轮数——worker 存活证据（应随时间增长） |
| `dropped_samples` | int8 | 目标槽满导致丢弃——>0 需调大 max_targets |

**删除的列**（及原因）：`layout_version`（内部机制，运维无需感知）；`session_running`（`session_state='running'` 已覆盖）；`session_id`（运维用名字不用编号）；`elapsed_secs`（`session_start_ts` 可推算）；`max_targets`/`session_max_samples`/`remote_poll`（GUC 回显，不应在视图重复）。

#### `pg_lrstat_send_stat`（发送端一目标一轮一行，27 列）

| 列 | 为什么需要 |
| --- | --- |
| `slot_name` / `ts` | 目标标识与本轮采样时间——时序图的坐标轴 |
| `plugin` / `temporary` / `application_name` / `client_addr` | 直通属性（不随轮存储，取自当前槽位）——识别对端 |
| `active` / `sender_pid` | 本轮 walsender 是否在线——连接闪断可见 |
| `state` / `sync_state` / `wal_status` | 本轮 walsender 状态 / 同步角色 / 槽保留状态（lost=订阅会断）——逐轮追踪恶化 |
| `current_lsn` / `sent_lsn` / `confirmed_flush_lsn` | 本轮生成/发送/确认水位——与 history 同源 |
| `backlog_unsent` / `backlog_inflight` / `backlog_peer_unapplied` / `backlog_total` | 本轮三段积压（MB）——定位积压在哪一段 |
| `retained_wal` | 本轮槽扣住 WAL（MB）——磁盘风险 |
| `gen_mbps` / `send_mbps` / `apply_mbps` | 本轮速率（与前一同目标样本作差÷间隔）；首轮为 NULL——可手工从 history 复算 |
| `spill_mb` | 本轮解码溢写增量——解码压力瞬时尖峰 |
| `write_lag` / `flush_lag` / `replay_lag` | 本轮发送端观测三段延迟 |
| `send_blocked` | 本轮有未发送积压且发送速率≈0 |

#### `pg_lrstat_recv_stat`（接收端一 worker 一轮一行，17 列）

| 列 | 为什么需要 |
| --- | --- |
| `recv_name` / `ts` | 订阅与本轮时间 |
| `worker_type` / `worker_pid` / `leader_pid` / `relid` | 本轮 worker 类型（apply/table sync）、PID、并行 leader、tablesync 目标表——worker 生灭逐轮可见 |
| `received_lsn` / `applied_lsn` | 本轮收到/应用水位（applied=origin∪反馈的单调融合，提交边界） |
| `last_msg_send_time` / `last_msg_receipt_time` | 本轮心跳——链路中断的时间证据 |
| `backlog_apply` | 本轮收到未应用（MB） |
| `recv_mbps` / `apply_mbps` / `local_wal_mbps` | 本轮速率；首轮为 NULL |
| `apply_error_count` / `sync_error_count` | 本轮累计错误数 |
| `apply_blocked` | 本轮有未应用积压且速率≈0——锁冲突信号 |

#### `pg_lrstat_cluster_stat`（一复制对一轮一行，26 列，**两端合成**）

每个 recv 轮与**时间最近的**发送端轮询样本（RSEND，同名目标）配对成行；两端游标在时间序扫描中单调前进。

| 列 | 为什么需要 |
| --- | --- |
| `recv_name` / `ts` / `remote_state` | 链路标识、本轮时间、远端轮询状态 |
| `send_current_lsn` / `sent_lsn` / `confirmed_flush_lsn` | 发送端水位（配对样本） |
| `received_lsn` / `applied_lsn` | 接收端水位 |
| `gen_mbps` / `send_mbps` | 发送端本轮生成/发送速率（配对样本相邻差） |
| `recv_mbps` / `apply_mbps` | 接收端本轮速率 |
| `backlog_unsent` / `backlog_inflight` / `backlog_unapplied` / `backlog_total` | 本轮三段积压与总量（MB）——瓶颈归因的依据 |
| `retained_wal` / `feedback_lag_mb` | 槽扣住 WAL / 反馈滞后 |
| `write_lag` / `flush_lag` / `replay_lag` | 发送端观测三段延迟 |
| `catchup_send_secs` / `catchup_total_secs` | 按本轮速率的追平预估（秒） |
| `send_blocked` / `apply_blocked` | 本轮发送/应用堵住 |
| `bottleneck` | **本轮瓶颈判定**：send（发送速率落后生成且未发送积压过半）/ recv_apply / network / none——时序上可见瓶颈何时开始/结束 |

#### `pg_lrstat_send_history`（发送端一目标一轮一行，全量原始样本，始终记录）

| 列 | 为什么需要 |
| --- | --- |
| `name` / `ts` | 坐标轴 |
| `current_lsn` / `sent_lsn` / `peer_recv_lsn` / `peer_flush_lsn` / `peer_applied_lsn` | 生成、发送与反馈三水位——相邻作差即速率 |
| `confirmed_flush_lsn` / `restart_lsn` | 槽确认边界与保水起点（retained_wal = current − restart） |
| `spill_bytes` / `stream_bytes` | 解码溢写/流式累计计数 |

#### `pg_lrstat_recv_history`（接收端一目标一轮一行，全量原始样本，始终记录）

| 列 | 为什么需要 |
| --- | --- |
| `name` / `ts` | 同上 |
| `received_lsn` / `applied_lsn` / `local_wal_lsn` | 收到/应用（单调融合）/本地 WAL 水位 |

> stat 视图的逐轮状态列（state、worker_type、lag 等）同样落在 `LRHistoryEntry` 中（§4.3），但为控制宽度不在 history 视图输出——导出报告的 Evidence 区带全量。

### 5.4 GUC

| GUC | 默认 | 作用域 | 说明 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期 = 瞬时速率粒度（下限 1s） |
| `pg_lrstat.session_max_samples` | `2880` | 重启 | 历史环形容量（每目标采样数；2s≈96min/目标） |
| `pg_lrstat.max_targets` | `32` | 重启 | 目标槽容量 |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标消失后多少秒从视图过期剔除、槽位回收 |
| `pg_lrstat.catchup_min_rate` | `0.001` | SIGHUP | 追平预估有效性下限（MB/s，avg 低于此值返回 NULL） |
| `pg_lrstat.remote_poll` | on | SIGHUP | 接收端是否轮询发送端 |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 远端连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮远端轮询总预算 |
| `pg_lrstat.database` | `postgres` | 重启 | 采样 worker 连接库 |
| `pg_lrstat.allow_inject` | off | SUSET | 测试注入口（superuser + 前缀约束） |

### 5.5 报告导出 `lrstat_export(name, format)`

导出会话报告并**直接写文件**到 `$PGDATA/pg_lrstat/exports/<name>.<format>`（名字做文件系统安全字符清洗），返回绝对路径。**export 即持久化**：数据源为当前会话或内存环形内的按名会话，报告文件是唯一持久产物（无独立会话文件）。

#### 5.5.1 自动分析结论（导出的核心价值）

以下结论在**两个层面**可用：

- **SQL 层（会话期间随时可查）**：`cluster_stat.bottleneck` 列实时判定瓶颈环节（CASE WHEN），`catchup_*_secs` 列实时给出追平预估；
- **导出层（stop 后完整报告）**：四项结论在导出时从视图/历史数组计算，以**人话**呈现在报告最顶部。

**① 瓶颈判定**——根据积压分布 + 速率对比，自动定位到具体环节：

```
判定逻辑（按优先级）：
  if send_avg < gen_avg AND backlog_unsent 占总积压 > 50%:
      → 瓶颈 = 发送端（发送跟不上生成）
      → 若 spill_avg > 0: 深层原因 = 解码（大事务溢写磁盘）
      → 若 sync_state != 'async': 深层原因 = 等同步复制确认（不是真慢）
  elif backlog_unapplied 占总积压 > 50% AND apply_avg < recv_avg:
      → 瓶颈 = 接收端应用（数据到了没写进表）
      → 若 apply_blocked: 深层原因 = 锁堵塞（查 worker_pid 的 wait_event）
  elif backlog_inflight 占总积压 > 50% AND inflight >> feedback_lag:
      → 瓶颈 = 网络（数据在路上推不动）
  else:
      → 无明显瓶颈（四速率同频，积压趋零）
```

输出格式（HTML/JSON 均含）：

```json
{
  "analysis": {
    "bottleneck": "接收端应用",           // 发送端 / 接收端应用 / 网络 / 无
    "deep_cause": "锁堵塞",              // 解码溢写 / 等同步复制 / 锁堵塞 / —
    "evidence": {
      "gen_avg": 7.5, "send_avg": 7.5, "recv_avg": 7.4, "apply_avg": 3.2,
      "backlog_unsent_pct": 1, "backlog_unapplied_pct": 85, "backlog_inflight_pct": 14,
      "spill_avg": 0, "apply_blocked": true
    }
  }
}
```

**② 追平预估**——当前没 apply 的数据还要多久：

```
当前积压追平时间 = backlog_unapplied_mb / avg_apply_mbps   （秒）
  若 avg_apply ≈ 0 且积压 > 0: 显示"应用已停，追平时间不可估"
```

**③ 容量外推**——按本次会话的平均 apply 速率，同步不同量级 WAL 需要多久：

```
同步 50G  ≈ 50 × 1024 / avg_apply_mbps  秒
同步 100G ≈ 100 × 1024 / avg_apply_mbps  秒
同步 200G ≈ 200 × 1024 / avg_apply_mbps  秒
```

输出示例（HTML 中的表格）：

```
┌──────────────────────────────────────────────────┐
│  按本次会话平均应用速率 3.2 MB/s 计算：             │
├──────────┬───────────┬───────────────────────────┤
│ 同步量    │ 预计耗时    │ 备注                      │
├──────────┼───────────┼───────────────────────────┤
│ 当前积压  │   85MB    │ 27 秒                     │
│ 50 GB    │  4.4 小时  │ 假设发送端能跟上           │
│ 100 GB   │  8.9 小时  │                           │
│ 200 GB   │ 17.8 小时  │ ≈ 17 小时 47 分            │
└──────────┴───────────┴───────────────────────────┘
│ ⚠ 注意：假设平均速率持续；若 gen > send 则永远追不平 │
└──────────────────────────────────────────────────┘
```

**④ 净追平速率**（最重要的判断）：

```
净速率 = min(avg_send, avg_apply) − avg_gen
  > 0: 能追平（值越大追得越快）
  = 0: 保持现状（积压不再增长也不缩小）
  < 0: 追不平（差距只会越来越大，必须扩容/拆负载）
```

#### 5.5.2 导出格式

**`format='html'`（默认）——自包含单文件**，无 CDN、浏览器双击即开：

| 区块 | 内容 |
| --- | --- |
| **Analysis**（最顶部） | 瓶颈判定徽章 + 四速率（gen/send/recv/apply avg）+ 三段积压 + 追平预估 + 50/100/200GB 容量表 + 净追平速率 |
| **Chart**（紧随其后） | gen/send/apply 逐间隔速率折线（内嵌 SVG，无 JS 依赖） |
| Session | 会话名/起止/样本数/truncated/degraded 标记 |
| Targets | 每目标一行：侧别、状态、平均速率、最新 LSN |
| **Evidence**（底部佐证区，四张表） | send history samples（7 水位+spill/stream）、recv history samples（3 水位）、send rate history（逐间隔 gen/send/反馈apply/spill）、recv rate history（逐间隔 recv/apply）——头部每个结论都能从此手工重算 |

**`format='json'`**——机器可读（速率 4 位小数，可与容量推算精确互算）：

```json
{
  "session": { "name": "sess_3", "running": false, "start": "...", "stop": "...",
               "truncated": false, "degraded": false },
  "analysis": { "bottleneck": "none", "gen_avg": 0.0518, "send_avg": 0.0518,
                "recv_avg": 0.0518, "apply_avg": 0.0518,
                "backlog_unsent_mb": 0.0, "backlog_inflight_mb": 0.0,
                "backlog_unapplied_mb": 0.0, "backlog_total_mb": 0.0 },
  "capacity": { "catchup_secs": 0, "net_catchup_mbps": 0.0,
                "sync_50g_secs": 988181, "sync_100g_secs": 1976362,
                "sync_200g_secs": 3952725 },
  "send_stat": [ ... ],          /* 每目标状态摘要 */
  "recv_stat": [ ... ],
  "history": {
    "total_samples": 22, "exported_samples": 22,
    "samples": [ { "name": "...", "ts": "...", "kind": "recv",
                   "received_lsn": "0/...", "applied_lsn": "0/...",
                   "local_wal_lsn": "0/..." }, ... ]   /* 最近 500 条 */
  }
}
```

**容量口径**：`sync_50g_secs = 50×1024 MB ÷ apply_avg`（MB 按 1048576 字节）；100G/200G 严格 2×/4×。平均速率窗口从**首个含有效位置的样本**到 stop_ts（clamp），不受 stop 后空闲稀释。

### 5.6 权限

- 视图（含 report/sessions/info）：`REVOKE ALL FROM PUBLIC; GRANT SELECT TO pg_monitor`
- `lrstat_start/stop/reset/delete`、inject、export(html)：superuser
- `lrstat_export(..., 'json')`：pg_monitor

### 5.7 使用示例（完整流程）

```sql
SELECT lrstat_start();               -- 开始测量

-- 压测进行中，看两端合成时序（日常巡检只盯这一个）：
SELECT ts, recv_name, bottleneck,
       round(gen_mbps::numeric,2)   AS 生成,
       round(send_mbps::numeric,2)  AS 发送,
       round(apply_mbps::numeric,2) AS 应用,
       round(backlog_total::numeric,1) AS 总积压MB
FROM pg_lrstat_cluster_stat ORDER BY ts DESC LIMIT 10;

SELECT lrstat_stop();               -- 停止测量

SELECT lrstat_export();             -- 导出报告（含 Evidence 佐证表）

-- 事后追溯：名字查 info 的归档列表，按名导出
SELECT archived_session_names FROM pg_lrstat_info;
SELECT lrstat_export('sess_3');

-- 手工复核任意一条速率（可验证性）：
SELECT pg_wal_lsn_diff(applied_lsn, lag(applied_lsn) OVER (ORDER BY ts))
       / 1048576 / extract(epoch FROM ts - lag(ts) OVER (ORDER BY ts))
FROM pg_lrstat_recv_history WHERE name = 'asub';
```

---

## 6. 采样查询定义

三条只读 SQL（worker 每轮执行）：

**发送端（发送实例本地，逻辑+物理通用，按 kind 区分）**：

```sql
SELECT s.slot_name::text, d.datname::text, s.plugin::text, s.temporary, s.active,
       r.pid AS sender_pid, r.application_name::text,
       host(r.client_addr) AS client_addr, r.state, r.sync_state,
       pg_current_wal_lsn() AS current_lsn,
       r.sent_lsn, r.write_lsn, r.flush_lsn, r.replay_lsn,
       s.confirmed_flush_lsn, s.restart_lsn, s.wal_status, s.safe_wal_size,
       rs.spill_bytes, rs.stream_bytes, rs.total_bytes,
       (EXTRACT(EPOCH FROM r.write_lag)  * 1000000)::bigint AS write_lag_us,
       (EXTRACT(EPOCH FROM r.flush_lag)  * 1000000)::bigint AS flush_lag_us,
       (EXTRACT(EPOCH FROM r.replay_lag) * 1000000)::bigint AS replay_lag_us,
       r.reply_time
FROM pg_replication_slots s
LEFT JOIN pg_database d ON d.oid = s.datoid
LEFT JOIN pg_stat_replication r ON r.pid = s.active_pid     -- walsender 持有槽，精确关联
LEFT JOIN pg_stat_replication_slots rs ON rs.slot_name = s.slot_name
WHERE s.slot_type IN ('logical', 'physical');
-- 逻辑与物理 walsender 都出现在 pg_stat_replication，kind 列区分（PG18）
```

**接收端——逻辑复制（订阅实例本地）**：

```sql
SELECT su.subname::text, su.subslotname::text, su.subconninfo,
       st.worker_type, st.pid AS worker_pid, st.leader_pid, st.relid,
       st.received_lsn, st.latest_end_lsn,
       o.remote_lsn, o.local_lsn AS origin_local_lsn,
       pg_current_wal_lsn() AS local_wal_lsn,
       st.last_msg_send_time, st.last_msg_receipt_time, st.latest_end_time,
       ss.apply_error_count, ss.sync_error_count
FROM pg_subscription su
LEFT JOIN pg_stat_subscription st ON st.subid = su.oid
LEFT JOIN pg_replication_origin_status o
       ON o.external_id IN ('pg_' || su.oid, su.subname)   -- PG18 命名与旧命名双匹配
LEFT JOIN pg_stat_subscription_stats ss ON ss.subid = su.oid;
```

（worker 侧跳过 `worker_type='parallel apply'` 行——leader 行是规范数据源。）

**接收端——物理复制（standby 实例本地）**：

```sql
-- standby 的接收/回放位点与 walreceiver 状态（无 origin，无订阅）
SELECT 'standby' AS recv_name,
       wr.slot_name, wr.status, wr.sender_host, wr.sender_port,
       wr.written_lsn, wr.flushed_lsn, wr.latest_end_lsn,
       pg_last_wal_receive_lsn() AS received_lsn,
       pg_last_wal_replay_lsn()  AS applied_lsn,
       NULL::pg_lsn AS origin_local_lsn,
       pg_current_wal_lsn() AS local_wal_lsn,
       wr.last_msg_send_time, wr.last_msg_receipt_time, wr.latest_end_time,
       wr.conninfo
FROM pg_stat_wal_receiver wr;
```

**远端轮询（接收端凭连接串在发送端执行，$1=槽名/连接标识）**：与发送端查询同构，`WHERE s.slot_name = $1`，仅取位点子集；结果时间戳打**接收端本地时钟**。

---

## 7. 性能与开销

- **采样**：每轮两条轻量 SELECT + 每订阅一次远端 SELECT，30s 周期下 CPU 可忽略；worker 每轮在独立内存上下文运行，轮末整体重置；
- **共享内存**：默认 ≈5MB（公式见 §4.3），启动期一次预留；
- **查询**：live 视图只读三槽位（O(目标数)），无历史扫描；report 聚合在 stop 时一次完成，查询读现成结果；intervals 视图线性于会话间隔数；
- **持久会话追加**：每轮一次 write+fsync（30s 一次，毫秒级）；非持久会话零文件 IO；
- **默认零写入承诺**：不写表、不产生 WAL、不建 PGDATA 目录。

## 8. 边界场景

| 场景 | 行为 |
| --- | --- |
| 会话中目标中途加入（建新订阅/槽） | 锚点取首见样本，报告 `partial=true` |
| 历史环形写满 | `truncated=true`，覆盖最旧样本，报告标注 |
| 实例重启 | 内存会话数据清空；**已 export 的报告文件保留**——export 即持久化 |
| 接收端锁堵塞应用（逻辑） | `apply_blocked=t`；逐间隔序列呈台阶形（起点/拐点=被堵/放开时刻） |
| 单个大事务回放 | apply 瞬时速率归 0（位点仅提交边界推进），`blocked` 区分，报告 min 值体现 |
| 远端发送端宕机 | `remote_state='unreachable'`，指数退避重连，本地采样不受影响 |
| 目标数超容量 | 丢弃计数 + 限频 WARNING（`pg_lrstat_info.dropped_samples`） |
| 重复 start / 重名 / 名字不符的 stop | 一律报错，不产生副作用 |
| 物理复制 standby 升主 / 断连 | RECV 目标 `worker_type='recovery'` 行的位点停止推进；RSEND 目 `remote_state` 转 unreachable |
| 物理复制级联（A→B→C） | B 同时是发送端（对 C）和接收端（对 A），两侧视图独立展现 |
| 逻辑+物理混合部署 | SEND 视图列出所有 walsender（kind 区分），RECV 视图同时有 apply worker 与 recovery 行 |

## 9. 测试方案

| 组 | 内容 |
| --- | --- |
| 回归（pg_regress，--temp-config 预加载） | 注入驱动：start('t1') 后注入样本 → 瞬时=末间隔差分、avg=全程差分精确断言；单间隔时瞬时=NULL、avg=首段值；stop 报告 total/min/max/峰值与手算一致；重复 start/重名/错名 stop 报错；报告按名可查 |
| TAP 001 | 同上注入算术 |
| TAP 002 / scripts/logical_rep_test.sh | 真实发布订阅：start → pgbench -T N → stat 时序速率非零、逐轮波动 → stop → stat 总量与 pgbench WAL 量级一致、history 行数 ≈ N/30 |
| 持久化专项 | stop 后 export → restart → 报告文件仍在；未 export 的会话重启后按名导出报错并提示 |
| 报告导出 | html 可解析且含三类图数据点与瓶颈判定；json 与视图逐字段对拍（含 analysis/capacity）；dest 越界拒绝；dest='-' 与落盘一致 |
| 分析结论 | 注入已知速率/积压 → 验证 `cluster_stat.bottleneck` 判定：send<gen+unsent>50%→'send'；apply<recv+unapplied>50%→'recv_apply'；四速率同频→'none'；spill>0 时 deep_cause 正确；export JSON 的 analysis/capacity 与手算对拍 |
| 会话边界 | 目标中途加入 partial；stop 恰逢采样轮；stop 后延迟 export |
| 物理复制 | primary+standby 集群：send_stat 列出物理连接（kind=physical）、recv_stat 列出 recovery 行、cluster_stat 合成两端；pgbench 负载下四速率一致；standby 断连→remote_state 转 unreachable；级联场景两侧独立 |

## 10. 实施计划

| 阶段 | 交付 |
| --- | --- |
| P0 | 会话状态机 + start/stop + 三槽位采样 + live 视图族（双速率，逻辑复制） |
| P1 | 内存会话日志 + send/recv/cluster_stat + send/recv_history + 回归/TAP 重写 |
| P2 | （历史阶段）持久化文件路径——后被移除，export 报告即持久化 |
| P3 | 报告导出（json → html+SVG）+ bottleneck 列 + 分析结论测试 |
| P4 | 物理复制支持（send SQL kind 过滤、standby 采样 SQL、pg_stat_wal_receiver 视角）+ 物理专项测试 |
| P5 | 用户文档（USER_MANUAL/BEST_PRACTICES 按会话模型改写，含物理复制） |

## 附录：示例输出（目标形态）

```sql
SELECT lrstat_start(true);
-- 压测期间（cluster_stat 是日常巡检唯一需要盯的视图，逐轮时序）：
SELECT ts, recv_name, bottleneck,
       round(send_mbps::numeric,1) AS 发送,
       round(apply_mbps::numeric,1) AS 应用,
       round(backlog_total::numeric,1) AS 总积压MB
FROM pg_lrstat_cluster_stat ORDER BY ts DESC LIMIT 3;

     ts      | recv_name | bottleneck | 发送 | 应用 | 总积压mb
------------+-----------+------------+------+------+----------
 11:26:02   | asub      | none       |  0.1 |  0.1 |         

SELECT lrstat_stop();
SELECT lrstat_export();       -- 报告 + Evidence 佐证表
```
