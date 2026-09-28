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
lrstat_start('mig_20260924', persist := true)      lrstat_stop('mig_20260924')
        │                                              │
        ▼                                              ▼
   ┌──────────────── 会话进行中 ────────────────────────┐
   │  采样 worker 每 30s 采集一次全链路位点               │
   │  随时查询：瞬时速率（最近一个采样间隔）＋平均速率（自 start） │
   │  persist=true 时逐间隔数据双写会话文件                │
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
| **瞬时速率（instant）** | 最近两个样本的差分 ÷ 采样间隔——"刚刚 30 秒怎么样" |
| **平均速率（avg）** | 最新样本 − 锚点 ÷ 经过时间——"这轮从头到现在平均多少" |
| **历史数组** | 每轮每目标一条**全量原始 LSN 样本**的环形记录（始终在记；persist 会话另落盘一份不覆盖的完整副本） |
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

### 3.2 双速率

对任一速率字段 f ∈ {gen(C0), send(C2), recv(C3′), apply(C5′), spill(D1), stream(D1), confirm(C6)}——apply 在逻辑复制=应用速率、在物理复制=回放速率，列名统一为 apply：

```
瞬时 instant(f) = (f(S_last) − f(S_prev)) / (ts(S_last) − ts(S_prev))
平均 avg(f)     = (f(S_last) − f(S_anchor)) / (ts(S_last) − ts(S_anchor))
```

- 单位：**MB/s**（内部按 LSN 差分得到字节/秒，输出前 ÷ 1048576 转为 MB/s）；
- 瞬时速率的粒度 = `sample_interval`（默认 30s，可调至 1s）；
- 数值示例：两样本相隔 110s，`sent_lsn` 从 `0/0` 到 `0/800000`（8 MB）→ `send` 瞬时 = 平均 = 8 ÷ 110 ≈ 0.0727 MB/s；
- 有效性规则：最新样本距查询超过 3×采样间隔（采样中断）→ 速率列 NULL；`S_prev` 缺失（会话首轮）→ 瞬时列 NULL，平均列=首段值；
- **速率是页码（LSN）差不是网线字节**：与带宽对比需乘解码膨胀率（`total_bytes` 增量 ÷ C0 增量）。

### 3.3 会话报告指标（stop 后聚合）

| 指标 | 计算 | 说明 |
| --- | --- | --- |
| `duration_secs` | 实际首末样本区间 | 以样本为准，非命令时刻 |
| `total_f_mb` | f(S_last) − f(S_anchor)（÷ 1048576） | 各字段会话总量（MB） |
| `avg_f` | total ÷ duration | 会话平均（= stop 时刻 live 的 avg） |
| `f_instant_min/max/avg` | 历史相邻样本差值的统计 | 瞬时速率分布（峰值/谷值/均值） |
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
| 采样器 | `lrstat_worker.c` | bgworker 主循环：采样三槽位轮转、全量样本入历史、persist 会话双写文件 |
| 远端轮询 | `lrstat_remote.c` | 接收端→发送端只读轮询（libpqsrv、预算/退避、反馈位回填） |
| 共享内存 | `lrstat_shmem.c` | 会话状态、目标表（anchor/prev/last）、历史环形数组（覆盖最旧） |
| 会话文件 | `lrstat_store.c` | persist 会话的文件读写（双写/fsync/原子头尾/启动恢复） |
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

**空间公式**：`MAXALIGN(头) + max_targets × MAXALIGN(sizeof(LRTargetCtl)+3×sizeof(LRSample)) + session_max_samples × max_targets × sizeof(LRHistoryEntry) + 16 × sizeof(LRSessionRegEntry)`。历史条目含 session_id（会话盖章，0=会话外）；会话注册表（16 槽环形）把 session_id 映射回会话名，视图据此输出 session_name 列。默认（32 目标 / 2880 间隔）≈ 0.3MB + 11MB ≈ **11MB**，postmaster 启动期一次预留。

### 4.4 会话机制

**状态机**：

```
idle ──start(name,persist)──► running ──stop(name)──► stopped ──start(name')──► running(...)
  │                             │  ▲
  │                             │  └─ 采样轮：prev=last; last=新样本;
  │                             │     每轮把全量样本 append 历史（+persist 会话文件）
  │                             └ 实例重启：恢复线程把 persist 会话收尾为 interrupted
  └ 历史（环形）始终在记，非 persist 会话数据随重启消失；目录中的历史会话任意可查
```

**start(name, persist)**：superuser。校验：当前无 running 会话、名字不与历史会话冲突（重名报错，提示换名或 `lrstat_delete`）。动作：`session_id++`、记录名字与 `start_ts`、清空历史数组与全部目标的 anchor/prev/last、`persist=true` 时创建会话文件（头 `state=running`）；**唤醒 worker 立即执行一轮采样**——该轮样本即锚点，保证 start 后一个采样间隔内即可查到双速率。

**采样轮**（每 `sample_interval`）：

```
1. 三槽位轮转：首见样本入 anchor；prev=last; last=新样本（非 running 同样轮转）
2. 历史 append：每目标本轮 last 的全量 LSN 快照 → 内存环形数组；
   环满覆盖最旧并置 truncated=true（视图按时间升序读）；
   persist 会话同步追加会话文件并 fsync（文件不覆盖，落全量）
   同一目标样本时间戳未前进（如 stop 后远端镜像冻结）则跳过，防止重复刷屏
3. 远端轮询（§4.5）+ 应用位点回填（§4.6）只在 running 时执行
```

**stop(name)**：superuser，名字与 running 会话一致否则报错。置 `running=false`、记录 `stop_ts`；persist 会话文件追加**尾部目标表**（target_idx → 名字/类型，magic 落在文件末 8 字节便于从 EOF 定位）后置头 `state=stopped`（条目数按文件大小回填）。停止后内存历史与视图继续可查，直到下一次 start 清零；**归档文件支持 `lrstat_export(name)` 按名重建报告**（从条目恢复每目标首/末样本作锚点，重启后亦然）。

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
采样时：applied = origin.remote_lsn（§6 接收端 SQL）
每轮远端轮询成功后：
    lrstat_bump_applied(recv_name, 反馈 flush 位)   ← 只增更新 last 槽位的 applied，
                                                    时间戳不动（差分序列保持单调）
视图/报告使用：max(origin, 反馈 flush)
```

**物理复制**：`pg_last_wal_replay_lsn()` 由恢复进程实时推进，物理反馈的 replay 位也是 walreceiver 直接上报的真实值——**无需融合**，直接采样即准确。

效果：速率与积压共用同一条既本地又连续的序列，行内自洽。逻辑的 `origin_local_lsn` 列保留 origin 原值（重启续传位点，排障对照用；物理复制此列恒 NULL）。

### 4.7 会话文件与持久化机制（仅 `persist=true`）

文件位于 `$PGDATA/pg_lrstat/sessions/<name>.sess`，worker 直接读写（不经 SQL 目录、不产生 WAL）。**默认 persist=false 不创建任何文件与目录**。

```
<name>.sess 布局（小端、带 layout_version）：
┌ 会话头 ─ magic, layout_version, session_name, session_id,
│          start_ts, stop_ts, state(running/stopped/interrupted),
│          truncated, degraded, n_targets, 目标键数组[(kind,name,relid,worker_char)]
├ 逐间隔记录[] ─ 每采样轮一条：{round_ts, n_entries, LRSessionEntry[n]}
│                （与内存日志同步双写，每轮 fsync——30s 一次的开销）
└ 会话尾 ─ stop 时聚合写入：§3.3 全部报告指标 + 每目标统计
```

- **原子性**：头/尾更新走"同目录临时文件 + rename"，避免半写；
- **崩溃语义**：双写 + fsync 使崩溃至多丢最后一个未落盘间隔；
- **启动恢复**：shmem 启动 hook 扫描会话目录（目录不存在即跳过——默认常态）；`state=running` 的文件自动补写会话尾、标 `interrupted` 归档；内存会话索引（名/id/状态/时间戳）随之重建。**不做跨重启续跑**（锚点失效，速率语义不可靠）；
- **降级**：文件写失败（磁盘满/权限）不阻断采样——内存侧继续、`degraded=true`、stop 时 WARNING；
- **清理**：`lrstat_delete(name)` 删除归档（superuser，不可恢复；非持久会话提示无归档）；无自动过期，目录大小靠巡检。
- **版本迁移**：`layout_version` 变更后，启动恢复**跳过**不匹配的旧文件并限频 WARNING（列出文件名），不做自动迁移——保留原文件不损坏，用户可手动删除。

### 4.8 并发、锁与内存安全规则

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
lrstat_start(name text DEFAULT NULL, persist boolean DEFAULT false)
    → (session_id bigint, session_name text, persisted boolean, started_at timestamptz)
    -- superuser。重复 start / 名字冲突 / 名字非法（路径字符）报错；
    -- 名字省略自动生成 sess_<id>_<yyyymmdd_hhmmss>；persist 默认 false

lrstat_stop(name text)
    → (session_name, started_at, stopped_at)
    -- superuser。结束指定名字的会话（须与 start 一致，防误停）；
    -- stop 后查 cluster_stat / send_stat / recv_stat 看完整报告

lrstat_export(name text, format text DEFAULT 'html', dest text DEFAULT NULL)
    → (path text, content text, bytes bigint)
    -- format: 'html'（自包含可视化）/ 'json'（机器可读）；详见 §5.5

lrstat_delete(name text) → void
    -- superuser。删除一个已归档会话文件；运行中/非持久会话相应报错或提示

pg_lrstat_reset() → void
    -- superuser。强制会话回 idle 并清内存数据；不动已归档文件
```

### 5.2 视图总览（仅 6 个）

| 视图 | 粒度 | 一句话 |
| --- | --- | --- |
| `pg_lrstat_info` | 一行 | 扩件健康 + 当前会话状态 + 配置生效值 |
| `pg_lrstat_send_stat` | 发送端一连接一行 | 发送端全量：位点 + 积压 + 双速率（gen/send/spill/stream） |
| `pg_lrstat_recv_stat` | 接收端一 worker/恢复进程一行 | 接收端全量：位点 + 积压 + 双速率（recv/apply/local_wal） |
| `pg_lrstat_cluster_stat` | 一复制对一行 | **唯一能看到两端合成数据的视图**：双端位点/速率/积压/追平预估 |
| `pg_lrstat_send_history` | 发送端一目标一轮一行 | 发送端全量原始 LSN 序列（始终记录，画曲线），首列 session_name 标归属 |
| `pg_lrstat_recv_history` | 接收端一目标一轮一行 | 接收端全量原始 LSN 序列（始终记录，画曲线），首列 session_name 标归属 |
| `pg_lrstat_session_stat` | 一会话×目标×侧一行 | 纯 SQL 视图：对 history 按会话聚合（总量 MB、平均 MB/s、样本区间）——按会话名查历史统计的入口 |

- `stat` 视图**会话期间实时更新**（显示最新样本+当前速率），**stop 后冻结**（显示会话最终值）——不需要区分 live/report 两套；
- `history` 视图**stop 后可查**（会话期内逐间隔数据）；持久会话跨重启可查；
- `cluster_stat` 是唯一的"两端"视图——`send_stat`/`recv_stat` 只看各自端，运维日常巡检/排障只需盯 `cluster_stat` 一个。

### 5.3 视图字段详解（每列注明"为什么需要它"）

#### `pg_lrstat_info`（一行，15 列）

| 列 | 类型 | 为什么需要 |
| --- | --- | --- |
| `loaded` | bool | 第一步自检：false = 没预加载，其余视图全空，先修配置 |
| `session_name` | text | 当前/最近会话名——确认你在看的确实是刚跑的那轮 |
| `session_state` | text | idle/running/stopped/interrupted——是还在跑、已结束、还是崩溃了 |
| `session_persisted` | bool | false = 重启后数据丢失——决定要不要在重启前导出报告 |
| `session_start_ts` / `session_stop_ts` | timestamptz | 会话起止时间——和其他系统日志对时间线 |
| `session_truncated` | bool | true = 日志超上限有丢失——报告数据不完整，需扩容 |
| `session_degraded` | bool | true = 文件写失败——持久化不完整，导出可能缺数据 |
| `archived_session_names` | text[] | 归档会话名列表——`lrstat_export(name)` 和 `lrstat_delete(name)` 需要知道名字，从这里查 |
| `sample_interval_ms` | int8 | 瞬时速率的粒度——读数前先知道"30 秒内的平均"是什么概念 |
| `last_round_ts` | timestamptz | 采样 worker 最近一次成功——长期不更新 = worker 挂了 |
| `last_round_ok` | bool | 上一轮采样成功与否 |
| `last_round_error` | text | 失败时的错误信息——定位采样问题 |
| `nrounds` | int8 | 启动以来完成轮数——worker 存活证据（应随时间增长） |
| `dropped_samples` | int8 | 目标槽满导致丢弃——>0 需调大 max_targets |

**删除的列**（及原因）：`layout_version`（内部机制，运维无需感知）；`session_running`（`session_state='running'` 已覆盖）；`session_id`（运维用名字不用编号）；`elapsed_secs`（`session_start_ts` 可推算）；`max_targets`/`session_max_samples`/`remote_poll`（GUC 回显，不应在视图重复）。

#### `pg_lrstat_send_stat`（发送端，一复制连接一行，37 列）

| 列 | 类型 | 为什么需要 |
| --- | --- | --- |
| **标识（11 列）** | | |
| `slot_name` | text | 目标标识——与 cluster_stat/日志对位 |
| `kind` | text | logical/physical——一种 SQL 适配两种复制 |
| `sample_time` | timestamptz | 数据新鲜度——距今多久 |
| `session_name` | text | 属于哪个测量会话 |
| `database` | text | 槽属于哪个库——多库集群区分 |
| `plugin` | text | 输出插件（pgoutput/wal2json）——解码排障要知道 |
| `temporary` | bool | true=tablesync 临时槽——别和主槽混淆 |
| `active` | bool | walsender 是否在线——false=订阅端断开 |
| `sender_pid` | int4 | 关联 pg_stat_activity 排查 walsender 进程 |
| `application_name` | text | 订阅端标识——定位是哪个订阅连的 |
| `client_addr` | text | 订阅端 IP——网络排障 |
| **状态（4 列）** | | |
| `state` | text | walsender 状态（streaming/catchup）——catchup=正在追 |
| `sync_state` | text | async/sync——sync 时的"慢"是在等备库不是真慢 |
| `wal_status` | text | reserved/unreserved/lost——**lost=数据已丢必须重建** |
| `safe_wal_size` | float8 | 距 WAL 上限余量（MB）——磁盘风险预警 |
| **位点（3 列）** | | |
| `current_lsn` | pg_lsn | WAL 生成顶端——与外部工具（pg_waldump）交叉对照 |
| `sent_lsn` | pg_lsn | 已发送位——同上 |
| `confirmed_flush_lsn` | pg_lsn | 槽确认水位——WAL 回收决策位 |
| **积压（5 列）** | | |
| `backlog_unsent` | float8 | 已生成未发送（MB） |
| `backlog_inflight` | float8 | 已发送对端未确认接收（MB） |
| `backlog_peer_unapplied` | float8 | 对端已收未应用（MB，反馈口径） |
| `backlog_total` | float8 | 端到端总积压（MB） |
| `retained_wal` | float8 | 槽扣住的 WAL（MB）——磁盘风险指标 |
| **速率（14 列，MB/s）** | | |
| `gen_instant` / `gen_avg` | float8 | WAL 生成——是否上游产出突增 |
| `send_instant` / `send_avg` | float8 | 发送——发送是否跟得上生成 |
| `apply_instant` / `apply_avg` | float8 | 对端应用（经反馈）——发布端视角看订阅端 |
| `spill_instant` / `spill_avg` | float8 | 解码溢写——>0 = 大事务解码压力 |
| `stream_instant` / `stream_avg` | float8 | 解码流式——>0 = 流式大事务 |
| **延迟与判定（5 列）** | | |
| `write_lag` / `flush_lag` / `replay_lag` | interval | 内核反馈延迟（唯一跨机时间源）——定位延迟在网络还是对端 |
| `send_blocked` | bool | **发送堵住**：unsent>0 且最近间隔 send≈0 |

**删除的列**（及原因）：`peer_recv_lsn`/`peer_flush_lsn`/`peer_applied_lsn`（三个中间位点——积压列已总结，运维不需要原值）；`restart_lsn`（retained_wal 已总结）；`spill_bytes`/`stream_bytes`/`total_bytes` 累计原值（速率列已总结，原值在 history 可查）；`confirm_instant`/`confirm_avg`（运维看 retained_wal 趋势，不看确认速率）；`reply_time`（active+state 已覆盖）；`elapsed_secs`（可推算）。

#### `pg_lrstat_recv_stat`（接收端，一 worker/恢复进程一行，23 列）

| 列 | 类型 | 为什么需要 |
| --- | --- | --- |
| **标识（8 列）** | | |
| `recv_name` | text | 逻辑=订阅名 物理=standby 标识 |
| `kind` | text | logical/physical |
| `sample_time` / `session_name` | — | 同 send_stat |
| `worker_type` | text | apply/tablesync/recovery——区分 worker 角色 |
| `worker_pid` | int4 | 关联 pg_stat_activity **查锁堵**——排障第一步 |
| `slot_name` | text | 与 send_stat 对位的键（= 目标键 name） |
| `leader_pid` / `relid` | int4/oid | 并行 apply 拓扑 / tablesync 目标表 |
| **位点（2 列）** | | |
| `received_lsn` | pg_lsn | 本地实收位——无反馈延迟，判断分仓状态首选 |
| `applied_lsn` | pg_lsn | 已应用位——origin∪反馈融合后 |
| **消息时间（2 列）** | | |
| `last_msg_send_time` / `last_msg_receipt_time` | timestamptz | 一对时间戳给出**单条消息的网络延迟**——网络排障原始素材 |
| **积压（1 列）** | | |
| `backlog_apply` | float8 | 已收未应用（MB） |
| **速率（6 列，MB/s）** | | |
| `recv_instant` / `recv_avg` | float8 | 接收速率——分仓收货能力 |
| `apply_instant` / `apply_avg` | float8 | 应用/回放速率——分仓上架能力（瓶颈最常见的列） |
| `local_wal_instant` / `local_wal_avg` | float8 | 分仓本地 WAL 速率——应用产生的写入量 |
| **错误与判定（3 列）** | | |
| `apply_error_count` / `sync_error_count` | int8 | 冲突/错误计数——在涨 = 反复重试 |
| `apply_blocked` | bool | **应用堵住**：backlog>0 且最近间隔 apply≈0——查 worker_pid 的 wait_event |

**删除的列**：`origin_local_lsn`（重启续传位点，日常运维不用，排障时从系统视图查）；`local_wal_lsn`（速率列已总结）；`latest_end_lsn`（received_lsn 的回退源，不直接展示）；`latest_end_time`（last_msg_receipt_time 已覆盖）。

#### `pg_lrstat_cluster_stat`（一复制对一行，**唯一两端合成视图**，38 列）

| 列组 | 列 | 为什么需要 |
| --- | --- | --- |
| **标识（5 列）** | | |
| | `recv_name` / `slot_name` / `kind` / `session_name` | 复制对标识 |
| | `sample_time` | 数据新鲜度 |
| **轮询健康（2 列）** | | |
| | `remote_state` / `last_remote_poll_time` | 接收端能否轮到发送端——unreachable=网络问题 |
| **位点（5 列）** | | |
| | `send_current_lsn` / `sent_lsn` | 发送端两个主检查点——与外部工具交叉对照 |
| | `received_lsn` / `applied_lsn` | 接收端两个主检查点 |
| | `confirmed_flush_lsn` | WAL 回收水位 |
| **速率（16 列，MB/s）** | | |
| | `gen_instant`/`gen_avg` | 生成——上游是否突增 |
| | `send_instant`/`send_avg` | 发送——是否跟得上 |
| | `recv_instant`/`recv_avg` | 接收——分仓收货 |
| | `apply_instant`/`apply_avg` | 应用——分仓上架（最常见瓶颈列） |
| | `spill_instant`/`spill_avg` | 解码溢写——大事务压力 |
| | `stream_instant`/`stream_avg` | 解码流式——流式大事务 |
| **积压（6 列）** | | |
| | `backlog_unsent` | 没发货 |
| | `backlog_inflight` | 在路上 |
| | `backlog_unapplied` | 到了没上架 |
| | `backlog_total` | 总积压（三段之和） |
| | `retained_wal` | 磁盘风险 |
| | `feedback_lag_mb` | 反馈滞后（MB）——区分"真在途"和"10s 回执假象" |
| **延迟（3 列）** | | |
| | `write_lag`/`flush_lag`/`replay_lag` | 时间维度延迟——定位网络 vs 应用 |
| **追平与堵住（4 列）** | | |
| | `catchup_send_secs` / `catchup_total_secs` | 还要多久追平（秒） |
| | `send_blocked` / `apply_blocked` | 告警源——哪个环节停了 |
| | `bottleneck` | text：`send`/`recv_apply`/`network`/`none`——SQL CASE WHEN 实时判定（§5.5.1），**会话期间随时可查** |

**删除的列**：`restart_lsn`（retained_wal 已总结）；`elapsed_secs`（可推算）。

#### `pg_lrstat_send_history`（发送端一目标一轮一行，全量原始样本，始终记录）

| 列 | 为什么需要 |
| --- | --- |
| `name` / `ts` | 画曲线的坐标轴 |
| `current_lsn` / `sent_lsn` | 生成与发送水位——相邻行作差即逐间隔速率（MB/s） |
| `peer_recv_lsn` / `peer_flush_lsn` / `peer_applied_lsn` | 接收端反馈三水位——网络/落盘/应用逐段延迟定位 |
| `confirmed_flush_lsn` / `restart_lsn` | 槽位点与保水起点（retained_wal = current − restart） |
| `spill_bytes` / `stream_bytes` | 解码溢出/流式计数——解码压力时序 |

#### `pg_lrstat_recv_history`（接收端一目标一轮一行，全量原始样本，始终记录）

| 列 | 为什么需要 |
| --- | --- |
| `name` / `ts` | 同上 |
| `received_lsn` / `applied_lsn` | 收到与应用水位——相邻行作差即 recv/apply 速率 |
| `local_wal_lsn` | 接收端本地 WAL 位点——与 received 对比看重放写入速率 |

**设计原则**：history 只存**原始 LSN 快照**，不预存差值/水位/积压——一切派生量（速率、积压、峰值）由查询或导出时对相邻样本现算，history 是其他状态表与报告的**最原始数据来源**。

**删除的列**（history 视图）：`level_restart`（retained_wal 可由 level_current - level_restart 算，但 export 不画保水曲线，不预存）；`kind`（视图名已区分侧）。

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

### 5.5 报告导出 `lrstat_export(name, format, dest)`

导出指定会话的全部 6 个视图数据 **+ 自动分析结论**。非持久会话从内存渲染（窗口期至下次 start），持久会话从归档渲染。

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

**`format='html'`（默认）——自包含单文件**：无 CDN、全部 CSS/JS 内嵌，浏览器双击即开：

| 区块 | 内容 |
| --- | --- |
| **分析结论卡片**（最顶部） | 瓶颈判定 + 深层原因 + 追平预估 + 容量外推表 + 净追平速率——运维第一眼就看这里 |
| 头部卡片 | 会话名/时间窗/时长/目标数/状态/三大总量 |
| 速率时序图 | gen/send/recv/apply 逐间隔曲线 + 平均线，内嵌 SVG + 原生 JS |
| 积压堆叠面积图 | unsent/inflight/unapplied 堆叠 + total 总线，峰值标注 |
| 四水位阶梯图 | current/sent/received/applied |
| 统计表 | send_stat / recv_stat / cluster_stat 全列，每目标一行可排序 |
| 解码压力 | spill/stream 逐间隔柱状图（有数据才渲染） |

**`format='json'`**：机器可读完整数据包：

```json
{
  "session": { "name": "...", "start": "...", "stop": "...", "state": "stopped" },
  "analysis": { ... },           // §5.5.1 的四项结论
  "capacity": {                   // §5.5.1-③ 的容量外推
    "avg_apply_mbps": 3.2,
    "catchup_current": { "backlog_mb": 85, "est_secs": 27 },
    "sync_50g_secs": 16000, "sync_100g_secs": 32000, "sync_200g_secs": 64000,
    "net_catchup_mbps": -0.1      // §5.5.1-④
  },
  "send_stat": [ ... ],
  "recv_stat": [ ... ],
  "history": {
    "total_samples": 101, "exported_samples": 101,
    "samples": [ ... ]              /* 全量原始 LSN 样本（最近 500 条） */
  }
}
```

**`dest`**：默认 `$PGDATA/pg_lrstat/exports/`；`dest := '-'` 直接返回内容。返回 `(path, content, bytes)`。

### 5.6 权限

- 视图（含 report/sessions/info）：`REVOKE ALL FROM PUBLIC; GRANT SELECT TO pg_monitor`
- `lrstat_start/stop/reset/delete`、inject、export(html)：superuser
- `lrstat_export(..., 'json')`：pg_monitor

### 5.7 使用示例（完整流程）

```sql
SELECT lrstat_start('mig_20260924', persist := true);

-- 压测进行中，看两端合成视图（日常巡检只盯这一个）：
SELECT recv_name,
       round(send_instant::numeric,1) AS 发送_瞬时,
       round(send_avg::numeric,1)     AS 发送_平均,
       round(apply_avg::numeric,1)    AS 应用_平均,
       round(backlog_total::numeric,1) AS 总积压MB,
       round(catchup_total_secs)      AS 追平秒
FROM pg_lrstat_cluster_stat;

SELECT lrstat_stop('mig_20260924');

-- 会话结束后看逐间隔发送速率（相邻样本 sent_lsn 作差）：
SELECT ts, round(pg_wal_lsn_diff(sent_lsn, lag(sent_lsn) OVER (ORDER BY ts)) / 1024 / 1024
                 / extract(epoch FROM ts - lag(ts) OVER (ORDER BY ts)), 1) AS 发送MBps
FROM pg_lrstat_send_history WHERE name = 'asub' ORDER BY ts;

-- 会话期间实时查瓶颈（不需要导出）：
SELECT recv_name, bottleneck, round(catchup_total_secs) AS 追平秒
FROM pg_lrstat_cluster_stat;

-- 可视化导出（全部视图数据 + 分析结论）：
SELECT lrstat_export('mig_20260924');            -- HTML 报告
SELECT lrstat_export('mig_20260924','json');     -- 机器可读
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
| 历史环形写满 | `truncated=true`，覆盖最旧样本（persist 文件不覆盖），报告标注 |
| persist 会话文件写失败 | 采样不中断，`degraded=true`，stop 时 WARNING |
| 实例在 persist 会话中崩溃 | 重启后自动收尾为 `interrupted` 归档，丢失 ≤1 间隔 |
| 非 persist 会话遇重启 | 会话与报告消失（设计行为） |
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
| TAP 002 / scripts/logical_rep_test.sh | 真实发布订阅：start → pgbench -T N → stat 双速率非零、instant 波动、avg 单调收敛 → stop → stat 总量与 pgbench WAL 量级一致、history 行数 ≈ N/30 |
| 持久化专项（persist=true） | stop 后 restart 报告完整；kill -9 mid-session → interrupted、丢失 ≤1 间隔；只读目录注入 → degraded；delete 生效 |
| 默认零文件路径 | persist=false 全流程不创建任何文件/目录；重启后无痕 |
| 报告导出 | html 可解析且含三类图数据点与瓶颈判定；json 与视图逐字段对拍（含 analysis/capacity）；dest 越界拒绝；dest='-' 与落盘一致 |
| 分析结论 | 注入已知速率/积压 → 验证 `cluster_stat.bottleneck` 判定：send<gen+unsent>50%→'send'；apply<recv+unapplied>50%→'recv_apply'；四速率同频→'none'；spill>0 时 deep_cause 正确；export JSON 的 analysis/capacity 与手算对拍 |
| 会话边界 | 目标中途加入 partial；stop 恰逢采样轮；混用 persist 模式 |
| 物理复制 | primary+standby 集群：send_stat 列出物理连接（kind=physical）、recv_stat 列出 recovery 行、cluster_stat 合成两端；pgbench 负载下四速率一致；standby 断连→remote_state 转 unreachable；级联场景两侧独立 |

## 10. 实施计划

| 阶段 | 交付 |
| --- | --- |
| P0 | 会话状态机 + start/stop + 三槽位采样 + live 视图族（双速率，逻辑复制） |
| P1 | 内存会话日志 + send/recv/cluster_stat + send/recv_history + 回归/TAP 重写 |
| P2 | 可选持久化（persist 路径：双写/fsync/原子头尾）、启动恢复、sessions/delete、专项测试 |
| P3 | 报告导出（json → html+SVG）+ bottleneck 列 + 分析结论测试 |
| P4 | 物理复制支持（send SQL kind 过滤、standby 采样 SQL、pg_stat_wal_receiver 视角）+ 物理专项测试 |
| P5 | 用户文档（USER_MANUAL/BEST_PRACTICES 按会话模型改写，含物理复制） |

## 附录：示例输出（目标形态）

```sql
SELECT lrstat_start('bench_am', true);
-- 压测 60s 期间（cluster_stat 是日常巡检唯一需要盯的视图）：
SELECT recv_name, round(send_instant::numeric,1) AS send_ins,
       round(send_avg::numeric,1) AS send_avg,
       round(apply_avg::numeric,1) AS apply_avg,
       round(backlog_total::numeric,1) AS total_mb
FROM pg_lrstat_cluster_stat;

 recv_name | send_ins | send_avg | apply_avg | total
----------+----------+----------+-----------+--------
 sub_a    |      7.4 |      7.5 |       7.5 | 8 kB

SELECT lrstat_stop('bench_am');
SELECT session_name, state, round(duration_secs) AS secs,
       round(total_current,0)        AS 生成MB, round(avg_send::numeric,1) AS 平均发送
FROM pg_lrstat_cluster_stat;

 session_name | state   | secs | 生成总量 | 平均发送
--------------+---------+------+----------+----------
 bench_am     | stopped |   62 | 453 MB   |     7.5
```
