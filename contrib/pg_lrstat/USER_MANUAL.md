# pg_lrstat 用户使用手册

版本：v1.0（对应扩展 1.0）｜配套文档：[DESIGN.md](DESIGN.md)（设计）、[BEST_PRACTICES.md](BEST_PRACTICES.md)（性能分析实践）、[TEST_REPORT.md](TEST_REPORT.md)（测试验证）

---

## 1. 这是什么

pg_lrstat 是一个逻辑复制 LSN 与速率统计扩展：一个后台采样 worker 周期性快照发布端与订阅端的系统视图，写入共享内存环形历史；SQL 视图在查询时做窗口差分，直接给出**速率（MB/s）、积压（bytes）、追平预估（秒）**，无需外部采样器。

部署形态（同一扩展，装哪端就有哪端的数据）：

```
发布端安装  →  pg_lrstat_pub_sample / pub_rate / pipeline / pub_history
订阅端安装  →  pg_lrstat_sub_sample / sub_rate / sub_history
                pg_lrstat_overall（凭订阅 conninfo 轮询发布端，两端合成，
                发布端无需安装任何东西）
两端都装    →  发布端精确视图 + 订阅端实时视图，互为补充
```

链路与检查点（手册通用的编号，来自设计文档 §2.2）：

```
WAL生成 C0 ─► 发送 C2 ─► 网络 ─► 订阅端接收 C3' ─► 应用 C5' ─► 反馈 ─► 确认 C6
                    └─► 对端已收 C3(反馈) ─► 对端已应用 C5(反馈)      保水 C7
```

## 2. 安装

### 2.1 构建

```sh
# 方式一：随 PG 源码树（本仓库）
./configure ... && make -j8
make -C contrib/pg_lrstat install

# 方式二：meson
meson setup bm && ninja -C bm contrib/pg_lrstat/pg_lrstat.dylib
ninja -C bm install        # 或指定 target
```

### 2.2 启用

```ini
# postgresql.conf（发布端、订阅端各自加）
shared_preload_libraries = 'pg_lrstat'
```

重启后：

```sql
CREATE EXTENSION pg_lrstat;

-- 第一步永远是看它：
SELECT * FROM pg_lrstat_info;
```

`pg_lrstat_info.loaded` 为 `t` 即就绪；为 `f` 说明没走 preload（所有数据视图为空），先检查 `shared_preload_libraries` 与重启。

### 2.3 权限

视图授予 `pg_monitor`（其余角色默认无权限）；`pg_lrstat_reset()` 与 `pg_lrstat_inject_*` 仅超级用户。远端轮询复用订阅自身的连接串，不引入新凭证面，`subconninfo` 绝不出现在任何视图或日志中。

## 3. 配置（GUC）

| GUC | 默认 | 作用域 | 说明 |
| --- | --- | --- | --- |
| `pg_lrstat.sample_interval` | `30s` | SIGHUP | 采样周期（下限 1s）；远端轮询同节拍 |
| `pg_lrstat.rate_window` | `2min` | SIGHUP | 速率差分窗口，需 ≥ 2×采样间隔 |
| `pg_lrstat.max_targets` | `32` | 重启 | 目标槽位数（槽×端 + 订阅 worker 数），见 `pg_lrstat_info.dropped_samples` |
| `pg_lrstat.raw_history_samples` | `1800` | 重启 | 每目标环长（30s 采样≈15 小时） |
| `pg_lrstat.stale_target_ttl` | `10min` | SIGHUP | 目标多久不更新才允许被新目标复用 |
| `pg_lrstat.eta_min_rate` | `1kB/s` | SIGHUP | ETA 有效性下限（低于此速率 ETA 置 NULL） |
| `pg_lrstat.remote_poll` | `on` | SIGHUP | 订阅端是否轮询发布端（off 则 overall 只有本地列） |
| `pg_lrstat.remote_connect_timeout` | `5s` | SIGHUP | 远端连接超时 |
| `pg_lrstat.remote_poll_budget` | `500ms` | SIGHUP | 单轮远端轮询总预算（超时置 stale，不拖慢本地采样） |
| `pg_lrstat.database` | `postgres` | 重启 | 采样 worker 连接的库（任意库皆可：底层目录/统计是集群级） |
| `pg_lrstat.allow_inject` | `off` | SUSET | 测试注入口开关（配合 `inject_name_prefix`） |
| `pg_lrstat.inject_name_prefix` | `''` | SUSET | 注入目标名前缀约束（建议测试环境设 `pg_lrstat_test_`） |

改 SIGHUP 项后 `SELECT pg_reload_conf();` 即生效（下一采样轮起）。

## 4. 快速开始

**发布端：这个槽健康吗？**

```sql
SELECT slot_name, state, wal_status,
       round(gen_rate::numeric,1)  AS gen_mb,
       round(send_rate::numeric,1) AS send_mb,
       pg_size_pretty(backlog_unsent) AS unsent,
       pg_size_pretty(retained_wal)  AS retained,
       eta_unsent, send_stalled
FROM pg_lrstat_pub_sample s JOIN pg_lrstat_pub_rate r USING (slot_name);
```

**订阅端：两端合成一张图（发布端零安装）**

```sql
SELECT sub_name, remote_state,
       round(gen_rate::numeric,1)  AS gen_mb,
       round(send_rate::numeric,1) AS send_mb,
       round(apply_rate::numeric,1) AS apply_mb,
       pg_size_pretty(backlog_unsent)    AS unsent,
       pg_size_pretty(backlog_inflight)  AS inflight,
       pg_size_pretty(backlog_unapplied) AS unapplied,
       pg_size_pretty(backlog_total)     AS total,
       round(eta_total::numeric)         AS eta_s
FROM pg_lrstat_overall;
```

**画趋势曲线（喂给监控）**

```sql
SELECT ts, current_lsn, sent_lsn, peer_applied_lsn
FROM pg_lrstat_pub_history('mysub', now() - interval '1 hour');
```

## 5. 视图详解（完整字段参考）

单位约定：**速率 MB/s（1MB=1048576B）**、**积压/字节类列 bytes**（可直接 `pg_size_pretty`）、**eta_* 秒**。本节覆盖全部 8 个视图的每一个字段；数据库内对关键列另有 `COMMENT ON COLUMN`（`\d+ 视图名` 可查），完整语义以本节为准。

通用约定（各视图反复出现，先记住这三条）：

- **时间三件套**：`rate_time`（本行速率的计算时刻）、`window_start_time`/`window_end_time`（差分实际使用的样本区间边界）、`window_secs`（实际窗口秒数）——速率读数可追溯；窗口数据不足半程时速率列为 NULL 而非 0。
- **NULL 哨兵**：`pg_lsn` 列值为 0 时显示 NULL（无数据）；pid 为 0 显示 NULL。
- **链路图**（overall/pub 视图通用编号）：

```
发布端                                订阅端
pub_current ──► sent ──► [网络] ──► received ──► applied ──► (反馈) ──► confirmed_flush
     └──restart(保水)
  |←unsent→|←──inflight──→|←─unapplied─→|
  |←──────────────── backlog_total ────────→|
```

### 5.1 `pg_lrstat_info` — 诊断（先看这个，13 列）

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `loaded` | bool | 扩展是否经 `shared_preload_libraries` 加载。`f` 时其余视图全空——排障第一步 |
| `layout_version` | int4 | 共享内存布局版本；升级后不匹配会拒绝附加（防御旧段被误读） |
| `ntargets` | int4 | 目标槽数（容量，等于 `max_targets`） |
| `ring_len` | int8 | 每目标环长（`raw_history_samples` 生效值） |
| `sample_interval_ms` | int8 | 采样周期生效值 |
| `rate_window_ms` | int8 | 速率窗口生效值 |
| `max_targets` | int8 | 同 ntargets（配置原值） |
| `remote_poll` | bool | 订阅端是否启用发布端轮询 |
| `last_round_ts` | timestamptz | 采样器最近一次完成轮的时刻；长期不更新=worker 异常 |
| `last_round_ok` | bool | 最近一轮是否成功 |
| `last_round_error` | text | 最近失败轮的错误摘要（成功时 NULL） |
| `nrounds` | int8 | 启动以来完成的轮数（worker 存活的证据） |
| `dropped_samples` | int8 | 因目标槽满而被丢弃的样本数；非 0 需调大 `max_targets` 并重启 |

### 5.2 `pg_lrstat_pub_sample` — 发布端采样表（一逻辑槽一行，32 列）

最新样本的瞬时事实。前 11 列标识与状态，中间 11 列位点与计数，后 10 列派生积压与延迟。

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `slot_name` | text | 逻辑槽名（与订阅端 `subslotname` 对位 join 的键） |
| `sample_time` | timestamptz | 本行样本的采样时刻 |
| `database` | text | 槽所属发布库 |
| `plugin` | text | 输出插件（pgoutput/wal2json…） |
| `temporary` | bool | true = 初始同步的临时槽（tablesync 期间出现） |
| `active` | bool | 槽是否被 walsender 持有；订阅断开时变 f |
| `sender_pid` | int4 | 服务该槽的 walsender pid（无则为 NULL） |
| `application_name` | text | 订阅侧连接标识（默认=订阅名） |
| `client_addr` | text | 订阅端地址（权限不足时 NULL） |
| `state` | text | walsender 状态（streaming/catchup…，无 walsender 为 NULL） |
| `sync_state` | text | 同步复制角色（async/sync…）；非 async 且 state 显示等待=被同步复制拖住而非吞吐问题 |
| `current_lsn` | pg_lsn | C0：`pg_current_wal_lsn()`——WAL 生成顶端 |
| `sent_lsn` | pg_lsn | C2：walsender 已发送位（无 walsender 为 NULL） |
| `peer_recv_lsn` | pg_lsn | C3：反馈报文报告的订阅端接收位（滞后一个反馈周期） |
| `peer_flush_lsn` | pg_lsn | C4：反馈报告的已提交落盘位 |
| `peer_applied_lsn` | pg_lsn | C5：反馈报告的已应用位 |
| `confirmed_flush_lsn` | pg_lsn | C6：槽确认水位（WAL 回收决策依据） |
| `restart_lsn` | pg_lsn | C7：槽保水位 |
| `wal_status` | text | 保留状态（reserved/unreserved/lost…），lost=已丢数据需重建订阅 |
| `safe_wal_size` | int8 | 距离 `max_slot_wal_keep_size` 的余量（bytes） |
| `spill_bytes` | int8 | 解码溢写累计字节（ReorderBuffer 落盘） |
| `stream_bytes` | int8 | 解码流式累计字节（未提交大事务） |
| `total_bytes` | int8 | 解码输出累计字节；与 WAL 增量之比≈解码膨胀率 |
| `backlog_unsent` | int8 | bytes：current−sent，未发送积压（含未解码） |
| `backlog_inflight` | int8 | bytes：sent−peer_recv，在途（含反馈滞后造成的假在途，见 overall 的 `feedback_lag_bytes`） |
| `backlog_peer_unapplied` | int8 | bytes：peer_recv−peer_applied，对端已收未应用（反馈口径） |
| `backlog_total` | int8 | bytes：current−peer_applied，端到端总积压（反馈口径） |
| `retained_wal` | int8 | bytes：current−restart，该槽扣住的 WAL 总量（磁盘风险） |
| `write_lag` / `flush_lag` / `replay_lag` | interval | 内核按反馈时间戳计算的延迟：对端确认接收/落盘/应用的时间滞后；唯一可跨机使用的时间延迟 |
| `reply_time` | timestamptz | 最近一次收到反馈的时刻 |

### 5.3 `pg_lrstat_pub_rate` — 发布端速率表（14 列）

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `slot_name` | text | 槽名 |
| `rate_time` + 窗口三件套 | — | 见通用约定 |
| `gen_rate` | float8 | WAL 生成速率（发布库全体写入） |
| `send_rate` | float8 | walsender 发送速率（LSN 等效；与带宽对比需乘解码膨胀率） |
| `apply_rate` | float8 | 对端应用速率（**经反馈**，含反馈周期延迟；订阅端本地的准确值看 `pg_lrstat_sub_rate`） |
| `confirm_rate` | float8 | 槽确认水位推进速率 |
| `spill_rate` / `stream_rate` | float8 | 解码溢写/流式速率，>0 = 大事务解码压力 |
| `eta_unsent` | float8 | 秒：unsent/send_rate；速率不足 `eta_min_rate` 或积压为 0 时 NULL |
| `eta_total` | float8 | 秒：排空总积压的预估（分段求和） |
| `send_stalled` | bool | 发布端停滞：unsent>0 且 send_rate≈0（告警源） |

### 5.4 `pg_lrstat_sub_sample` / `pg_lrstat_sub_rate` — 订阅端（18 + 12 列）

采样表一行/worker（apply 或 `table synchronization`；`leader_pid` 区分并行 apply），速率表同粒度。

**`pg_lrstat_sub_sample`：**

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `sub_name` | text | 订阅名 |
| `sample_time` | timestamptz | 采样时刻 |
| `subslotname` | text | 对应发布端槽名 |
| `worker_type` | text | `apply` / `table synchronization`（v18 拼写） |
| `worker_pid` / `leader_pid` | int4 | worker 进程 / 其 leader（并行 apply 时非空） |
| `relid` | oid | tablesync 的目标表（apply 行为 NULL） |
| `received_lsn` | pg_lsn | C3′：实收位（`received_lsn`，本地实时、无反馈延迟） |
| `latest_end_lsn` | pg_lsn | 最后一个 keepalive/数据消息的结束位（received 为空时的回退源） |
| `applied_lsn` | pg_lsn | C5′：已应用位（origin `remote_lsn`；v18 注意见 overall 的说明） |
| `origin_local_lsn` | pg_lsn | origin 的本地 WAL 位点（最后一次带 origin 提交的本地位置） |
| `local_wal_lsn` | pg_lsn | 订阅库 `pg_current_wal_lsn()`（含非复制写入，`local_wal_rate` 的数据源） |
| `last_msg_send_time` / `last_msg_receipt_time` | timestamptz | 最近消息的发布端发送时刻/本地接收时刻（诊断网络延迟的原始素材） |
| `latest_end_time` | timestamptz | 最近 keepalive 结束时刻 |
| `backlog_apply` | int8 | bytes：received−applied，已收未应用（本地口径，overall 同名列无反馈失真） |
| `apply_error_count` / `sync_error_count` | int8 | 应用/同步错误累计（-1 显示 NULL=未知） |

**`pg_lrstat_sub_rate`：**

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `sub_name` / `worker_type` / `relid` | — | 目标标识（同上） |
| `rate_time` + 窗口三件套 | — | 见通用约定 |
| `recv_rate` | float8 | 接收速率（本地实收位差分） |
| `apply_rate` | float8 | 应用速率（origin 位点差分，**本地口径、无反馈延迟**） |
| `local_wal_rate` | float8 | 订阅端本地 WAL 生成速率（应用回放产生 + 本库其他写入噪声；专用订阅库才可当"应用产生的 WAL"读） |
| `eta_apply` | float8 | 秒：backlog_apply/apply_rate |
| `apply_stalled` | bool | 订阅端停滞：unapplied>0 且 apply_rate≈0——先查 `pg_stat_activity` 中 worker 的 `wait_event`（多为锁冲突） |

### 5.5 `pg_lrstat_pipeline` — 发布端阶段分解（一槽四行，8 列）

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `slot_name` | text | 槽名 |
| `stage` | text | `unsent`（current→sent）/ `inflight`（sent→对端已收）/ `peer_unapplied`（对端已收→已应用）/ `retained`（槽保水） |
| `rate_time` + 窗口三件套 | — | 见通用约定（窗口取自 send_rate 的差分区间） |
| `backlog_bytes` | int8 | 本段积压（bytes）；四段堆叠图直接用 |
| `rate` | float8 | 本段排空速率（MB/s；retained 段恒 NULL） |
| `lag` | interval | 本段反馈时间滞后：inflight=write_lag、peer_unapplied=replay−write（负则 NULL）、unsent/retained=NULL |

### 5.6 `pg_lrstat_overall` — 订阅端整体视图（一订阅一行，34 列）

两端合成：发布端列来自 **RPUB 环**（采样器凭 conninfo 轮询发布端，时间戳为本地时钟，**两端时钟偏差不进入任何计算**），订阅端列来自 **SUB 环**（本地实时）。这是日常监控的主视图。

**标识与轮询健康：**

| 列 | 类型 | 含义与用途 |
| --- | --- | --- |
| `sub_name` | text | 订阅名 |
| `subslotname` | text | 发布端槽名（与发布端视图 join 的钥匙） |
| `remote_state` | text | `ok` 正常；`unreachable` 连不上发布端（指数退避重连）；`stale` 可达但槽不存在（新建订阅未建槽）或单轮预算耗尽；`n/a` 无远端数据（`remote_poll=off` 或首轮未完成） |
| `last_remote_poll_time` | timestamptz | 最近一次成功轮询时刻（本地时钟）；与 `rate_time` 差值判断远端数据新鲜度 |

**时间轴：** `sample_time`（SUB 数据采样时刻）、`rate_time` + 窗口三件套——见通用约定。

**六个位点（0 显示 NULL）：**

| 列 | 检查点 | 来源 | 含义 |
| --- | --- | --- | --- |
| `pub_current_lsn` | C0 | RPUB | 发布端 WAL 顶端 |
| `sent_lsn` | C2 | RPUB | 已发送位 |
| `received_lsn` | C3′ | SUB | 本地实收位（无反馈延迟） |
| `applied_lsn` | C5′ | SUB | 已应用位（origin）；注意：积压计算内部用 `max(origin 位, 反馈 flush 位)` 取更靠前者（v18 反馈位更可靠），本列展示 origin 原值 |
| `confirmed_flush_lsn` | C6 | RPUB | 槽确认水位 |
| `restart_lsn` | C7 | RPUB | 槽保水位 |

**六个速率（MB/s）：** `gen_rate`/`send_rate`/`spill_rate`/`stream_rate` 来自 RPUB 环差分；`recv_rate`/`apply_rate` 来自 SUB 环差分。判读：`send<gen 持续`=发布端发不动；`apply<recv 持续`=订阅端短板；四速率同频且 backlog→0=健康。

**六个积压（bytes）：**

| 列 | 公式 | 含义与用途 |
| --- | --- | --- |
| `backlog_unsent` | current−sent | 发布端未发送 |
| `backlog_inflight` | sent−received（本地实收） | **真网络在途**——不含发布端视图那种反馈假在途 |
| `backlog_unapplied` | received−applied | 已收未应用 |
| `backlog_total` | current−applied | 端到端总积压（三段之和） |
| `retained_wal` | current−restart | 槽扣住的 WAL（磁盘风险，配合发布端 wal_status） |
| `feedback_lag_bytes` | received−反馈接收位 | 反馈滞后量的直接观测：若 `inflight≈feedback_lag_bytes`，"在途"是反馈延迟假象而非网络问题 |

**反馈时间延迟：** `write_lag`/`flush_lag`/`replay_lag`（interval）——同 §5.2，唯一可跨机的时间延迟来源，NULL=尚无反馈。

**ETA 与停滞：**

| 列 | 含义与用途 |
| --- | --- |
| `eta_unsent`（秒） | unsent/send_rate；速率不足或积压为 0 时 NULL |
| `eta_total`（秒） | unsent/send + (inflight+unapplied)/apply；**持续写入场景用净追平速率重估**（`min(send,apply)−gen` 为负则追不平） |
| `send_stalled` | 发布端停滞标志（告警源） |
| `apply_stalled` | 订阅端停滞标志——先查 apply worker 的 wait_event |

**NULL 规则速记**：`remote_state='n/a'` 时发布端侧 5 位点、4 速率、5 积压、3 lag、`eta_unsent`、`send_stalled` 全 NULL，仅本地列有效；采样中断/窗口不足时对应速率 NULL 而非 0。日常巡检盯 8 列：`remote_state`、四速率、`backlog_total`、`eta_total`、两个 stalled。

### 5.7 `pg_lrstat_pub_history / sub_history` — 原始时间序列（8 + 7 列）

`(name, since)` 两个可选过滤参数；按时间升序返回环内原始样本（监控画图用）。

| pub_history 列 | 含义 | | sub_history 列 | 含义 |
| --- | --- | --- | --- | --- |
| `name` | 槽名 | | `name` | 订阅名 |
| `ts` | 样本时刻 | | `ts` | 样本时刻 |
| `current_lsn` | C0 | | `received_lsn` | C3′ |
| `sent_lsn` | C2 | | `latest_end_lsn` | 消息结束位 |
| `peer_recv_lsn` | C3 | | `applied_lsn` | C5′ |
| `peer_applied_lsn` | C5 | | `origin_local_lsn` | origin 本地位 |
| `confirmed_flush_lsn` | C6 | | `local_wal_lsn` | 本地 WAL 顶端 |
| `restart_lsn` | C7 | | | |

## 6. 速率是怎么算出来的

### 6.1 采样模型：先存样本，查询时才算

后台 worker 每 `sample_interval`（默认 30s）把一组位点快照（`pg_current_wal_lsn()`、`sent_lsn`、`received_lsn`、`applied_lsn`…）写入共享内存的**环形历史**（默认 1800 点 ≈ 15 小时）。视图里的速率不是存的，而是**查询时对历史做窗口差分**现算的——所以改 `rate_window` 后立刻重查同一段历史就能得到不同平滑度的读数，历史本身不受影响。

### 6.2 差分算法与数值示例

对任一速率列（如 `send_rate`）：

```
取环内最新样本 s1；
从 s1 向前找仍落在 rate_window 内的最旧样本 s0；
rate = (sent_lsn(s1) − sent_lsn(s0)) / (ts(s1) − ts(s0))   → 字节/秒
显示值 = rate ÷ 1048576                                     → MB/s
```

**手算示例**（与回归测试同款数字）：两次采样间隔 110s，`sent_lsn` 从 `0/0` 推进到 `0/800000`（8 MiB）：

```
send_rate = 8 × 1048576 B ÷ 110 s ≈ 76.1 kB/s ≈ 0.0727 MB/s
```

积压列则**不做差分**，是最新样本上两点位相减（负值截为 0）：`backlog_unsent = current_lsn − sent_lsn`，即查询那一刻的字节量。

**有效性规则**（防止误导性读数，违反时速率列为 NULL 而非 0）：

- 新鲜度：最新样本距 now 超过 3×`sample_interval`（采样中断/worker 刚重启）→ NULL；
- 跨度：实际样本跨度不足 `rate_window` 的一半（刚启动、历史不足）→ NULL。

每行的 `window_start_time / window_end_time / window_secs` 就是本次差分实际用的区间，读数可复核。

### 6.3 ETA 与停滞判定

```
eta_unsent = backlog_unsent / send_rate                     （秒）
eta_total  = backlog_unsent / send_rate
           + (backlog_inflight + backlog_unapplied) / apply_rate
```

速率低于 `eta_min_rate`（默认 1kB/s）或积压为 0 时 ETA 为 NULL（避免"速率≈0 → ETA=∞"噪声）。`*_stalled` = 积压>0 且对应速率有效但 < 1 B/s。**持续写入场景下 `eta_total` 会偏乐观**，应改用净追平速率：`min(send,apply) − gen` 为正才追得平（见 §7.3）。

### 6.4 单位语义：LSN 等效字节 vs 网络字节

速率的分母是 **WAL 位点差**（LSN 等效字节/秒），不是 socket 字节数。要与网卡带宽对比，先算**解码膨胀率**：

```
膨胀率 = Δtotal_bytes / Δcurrent_lsn        （协议输出字节 ÷ 输入 WAL 字节）
线上字节速率 ≈ gen_rate × 膨胀率            ← 拿这个跟带宽比
```

（`total_bytes` 在 `pg_lrstat_pub_sample`；历史视图暂不含它，用两次快照相除即可。）

### 6.5 时钟纪律

SUB 环与 RPUB 环（轮询发布端）的时间戳**全部取订阅端本地时钟**——发布端速率实为"本地时钟轴上的轮询差分"，两机 NTP 偏差不进入任何计算。跨机的时间滞后只用反馈 lag 列（`write/flush/replay_lag`，内核按反馈时间戳计算），绝不拿两端 `now()` 相减。

### 6.6 调参影响

- 窗口调大（如 10min）：读数平滑、适合容量结论，反应迟钝；
- 窗口调小：灵敏但毛刺多；约束 `rate_window ≥ 2 × sample_interval`（否则凑不够样本，速率为 NULL）；
- 排障临时组合：`sample_interval=1s, rate_window=4s`（`pg_reload_conf()` 生效），用完调回默认。

## 7. 性能分析实战（五分钟流程）

完整方法论文档是 [BEST_PRACTICES.md](BEST_PRACTICES.md)（巡检 SQL 清单、四类场景剧本、容量规划），本节是可直接上手的核心流程。

### 7.1 三步定位：总量 → 分段 → 速率关系

```
① backlog_total 超阈值且持续增长？
├─ 否 → 稳态，做容量评估即可（余量 = gen − min(send, apply)）
└─ 是 → ② 积压集中在哪一段？（overall 三列占比）
    ├─ unsent 占大头     → 发布端问题     → 7.2-A
    ├─ inflight 占大头   → 网络或反馈     → 7.2-B
    └─ unapplied 占大头  → 订阅端应用     → 7.2-C
```

| 速率关系（须在窗口内持续成立） | 结论 |
| --- | --- |
| `send_rate < gen_rate` 且 unsent 增长 | 发布端发送/解码吞吐不足 |
| `send_rate ≈ gen_rate` 且 total 稳定非零 | 发送已尽力，短板在网络或对端 |
| `recv_rate ≈ send_rate` 且 inflight 小 | 网络不是瓶颈 |
| `apply_rate < recv_rate` 且 unapplied 增长 | 订阅端应用是短板 |
| 四速率接近且 total → 0 | 全链路健康 |

### 7.2 分段下钻

**A. 发布端（unsent）**：看 `spill_rate/stream_rate > 0`？是则大事务解码被磁盘拖累（拆事务或调 `logical_decoding_work_mem`）；`sync_state` 非 async 且 state 显示等待 = 被同步复制拖住，不是吞吐问题；多槽 unsent 同步增长而 gen 正常 = 发布端 CPU/IO 共享瓶颈。

**B. 网络（inflight）**：**先排除假在途**——若 `backlog_inflight ≈ feedback_lag_bytes`，说明"在途"主要是反馈周期（默认 10s）的观测滞后，不是网络问题；真在途持续增长才按 §6.4 换算线上速率与带宽对照。

**C. 订阅端（unapplied）**：`apply_stalled=t` → 查 `pg_stat_activity` 中该 worker 的 `wait_event`（锁等待最常见）；无停滞但 `apply < recv` → 单 apply worker 串行回放是常态瓶颈（大事务/触发器/索引/外键），核对 `streaming` 与并行 worker（`leader_pid` 非空）是否生效；存在 `worker_type='table synchronization'` 行时 unapplied 增长属初始同步正常现象。

### 7.3 两个必会换算

```sql
-- 净追平速率：为负 = 永远追不平，先扩容短板侧再谈 ETA
SELECT sub_name,
       round(least(send_rate, apply_rate)::numeric,1) AS drain_mb,
       round(gen_rate::numeric,1)                      AS gen_mb,
       round((least(send_rate, apply_rate) - gen_rate)::numeric,1) AS net_mb
FROM pg_lrstat_overall;

-- 槽拖爆磁盘倒计时：retained 增速 ≈ 无推进时的 WAL 生成速率
WITH d AS (
  SELECT current_lsn - lag(current_lsn) OVER (ORDER BY ts) AS bytes,
         extract(epoch FROM (ts - lag(ts) OVER (ORDER BY ts))) AS secs
  FROM pg_lrstat_pub_history('mysub', now() - interval '10 minutes')
)
SELECT round(avg(bytes / nullif(secs,0))::numeric) AS retained_bps FROM d;
```

### 7.4 故障模式速查

| 模式 | 签名 | 第一动作 |
| --- | --- | --- |
| 大事务回放 | spill/stream 飙升；unsent 先增后骤降；apply_rate 短暂归零 | 等待+溯源大事务 |
| 订阅端锁冲突 | apply_stalled 且 wait_event 为锁；unapplied 阶梯增长 | `pg_blocking_pids()` 找阻塞源 |
| 订阅暂停/断开 | 槽 active=f 或 send_stalled；retained 线性增长；wal_status 沿 reserved→unreserved→lost 恶化 | 恢复订阅；wal_status=unreserved 即亮红灯 |
| 反馈滞后误报 | inflight ≈ feedback_lag_bytes 且 applied_lsn 持续推进 | 无需处理 |
| 写入洪峰 | gen_rate 突增，积压沿 unsent→inflight→unapplied 依次传导 | 各段到达时间差=各段延迟 |

## 8. 运维操作

```sql
-- 清空所有目标与历史（superuser）
SELECT pg_lrstat_reset();

-- 目标不够用（dropped_samples 增长 / 新槽不出现）：
--   postgresql.conf: pg_lrstat.max_targets = 64 → 重启

-- 测试注入（superuser + allow_inject=on，生产勿开）
SET pg_lrstat.allow_inject = on;
SELECT pg_lrstat_inject_pub('pg_lrstat_test_t', now(), '0/1000000', '0/800000',
                            '0/400000', '0/400000', '0/200000');
```

## 9. 故障排查

| 现象 | 看 | 结论/处理 |
| --- | --- | --- |
| 所有视图空 | `pg_lrstat_info.loaded` | `f` → 未 preload；`t` → 看 `last_round_ok` |
| `loaded=t` 但无数据 | `last_round_ok/last_round_error` | 采样轮失败（如权限/视图变更），看 error 文本；失败日志仅状态翻转时记一条 |
| `dropped_samples > 0` | `pg_lrstat_info` | 目标槽满，调大 `max_targets` 重启 |
| `remote_state='unreachable'` | `pg_lrstat_overall` | 发布端连不上：连接串/防火墙/发布端 down（指数退避重连） |
| `remote_state='stale'` | 同上 | 槽不存在（订阅新建未同步）或单轮预算耗尽（订阅数多时调大 `remote_poll_budget`） |
| `remote_state='n/a'` | 同上 | 无远端数据（`remote_poll=off` 或首轮未完成） |
| 速率列全 NULL | `window_secs` | 采样中断/刚启动：最新样本距 now > 3×interval，或窗口跨度不足半程 |

## 10. 已知限制（读前必知）

1. **单库采样**：worker 只连一个库；因槽/订阅/walsender 统计均为集群级，通常无感。跨库特殊场景见设计文档附录 D。
2. **反馈延迟**：发布端视图的对端位置（C3~C5）滞后一个 `wal_receiver_status_interval`（默认 10s）；订阅端视图无此延迟——这是两端都装的价值。
3. `backlog_inflight`（发布端口径）含反馈滞后造成的"假在途"；`pg_lrstat_overall` 用本地接收位计算，无此失真。
4. `local_wal_rate` 含订阅库自身其他写入的噪声。
5. 大事务回放期间 `apply_rate` 可能短暂为 0（单事务内无 commit 位点推进），`apply_stalled` 辅助区分。
6. `pg_lrstat_pipeline` 的 progress 为 LSN 比值近似，不是精确百分比。
