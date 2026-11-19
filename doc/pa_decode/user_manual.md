# 发布端并行逻辑解码 用户手册

| 项目 | 内容 |
| --- | --- |
| 特性名称 | 发布端并行逻辑解码（publisher-side parallel logical decoding） |
| 目标版本 | PostgreSQL 17（本仓库分支 `pa_decode`） |
| 生效端 | **发布端（publisher）** |
| 订阅端 | 本特性**不要求**订阅端做任何改动；要用上订阅端的并行 apply，需订阅端使用 `streaming = 'parallel'` |
| 关联文档 | `design.md`（概要设计说明书）、`need.md`（需求说明书） |
| 文档状态 | 与 `pa_decode` 分支的实现同步 |

## 目录

- [1. 概述](#1-概述)
- [2. 快速开始](#2-快速开始)
- [3. 运行前提](#3-运行前提)
- [4. 配置参数](#4-配置参数)
- [5. 监控](#5-监控)
- [6. 帧级追踪与比对](#6-帧级追踪与比对)
- [7. 调优](#7-调优)
- [8. 故障排查](#8-故障排查)
- [9. 已知限制](#9-已知限制)
- [10. 附录](#10-附录)

---

## 1. 概述

### 1.1 这个特性解决什么问题

逻辑复制链路有两端：

- **发布端**：读 WAL、维护一致性快照、重组事务、把 WAL 解析为 tuple 变更、经流复制协议发给订阅端。
- **订阅端**：收变更，在 apply worker 里（PG 16 起可以并行）回放落盘。

从 PG 16 起订阅端已经可以并行 apply，**发布端却始终是单进程串行解码**。发布端消耗的 CPU 最多，却只能用到一个核，于是端到端吞吐的上限由较慢的一端决定——订阅端的并行能力得不到兑现。

本特性把发布端的解码拆成**三个进程**，让最耗 CPU 的那部分（tuple 解析、插件回调）并行起来：

| 进程 | 每个会话的数量 | 职责 |
| --- | --- | --- |
| **walreader** | 1 | 唯一的 WAL 读取者。读 WAL、维护快照、按事务拥有者路由记录、分配提交序号（cseq）、推进复制槽水位 |
| **decode worker** | N（`max_parallel_decode_workers`） | 解码被分配给自己的事务，把结果转成 pgoutput 协议帧 |
| **walsender** | 1（复用现有的） | 持有复制槽与 TCP 连接，按提交顺序发出帧、keepalive、提交/中止 |

这三个进程通过一块**动态共享内存**（`dsm`）里的 `shm_mq` 队列通信：walreader → worker 的 WAL 记录、worker → walsender 的协议帧、以及一个 walreader → walsender 的控制通道。

### 1.2 与串行路径的关系

本特性是**发布端路径的替换**，不是新协议、不是新插件：

- **语义等价**：同一批 WAL，串行路径与并行路径产出的 wire 字节**逐消息一致**。允许的差异只有两类，且都是有意为之（见 `design.md` 4.1）：帧的**顺序**（并行路径可以提前发流段），以及帧头 **LSN 的钳制值**（并行路径把流段帧的 LSN 钳到"放行前沿"，防止订阅端把 `confirmed_flush` 抬过尚未放行的事务）。
- **开关关闭即完全等同现状**：`max_parallel_decode_workers = 0`（默认）时，走的是原有的进程内串行解码路径，一个字节都不变。
- **订阅端零改动**：订阅端不需要知道发布端用了哪条路径。协议字节相同，订阅端的行为也就相同。

### 1.3 什么时候该用

适合：

- 发布端 CPU 是瓶颈（发布端单核跑满，订阅端 apply worker 在等数据）；
- 负载是**大量小事务**（并行收益最明显，因为事务之间可以真正并行）；
- 订阅端已经用 `streaming = 'parallel'`（这时"尽早流式"才有意义，见 `parallel_decode_streaming_mode`）。

不适合：

- 发布端 CPU 并不紧张——并行解码会引入额外的进程间通信开销；
- 负载是**单个超大事务**——一个事务的全部变更必须由同一个 worker 按顺序解码，无法再拆（见 9. 已知限制）。

> **本机开发只做了功能与正确性验证。** 本文出现的所有吞吐数字都是**静态估算或待实测**，不能当作性能结论。实测清单见 `design.md` 15.2。

---

## 2. 快速开始

### 2.1 发布端

编辑 `postgresql.conf`：

```conf
wal_level = logical

# 每个并行解码会话会起 N+1 个后台进程（1 个 walreader + N 个 decode worker）
max_worker_processes = 16
max_replication_slots = 10
max_wal_senders = 10

# 打开并行解码路径
max_parallel_decode_workers = 4
```

```bash
pg_ctl reload
```

`max_parallel_decode_workers` 是 **sighup 级**参数，**下一次复制会话**才生效（见 4.3）。要让已经建立的订阅用上并行路径，需要断开并重连该订阅（`ALTER SUBSCRIPTION ... DISABLE/ENABLE` 或重启 apply worker）。

### 2.2 订阅端

本特性不要求订阅端做任何改动。若要用上订阅端的**并行 apply**（推荐），订阅端需要一个 `streaming = 'parallel'` 的订阅：

```sql
CREATE SUBSCRIPTION my_sub
  CONNECTION 'host=pub user=repl dbname=mydb'
  PUBLICATION my_pub
  WITH (streaming = 'parallel');
```

并行 apply worker 的数量由订阅端参数控制（PG 17 默认 2）：

```conf
max_parallel_apply_workers_per_subscription = 4
```

发布端的 `parallel_decode_streaming_mode = auto`（默认）会**自动**按订阅端请求的流式模式选择策略：订阅端要 `parallel` 就尽早流式，订阅端只要 `on` 就保持现状行为（见 4.2.2）。

### 2.3 确认并行路径已生效

在**发布端**查询：

```sql
SELECT pid, worker_index, worker_pid, status_text, in_flight_txns, decoded_txns, wakeups
FROM pg_parallel_decode_workers
ORDER BY worker_index;
```

有行返回，说明有一个会话正在走并行路径；`worker_index` 从 0 到 `max_parallel_decode_workers - 1`，`status_text` 为 `running`。

再看会话级的输出与唤醒计数：

```sql
SELECT frames, frame_bytes, socket_writes, wakeups_down, wakeups_up
FROM pg_parallel_decode_wakeups;
```

**没有行返回通常不是故障**——走串行路径的复制会话不会出现在这些视图里（见 5.4）。

---

## 3. 运行前提

| 要求 | 类别 | 说明 |
| --- | --- | --- |
| `wal_level = logical` | 发布端 | 逻辑复制的前提，缺了就没有 WAL 逻辑解码 |
| 一个**逻辑复制槽** | 发布端 | 订阅端用 `create_slot = true` 自动建，或手工 `pg_create_logical_replication_slot()` |
| `max_wal_senders` ≥ 并发会话数 | 发布端 | 每个复制会话一个 walsender |
| `max_replication_slots` ≥ 槽数 | 发布端 | 标准要求 |
| `max_worker_processes` ≥ **N+1 × 并发会话数** | 发布端 | 每个并行会话起 1 个 walreader + N 个 decode worker，都是动态后台进程，占用 `max_worker_processes` 的名额。不够时报 `could not register background worker "parallel decode worker"`，并附带 `You might need to increase "max_worker_processes"` 的提示 |
| `max_parallel_decode_workers` ≤ **64** | 发布端 | 硬上限（`MAX_PARALLEL_DECODE_WORKERS`）。超过会在会话建立时报 `max_parallel_decode_workers must be at most 64` |
| 订阅端表**事先存在** | 订阅端 | PostgreSQL **不复制 DDL**。订阅端的表必须先建好，形状由订阅端自己维护（复制的元组按列名匹配，订阅端多出来的列留空） |
| `streaming = 'parallel'` | 订阅端（可选） | 只有要并行 apply 才需要；不设时发布端仍走并行解码，但流式策略退化为 `buffered` |

### 3.1 共享内存开销

每个并行解码会话创建一块动态共享内存段，大小约为：

```
256KB × (1 + 2N) + 控制块
```

其中 `N = max_parallel_decode_workers`，256KB 是**每条队列**的大小（`PARALLEL_DECODE_QUEUE_SIZE`），每条队列上下行各一条，再加上 walreader → walsender 的控制通道。

举例：`N = 4` 时约 2.3MB；`N = 16` 时约 8.3MB。这块内存由 `dsm` 分配，生命周期与会话相同，会话结束即释放。

---

## 4. 配置参数

### 4.1 概览

全部参数都是 `sighup` 级、作用域为 `REPLICATION_SENDING`（可写在 `postgresql.conf` 里，也可由超级用户在复制会话中 `SET`）。

| 参数 | 类型 | 默认值 | 范围 | 一句话说明 |
| --- | --- | --- | --- | --- |
| `max_parallel_decode_workers` | int | `0` | 0 – 64 | 并行解码 worker 的数量；0 = 关闭并行路径 |
| `parallel_decode_streaming_mode` | enum | `auto` | `auto` / `immediate` / `buffered` | 事务流式化的激进程度 |
| `parallel_decode_stream_min_size` | int | `0` | 0 – INT_MAX | 小于这个字节数的事务**不**流式化 |
| `parallel_decode_stream_segment_size` | int | `1MB` | 0 – INT_MAX | 单个流段的字节上限 |
| `parallel_decode_work_mem` | int | `-1` | -1 – INT_MAX | 每个 decode worker 的解码内存上限；-1 = 用 `logical_decoding_work_mem` |
| `parallel_decode_batch_size` | int | `64KB` | 0 – INT_MAX | 队列消息的合并批次大小；0 = 不合并 |
| `parallel_decode_barrier_timeout` | int | `60` | 0 – INT_MAX | 停滞检测的超时（秒）；0 = 关闭检测 |
| `parallel_decode_pending_max_bytes` | int | `64MB` | 0 – INT_MAX | 待放行帧的字节上限，超过时发布端告警；0 = 关闭告警 |
| `parallel_decode_trace_file` | string | `""` | — | 帧级追踪文件；空 = 关闭 |
| `parallel_decode_fault_inject` | enum | `0` | 0 / 1 / 2 | **调试用**的故障注入开关 |

### 4.2 逐项说明

#### 4.2.1 `max_parallel_decode_workers`

并行解码 worker 的数量。`0`（默认）表示完全关闭并行路径，走原有的进程内串行解码。

**取值建议**：一般取**发布端物理核数的一半左右**起步，再按实测调整。理由是每个 worker 都会起一个进程，除了解码本身还要承担进程调度、共享内存通信的开销；worker 数超过核数时，上下文切换的开销会吃掉并行收益。

**上界**：64。超过会在会话建立时报错。

**生效时机**：**下一次复制会话**。worker 池是会话级的（随 walsender 一起建立和销毁），会话中途不能增删——正在解码的事务不能迁移到别的 worker。

**注意**：`max_worker_processes` 必须留够 N+1 × 并发会话数的名额。

#### 4.2.2 `parallel_decode_streaming_mode`

控制事务被**流式化**的激进程度。有三个取值：

| 取值 | 行为 |
| --- | --- |
| `auto`（默认） | 按订阅端请求的流式模式自动选择：订阅端要 `parallel` → `immediate`；订阅端只要 `on` → `buffered`；订阅端不要流式 → 关闭流式 |
| `immediate` | 一律尽早流式。事务的变更在解码完成之前就分片下发给订阅端 |
| `buffered` | 一律等整个事务解码完再下发（保持现有行为） |

**为什么 `auto` 要分档**：

- 订阅端用 `streaming = 'parallel'` 时，它的并行 apply worker 需要**提前拿到变更**才有事可做。没有流段，并行 apply 根本启动不了。
- 订阅端用 `streaming = 'on'` 时，它只有一个 apply worker，没有并行 apply worker。此时提前流式只会把订阅端的每个事务推到 spool 慢路径（`TRANS_LEADER_SERIALIZE`），是**性能回归**。

**流式的含义**：事务被切成若干"流段"，每个流段是 `STREAM START` + 若干变更 + `STREAM STOP`。同一时刻 wire 上只能有一个打开的流段（订阅端的协议约束），流段之间不能交织。

**生效时机**：**下一次复制会话**。模式在会话建立时按订阅端的 `streaming` 选项解析并冻结，会话中途改值会与已经切出的流段不一致。

#### 4.2.3 `parallel_decode_stream_min_size`

**小于这个字节数的事务不流式化**，走整事务下发（整个事务的字节作为一个块发出去）。

**默认必须是 `0`**（不设门槛）。门槛若大于典型事务大小，所有事务都不流式化 → 订阅端收不到 `STREAM START` → **并行 apply 根本不启动**。这是一个很容易踩的坑：调大这个参数看起来像"减少开销"，实际上会让订阅端的并行 apply 整个失效。

只有当"整事务下发"和"流式下发"之间的差异对某些特定负载有实际意义时才动它。

**生效时机**：**下一次复制会话**（值在会话建立时拷进共享内存）。

#### 4.2.4 `parallel_decode_stream_segment_size`

**单个流段的字节上限**，默认 1MB。

它决定了"一个流段最长能独占 wire 多久"。由于 wire 上同时只能有一个打开的流段，一个卡住的流段会把其它 worker 的产出一起堵住——这是并行路径的固有代价，只能靠限制段的粒度来限制单块时长。

调小的代价是：流段更碎，`STREAM START` / `STREAM STOP` 的协议消息更多（每事务多 2 帧）。

**生效时机**：**下一次复制会话**。

#### 4.2.5 `parallel_decode_work_mem`

**每个 decode worker** 的解码内存上限。`-1`（默认）表示沿用 `logical_decoding_work_mem` 的值。

总内存上界 ≈ **worker 数 × 本值**。这是并行走起来之后最容易踩的资源限制：一个事务如果大到单个 worker 的内存装不下，会走 reorder buffer 的落盘路径。

**生效时机**：**下一次复制会话**。`ReorderBufferCheckMemoryLimit()` 只读本进程的 GUC 值，worker 进程必须重启才能看到新值。

#### 4.2.6 `parallel_decode_batch_size`

**队列消息的合并批次大小**，默认 64KB。这是**跨进程唤醒的速率上限**。

walreader 把 WAL 记录交给 worker、worker 把协议帧交给 walsender，都走共享内存队列。队列只有在两种情况下才把数据交给对端：发送方显式要求刷新，或者累计写入量达到了环的 1/4。合并批次就是显式控制"多久刷新一次"：

- 发送方记着"上次刷新以来写了多少字节"，写消息时累加；攒满一批就把这条消息标记为"要刷新"，于是这一批的数据连同这条消息一起交给对端，**对端被唤醒一次**。
- 批次没满但发送方要挂起等待时，会发一条**长度为 0 的刷新消息**把数据交出去。没有它，对端会对着一条已经躺在环形缓冲里、却还没交出来的数据空等。

**取值权衡**：

| 方向 | 效果 |
| --- | --- |
| 调大 | 唤醒次数少（跨进程信号是 `kill()` 系统调用 + 上下文切换，不便宜），但复制延迟抬高 |
| 调小 | 延迟低，但唤醒次数多 |

**0 = 关闭合并**：每条记录、每帧都单独唤醒对端。这是接线之前的行为，也是排查问题时用的对照组。

**有效值被钳制在共享队列的 1/4 以内**（即 64KB）。理由：队列在累计写入量达到环的 1/4 时自己会刷新，批再大也不会多攒住数据；而批大于环则会让发送方等待接收方腾空间，而接收方恰恰看不到还没发布的数据，构成死锁。

**生效时机**：`sighup` 后**立即**对正在进行的会话生效（值是每次发消息时读的）。

> 注：GUC 自带的说明文字写的是"下次复制会话生效"，那是保守说法；实际实现每次发送都重新读这个值。

#### 4.2.7 `parallel_decode_barrier_timeout`

**停滞检测的超时**，默认 60 秒。

并行路径有一个提交屏障：一个事务的帧要等到它"轮到"才能下发，而"轮到"取决于排在它前面的事务都解码完了。因此发布端要等解码它的那一个 worker。

如果某个 worker **还有记录没解码**却长时间没有任何进展，会话就会一直等下去。停滞检测是这种情况的兜底：

判定卡死需要**同时**满足三个条件：

1. 屏障生效——有待放行的事务；
2. 某个 worker 的解码位置**落后于** walreader 的读取位置（即它确实还有活没干完）；
3. 该 worker 在这段时间内没有送来任何消息。

满足时发布端报错 `the parallel decode session stalled` 并结束会话，而不是静默地卡住。

**只看会话真正在等的那个 worker**：条件 2 与 3 检查的是**待放行队列队头事务的拥有者**——会话就是在等它，其余 worker 哪怕落后也没有关系。这样判定的原因是：其余 worker 可能只是在等新 WAL（发布端空闲时它们的解码位置天然落后于 walreader 的读取位置，落后量约为一个 marker 的距离），把这种情况当成卡死会误杀会话。

**空闲的发布端不会误触发**：没有待放行的事务时条件 1 不成立。

**0 = 关闭检测**（不推荐，除非确实知道自己在做什么）。

**生效时机**：`sighup` 后**立即**生效（检测时读的是当前值）。

#### 4.2.8 `parallel_decode_pending_max_bytes`

**待放行帧的字节上限**，默认 64MB（`67108864`）。

一个事务的帧要等到它"轮到"才能下发。在它前面排队的、以及正在解码但还没轮到的事务，其帧都堆在发布端 walsender 的**待放行区**里。一个解码很慢的事务会把后面所有事务的帧都堵住，待放行区的字节因此可能涨得很大。

超过这个上限时发布端发一条 `WARNING`：

```
WARNING:  the parallel decoding session holds 73400320 bytes of frames that wait for their turn
```

**这是告警，不是错误**：会话继续运行，数据不受影响。它告诉运维的是"发布端的内存被这个会话吃掉了"，处置办法是缩小事务的规模、减少并发会话数，或者把这个上限调大。

告警**每个"episode"只发一次**：待放行区缩回上限以下之后，下一次超限会再告警一次。

**0 = 关闭告警**（不推荐，除非确实知道自己在做什么）。

**生效时机**：`sighup` 后**立即**生效（每次加帧时读当前值）。

#### 4.2.9 `parallel_decode_trace_file`

帧级追踪文件，默认为空（关闭）。

开启后，发布端把它发出的**每一个复制帧**写进这个文件，格式是"4 字节网络序长度 + 帧载荷"。文件开头有一条会话标记（首字节为 0，后 8 字节是会话 id），用来区分同一个文件里的不同会话。

这是**调试设施**，配合 `t/pd_frame_tool.py` 使用（见第 6 章）。生产环境保持为空。

**生效时机**：**下一次复制会话**（文件在会话建立时打开）。

**注意**：文件用裸 fd 打开，整个会话期间保持打开。文件按 `O_APPEND` 打开，多次运行会追加而不是覆盖。

#### 4.2.10 `parallel_decode_fault_inject`

**调试设施**，默认 `0`（不注入）。

| 值 | 效果 |
| --- | --- |
| `0` | 不注入 |
| `1` | decode worker 启动后立刻失败 |
| `2` | walreader 启动后立刻失败 |

用来验证"参与者出错时怎么上报"和"会话怎么恢复"。生产环境必须保持 `0`。

**生效时机**：**下一次复制会话**（在 worker / walreader 启动时读）。

### 4.3 变更生效时机汇总

| 参数 | 生效时机 | 原因 |
| --- | --- | --- |
| `max_parallel_decode_workers` | 下次会话 | worker 池是会话级的；会话中途增删会改变共享内存的队列条数，且正在解码的事务不能迁移 |
| `parallel_decode_streaming_mode` | 下次会话 | 会话建立时按订阅端的模式解析并冻结；中途改值会与已切出的流段不一致 |
| `parallel_decode_stream_min_size` | 下次会话 | 参与流段切分判定 |
| `parallel_decode_stream_segment_size` | 下次会话 | 参与流段切分 |
| `parallel_decode_work_mem` | 下次会话 | worker 必须重启才能看到新值 |
| `parallel_decode_batch_size` | **sighup 后立即** | 每次发送消息时读当前值 |
| `parallel_decode_barrier_timeout` | **sighup 后立即** | 每次检测时读当前值 |
| `parallel_decode_pending_max_bytes` | **sighup 后立即** | 每次加帧时读当前值 |
| `parallel_decode_trace_file` | 下次会话 | 文件在会话建立时打开 |
| `parallel_decode_fault_inject` | 下次会话 | 在参与者启动时读 |

虽然都是 `sighup` 级参数，但**除后三项外都只在下一次复制会话生效**。运维时不要给出"改完立刻生效"的预期。

---

## 5. 监控

### 5.1 视图总览

发布端提供六个视图，全部是**会话级**的统计：

| 视图 | 回答什么问题 |
| --- | --- |
| `pg_parallel_decode_workers` | 每个 decode worker 在干什么 |
| `pg_parallel_decode_commit_lag` | 提交顺序上积压了多少 |
| `pg_parallel_decode_txn_latency` | 单个事务的解码耗时 |
| `pg_parallel_decode_pending_commit_age` | 最长的那个待放行事务已经等了多久 |
| `pg_parallel_decode_walsender_frames` | 会话往网络上写了多少 |
| `pg_parallel_decode_wakeups` | 进程之间互相唤醒了多少次 |

### 5.2 列说明

#### `pg_parallel_decode_workers`

| 列 | 含义 |
| --- | --- |
| `pid` | 会话的 walsender 进程 id |
| `usename` / `application_name` / `client_addr` | 连接信息（来自 `pg_stat_activity`） |
| `worker_index` | worker 在会话内的序号，0 起 |
| `worker_pid` | worker 的操作系统进程 id |
| `status` / `status_text` | 状态：`unused` / `starting` / `running` / `failed` / `exited` |
| `in_flight_txns` | 该 worker 手上的在途事务数（**只有 walreader 写这一个计数**） |
| `decoded_txns` | 该 worker 已解码的事务数 |
| `in_flight_bytes` | 该 worker 在途事务占用的字节数 |
| `queue_depth` | 输出队列的深度 |
| `wakeups` | 该 worker 唤醒 walsender 的次数（= 刷新次数，见 4.2.6） |

#### `pg_parallel_decode_commit_lag`

| 列 | 含义 |
| --- | --- |
| `cseq_allocated` | 已分配的提交序号总数 |
| `cseq_released` | 已放行的提交序号总数 |
| `commit_lag` | 两者之差，即**顺序积压** |

`commit_lag` 持续增长说明有事务卡在放行队列里——先查 `pg_parallel_decode_pending_commit_age`，再看对应 worker 的 `decoded_txns` 是否在涨。

#### `pg_parallel_decode_txn_latency`

| 列 | 含义 |
| --- | --- |
| `txn_count` | 已解码的事务数 |
| `txn_latency_us_sum` | 各事务解码耗时之和（微秒） |
| `txn_latency_us_max` | 最慢的一个事务（微秒） |
| `txn_latency_us_avg` | 平均值（视图里算的） |

#### `pg_parallel_decode_pending_commit_age`

| 列 | 含义 |
| --- | --- |
| `commit_lag` | 同上 |
| `pending_commit_age_us` | **最长的待放行事务已经等了多久**（微秒） |

只看积压深度不足以定位停顿：一个深度为 1 的积压可能等了很久，一个深度为 100 的积压可能刚形成。这一列给的是"年龄"。

#### `pg_parallel_decode_walsender_frames`

| 列 | 含义 |
| --- | --- |
| `frames` | 写出的帧数 |
| `frame_bytes` | 写出的字节数 |
| `socket_writes` | socket 写次数（并行路径按批刷出，因此远小于帧数） |

#### `pg_parallel_decode_wakeups`

| 列 | 含义 |
| --- | --- |
| `wakeups_down` | walreader → worker 的唤醒次数（下行） |
| `wakeups_up` | worker → walsender 的唤醒次数（上行） |
| `wakeups_total` | 两者之和 |

**计数口径是"刷新次数"**，不是"消息条数"。刷新才真的唤醒了对端。承载进度的消息（位置心跳、事务结束事件）不批量，每次都刷新，因此各计一次。

`pg_parallel_decode_wakeups.wakeups_up` 等于 `pg_parallel_decode_workers.wakeups` 的和。

### 5.3 采样方法

视图给出的是**会话启动以来的累计值**，不是速率。速率要自己做差分：

```sql
SELECT
    now()                                        AS ts,
    frames,
    frame_bytes,
    socket_writes,
    wakeups_down,
    wakeups_up
FROM pg_parallel_decode_walsender_frames
CROSS JOIN pg_parallel_decode_wakeups;
```

隔一段时间采两次，相减再除以时间差，就是速率。

### 5.4 权限与可见性

- 需要 `pg_read_all_stats` 角色（或超级用户）才能看到除 `pid` 以外的列；没有该权限的用户只能看到 `pid`，其余列全为空。这与 `pg_stat_get_wal_senders()` 的口径一致。
- **走串行路径的复制会话不会出现在这六个视图里**。视图只在并行路径的会话建立之后才有数据。这不是故障。
- 统计值由 walsender 侧**节流到 100 毫秒一次**地从会话的共享内存拷到主共享内存，因此有最多约 100ms 的延迟。

---

## 6. 帧级追踪与比对

`t/pd_frame_tool.py` 是一个帧级 dump / diff 工具，用来回答"并行路径产出的字节和串行路径一样吗"。

### 6.1 开启追踪

发布端：

```conf
parallel_decode_trace_file = '/tmp/pd.trace'
```

```bash
pg_ctl reload
```

### 6.2 dump

```bash
python3 doc/pa_decode/t/pd_frame_tool.py dump /tmp/pd.trace --messages
```

`--messages` 会把每一帧里的 pgoutput 消息也列出来。帧类型标记：`S` = STREAM START、`E` = STREAM STOP、`c` = COMMIT、`A` = STREAM ABORT、`p` = 其它（普通变更）。

### 6.3 diff

```bash
python3 doc/pa_decode/t/pd_frame_tool.py diff /tmp/serial.trace /tmp/parallel.trace --messages
```

工具比较的是 **pgoutput 消息**而不是裸帧，因为并行路径可能把消息分到不同的帧里。

**允许两类差异**，工具会分开报告：

1. **帧的顺序**——并行路径可能把流段发在更早的事务的提交消息之前；
2. **帧头 LSN**——并行路径把流段帧的 LSN 钳到放行前沿。

**其它任何差异都是缺陷。**

### 6.4 典型用法

对比同一负载在两条路径上的产出：

```conf
# 串行路径
max_parallel_decode_workers = 0
```

```conf
# 并行路径
max_parallel_decode_workers = 4
```

各跑一遍同样的负载，把 trace 文件交给 `diff`。

---

## 7. 调优

### 7.1 先量再调

调优之前先把这两件事做掉：

1. 确认瓶颈确实在发布端（发布端 CPU 打满？walsender 进程的 CPU 占用如何？）；
2. 确认订阅端的并行 apply 真的起来了（`pg_stat_subscription` / `pg_stat_replication` 里的 apply worker 数量）。

如果订阅端的 apply worker 数不够，发布端并行解码出来的数据会堆在订阅端的 spool 里——**发布端更快反而可能让端到端更慢**（订阅端落 spool 的开销）。

### 7.2 常见的负载形态与对策

| 负载 | 现象 | 对策 |
| --- | --- | --- |
| 大量小事务 | 唤醒次数多，进程间信号成为开销 | 调大 `parallel_decode_batch_size`；确认 worker 数与核数匹配 |
| 少量超大事务 | 并行收益有限（一个事务不能拆） | 调大 `parallel_decode_work_mem` 避免落盘；流式化让订阅端早点开始 apply |
| 延迟敏感 | 批合并让数据晚到 | 调小 `parallel_decode_batch_size`；`parallel_decode_streaming_mode = immediate` |
| 订阅端 apply 慢 | 发布端积压 | 调大订阅端的 `max_parallel_apply_workers_per_subscription` |

### 7.3 观测建议

一次典型的排查顺序：

```sql
-- 1. 会话活着吗？worker 都在跑吗？
SELECT worker_index, status_text, decoded_txns, wakeups FROM pg_parallel_decode_workers;

-- 2. 有顺序积压吗？
SELECT cseq_allocated, cseq_released, commit_lag FROM pg_parallel_decode_commit_lag;

-- 3. 最长的待放行事务等了多久？
SELECT pending_commit_age_us FROM pg_parallel_decode_pending_commit_age;

-- 4. 唤醒是不是太多？调大 batch_size 之后应该下降。
SELECT wakeups_down, wakeups_up FROM pg_parallel_decode_wakeups;
```

**对照实验**：把 `parallel_decode_batch_size` 设成 `0`（关闭合并）再测一遍同样的负载，对比 `wakeups_up`——这能直接量化"合并批次"省下了多少次跨进程唤醒。

### 7.4 性能数据

**本机没有做性能测试。** 本文与 `design.md` 中出现的所有吞吐、速率、降幅倍数都是**静态估算或待实测**，必须在性能机上按 `design.md` 15.2 的清单实测后回填。

已经做过的**功能层面**验证（非性能测试）：

| 项目 | 结果 |
| --- | --- |
| `doc/pa_decode/t/` 的 13 个冒烟脚本 | 全部通过 |
| 上游 `publication` / `subscription` 回归测试 | 全部通过 |
| `src/test/subscription/t/` 的 34 个 TAP 用例（459 个断言） | 全部通过 |
| 同一负载下关闭/开启合并批次的唤醒次数对比 | 上行 1338 → 417，下行 969 → 363（帧数同阶） |

---

## 8. 故障排查

### 8.1 错误信息对照表

| 错误信息 | 含义 | 处理 |
| --- | --- | --- |
| `could not register background worker "parallel decode worker"`<br>`You might need to increase "max_worker_processes"` | 后台进程名额不够 | 调大 `max_worker_processes`，至少 N+1 × 并发会话数 |
| `max_parallel_decode_workers must be at most 64` | 超过硬上限 | 调小 `max_parallel_decode_workers` |
| `the parallel decode worker failed: <原因>` | worker **主动报错**并把原因带上来 | 看 `<原因>`：多半是解码错误或资源问题 |
| `the parallel decode worker died unexpectedly` | worker **进程没了**且没留下原因 | 看 postmaster 日志里 worker 崩溃前的上下文 |
| `the parallel decode walreader failed: <原因>` | walreader 报错 | 同上 |
| `the parallel decode walreader died unexpectedly` | walreader 进程没了 | 看日志 |
| `the parallel decode session stalled` | 停滞检测触发：会话在等的那个 worker 长时间没有进展且还有活没干完 | 先用 `pg_parallel_decode_workers` 看是哪个 worker；再用 `parallel_decode_trace_file` 抓 trace；必要时调大 `parallel_decode_barrier_timeout` |
| `the parallel decoding session holds <N> bytes of frames that wait for their turn`（WARNING） | 待放行区的字节超过 `parallel_decode_pending_max_bytes`；**不是错误**，会话继续 | 缩小事务规模、减少并发会话数，或调大 `parallel_decode_pending_max_bytes` |
| `the parallel decode walreader did not report readiness` | walreader 没能就绪 | 看日志里 walreader 的启动错误 |
| `parallel decode frame message has an inconsistent length` | 队列里的消息长度对不上 | 这是内部一致性错误，应当上报为缺陷 |

### 8.2 会话失败后的数据安全

**会话失败不会丢数据。** 失败时数据仍在 WAL 里；订阅端重连后从复制槽的 `confirmed_flush` 续传。并行路径保证 `confirmed_flush` 不会越过尚未放行的事务，因此重连后这些事务会被重新解码并送达。

修复问题后，订阅端会自动重连并把数据补齐——不需要人工干预。

### 8.3 常见问题

**Q：开了 `max_parallel_decode_workers`，但六个视图都是空的。**

A：先确认走的是并行路径。视图只显示走并行路径的会话。检查：

- 会话是不是在参数生效**之后**建立的（`max_parallel_decode_workers` 只在下次会话生效）；
- 复制槽是不是逻辑槽；
- 订阅端连的是不是这个发布端。

**Q：订阅端没有并行 apply 生效。**

A：并行 apply 需要订阅端 `streaming = 'parallel'`。另外注意 `parallel_decode_stream_min_size` **默认必须是 0**——门槛设大了会让所有事务都不流式化，订阅端收不到 `STREAM START`，并行 apply 根本不会启动。

**Q：改了 `parallel_decode_batch_size` 没反应。**

A：这个参数 `sighup` 后**立即**生效，不需要重连。如果没反应，确认改的是**发布端**的配置，并且 `pg_ctl reload` 执行过。

**Q：改了 `max_parallel_decode_workers` 或 `parallel_decode_streaming_mode` 没反应。**

A：这两个（以及其它大部分参数）**只在下一次复制会话生效**。要让订阅端用上新值，需要断开重连该订阅。

**Q：内存占用比预期高。**

A：总解码内存上界 ≈ worker 数 × `parallel_decode_work_mem`（或 `logical_decoding_work_mem`）。另外每个会话还有约 `256KB × (1 + 2N)` 的共享内存。

---

## 9. 已知限制

| 限制 | 说明 |
| --- | --- |
| **一个事务不能跨 worker 拆分** | 事务在 walreader 侧一次性绑定到一个 worker，其后的所有变更都由该 worker 按顺序解码。单个超大事务无法并行；事务亲和性也意味着**顺序写入的小事务会全部落在同一个 worker 上** |
| **DDL 不被复制** | PostgreSQL 不复制 DDL。订阅端的表必须事先存在 |
| **订阅端的表清单在 `CREATE SUBSCRIPTION` 时固定** | 之后发布端新建的表不在清单里，订阅端会**静默丢弃**它的变更。官方手段是 `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` |
| **会话中途不能改 worker 数** | `max_parallel_decode_workers` 只在下次会话生效 |
| **流段不能交织** | wire 上同时只能有一个打开的流段。一个卡住的流段会阻塞其它 worker 的产出——这是并行模型的固有代价，只能靠 `parallel_decode_stream_segment_size` 限制单块时长 |
| **性能数据待实测** | 本特性只在本机做了功能与正确性验证。所有吞吐数字必须在性能机上实测后回填（`design.md` 15.2） |

---

## 10. 附录

### 10.1 冒烟测试脚本

`doc/pa_decode/t/` 下的脚本都是可独立运行的功能验证脚本。环境变量：`PDTEST_ROOT`（数据目录根）、`PORT_PUB` / `PORT_SUB`（端口）、`PGBIN`（二进制目录）、`WORKERS`（worker 数）。

| 脚本 | 验证什么 |
| --- | --- |
| `setup_env.sh` | 测试脚手架：建发布端/订阅端两个集群 |
| `repl_smoke.sh` | 基本的逻辑复制 |
| `frame_diff_smoke.sh` | 并行路径与串行路径**逐帧**比对 |
| `concurrent_diff_smoke.sh` | 并发会话下的逐帧比对 |
| `live_diff_smoke.sh` | 订阅正在流式时写入负载的逐帧比对 |
| `ddl_diff_smoke.sh` | 混合 DDL/DML 负载的逐帧比对 |
| `rel_dedup_smoke.sh` | 关系描述只发一次 |
| `slot_watermark_smoke.sh` | 槽水位（`restart_lsn` / `catalog_xmin`）正常推进 |
| `streaming_smoke.sh` | 尽早流式：分片提前到达、不交错、重启不丢数据 |
| `keepalive_smoke.sh` | 空闲期的 keepalive |
| `failure_smoke.sh` | 故障注入：报错带原因、会话及时终止、恢复后补齐数据 |
| `subxact_abort_smoke.sh` | 子事务 abort：STREAM ABORT 发出、行集与串行路径一致 |
| `monitoring_smoke.sh` | 六个监控视图；并对比开关合并批次前后的唤醒次数 |
| `worker_death_repro.sh` | 订阅在流式中途被删除时的关停次序 |
| `pd_frame_tool.py` | 帧级 dump / diff 工具 |

### 10.2 排查用的 SQL 速查

```sql
-- 会话与 worker
SELECT * FROM pg_parallel_decode_workers ORDER BY worker_index;

-- 顺序积压与最老待放行事务
SELECT cseq_allocated, cseq_released, commit_lag FROM pg_parallel_decode_commit_lag;
SELECT pending_commit_age_us FROM pg_parallel_decode_pending_commit_age;

-- 输出与唤醒
SELECT frames, frame_bytes, socket_writes FROM pg_parallel_decode_walsender_frames;
SELECT wakeups_down, wakeups_up, wakeups_total FROM pg_parallel_decode_wakeups;

-- 单事务解码耗时
SELECT txn_count, txn_latency_us_avg, txn_latency_us_max FROM pg_parallel_decode_txn_latency;
```

### 10.3 相关文档

| 文档 | 内容 |
| --- | --- |
| `need.md` | 需求说明书：背景、目标、功能需求、约束、验收标准、风险 |
| `design.md` | 概要设计说明书：三进程模型、共享内存布局、消息协议、顺序保证、性能分析、实现状态 |
| `user_manual.md` | 本文：使用与运维 |
| `t/` | 功能验证脚本与帧级比对工具 |

### 10.4 提交历史

本特性在 `pa_decode` 分支上开发。实现状态与设计决策的逐条记录见 `design.md` 的 16.1 与附录 B。
