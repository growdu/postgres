# 流段绕过排序：cseq=0 的帧缺一道门

> 行号对应 2026-11-19 03:40 的工作区（`postgresql-ee` 正在改 `parallel_decode.c` 等四个文件，本文只做分析，不动源码）。

## 0. 结论

分析成立。当前并行路径把 **cseq=0 的帧（流段、子事务 abort 触发的 STREAM ABORT）一到就直接放进 `ready`**，既不看 `parallel_decode_pending` 里还有没有等待放行的事务，也不看放行前沿 `frontier`。于是它们可以越过"在它们之前就已经结束"的事务的结束事件上 wire。

串行路径没有这个问题：串行路径的输出顺序就是 WAL 记录的处理顺序，结束记录在位置 p 被输出，流段在位置 q 被输出，`p < q` 时 COMMIT 一定在流段之前。

用户提的修复思路（**加门 + held 表 + 随 `release_txns()` 按到达序放行**）是对的，下面逐条核对，并补三个实现上必须加的点。

---

## 1. 代码证据

### 1.1 cseq=0 是怎么来的

`pd_walreader_record_ends_xact()`（`parallel_walreader.c:673`）只在**顶层事务的结束记录**上返回 true：

```c
	xid = pd_walreader_record_xid(reader);
	if (!XLogIdIsValid(xid) || xid != info->toplevel)
		return false;
```

所以 `cseq` 只分配给顶层事务的 commit / abort / prepare。worker 侧 `pd_current_cseq = pd_wal_header.cseq`（`parallel_decode_worker.c:898`），于是两类帧的 cseq 恒为 0：

| 帧 | 产生点 | cseq |
|---|---|---|
| 流段（STREAM START/变更/STREAM STOP） | `PDQueueSendSegment()`（worker.c:314），由插件的 stream_start/stream_stop 回调驱动 | 0 |
| 子事务 abort 的 `STREAM ABORT` | `ReorderBufferAbort()` → `rb->stream_abort()`（reorderbuffer.c:2948）→ 插件写帧 → `PDQueueSendFrame()`（worker.c:294） | 0 |

（顶层的 ABORT 帧不受影响：顶层 abort 记录有 cseq，走正常的 pending 路径。）

### 1.2 发送侧：cseq=0 直接进 ready

`parallel_decode.c`：

```c
1225:				if (fh.cseq == 0)
1226:				{
					/* ... There is no commit sequence number to order it by,
					   so it is sent as soon as it arrives. ... */
1244:					parallel_decode_ready = lappend(parallel_decode_ready, frame);
				}
```

```c
1298:		case PD_ENT_SEGMENT:
			...
1351:					parallel_decode_ready = lappend(parallel_decode_ready, frame);
```

两处都**没有**查 `parallel_decode_pending`、没有查 `parallel_decode_frontier`。而 cseq != 0 的帧走的是另一条路：

```c
	parallel_decode_add_pending_frame(fh.cseq, ...)   →  挂在 parallel_decode_pending 上
	...
676: parallel_decode_release_txns(void)
	{
		for (;;)
		{
			... 只有 cseq == parallel_decode_next_cseq 且已 ack 的事务 ...
716:				parallel_decode_ready = lappend(parallel_decode_ready, frame);
```

`parallel_decode_take_ready_frame()`（:1099）从 `ready` 的**头部**取帧，`ready` 是 FIFO ⇒ **先进 ready 的先上 wire**。

⇒ cseq=0 的帧一到就发；队列里有没有等待放行的事务、frontier 到了哪里，一眼都没看。

---

## 2. 被破坏的不变量

**应当成立的不变量（I-ORDER）**：

> 位置为 q 的 cseq=0 帧，**不得越过**在 q 之前就已结束的事务的结束事件（COMMIT / ABORT / PREPARE）。

**串行路径为什么天然满足**：串行路径由 walsender 自己按 WAL 顺序读记录、逐条输出。事务 T 的结束记录在位置 p 被处理 → T 的 BEGIN..COMMIT 在此时输出；流段在位置 q 被处理 → 此时输出。`p < q` ⇒ COMMIT 在流段之前。

**并行路径当前为什么违反**：帧的产出时刻由**哪个 worker 先解码完**决定，与 WAL 顺序无关：

* T1 在位置 p 结束（cseq = k），但它的 worker 还没 ack → T1 的帧还挂在 `parallel_decode_pending` 上，没进 `ready`；
* T2 的流段在位置 q（q > p）被 worker 2 产出 → `:1351` 直接进 `ready`；
* `ready` 里只有流段 → **流段先上 wire**，T1 的 COMMIT 后上 wire。

⇒ 顺序倒置。

---

## 3. 后果：订阅端的 deadlock

订阅端 `streaming = 'parallel'` 时，主 apply worker（LA）与并行 apply worker（PA）的协作是：

* LA 收到 STREAM START 就 `pa_allocate_worker()`，把变更喂给 PA（`worker.c:1467` 起）；
* PA 收到 `'w'` 消息就 `apply_dispatch()` **立刻加锁**，事务悬着，直到 commit 命令到达才提交（`applyparallelworker.c:766-806`）。

即：**PA 加锁的时刻 = 变更到达订阅端的时刻，与提交顺序无关。**

发布端把 T2 的流段（q2）放在 T1 的 COMMIT（p1 < q2）之前，订阅端就会：

```
PA-2: 先锁 T2 的行
PA-1: 再锁 T1 的行
      两个事务的行锁顺序相反 → 死锁
```

而这两个事务在**发布端**是被行锁串行化的（发布端不会死锁，死锁的那一方会在发布端被检测掉）。所以形态就是"发布端正常、订阅端报 deadlock detected"。

补充（不要误判）：上游的 parallel apply 本身就有死锁风险，`applyparallelworker.c:60-130` 明确写了它是用 lmgr 的锁把死锁**检测**出来而不是**避免**（"We have a risk of deadlock due to concurrently applying the transactions in parallel mode that were independent on the publisher side but became dependent on the subscriber side"）。本条缺陷是**在发布端新增了一个本不该有的并发窗口**：本来被发布端的提交序串行化的一对事务，因为流段插队而在订阅端变成了并发 apply。

---

## 4. 修复方案

### 4.1 用户提的思路（确认成立）

```
门（gate）：  frontier == NIL  ||  frontier_head->end_lsn > 帧.end_lsn
              过门   → 直接进 ready
              不过门 → 进 held 表（FIFO）

held 表：    随 parallel_decode_release_txns() 推进 frontier 时，按到达序放行
```

逐条核对：

| 要素 | 核对结果 |
|---|---|
| `frontier` 取 `parallel_decode_frontier`（:185）链表头 | ✔ 链表按 cseq 顺序追加（:828）、从头部弹出（:752-759），头即**最老的未放行事务** |
| `end_lsn` 的口径 | ✔ 是结束记录的**起始 LSN**（`parallel_walreader.c:2170` 传 `reader->record->lsn`），与设计文档 §7.4 的 F 定义一致 |
| 比较方向 `head->end_lsn > 帧.end_lsn` | ✔ 方向正确且偏保守：只有"所有未放行的已结束事务都结束在帧之后"时才放行 |
| "只等已终结事务保活" | ✔ frontier 里只有**已结束**事务；还在进行中的事务不挡门 → 不破坏设计 §8.1 "尽早流式"的初衷（段照样早发，只是排在更早的结束事件之后） |
| 放行点只能在 `parallel_decode_release_txns()`（:676） | ✔ 那里是 frontier 唯一前进的地方（:752-759 弹表头）；每次 ack（`PD_ENT_EVENT` → :1198）都会调用它 |
| held 表按到达序放行 | ✔ 段与 STREAM ABORT 帧必须**共用同一条 FIFO**，否则两者之间的相对顺序会乱 |

### 4.2 必须补的三点

**(a) 判门之前要再 drain 一次控制通道 —— 这是必须的，否则门有漏洞。**

walreader 的顺序是"**先发 CSEQ，再把记录发给 worker**"：

```c
2170:		pd_walreader_send_cseq(cseq, reader->record->lsn,
					reader->record->next_lsn, worker);
			...
			pd_walreader_send_worker(target, PD_ENT_WAL, buf->data, buf->len);
```

所以当段到达 walsender 时，那条 CSEQ **一定已经在控制队列里**（worker 拿到记录才可能产出段）。但 walsender 的 `parallel_decode_drain_control()` 是在循环开头调的（`parallel_decode.c:1560` 附近），**可能发生在 CSEQ 入队之前** ⇒ frontier 是陈旧的（空的，或少了最早那一条）⇒ 门被误开 ⇒ 倒置照旧。

⇒ **在处理 cseq=0 帧的入口处，先 `parallel_decode_drain_control()` 一次再判门。** 这样判门时用到的 frontier 一定是"该帧之前结束的所有事务"的完整集合。

**(b) 两个入口必须走同一道门。**

`PD_ENT_FRAME` 的 `fh.cseq == 0` 分支（:1225，子事务 abort 的 STREAM ABORT 走这里）和 `PD_ENT_SEGMENT`（:1298）都要过门，否则 STREAM ABORT 帧仍然会越过更早的结束事件。

**(c) held 表的内存与背压。**

被挡住的段留在 walsender 的内存里（`ready` + `held` 都是 List）。段默认 1MB（`parallel_decode_stream_segment_size`），积压 N 段就是 N MB 的 walsender 内存。建议给 held 表设上限并告警，**不要静默丢弃**（丢一段 = 订阅端数据损坏）。

### 4.3 判门用的位置

段的 `end_lsn` 是 worker 当时处理到的记录终点（worker.c:314 传 `pd_end_lsn`），段的变更本身的位置 ≤ 它 ⇒ 用 `end_lsn` 判门比用"变更的位置"更严，安全。注意 frontier 那边的 `end_lsn` 是"结束记录**起点**"，两边口径不同，但方向是安全的（不会出现"越过更早结束的事务"）。

---

## 5. 设计文档要改的一句

`design.md:1485`：

> 与 barrier 的关系：尽早流式与 commit barrier 正交——**分片提前发，提交仍按 cseq 放行**（分片属于事务内部，不受 cseq 约束）。二者不冲突。

这句是当前实现的来源，但它把"不受 **cseq** 约束"写成了"不受**结束事件顺序**约束"。分段自己确实没有 cseq（还没结束），但它必须受**更早结束的其它事务**的结束事件约束，否则订阅端会把两个在发布端被串行化的事务并发 apply。

建议改成：

> 与 barrier 的关系：尽早流式与 commit barrier 正交——**分片提前发，提交仍按 cseq 放行**。分段自己还没有 cseq（事务尚未结束），所以它不受自己的 cseq 约束；但它必须排在**在它之前就已结束**的事务的结束事件之后（判据：放行前沿 F 已越过分段的位置），否则订阅端会把两个在发布端被行锁串行化的事务并发 apply，导致订阅端死锁。

---

## 6. 本文的证据边界

* 结论来自代码路径的静态核对（发送路径、放行路径、frontier 的口径、walreader 的 CSEQ 发送顺序）。
* **尚未做线上的复现**：还没有在 wire 上抓到"流段早于更早事务的 COMMIT"的实例。判据：发布端开 `parallel_decode_trace_file`，或用 `pg_recvlogical` 观察消息顺序，看 `STREAM START` 是否出现在更早事务的 `COMMIT` 之前。
* 订阅端的 deadlock 传导链（§3）依赖上游 parallel apply 的行为，引用的是 `applyparallelworker.c:60-130` 的设计说明与 `766-806` 的接收循环。

---

## 7. 与 commit barrier（提交同步机制）的性能对比

本节回答："当前的发送顺序（流段绕过 barrier）会不会带来性能问题"。

### 7.1 两条通道的差别

| 通道 | 帧 | 排队方式 | 上 wire 的时机 |
|---|---|---|---|
| 提交路径 | 结束事件（commit/abort/prepare）、非流式事务的整事务字节 | `parallel_decode_pending`，按 cseq 排序 | `release_txns()` 轮到它时 |
| 数据路径（当前：旁路） | cseq=0 的流段、子事务 abort 的 STREAM ABORT | 直接 `lappend` 到 `ready` | 立刻（`take_ready_frame()` 取 FIFO 头） |

即：**当前顺序 = 提交路径有 barrier，数据路径没有 barrier**。这正是 design §4.3 方案 1 的字面表述（"流式分片不受 barrier 约束，随时可发"），所以实现符合设计的字面；欠规范的是 §4.3 与 §8.1:1485（见 §5）。

### 7.2 吞吐 vs 时延：当前顺序改善的是时延，不是吞吐

* **发布端发出的协议字节速率**（AC-1 的口径）由解码能力（walreader + N 个 worker 的 CPU）决定，与发送顺序无关。barrier 不产生字节、只产生顺序；旁路也不产生字节。⇒ **顺序不改变发布端的吞吐上限**。
* 顺序只改变**字节到达订阅端的时刻**。
  * 当前顺序：分片产出即上 wire，延迟≈0。
  * 加门后：分片推迟到"它之前结束的事务都被放行"那一刻。

⇒ §4.3 对比表里"对尽早流式的影响：无影响 / 队头阻塞（数据路径）：无"这两行，讲的是**数据路径没有队头阻塞**，不是"吞吐更高"。这两件事不要混。

### 7.3 加门的延迟代价有上界，且与 barrier 同源

分片被挡住的时间 = **它之前那些事务的 barrier 等待时间**。这些事务的结束事件本来就要等这么久，所以：

* 加门**不新增一类等待**，只是让分片与"它前面那批事务的结束事件"共享同一批等待；
* 最坏情况是"分片与那批结束事件在同一批上 wire"；
* 上界由 barrier 的队头阻塞决定，而 barrier 的队头阻塞由 `barrier_timeout` 盯着（`parallel_decode.c:1495` 起），不会无界。

⇒ 加门的延迟代价是**有界、可观测、可调**的（调 worker 负载均衡就是调它）。具体分位数 **待实测**。

### 7.4 当前顺序把发布端的 barrier 延迟转移成了订阅端的锁持有时间

这是容易被忽略的一条，而且是当前顺序**最实在**的性能代价：

* 订阅端的 PA 收到变更就 `apply_dispatch()` 加锁，事务悬着直到 commit 到达（`applyparallelworker.c:766-806`）。
* 流段的 **commit**（STREAM COMMIT）有 cseq，**必须过 barrier**；流段**本身**不过 barrier。
* ⇒ 在当前顺序下，流段的变更在订阅端**先拿锁**，却要等到 barrier 轮到该事务才提交。**锁持有时间被拉长了 ≈ barrier 队头阻塞时延**。
* 加门后，流段与它前面的结束事件一起到，PA 拿锁到 commit 的间隔≈0 ⇒ 锁持有时间回到正常。

死锁需要一个"等待环"，而等待环需要**两个事务的锁持有区间重叠**。锁持有时间被拉长 = 死锁窗口变大。所以当前顺序有**两个**独立的机制在制造死锁：

1. **序反转**：分片越过更早事务的结束事件 ⇒ 两个在发布端被行锁串行化的事务在订阅端并发 apply；
2. **锁持有时间拉长**：分片早到但 commit 受 barrier 排队 ⇒ 订阅端长期持锁。

### 7.5 死锁的代价量级远大于"分片晚一点到"

上游的 parallel apply 是**检测**死锁而不是避免死锁（`applyparallelworker.c:62-78`：*"We have a risk of deadlock due to concurrently applying the transactions in parallel mode that were independent on the publisher side but became dependent on the subscriber side"*）。检测出来之后：

* apply worker 报错退出 → 订阅端重启 apply worker → **从 `confirmed_flush` 重放**。

⇒ 一次死锁的代价 = 订阅端把上次确认点之后的全部变更重做一遍。这个量级比"分片晚到一个 barrier 延迟"大得多。

⇒ **性能上的结论是：加门划算。** 用"有界的时延增量"换"不回放"。

补一条反向的诚实说明：当前顺序在**没有冲突负载**时确实更快（订阅端能提前 prepare，PA 的 CPU 工作与前序事务重叠更多）。所以"当前顺序更快"这个说法在无冲突负载上成立。但正确性不能靠负载碰巧不冲突来保证（need.md AC-2/AC-3 要求并行路径与串行路径 apply 结果一致）。

### 7.6 加门引入的两笔新成本

**(a) walsender 内存：held 区积压。**
被挡的分片留在 walsender 内存里。段大小上限 `parallel_decode_stream_segment_size`（默认 1MB），积压 N 段 = N MB。参照量级：`parallel_decode_pending_max_bytes` 默认 64MB，且它**不统计**流段（流段从不进 pending 区）。⇒ held 区需要单独的上限 + 告警，**不能静默丢弃**（丢一段 = 订阅端数据损坏）。

**(b) 订阅端 apply 的"提前量"被压缩。**
当前顺序下分片早到，PA 提前拿锁、提前 prepare，与前序事务的 CPU 重叠更多；加门后这段重叠被压缩 ⇒ 订阅端 apply 吞吐可能下降。**这是加门真正的性能代价**，量级 **待实测**。

### 7.7 待实测清单（补充到 design §15.2）

| # | 观测量 | 用途 | 现状 |
|---|---|---|---|
| P1 | barrier 队头阻塞时延分布（p50/p95/p99） | 上界：加门的时延增量 | 待实测 |
| P2 | 加门后"分片 wire 时刻 − 分片产出时刻"的分布 | 加门的实际时延代价 | 待实测 |
| P3 | 当前顺序下的死锁发生率（每万事务） | 死锁代价 vs 时延收益 | 待实测（现场已见 deadlock detected） |
| P4 | 订阅端 apply worker 重启次数 / 重放字节量 | 死锁代价的量级 | 待实测 |
| P5 | walsender 峰值 RSS 与 held 区峰值字节 | held 上限怎么定 | 待实测 |
| P6 | 订阅端 apply 吞吐（改动前/后） | 7.6(b) 的量级 | 待实测 |

本机只做功能与正确性验证，以上全部不在本机测。

---

## 8. 对 §4 修复方案的两处修正

### 8.1 held 表不能纯按到达序放行（会放行死锁）

用户方案里"held 表随 `release_txns()` 推进按到达序放行"。纯 FIFO 有 liveness 问题：

```
T1 的段（end_lsn=400）先到，T2 的段（end_lsn=320）后到
held = [T1段, T2段]
  T1 段的门：frontier 头 > 400 → 现在头是 T2(350) → 不开
  T2 段的门：350 > 320 → 开，但排在 T1 段后面 → 放不出去
  T2 的 commit 要等 T2 段进 ready → T2 放不出去
  frontier 头永远停在 T2 → T1 段的门永远不开
⇒ 循环等待
```

**修正：held 表按 `end_lsn` 排序放行**（同事务内按产出序，天然单调）。判据不变（门 = `frontier == NIL || frontier_head->end_lsn > frame.end_lsn`），但放行时取的是**位置最小且门已开**的那一帧，而不是"队头"。

为什么按 `end_lsn` 排序就够：设两个事务 T2、T1 的冲突变更在 WAL 上依次为 p2 < p1（发布端靠行锁串行化 ⇒ T2 的提交在 p1 之前）。段满足 `segment.end_lsn ≤ 该事务的提交位置`，于是

```
T2 段.end_lsn ≤ T2 提交位置 < p1 ≤ T1 段.end_lsn
```

⇒ 冲突的两个段按 `end_lsn` 排出来的顺序 = 发布端的串行化顺序 ⇒ 订阅端的锁序与发布端一致 ⇒ 不会因为段的乱序而反转。

### 8.2 子事务 abort 的 STREAM ABORT：不能"不受门控"

用户方案里"subxact abort 帧一并过门"是对的，但**理由和 need.md 的字面要求冲突**，需要修订措辞：

* need.md FR-21（:176）与约束 19（:209）、design §5.2.2/§5.2.3（:762-772、:847、:870）都写着子事务 abort 帧"**不占 cseq、不受 barrier 门控**、必须即时下发、独占一块"。
* 但如果 abort 帧不过门，它会**越过它自己事务已经产出的流段**上 wire（流段被 held 表挡住，abort 帧直接进 ready）⇒ 订阅端先收到 abort（把 spool 文件截断到空），随后才收到被 abort 的那些变更 ⇒ **被 abort 的数据被 apply 出去 ⇒ 静默数据损坏**。

⇒ 正确的表述是：

> 子事务 abort 的 STREAM ABORT 帧**不占 cseq**（它不是结束事件，不需要参与提交序），但它**必须排在自己事务已产出的流段之后**，因此与流段走**同一道门、同一张 held 表**（同事务内按产出序 ⇒ 天然满足）。FR-21 里"不受 barrier 门控、必须即时下发"的本意是"worker 必须即时产出、不能压在 reorder buffer 里"（否则 abort 帧要等整个大事务结束才发），不是"可以越过 barrier 的结束事件"。

需要同步修订：need.md FR-21 / 约束 19、design §5.2.2 / §5.2.3 / §7.1 cseq 表（:1337-1352）、§8.1:1485。

---

## 9. B：会话拆除顺序（与 A 无关，另一件事）

### 9.1 现象与机制

发布端日志：

```
could not send a parallel decode payload: detached
```

三个进程（walsender / walreader / decode worker）通过**一个 DSM 段**里的 shm_mq 队列通信：

```
walreader  --(WAL 队列)-->  worker i
worker i   --(输出队列)-->  walsender
walreader  --(控制队列)-->  walsender
```

1. `shm_mq_attach()`（`shm_mq.c:309`）给**每个 attach 的进程自己**注册一个 `on_dsm_detach(shm_mq_detach_callback)`；这个回调在本进程 detach 的时候只处理**本进程手里的那些队列**。
2. DSM 段本身不会因为 walsender 退出就消失：段的引用计数（`dsm.c` 的 `refcnt`）在别的进程还持有映射时不会归零。真正消失的是**队列**：walsender 退出时 `proc_exit()` → `dsm_backend_shutdown()` → `dsm_detach()` → 它自己的输出队列 / 控制队列被 `shm_mq_detach_internal()` 标记为 detached，并唤醒对端。
3. 对端（还在发帧的 worker / walreader）下一次 `shm_mq_sendv()` 就返回 `SHM_MQ_DETACHED`；发送侧 `pd_queue_send_chunked()` 把"非 SUCCESS"一律 `ereport(ERROR)`（`parallel_decode_msg.c:349-353`）⇒ 这句报错。

⇒ 这句话的含义是"我还在发帧，但通道已经没了"。在拆除期它不是故障，是**会话结束的通知**。

> 更正：本树的 `shm_mq_result` 只有三个值 —— `SHM_MQ_SUCCESS` / `SHM_MQ_WOULD_BLOCK` / `SHM_MQ_DETACHED`（`src/include/storage/shm_mq.h:38-40`），**没有 `SHM_MQ_INTERRUPTED`**。早先写的"发送侧会看到 INTERRUPTED"是错的：被信号打断的阻塞发送在 `shm_mq_sendv()` 里是直接 `ereport(ERROR)`（`shm_mq.c` 的 `CHECK_FOR_INTERRUPTS()`），不走返回值。

### 9.2 三个子问题

**(1) 拆除期的 DETACHED 被当成 ERROR。**
接收侧已经有对称的处理（`PD_RECV_STOPPED`，`parallel_decode_worker.c:798`、`parallel_decode.c:462/1627/1784`），发送侧没有：任何非 SUCCESS 的返回值都是 ERROR。拆除期撞上 DETACHED（对端没了）应该是"对方没了，正常停"。

**(2) 异常退出路径没有保证"参与者先退、段后消失"。**
正常路径是对的：`walsender.c:1600-1614` 的 `PG_FINALLY` 一定调 `ParallelDecodeSessionStop()`，它（`parallel_decode.c:2284`）置 `ctl->shutdown=1`、逐个 terminate+wait 参与者，最后才释放 DSM 段。
漏的是**不走 PG_FINALLY 的退出路径**：`proc_exit()` **不会回卷 `PG_FINALLY`**（它不是 error unwind，是 `exit()`）。所以 walsender 被 SIGTERM、FATAL、postmaster 退出等路径带走时，`ParallelDecodeSessionStop()` 根本不会执行。那时只剩 `parallel_decode_before_shmem_exit()`（`parallel_decode.c:224`），它只把 `shutdown` 标志置 1（注释写明"waiting for the workers here would not be safe"）⇒ walsender 直接 proc_exit ⇒ 它的队列被 detach ⇒ 参与者撞上 DETACHED ⇒ 报 ERROR、退出码 1。

**(3) 拆除顺序存疑。**
`ParallelDecodeSessionStop()` 的注释写的是"Terminate the decode workers first, then the walreader: the walreader may still be feeding the workers."——**理由支持的是相反的顺序**（先停生产者 walreader，再停消费者 worker）。
而且：walreader 对 worker 的 WAL 队列是**阻塞发送**（`parallel_walreader.c:1200`，`nowait=false`），worker 先死 ⇒ walreader 可能在 `shm_mq_sendv` 里卡住；`parallel_decode_wait_for_worker()`（`parallel_decode.c:2249`）没有超时，会一直等 walreader 退出。
另外，参与者退出时**都没有 `shm_mq_detach` 自己的队列**（worker、walreader 都没有），所以接收方死掉后发送方是"队列满则阻塞"，而不是立刻收到 DETACHED。

### 9.3 验收标准

* 会话拆除时不再出现 `could not send a parallel decode payload: detached`；
* walreader / worker 在拆除期的退出码为 0，日志里不再有 ERROR；
* 拆除耗时不被 `shm_mq_sendv` 的阻塞拖长（不出现拆会话卡住）；
* 与 A 分开提交（A 改语义，B 改生命周期）。

### 9.4 实现（B 的提交内容）

| 文件 | 改了什么 |
|---|---|
| `parallel_decode_msg.c` | 发送侧记住"这次失败是因为队列被 detach"（`pd_queue_detached`），并由 `PDQueueDetached()` 暴露出来；错误本身照旧抛出（真故障还要报） |
| `parallel_decode.h` | `PDQueueDetached()` 的声明 |
| `parallel_decode_worker.c` / `parallel_walreader.c` | ① 收到 DETACHED 且"会话确实在收尾"时，把错误降级成干净退出（`proc_exit(0)`，日志一句 `... stopped while the session was ending`）；② 发送前先看廉价的 shutdown 标志（`pd_ctl->shutdown` / `got_sigterm`），不再往没人读的队列里发；③ 退出前 `shm_mq_detach()` 自己的队列，让对端立刻知道"不会再有消息" |
| `parallel_decode.c` | ① `before_shmem_exit` 里除了置 `shutdown`，还 drain 一次输出队列（给正卡在发送里的 worker 腾出空间，让它能完成传输、看到标志、干净退出）；② `ParallelDecodeSessionStop()` 里**先停 walreader（生产者）再停 worker（消费者）**；③ 等待参与者退出加了 30 秒上限，超时只 WARNING，不让拆除无限期挂住 |

**"会话确实在收尾"的判据是两条同时成立**，缺一不可：

* 队列被 detach（`PDQueueDetached()`），**并且**
* 这个参与者自己也该停了：`got_sigterm` / `pd_ctl == NULL` / `pd_ctl->shutdown` / walsender 进程已经不在了（`pd_worker_stopped()` / `pd_walreader_stopped()`）。

所以"对端意外死掉"不会被误当成正常结束：walsender 还活着、也没请求收尾时，DETACHED 仍然是 ERROR，仍然上报给 walsender。

### 9.5 验证

* 负例（未打 B 的二进制）：`doc/pa_decode/t/teardown_smoke.sh` 让 walsender 停在读帧的路上（`SIGSTOP`），把队列灌满，再把它杀掉 ⇒ 发布端日志出现
  `错误: could not send a parallel decode payload: detached`、`错误: could not send a parallel decode message: detached`，walreader / worker 以退出码 1 结束。
* 正例（打了 B 的二进制）：同一脚本 ⇒ 日志里没有任何 `detached`、没有任何 ERROR，参与者全部退出码 0，日志是
  `parallel decode worker 0 stopped while the session was ending` / `parallel decode walreader stopped while the session was ending`。
* 全量冒烟：`doc/pa_decode/t/` 下 17 个脚本全部通过（含新增的 `teardown_smoke.sh`）。

### 9.6 状态

已实现并验证，单独一个提交（与 A 的 `285bfc734b4` 分开）。
