# design.md 的改动清单（给 peer 会话）

本文件**不修改** `doc/pa_decode/design.md`（那是 peer 会话的地盘），只列出实现已经落地的部分与
设计文档不一致的地方，以及建议的措辞。行号按 peer 会话最近一次提交后的 `design.md`（2026-11-19）。

涉及的两个实现改动：

1. **块化 + 接管缓冲**（发送路径）：worker → walsender 的流段字节从"拷 5 遍"降到"拷 4 遍"，
   walsender 侧只剩"块 → socket"一次拷贝。详见 `send_bottleneck.md` §4、§4.5。
2. **所有事务都流式化**（`stream_all_transactions`）：并行解码路径下，每个事务都是流段——
   进行中攒够 `parallel_decode_stream_segment_size` 就发一段，没攒够的**在提交时**作为一段发出。
   `parallel_decode_stream_min_size` 变成"低于这个大小的事务**完全不被流式化**（整块下发）"，
   0 = 每个事务都被流式化。两个尺寸不再互相抬高（原来的
   `if (min_size > segment_size) segment_size = min_size;` 已删除）。

---

## A. §5.2.3 上行条目（worker → walsender），第 830–875 行

### A.1 块边界不再用 FIRST / MORE / LAST 表达

设计里的 `PD_ENT_BLOCK` + FIRST/MORE/LAST 边界标记没有实现。实现是**一条消息 = 一块**
（枚举里 `PD_ENT_BLOCK = 7` 还在，但全代码没有用过——可以删掉，或者在设计里说明它是预留的）：

| 实现里的消息类型 | 载荷 | 对应设计里的块类型 |
| --- | --- | --- |
| `PD_ENT_SEGMENT` | 一个流段的全部字节；**帧之间用 `uint32` 长度前缀分隔** | `PD_BLK_SEG` |
| `PD_ENT_FRAME` | 一帧的字节（头里带 cseq、end_lsn）；cseq == 0 表示这一帧没有提交序 | `PD_BLK_TXN` 的一帧 / `PD_BLK_ABORT` |
| `PD_ENT_RELATION` | 关系描述帧（relid、cseq、epoch、字节） | 关系定义帧（去重用，见 6.7） |
| `PD_ENT_EVENT` / `PD_ENT_ERROR` / `PD_ENT_FLUSH` | 同设计表 | 同设计表 |

FIRST/MORE/LAST 的作用（"块内字节在 wire 上必须连续"）由**消息边界**承担：消息的载荷超过队列槽位时，
`shm_mq` 的 chunked 传输把它拆成若干 chunk，接收方 `PDChunkReceiverRecv()` 重组完之后才交给上层，
**重组完成之前不会有别的消息的字节混进来**——所以"整块连续"与"消息边界"等价。

> 建议在 §5.2.3 末尾加一段：
>
> **块边界由消息边界表达。** 实现不引入 FIRST / MORE / LAST 标记：一块 = 一条队列消息。消息载荷
> 超过队列槽位时由 chunked 传输拆分并在接收方重组（`PDChunkReceiverRecv`），重组期间该消息独占
> 接收缓冲，因此"块内字节在 wire 上连续"由消息边界保证。

### A.2 "块与 cseq 的两种组合"表（第 856–861 行）

第一行（被流式）不变，但补一句：**并行解码路径下每个事务都是流段**（进行中未达段大小的，
在提交时作为一个流段发出）。第二行（未被流式）的"数据"一栏要从
"一个 `PD_BLK_TXN`，在 `PD_ENT_TXN_END` 到达时一次性产出"改成
"若干条 `PD_ENT_FRAME`（一条变更一条消息），在 `PD_ENT_TXN_END` 到达时陆续产出"——
**整事务攒成一条消息没有实现**。

### A.3 `PD_BLK_TXN` 与 `PD_BLK_ABORT` 两行（第 847、848 行）

- `PD_BLK_TXN` 行：块类型保留，但正文要改成"**一个非流式事务的一帧**（实现上一帧一条消息）"。
- `PD_BLK_ABORT` 行：子事务 abort 在实现里走 `PD_ENT_FRAME`（cseq == 0），不是独立的条目类型；
  它独占一块的性质由"cseq == 0 ⇒ 受放行前沿约束"保证。

### A.4 第 873 行

"因此 `PD_BLK_TXN` 必须**自带 cseq**" —— 这条仍然成立，但载体是 `PD_ENT_FRAME` 的头。

---

## B. §8.2 分片与整事务两种输出形态（第 1550–1573 行）

- 第 1555 行的"整事务输出：worker 不切分，事务数据是一个完整的 `PD_BLK_TXN` 块，在提交时整块写出"
  → 改为"整事务输出：worker 不切分，事务的每一帧各自是一条消息（`PD_ENT_FRAME`），在提交时陆续写出"。
- 第 1564 行"非流式（`PD_BLK_TXN`）"行的"wire 被占用的时长 = 整个事务"仍然成立（字节仍然是连续的，
  只是传输粒度是帧）。
- **并行解码路径的热路径只有分片一种**：所有事务都被流式化之后，`PD_BLK_TXN` 形式只在
  `streaming='on'`（buffered 档）或串行路径上出现。

---

## C. §12.3 性能风险点，"walsender 的 FR-20 逐帧钳制"行（第 1706 行）

现措辞的缓解列是"worker 随块携带每帧 LSN 元数据（5.2 的 `PD_BLK_TXN` 增加帧边界表），使 walsender
不必自己解析 CopyData 边界；或者由 worker 直接完成钳制、walsender 只转发"。

**前半句已经实现**，而且不是"增加帧边界表"，是**流段的帧本来就带 `uint32` 长度前缀**
（worker 拼段时就加了），walsender 据此定位帧边界，**不必自己解析 CopyData 协议**。

建议把缓解列改成：

> **已实现（前半句）**：worker 在流段里给每帧加了 `uint32` 长度前缀，walsender 据此定位帧边界，
> **不必自己解析 CopyData 协议**。块化之后帧也**不再从段里拷出来**——帧的字节直接指向段缓冲。
> 剩下的是每帧改写帧头的两个 int64（发送时间戳 + FR-20 钳制）。
> **后半句（由 worker 直接钳制）未做**：钳制要按放行前沿逐帧判断，而放行前沿只有 walsender 知道。
> 成本待实测（15.2 第 6 项）。

---

## D. §12.3 "协议帧率"行（第 1702 行）

缓解列现在是"只能让每一帧的处理足够便宜"。可以补上具体做到了哪一步：

> walsender 每帧的剩余工作只有：**改帧头的发送时间戳 + FR-20 钳制 + 一次 `pq_putmessage_noblock`**。
> **没有额外的帧拷贝**——帧的字节直接指向它所在的块缓冲（块化之后），块本身又直接接管了
> worker 消息的缓冲。

---

## E. §5.3 低拷贝策略（第 940、966 行）

第 940 行"**串行路径上只剩一次拷贝**，这是本节的目标"——**已经达成**，可以把实际链路写出来：

```text
worker:     ctx->out --memcpy--> pd_segment --memcpy--> shm_mq 环
walsender:  环 --memcpy--> 接收缓冲 --(接管，不拷贝)--> 块缓冲 --memcpy--> socket
```

即 **walsender 侧只剩一次拷贝**（块 → socket）。

第 966 行"**输出侧不需要额外拷贝**：worker 把插件产出的字节按 FIRST / MORE / LAST 边界切成块片段
直接写入共享内存输出队列，walsender 取出后原样写入 socket。整块连续性由"walsender 一次只取一整块"
保证" → 改为：

> **输出侧不需要额外拷贝**：worker 把插件产出的字节拼成一段（一段 = 一条队列消息），walsender 取出后
> **接管**那块缓冲，帧的字节直接指向它，逐帧写进 socket 缓冲。整块连续性由"消息边界 = 块边界"保证，
> 而不是靠拷贝成一个连续大块。

---

## F. §8.1 与 §11.1：两个尺寸的语义

`parallel_decode_stream_segment_size` 与 `parallel_decode_stream_min_size` 的语义现在是：

| GUC | 语义 | 0 的含义 |
| --- | --- | --- |
| `parallel_decode_stream_segment_size` | 一个事务攒够这么多字节就切一段发出去 | 0 = 不切段（只在内存压力下流式化，buffered 档） |
| `parallel_decode_stream_min_size` | **低于这个大小的事务完全不被流式化**（整块下发） | 0 = **每个事务都被流式化** |

两个尺寸**不再互相抬高**（旧的 `segment_size = max(segment_size, min_size)` 已删除）。

设计文档里需要相应改的地方：

- 第 1510 行"**流式门槛 `parallel_decode_stream_min_size` 默认取 0（不设门槛）**"——不变，但要补上
  新的语义：0 = 每个事务都被流式化（**包括在进行中没有攒够段大小、在提交时才被流式化的**）。
- 第 1526 行"未流式化的事务，其整事务字节必须等到自己的 cseq 放行才能发出（`PD_BLK_TXN` 受 barrier
  门控）……挂起区因此可能装下整个事务的字节"——在 `immediate` 档 + `min_size = 0` 下**不再成立**：
  每个事务都是流段，挂起区里只剩几十字节的提交消息。
- 第 1539–1541 行"因此 `parallel_decode_stream_min_size` 的默认值必须是 0"——结论不变，理由要补一条：
  `min_size > 0` 现在意味着"低于门槛的事务**整个不流式化**"，比原来"只是不在进行中流式化"更硬。
- §11.1 的 GUC 表要同步（帮助文本已经改了，见 `guc_tables.c`）。

---

## G. §12.1 达成条件里的一处旧口径

第 1682 行提到"`parallel_decode_stream_min_size = 0` 不设门槛——否则订阅端的 apply worker 无事可做"。
按新语义还要补一句：**段大小必须小于典型事务大小**，否则一个事务永远攒不到段大小，
在 `min_size = 0` 的新语义下它会在**提交时**被流式化（而不是不流式化）——这一点与旧语义不同，
需要在 §15.2 的待实测清单里加一项：**`parallel_decode_stream_segment_size` 的默认值 1MB
对 tpcc（每事务约 3KB）意味着"每个事务都在提交时才被流式化"**，端到端时延因此接近整事务下发，
收益只剩"订阅端能并行 apply"。

---

## H. §11.3 唤醒计数的口径：下行要加上 walreader → walsender 的控制流

实现已落地：控制队列上的唤醒现在**逐条计数**，控制消息仍然**不批量**。

### H.1 为什么要修

`PDQueueSend()`（控制队列用的发送函数）不经过 `PDQueueSender`，因此**控制队列的刷新从来没被计过**。
而 walreader → walsender 的控制流（`PD_ENT_CSEQ` / `PD_ENT_READ_POS` / `PD_ENT_WATERMARK` /
`PD_ENT_READY`）走的正是 `PDQueueSend()`。⇒ `wakeups_down` **每提交少算约 1 次**，
报出来的下行唤醒速率只是**下界**。

### H.2 实现方式

- 新增 `PDQueueSetWakeupCounter(shm_mq_handle *mqh, volatile uint64 *wakeups)`：
  给一条"用 `PDQueueSend()` 写、没有自己的发送方"的队列注册唤醒计数器。
  注册表是进程内的小数组（每条队列恰好一个发送者，无需加锁）。
- `PDQueueSend()` 在**刷新成功**时给该队列的计数器加一。计数口径仍是"**刷新次数**"，
  与 `PDQueueSender` 那条路径一致（`SetLatch` 若因 latch 已置而早退，flush 次数是 SIGURG 次数的上界，
  这个 caveat 不变）。
- walreader 在挂上控制队列后注册自己的计数器（`parallel_walreader.c`），计数写进
  `pd_ctl->walreader_wakeups`，也就是 `pg_parallel_decode_wakeups.wakeups_down` 的来源。

### H.3 §11.3 表格第 1702 行

> `| pg_parallel_decode_wakeups | 跨进程唤醒（信号）的速率，区分上下行（5.4） |`

建议改成：

> `| pg_parallel_decode_wakeups | 跨进程唤醒（信号）的速率，区分上下行（5.4）。下行 = walreader 造成的**全部**唤醒：→ worker 的队列刷新 **加上** → walsender 的控制队列刷新 |`

### H.4 §11.3 实现要点第 1710 行那条 bullet

现有的措辞是"**唤醒次数的上下行不对称是设计使然**：下行（walreader → worker）传的是 WAL 记录，
上行（worker → walsender）传的是帧。两侧都按字节合并批次……"。这条现在**只覆盖了一半**：
下行其实有两条通路，**控制队列那条不批量**。建议改成：

> - **唤醒次数的上下行不对称是设计使然**：下行（walreader → worker）传的是 WAL 记录，上行（worker → walsender）传的是帧。两侧都按字节合并批次（5.4 第 9 项，`parallel_decode_batch_size`），因此**唤醒次数与"字节数 / 批大小"同阶，而不是与记录数 / 帧数同阶**。计数器的口径是"**刷新次数**"——刷新才真的唤醒了对端；承载进度的消息不批量，每次都刷新，因此各计一次。批处理关闭时（`parallel_decode_batch_size = 0`）计数退回"每条记录 / 每帧一次"。**唤醒速率的具体数值待实测**（15.2）
> - **下行有两条通路，控制队列那条不批量**：`wakeups_down` 是 walreader 造成的**全部**唤醒——(a) walreader → worker 的队列刷新，按字节合批；(b) walreader → walsender 的**控制队列**刷新（CSEQ / READ_POS / WATERMARK / READY），**逐条刷新、逐条唤醒**，因此这条通路的刷新次数**至少等于 walreader 已分配的提交序号数**。⇒ 恒有 `wakeups_down ≥ pg_parallel_decode_commit_lag.cseq_allocated`；这条不等式可以用来核对控制队列的唤醒是否被计入。

### H.5 §16 变更记录里"唤醒次数的口径从'每条记录 / 每帧'改为'每次刷新'"那条（第 2005 行）

那条写的是"计数点下沉到发送方层（`PDQueueSender` 持有一个指向共享内存计数器的指针）"——
现在多了一条通路，建议在末尾补一句：

> 控制队列不走 `PDQueueSender`，它的刷新由 `PDQueueSetWakeupCounter()` 注册的计数器单独计；
> 因此"下行"= walreader → worker **加上** walreader → walsender 的控制流。

### H.6 §15.2 第 14 项（待实测清单，第 1902 行）

那一条写的是"**批处理后的跨进程唤醒速率**（下行 walreader → worker、上行 worker → walsender）"。
下行的口径变宽了，建议把括号里的说明改成：

> （下行 walreader → worker **加上 walreader → walsender 的控制队列**，上行 worker → walsender）

同一行的实测记录"下行 969 → 363"是**旧口径**的数字（不含控制队列），建议注明"旧口径"，
或者按新口径重测后再回填。

### H.7 本机验证（只是计数，不是吞吐性能测试）

`monitoring_smoke.sh` 增加了一条断言：**`wakeups_down ≥ cseq_allocated`**（每个被编号的提交
都至少经控制队列唤醒过 walsender 一次）。本机同一负载（4 worker × 8 并发 × 40 事务 + 500 事务的
突发）下：批量关闭时下行 3569、批量 64KB 时下行 1782，同一负载的 `cseq_allocated` 是 820。

> ⚠️ **这条断言在当前负载下不能单独区分"控制队列被计入"与"没被计入"**：把控制队列的注册临时去掉
> 重测，下行是 904 —— worker 队列的刷新次数本身就与提交数同阶（本机实测 1 worker 时 839、
> 4 worker 时 904），所以 `904 ≥ 820` 仍然成立。**本机只做功能与正确性验证**，
> 上述数字是唤醒**次数**，不是速率；唤醒**速率**仍待性能环境实测回填。

---

## I. 流段切分改为"按变更条数"，并删除三个尺寸类 GUC

这一轮把流段的切分从**字节**改成**变更条数**，并把"事务是否被流式化"的选择权从发布端的
GUC 收回到"订阅端是否要并行 apply"这件事上。

### I.1 删掉的参数

| GUC | 去向 |
| --- | --- |
| `parallel_decode_streaming_mode`（enum） | 删除。取而代之的是一条硬规则：**只有 `streaming = 'parallel'` 的订阅端走并行路径**，其余（`on` / `off`）走串行路径 |
| `parallel_decode_stream_min_size`（字节） | 删除。"低于门槛的事务整个不流式化"这个语义没有了：并行路径的事务**一定**被流式化 |
| `parallel_decode_stream_segment_size`（字节） | 删除。切分点改成按**变更条数**：`parallel_decode_stream_segment_changes`（默认 8） |

新的参数：

| GUC | 语义 | 0 的含义 |
| --- | --- | --- |
| `parallel_decode_stream_segment_changes` | 一个事务攒够这么多条变更就切一个流段发出去 | 0 = 不切段，事务在提交时整块发出（并行 apply 退化成串行 apply） |

### I.2 "是不是 decode worker"由进程身份决定

`AmDecodeWorkerProcess()`（`src/include/replication/parallel_decode.h` 里的 `static inline`）
判断自己跑在哪个进程里。判据是**框架分配的类型** `ParallelDecodeWorkerType`
（`NONE` / `WALREADER` / `WORKER`）：walsender 在注册 bgworker 时把类型写进参数块
`ParallelDecodeWorkerArg`（连同一个 magic 数，用来把我们的块和别人的 bgworker 负载区分开），
参数块随 `bgw_extra` 交给 worker 进程；判断函数再从本进程自己的
`MyBgworkerEntry->bgw_extra` 读回来。walsender 不是 bgworker（`MyBgworkerEntry` 为 NULL），
别人的 bgworker 参数块没有 magic 数，两者都得到 `NONE`。

这与 upstream 订阅端的 `am_parallel_apply_worker()`（`worker_internal.h`：类型由 launcher
分配到共享结构里，worker 进程只读不写）同构，也与 `AmWalSenderProcess()` 同一个思路——
**进程的身份由它的启动方式决定，不需要自己记住并宣布**。

core 的 `reorderbuffer.c` 用它判断自己跑在哪个进程里：

- `ReorderBufferQueueChange()`：worker 走 `ReorderBufferStreamSegment()`（按条数切段），
  串行路径照旧走 `ReorderBufferCheckMemoryLimit()`（**一个字没改**）；
- `ReorderBufferStreamAtCommit()`：只有 worker 会把"提交时还没攒够条数的事务"作为一段发出去。

`ctx->worker_index` 的职责没变（它是"我是几号 worker"，用于区分事务归属），
"我是不是 worker"由 `AmDecodeWorkerProcess()` 回答——见 `src/include/replication/logical.h` 的注释。

### I.3 两条路径的字节不再相同

并行路径**一律**以流段下发事务（`STREAM START` / `STREAM STOP` / `STREAM COMMIT`，段内消息带
in-band xid），串行路径仍按整事务块下发（`BEGIN` / `COMMIT`）。⇒ 设计文档 4.1 里"两条路径
产出的 wire 字节逐消息一致"这条**不再成立**，成立的是"**事务内容一致**"。

比对工具（`t/pd_frame_tool.py`）相应改成**按事务内容**比对：把流段按 xid 重新拼回事务、
把 in-band xid 从消息字节里摘掉，再逐事务比较变更；关系描述单独按集合比较。
`rel_dedup_smoke.sh` 的期望也随之改成：串行路径整个会话 1 条关系描述，并行路径**每个事务 1 条**。

### I.4 两个已知缺口（本轮不补）

1. **worker 的内存没有上界。** 并行路径的 decode worker 不走 reorder buffer 的内存上限检查
   （`ReorderBufferCheckMemoryLimit()`），它的 reorder buffer 也不会落盘——worker 侧的记忆兜底
   （超限时把最大的可流式事务发出去）还没有实现。**后果**：`parallel_decode_work_mem` 目前
   **无效**（GUC 的说明文字与 `user_manual.md` 4.2.3 都已注明），一个足够大的事务可以把 worker
   的内存吃光。
2. **流段没有字节上限。** 段是按变更条数切的，一条变更可以很大，所以单个流段可以很大。
   串行路径靠 `logical_decoding_work_mem` 间接限制流段大小，并行路径目前没有等价手段。

### I.5 建议在设计文档里同步的几处

- §8.2（分片与整事务两种输出形态）：并行路径**只有分片一种形态**，"整事务输出"只在串行路径上出现。
- §11.1 的 GUC 表：换成 I.1 的新表。
- §12.1 达成条件里"`parallel_decode_stream_min_size = 0` 不设门槛"那一类表述：参数已不存在，
  换成"并行路径的事务一定被流式化"。
- §15.2 的待实测清单：段大小从字节变成条数之后，"段粒度 vs 端到端时延"的实测项要重写。
