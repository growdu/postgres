# 发布端发送路径的瓶颈：帧率而不是 syscall 率

本文记录 2026-11-19 对"发布端发送只有 20MB/s、达不到 `design.md` §12.1 的 70MB/s 目标"的定位。

**口径**：本机只做功能与正确性验证，不跑吞吐测试；下文所有数字都是**静态推算或待实测**，不是实测结论。

**读法**：§1–§3 描述的是 `777aecdd204` **之前**的实现（逐帧切分、`list_sort`、`pd_frame_buf` 拷贝），
这三项在 §4 里已经改掉（块化 + 二叉堆 + 零拷贝）。**当前的分析在 §9**，
最新的实测复核在 §9.9、等待事件的解读在 §9.10。

---

## 1. 结论

瓶颈**不在系统调用率**（8KB 批量刷出已经做过了，`walsender.c:3648`），而在 **walsender 的逐帧簿记**：

1. **worker 已经把一个流段拼成连续字节，walsender 又把它拆回逐帧的堆对象**（`parallel_decode.c:1725-1726`）。每个帧一次 `palloc` + 一次 `memcpy` + 一个 list cell。
2. **每收到一个块就把整个 held 表重新排序**（`parallel_decode.c:929` 的 `list_sort`）。held 表装的是"已解码但还没放行"的块，表越长每次插入越贵 → `O(H² log H)`。held 表在放行跟不上时会一直涨，排序代价同步膨胀，吞吐自我崩塌。
3. **每个帧再被拷一遍进 `pd_frame_buf`**（`parallel_decode.c:1473`），然后才进 socket 缓冲。

设计文档 §12.3 已经把这一条列为风险项（"walsender 的 FR-20 逐帧钳制 … 是 O(changes) 的单进程工作量，**并行化后新增而非消除**"），缓解手段写的是"worker 随块携带每帧 LSN 元数据 … 或由 worker 直接完成钳制"。本实现里这条风险的实际落点不是钳制本身（钳制只是在帧头改两个 int64），而是**为了钳制而必须先把块拆成帧**。

---

## 2. 证据

### 2.1 worker 侧已经是块化的

`parallel_decode_worker.c:306-315`：流段的帧被 `appendBinaryStringInfo` 追加到 `pd_segment`，每帧前面加一个 `uint32` 长度；`pd_worker_send_segment()`（`:381`）把**整段作为一条队列消息**发给 walsender。

### 2.2 walsender 侧把块拆回了帧

`parallel_decode.c:1706-1738`（`PD_ENT_SEGMENT` 的处理）：

```c
for (i = 0; i < sh.nframes; i++)
{
        ...
        frame = palloc(sizeof(PDQueuedFrame));      /* :1725 */
        frame->data = palloc(frame_len > 0 ? frame_len : 1);   /* :1726 */
        memcpy(frame->data, data, frame_len);
        ...
        frames = lappend(frames, frame);
}
```

一个含 N 帧的流段 → N 次 `palloc` + N 次 `memcpy` + N 个 list cell。

### 2.3 held 表每次插入都整表重排

`parallel_decode.c:912-937`（`parallel_decode_hold_frames`）：

```c
parallel_decode_held = list_concat(parallel_decode_held, frames);
list_sort(parallel_decode_held, parallel_decode_frame_pos_cmp);   /* :929 */
```

`list_sort` 是归并排序，每次调用都要额外分配一个同样长的临时数组。held 表的长度 H 是"已解码但尚未放行"的帧数——放行前沿卡住时它只涨不落。

### 2.4 逐帧的其它开销

| 位置 | 每帧的开销 |
|---|---|
| `parallel_decode.c:1959` | `parallel_decode_drain_control()`：控制通道的一次 `shm_mq_receive`（取自旋锁） |
| `parallel_decode.c:1991` | 轮询**每个** worker 的输出队列，每个一次 `shm_mq_receive`（取自旋锁） |
| `parallel_decode.c:1473` | `appendBinaryStringInfo(buf, frame->data, frame->len)`：再拷一遍 |
| `parallel_decode.c:1476-1477` | `pfree(frame->data); pfree(frame);` |
| `walsender.c:3627` | `pq_putmessage_noblock('d', …)`：拷进 socket 缓冲（协议要求一条变更一个 CopyData，不能省） |

---

## 3. 修法

### 3.1 块化（walsender 侧）

把 held / ready 两个队列的元素从"帧"改成"**块**"：

- 一个流段 = 一个块（`nframes` 个帧，帧在块内以长度前缀排列）；
- 一个单独的帧 = 一个只含一帧的块。

块在写出时才被拆开：写出侧拿着一个游标，按长度前缀依次遍历块内的帧，**直接指向块缓冲**写进 socket 缓冲，不再逐帧 `palloc`/`memcpy`/list cell。

这样每帧省掉：2 次 `palloc` + 2 次 `pfree` + 2 个 list cell + 2 次 `memcpy`（拆帧时的一次、拷进 `pd_frame_buf` 的一次）。剩下的每帧必需工作只有：改帧头的发送时间戳、FR-20 钳制、`pq_putmessage_noblock`。

### 3.2 held 表改用二叉堆

held 表的元素只会被**按位置从小到大**取出，所以用按 (位置, 序号) 排序的最小堆即可：

- 插入 `O(log H)`（原来是一次 `O(H log H)` 的整表排序）；
- 取出 `O(log H)`；
- 序号作第二关键字，顺序语义与原来的 `list_sort` 完全一致（`parallel_decode_frame_pos_cmp` 本来就是 (位置, 序号) 的全序）。

整体从 `O(H² log H)` 降到 `O(H log H)`。

### 3.3 不改的东西

- **wire 格式**：仍然一条变更一个 `CopyData`，订阅端一行不改（AC-9）。
- **FR-20 钳制语义**：仍然逐帧把帧头的两个 int64 钳到放行前沿；只是不再需要为了钳制而拆帧。
- **cseq 门控与放行前沿的判定**：`parallel_decode_release_txns()`、`parallel_decode_frame_may_go()` 的逻辑一字不改。

---

## 4. 实现（已完成，提交见 §6）

### 4.1 块化

`PDQueuedFrame` 改成 `PDQueuedBlock`（`parallel_decode.c`）：

| 字段 | 含义 |
|---|---|
| `data` / `len` | 块的字节 |
| `nframes` | 块里的帧数 |
| `segmented` | 帧在 `data` 里以 `uint32` 长度前缀排列（流段），否则整块就是一帧 |
| `end_lsn` / `relid` / `epoch` / `counted` / `seq` | 与原来的帧相同 |

* `PD_ENT_SEGMENT` 的处理不再逐帧 `palloc`+`memcpy`：长度前缀先整体校验一遍，然后整段一次 `palloc`+`memcpy` 拷成一个块（`parallel_decode.c:1848`）。
* `PD_ENT_FRAME`（`cseq == 0`）与 `parallel_decode_add_pending_frame()` 各产生一个只含一帧的块。
* 写出侧新增游标（`parallel_decode_cur_block` / `_index` / `_off`）：`parallel_decode_take_frame_of_block()` 按长度前缀依次取出块内的帧，**帧的字节直接指向块缓冲**，不再逐帧拷进 `pd_frame_buf`。

`ParallelDecodeNextFrame()` / `ParallelDecodeNextFrameNoWait()` 的签名从 `(StringInfo buf, XLogRecPtr *end_lsn)` 改成 `(PDFrame *frame)`；`ParallelDecodeClampFrame()` 从 `(StringInfo)` 改成 `(char *data, size_t len)`（原地改写，字节必须可写）。

### 4.2 held 表改二叉堆

`parallel_decode_held` 从 `List *` 改成 `binaryheap *`：

* 比较函数 `parallel_decode_block_pos_cmp()` 的键仍是 **(位置, 序号)** 全序，与原来的 `list_sort` 完全一致。PG 的 `binaryheap` 是**大顶堆**，所以比较函数是反的（`a < b` 返回 `1`），堆顶才是位置最小的块。
* `binaryheap_allocate()` 的容量固定，`binaryheap_add()` 满了会 `elog(ERROR, "out of binary heap slots")`，所以 `parallel_decode_held_add()` 在满时**手工扩容**（新建 2 倍容量的堆 → `binaryheap_add_unordered` → `binaryheap_build` → 释放旧堆）。
* 插入 `O(log H)`，取出 `binaryheap_first` + `binaryheap_remove_first`，也是 `O(log H)`。整体从 `O(H² log H)` 降到 `O(H log H)`。

### 4.3 不变量：wire 上的字节与顺序**逐字节不变**

块化只改"怎么存"，不改"发什么、什么顺序发"：

* 排序键不变：块按 (位置, 序号) 排序，序号在**块**一级递增。一个流段的所有帧在原实现里共享同一个 `end_lsn` 且序号连续，所以它们在排序后是连续的；块化后它们在**一个块**里，更连续。
* 原来"流段 A1 的 3 帧 + 别的块 B + 流段 A2 的 2 帧"在 ready 队列里的展开顺序是 `A1f1 A1f2 A1f3 B A2f1 A2f2`；块化后展开顺序完全一样。
* 同一事务的多个流段之间**不会夹进别的任务的字节**这件事，与原实现同源（原来靠"同 end_lsn + 连续序号"保证流段内的帧连续），块化后由"一个流段 = 一个块"直接保证。
* `frame_diff_smoke.sh`（串行路径 vs 并行路径的 wire 字节比对）通过，说明帧的字节与顺序没有变化。

### 4.4 验证

本机只做功能与正确性验证（不跑吞吐测试）：`doc/pa_decode/t/` 的 **17 个冒烟脚本全部通过**
（`frame_diff` / `ddl_diff` / `live_diff` / `concurrent_diff` / `streaming` / `keepalive` / `failure` /
`rel_dedup` / `slot_watermark` / `stream_order` / `subxact_abort` / `pending_area` / `copy_data` /
`monitoring` / `repl` / `worker_death_repro` / `teardown`）。

### 4.5 块接管消息缓冲（每字节少一次拷贝）

块化之后，worker → walsender 的流段字节还要被**再拷一遍**：消息处理函数（`PD_ENT_SEGMENT`）把
`rcv->buffer` 里的整段拷进新分配的块缓冲。这一次拷贝是可以去掉的：

* `PDChunkReceiverRecv()`（`parallel_decode_msg.c:262-274`）在交付载荷时**把缓冲的所有权交给调用方**
  （`rcv->buffer = NULL`），下一次调用会另分配一块新的，**不会复用旧缓冲**⇒ 块可以长期持有它；
* 于是块不再拷贝，而是**接管**消息缓冲：`PDQueuedBlock` 多了一个 `raw` 字段（缓冲的起点），
  `data` 指向 `raw` 里面（流段的头部在前面，所以 `data = raw + sizeof(PDSegmentHeader)`）；
* `parallel_decode_handle_message()` 改成返回 `bool`，表示"缓冲已经被我接管了"，调用方据此决定
  要不要 `pfree(payload)`。

这条路径上一共**每一份流段字节被拷贝 5 次**，现在降到 **4 次**：

| # | 拷贝 | 位置 |
|---|---|---|
| 1 | `ctx->out` → `pd_segment` | worker：`appendBinaryStringInfo`（`parallel_decode_worker.c:306`） |
| 2 | `pd_segment` → `shm_mq` 环 | worker：`shm_mq_sendv` 的 `memcpy`（`shm_mq.c:1043`） |
| 3 | 环 → `rcv->buffer` | walsender：`PDChunkReceiverRecv` 的 `memcpy`（`parallel_decode_msg.c:259`） |
| ~~4~~ | ~~`rcv->buffer` → 块缓冲~~ | **已去掉**：块接管缓冲 |
| 5 | 块 → `PqSendBuffer` | walsender：`pq_putmessage_noblock`（协议要求一条变更一个 CopyData，不能省） |

剩下的 1、2、3 都在 worker / 接收侧，5 是协议本身的要求；串行关键路径（walsender）上现在只剩
**一次拷贝**（第 5 次），这正是设计文档 §5.3 "串行路径上只剩一次拷贝" 的目标。

> 第 3 次拷贝去不掉：`shm_mq_receive()` 返回的是**环里的指针**，接收方一读，发送方就可以覆写那圈内存，
> 所以 walsender 必须把字节拷到自己的缓冲里。

---

## 5. 待实测

以下都是**待实测**（本机不跑性能测试）：

| 事项 | 说明 |
|---|---|
| 块化后的协议字节率 | 目标仍是 §12.1 的 ≥ 70MB/s |
| held 区在稳态下的长度 H | 决定了排序代价；块化后 H 的单位从"帧"变成"段"（约小 30 倍） |
| 每帧的剩余开销 | 改时间戳 + 钳制 + `pq_putmessage_noblock` 三项 |
| 帧率 | `pg_parallel_decode_walsender_frames`（设计文档 §11.3）采样 |

## 6. 一个流段都没有产生的问题（已解决）

**这一条比上面所有的优化都重要。** 测出 20MB/s 的那次运行，发布端很可能**一个流段都没有产生**。

**当时的状态：**

* 流式门槛在 `ReorderBufferCheckMemoryLimit()`：`segment_size > 0 && rb->size >= segment_size && ReorderBufferStreamableTxnReached(rb, segment_size)`，而 `ReorderBufferStreamableTxnReached()` 要求 `txn->total_size >= segment_size`；`segment_size = max(ctx->stream_segment_size, ctx->stream_min_size)`，这两个值在**会话开始时**从 GUC 取。
* `parallel_decode_stream_segment_size` 默认 **1MB**，`parallel_decode_stream_min_size` 默认 0。⇒ **一个事务要先攒够 1MB 的变更才会产生 STREAM START**。tpcc 那种 3KB 的事务永远达不到，于是走普通事务块。

后果有两个：

1. **订阅端不会启动并行 apply**（它看不到 STREAM START），apply 是串行的 ⇒ 端到端速率被订阅端卡住，发布端"发不出去"未必是发布端慢。
2. **本文改的 held 表根本没有被触发**（held 表装的是"没有 cseq 的帧"，只有流段和子事务 abort 属于这一类）。非流式路径上，发送侧的每帧开销是：worker 一次 `shm_mq_send` + walsender 一次 `shm_mq_receive`（一次自旋锁）+ 接收缓冲的一次 `palloc`+`memcpy` + 块的一次 `palloc`+`memcpy` + 两个 list cell。

**现在的状态（已解决）：** 并行解码路径在 `immediate` 档下把 `LogicalDecodingContext.stream_all_transactions` 打开，**没攒够段大小的事务在提交时作为一个流段发出**（`reorderbuffer.c` 的 `ReorderBufferStreamAtCommit()`，由 `ReorderBufferReplay()` 在提交时调用）。于是只要 `parallel_decode_stream_min_size = 0`（默认），**每个事务都是流段**，订阅端一定会看到 STREAM START。

* 两个尺寸的语义不再重叠：段大小只是**分段点**，最小大小是"能不能被流式化"的门槛；旧的 `if (min_size > segment_size) segment_size = min_size;` 已删除（两个尺寸不再互相抬高）。
* 提交时流式化有四个前提（见设计文档 §8.1）：路径是并行解码路径且会话被要求立即流式、事务此前没有被流式化、快照是一致的、事务不是两阶段提交的准备事务。
* 低于 `parallel_decode_stream_min_size` 的事务仍然以整事务块的形式下发。

**仍然成立的一条：** 段大小决定了"事务在**进行中**就被切段发出"的比例。默认 1MB 对 tpcc 那种 3KB 的事务来说太大，因此这些事务都在**提交时**才被流式化——端到端时延因此接近整事务下发，收益只剩"订阅端能并行 apply"。**这一条待实测**（设计文档 §15.2 第 15 项）。

## 7. 后续（本次不做）

- **worker 侧把非流式事务的帧攒成一个 `PD_BLK_TXN` 块**（设计文档 §5.2 的 `PD_BLK_TXN`）。当前非流式路径仍然是一条变更一条队列消息，walsender 侧只能逐帧持有。把一个事务的帧攒成一条队列消息，可以把**每帧一次 `shm_mq` 往返**摊薄成**每事务一次**——这是非流式路径上剩下的最大的一笔开销。注意：非流式事务的帧本来就要等到 cseq 轮到才放行，所以攒批**不增加延迟**。
  *这条的必要性取决于 §8：并行解码如果把所有事务都变成流段（`stream_all_transactions`），非流式路径就只剩兼容意义，这条可以降级。*

- **held 区的背压**：目前 `parallel_decode_held_max_bytes` 只告警、不限流（`parallel_decode.c:57`）。放行跟不上时 held 区会一直涨。加背压要小心与 barrier 的交互（可能死锁），需要单独设计。

## 8. 方向：并行解码只走流式路径

按"并行解码就是要所有事务都 stream 发出，区别只是一个 stream 块有多大"这条方向：

* 事务在**进行中**攒够 `parallel_decode_stream_segment_size` 就以流段发出；没攒够的**在提交时**作为
  一个流段发出（`reorderbuffer.c` 的 `ReorderBufferStreamAtCommit()`）。于是**每个事务都是流段**，
  非流式的整事务块（`PD_BLK_TXN`）不再是热路径。
* 这条路上**热路径只有一条**：worker 拼段 → 一条队列消息 → walsender 接管缓冲 → 逐帧写 socket。
  本文 §4 的块化、§4.5 的接管缓冲，都是针对这一条路做的；每帧剩下的工作只有
  **改帧头的时间戳、FR-20 钳制、`pq_putmessage_noblock`** 三项。
* 因此 §7 的第一条（`PD_BLK_TXN` 攒批）在流式路径上**不适用**：流段本来就是"一个事务的一段 =
  一条队列消息"，每帧一次 `shm_mq` 往返的问题在线段级别上已经解决了。
* **性能上的前提**（不再是正确性上的前提）：`parallel_decode_stream_segment_size` 小于典型事务大小，
  事务才会在**进行中**被切段发出。默认 1MB 对 tpcc 那种 3KB 的事务来说太大，于是它们都在提交时
  才被流式化（见 §6）——功能正确，但端到端时延接近整事务下发，收益只剩"订阅端能并行 apply"。**待实测**。

---

## 9. 从 50MB/s 到 70MB/s：缺口在哪

**口径**：本机不跑性能测试，下文所有数字都是**静态推算**（依据 `design.md` §15.1 的 tpcc 常数），
一律**待实测**。唯一的事实输入是用户报告的"发送端 50MB/s"。

### 9.1 前提变了：`immediate` 档把 RELATION 重发变成了常态

§6 之前，发布端在 tpcc 下**一个流段都不产生**（段大小默认 1MB，事务只有 3KB），走的是整事务块路径，
RELATION 由发布端自己的去重（`design.md` §6.7）压到"每关系每 epoch 一条"。改成"每个事务都是流段"之后：

* pgoutput 在流式模式下按 **(relation, top-xid)** 追踪关系描述是否已发（`pgoutput.c:743/767/1887`），
  于是**每个事务都要为它触及的每张表重发一次 RELATION**；
* 这条路径**进不了发布端的去重**：`maybe_send_schema()` 调 `send_relation_and_attrs(..., announce = !data->in_streaming)`，
  而 `rel_definition_hook` 只在 `announce` 为真时触发（`pgoutput.c:827`）⇒ 流式模式下钩子不触发 ⇒
  worker 不会把它标成关系帧（`parallel_decode_worker.c:279-335` 里 `pd_segment_active` 的判断在关系判断之前），
  它作为**普通帧**被拼进流段 ⇒ walsender 的 `parallel_decode_relation_sent()` 去重**看不到它**。
* `design.md` §6.7 已经写明"流式事务一律照发"，这是**有意为之**，不是 bug。

**关键结论：这部分字节发布端消不掉。** 订阅端的 apply worker 是从流里拿到关系描述的
（`worker.c:559-643` 的 `handle_streamed_transaction()` 对每条消息（含 RELATION/TYPE）做一次 `pa_send_data`，
leader 自己也用 `apply_handle_relation()` → `logicalrep_relmap_update()` 刷新缓存）。
发布端把 RELATION 吞掉，apply worker 就没有这份定义可用。而"订阅端源码一行不改"是硬边界
⇒ **RELATION 重发是当前设计下的固定开销**。

按 `design.md` §15.1 的静态估算（tpcc NewOrder：5 张表、约 600 B/条 RELATION）：

| 常数 | `buffered`（整事务块） | `immediate`（每事务流段） |
| --- | --- | --- |
| B：每事务协议字节 | 约 5 KB | 约 **8 KB**（其中 RELATION 重发约 **3 KB**） |
| C：每事务帧数 | 约 25 | 约 **30**（其中 RELATION 约 **5**） |

⇒ **`immediate` 档下约 37.5% 的协议字节、约 17% 的帧是纯冗余的 RELATION 重发。**

### 9.2 缺口换算

| 指标 | 50MB/s（用户实测） | 70MB/s（`design.md` §12.1 的 G1） | 缺口 |
| --- | --- | --- | --- |
| 事务/秒 | 约 6.25k | 约 8.75k | **+2.5k** |
| 帧/秒 | 约 187k | 约 262k | **+75k** |
| 协议字节/秒 | 50 MB | 70 MB | **+20 MB** |
| 其中 RELATION 重发 | 约 18.8 MB | 约 26.3 MB | +7.5 MB |
| **有效变更字节/秒** | **约 31 MB** | **约 44 MB** | +13 MB |
| walreader 要读的 WAL 记录/秒（2–4 记录/变更） | 约 310k–625k | 约 437k–875k | +130k–250k |

**第一件事是口径，不是代码。** G1 的 70MB/s 是"协议字节率"，而在 `immediate` 档下这 70MB/s 里有
**约 26MB 是重发的关系描述**。如果 70MB/s 的本意是"有效数据吞吐"，那发布端要写的是 **约 135MB/s 的协议字节**，
缺口是 **2.7 倍**而不是 1.4 倍。两者的整改方向完全不同，**先定口径**。

### 9.3 三个候选瓶颈（按可能性排序）

发布端逐帧的**剩余**工作已经很少了（§4.5、§8）：改帧头时间戳（一次 8 字节 `memcpy`）+ FR-20 钳制
（两个 `int64` 的 `pg_ntoh64`/`pg_hton64` + `memcpy`）+ `pq_putmessage_noblock`（拷进 8KB 的 `PqSendBuffer`）。
按每帧约 0.2 µs 静态估，**补上 75k 帧/秒只多花约 15 ms/秒 ≈ 1.5% 的一个核**；
socket 写次数 70MB/s ÷ 8KB ≈ 8.5k 次/秒，也微不足道。**walsender 的逐帧路径已经榨不出 20% 了。**

于是剩下的三个候选：

| # | 候选 | 性质 | 规模（静态估算，待实测） |
| --- | --- | --- | --- |
| 1 | **订阅端 leader 的每帧派发**：`PQgetCopyData` → `apply_dispatch` → 流式事务里每条变更一次 `pa_send_data` | 订阅端 leader 上的 O(changes) 串行工作量。`design.md` §15.1 把它列为**端到端瓶颈 #1**，且"只改发布端"这条边界**无法从发布端缓解** | 现有 187k 帧/秒已需 0.4–0.9 核；再补 75k 帧/秒再加 0.15–0.4 核 |
| 2 | **订阅端 apply worker 容量**：`max_parallel_apply_workers_per_subscription` 默认 **2**，超限即 `TRANS_LEADER_SERIALIZE`（落盘慢路径） | 端到端并行度的**硬上限**。`design.md` §12.1 按目标变更率反推的下限是 **4–40** | 低于下限 ⇒ 多出来的事务全部走 spool 慢路径 ⇒ 端到端**反而更慢** |
| 3 | **walreader 的每条 WAL 记录成本** | 唯一 WAL 读取者，单进程：CRC32C + 分类 + shadow decode + 序列化拷贝 + `shm_mq` 拷贝 | 现有 310k–625k 记录/秒；按每记录约 1 µs 估已占 0.3–0.6 核，**再补 130k–250k 记录/秒就可能顶到单核** |

**这三个候选有一个共同点：没有一个是"walsender 的逐帧开销"。** 50MB/s 到 70MB/s 的缺口，
在发布端**进程内**已经很难再找到可观的收益；真正的杠杆在订阅端配置（#2）、订阅端协议（#1，本设计管不到）、
以及 walreader 的单进程成本（#3，要动架构）。

### 9.4 分诊步骤（性能机上，约 20 分钟）

先做这一步再动代码。**四个检查项，按顺序做**：

**① 确认 trace 文件是关的。** `parallel_decode_trace_file` 只要非空，`PDTraceWrite()`
（`parallel_decode_trace.c:154-174`）就会**每帧**往磁盘写 4 字节长度 + 帧内容——两次 `write()` 系统调用
加一次落盘。18.7 万帧/秒 × 2 次系统调用 = 3.7 万次/秒的系统调用加上磁盘写。**这一个开关就足以
把速率压到 50MB/s 这个量级**，而它恰好是本仓库冒烟脚本的默认做法（`doc/pa_decode/t/*.sh` 普遍设了它）。
性能机上必须 `parallel_decode_trace_file = ''`。

**② 看 walsender 的 CPU 占用。**

```sql
-- 发布端，间隔 10s 采两次做差
select frames, frame_bytes, socket_writes from pg_parallel_decode_walsender_frames;
```

| 观察 | 判读 |
| --- | --- |
| walsender 进程 CPU ≪ 100%，字节率却稳定在 50MB/s | **发布端不是瓶颈**——它阻塞在 `write()` 上，**限速在下游**（订阅端消费能力或网络），去查 #1/#2 |
| walsender 进程 CPU ≈ 100% | 发布端进程内到顶了，去查 #3（walreader 是另一个进程，也要单独看） |
| `frame_bytes / socket_writes` ≈ 8192 | 批量刷出正常（8KB 缓冲），**系统调用率不是问题** |
| `frame_bytes / socket_writes` ≪ 8192 | 批量刷出没生效——每帧一次 `write()`，先查这里 |

**③ 看订阅端的 apply worker 占用。**

```sql
-- 订阅端
select parallel_apply_workers from pg_stat_subscription;   -- 应接近配置值，且不是恒定等于上限
```

`parallel_apply_workers` 长期贴着 `max_parallel_apply_workers_per_subscription` ⇒ 池子不够，
超出的事务在落盘 spool 慢路径上 ⇒ **端到端被订阅端卡住，而发布端看起来"发不出去"**。
把 `max_parallel_apply_workers_per_subscription` 调到 **8–16** 再测一遍（订阅端**配置**改动，
不改订阅端源码，允许）。

**④ 看放行前沿有没有卡住。** 这是一条与 CPU 无关的限速路径：

```sql
select cseq_allocated, cseq_released, commit_lag from pg_parallel_decode_commit_lag;
select commit_lag, pending_commit_age_us from pg_parallel_decode_pending_commit_age;
```

`cseq_allocated` 持续增长而 `cseq_released` 停滞 ⇒ 有事务卡在提交序上，它后面的所有字节都发不出去
（held 区只涨不落，`parallel_decode_held_max_bytes` 只告警不限流）。tpcc 里一个长事务就能造成这个现象。

### 9.5 各侧的杠杆

**发布端（能改的）**

| 杠杆 | 预期收益 | 代价 |
| --- | --- | --- |
| 关掉 `parallel_decode_trace_file` | 若开着，**这一个就可能是全部差距** | 无 |
| 加大 `parallel_decode_batch_size`（默认 64KB） | 减少跨进程唤醒（`kill(pid, SIGURG)`）的次数；按 §5.4 第 9 项，唤醒次数与"字节数/批大小"同阶 | 共享内存占用线性上升；唤醒速率本来就不是瓶颈（`pg_parallel_decode_wakeups` 采样确认） |
| 加大 `max_parallel_decode_workers` | **解码侧不是瓶颈**（§15.1：N = 3~4 就够）⇒ 收益递减 | 共享内存与信号开销线性上升 |
| 调小 `parallel_decode_stream_segment_size`（默认 1MB） | 对 tpcc（3KB 事务）**没有任何吞吐影响**——事务仍然在提交时才被流式化，只是切段点变了 | 只影响时延，不影响速率 |
| 减少 walreader 的每记录成本 | 需要动架构（walreader 是唯一 WAL 读取者），本轮不做 | 大 |

**发布端（改不了的）**

* **RELATION 重发**（9.1）：订阅端 apply worker 依赖它，订阅端源码不能改 ⇒ 消不掉。
* **帧数**：协议规定一条逻辑消息一个 CopyData，订阅端 `apply_dispatch()` 一个 CopyData 只解析一条消息
  ⇒ 合并帧要改协议。

**订阅端（配置可改，源码不改）**

* `max_parallel_apply_workers_per_subscription`：从默认 2 提到 8–16（`design.md` §12.1 的下限是 4–40）。
* 订阅端 leader 的每帧派发是端到端瓶颈 #1，只能靠协议改（批量 CopyData）解决，**超出本设计边界**。

### 9.6 结论

1. **发布端的逐帧发送路径已经不是瓶颈**：每帧剩余工作只有时间戳、钳制、一次 `pq_putmessage_noblock`，
   补齐 50→70MB/s 的缺口只多花约 1.5% 的一个核。继续在这条路径上做优化，预期收益接近 0。
2. **先定口径**：70MB/s 是"协议字节率"（含约 37.5% 的 RELATION 重发）还是"有效数据吞吐"？
   前者的缺口是 1.4 倍，后者的缺口是 2.7 倍。
3. **分诊优先于改代码**：trace 文件是否关着、walsender 的 CPU 占用、订阅端 apply worker 的占用、
   放行前沿有没有卡住——这四项能直接指出瓶颈在哪一端，本机不跑性能测试，必须在性能机上做。
4. **最可能的两个瓶颈都不在发布端的发送路径上**：订阅端 leader 的每帧派发（协议问题，本设计管不到）
   与订阅端 apply worker 容量（配置可改，最便宜）。

### 9.7 用 WAL 写速率给"缺口"重新定标

用户报告：**发布端 WAL 写速率 130MB/s**，同期协议字节率 50MB/s。这两个数字必须先对账，否则
"50 → 70" 这个目标本身就可能是错的。

`design.md` §15.1 的换算是 **协议字节率 = B/W × WAL 字节率**，其中 B（每事务协议字节）≈ 8KB、
W（每事务 WAL 字节）≈ 3KB ⇒ **B/W ≈ 2.6**。于是：

> 若 130MB/s 的 WAL **全部属于被发布的事务**，则发布端应写出 **约 340MB/s** 的协议字节。
> 实测 50MB/s ⇒ 只有预期的 **约 15%**。**发布端不是"离 70 还差 40%"，而是远远跟不上 WAL 的产生速度。**

但 130MB/s 这个数本身有两个水分，必须先拆开：

| 水分 | 说明 | 怎么拆 |
| --- | --- | --- |
| **非发布的 WAL** | 发布集之外的表、其它库、catalog WAL、checkpoint 后的全页镜像（FPI） | FPI 单独占多少：`pg_stat_wal.wal_fpi`；发布集只含 tpcc 的 9 张表时，订阅端的 WAL 速率才是对应的那个数 |
| **发布端跟不上的部分** | 复制槽没能及时消化、还挂在槽上的 WAL | **槽的推进速率** vs **WAL 的产生速率**（见下） |

**决定性的一次测量（发布端，间隔 10s 采两次）：**

```sql
select now(),
       pg_current_wal_lsn(),
       sent_lsn, write_lsn, flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as lag_to_sent
from pg_stat_replication;
```

再取 `pg_stat_wal` 的 `wal_bytes` 差分。两种结果对应完全不同的结论：

| 槽的推进速率 | 结论 |
| --- | --- |
| **≈ WAL 产生速率（130MB/s）** | 发布端**跟得上** WAL。那么协议字节率只有 50MB/s 说明 **B/W 对被发布的那部分事务远小于 2.6**（大部分 WAL 不在发布集里，或 FPI 占比高）。此时"70MB/s 协议字节"这个目标应当**按被发布的事务率重新推导**，而不是按总 WAL 率推导 |
| **≪ WAL 产生速率** | 发布端**在落后**，且落后量在持续增长。此时真正的问题不是"离 70MB/s 还差 20MB"，而是**复制槽在囤 WAL**：磁盘会被吃光，`max_slot_wal_keep_size` 到了就会断开复制。整改目标是"跟上 WAL 的产生速度"，50MB/s 就是当前的能力上限 |

**磁盘风险要一并查**（本机根分区 70G）：

```sql
select slot_name, active, wal_status, safe_wal_size
from pg_replication_slots;
```

`wal_status` 不是 `reserved`/`extended` 就说明槽已经保不住 WAL 了；配合 `df -h` 看剩余空间。

**结论**：在把"70MB/s"当成目标之前，先做 9.7 的这一次测量。它决定这件事是
(a) 发布端吞吐不足（缺口分析见 §9.3–§9.5），
(b) 指标口径不对（协议字节率 vs 有效数据率，见 §9.2），
还是 (c) **发布端正在被 WAL 的产生速度拖着走，真正的目标是"跟上"而不是"到 70"**。

### 9.8 "CPU 全闲、事务并发上不去"：放行链是一条一次一个事务的串行链

**现象**：tpcc 100 事务并发，发布端发出去了的事务并发只有 65，订阅端 apply worker 起不来；
**发布端与订阅端的 CPU 全部富余**。CPU 富余而速率上不去 ⇒ **系统是被"等"住的，不是在"算"**。

#### 9.8.1 放行链的结构（代码事实）

| 环节 | 位置 | 事实 |
| --- | --- | --- |
| 放行顺序 | `parallel_decode.c:1056-1199` | `parallel_decode_release_txns()` 只放行 `pending[parallel_decode_next_cseq]`——**严格按 cseq，一次一个** |
| 段的��置 | `parallel_decode_worker.c:355-370` | `position = (pd_current_cseq != 0) ? pd_record_lsn : pd_end_lsn`；提交时产生的那一段带的是**它自己提交记录的位置** |
| 放行闸门 | `parallel_decode.c:968-980` | `frame_may_go(end_lsn)` = `frontier ≥ end_lsn`，而 frontier = **最老的未放行事务的提交位置** |

三条合起来：**事务 k 的数据必须等事务 k-1 被放行之后才能上线**。wire 上事务是一个接一个、
按提交序交付的。订阅端的并行**不来自 wire 的交错**，而来自"apply worker 在 apply 上一个事务时，
leader 已经在收下一个事务的字节"。

⇒ **发布端的速率上限 = 1/(放行链单次往返) × B，而不是 CPU。**
放行链的一次往返是跨进程的：walreader 读到提交记录 → 交给 worker → worker 解码 → 发 END_ACK
（`PDQueueSendEvent` → `PDQueueSendUnbatched`，立即刷出，`parallel_decode_msg.c:648-657`）
→ walsender 放行。

50MB/s ÷ 8KB ≈ **6.25k 事务/秒** ⇒ 单次往返 ≈ **160 µs**。这是"进程间唤醒 + 解码 + 往返"的量级。

#### 9.8.2 往返里最大的一项：64KB 的批刷出间隔

> **⚠️ 本节的推论方向已被 §9.9.3 更正**：批间隔进的是**延迟**，不是**吞吐**。
> 下面保留原文作为推理过程，结论以 §9.9.3 与 §9.10.8 为准。

**队列的批只在字节阈值到时才刷出，没有基于时间的刷出**：

* walreader → worker：`pd_walreader_send_worker()` 里 `pd_wal_pending >= PDQueueBatchSize()`
  才 `pd_walreader_flush_queues()`（`parallel_walreader.c:1313-1314`）；
  另一个刷出点是"没有更多 WAL 了"（`:2236`）——高负载下永远走不到；
* worker → walsender：worker 只在"没有记录可解码"时刷出（`parallel_decode_worker.c:957`），
  或批满 64KB。

`PDQueueBatchSize()` = `min(parallel_decode_batch_size, PARALLEL_DECODE_QUEUE_SIZE/4)`，
**默认 64KB**。在 130MB/s 的 WAL 速率下：**64KB 一批 ≈ 0.5 ms**，
一条提交记录在批里的**平均等待 ≈ 0.25 ms**——与上面推出来的 160 µs 同量级。

⇒ **提交记录（也就是解锁放行链的那条记录）要等下一次批刷出才被 worker 看见。**
⇒ **放行链的往返时延由批间隔主导，而批间隔由 `parallel_decode_batch_size` 决定。**
（**更正**：这只对 walreader → worker 那一段成立。worker → walsender 那一段的 END_ACK 走
`PDQueueSendUnbatched()`，`force_flush = true` 会把 ring 里攒的数据一并发布，
所以段与 ACK 是同时到的，批大小对它没有影响。）

#### 9.8.3 决定性判据（发布端自己就能测）

| 测量 | 判读 |
| --- | --- |
| `select txn_count, txn_latency_us_avg, txn_latency_us_max from pg_parallel_decode_txn_latency;` | 这个延迟就是**"cseq 分配 → END_ACK"**，即放行链的单次往返。**若 `txn_latency_us_avg × 放行速率 ≈ 1`，就证明链是串行的、时延就是吞吐上限**（**更正**：这个判据把延迟当成了吞吐上限，见 §9.9.3——链的吞吐等于 ACK 的到达速率，链的深度足够时延迟项压不住吞吐） |
| `select cseq_allocated, cseq_released, commit_lag from pg_parallel_decode_commit_lag;` | `commit_lag` 稳定在几十上百 ⇒ 链后面压着一串事务，速率由链决定 |
| `select commit_lag, pending_commit_age_us from pg_parallel_decode_pending_commit_age;` | 最老的待提交事务的年龄 |
| `select wakeups_down, wakeups_up from pg_parallel_decode_wakeups;` | 唤醒速率。若下行唤醒 ≈ 事务数，说明**每事务一次唤醒**，批没有摊薄 |

**⇒ 最省事的一次实验（单变量、纯 GUC）**：

```sql
-- 发布端
alter system set parallel_decode_batch_size = 8192;   -- 64KB -> 8KB
select pg_reload_conf();
```

批间隔从 0.5 ms 降到约 30 µs。**更正：预期收益是 `txn_latency_us_avg` 下降，不是速率上升**
（§9.9.3）；实测"改成 8192 后速率没动"与这个预期一致，**这个实验没有否掉任何东西**。
注意：批变小 ⇒ 唤醒变多（130MB/s ÷ 8KB ≈ 16k 次/秒的下行唤醒），要一并看 `pg_parallel_decode_wakeups`。

#### 9.8.4 如果批间隔不是主因：代码上的两个候选

| 候选 | 位置 | 改法 |
| --- | --- | --- |
| **提交记录应当单独刷出** | walreader 只在批满时刷 | 转发一条**提交记录**时立刻刷出那一批——提交记录正是解锁放行链的那条消息，其它记录（变更、位置标记）继续按批走 |
| **放行链的串行段** | `parallel_decode_release_txns()` 一次只放行一个 cseq | 放行循环本来就能一次放行多个已 ACK 的事务（`for(;;)`），所以只要 ACK 是一批一起来的，链就能摊薄；问题在于 ACK 到达的节奏由批间隔决定 |

#### 9.8.5 并发数 65 vs 100 的口径要先定

"发出去的事务并发"有两种读法，含义完全不同：

| 读法 | 怎么量 | 含义 |
| --- | --- | --- |
| **发布端在途事务数**（已开工未结束） | `select sum(in_flight_txns) from pg_parallel_decode_workers;` | 反映**负载本身**的并发：tpcc 的 warehouse/district 行锁会让一部分事务排队，这部分与解码无关 |
| **已上线但订阅端未提交的事务数** | 订阅端侧（apply worker 的在途数） | 反映**放行链**的深度：链一次放行一个，这个数就是链的积压 |

⇒ 在拿 65 去和 100 比之前，先确认量的是哪一个。**如果是前者，65 < 100 可能是 tpcc 的行锁
（负载属性），不是解码瓶颈；如果是后者，它就是放行链积压的直接证据。**

### 9.9 实测数据下的复核：tpcc 100 并发 / 126w tpmc / 4 worker / 65 pa / 40–50MB/s

新的实测：发布端和订阅端的 CPU 都有富余，事务并发上不去（65 vs 100），速率 40–50MB/s。
这一节用这组数据复核 §9.3 的四个瓶颈点，并给出可以落地的判据与收益排序。

#### 9.9.1 四个瓶颈点在新数据下的复核

| 瓶颈 | 复核结论 |
| --- | --- |
| **3　held 区整表重排** | **不成立。** HEAD 的 held 区是二叉堆（`777aecdd204` 起），插入/弹出 O(log H)；§9.3 建议的"全局最小堆"就是现在的实现 |
| **5　唤醒风暴** | 成立，但**只是延迟项，不是吞吐上限**。CPU 富余时唤醒（SIGURG + 上下文切换）的代价不足以压住速率；它进的是 `txn_latency_us_avg`。而且计数器本身有缺口，见 §9.9.6 |
| **6　walreader 单核** | **要一个数才能排除**，见 §9.9.5 的第 0 项 |
| **4　严格 cseq 释放 + 无背压** | 成立，但它现在的表现是**被下游反压**，不是"限速"。见 §9.9.3、§9.9.4 |

⇒ 新数据最有价值的信息不是"CPU 富余"，而是 **65 vs 100**：这是唯一一个直接指向"有东西在等"的现象。

#### 9.9.2 一条之前没写进来的硬约束：协议层只允许一个未关闭的 stream

订阅端 `worker.c`：

* `apply_handle_stream_start()`（`worker.c:1467`）里有断言 "There must not be an active
  streaming transaction."（`worker.c:1481`），`in_streamed_transaction` 和 `stream_xid`
  是**单值全局**；
* `apply_handle_stream_stop()`（`worker.c:1626`）才把它们复位。

⇒ **线上同一时刻只能有一个开着的 stream。** 事务在 wire 上是逐个串行的。订阅端的 65 个
parallel apply worker 不是被 wire 并行喂的，而是被 leader apply worker 逐个转发
（`pa_send_data()` → `shm_mq_send()` → `LogicalParallelApplyLoop()` → `apply_dispatch()`，
转的是**原始字节**）。

所以发布端的"严格按 cseq 释放"不是本地实现的选择，是被协议锁死的。这一点在设计文档里
应该写死，否则以后一定会有人提"能不能乱序发"。

#### 9.9.3 释放链的**吞吐**不等于往返**延迟**（§9.8 的推论方向要改）

§9.8 把瓶颈归到"往返里最大的一项是 64KB 批间隔"。方向要改：批间隔进的是**延迟**，不是**吞吐**。

理由是放行是连续的：`parallel_decode_release_txns()`（`parallel_decode.c:1056`）在收到一个
ACK 后会**一路把后面所有已 ACK 的事务放下去**（`for(;;)`），所以

* 释放链的**吞吐** = ACK 的到达速率（前提是 ACK 顺序与 cseq 顺序不差太远）；
* 链的**深度**由 worker 的输入队列决定（每个 worker 256KB，共 4 个 ≈ 1MB WAL ≈ 两三百个事务）。

深度足够时，延迟项不会压住吞吐。**所以"降批间隔 → 提速率"这个实验的预期收益是降延迟，
不是提速率**；如果目标是速率，这个实验大概率测不出东西（§9.9.7 有更合适的替代实验）。

#### 9.9.4 "65 vs 100"有两种完全不同的解释，而在发布端看到的现象**完全一样**

`in_flight_txns` 的定义是"已分配给 worker、还没解码完"的事务数（`parallel_walreader.c:526`）。
它稳定在 65，可能是：

**(A) 发布端被订阅端反压**（可能性更大）

```
订阅端 pa worker 忙（等锁）
  → leader 卡在 pa_send_data()（applyparallelworker.c:1146，PA 队列 64KB 满）
  → leader 不读 socket
  → socket 缓冲满
  → walsender 卡在 ProcessPendingWrites()（walsender.c:3682 附近）
  → 发布端 worker 的 256KB 出队满 → worker 卡在 shm_mq_sendv()
  → worker 不再解码 → in_flight_txns 冻结在 65
```

**(B) 发布端自己慢**：worker 的解码/发送侧到了上限。

**两条链在发布端的表现是一样的**（都是 worker 卡在出队、in_flight 不再增长）。
区分只能看订阅端：

| 订阅端 65 个 pa worker 的状态 | 结论 |
| --- | --- |
| 都在跑，但 CPU 只有 20% 左右 | 它们在**等锁**（tpcc 的 stock/warehouse/district 行锁）⇒ 瓶颈在订阅端，发布端只是忠实反映 |
| 大部分是空闲的（在等数据） | 数据喂不进去 ⇒ 瓶颈在发布端 |

⇒ **在跑 tpcc 之前先抓这一张订阅端的快照**，否则发布端的所有优化都可能做在错误的一侧。

#### 9.9.5 决定性测量（性能机，约 20 分钟）

**第 0 项（先做，成本最低）：确认 walreader 进程本身有没有跑满。**

"发布端 CPU 富余"如果指的是整机平均，那么排除不了瓶颈 6——walreader 只有一个线程，
跑满 100% 在多核机器上整机平均完全看不出来。

```bash
# 发布端
top -H -p $(pgrep -f "parallel decode walreader" | head -1)
# 或者按进程看
ps -o pid,psr,pcpu,comm -C postgres | grep -i postgres
```

**第 1 项：发布端是不是真的落后于 WAL**（§9.7 的判据，仍然有效）

```sql
select application_name, state, sent_lsn, write_lsn, flush_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as behind_bytes
  from pg_stat_replication;

select wal_status, restart_lsn,
       pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) as retained_bytes
  from pg_replication_slots;
```

`behind_bytes` 持续增长 ⇒ 发布端确实追不上 WAL（瓶颈 6 或解码吞吐）；
基本为 0 ⇒ **发布端跟得上自己的解码，积压全在网络和订阅端**。

**第 2 项：放行链有没有积压**（连采两次，间隔 1s）

```sql
select cseq_allocated, cseq_released, commit_lag, pending_commit_age_us
  from pg_parallel_decode_pending_commit_age;
select txn_count,
       txn_latency_us_sum / greatest(txn_count, 1) as latency_avg_us,
       txn_latency_us_max
  from pg_parallel_decode_txn_latency;
select worker_index, status_text, in_flight_txns, in_flight_bytes,
       queue_depth, wakeups
  from pg_parallel_decode_workers order by worker_index;
```

判据：

| 观测 | 含义 |
| --- | --- |
| `commit_lag` 持续增长 | 解码完成了但放行跟不上 ⇒ **放行链**是瓶颈（§9.9.3 的深度不够，或 ACK 顺序乱） |
| `commit_lag` ≈ 0 | 放行链没问题 ⇒ 瓶颈在**放行之后**（socket / 订阅端） |
| `in_flight_bytes` 顶到 256KB 附近 | walreader 被 worker 挡住了（worker 侧慢或被反压） |
| `queue_depth` 顶高 | 同样：worker 消费不过来 |
| `latency_avg_us` 很大 | 延迟项主导，速率未必受影响（见 §9.9.3） |

**第 3 项：订阅端在干什么**（区分 §9.9.4 的 A / B）

```sql
-- 订阅端
select pid, application_name, state, wait_event_type, wait_event,
       backend_xid, query
  from pg_stat_activity
 where application_name like '%apply%' or backend_type = 'walsender'
 order by wait_event_type, wait_event;
```

`wait_event` 集中在 `LogicalRepApply*Lock` / `transactionid` / `tuple` ⇒ 等锁 ⇒ 走 (A)。

#### 9.9.6 收益排序：先否掉一个大的

**❌ 不该做：RELATION 去重（§9.1 的"37.5% 是冗余 RELATION"）**

机制描述是对的：pgoutput 在 streaming 模式下 `announce = !data->in_streaming`（`pgoutput.c:773`），
所以 `rel_definition_hook` 不触发（`pgoutput.c:827`），发布端已有的 RELATION 去重
（`parallel_decode.c:838`、`:1130`）看不到这些帧。

**但即使让它触发也不能去重**，原因是订阅端：

1. parallel apply worker 是**一事务一个**的：`pa_allocate_worker()`（`applyparallelworker.c:470`）
   在 stream 开始时分配，`pa_xact_finish()`（`applyparallelworker.c:1618`）在 COMMIT 时回收，
   进程随之退出；
2. 每个 worker 是新进程，**relcache 是空的**；
3. leader 只把**原始字节**转发给 worker，不分发关系定义
   （`pa_send_data()` → `shm_mq_send()` → `LogicalParallelApplyLoop()` → `apply_dispatch()`）。

⇒ **每个事务的 stream 里都必须带 RELATION。这是协议要求，不是浪费。**

这条要写进文档：否则后面一定会有人拿"37.5% 的字节是白发的"去追一个不存在的优化。
顺带说明 pa_decode "所有事务都 stream" 的代价：上游只在事务大于 `min_size` 时才流式化，
pa_decode 把 `min_size` 设成 0，于是**每个事务**都要付一次 RELATION 的固定成本，
而这部分字节直接抬高了"每事务协议字节数"这个下限。

**⚠️ 还有一个计数口径的问题要先修**：唤醒计数器数不到控制队列。
`PDQueueSend()`（`parallel_decode_msg.c:78`）不调 `pd_queue_count_wakeup()`，而 walreader →
walsender 的控制流（`PD_ENT_CSEQ` / `PD_ENT_READ_POS` / `PD_ENT_WATERMARK`）走的是裸
`PDQueueSend()`。⇒ `wakeups_down` 每提交少算约 1 次；而且 flush 次数是 SIGURG 次数的**上界**
（`SetLatch` 在 `latch.c:648`/`:654` 会早退）。**现在报出来的唤醒速率只能当下界看。**

**✅ 有收益、可以试的（按性价比）**

1. **先做 §9.9.5 的测量，别先改代码。** 如果结论是 (A)（订阅端锁竞争），发布端改什么都不会
   提高订阅端的 apply 速率；该做的是加 PA worker 或调 tpcc 的锁粒度。
2. **降延迟（不是降速率）**：`parallel_decode_batch_size` 64KB → 8KB。影响的是
   walreader → worker 那一段：提交记录要等 walreader 攒够一批才 flush
   （`parallel_walreader.c:1313`），在 130MB/s 下平均等 ~0.25ms。
   **预期指标是 `txn_latency_us_avg` 下降，不是速率上升。**
3. **补上唤醒计数**（口径修复，不是优化）：给 `PDQueueSend()` 加一个 wakeup 指针参数，
   控制队列的每条消息单独唤醒并计数，**不要批量化控制消息**（批量化会给放行链加延迟）。
   修完 `pg_parallel_decode_wakeups` 的语义要改：下行 = walreader → worker **加上**
   walreader → walsender 的控制流。
4. **释放链的深度**：如果 §9.9.5 的第 2 项显示 `commit_lag` 在涨，那是深度/顺序问题，
   与批间隔无关，得单独查 ACK 的乱序程度。

**❓ 还要核的一件事：40–50MB/s 是不是"该有的量"**

126w tpmc = 21000 事务/秒。40–50MB/s ÷ 21000 ≈ **2.0–2.4KB/事务**。
其中 RELATION 占比按 §9.1 的口径约 37.5%（待复测）。
⇒ **每事务协议字节数有一个由协议决定的下限**，这个下限乘以事务速率就是速率。
如果目标是"更大的 MB/s"而事务速率不变，那在协议不改的前提下是达不到的；
能改的只有"更小的每事务固定成本"，而 RELATION 那部分被订阅端锁死了（见上面）。

⇒ **所以要先问清楚目标是"更高的事务速率"还是"更高的字节速率"。** 前者受每事务固定成本
（RELATION + STREAM START/COMMIT）限制，后者受订阅端的 apply 能力限制。

#### 9.9.7 结论

1. **四个瓶颈点里，在这份数据下只有瓶颈 4 还成立**，而且它表现为"被反压"，不是"限速"。
2. **瓶颈 5 是延迟项，瓶颈 3 不成立，瓶颈 6 需要一个逐进程 CPU 的数才能排除。**
3. **当前最大的一笔"看似白费"的字节（RELATION 冗余）是不能省的**，省了订阅端会报错。
4. **下一步不是改代码，是做 §9.9.5 的四组测量**，尤其是第 3 项（订阅端 pa worker 的等待事件），
   它决定后面所有工作是做在发布端还是做在订阅端。


### 9.10 把"ipc / queue receive"读出来：发布端在**挨饿**，不是在**堵**

新观测（把 `parallel_decode_batch_size` 改成 8192 之后）：**walsender 的 CPU 只有 35%，
等待事件全是 `ipc / queue receive`**。这一节把这条观测翻译成机器可读的结论。

#### 9.10.1 事件翻译

`ipc / queue receive` = `wait_event_type = 'IPC'`、`wait_event = 'MessageQueueReceive'`，
就是 `shm_mq_receive()`（`shm_mq.c:1165`）。它的语义只有一句：**这个进程在等对端往队列里放东西，
队列是空的。**

⇒ **这是饥饿的信号，不是开销的信号。** 三个方向的推论：

| 如果瓶颈是这个 | 队列的状态 | 等待事件会长什么样 |
| --- | --- | --- |
| 发布端解码不过来 | **满** | `IPC / MessageQueueSend`（`shm_mq.c:1019`），或干脆 CPU 打满 |
| 订阅端消费不过来 | **满** | walsender 卡在 socket 写：`Client / ClientWrite` |
| **上游没东西可解码** | **空** | **`IPC / MessageQueueReceive`** ← 观测到的就是这个 |

#### 9.10.2 谁在等谁（代码事实）

| 进程 | 等的东西 | 等待事件 | 位置 |
| --- | --- | --- | --- |
| **decode worker** | walreader 给它的 WAL 记录 | **`IPC / MessageQueueReceive`** | `pd_worker_recv_record()` → `PDChunkReceiverRecv(..., nowait=false)` → `PDQueueRecv` → `shm_mq_receive`（`parallel_decode_worker.c:959`、`:812`） |
| **walsender** | worker 给它的帧 | `Extension / Extension`（`PG_WAIT_EXTENSION`）——它对队列是**非阻塞轮询**，然后在自己的 latch 上等 | `ParallelDecodeNextFrame()`，`parallel_decode.c:2182-2265` |
| **walsender** | socket 写（订阅端慢） | `Client / ClientWrite` | `ProcessPendingWrites()`，`walsender.c:3689` |
| **walreader** | **新的 WAL** | `Extension / paralleldecodedewalreader` | `pd_walreader_read_page()`，`parallel_walreader.c:423`、`:2427` |
| **walreader** | 往 worker 队列写（队列满） | `IPC / MessageQueueSend` | `pd_walreader_send_worker()` |

⇒ **`ipc / queue receive` 只能是 decode worker 在等 walreader 的记录**（订阅端那边，
`pa worker` 等 leader 的字节也是同一个事件，见 §9.9.4）。
⇒ **这说明：worker 饿着。唯一的生产者 walreader 没喂它们。**
⇒ **"所有等待事件都是 ipc / queue receive"这句话本身就是个矛盾**：walsender 的等待事件应该是
`Extension`，walreader 的应该是 `Extension / paralleldecodedewalreader` 或 `IPC / MessageQueueSend`。
⇒ 所以这张表**必须按进程分组看**，不能只看事件名。三件事被读成了一件。

#### 9.10.3 串行路径的对照：WAL 一刷出就把人叫醒

**串行路径（同一个进程里做）**：

```
logical_read_xlog_page()            walsender.c:1057
  → WalSndWaitForWal()              walsender.c:1920
      RecentFlushPtr = GetFlushRecPtr(NULL)          walsender.c:1981
      ConditionVariablePrepareToSleep(&WalSndCtl->wal_replay_cv)   walsender.c:4079
      WaitEventSetWait(...)
```

唤醒方在 WAL 刷出之后：`XLogFlush()` 末尾 `WalSndWakeupProcessRequests(true, !RecoveryInProgress())`
（`xlog.c:2930`、`:3105`）→ `WalSndWakeup()`（`walsender.c:4019`）→
`ConditionVariableBroadcast(&WalSndCtl->wal_replay_cv)` → `SetLatch()` →
**walsender 立刻醒，延迟 = 一个刷出周期。**

**并行路径**：

```
pd_walreader_read_page()            parallel_walreader.c:358
  read_upto = GetFlushRecPtr(&currTLI)                :382
  ...
  WaitLatch(MyLatch, WL_LATCH_SET|WL_TIMEOUT|WL_EXIT_ON_PM_DEATH, 100L, pd_wait_event)   :423
```

walreader 的 latch **只有它自己的 SIGTERM handler 会 set**（`:268`），
**没有任何人因为"WAL 刷出了"去 set 它** ⇒ **新 WAL 只能靠 100 ms 的超时被发现。**

⇒ **这是并行路径与串行路径的一处真实差异：串行是事件驱动，并行是 100 ms 轮询。**

#### 9.10.4 100 ms 轮询的形状后果

流水线按 100 ms 成波推进：一波里 walreader 读走当时能读到的全部 WAL 并转发给 worker，
worker 解码、walsender 放行；波与波之间**没有任何新记录**，worker 全部阻塞在 `shm_mq_receive`，
walsender 阻塞在自己的 latch 上。

⇒ **"CPU 全富余 + 等待事件全是 ipc / queue receive"正是这个波形的指纹。**
但要说清楚：**它解释的是形状（谁在等什么、为什么 CPU 闲），不自动解释平均速率。**
每一波的平均速率取决于那一波能被多快排空。

#### 9.10.5 由等待事件分出的两条互斥结论

| 观测 | 结论 | 责任方 |
| --- | --- | --- |
| walreader 停在 `Extension / paralleldecodedewalreader`（或 CPU 很低） | **发布端在等 WAL**：要么磁盘刷出慢（walreader 等的是 `GetFlushRecPtr()`），要么它就睡在 100 ms 的轮询里 | 先查机器，再谈代码 |
| walreader 停在 `IPC / MessageQueueSend`，或 CPU ≈ 100% | **发布端追不上 WAL**：瓶颈是 walreader 单核（§9.3 候选 3、§9.9.5 第 0 项） | 发布端 |
| walsender 停在 `Client / ClientWrite` | **订阅端消费不过来**，socket 堵住了 | 订阅端 |

#### 9.10.6 决定性测量（一轮，三条）

**① 按进程分组的等待事件（发布端）**——这一条就能把 §9.10.5 的三行分开：

```sql
select backend_type, pid, state, wait_event_type, wait_event, count(*)
  from pg_stat_activity
 where backend_type like 'parallel decode%' or backend_type = 'walsender'
 group by 1,2,3,4,5
 order by 1,2;
```

`backend_type` 的取值：`parallel decode walreader` / `parallel decode worker` / `walsender`
（`parallel_decode.c:2743`、`:2758`）。

**② WAL 的三条速率：插、写、刷**——**发布端只解码"刷出来"的那部分**：

```sql
-- 采两次，间隔 10s，做差
select pg_current_wal_insert_lsn(), pg_current_wal_lsn(), pg_current_wal_flush_lsn();
```

* `pg_current_wal_lsn()` = **写**位置（已经 write 给操作系统，不一定 fsync 过）；
* `pg_current_wal_flush_lsn()` = **刷**位置（fsync 过的位置）。

walreader 等的是 `GetFlushRecPtr()`（`parallel_walreader.c:382`），
**串行路径等的是同一个东西**（`walsender.c:1981`）。
⇒ **若刷出速率只有写速率的零头，发布端的 50MB/s 就是"该有的量"**，问题在磁盘，不在发布端的代码。
辅助证据：`select wal_write, wal_sync, wal_sync_time, wal_buffers_full from pg_stat_wal;`
（`wal_buffers_full` 频繁增长 = 插入端被刷出端顶住了）。

**③ 逐进程 CPU**：`top -H -p <walreader pid>`（walreader 只有一个线程，整机平均看不出来）。

**④ 对照实验（最省事、最有说服力）**：在同一台机器上用**串行**逻辑解码订阅同一个发布端，
比协议字节率。**串行也是等 `GetFlushRecPtr()` 的**（`walsender.c:1981`），
所以串行与并行的字节率如果差不多，**说明上限在机器上，不在并行设计上**。

#### 9.10.7 如果结论是"发布端在等 WAL 刷出"

* 这**不是并行解码的缺陷**（串行等的是同一个位置）。该查的是 walwriter 的刷出能力
  （`pg_stat_wal.wal_sync` / `wal_sync_time` / `wal_buffers_full`）与磁盘，不是发布端的发送路径。
* 发布端唯一能改的一项：**把 walreader 的 100 ms 轮询换成"WAL 刷出"通知**（只动发布端，不碰订阅端）：
  在 `XLogFlush()` 之后 `WalSndWakeupProcessRequests()` 旁边挂一个 `ParallelDecodeNotifyWalFlush()`，
  广播并行解码共享内存里的一个 ConditionVariable；walreader 改成在 CV 上等
  （`ConditionVariablePrepareToSleep` → `WaitLatch(MyLatch)` → `ConditionVariableCancelSleep`），
  保留超时兜底。
  **收益是去掉 100 ms 的发现延迟与波纹（延迟项 + 波间空转的 CPU），不是把 50MB/s 变成 70MB/s。**

#### 9.10.8 对前文的修订

* **§9.8.2 / §9.8.3 的方向要改**（§9.9.3 已给出理由，这里补上代码依据）：
  worker → walsender 这一段，END_ACK 走 `PDQueueSendUnbatched()`（`parallel_decode_msg.c:556`），
  它先 `sender->pending = 0` 再以 `force_flush = true` 写 ⇒ **ring 里攒的数据被一并发布**，
  所以**每个事务的段和它的 ACK 是同时到的**，批大小对它没有影响。
  批大小真正影响的是 walreader → worker 那一段（`parallel_walreader.c:1313`）。
  ⇒ **`parallel_decode_batch_size` 8192 的预期收益是 `txn_latency_us_avg` 下降，不是速率上升**；
  实测"改成 8192 后速率没动"与这个预期一致，**这个实验没有否掉任何东西**。
* **§9.1–§9.6 与 §1–§3 描述的是 `777aecdd204` 之前的实现**（逐帧切分、`list_sort`、`pd_frame_buf` 拷贝），
  **§4 之后已经改掉**（块化 + 二叉堆 + 零拷贝）。照着 §1–§3 找瓶颈会得出已经不存在的结论。
