# 发布端发送路径的瓶颈：帧率而不是 syscall 率

本文记录 2026-11-19 对"发布端发送只有 20MB/s、达不到 `design.md` §12.1 的 70MB/s 目标"的定位。

**口径**：本机只做功能与正确性验证，不跑吞吐测试；下文所有数字都是**静态推算或待实测**，不是实测结论。

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

