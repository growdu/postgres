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

---

## 5. 待实测

以下都是**待实测**（本机不跑性能测试）：

| 事项 | 说明 |
|---|---|
| 块化后的协议字节率 | 目标仍是 §12.1 的 ≥ 70MB/s |
| held 区在稳态下的长度 H | 决定了排序代价；块化后 H 的单位从"帧"变成"段"（约小 30 倍） |
| 每帧的剩余开销 | 改时间戳 + 钳制 + `pq_putmessage_noblock` 三项 |
| 帧率 | `pg_parallel_decode_walsender_frames`（设计文档 §11.3）采样 |

## 6. 还没有解决的事：20MB/s 未必是发布端的发送能力

**这一条比上面所有的优化都重要。** 测出 20MB/s 的那次运行，发布端很可能**一个流段都没有产生**：

* 流式门槛在 `ReorderBufferCheckMemoryLimit()`（`reorderbuffer.c:3800`）：`segment_size > 0 && rb->size >= segment_size && ReorderBufferStreamableTxnReached(rb, segment_size)`，而 `ReorderBufferStreamableTxnReached()`（`:3719`）要求 `txn->total_size >= segment_size`（`:3730`）；`segment_size = max(ctx->stream_segment_size, ctx->stream_min_size)`（`:3767`、`:3775`），这两个值在**会话开始时**从 GUC 取（`parallel_decode.c:2589`）。
* `parallel_decode_stream_segment_size` 默认 **1MB**（`parallel_decode.c:53`），`parallel_decode_stream_min_size` 默认 0（`:52`）。⇒ **一个事务要先攒够 1MB 的变更才会产生 STREAM START**。tpcc 那种 3KB 的事务永远达不到，于是走普通事务块。

后果有两个：

1. **订阅端不会启动并行 apply**（它看不到 STREAM START），apply 是串行的 ⇒ 端到端速率被订阅端卡住，发布端"发不出去"未必是发布端慢。
2. **本文改的 held 表根本没有被触发**（held 表装的是"没有 cseq 的帧"，只有流段和子事务 abort 属于这一类）。非流式路径上，发送侧的每帧开销是：worker 一次 `shm_mq_send` + walsender 一次 `shm_mq_receive`（一次自旋锁）+ 接收缓冲的一次 `palloc`+`memcpy` + 块的一次 `palloc`+`memcpy` + 两个 list cell。

所以在动发送路径之前，**先把 `parallel_decode_stream_segment_size` 调到小于典型事务大小**（tpcc 要调到 KB 级），否则测出来的是"串行 apply 的速率"，不是发布端的发送能力。

`parallel_decode_stream_min_size` 的帮助文本写的是"Zero means that every transaction is streamed"，但实现是 `if (min_size > segment_size) segment_size = min_size;`——min_size 只能把门槛**抬高**，0 并不等于"全都流式"。这条文档与实现不一致由 peer 会话在改发布端 GUC 时一并对齐。

## 7. 后续（本次不做）

- **worker 侧把非流式事务的帧攒成一个 `PD_BLK_TXN` 块**（设计文档 §5.2 的 `PD_BLK_TXN`）。当前非流式路径仍然是一条变更一条队列消息，walsender 侧只能逐帧持有。把一个事务的帧攒成一条队列消息，可以把**每帧一次 `shm_mq` 往返**摊薄成**每事务一次**——这是非流式路径上剩下的最大的一笔开销。注意：非流式事务的帧本来就要等到 cseq 轮到才放行，所以攒批**不增加延迟**。
- **held 区的背压**：目前 `parallel_decode_held_max_bytes` 只告警、不限流（`parallel_decode.c:57`）。放行跟不上时 held 区会一直涨。加背压要小心与 barrier 的交互（可能死锁），需要单独设计。

