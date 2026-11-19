# 发布端并行解码：worker 反复启停、`detached` 报错、以及空闲时位置不前进

本文记录 2026-11-19 定位的三个问题，都在发布端一侧；订阅端代码一行未改（AC-9）。

| 编号 | 现象 | 根因位置 |
|---|---|---|
| A | 每张表同步一次，发布端就起一轮 walreader + N 个 decode worker，几毫秒后全部停止 | 订阅端的表同步 catchup 连接（上游 `tablesync.c`），每个表一条逻辑复制连接 |
| B | walreader 报 `could not send a parallel decode payload: detached`，退出码 1 | `ParallelDecodeSessionStop()` 的拆解顺序反了 |
| C | 发布端空闲时，会话位置不前进；表同步停在 `'s'`，直到发布端写入新 WAL 才提升为 `'r'` | walreader 等 WAL 期间不上报读取位置（C-1）+ 追平 keepalive 的判据参数顺序反了，一次都没发出去（C-2） |

C 是功能性的：A 是"看起来很吵"，B 是错误日志。

---

## A. worker 反复启停的来源：表同步的 catchup 连接

订阅端的表同步 worker 走的是两步（上游 `tablesync.c:1686-1710`）：

1. `start_table_sync()`：用一条**普通 SQL 连接**做 `COPY <table> TO STDOUT`，把表内容搬过去——这条连接不是复制连接，发布端不做解码；
2. `walrcv_startstreaming()`：再开一条**逻辑复制连接**做 catchup，追上主 apply 连接。

第二步的连接参数由 `set_stream_options()`（`worker.c:4349`）生成，它用的是**订阅自己的 streaming 模式**。订阅是 `streaming = 'parallel'`，所以这条 catchup 连接请求的也是 `streaming 'parallel'`，发布端于是为它启动一整套并行会话：一个 walreader + `max_parallel_decode_workers` 个 decode worker。

catchup 连接只活几毫秒：订阅端到达同步点后立刻 `walrcv_endstreaming()`（`tablesync.c:322`），发布端的 walsender 随即退出，整套并行会话被拆掉。

于是**每张表一轮启停**。实测（3 张表，`copy_data = true`，`streaming = 'parallel'`）：

```
03:09:41.273  parallel decode walreader started                <- apply 连接
03:09:41.274  parallel decode worker 0..3 started
03:09:41.303  parallel decode walreader started                <- 表 pd_t2 的 catchup
03:09:41.303  parallel decode worker 0 started
03:09:41.303  parallel decode worker 0 stopped
...
03:09:41.310  parallel decode walreader started                <- 表 pd_t1 的 catchup
03:09:41.331  parallel decode walreader started                <- 表 pd_t3 的 catchup
03:09:41.433  parallel decode worker 0..3 stopped
```

这是上游表同步协议决定的，发布端无法从连接本身区分"catchup 连接"和"apply 连接"（两者都是 `START_REPLICATION ... LOGICAL`，只是槽名不同：catchup 用的是临时槽 `pg_<subid>_sync_<relid>_<lsn>`）。

**结论**：worker 的启停本身不是缺陷，但每个表同步都要起一整套并行会话，代价与噪音都不小；而且这套短命会话正是 B 的触发现场。

---

## B. `could not send a parallel decode payload: detached`

### 证据

同一个 3 表的测试里，3 个 catchup 会话有 2 个报了这个错：

```
03:09:41.303 [3432934] parallel decode worker 0 started
03:09:41.303 [3432934] parallel decode worker 0 stopped
03:09:41.304 [3432933] 错误:  could not send a parallel decode payload: detached
03:09:41.305 [3432933] 日志:  后台工作进程 "parallel decode walreader" (PID 3432933) 已退出, 退出代码 1
```

worker 0 在同一毫秒里"启动→停止"，紧接着 walreader 报 `detached` 并以退出码 1 结束。

### 根因：拆解顺序反了

`ParallelDecodeSessionStop()`（`parallel_decode.c:2197`）：

```c
	parallel_decode_ctl->shutdown = 1;

	/* 先 terminate decode worker，再 terminate walreader */
	for (i = 0; i < parallel_decode_num_workers; i++) { ... TerminateBackgroundWorker ... }
	if (parallel_decode_walreader_handle) { ... TerminateBackgroundWorker ... }
```

注释写的是"先停 worker，因为 walreader 还在喂 worker"。但**队列的方向反了**：

```
walreader  --(WAL 队列)-->  worker i
worker i   --(输出队列)-->  walsender
```

walreader 是 worker 队列的**发送方**。worker 一退出就 `shm_mq_detach()` 它的队列端，`shm_mq_sendv()` 随即返回 `SHM_MQ_DETACHED`（`shm_mq.c:949`）。而 `pd_queue_send_chunked()`（`parallel_decode_msg.c:345`）把 `DETACHED` 一律当成错误：

```c
		if (res != SHM_MQ_SUCCESS)
			ereport(ERROR,
					(errcode(ERRCODE_IO_ERROR),
					 errmsg("could not send a parallel decode payload: %s",
							pd_mq_result_name(res))));
```

于是 walreader 报错退出（退出码 1），发布端日志就是这一句。

### 相关的第二个隐患

> 更正：本树的 `shm_mq_result` 只有 `SHM_MQ_SUCCESS` / `SHM_MQ_WOULD_BLOCK` / `SHM_MQ_DETACHED` 三个值（`src/include/storage/shm_mq.h:38-40`），**没有 `SHM_MQ_INTERRUPTED`**。所以"发送侧会看到 interrupted"这句不成立：被信号打断的阻塞发送是在 `shm_mq_sendv()` 内部直接 `ereport(ERROR)`，不走返回值。拆除期真正会遇到的返回值只有 `SHM_MQ_DETACHED`。

### 建议的修法

1. **拆解顺序反过来**：先 terminate walreader（它是发送方），等它退出，再 terminate worker。`shutdown` 标志已经先置了，walreader 会自己停；`TerminateBackgroundWorker` 只是兜底。
2. **发送路径把"会话结束"与"错误"分开**：`SHM_MQ_DETACHED` 表示会话正在收尾，不是错误。做法是发送侧记住"这次失败是因为队列被 detach"（`PDQueueDetached()`），参与者在**自己确实该停了**的前提下把错误降级成干净退出。
3. **发送前先看 shutdown 标志**，尽量不往已经没人接收的队列里发。

以上三条已实现，见 `doc/pa_decode/stream_segment_order.md` §9（单独一个提交，与 A 分开）。

---

## C. 空闲时位置不前进（功能性问题）

### 现象

`copy_data = true` 的订阅，表同步停在 `'s'`（SYNCDONE）不提升为 `'r'`。串行发布端（`max_parallel_decode_workers = 0`）1 秒就绪，并行发布端要等到发布端写入新 WAL。

决定性实验（发布端空闲 vs. 每秒插一行）：

```
== phase 1: 发布端不写 WAL
   1s: s
   2s: s
  ...
  45s: s
   -> 45 秒后仍未提升
== phase 2: 每秒插一行
   1s: r          <- 第一次插入之后 1 秒内就提升了
```

即：**表能不能提升，完全取决于发布端有没有写 WAL**。

### 根因：放行前沿在空闲时被冻结

会话位置的钳制上限是放行前沿 F。`ParallelDecodeFrontier()`（`parallel_decode.c:910`）的取值顺序是：

1. 待放行事务集合非空 → 取集合里最小的结束记录起点；
2. 否则取 `parallel_decode_walreader_pos`（walreader 认证过的读取位置）；
3. 再否则取 `parallel_decode_certified_lsn`（只在事务结束时才更新）；
4. 最后兜底取会话开始时的槽 `confirmed_flush`（设计文档明令禁止的兜底）。

第 2 步的值**从来不到**。walreader 只在两处上报读取位置：

* 每读满 `PD_POSITION_MARK_RECORDS`（64）条记录一次（`parallel_walreader.c:2224`）；
* 主循环里 `XLogReadRecord()` 返回 NULL、即"WAL 暂时读完"时（`parallel_walreader.c:2089`）。

而"暂时读完"这个分支**在并行路径下几乎到不了**：walreader 等新 WAL 的地方是页读回调 `pd_walreader_read_page()`（`parallel_walreader.c:311`），它在回调里一直等到需要的 WAL 到齐才返回：

```c
		if (state->currTLI == currTLI)
		{
			if (targetPagePtr + reqLen <= read_upto) { ... break; }
			/* WAL 还没到，就在这里等 */
			pd_walreader_flush_queues();
			rc = WaitLatch(MyLatch, ..., 100L, pd_wait_event);
			continue;
		}
```

发布端空闲 → `read_upto` 不动 → walreader 永远卡在这个回调里 → 既不到 64 条记录的下一次上报，也到不了"WAL 暂时读完"那个分支。

于是 walsender 手上的 `parallel_decode_walreader_pos` 一直是 0：

```
PDDEBUG idle callback: position=0/1504960 frontier=0/1504928 walreader_pos=0/0
PDDEBUG idle callback: position=0/1504A80 frontier=0/1504A48 walreader_pos=0/0
```

* `position` 是 worker 上报的解码位置（`PD_ENT_POSITION`），会前进；
* `frontier` 是钳制上限，**停在会话起点** `0/1504928`；
* `walreader_pos` 恒为 0，说明 walsender 从头到尾没收到过一条 `PD_ENT_READ_POS`。

keepalive 的 `end_lsn` 要经 `ParallelDecodeClampLSN()` 钳到前沿，于是 keepalive 报出去的是**会话起点**，订阅端的 `last_received` 永远追不上表的同步点，`'s' → 'r'` 的提升（`tablesync.c:474`，要求 `last_received >= rstate->lsn`）也就永远不发生。

只有当发布端写入新 WAL 时，才会有事务结束 → `PD_ENT_CSEQ` → `certified_lsn` 前进 → 前沿短暂前进 → 订阅端追上来 → 提升发生。这就是 phase 2 里 1 秒内变成 `'r'` 的原因（那一插正好是一个事务，前沿被 CSEQ 推到了同步点之后）。

### C-2：追平 keepalive 一次都没发出去（判据参数顺序反了）

即使前沿修好，keepalive 本身也发不出去。`pd_walsender_idle()`（`walsender.c:3565`）里的判据是：

```c
		TimestampDifferenceExceeds(GetCurrentTimestamp(),
								   pd_last_catchup_keepalive,
								   PD_CATCHUP_KEEPALIVE_INTERVAL_MS)
```

而 PG 的语义是 `TimestampDifferenceExceeds(start_time, stop_time, msec)` 计算 **`stop_time - start_time`**（`src/backend/utils/adt/timestamp.c:1790`）：

```c
	TimestampTz diff = stop_time - start_time;
	return (diff >= msec * INT64CONST(1000));
```

这里传的是 `(now, last)`，差值恒为负 → 判据恒不成立 → **追平 keepalive 一次都没发出去**。串行路径在 `WalSndWaitForWal()`（`walsender.c:2016`）里发追平 keepalive，并行路径没有这个等价物，于是这条唯一能让空闲会话前进的路也是断的。

### 与设计文档的关系

设计文档 §7.4（`design.md:1444`、`:1903`）把"集合为空"明确定义为**稳态常态**（发布端空闲），并规定此时

> F := **walreader 认证过的读取位置**，认证条件是"walreader 已读到的全部结束记录都已放行"，该认证必须**与映射一起下发**。

也就是说，设计要求的就是"walreader 认证过的读取位置"，而实现里这条值在空闲时**根本没有上报**——`PD_ENT_READ_POS` 只在处理记录时上报，walreader 等 WAL 期间不上报。设计里 `PD_ENT_MARKER` 的说明（`design.md:744`）也说它应当在"WAL 暂时读完时"下发，而并行路径的页读回调把"WAL 暂时读完"这个状态吃掉了。

### 建议的修法

在 `pd_walreader_read_page()` 的等待分支里，把"我读到这里了"告诉 worker 和 walsender——即串行路径 `WalSndWaitForWal()`（`walsender.c:2016`）在等 WAL 时发追平 keepalive 的等价物：

```c
			/* WAL 还没到：先把已经读到的位置告诉 worker 和 walsender */
			pd_walreader_send_marker(pd_read_lsn);
			pd_walreader_send_read_pos(pd_read_lsn);
			pd_walreader_flush_queues();
```

`pd_walreader_send_read_pos()` 自带"位置没变就不发"的去重（`parallel_walreader.c:846`），所以不会刷屏；但要注意它只在位置**变化**时才发，因此首次进入空闲状态时必须真的发出去一次（`last_sent` 的初值是无效 LSN，第一次调用会发）。

另外要让 `pd_walreader_send_marker()` 的节流不要把这次上报吃掉（marker 每 64 条记录才发一次，worker 侧靠它知道"没有落下任何记录"）。

改完之后，空闲的发布端应当能：

* 每秒一次追平 keepalive（`PD_CATCHUP_KEEPALIVE_INTERVAL_MS = 1000`，`walsender.c:3514`），报出的位置是 walreader 认证过的读取位置——**前提是 C-2 的判据参数顺序同时改正**；
* 订阅端的 `last_received` 跟着前进；
* 表同步在 1 秒左右从 `'s'` 提升为 `'r'`，不再依赖发布端是否写 WAL。

### 安全性的补充论证

在页读回调的等待分支里上报 `pd_read_lsn` 是安全的：

* `pd_read_lsn` 是**最后一条已处理完记录**的 `next_lsn`；它之前结束的每个事务，其 `PD_ENT_CSEQ` 消息都已先入队（`pd_walreader_send_cseq()` 在处理该记录的同一轮里调用），控制通道 FIFO 保证 walsender 读到 READ_POS 时这些 CSEQ 都在它前面；
* 尚未提交（还在进行中）的事务，其**提交记录**必然在读取位置之后，因此 `confirmed_flush <= F < 该事务的提交记录起点`，发布端的跳过判据不会把它跳过；
* 放行前沿取 `min(READ_POS, 最早未放行事务的提交记录起点)`，集合非空时以集合为准，所以上报 READ_POS 不会让前沿越过任何"已提交但未放行"的事务。

### 还要注意

`pd_walreader_send_marker()` 没有"位置没变就不发"的去重，页读回调的等待循环每 100ms 转一圈，会往每个 worker 队列里灌重复 marker，需要按位置去重。

### 顺带

停滞检测的判据（`parallel_decode.c:1434`）依赖"worker 的位置落后于 walreader 的读取位置"。C 修好之后这个比较才有意义（现在 `walreader_pos` 恒为 0，比较永远不成立）。
