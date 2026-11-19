# 发布端并行解码下 `copy_data = true` 的初始同步永久卡在 `srsubstate = 's'`

本文记录 2026-11-19 定位的一个缺陷：开启发布端并行逻辑解码后，订阅的**初始表同步永远不会完成**——表停在 `SYNCDONE`（`'s'`），不会提升为 `READY`（`'r'`），订阅也就永远不 ready。

本文只描述发布端一侧的行为，订阅端代码一行未改（AC-9）。

## 1. 症状与复现

复现脚本：`doc/pa_decode/t/copy_data_smoke.sh`（`copy_data = true`）。

| 场景 | 结果 |
|---|---|
| `max_parallel_decode_workers = 0`（串行发布端） | 1 秒后 `srsubstate = 'r'`，正常完成 |
| `= 4`，`streaming = 'parallel'` | 90 秒后仍是 `'s'` |
| `= 4`，`streaming = 'on'` | 同样卡住 |

`streaming` 取 `parallel` 还是 `on` 没有区别，所以变量是**并行解码路径本身**，不是流式模式。

**决定性实验**：卡住期间在发布端插入一行，`srsubstate` 立刻从 `'s'` 变成 `'r'`，该行也到达订阅端。也就是说，只差"发布端有东西可解码"这一个条件。

## 2. 根因

订阅端 apply worker 用 `'w'` 帧头与 `'k'` keepalive 的 `end_lsn` 推进 `last_received`（`worker.c:3594-3612`）；`SYNCDONE → READY` 的提升只发生在 apply worker 的 `process_syncing_tables_for_apply()` 中，且要求 `last_received >= rstate->lsn`（`tablesync.c:474`，由 `worker.c:3639` 的 `process_syncing_tables(last_received)` 触发）。

表的同步点（`pg_subscription_rel.srsublsn`，即 COPY 结束时发布端的 WAL 位置）**必然晚于**主会话的起点 LSN：主会话先起，tablesync worker 之后才创建自己的临时槽（建槽会写一条 running-xacts 记录），COPY 结束时的 WAL 位置自然更靠后。实测两轮：

| 轮次 | 主会话起点 LSN | 表的同步点 `srsublsn` |
|---|---|---|
| 1 | `0/151E090` | `0/151E0C8` |
| 2 | `0/1560B48` | `0/1560B80` |

因此**只要发布端报出的位置停住不动，表就永远提升不了**。并行路径在发布端空闲时正是如此——三个缺陷叠加：

### D1：walsender 的 `sentPtr` 冻结在会话起点

并行分支中 `sentPtr = ParallelDecodePosition()` 位于 `XLogSendLogical()` 的末尾（`walsender.c:3702`），而在到达该行之前先调用 `pd_walsender_write_frames()` → `ParallelDecodeNextFrame()`。后者是一个**内部无限循环**的等待（`parallel_decode.c:1417` 的 `for(;;)`：drain 控制通道 → 取就绪帧 → 停滞检测 → idle 回调 → `WaitLatch(100ms)`），只在有帧就绪或会话收尾时才返回。

于是发布端空闲时 `XLogSendLogical()` 根本不返回，`sentPtr` 永远保持 `StartLogicalReplication()` 中写死的 `confirmed_flush`（`walsender.c:1591`）。

旁证：

- `pg_stat_replication.sent_lsn` 停在会话起点的 `restart_lsn`（`walsender.c:1595` 写入的初值），不随 WAL 前进；
- walsender 的 `wait_event` 是 `Extension`——正是 `ParallelDecodeNextFrame()` 里那个 `PG_WAIT_EXTENSION` 的 `WaitLatch`。

附带一点：`ParallelDecodeNextFrame()` 自己的注释写的是"等待区间内没有帧就返回 false，让调用方回到主循环"，实现并未如此。

**修法**：idle 回调 `pd_walsender_idle()` 每 100ms 被调用一次，在其中把 `sentPtr` 与 `MyWalSnd->sentPtr` 更新为 `ParallelDecodePosition()`。这条已经落地。

### D2：放行前沿的兜底值，正是设计文档禁止的那一个

`ParallelDecodeFrontier()`（`parallel_decode.c:877`）在 walreader 尚未报告过任何事务时，兜底返回 `parallel_decode_ctl->start_lsn`（`:901`）。而 `parallel_decode_certified_lsn` 只在**事务结束**时才更新（`PD_ENT_CSEQ`，`parallel_decode.c:716`），发布端空闲 ⇒ 它永远 `Invalid` ⇒ 前沿被永久冻结在会话起点。

这与设计文档的口径直接冲突（`design.md:1446`、`design.md:1903`，以及 v0.9 变更记录第 (3) 条）：

> **不得用"会话开始时的槽 `confirmed_flush`"兜底**……集合为空是稳态常态而非例外；用会话起点的 `confirmed_flush` 兜底会把前沿**永久冻结在会话起点**
>
> 集合为空时唯一合法取值是 **walreader 认证过的读取位置**（`PDCseqInfo.read_lsn`）

即使只修 D1，钳制也会把位置拉回会话起点（`0/1560B48 < 0/1560B80`），表照样卡住。**D1 与 D2 必须一起修。**

**D2 的真正机理（本轮定位）**：设计文档要的"walreader 认证过的读取位置"在实现里没有载体。`PDCseqInfo.read_lsn` 只在**事务结束**时才随 `PD_ENT_CSEQ` 下发——发布端空闲 ⇒ 没有任何事务结束 ⇒ 认证位置永远不前进。

为此新增了控制通道消息 `PD_ENT_READ_POS`（walreader → walsender），携带 walreader 的读取位置。但**第一版实现只在两处上报它**：

1. 每读满 `PD_POSITION_MARK_RECORDS`（64）条记录一次（`parallel_walreader.c:2224`）；
2. 主循环里 `XLogReadRecord()` 返回 `NULL`（"WAL 暂时读完"）时（`:2089`）。

而并行路径的 walreader **等新 WAL 的地方是页读回调 `pd_walreader_read_page()`**（`parallel_walreader.c:311`）：它在回调里一直等到 WAL 到齐才返回。发布端空闲 ⇒ `read_upto` 不动 ⇒ walreader 永远卡在回调里 ⇒ 两个上报点都到不了 ⇒ `parallel_decode_walreader_pos` 恒为 `0/0` ⇒ 前沿一直兜底到会话起点。

串行路径没有这个问题：它等 WAL 的地方是 `WalSndWaitForWal()`，那里会发追平 keepalive。

**认证为什么成立**：`PD_ENT_READ_POS` 与 `PD_ENT_CSEQ` 走**同一条 FIFO 控制通道**，walreader 先发"事务结束"消息再发"读取位置"消息，因此 walsender 读到位置时，此前结束的事务它都已知道；放行前沿取 `min(认证读取位置, 最早未放行事务的结束记录起点)`，不会越过任何未送达的事务。

### D3：并行路径缺少"追平用"的 keepalive，且判据写反

串行路径在 `WalSndWaitForWal()` 中，只要订阅端尚未确认位置就发 keepalive（`walsender.c:2016`；串行逻辑解码经 `logical_read_xlog_page()` → `WalSndWaitForWal()` 到达该处）。并行路径的等待循环里只有 30 秒一次、由 `WalSndKeepaliveIfNecessary()` 触发的 keepalive。

补上追平 keepalive 的第一版实现里，节流判据写反了：

```c
/* 错误：PG 的 TimestampDifferenceExceeds(start, stop, msec) 算的是 stop - start */
TimestampDifferenceExceeds(GetCurrentTimestamp(), pd_last_catchup_keepalive, 1000)
```

差值恒为负 ⇒ **keepalive 一次都没发出去**（日志里每次都是 "keepalive skipped"，"keepalive queued" 零次；`strace` 证实 walsender 只往 fd 2 写日志，从不往客户端 socket 写）。

正确写法是 `TimestampDifferenceExceeds(pd_last_catchup_keepalive, GetCurrentTimestamp(), PD_CATCHUP_KEEPALIVE_INTERVAL_MS)`。

D2 与 D3 叠加的后果：即使修了 D2，前沿放开后位置能前进，但 keepalive 仍然一次都不发 ⇒ 订阅端 `last_received` 依然不动。**两者必须一起修。**

## 3. 证据

修之前的发布端日志（加临时 `PDDEBUG` 埋点）：

```
PDDEBUG idle callback: position=0/151E0C8 frontier=0/151E090 walreader_pos=0/0
PDDEBUG keepalive skipped: flush=0/0 write=0/0 sent=0/151E0C8 ping=0
```

读法：worker 已经解到 `0/151E0C8`（并行路径的位置 `position` 正常），但**放行前沿** `frontier` 还停在会话起点 `0/151E090`（D2：认证读取位置从未上报，兜底到 `start_lsn`），**认证读取位置** `walreader_pos` 是 `0/0`（同 D2）；而 keepalive 因为 D3 的判据写反，每次都被跳过。

`sentPtr`（`0/151E0C8`）确实已经被 idle 回调刷新了——D1 已修好，且**正是 D1 修好之后 D2/D3 才暴露出来**：位置不再冻结，但因为前沿被钳在会话起点、keepalive 又不发，订阅端仍然收不到任何东西。

发布端开启 `parallel_decode_trace_file` 后，卡住的那个会话在 25 秒内**只发出一条 keepalive**：

```
session pid 2475479: 1 keepalive(s), 0 frame(s)
    keepalive end_lsn = 0/1560B48  request_reply=0
```

而该表的同步点是 `0/1560B80`——keepalive 报出的位置比同步点小 `0x38`，订阅端的 `last_received` 永远追不上。

对应时刻发布端的状态：

```
select application_name, sent_lsn, write_lsn from pg_stat_replication;
 pd_sub3 | 0/1560B10 |            -- sent_lsn 冻结在会话起点的 restart_lsn
select slot_name, restart_lsn, confirmed_flush_lsn from pg_replication_slots;
 pd_sub3 | 0/1560B10 | 0/1560B48   -- 会话起点
select srrelid, srsubstate, srsublsn from pg_subscription_rel;
 pd_t3   | s         | 0/1560B80   -- 同步点，永远够不到
```

`pg_waldump` 显示该区间内**只有 running-xacts 记录**（`0/151E058` / `0/151E090` / `0/151E0C8`），没有任何用户事务——即"发布端空闲"时发布端能拿出来的全部内容。

## 4. 修法（已落地）

1. **D1**：idle 回调 `pd_walsender_idle()` 里把 `sentPtr` 与 `MyWalSnd->sentPtr` 更新为 `ParallelDecodePosition()`。
2. **D2**：新增控制通道消息 `PD_ENT_READ_POS`，并让 walreader 在**页读回调的等待分支**里上报认证读取位置——即"我读到这里了，且在此之前结束的每个事务都已上报"。这一步是并行路径与串行路径 `WalSndWaitForWal()` 的等价物。认证位置的下发点收拢为 `pd_walreader_publish_position()`：同时发 `PD_ENT_MARKER`（给 worker）与 `PD_ENT_READ_POS`（给 walsender），三处调用点（每 64 条记录、WAL 暂时读完、等待新 WAL）统一走它。`PD_ENT_MARKER` 按位置去重（首次一定发），否则等待循环每 100ms 就会往每个 worker 队列里灌一条重复 marker。
3. **D3**：补上追平 keepalive，节流 1 秒一次，判据用正确的参数顺序。

### 安全性论证（为什么空闲时上报位置是安全的）

- **未结束的事务**：其提交记录必然在读取位置之后，认证读取位置不会越过它 → 发布端的跳过判据（`SnapBuildXactNeedsSkip()` 比的是结束记录**起点**）不会跳过它。
- **已结束但未放行的事务**：其 `PD_ENT_CSEQ` 已在同一轮里先入队 → walsender 读到 `PD_ENT_READ_POS` 时它已在放行前沿的集合里 → 前沿被它的结束记录起点钉住 → `confirmed_flush` 不会越过它。
- **已放行的事务**：前沿放开到认证读取位置，位置越过它们是应该的。

## 5. 验证结果

`doc/pa_decode/t/copy_data_smoke.sh`（本机功能与正确性验证，非性能测试）：

| 场景 | 修复前 | 修复后 |
|---|---|---|
| `WORKERS=4`（并行路径），`copy_data = true` | 90 秒后仍是 `'s'` | **2 秒**到 `'r'`，后续写入的变更也正常到达 |
| `WORKERS=0`（串行路径） | 1 秒到 `'r'` | 1 秒到 `'r'`（未受影响） |

`keepalive_smoke.sh`（发布端空闲 20 秒）：trace 里的 keepalive 帧 6 个 → 通过。
`slot_watermark_smoke.sh`：槽的水位正常推进 → 通过。

## 6. 相邻问题：停滞检测的判据

另一轮日志中出现过：

```
错误:  the parallel decode session stalled
详细信息:  Decode worker 0 made no progress for 123 seconds.
提示:  The session was waiting for a transaction to be released to the subscriber.
```

它出现在表卡死约 3 分钟之后（发布端此时写了一条 checkpoint，walreader 读到了而 worker 仍停在旧位置），与本次根因同源。`parallel_decode_check_stall()` 的判据把"worker 已追上 walreader"与"worker 卡住"混在了一起，宜一并复核。

### 复核结论（已修）

原来的判据**遍历全部 worker**：只要有任意一个 worker 落后于 walreader 的读取位置、且超过 `parallel_decode_barrier_timeout` 没有上报，就报卡死。这会把两件事混在一起：

- **owner 真的卡住**——会话确实在等它，必须报；
- **owner 之外的 worker 落后**——它们可能只是在等新 WAL。发布端空闲时它们的解码位置天然落后于 walreader 的读取位置（落后量约为一个 marker 的距离），而且在发布端空闲期间收不到新的 marker，也就不会再上报。

现在判据只看**待放行队列队头事务的拥有者**：会话就是在等它，检测能力不变，而"待放行事务 + 空闲发布端"不再误杀会话。

验证见 `doc/pa_decode/t/pending_area_smoke.sh`（订阅端 `streaming = 'off'`，发布端 `parallel_decode_barrier_timeout = 5`，写一个 20 万行的事务）：告警出现、会话没有被误杀、事务耗时超过超时（7 秒 > 5 秒），行数与校验和一致。

> 注：把判据改回旧写法重跑同一个脚本，结果**同样通过**——本次卡死的主因是上面三处缺陷，停滞检测的收窄是消除另一类误报，不是这个 bug 的必要条件。
