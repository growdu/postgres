# commit_sync 相位屏障实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把发布端并行解码的放行机制从「cseq 严格序 + frontier 闸门」换成 design2 的 commit_sync 相位屏障：数据帧 settle 即放行（可跨事务乱序），commit/abort 标记按相位（cseq）保序下发。

**Architecture:** worker 给 commit/abort/prepare 标记帧打独立标记；walsender 把一个事务的帧拆成「数据帧」和「标记帧」两组，数据帧在事务 settle（acked）时即放行，标记帧走 commit_sync 相位屏障（相位 k 的标记在 1..k-1 的标记都已下发且 k 的数据已 settle 时才下发）；删除 frontier 闸门，流分片只受「本事务已 settle」约束。

**Tech Stack:** C（PostgreSQL 后端，src/backend/replication/logical/），Meson 构建，bash + psql 冒烟集群（doc/pa_decode/t/），Python 帧比对工具（pd_frame_tool.py）。

**Spec:** doc/pa_decode/commit_sync_barrier_design.md（已评审，§4/§5 决策已锁定）、doc/pa_decode/design2.md（commit_sync 算法，:71-103）。

## Global Constraints

- 只动发布端：walreader（parallel_walreader.c）、decode worker（parallel_decode_worker.c）、walsender 放行逻辑（parallel_decode.c）、相关 struct（parallel_decode.h）。订阅端源码一行不改，wire 协议字节格式不变（一条变更一个 CopyData）。
- 本机只做功能与正确性验证，不跑吞吐测试（memory: local-dev-no-perf-testing）。
- 订阅端单 stream 串行 apply 是删除 frontier 的安全前提；新增冒烟必须覆盖「相交事务乱序下发 + 订阅端不报 deadlock」。
- 2PC 标签：并行路径发 p/A，订阅端要开 max_prepared_transactions（memory: 2pc-tags-parallel-vs-serial）；本次不改变这个标签。
- 段切分已按条数，parallel_decode_work_mem 单位是裸数字=KB 且当前无效（memory: size-gucs-are-bytes）—— 与本计划无关，不要顺手动它。
- 验证用私有构建目录，不碰 peer 的 build/（memory: private-build-dir）。

## Review Focus

按 spec + memory，最可能咬人的失败模式（按概率排序，每行 owning task 会加测试）：

1. **相交事务乱序下发导致订阅端 deadlock**：删 frontier 后，被发布端行锁串行化的两个相交事务若在订阅端并发 apply，订阅端报发布端没有的 deadlock。前提依赖订阅端串行 apply；冒烟必须显式构造两个相交事务（同一行）乱序下发，断言订阅端无 deadlock。（Task 4 的 smoke + Task 2/3 的帧序断言）
2. **commit/abort 标记相位错乱**：数据帧乱序后，commit/abort 标记若也乱序，订阅端 apply 顺序与发布端不一致，数据错乱。相位屏障必须保证标记严格按 cseq。（Task 3 测试）
3. **流分片的段原子性被破坏**：订阅端一次只接受一个打开的 stream segment，分片帧不能与其它事务的帧交错。`PDQueueSendSegment` 的整段发送是原子性保证；删 frontier 不能让一个 segment 的帧被另一事务的帧插入。（Task 4 测试）
4. **事务先于其数据 settle 就下发 commit**：commit 标记必须在相位 k 的数据帧全部下发后才发，否则订阅端先看到 commit 再看到 changes，apply 失败。屏障的「k 的数据已 settle」条件。（Task 3 测试）
5. **2PC prepare/commit_prepared/rollback_prepared 的标记相位**：2PC 事务的 prepare 与 commit_prepared 是分开的 WAL 记录，相位号可能不同；prepare 走流、commit_prepared 走标记通道，二者相位序要对上。（Task 1/3 测试，two_phase_stream_smoke 不回归）

---

### Task 1: worker 给 commit/abort/prepare 标记帧打独立标记

**Files:**
- Modify: `src/include/replication/parallel_decode.h:729-734`（PDFrameHeader，加标记位）
- Modify: `src/backend/replication/logical/parallel_decode_worker.c`（包 commit_cb/abort_cb/prepare_cb/commit_prepared_cb/rollback_prepared_cb，置 flag；pd_worker_write 据 flag 打标）
- Test: `doc/pa_decode/t/marker_tag_smoke.sh`（新增，用 pd_frame_tool.py 验证标记帧可被识别）

**Interfaces:**
- Consumes: 现有 `pd_worker_write`（:280）、`pd_current_cseq`（:100）、worker 安装回调处（:1256-1313）、`PDQueueSendFrame`（:333）。
- Produces:
  - `PDFrameHeader` 新增 `uint8 flags` 字段，`#define PD_FRAME_MARKER 0x01`（标记帧：该帧是 commit/abort/prepare 的 wire 报文，不是数据）。
  - worker 静态标志 `pd_pending_marker`（bool），由包装回调置位，`pd_worker_write` 发送后清零。
  - 包装回调命名：`pd_worker_commit`、`pd_worker_abort`、`pd_worker_prepare`、`pd_worker_commit_prepared`、`pd_worker_rollback_prepared`，签名与 `LogicalDecodingContext` 对应回调一致，先置 `pd_pending_marker = true` 再调原插件回调。

- [ ] **Step 1: 写失败的冒烟**

新增 `doc/pa_decode/t/marker_tag_smoke.sh`：起发布/订阅集群，开并行解码 + `parallel_decode_trace_file`，跑一个会触发 commit（begin+changes+commit）和一个 2PC（begin_prepared+prepare+commit_prepared）的工作负载，用 `pd_frame_tool.py` 解析 trace，断言：每个事务恰好有一帧带 `PD_FRAME_MARKER`（commit 事务一帧，2PC 事务 prepare 与 commit_prepared 各一帧），且 marker 帧的位置紧邻该事务最后一条数据之后。

Run: `doc/pa_decode/t/marker_tag_smoke.sh`
Expected: FAIL（当前没有 PD_FRAME_MARKER，pd_frame_tool 报「no marker frames」或断言失败）

- [ ] **Step 2: 在 PDFrameHeader 加 flag 字段与 PD_FRAME_MARKER**

在 `parallel_decode.h:729` 的 `PDFrameHeader` 加 `uint8 flags;`（放在 length 之后、保留位之前），并在 `#define PD_FRAME_MARKER 0x01` 处定义宏。`PD_ENT_RELATION` 的 `PDRelationHeader` 同步加同字段（relation 帧也走 pd_worker_write 路径，保持布局一致）。

- [ ] **Step 3: 在 worker 包 commit/abort/prepare 回调**

在 `parallel_decode_worker.c` 安装回调处（:1306 附近，stream_start/stop 包装旁边）保存并包装 `pd_ctx->callbacks.commit_cb`、`abort_cb`、`prepare_cb`、`commit_prepared_cb`、`rollback_prepared_cb`（存在才包，NULL 跳过），包装函数置 `pd_pending_marker = true` 后调原回调。新增静态 bool `pd_pending_marker = false`。

- [ ] **Step 4: pd_worker_write 据 flag 打标**

在 `pd_worker_write`（:280）里，构造帧时（数据走 `PDQueueSendFrame` :333 / 关系走 `PDQueueSendRelation` :327 的分支）把 `pd_pending_marker` 写进 header 的 `flags`，发送后清零。注意只在非 segment、非 relation 的普通数据帧上打标（segment 帧走 :381 单独路径，relation 帧不打标）。

- [ ] **Step 5: 跑通冒烟**

Run: `doc/pa_decode/t/marker_tag_smoke.sh`
Expected: PASS

- [ ] **Step 6: 提交**

```bash
git add src/include/replication/parallel_decode.h src/backend/replication/logical/parallel_decode_worker.c doc/pa_decode/t/marker_tag_smoke.sh
git commit -m "feat: tag commit/abort marker frames in the decode worker"
```

---

### Task 2: walsender 拆分数据帧与标记帧，数据帧 settle 即放行

**Files:**
- Modify: `src/include/replication/parallel_decode.h`（PDPendingTxn 加 `List *markers` 或 `marker` 单帧字段）
- Modify: `src/backend/replication/logical/parallel_decode.c`（`parallel_decode_add_pending_frame` :721、`parallel_decode_release_txns` :1053、`parallel_decode_handle_message` 的 `PD_ENT_FRAME` 分支 :1731）
- Test: `doc/pa_decode/t/disjoint_reorder_smoke.sh`（新增，两个不相交事务乱序下发，订阅端行内容正确）

**Interfaces:**
- Consumes: Task 1 的 `PD_FRAME_MARKER`；现有 `parallel_decode_add_pending_frame`（:721）、`parallel_decode_release_txns`（:1053）、`parallel_decode_ack_txn`（:789）。
- Produces:
  - `PDPendingTxn` 新增 `PDQueuedBlock *marker`（默认 NULL），存放该事务的 commit/abort/prepare 标记帧（一个事务至多一个标记帧；2PC 的 prepare 与 commit_prepared 分属不同 cseq，各自落在对应 cseq 的 pending）。
  - `parallel_decode_release_txns` 改造后：释放 `pending->frames`（数据帧）当 `acked`；`pending->marker` 留在 `parallel_decode_markers`（新结构，按 cseq 有序）等 Task 3 的相位屏障。

- [ ] **Step 1: 写失败的冒烟**

新增 `doc/pa_decode/t/disjoint_reorder_smoke.sh`：两个**不相交**事务（不同表/不同行），让它们在不同 worker 上并发解码，构造「t2 的数据先 settle、t1 的 commit 后到」的时序。断言：订阅端两个事务的行都正确落地（顺序不限），无 deadlock。当前 cseq 闸门下 t2 必须等 t1，所以这是回归当前行为的对照。

Run: `doc/pa_decode/t/disjoint_reorder_smoke.sh`
Expected: PASS（当前行为也正确，但 t2 不乱序；本步先建立基线，Task 3 完成后此 smoke 仍须 PASS 且 t2 乱序）

- [ ] **Step 2: PDPendingTxn 加 marker 字段**

在 `parallel_decode.h` 的 `PDPendingTxn` struct 加 `PDQueuedBlock *marker;`；初始化处（`parallel_decode.c:735`、`797`、`1246`、`800`）设 NULL。

- [ ] **Step 3: add_pending_frame 分流标记帧**

在 `parallel_decode_add_pending_frame`（:721）里，若 `block->flags & PD_FRAME_MARKER`（需把 flags 从 handle_message 传进来，或在 handle_message 的 PD_ENT_FRAME 分支 :1753 直接分流到 `pending->marker`），则不 append 到 `pending->frames`，存到 `pending->marker`。数据帧仍 append 到 `pending->frames`。

- [ ] **Step 4: release_txns 数据帧 settle 即放行**

在 `parallel_decode_release_txns`（:1053）的循环里，把「`pending->frames` 全部 append 到 `parallel_decode_ready`」与「`pending->marker` 转移到 `parallel_decode_markers`」分开：acked 时数据帧立即进 ready，marker 进 markers 等待相位屏障（Task 3 接管）。relation 去重逻辑（:1127）对 marker 帧跳过（marker 不可能是 relation 定义）。

- [ ] **Step 5: 跑通 + 不回归**

Run: `doc/pa_decode/t/disjoint_reorder_smoke.sh`
Expected: PASS（数据已能乱序，marker 暂仍按 cseq 顺序走，行为正确）

Run: `doc/pa_decode/t/frame_diff_smoke.sh`
Expected: PASS（与串行路径的帧内容/顺序一致；顺序差异仅允许「分组方式」和「标记帧相位」，不允许数据错乱）

- [ ] **Step 6: 提交**

```bash
git add src/include/replication/parallel_decode.h src/backend/replication/logical/parallel_decode.c doc/pa_decode/t/disjoint_reorder_smoke.sh
git commit -m "feat: release data frames of a settled transaction, hold markers"
```

---

### Task 3: commit_sync 相位屏障下发标记帧

**Files:**
- Modify: `src/backend/replication/logical/parallel_decode.c`（新结构 `parallel_decode_markers`、`parallel_decode_marker_phase`、改造 `parallel_decode_release_txns` :1053、ack 时驱动屏障）
- Test: `doc/pa_decode/t/marker_order_smoke.sh`（新增，数据乱序但 commit/abort 严格保序）

**Interfaces:**
- Consumes: Task 2 的 `parallel_decode_markers`、`PDPendingTxn.marker`、`parallel_decode_ack_txn`（:789）。
- Produces:
  - 静态 `List *parallel_decode_markers`：元素是 `{ uint64 cseq; PDQueuedBlock *marker; bool data_settled; }`，按 cseq 升序。
  - 静态 `uint64 parallel_decode_marker_phase = 1`：下一个要下发的标记相位。
  - `parallel_decode_release_markers(void)`：当 `parallel_decode_marker_phase` 对应的事务 data_settled 且其 marker 已在表里时，把 marker 进 ready、phase++、循环。

- [ ] **Step 1: 写失败的冒烟**

新增 `doc/pa_decode/t/marker_order_smoke.sh`：构造 t1（大、慢解码）、t2（小、快）**不相交**，t1 先 commit、t2 后 commit。断言：订阅端先收到 t2 的 changes 再收到 t2 的 commit？否——断言「t1 的 commit 先于 t2 的 commit 到达订阅端」（相位保序），同时「t2 的 changes 可先于 t1 的 commit 到达」（数据乱序）。用 trace + pd_frame_tool 验证相位序。

Run: `doc/pa_decode/t/marker_order_smoke.sh`
Expected: FAIL（当前 marker 跟数据一起按 cseq 走，t2 的 changes 不可能先于 t1 的 commit 到达）

- [ ] **Step 2: 建 markers 结构与 release_markers**

在 `parallel_decode.c` 加上述结构与 `parallel_decode_release_markers`。`data_settled` 在该事务 ack 时置位。

- [ ] **Step 3: ack 与 release_txns 驱动屏障**

`parallel_decode_ack_txn`（:789）置 acked 后，调 `parallel_decode_release_markers`。`parallel_decode_release_txns`（:1053）把 marker 转入 `parallel_decode_markers` 时也调 `parallel_decode_release_markers`。`parallel_decode_release_markers` 内：从 phase 起，若 `parallel_decode_markers` 中最小 cseq == phase 且其 data_settled，则 marker 进 ready、phase++；否则停（后续 marker 相位更大，不能越前）。

- [ ] **Step 4: 跑通 + 不回归**

Run: `doc/pa_decode/t/marker_order_smoke.sh`
Expected: PASS

Run: `doc/pa_decode/t/frame_diff_smoke.sh`
Expected: PASS（提交/abort 顺序与串行一致）

Run: `doc/pa_decode/t/stream_order_smoke.sh`
Expected: 需同步更新断言（见 Task 4），此步可能 FAIL —— 在 Task 4 处理

- [ ] **Step 5: 提交**

```bash
git add src/backend/replication/logical/parallel_decode.c doc/pa_decode/t/marker_order_smoke.sh
git commit -m "feat: release commit/abort markers through the commit_sync phase barrier"
```

---

### Task 4: 删除 frontier 闸门与 WARNING，更新 stream_order 冒烟

**Files:**
- Modify: `src/backend/replication/logical/parallel_decode.c`（`parallel_decode_frame_may_go` :964、`parallel_decode_release_held_frames` :1024、`parallel_decode_hold_block` :1004、`PD_ENT_FRAME` 无 cseq 分支 :1753、`:1095` WARNING）
- Modify: `doc/pa_decode/t/stream_order_smoke.sh`（断言改为「数据可越序、commit/abort 保序、订阅端无 deadlock」）
- Test: `doc/pa_decode/t/stream_order_smoke.sh`（改造）+ 相交事务 deadlock 断言

**Interfaces:**
- Consumes: Task 3 的相位屏障；现有 `parallel_decode_hold_block`（:1004）、`parallel_decode_release_held_frames`（:1024）。
- Produces: 无新接口；删除 frontier 相关状态（`parallel_decode_frontier`、`parallel_decode_sent_stream_max`、`parallel_decode_certified_lsn` 的 frontier 用途——certified_lsn 保留给 read_pos，仅移除 frontier 比较）。

- [ ] **Step 1: 改造 stream_order_smoke 断言**

把 `stream_order_smoke.sh` 的核心断言从「publisher holds 流分片等 frontier」改为「publisher 不再因 frontier 持有分片；流分片可在更早事务 commit 前下发；订阅端无 deadlock；相交事务（同行）的两个事务乱序下发后订阅端不报 deadlock（或若报则属于已知风险，需在 trace 标注）」。增加一个相交事务场景：t1 update 同一行、t2 update 同一行，二者并发，断言订阅端 apply 后行内容正确（以发布端串行路径的结果为基线比对）。

Run: `doc/pa_decode/t/stream_order_smoke.sh`
Expected: FAIL（frontier 还在，分片被持有，新断言不成立）

- [ ] **Step 2: 删 frontier 闸门**

`parallel_decode_frame_may_go`（:964）直接返回 true（或删除该函数、`release_held_frames` 里移除对该函数的调用）。`PD_ENT_FRAME` 无 cseq 分支（:1753）的 `parallel_decode_hold_block` + `parallel_decode_release_held_frames` 改为：分片帧到达即进 ready（仍按到达顺序，保证段内帧序），不再等 frontier。

- [ ] **Step 3: 删 WARNING 与 sent_stream_max**

删除 `:1095-1113` 的 `parallel_decode_sent_stream_max` WARNING 块（design §5 决策）。删除 `parallel_decode_sent_stream_max` 的声明/更新/清理（:988、:2953）。`parallel_decode_note_stream_frame` 若仅服务该 WARNING 则一并删除；若服务监控则保留。

- [ ] **Step 4: 跑通 + 全量冒烟**

Run: `doc/pa_decode/t/stream_order_smoke.sh`
Expected: PASS

Run: `doc/pa_decode/t/frame_diff_smoke.sh` 与 `doc/pa_decode/t/segment_changes_smoke.sh`
Expected: PASS（段原子性不破：一个 segment 的帧不被另一事务帧插入）

Run: `doc/pa_decode/t/subxact_abort_smoke.sh`、`doc/pa_decode/t/two_phase_stream_smoke.sh`
Expected: PASS（子事务中止、2PC 流不回归）

- [ ] **Step 5: 提交**

```bash
git add src/backend/replication/logical/parallel_decode.c doc/pa_decode/t/stream_order_smoke.sh
git commit -m "feat: drop the release frontier, stream segments follow their own transaction"
```

---

### Task 5: 监控视图字段语义对齐 + 文档

**Files:**
- Modify: `src/backend/catalog/system_views.sql`（pg_parallel_decode_commit_lag 视图定义）
- Modify: `doc/src/sgml/config.sgml`（GUC/视图文档，cseq_allocated/cseq_released 语义改为相位号）
- Test: `doc/pa_decode/t/monitoring_smoke.sh`（不回归，字段名保留）

**Interfaces:**
- Consumes: 现有 `parallel_decode_ctl->cseq_allocated`/`cseq_released`（parallel_decode.h:306-308）。
- Produces: 无新接口；视图字段名不变，语义注释更新。

- [ ] **Step 1: 跑监控冒烟基线**

Run: `doc/pa_decode/t/monitoring_smoke.sh`
Expected: PASS（记录基线）

- [ ] **Step 2: 更新视图与 GUC 文档语义**

在 `system_views.sql` 的 `pg_parallel_decode_commit_lag` 列注释和 `config.sgme` 的相应 GUC 文档里，把 `cseq_allocated`/`cseq_released` 的描述从「commit sequence number」改为「phase number（相位号，沿用 cseq 分配）」；说明 committed 事务的数据可乱序下发、标记按相位保序。

- [ ] **Step 3: 不回归**

Run: `doc/pa_decode/t/monitoring_smoke.sh`
Expected: PASS（字段名保留，输出结构不变）

- [ ] **Step 4: 提交**

```bash
git add src/backend/catalog/system_views.sql doc/src/sgml/config.sgml
git commit -m "doc: rename the meaning of cseq fields to phase numbers"
```

---

## 全量验证（所有 Task 完成后）

- [ ] 跑全套冒烟：`doc/pa_decode/t/` 下全部 `*_smoke.sh`，重点 `frame_diff_smoke.sh`、`stream_order_smoke.sh`、`marker_order_smoke.sh`、`disjoint_reorder_smoke.sh`、`two_phase_stream_smoke.sh`、`subxact_abort_smoke.sh`、`monitoring_smoke.sh`。
- [ ] 私有构建目录编译 + 安装，冒烟跑安装树（memory: smoke-uses-install-tree）。
- [ ] 相交事务 + 不相交事务两类场景，订阅端行内容以串行路径为基线比对，无 deadlock、无错乱。
- [ ] 本机只做功能与正确性验证，吞吐数字标注待实测（memory: local-dev-no-perf-testing）。
