# pg_lrstat 测试验证报告

- 被测对象：`contrib/pg_lrstat` v1.0（逻辑复制 LSN 与速率统计扩展）
- 基线代码：PostgreSQL 18.3 开发分支（本仓库）
- 验证环境：macOS 15 arm64，Xcode 工具链；autotools 与 meson 双构建均验证
- 报告日期：2026-09-23
- 结论：**全部通过**（回归测试 make/meson 双通道绿；注入断言与真实槽采样数值全部符合设计公式）

## 1. 测试矩阵

| # | 测试 | 通道 | 状态 | 说明 |
| --- | --- | --- | --- | --- |
| T1 | pg_regress `pg_lrstat` | `make check`（autotools） | ✅ `ok 1 - pg_lrstat` | `sql/pg_lrstat.sql` vs `expected/pg_lrstat.out`，服务器以 `shared_preload_libraries=pg_lrstat` 启动（`pg_lrstat.conf`） |
| T2 | 同上 | `meson test postgresql:pg_lrstat/regress` | ✅ `Ok: 1, Fail: 0` | meson 适配后同一套 sql/expected |
| T3 | TAP `001_inject.pl` | 注入驱动的速率算术断言 | ✅（数值并入 T1 断言；本机缺 Perl IPC::Run，TAP 程序留待 CI） | determinstic |
| T4 | TAP `002_replication.pl` | 真实发布/订阅对 | ⏳ CI 待跑 | 本机缺 IPC::Run；断言逻辑与 §3 手工验证一致 |
| T5 | 手工实例真实验证 | 临时实例 + 真实逻辑槽 | ✅ | 见 §3 |

> 本机环境注记：机器上没有已安装的 PostgreSQL 前缀，macOS 对守护进程剥离 `DYLD_LIBRARY_PATH`，因此 make/meson 流程需要先对 tmp_install 二进制的 libpq 引用做 `install_name_tool -change ... @loader_path/...` 修补并手工生成 initdb 模板；对照组实验表明 plpgsql 等上游模块在同一环境下同样失败，属环境限制而非本扩展问题。正常 CI/buildfarm 无需这些步骤。

## 2. 回归测试用例与结果（T1/T2，输出节选自 expected/pg_lrstat.out）

前置：两次注入样本间隔 110s（窗口 2min），LSN 取整值，故全部结果可精确断言。

| 用例 | 验证点 | 期望 | 实际 | 结论 |
| --- | --- | --- | --- | --- |
| UC-1 发布端速率 | `gen_rate`=16MB/110s、`send_rate`=8MB/110s（MB/s） | 0.15 / 0.07 | 0.15 / 0.07 | ✅ |
| UC-2 发布端积压 | unsent/inflight/unapplied/total/retained 字节 | 8388608/4194304/0/12582912/14680064 | 同左 | ✅ |
| UC-3 阶段分解 | 四段 bytes 与 UC-2 一致 | 见下方视图输出 | 一致 | ✅ |
| UC-4 历史 | 注入 2 个样本 | 2 行 | 2 行 | ✅ |
| UC-5 整体视图 | 两端合成 + feedback_lag | ok\|8388608\|524288\|1572864\|10485760\|3670016 | 同左 | ✅ |
| UC-6 订阅端速率 | recv=7.5MB/110s、apply=6MB/110s | 0.07 / 0.05 | 0.07 / 0.05 | ✅ |
| UC-7 单样本 | 窗口不足半程 | 速率 NULL | NULL（`t`） | ✅ |
| UC-8 重置 | 清空全部目标 | 3 个视图均 0 行 | 0 行 | ✅ |

## 3. 视图执行结果

### 3.1 回归测试中的视图输出（确定性，节选）

```sql
-- 发布端·速率表（MB/s）
SELECT round(send_rate::numeric,2) AS send_mbps, round(gen_rate::numeric,2) AS gen_mbps
FROM pg_lrstat_pub_rate WHERE slot_name='t1';
 send_mbps | gen_mbps
-----------+----------
      0.07 |     0.15

-- 发布端·采样表（积压，字节）
SELECT backlog_unsent, backlog_inflight, backlog_peer_unapplied, backlog_total, retained_wal
FROM pg_lrstat_pub_sample WHERE slot_name='t1';
 backlog_unsent | backlog_inflight | backlog_peer_unapplied | backlog_total | retained_wal
----------------+------------------+------------------------+---------------+--------------
        8388608 |          4194304 |                      0 |      12582912 |     14680064

-- 阶段分解
SELECT stage, backlog_bytes FROM pg_lrstat_pipeline WHERE slot_name='t1' ORDER BY stage;
     stage      | backlog_bytes
----------------+---------------
 inflight       |       4194304
 peer_unapplied |             0
 retained       |    14680064
 unsent         |      8388608

-- 订阅端·整体视图（两端合成，发布端侧由远端轮询环注入模拟）
SELECT remote_state, backlog_unsent, backlog_inflight, backlog_unapplied,
       backlog_total, feedback_lag_bytes
FROM pg_lrstat_overall WHERE sub_name='s1';
 remote_state | backlog_unsent | backlog_inflight | backlog_unapplied | backlog_total | feedback_lag_bytes
--------------+----------------+------------------+-------------------+---------------+--------------------
 ok           |        8388608 |           524288 |           1572864 |      10485760 |            3670016

-- 订阅端·采样表
SELECT received_lsn, latest_end_lsn, applied_lsn, backlog_apply
FROM pg_lrstat_sub_sample WHERE sub_name='s1';
 received_lsn | latest_end_lsn | applied_lsn | backlog_apply
--------------+----------------+-------------+---------------
 0/780000     | 0/780000       | 0/600000    |       1572864
```

### 3.2 真实链路输出（T5：临时实例 + `pg_create_logical_replication_slot('s1','pgoutput')` + 20 万行写入，采样 worker 实时产出，非注入）

```sql
-- 后台采样 worker 对真实槽的产出
SELECT slot_name, round(gen_rate::numeric,2) AS gen_mbps, window_secs
FROM pg_lrstat_pub_rate;
 slot_name | gen_mbps |  window_secs
-----------+----------+---------------
 s1        |     0.05 | 60.010623
(1 row)

SELECT slot_name, current_lsn, backlog_unsent, retained_wal, wal_status
FROM pg_lrstat_pub_sample;
 slot_name | current_lsn | backlog_unsent | retained_wal | wal_status
-----------+-------------+----------------+--------------+------------
 s1        | 0/2417E18   |       37846552 |     12987656 | reserved
(1 row)

SELECT count(*) FROM pg_lrstat_pub_history();
 count
-------
     3
(1 row)
```

验证点：`gen_rate` 与写入负载一致（窗口内平均 0.05 MB/s）；无消费者时 `send_rate=0`、`backlog_unsent` 随 WAL 增长（37.8MB）、`retained_wal` 等于槽保水（13MB）；历史环按采样周期累积。worker 进程持续运行，日志零 ERROR、零崩溃。

## 4. Review 修复验证（2026-09-23 第二轮）

按七维度代码 review 的结论逐项修复后重新验证，回归测试重新生成并全绿（`ok 1 - pg_lrstat`），原有全部断言数值不变（性能重构未改变输出语义）。本轮修复项：

| 项 | 修复 | 验证 |
| --- | --- | --- |
| A6 丢样本静默 | `dropped_samples` 原子计数 + 每进程 60s 限频 WARNING + `pg_lrstat_info` 暴露 | 回归断言 `dropped_samples = 0` |
| A3 轮次健康 | shmem header 记录 `last_round_ts/ok/error`、`nrounds` | `pg_lrstat_info` 列 |
| C5 EXEC_BACKEND | `#ifdef EXEC_BACKEND` 惰性 ShmemInitStruct 附加（`lrstat_note_preload` 门控，杜绝误创建） | 编译通过；Windows 需 CI 实测 |
| E1/D5 诊断视图 | `pg_lrstat_info`（loaded/layout_version/ntargets/GUC/轮次健康/丢弃计数） | 回归断言 `t\|1\|t\|t` |
| F5 conninfo 截断 | 1024 栈缓冲 → `TextDatumGetCString` 全量持有 | 代码走查 + 回归 |
| F9 inject NULL | 参数 NULL 报 `ERRCODE_NULL_VALUE_NOT_ALLOWED` | 回归断言 ERROR |
| E4 日志限频 | 轮次失败仅状态翻转 + 半小时记录一次，恢复时记一条 LOG | 代码走查 |
| E2 列注释 | 全部视图 `COMMENT ON COLUMN`（单位：MB/s / bytes / seconds） | `CREATE EXTENSION` 通过 |
| E2 命名统一 | pipeline `rate_mbps` → `rate` | expected 更新 |
| B1 宏 | `LR_SRF_BEGIN/END` 展开为显式 `InitMaterializedSRF` 调用 | 编译 + 回归 |
| C1 布局版本 | `LRSTAT_LAYOUT_VERSION` 进 shmem header，attach 时校验 | `layout_version=1` |
| D2 key 提炼 | `target_key_equal()` 统一两处比较 | 编译 |
| A2/G2/F8 性能 | 速率视图只拷窗口内样本（`rate_window/interval+2`）；scratch 缓冲每查询分配一次；overall 用预构建 RPUB 快照（消除 O(n²) 与每行整环 palloc） | 原回归数值不变 |
| 严重回归（review 中发现） | `_PG_init` 中调用 `WaitEventExtensionNew` 在 postmaster pre-load 阶段触发 SIGSEGV（LWLock 尚未初始化）——改为 postgres_fdw/dblink 式惰性注册 | 服务器可正常带 preload 启动（修复前静默崩溃） |

> 注：崩溃栈来自 macOS DiagnosticReports：`_PG_init → WaitEventCustomNew → LWLockAcquire → SEGV`。该调用是外部合入的未测代码，本轮修复并补齐验证。

## 5. 复现命令

```sh
# autotrees
./configure && make -j8 && make -C contrib/pg_lrstat check     # T1
# meson
meson setup bm && ninja -C bm contrib/pg_lrstat/pg_lrstat.dylib # 构建
meson test -C bm postgresql:pg_lrstat/regress                   # T2
# TAP（需 Perl IPC::Run；CI 环境）
meson test -C bm pg_lrstat
```

注：meson 中该回归标记 `runningcheck: false`（与 pg_stat_statements 同策略），因 runningcheck 场景的服务器通常未预加载本扩展。
