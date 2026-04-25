# DDL异常处理机制设计与分析

## 1. 文档目标与范围

本文聚焦逻辑复制中 `pg_publication_sync` 的 DDL apply 异常处理机制，回答三个关键问题：

1. 机制原理是什么，为什么这样设计。
2. 当前能力边界是什么，在哪些场景会“主动跳过”或“报错退出”。
3. 后续如何扩展，既提升稳定性又避免静默漂移。

本文仅覆盖当前代码路径（`src/backend/replication/logical/worker.c`、`src/backend/tcop/utility.c`、`src/backend/commands/publicationcmds.c`）。

## 2. 总体设计原则

该机制围绕“可靠性优先”建立，核心原则如下：

1. 默认失败安全（Fail Safe）
   - 无法明确归类为“安全可忽略”的异常，默认 `RETHROW`，遵循原生失败语义。

2. 可分类、可解释、可审计
   - 先按 SQLSTATE 和语句类型分类，再统一分发动作。
   - 每条“被忽略/被跳过”的异常都有日志记录，避免无痕吞错。

3. 资源一致性优先于吞错
   - 异常分支先复制错误、清空错误状态并回滚子事务，再做“是否忽略/跳过”判定。
   - 避免出现“错误被吞掉但资源未释放”导致后续段错误。

4. 单条原始语句最小原子单元
   - `Q` 消息中的每个 raw statement 在一个内部子事务里执行。
   - 单条语句内部失败可回滚，避免多目标 DDL 半成功污染会话状态。

## 3. 端到端流程（发布端 -> 订阅端）

### 3.1 发布端：DDL捕获与范围过滤

入口在 `CapturePublicationSyncDDL()`（`src/backend/tcop/utility.c`）。

发布端主要做三件事：

1. 判断是否属于可捕获 DDL（`UtilityStmtShouldCaptureDDL`，`UtilityStmtDDLMask`）。
2. 结合 publication 的 `FOR` 范围和 `WITH (ddl=...)` 做过滤（`PublicationShouldCaptureDDL`、`PublicationAllowsDDLMaskByScope`、`PublicationMatchesRelationScope`）。
3. 对存在风险的语句提前降级处理，例如：
   - `FOR TABLE/FOR TABLES IN SCHEMA` 下 `ALTER SCHEMA` 仅告警并不进入同步。
   - 多目标 `DROP` 混合范围时告警并跳过（避免误回放）。

此外，在 `publicationcmds.c` 的接口层会给配置告警：

1. `FOR TABLE/FOR TABLES IN SCHEMA` 下，仅 `table,index` 有效。
2. `FOR ALL TABLES` 且未包含 `ddl='table'` 时给 NOTICE，提示 DDL/DML 一致性风险。

### 3.2 订阅端：消息分发

`maybe_apply_publication_sync_message()` 根据 `msgtype` 走 `Q/A/D` 三类处理：

1. `Q`: 执行 DDL SQL。
2. `A`: 注册关系进入订阅同步状态。
3. `D`: 删除关系同步映射并停止对应 worker。

只允许 leader/parallel apply worker 执行，tablesync worker 不执行该路径。

## 4. Q消息执行与异常处理内核

入口：`apply_publication_sync_message_q()` -> `execute_publication_sync_sql_command()`。

### 4.1 执行前上下文准备

1. 校验 `pfsyncddlsql`、`pfsyncsearchpath` 非空。
2. 将会话 `search_path` 临时设置为发布端捕获值，保证名称解析语义一致。
3. 若 `ddlmask` 包含 `TABLE`，预解析 `pfsynctargetlist`，用于后续生命周期对齐。

### 4.2 DROP TABLE 特殊预处理

对于 `DROP TABLE` 类语句：

1. 在执行 SQL 前先 stop 对应关系 worker。
2. 预先移除订阅关系映射（`RemoveSubscriptionRel`），规避 relation-drop 与 tablesync 状态冲突导致的崩溃。

### 4.3 子事务执行模型

每个 raw statement 使用 `BeginInternalSubTransaction()` 包裹。

正常路径：

1. parse/analyze/rewrite/plan。
2. `ProcessUtility` 执行。
3. `ReleaseCurrentSubTransaction()` 提交子事务。

异常路径：

1. `CopyErrorData()`。
2. `FlushErrorState()`。
3. `RollbackAndReleaseCurrentSubTransaction()`。
4. 分类异常动作并执行分发。

这个顺序是可靠性关键点：先保证资源和事务上下文已回收，再决定是否继续。

## 5. 异常动作模型（Action）

当前统一动作枚举在 `PublicationSyncDdlErrorAction`：

1. `PUBLICATION_SYNC_DDL_ERROR_RETHROW`
2. `PUBLICATION_SYNC_DDL_ERROR_IGNORE_IDEMPOTENT`
3. `PUBLICATION_SYNC_DDL_ERROR_SKIP_PRECONDITION`
4. `PUBLICATION_SYNC_DDL_ERROR_SKIP_NONCRITICAL`

这是“动作层”抽象，而非“错误来源层”抽象。其意义是：分类器输出的最终结果只能是这四种终态之一。

## 6. 当前分类规则（Classifier）

入口：`publication_sync_classify_ddl_error(const ErrorData *edata, Node *utility_stmt)`。

### 6.1 IGNORE_IDEMPOTENT（幂等重复冲突）

触发条件：

1. duplicate 系列 SQLSTATE：
   - `DUPLICATE_OBJECT`
   - `DUPLICATE_TABLE`
   - `DUPLICATE_SCHEMA`
   - `DUPLICATE_FUNCTION`
   - `DUPLICATE_COLUMN`
   - `DUPLICATE_ALIAS`
   - `DUPLICATE_DATABASE`
   - `DUPLICATE_FILE`
2. `UNIQUE_VIOLATION` 且命中 catalog 索引冲突特征（`schema_name='pg_catalog'` 且 `table_name/constraint_name` 前缀 `pg_`）。

设计意图：

1. 处理多订阅并发回放相同 DDL 的重复创建冲突。
2. 避免 worker 因“已存在”型冲突崩溃循环。

### 6.2 SKIP_PRECONDITION（前置条件缺失）

触发 SQLSTATE：

1. `INVALID_SCHEMA_NAME`
2. `UNDEFINED_TABLE`
3. `UNDEFINED_COLUMN`
4. `UNDEFINED_FUNCTION`
5. `UNDEFINED_OBJECT`
6. `OBJECT_NOT_IN_PREREQUISITE_STATE`

设计意图：

1. 某条 DDL 因环境/顺序缺前置对象时，不阻断其他关系继续 apply。
2. 与“单关系异常不应拖垮全局复制”的原则保持一致。

### 6.3 SKIP_NONCRITICAL（非关键环境差异）

当前实例：

1. `CreateExtensionStmt` 失败时归入该类。

设计意图：

1. 扩展文件、版本、插件环境在订阅端不一致时，避免无限重启。
2. 通过 warning + hint 引导人工修复。

### 6.4 RETHROW（默认）

所有未明确判定为安全可跳过/可忽略的错误，均回抛。

设计意图：

1. 不隐式吞掉未知高风险错误。
2. 保持故障可见性，遵循 PostgreSQL 原生“异常即失败”基线行为。

## 7. 可靠性收益分析

### 7.1 已解决的主要故障类型

1. 多订阅并发执行同一 DDL 的“already exists/duplicate key”崩溃循环。
2. `DROP TABLE` 与 tablesync 状态映射冲突导致的崩溃风险。
3. 关系缺失时 DML 路径直接硬失败导致的持续重启（通过 pause/resume 与 DDL 生命周期协同缓解）。

### 7.2 资源安全改进点

1. 子事务边界明确，错误后资源归属恢复。
2. 快照显式 `PopActiveSnapshot()`。
3. `search_path` 在 `PG_TRY/PG_CATCH` 双路径恢复。

## 8. 当前能力边界

以下为当前实现“明确支持/明确不支持”边界：

### 8.1 支持边界

1. 按 publication 范围与 ddl mask 捕获并分发 DDL。
2. 对可判定的幂等重复冲突进行无害放行。
3. 对前置缺失与非关键环境异常做 best-effort 跳过。
4. `Q/A/D` 三类消息与表生命周期对齐（创建后补状态、删除后清映射停 worker）。

### 8.2 非目标或未覆盖边界

1. 未实现“自动重试”动作：
   - 如 `DEADLOCK_DETECTED`、`LOCK_NOT_AVAILABLE` 仍无统一退避重试策略。
2. `SKIP_NONCRITICAL` 当前仅覆盖 `CREATE EXTENSION`。
3. 分类器以动作为中心，不保留“细粒度原因码”对象（可读但扩展维度有限）。
4. 跳过策略可能引入“可运行但结构漂移”，当前依赖日志与人工修复。
5. 多条 raw statement 之间不保证整体原子性（原子单元是“单条 raw statement”）。

## 9. 后续扩展建议（按优先级）

### P0：增强可观测性（先做）

1. 为每个动作增加统计计数（按 subscription、SQLSTATE、stmt tag 聚合）。
2. 增加系统视图或诊断函数，查询最近 N 条被忽略/跳过的 DDL 异常。
3. 关键日志统一结构化字段：`action/sqlstate/stmttag/subid/pubid`。

### P1：动作层扩展 `RETRY`

1. 新增 `PUBLICATION_SYNC_DDL_ERROR_RETRY`。
2. 对 `40P01`、`55P03` 等瞬时并发异常做有限重试（指数退避 + 最大次数）。
3. 超限后再进入 `RETHROW`，避免无穷重试。

### P1：原因层抽象（Reason + Action）

将当前“动作即分类”升级为两层模型：

1. Reason：`DUPLICATE_IDEMPOTENT`、`PRECONDITION_MISSING`、`NONCRITICAL_ENV`、`TRANSIENT_CONFLICT`、`UNKNOWN`。
2. Action：`IGNORE`、`SKIP`、`RETRY`、`RETHROW`。

收益：

1. 更易扩展策略矩阵。
2. 便于策略配置与可观测面展示。

### P2：策略可配置

1. 提供 subscription 级策略开关，例如：
   - 是否允许 `SKIP_PRECONDITION`。
   - `SKIP_NONCRITICAL` 白名单语句类型。
2. 支持严格模式：任何非幂等异常一律 `RETHROW`。

### P2：漂移修复闭环

1. 为被跳过异常提供“待处理队列”（死信表）与人工回放接口。
2. 提供校验工具比较发布端/订阅端关键对象差异，辅助恢复一致性。

## 10. 建议的测试矩阵

建议将异常处理回归分为四组：

1. 幂等冲突组
   - 多订阅并发 `CREATE SCHEMA/CREATE TABLE`。
   - catalog unique violation（`pg_namespace` 相关）。

2. 前置缺失组
   - 缺 schema/缺 function/缺 column 的 DDL 回放。
   - 验证“单关系跳过不影响其他关系推进”。

3. 非关键环境组
   - `CREATE EXTENSION` 在订阅端缺扩展文件。
   - 验证 worker 不崩溃、日志有明确 hint。

4. 默认失败组
   - 构造未知 SQLSTATE，验证 `RETHROW` 与失败可见性。

## 11. 结论

当前机制已经从“遇错即崩”演进到“可分类容错 + 默认失败安全”的平衡态：

1. 在高并发和环境不完全一致场景下显著降低了崩溃循环概率。
2. 通过动作分层保持了系统可运行性。
3. 仍需通过“可观测性 + 可重试 + 可配置策略”补齐工程化闭环，避免长期静默漂移风险。

