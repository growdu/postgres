下面继续扩展成一版**详细设计文档**，在前面的概要设计基础上，进一步补充：

* catalog 与系统表变更
* 主要模块设计
* 关键函数与调用链
* 发布端/订阅端状态流转
* 错误处理与幂等
* 测试矩阵
* 实施拆分建议

---

# PostgreSQL 逻辑复制支持 DDL 详细设计文档

## 1. 设计范围与阶段目标

## 1.1 一期目标

一期聚焦于在 PostgreSQL 现有逻辑复制框架中支持以下 DDL 的自动同步：

* `CREATE TABLE`
* `ALTER TABLE` 的安全子集
* `DROP TABLE`
* `CREATE INDEX`
* `ALTER INDEX` 的安全子集
* `DROP INDEX`

并满足以下要求：

1. 用户正常执行 DDL，无需手工调用函数；
2. 发布端自动捕获；
3. 发布端将 DDL 事件写入系统表 `pg_publication_sync`；
4. 逻辑复制解码链路对该系统表特判放行；
5. 输出插件将其转换为 DDL 消息发送；
6. 订阅端接收后执行 DDL；
7. 避免循环复制；
8. 支持按 publication / subscription 过滤。

---

## 1.2 非目标

一期不解决：

* 所有对象类型的 DDL 复制
* 完全结构化 DDL apply
* 跨大版本复杂语法兼容
* 自动冲突修复
* 手工审批模式
* DDL 与 tablesync 初始化复制深度联动

---

# 2. Catalog 与系统表设计

## 2.1 `pg_publication` 扩展

为 `pg_publication` 增加 DDL 配置字段：

```c
int32 pubddl;
```

采用 bitmask 表示支持的 DDL 类型：

```c
#define PUBDDL_NONE      0
#define PUBDDL_TABLE     (1 << 0)
#define PUBDDL_INDEX     (1 << 1)
#define PUBDDL_TRIGGER   (1 << 2)
#define PUBDDL_VIEW      (1 << 3)
#define PUBDDL_RULE      (1 << 4)
#define PUBDDL_SCHEMA    (1 << 5)
#define PUBDDL_FUNCTION  (1 << 6)
#define PUBDDL_TYPE      (1 << 7)
#define PUBDDL_DOMAIN    (1 << 8)
#define PUBDDL_EXTENSION (1 << 9)
#define PUBDDL_ALL       0x7FFFFFFF
```

一期实际启用：

* `PUBDDL_TABLE`
* `PUBDDL_INDEX`

---

## 2.2 `pg_subscription` 扩展

为 `pg_subscription` 增加订阅侧 DDL 配置字段：

```c
int32 subddl;
```

含义：

* 当前 subscription 愿意接收哪些 DDL 类型

一期也只支持：

* `table`
* `index`

---

## 2.3 新增系统表 `pg_publication_sync`

该表加入系统 catalog，定义在 `pg_publication_sync.h` / `.dat` 中。

建议字段：

| 列号 | 字段                | 类型            | 说明               |
| -: | ----------------- | ------------- | ---------------- |
|  1 | `lsn`             | `pg_lsn`      | 参考 WAL 位点        |
|  2 | `xid`             | `xid8`        | 顶层事务号            |
|  3 | `ddl_seqno`       | `int4`        | 事务内顺序号           |
|  4 | `ts`              | `timestamptz` | 时间戳              |
|  5 | `message_type`    | `"char"`      | `Q/A/D`          |
|  6 | `ddl_kind`        | `text[]`      | DDL 类型数组         |
|  7 | `publication`     | `text[]`      | publication 名称数组 |
|  8 | `object_identity` | `text`        | 对象身份标识           |
|  9 | `target_table`    | `text`        | 目标表              |
| 10 | `command_tag`     | `text`        | 命令标签             |
| 11 | `message_data`    | `text`        | 规范化 SQL          |
| 12 | `message_extra`   | `jsonb`       | 扩展信息             |

---

## 2.4 `pg_publication_sync` 的 catalog 定位

该表是：

* 系统表
* 仅内核写入
* 不允许用户直接 DML
* 不通过普通 publication object 管理
* 当 publication 开启 ddl 时，隐式纳入复制控制面

### 设计含义

它不是用户对象，也不是普通 catalog 数据，而是：

> 复制控制面内部系统表

---

# 3. 语法与用户接口详细设计

## 3.1 `CREATE PUBLICATION` 语法解析

在 `gram.y` 中扩展 publication 参数 `ddl`，允许如下写法：

```sql
WITH (ddl = 'table,index')
```

### 解析规则

* 将字符串分割为 token 列表
* 校验 token 是否在允许集合中
* 转成 bitmask 存入 `pg_publication.pubddl`

### 约束

* `FOR TABLE` / `FOR TABLES IN SCHEMA` 下只允许 `table,index`
* 一期 `FOR ALL TABLES` 也仅开放 `table,index`

---

## 3.2 `CREATE SUBSCRIPTION` 语法解析

在 subscription 参数中增加：

```sql
WITH (ddl = 'table,index')
```

解析为 `pg_subscription.subddl`。

### 校验规则

建立 subscription 时，连接发布端获取目标 publications 的 `pubddl` 配置，要求：

```text
subddl ⊆ OR(pubddl of all selected publications)
```

否则报错。

---

# 4. 内部中间结构设计

## 4.1 `LogicalDDLCommand`

定义统一的 DDL 中间表示：

```c
typedef struct LogicalDDLCommand
{
    ReplicableDDLKind kind;

    Oid         relid;          /* 仅发布端内部使用 */
    Oid         nspid;          /* 仅发布端内部使用 */

    char       *command_tag;
    char       *query_string;
    char       *normalized_sql;
    char       *object_identity;
    char       *target_table;

    List       *publication_names;   /* List<char *> */
    List       *ddl_kind_names;      /* List<char *> */

    uint64      xid;
    uint32      ddl_seqno;
    TimestampTz ts;
    XLogRecPtr  lsn;

    Jsonb      *extra;
} LogicalDDLCommand;
```

---

## 4.2 结构设计原则

### `relid/nspid`

仅发布端本地分析时使用，不跨节点传输。

### `publication_names`

必须使用 publication 名称，而不是 OID。

### `normalized_sql`

订阅端实际执行内容。

### `ddl_seqno`

同一事务内顺序控制。

---

# 5. 发布端模块设计

## 5.1 主要模块拆分

建议新增或修改模块如下：

### 语法与 catalog

* `gram.y`
* `publicationcmds.c`
* `subscriptioncmds.c`
* `pg_publication.h`
* `pg_subscription.h`

### DDL 捕获与建模

* `tcop/utility.c`
* 新增 `replication/logical/logicalddl.c`
* 新增 `include/replication/logicalddl.h`

### 系统表插入

* 新增 `catalog/publication_sync.c`
* 新增 `include/catalog/pg_publication_sync.h`

### 输出与复制

* `replication/logical/decode.c` 或对应 relation filtering 路径
* `replication/pgoutput/pgoutput.c`

### 订阅端 apply

* `replication/logical/worker.c`

---

## 5.2 DDL 自动捕获调用链

建议在 `ProcessUtility` 附近接入如下逻辑：

```text
standard_ProcessUtility()
  -> BuildLogicalDDLCommandIfNeeded()
  -> standard_ProcessUtility_real()
  -> PublicationSyncInsert()
```

### 关键点

* 捕获发生在 utility 路径
* 写系统表发生在 DDL 成功之后
* 插入与用户 DDL 处于同一事务

---

## 5.3 `BuildLogicalDDLCommandIfNeeded`

函数原型建议：

```c
bool BuildLogicalDDLCommandIfNeeded(PlannedStmt *pstmt,
                                    const char *queryString,
                                    ProcessUtilityContext context,
                                    LogicalDDLCommand *cmd);
```

### 输出内容

* 是否属于可复制 DDL
* `kind`
* `command_tag`
* `normalized_sql`
* `object_identity`
* `target_table`
* 命中的 publication_names
* `message_extra`

### 一期识别范围

* `CreateStmt`
* `AlterTableStmt`
* `DropStmt`（仅 table/index）
* `IndexStmt`

---

## 5.4 作用域判断

### `FOR TABLE`

只有当 DDL 直接命中该表时才进入 publication 名单。

### `FOR TABLES IN SCHEMA`

当目标表处于 schema 中时命中。

### `FOR ALL TABLES`

所有支持的一期 DDL 均可命中。

---

## 5.5 `PublicationSyncInsert`

函数原型建议：

```c
void PublicationSyncInsert(LogicalDDLCommand *cmd);
```

### 插入逻辑

1. 打开系统表
2. 将 `LogicalDDLCommand` 映射为 tuple
3. 走系统 catalog 插入路径
4. 由当前事务控制提交/回滚

---

## 5.6 `LogicalDDLCommand -> tuple` 映射函数

```c
HeapTuple PublicationSyncBuildTuple(Relation rel,
                                    LogicalDDLCommand *cmd);
```

职责：

* 将内存结构映射为表字段
* 处理 `List -> text[]`
* 处理 `Jsonb`
* 填充 `xid/ddl_seqno/ts/lsn`

---

# 6. 发布端输出设计

## 6.1 逻辑解码放行规则

默认逻辑复制跳过系统表，因此需特判：

```c
if (IsCatalogRelation(relation))
{
    if (RelationGetRelid(relation) != PublicationSyncRelationId)
        return false;
}
```

### 含义

* 其它系统表仍跳过
* `pg_publication_sync` 唯一例外

---

## 6.2 为什么不直接发系统表 tuple

不推荐把 `pg_publication_sync` 的 row 像普通 INSERT 一样发到订阅端，原因：

* 订阅端不应该物化这张表
* apply 端按 relation+tuple 处理会更绕
* 消息语义不直观

因此推荐在输出阶段做转换。

---

## 6.3 输出阶段转换流程

```text
decode 得到对 pg_publication_sync 的 INSERT
-> 从 tuple 中读取字段
-> SyncTupleToLogicalDDLCommand()
-> 转成 DDL replication message
-> 发给订阅端
```

---

## 6.4 输出消息格式

一期可以采用两种策略之一：

### 策略 A：复用 generic message

* prefix 固定，如 `pg_ddl`
* payload 为序列化后的 `LogicalDDLCommand`

### 策略 B：新增专用协议消息 `'D'`

* 更干净
* 改动更大

### 一期建议

优先采用 **策略 A**，因为：

* 可复用现有 MESSAGE 通路
* 修改面小
* 验证快

---

## 6.5 `SyncTupleToLogicalDDLCommand`

函数原型：

```c
LogicalDDLCommand *SyncTupleToLogicalDDLCommand(TupleTableSlot *slot);
```

职责：

* 从 `pg_publication_sync` 的 slot 中提取字段
* 构造统一中间结构
* 供输出阶段序列化发送

---

# 7. 订阅端模块设计

## 7.1 订阅端接收路径

建议流程：

```text
walsender -> apply worker
-> read message
-> if prefix == pg_ddl
-> deserialize LogicalDDLCommand
-> apply_handle_ddl()
```

---

## 7.2 `DDLMessageMatchesSubscription`

函数原型：

```c
bool DDLMessageMatchesSubscription(LogicalDDLCommand *cmd);
```

判断条件：

1. `publication` 与当前 subscription 的 publication 名称列表相交
2. `ddl_kind` 被当前 subscription 启用

---

## 7.3 `apply_handle_ddl`

函数原型：

```c
static void apply_handle_ddl(LogicalDDLCommand *cmd);
```

职责：

* 过滤
* 幂等检查
* 设置 replay 标志
* 执行 DDL
* 错误处理

---

## 7.4 执行 DDL 的方式

一期直接执行：

```text
cmd->normalized_sql
```

通过 utility 执行路径完成。

### 原则

* 只执行发布端已经规范化后的 SQL
* 不依赖订阅端 search_path
* 以 subscription owner 身份执行

---

## 7.5 循环复制抑制

订阅端执行复制来的 DDL 时设置：

```c
in_ddl_replay = true;
```

自动捕获逻辑必须检查该标志：

```c
if (in_ddl_replay)
    return;
```

避免：

```text
subscriber apply DDL
-> again capture DDL
-> again write pg_publication_sync
-> replicate forever
```

---

# 8. 顺序与事务处理设计

## 8.1 事务内顺序

对于：

```sql
BEGIN;
CREATE TABLE t1(id int);
INSERT INTO t1 VALUES (1);
COMMIT;
```

源端事务内变化顺序应为：

1. 执行 CREATE TABLE
2. 写 `pg_publication_sync`
3. 执行对 `t1` 的 INSERT
4. COMMIT

这样订阅端就能先处理 DDL，再处理 DML。

---

## 8.2 事务回滚

若源端事务回滚：

* `pg_publication_sync` 中的插入回滚
* 不产生可见复制消息
* 订阅端不会收到该 DDL

---

## 8.3 同事务多 DDL 顺序

对于：

```sql
BEGIN;
CREATE TABLE t1(...);
CREATE INDEX idx_t1_id ON t1(id);
COMMIT;
```

需要通过：

* `xid`
* `ddl_seqno`

保证顺序：

```text
1. CREATE TABLE
2. CREATE INDEX
```

---

# 9. 幂等与去重设计

## 9.1 为什么需要幂等

逻辑复制槽在崩溃恢复后，可能从较早 LSN 重新发送最近消息，因此订阅端不能完全假定“只收到一次”。

---

## 9.2 一期简化方案

一期不做专门的持久去重表，但在 apply 前做最小幂等检查：

### 例子

* `CREATE TABLE public.t1`：若本地已存在结构兼容表，可视情况跳过或报错
* `CREATE INDEX public.idx_t1_id`：若同名索引存在且兼容，可跳过

### 保守策略

* 冲突默认报错停订阅
* 不做激进自动修复

---

## 9.3 后续增强方向

后续可引入轻量去重信息：

```text
(origin, xid, ddl_seqno, end_lsn)
```

---

# 10. 错误处理设计

## 10.1 发布端

### DDL 执行失败

* 不写 `pg_publication_sync`
* 事务按原生错误处理

### 写系统表失败

* 当前事务失败
* 不允许“DDL成功但同步记录失败后继续提交”

---

## 10.2 输出阶段

### 解析 `pg_publication_sync` tuple 失败

* 视为复制错误
* 停止当前复制输出

---

## 10.3 订阅端

### apply DDL 失败

* 中止当前远端事务 apply
* 按现有 subscription 错误机制停 worker 或禁用订阅

### 默认不自动跳过

因为 DDL 失败通常意味着 schema 已分叉。

---

# 11. 清理与维护设计

## 11.1 为什么必须 prune

`pg_publication_sync` 只在发布端持久化，会持续增长。

---

## 11.2 一期方案

新增维护函数：

```sql
SELECT pg_publication_sync_prune();
```

### 清理依据

根据：

* 各 subscription 的确认进度
* 已不再需要的旧 DDL 记录

---

## 11.3 后续增强

* 自动后台清理
* 按最小 confirmed LSN 自动 prune
* 提供可观测视图

---

# 12. 关键函数设计清单

## 12.1 发布端

```c
bool BuildLogicalDDLCommandIfNeeded(PlannedStmt *pstmt,
                                    const char *queryString,
                                    ProcessUtilityContext context,
                                    LogicalDDLCommand *cmd);

List *GetDDLTargetPublications(LogicalDDLCommand *cmd);

void PublicationSyncInsert(LogicalDDLCommand *cmd);

HeapTuple PublicationSyncBuildTuple(Relation rel,
                                    LogicalDDLCommand *cmd);
```

---

## 12.2 输出阶段

```c
bool RelationIsPublicationSync(Relation rel);

LogicalDDLCommand *SyncTupleToLogicalDDLCommand(TupleTableSlot *slot);

void pgoutput_write_ddl_message(LogicalDecodingContext *ctx,
                                LogicalDDLCommand *cmd);
```

---

## 12.3 订阅端

```c
LogicalDDLCommand *logicalrep_read_ddl_message(StringInfo s);

bool DDLMessageMatchesSubscription(LogicalDDLCommand *cmd);

bool DDLCommandAlreadyApplied(LogicalDDLCommand *cmd);

void ExecuteReplicatedDDL(const char *normalized_sql);

static void apply_handle_ddl(LogicalDDLCommand *cmd);
```

---

# 13. 状态流转图

## 13.1 发布端状态流转

```text
[用户执行DDL]
    ->
[ProcessUtility识别]
    ->
[执行真实DDL]
    ->
[成功?] --否--> [结束]
    |
   是
    v
[构造LogicalDDLCommand]
    ->
[写pg_publication_sync]
    ->
[事务提交]
    ->
[WAL解码]
    ->
[识别系统表 pg_publication_sync]
    ->
[转为DDL message]
    ->
[发送订阅端]
```

---

## 13.2 订阅端状态流转

```text
[收到DDL message]
    ->
[反序列化为 LogicalDDLCommand]
    ->
[publication 匹配?] --否--> [忽略]
    |
   是
    v
[ddl_kind 匹配?] --否--> [忽略]
    |
   是
    v
[幂等/冲突检查]
    ->
[设置 in_ddl_replay = true]
    ->
[执行 normalized_sql]
    ->
[成功?] --否--> [报错并停订阅]
    |
   是
    v
[清理 replay 标志]
    ->
[继续处理后续复制消息]
```

---

# 14. 测试设计

## 14.1 回归测试范围

### publication/subscription 接口测试

1. `CREATE PUBLICATION ... WITH (ddl='table')`
2. `CREATE SUBSCRIPTION ... WITH (ddl='table,index')`
3. 非法 ddl 参数报错
4. `subddl` 超出 `pubddl` 报错

### 发布端自动捕获

5. `CREATE TABLE` 自动写 `pg_publication_sync`
6. `CREATE INDEX` 自动写 `pg_publication_sync`
7. DDL 失败不写记录

### 复制链路

8. `pg_publication_sync` 系统表被特判放行
9. 输出阶段能转成 DDL message
10. 普通系统表仍不会被复制

### 订阅端 apply

11. 收到 DDL message 能正确执行
12. `publication` 不匹配时忽略
13. `ddl_kind` 不匹配时忽略

### 顺序

14. `CREATE TABLE + INSERT` 同事务顺序正确
15. `CREATE TABLE + CREATE INDEX` 顺序正确

### 循环抑制

16. 订阅端 replay DDL 不再次写 `pg_publication_sync`

### 错误

17. 本地冲突导致 apply 失败时订阅停止
18. 权限不足导致 apply 失败时订阅停止

### 清理

19. `pg_publication_sync_prune()` 能回收旧记录

---

## 14.2 推荐测试方式

* `src/test/subscription/` 增加 TAP 测试
* 必要时补 isolation test 验证事务顺序

---

# 15. 实施拆分建议

## Patch 1

* catalog 变更：`pg_publication` / `pg_subscription`
* 语法支持：publication/subscription 的 `ddl` 参数

## Patch 2

* 新增系统表 `pg_publication_sync`
* `PublicationSyncInsert`
* `LogicalDDLCommand` 基础结构

## Patch 3

* `ProcessUtility` 自动捕获 table/index DDL
* 写 `pg_publication_sync`

## Patch 4

* logical decoding 放行 `pg_publication_sync`
* `pgoutput` 将其转为 DDL message

## Patch 5

* 订阅端 `apply_handle_ddl`
* replay 抑制
* 基础幂等/错误处理

## Patch 6

* prune 函数
* TAP 测试
* 文档

---

# 16. 风险与缓解

## 风险 1：SQL replay 仍有环境依赖

### 缓解

* 使用 `normalized_sql`
* 强制 schema-qualified
* 一期限制对象类型

## 风险 2：系统表放行破坏逻辑复制边界

### 缓解

* 仅单表特判
* 输出阶段立即转 message
* 订阅端不物化系统表

## 风险 3：顺序错误

### 缓解

* `pg_publication_sync` 插入与 DDL/DML 同事务
* 使用 `ddl_seqno`

## 风险 4：循环复制

### 缓解

* `in_ddl_replay` 标志强制抑制

---

# 17. 后续演进建议

## 二期

* 扩展 `view/trigger/rule/schema`
* 手动审批模式
* 更强幂等

## 三期

* `message_data` 从 SQL 主导转向结构化 DDL
* apply 端优先走内核 API
* 逐步减少对系统表载体的依赖

---

如果你愿意，我下一步可以继续把这份详细设计再往下扩成两种之一：

**A. 模块级伪代码设计**
直接给出 `utility.c / publication_sync.c / pgoutput.c / worker.c` 的关键函数骨架

**B. 评审版设计文档**
整理成更正式的章节结构、术语表、约束表、时序图说明，适合内部评审。

