# 逻辑复制支持DDL概要设计

## 1. 背景与目标

PostgreSQL 当前内建逻辑复制主要覆盖表级 DML 复制。logical decoding 的本质是从 WAL 中提取持久化变更，转成更高层可理解的变更流；复制槽保证“按源端发生顺序”向客户端提供变化序列。([PostgreSQL][1])

本 patch 的目标是：

1. 在 **不引入“辅助系统表承载 DDL”** 的前提下，实现 **DDL 作为逻辑复制流中的一等消息**。
2. 保证 **DDL 与同事务内 DML 的顺序一致性**。
3. 与现有 `CREATE PUBLICATION` / `CREATE SUBSCRIPTION` 扩展参数模型保持一致。当前文档显示 publication 和 subscription 都是通过 `WITH (...)` 扩展行为参数。([PostgreSQL][2])
4. 使实现尽可能复用现有 `ProcessUtility -> WAL -> logical decoding -> pgoutput -> apply worker` 主链路。逻辑复制协议和 message format 文档也说明了 walsender 输出的逻辑消息是有正式消息格式定义的。([PostgreSQL][3])

---

## 2. 设计原则

### 2.1 不采用“系统表 + DML 复制 DDL”的方案

原因：

* 逻辑复制协议天然有事务边界，Begin/Commit 内消息顺序是协议层定义的一部分。([PostgreSQL][3])
* 如果把 DDL 先写成某张特殊表的 INSERT，再靠普通表复制带过去，DDL/DML 顺序就变成“旁路约束”，而不是由逻辑解码器和输出插件天然保证。

### 2.2 DDL 以逻辑复制消息形式传输

现有 `worker.c` 中可以看到 apply 端已经能识别 `LOGICAL_REP_MSG_MESSAGE`，只是当前内建逻辑复制“还没有使用 generic logical messages”。这恰好给 DDL 复制提供了清晰扩展点。([doxygen.postgresql.org][4])

### 2.3 优先做 transactional、可重放、白名单化的 DDL

DDL 并不等同于所有 utility statement。第一期只支持安全子集，避免把 role/database/tablespace/ALTER SYSTEM 之类全局对象和高风险命令纳入复制。

---

## 3. 范围定义

## 3.1 一期支持范围

建议一期只支持：

* `table`
* `index`

并支持三种 publication scope：

* `FOR TABLE`
* `FOR TABLES IN SCHEMA`
* `FOR ALL TABLES`

这样做有两个原因：

一是实现路径最短；二是 PostgreSQL 社区公开的 DDL replication 讨论材料里，也明确展示了 `CREATE PUBLICATION ... WITH (ddl = 'table')`、`'table, index'` 这类设计方向。

## 3.2 二期再扩展

后续再考虑：

* `trigger`
* `view`
* `rule`
* `schema`
* `function`
* `type`
* `domain`
* `extension`

---

## 4. 用户接口设计

## 4.1 CREATE/ALTER PUBLICATION

扩展为：

```sql
CREATE PUBLICATION name
    [ FOR ALL TABLES
      | FOR publication_object [, ... ] ]
    [ WITH ( publication_parameter [= value] [, ... ],
             ddl [= value] ) ]
```

其中 `ddl` 为字符串列表，例如：

```sql
WITH (ddl = 'table,index')
```

### 校验规则

* `FOR TABLE` / `FOR TABLES IN SCHEMA`：仅允许 `table,index`
* `FOR ALL TABLES`：允许一期支持的全部 ddl kind

社区 DDL replication 讨论材料已经把 `ddl` 作为 publication 的新参数来表达“发布哪些对象类型的 DDL”。

## 4.2 CREATE/ALTER SUBSCRIPTION

扩展为：

```sql
CREATE SUBSCRIPTION subname
    CONNECTION 'conninfo'
    PUBLICATION pubname [, ...]
    [ WITH ( subscription_parameter [= value] [, ... ],
             ddl [= value] ) ]
```

### 订阅侧校验规则

设：

```text
wanted = subscription.ddl
offered = union(all publication.ddl)
```

要求：

```text
wanted ⊆ offered
```

否则报错。

这比“必须完全一致”更符合现有 publication/subscription 的组合过滤模型。当前 `CREATE SUBSCRIPTION` 文档也说明 subscription 可以订阅多个 publications。([PostgreSQL][2])

---

## 5. Catalog 设计

不新增 `pg_publication_sync` 之类运行时消息表。

只改静态 catalog。

## 5.1 `pg_publication`

新增字段：

```c
int32 pubddl;
```

按 bitmask 存储：

```c
#define PUBDDL_TABLE   (1 << 0)
#define PUBDDL_INDEX   (1 << 1)
#define PUBDDL_TRIGGER (1 << 2)
...
```

### 原因

* 比 text[] 更适合执行期快速判断
* 便于 catalog 升级和 future extension

## 5.2 `pg_subscription`

新增字段：

```c
int32 subddl;
bool  subddlmanual;   /* 二期可选 */
```

一期可只做 `subddl`。

---

## 6. 核心架构

```text
DDL SQL
  -> ProcessUtility
  -> 识别出可复制 DDL
  -> 构造 LogicalDDLCommand
  -> 记入当前事务的 logical decoding message
  -> WAL
  -> reorderbuffer / decoding
  -> pgoutput 输出 DDL 消息
  -> apply worker 按事务顺序执行 DDL
  -> 再继续执行同事务后续 DML
```

这个设计完全建立在 PostgreSQL 当前“逻辑复制基于 WAL 解码、复制槽按顺序输出、协议有明确事务消息流”的基础上。([PostgreSQL][1])

---

## 7. 发布端实现设计

## 7.1 DDL 捕获点

主入口选在 `ProcessUtility` 路径。

原因：

* DDL 的统一入口就在 utility 执行器
* 这里能拿到 parse tree、command tag、原始 query string
* 与 event trigger 相比，更适合作为复制基础设施

实现上建议在 `standard_ProcessUtility()` 相邻路径增加：

```c
bool GetLogicalDDLInfo(PlannedStmt *pstmt,
                       const char *queryString,
                       ProcessUtilityContext context,
                       LogicalDDLCommand *cmd);
```

## 7.2 内部结构体

```c
typedef enum ReplicableDDLKind
{
    REPL_DDL_TABLE,
    REPL_DDL_INDEX,
    REPL_DDL_TRIGGER,
    REPL_DDL_VIEW,
    ...
} ReplicableDDLKind;

typedef struct LogicalDDLCommand
{
    ReplicableDDLKind kind;

    Oid classid;
    Oid objid;
    int32 objsubid;

    Oid relid;      /* 若有目标表 */
    Oid nspid;      /* 若有命名空间 */

    char *command_tag;
    char *query_string;
    char *normalized_sql;
    char *object_identity;

    List *pubids;   /* 命中的 publication OIDs */

    uint32 flags;   /* transactional / requires_relcache_flush / etc */
} LogicalDDLCommand;
```

## 7.3 为什么不能只存原始 query string

因为订阅端重放时：

* `search_path` 可能不同
* 大小写折叠、未限定 schema 名称可能产生歧义
* 同一语句在不同版本或不同环境中副作用不同

所以建议同时生成：

* `query_string`：仅用于日志/调试
* `normalized_sql`：用于复制执行，尽量 schema-qualified
* `object_identity`：用于冲突检查/幂等判断

---

## 8. WAL / 解码层设计

## 8.1 推荐方案：用 transactional logical message 承载 DDL

逻辑解码文档明确说，解码结果可以表现为 tuple 流，也可以表现为 SQL statements 一类的上层语义。([PostgreSQL][1])

因此这里建议新增内部接口：

```c
void LogLogicalDDLMessage(LogicalDDLCommand *cmd);
```

其底层语义等价于写入一条 **transactional logical decoding message**，prefix 固定为：

```text
pg_ddl
```

### 为什么推荐 message，而不是专门新 WAL rmgr

* 更贴近现有 logical decoding message 能力
* 侵入面小
* 可直接复用 `pgoutput` 的 message 输出路径

## 8.2 与 ReorderBuffer 的关系

DDL message 要和普通 DML change 一样，挂到当前事务的 reorder buffer 流中。这样才能保证：

```sql
BEGIN;
CREATE TABLE t1(...);
INSERT INTO t1 VALUES (1);
COMMIT;
```

在输出端看到的仍然是：

```text
BEGIN
DDL(create table t1)
INSERT(t1)
COMMIT
```

而不是乱序。

---

## 9. pgoutput 扩展设计

PostgreSQL 当前的逻辑流复制协议文档说明，`pgoutput` 作为标准输出插件通过 `START_REPLICATION` 选项协商能力，并按正式 message format 发送逻辑复制消息。([PostgreSQL][3])

## 9.1 握手参数新增

在 `START_REPLICATION ... LOGICAL` 的 `pgoutput` options 中新增：

* `ddl` = `true|false`
* `ddl_kinds` = `'table,index'`
* `ddl_proto_version` = `1`

## 9.2 新增逻辑复制消息类型

建议新增：

```text
Byte1('D')  -- DDL message
```

消息体定义：

```text
Byte1    'D'
Int8     version
Int8     ddl_kind
Int16    flags
Int32    classid
Int32    objid
Int32    objsubid
Int32    relid
Int32    nspid
String   command_tag
String   object_identity
String   normalized_sql
Int32    npubs
Int32[]  pubids
```

### 设计理由

* 不依赖 JSON
* 二进制更稳定
* 可按版本演进
* apply 端可先基于 kind/pubids 做过滤，再决定是否执行 SQL

## 9.3 输出过滤

`pgoutput_change()` 或其消息分发路径中增加：

1. 是否协商了 `ddl = true`
2. 该 DDL kind 是否在 publication 的 `pubddl` 中
3. 若为 `FOR TABLE` / `FOR TABLES IN SCHEMA`，是否命中作用域
4. 若命中多个 publication，则带上对应 pubids

---

## 10. 订阅端 apply 设计

## 10.1 apply worker 增加 DDL 分发

当前 `worker.c` 已能识别 generic message，但当前逻辑复制“并未使用它”。([doxygen.postgresql.org][4])

需要新增：

```c
static void apply_handle_ddl(StringInfo s);
```

并在 dispatch 中添加：

```c
case LOGICAL_REP_MSG_DDL:
    apply_handle_ddl(s);
    break;
```

## 10.2 执行时机

DDL 必须在远端事务上下文中按顺序执行：

```text
remote BEGIN
  -> DDL #1
  -> DDL #2
  -> DML #1
  -> DML #2
remote COMMIT
```

这样才能保证 schema 先到位，再 apply 同事务内后续 tuple 变更。

## 10.3 tablesync worker 不处理 DDL

初始表同步语义是 copy table data，不是重放复制流中的 schema change。当前 publication/subscription 文档也强调若干行为只影响逻辑复制流，而不是 initial copy 本身。([PostgreSQL][2])

因此：

* apply worker：处理 DDL
* tablesync worker：忽略 DDL

---

## 11. 安全模型

订阅安全性必须单独说明。

PostgreSQL 当前文档说明：

* subscription apply 进程会话级以 subscription owner 权限运行
* 对表 DML，默认会切换到表 owner 执行
* `run_as_owner = true` 时不会切换，而是全部以 subscription owner 权限执行
* 同时文档明确警告：这会带来更高安全风险，例如表 owner 可借触发器获得 subscription owner 权限。([PostgreSQL][2])

基于这一现状，DDL 复制一期建议：

### 11.1 执行身份

DDL 一律以 **subscription owner** 执行，不做“切换为对象 owner”。

原因：

* DDL 不是单表 DML，无法复用“按表 owner 切换”模型
* 安全边界更清晰

### 11.2 前置要求

创建启用 DDL 复制的 subscription 时，要求：

* owner 具备目标对象上的足够 DDL 权限
* 若不满足，创建时警告，执行时失败停订阅

### 11.3 文档告警

必须在文档中明确：

> 开启 DDL 复制相当于允许 publisher 侧通过复制流驱动 subscriber 执行部分 utility statements，应仅在强信任环境下启用。

---

## 12. 错误处理策略

## 12.1 默认策略：失败即停订阅

当前 `worker.c` 中已有错误路径：apply worker 出错时会清理当前复制 origin 事务状态，并根据订阅配置走禁用或抛错流程。([doxygen.postgresql.org][4])

DDL apply 失败时建议保持同样语义：

* abort 当前远端事务
* 记录 subscription error
* 停 worker / 按 `disableonerr` 行为处理

## 12.2 不自动跳过

一期不做“自动 skip 某条 DDL”。

原因：

* DDL 失败通常意味着 schema 已分叉
* 自动跳过会导致后续 DML 继续在错误 schema 上执行

---

## 13. 幂等与重复投递

逻辑 decoding/slot 语义决定了消费者应考虑最近消息重发的可能性；复制槽维护的是按顺序消费的位置，而不是“绝对一次且无重复”的业务幂等保障。logical decoding 文档强调 slot 表示一个按源端顺序回放的变化流。([PostgreSQL][1])

因此 subscriber 侧需要幂等策略。

## 13.1 DDL 去重键

建议组合：

```text
(origin_id, xid, end_lsn, ddl_seqno)
```

其中 `ddl_seqno` 是事务内第几个 DDL 事件。

## 13.2 最低限度幂等检查

执行前检查：

* 若目标对象已存在，且 `object_identity` 相同、结构兼容，则可视为已应用
* 若对象存在但定义冲突，则报错停订阅

---

## 14. 作用域匹配规则

## 14.1 FOR TABLE

一期仅允许：

* 该表本身的 `CREATE/ALTER/DROP TABLE`
* 与该表关联的 `CREATE/ALTER/DROP INDEX`

判断依据主要是 `relid`。

## 14.2 FOR TABLES IN SCHEMA

一期允许：

* 命中 schema 的 table/index DDL

判断依据主要是 `nspid`。

## 14.3 FOR ALL TABLES

一期允许所有受支持的 ddl kinds。

---

## 15. 非支持对象与限制

一期直接拒绝以下命令进入 DDL 复制流：

* `CREATE DATABASE`
* `DROP DATABASE`
* `ALTER SYSTEM`
* role / tablespace / database 级对象
* 非事务型或副作用不易重放的 utility command
* 影响 subscription/publication/slot 自身管理的命令

## 特别说明：extension

虽然你原始需求里列了 `extension`，但 extension 安装/升级通常带脚本、副作用和环境依赖，不建议放入一期。

---

## 16. Manual 模式设计

你之前想提供 `pg_sync_ddl(ddl)` 这种手动函数。按内核级方案，建议不要让用户手输 SQL 再“二次广播”。

更合理的是二期引入：

```sql
ALTER SUBSCRIPTION sub SET (ddl_manual = true);
```

含义：

* publisher 仍发送 DDL message
* subscriber 收到后不立即执行，而是记为 pending
* 用户再执行：

  ```sql
  SELECT pg_apply_pending_ddl('subname', lsn);
  ```

一期可以先不做 manual，先把 automatic 跑通。

---

## 17. 源码改动点清单

## 17.1 Catalog / 命令解析

* `src/include/catalog/pg_publication.h`
* `src/include/catalog/pg_subscription.h`
* `src/backend/commands/publicationcmds.c`
* `src/backend/commands/subscriptioncmds.c`
* `src/backend/parser/gram.y`
* catalog version bump / `.dat` 更新

## 17.2 DDL 捕获

* `src/backend/tcop/utility.c`
* 新增：

  * `src/backend/replication/logical/logicalddl.c`
  * `src/include/replication/logicalddl.h`

## 17.3 解码与输出

* `src/backend/replication/logical/message.c` 或相邻逻辑消息路径
* `src/backend/replication/pgoutput/pgoutput.c`
* `src/backend/replication/logical/proto.c`
* `src/include/replication/logicalproto.h`

## 17.4 订阅端 apply

* `src/backend/replication/logical/worker.c`

## 17.5 文档

* `doc/src/sgml/ref/create_publication.sgml`
* `doc/src/sgml/ref/create_subscription.sgml`
* `doc/src/sgml/protocol.sgml`
* `doc/src/sgml/logical-replication.sgml`
* `doc/src/sgml/logical-replication-security.sgml`

当前官方文档已经分别覆盖了 `CREATE SUBSCRIPTION`、逻辑复制安全、协议与消息格式，所以这些 sgml 改动点是自然的落脚位置。([PostgreSQL][2])

---

## 18. 关键内部接口草案

### 18.1 发布端

```c
extern bool GetLogicalDDLInfo(PlannedStmt *pstmt,
                              const char *queryString,
                              ProcessUtilityContext context,
                              LogicalDDLCommand *cmd);

extern void LogLogicalDDLMessage(LogicalDDLCommand *cmd);
```

### 18.2 协议层

```c
extern void logicalrep_write_ddl(StringInfo out,
                                 LogicalDDLCommand *cmd);

extern LogicalDDLCommand *logicalrep_read_ddl(StringInfo in);
```

### 18.3 订阅端

```c
static void apply_handle_ddl(StringInfo s);
static void execute_replicated_ddl(LogicalDDLCommand *cmd);
static bool ddl_already_applied(LogicalDDLCommand *cmd);
```

---

## 19. 测试方案

## 19.1 isolation / TAP 用例矩阵

### 基础功能

1. `CREATE TABLE` 后同事务 `INSERT`
2. `ALTER TABLE ADD COLUMN`
3. `DROP TABLE`
4. `CREATE INDEX`
5. `DROP INDEX`

### 过滤

6. `publication ddl=table`，index 不应复制
7. `subscription ddl=index` 但 publication 无 index，应报错
8. `FOR TABLE` 下 table/index 命中与未命中对象验证
9. `FOR TABLES IN SCHEMA` 下跨 schema DDL 不应复制

### 顺序

10. 同事务 `CREATE TABLE + INSERT`
11. 同事务 `ALTER TABLE ADD COLUMN + UPDATE 新列`

### 错误

12. subscriber 已存在冲突表定义，DDL apply 失败后订阅停住
13. 权限不足导致 DDL 执行失败

### 幂等

14. 故障恢复后重复接收最近 DDL message，不应错误重复建表

### worker 边界

15. tablesync worker 不处理 DDL
16. apply worker 正常处理 DDL

---

## 20. 一期 patch 切分建议

### Patch 1

catalog + grammar + `CREATE/ALTER PUBLICATION|SUBSCRIPTION` 参数解析

### Patch 2

`ProcessUtility` 捕获 table/index DDL，构造 `LogicalDDLCommand`

### Patch 3

transactional logical DDL message 写入 + `pgoutput` 输出 `D`

### Patch 4

apply worker 支持 `D` 消息执行

### Patch 5

测试与文档

这种切法更适合代码评审，也便于每一步单独验证。

---

## 21. 与原“pg_publication_sync 表方案”的对比结论

新方案相对你原设计的主要收益：

1. **顺序正确**
   DDL 与同事务内 DML 一起走 WAL 解码与复制协议顺序。逻辑复制协议的 Begin/Commit 和消息流本来就是为此设计的。([PostgreSQL][3])

2. **不需要特判系统表复制**
   不再要求放开系统表过滤，也不必维护清理函数。

3. **没有旁路状态表**
   不用再管 `pg_publication_sync_prune()`、表膨胀、vacuum、catalog 特殊语义。

4. **更贴近现有框架**
   现有 apply 端已经能识别 generic message，只是还没拿来做 built-in DDL replication。([doxygen.postgresql.org][4])

5. **更容易进入长期演进方向**
   社区已有公开 DDL replication 方向讨论，publication 侧增加 `ddl` 参数是已有思路。

---

## 22. 最终建议

如果你准备真正开工，我建议你按下面这条主线实施：

* 一期只做 `table,index`
* 只做 automatic apply
* 用 transactional logical message 承载 DDL
* `pgoutput` 新增 `D` 消息
* apply worker 在同事务内先执行 DDL，再执行 DML

这版最稳，也最像“能在 PG 内核里长期存活”的方案。

下一步我可以继续直接给你两样东西中的一个：

**A. 详细到函数级别的源码改造清单**
按 `publicationcmds.c / utility.c / pgoutput.c / worker.c` 展开

**B. 一版 patch 伪代码骨架**
直接给你 `struct`、关键函数签名和主要调用链伪代码

[1]: https://www.postgresql.org/docs/current/logicaldecoding-explanation.html "PostgreSQL: Documentation: 18: 47.2. Logical Decoding Concepts"
[2]: https://www.postgresql.org/docs/current/sql-createsubscription.html "PostgreSQL: Documentation: 18: CREATE SUBSCRIPTION"
[3]: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html "PostgreSQL: Documentation: 18: 54.9. Logical Replication Message Formats"
[4]: https://doxygen.postgresql.org/backend_2replication_2logical_2worker_8c.html "PostgreSQL Source Code: src/backend/replication/logical/worker.c File Reference"
