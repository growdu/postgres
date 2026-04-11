下面我继续，把前面的内容**收敛成一版更正式、可评审的完整设计文档正文**。这一版会尽量统一术语、统一边界、统一一期/二期分层，并把前面零散补充的内容整理到同一文档结构中。整体方案是在你原先一期文档“DDL 作为逻辑复制消息、一等进入事务顺序流”的主线基础上扩展而来。

---

# PostgreSQL 逻辑复制支持 DDL 统一详细设计文档

## 1. 摘要

本文档描述 PostgreSQL 内建逻辑复制支持 DDL 的统一设计方案。方案覆盖两个阶段：

* 一期：支持 `table`、`index` 的 automatic DDL 复制
* 二期：扩展 `schema`、`trigger`、`view`、`function`、`type`、`domain`、`rule` 的 automatic DDL 复制，并引入 manual DDL 广播能力

本设计不采用“借助普通表承载 DDL 再通过 DML 复制”的旁路方案，也不引入 pending、队列、延迟执行等中间状态。DDL 被建模为逻辑复制流中的一等消息，在发布端事务执行路径上主动写入 WAL 中的 transactional logical message，并在逻辑解码、输出插件、订阅端 apply worker 中与 DML 一起按事务顺序处理。

---

## 2. 背景

PostgreSQL 当前内建逻辑复制以表级 DML 为核心，支持 publication / subscription 模型、logical decoding、replication slot、pgoutput 协议和 apply worker 执行链路，但不提供内建 DDL 复制能力。现有用户若需要 DDL 复制，通常只能：

* 手工在订阅端执行 DDL
* 借助外部工具或扩展
* 通过自定义 message 或自定义中间表旁路传递 DDL

这些方式要么无法保证与同事务 DML 的顺序一致性，要么侵入现有复制架构较大，要么要求用户维护额外状态。

因此，需要一种更贴近 PostgreSQL 现有逻辑复制内核架构的方案，使 DDL：

1. 能与 DML 一起进入同一条逻辑复制事务流
2. 能复用 WAL → decoding → pgoutput → apply worker 的主链路
3. 能在 publication / subscription 模型下进行过滤和分发
4. 不依赖系统表 DML 化承载 DDL 的旁路设计

---

## 3. 设计目标

### 3.1 核心目标

第一，支持 DDL 作为逻辑复制流中的一等消息。
第二，保证 DDL 与同事务 DML 的严格顺序一致。
第三，保持与现有 publication / subscription 参数扩展模型一致。
第四，尽可能复用现有 logical decoding、pgoutput、apply worker 机制。
第五，为后续更多对象类型和手动广播能力预留统一架构。

### 3.2 非目标

本设计明确不做以下事项：

* 不实现 pending / queue / 延迟执行模型
* 不实现“收到 DDL 但暂不执行”的手动确认流程
* 不实现按 subscription 单独定向发送 DDL
* 不尝试从普通物理 WAL record 中反推出高层 DDL 语义
* 不在一期支持高风险全局对象，如 database、role、tablespace、ALTER SYSTEM 等

---

## 4. 总体设计原则

### 4.1 automatic 与 manual 入口分离

automatic DDL 指用户正常执行 DDL 时，由内核在 ProcessUtility 路径自动捕获、识别、过滤并写入复制流。
manual DDL 指用户显式调用 SQL 函数，把一条 DDL 主动广播到指定 publication 的复制流中。

二者不是同一能力的两种执行状态，而是两条独立入口。

### 4.2 下游复制链路统一

无论是 automatic 还是 manual，只要决定进入逻辑复制流，都统一转换为 `LogicalDDLCommand`，并统一通过 `LogLogicalDDLMessage()` 写入 transactional logical message，再统一走 logical decoding、pgoutput 和 apply worker。

### 4.3 publication 是唯一分发单位

DDL 不直接“发送给订阅者”。
用户无论配置 automatic 还是调用 manual，真正绑定的都是 publication。最终由每个 subscription 根据自己订阅的 publication 集合决定是否接收该 DDL。

### 4.4 DDL 不从普通 WAL 反推，而是在执行路径主动写逻辑消息

普通物理 WAL 适合 crash recovery 和物理复制，但不足以稳定表达用户层 DDL 语义。本设计不尝试从 catalog heap WAL、smgr 变更或其他低层 record 中拼接出 DDL，而是在 DDL 执行路径上直接写入一条高层语义的 logical message。

### 4.5 不引入中间状态机

一旦 DDL 进入逻辑复制流，就与 DML 一样成为正式复制事务的一部分。不存在“已收到但未执行”的挂起态，不需要 queue、pending、手动 apply、skip 等补充系统。

---

## 5. 统一架构

整体架构如下：

```text
automatic DDL / manual DDL
          ↓
  LogicalDDLCommand
          ↓
 LogLogicalDDLMessage
          ↓
 WAL transactional logical message
          ↓
 logical decoding + ReorderBuffer
          ↓
      pgoutput 'D'
          ↓
   apply_handle_ddl
          ↓
 execute_replicated_ddl
```

automatic 与 manual 的差别仅体现在进入 `LogicalDDLCommand` 之前：

* automatic：来自 ProcessUtility 自动捕获
* manual：来自 `pg_emit_logical_ddl(...)` 显式调用

---

## 6. 分阶段功能范围

### 6.1 一期

一期只支持 automatic DDL，范围限定为：

* `table`
* `index`

作用域支持：

* `FOR TABLE`
* `FOR TABLES IN SCHEMA`
* `FOR ALL TABLES`

### 6.2 二期

二期扩展两部分能力。

第一部分是 automatic DDL 对象扩展：

* `schema`
* `trigger`
* `view`
* `function`
* `type`
* `domain`
* `rule`

第二部分是 manual DDL 广播能力：

* 用户显式指定一条受控 DDL SQL
* 用户显式指定目标 publication 集合
* 系统解析、校验、规范化后，写入逻辑复制流

### 6.3 明确不支持或暂不支持

以下对象不进入 automatic，manual 也默认不开放或仅保留未来扩展位：

* database
* role
* tablespace
* ALTER SYSTEM
* replication slot
* publication / subscription 本身
* event trigger
* 高风险 extension 安装/升级类对象

---

## 7. 用户接口设计

### 7.1 publication 接口

扩展 `CREATE PUBLICATION` / `ALTER PUBLICATION` 的 `WITH (...)` 参数，新增 `ddl`：

```sql
CREATE PUBLICATION pub1
FOR ALL TABLES
WITH (ddl = 'table,index,trigger,view,function,schema');
```

`ddl` 是字符串列表，内部转换为 bitmask。

### 7.2 subscription 接口

扩展 `CREATE SUBSCRIPTION` / `ALTER SUBSCRIPTION` 的 `WITH (...)` 参数：

```sql
CREATE SUBSCRIPTION sub1
CONNECTION '...'
PUBLICATION pub1
WITH (
  ddl = 'table,index,trigger,view,function',
  enable_ddl = true,
  enable_ddl_manual = false
);
```

语义如下：

* `ddl`：订阅端允许接收的 DDL 类型集合
* `enable_ddl`：订阅端是否启用 DDL 复制
* `enable_ddl_manual`：订阅端是否接受 manual DDL 消息

### 7.3 subscription 参数校验

设：

* `wanted = subscription.ddl`
* `offered = union(all publication.ddl)`

要求：

```text
wanted ⊆ offered
```

否则创建或修改 subscription 时报错。

### 7.4 manual DDL SQL 接口

提供 SQL 函数：

```sql
SELECT pg_emit_logical_ddl(
    publications => ARRAY['pub1'],
    ddl_sql      => 'CREATE VIEW public.v1 AS SELECT * FROM public.t1'
);
```

便捷重载可以支持：

```sql
SELECT pg_emit_logical_ddl(
    publication => 'pub1',
    ddl_sql     => 'CREATE INDEX idx_t1_id ON public.t1(id)'
);
```

注意，这里指定的是 publication，不是 subscription。

---

## 8. Catalog 设计

### 8.1 pg_publication

新增字段：

```c
int32 pubddl;
```

bitmask 定义建议如下：

```c
#define PUBDDL_TABLE      (1 << 0)
#define PUBDDL_INDEX      (1 << 1)
#define PUBDDL_SCHEMA     (1 << 2)
#define PUBDDL_TRIGGER    (1 << 3)
#define PUBDDL_VIEW       (1 << 4)
#define PUBDDL_FUNCTION   (1 << 5)
#define PUBDDL_TYPE       (1 << 6)
#define PUBDDL_DOMAIN     (1 << 7)
#define PUBDDL_RULE       (1 << 8)
#define PUBDDL_EXTENSION  (1 << 9)
```

### 8.2 pg_subscription

新增字段：

```c
int32 subddl;
bool  subenableddl;
bool  subenableddlmanual;
```

### 8.3 升级要求

由于涉及 catalog 字段扩展，需要：

* bump catalog version
* 更新 catalog `.dat`
* 为 `pg_upgrade` 设计升级路径
* 保证默认值为“关闭 DDL 复制”，以保持向后兼容

---

## 9. 核心内部数据结构

### 9.1 DDL 类型枚举

```c
typedef enum ReplicableDDLKind
{
    REPL_DDL_NONE = 0,
    REPL_DDL_TABLE,
    REPL_DDL_INDEX,
    REPL_DDL_SCHEMA,
    REPL_DDL_TRIGGER,
    REPL_DDL_VIEW,
    REPL_DDL_FUNCTION,
    REPL_DDL_TYPE,
    REPL_DDL_DOMAIN,
    REPL_DDL_RULE,
    REPL_DDL_EXTENSION
} ReplicableDDLKind;
```

### 9.2 来源类型枚举

```c
typedef enum ReplicatedDDLSource
{
    REPL_DDL_SOURCE_AUTOMATIC = 1,
    REPL_DDL_SOURCE_MANUAL    = 2
} ReplicatedDDLSource;
```

### 9.3 LogicalDDLCommand

```c
typedef struct LogicalDDLCommand
{
    ReplicableDDLKind   kind;
    ReplicatedDDLSource source;

    Oid     classid;
    Oid     objid;
    int32   objsubid;

    Oid     relid;
    Oid     nspid;

    char   *command_tag;
    char   *query_string;
    char   *normalized_sql;
    char   *object_identity;

    uint32  ddl_seqno;

    int     npubs;
    char  **pubnames;

    uint32  flags;
} LogicalDDLCommand;
```

其中：

* `query_string` 主要用于调试与日志
* `normalized_sql` 是真正发送并在订阅端执行的 SQL
* `object_identity` 是稳定对象标识
* `pubnames` 是 publication 名称集合，而不是源端 OID

---

## 10. automatic DDL 设计

### 10.1 捕获入口

automatic DDL 的统一挂点位于：

```c
standard_ProcessUtility(...)
```

在适当位置调用：

```c
cmd = BuildAutomaticLogicalDDLCommand(...);

if (cmd != NULL && should_publish_ddl(cmd))
    LogLogicalDDLMessage(cmd);
```

### 10.2 BuildAutomaticLogicalDDLCommand 设计目标

该函数负责：

1. 判断当前 utility statement 是否属于支持的 automatic DDL
2. 将 parse tree 识别并映射为内部 `kind`
3. 提取对象标识与作用域信息
4. 生成 `normalized_sql`
5. 匹配 publication，填充 `pubnames`

返回值：

* `NULL`：不是支持的 automatic DDL，或不命中 publication
* 非 `NULL`：构造好的 `LogicalDDLCommand`

### 10.3 automatic 识别流程

#### 第一步：识别 utility statement 类型

根据 `pstmt->utilityStmt` 的 parse tree 类型判断，例如：

* `T_CreateStmt`
* `T_IndexStmt`
* `T_ViewStmt`
* `T_CreateFunctionStmt`
* `T_CreateTrigStmt`
* `T_CreateSchemaStmt`
* `T_AlterTableStmt`
* `T_DropStmt`
* `T_RenameStmt`

#### 第二步：映射为 DDL kind

例如：

* `CreateStmt / AlterTableStmt / 部分 DropStmt` → `REPL_DDL_TABLE`
* `IndexStmt` → `REPL_DDL_INDEX`
* `ViewStmt` → `REPL_DDL_VIEW`
* `CreateFunctionStmt` → `REPL_DDL_FUNCTION`
* `CreateTrigStmt` → `REPL_DDL_TRIGGER`

#### 第三步：提取对象信息

提取并填充：

* `classid / objid / objsubid`
* `relid`
* `nspid`
* `object_identity`
* `command_tag`

#### 第四步：生成 `normalized_sql`

由系统根据 parse tree 和 catalog 生成尽量稳定、可重放、schema-qualified 的 SQL。

#### 第五步：匹配 publication

基于 `kind`、作用域和 publication 配置计算命中的 `pubnames`。

---

## 11. 对象类型识别与作用域匹配

### 11.1 table

包括：

* `CREATE TABLE`
* `ALTER TABLE`
* `DROP TABLE`
* `RENAME TABLE`
* 分区表相关 DDL

作用域匹配：

* `FOR TABLE`：命中
* `FOR TABLES IN SCHEMA`：命中
* `FOR ALL TABLES`：命中

### 11.2 index

包括：

* `CREATE INDEX`
* `ALTER INDEX`
* `DROP INDEX`

index 视为表附属对象，按基表 `relid` 进行作用域判断。

作用域匹配：

* `FOR TABLE`：命中所属表
* `FOR TABLES IN SCHEMA`：命中所属 schema
* `FOR ALL TABLES`：命中

### 11.3 trigger

包括：

* `CREATE TRIGGER`
* `DROP TRIGGER`
* `ALTER TRIGGER`
* 相关 enable/disable 触发器命令

trigger 按所属表判断作用域。

### 11.4 view

包括：

* `CREATE VIEW`
* `ALTER VIEW`
* `DROP VIEW`
* `CREATE OR REPLACE VIEW`

view 不是表附属对象，因此：

* 不命中 `FOR TABLE`
* 可命中 `FOR TABLES IN SCHEMA`
* 可命中 `FOR ALL TABLES` 的全局作用域语义

### 11.5 function

包括：

* `CREATE FUNCTION`
* `ALTER FUNCTION`
* `DROP FUNCTION`
* `CREATE OR REPLACE FUNCTION`

function 不附属于表，当前建议仅在全局 publication 下启用 automatic：

* 不命中 `FOR TABLE`
* 不命中 `FOR TABLES IN SCHEMA`
* 命中 `FOR ALL TABLES`

### 11.6 schema

包括：

* `CREATE SCHEMA`
* `ALTER SCHEMA`
* `DROP SCHEMA`

当前建议仅在全局 publication 下启用 automatic。

### 11.7 type / domain / rule

二期保留接口并逐步放开。初始建议仅在 `FOR ALL TABLES` 的全局 publication 下启用 automatic。

---

## 12. normalized_sql 生成规则

订阅端执行的是 `normalized_sql`，不是用户原始输入 SQL。因此必须定义清晰的规范化策略。

### 12.1 不能直接转发 query_string 的原因

原始 SQL 可能依赖：

* `search_path`
* 类型简写
* 未限定 schema 名称
* 会话局部 GUC
* 大小写与语法糖

直接转发容易导致订阅端执行到错误对象或产生语义偏差。

### 12.2 normalized_sql 的目标

* 语义稳定
* 可在订阅端直接执行
* 尽量不依赖发布端会话上下文
* 尽量全限定对象名
* 保留必要定义细节

### 12.3 基本规则

第一，对象名尽量 schema-qualified。
第二，函数参数类型要显式展开。
第三，view / rule / trigger function 的引用对象尽量全限定。
第四，函数定义中语言、返回类型、VOLATILE/STABLE/IMMUTABLE、SECURITY DEFINER/INVOKER 等信息不能丢失。

### 12.4 生成方式建议

automatic 优先借助系统已有 deparse / `pg_get_*def` 风格能力。
manual 虽然由用户提供 SQL，也仍建议 parse 后重新规范化，而不是原样透传。

---

## 13. manual DDL 设计

### 13.1 定义

manual DDL 是显式广播能力，不是 automatic DDL 的一种“挂起执行模式”。

### 13.2 `pg_emit_logical_ddl` 内核实现步骤

#### 第一步：解析参数

把 publication 或 publications 参数统一成 `pubnames[]`。

#### 第二步：权限检查

建议限制为：

* superuser，或
* 所有目标 publication 的 owner

#### 第三步：解析 SQL

对 `ddl_sql` 进行 parse，得到 parse tree。

#### 第四步：安全白名单校验

允许的 DDL 类型应与整体设计目标一致，例如：

* table
* index
* view
* function
* trigger
* schema

拒绝：

* database
* role
* tablespace
* ALTER SYSTEM
* replication objects
* event trigger
* publication / subscription 管理语句

#### 第五步：构造 `LogicalDDLCommand`

填充：

* `source = REPL_DDL_SOURCE_MANUAL`
* `kind`
* `object_identity`
* `normalized_sql`
* `command_tag`
* `classid / objid / objsubid`
* `relid / nspid`
* `pubnames[]`

#### 第六步：publication 能力校验

要求：

```text
manual ddl kind ∈ publication.pubddl
```

如果目标 publication 未声明支持该 kind，则报错。

#### 第七步：写入逻辑消息

调用：

```c
LogLogicalDDLMessage(cmd);
```

### 13.3 manual 的分发语义

manual 不直接指定 subscription，而是指定 publication。
最终由各个 subscription 根据其订阅的 publication 集合自行接收。
若用户想让一条 manual DDL 只到达某一个订阅者，应通过 publication 拓扑设计实现，而不是增加“按 subscription 定向发送”的新机制。

---

## 14. DDL WAL 写入机制

### 14.1 统一写入接口

```c
void LogLogicalDDLMessage(LogicalDDLCommand *cmd);
```

automatic / manual 最终都只通过该接口进入 WAL。

### 14.2 写入形式

本设计不引入新的 rmgr，而是复用 PostgreSQL 现有 logical message 机制，写入一条 **transactional logical message**。

推荐固定 prefix：

```text
pg_ddl
```

### 14.3 为什么使用 logical message

第一，侵入性最小。
第二，天然进入 reorder buffer。
第三，最符合 DDL 作为高层逻辑语义消息的本质。

### 14.4 payload 内容

建议包括：

```text
version
source
kind
flags
ddl_seqno
classid
objid
objsubid
relid
nspid
command_tag
object_identity
normalized_sql
npubs
pubnames[]
```

### 14.5 ddl_seqno

为了表达同一事务内多条 DDL 的先后顺序，需要维护事务内递增的 `ddl_seqno`。建议在 `LogLogicalDDLMessage()` 中统一分配。

---

## 15. DDL 的逻辑解码机制

### 15.1 核心原则

DDL 复制不是从普通 WAL redo record 中反推 DDL 语义，而是在 DDL 执行路径上主动写入一条高层 logical message。这样 logic decoding 只需要识别和解码这条 message，不需要理解底层 catalog 物理变更的组合语义。

### 15.2 解码过程

第一步，logical decoding 从 slot 位置继续读取 WAL。
第二步，当识别到 `pg_ddl` transactional logical message 时，将其解码为一条 message change。
第三步，这条 message change 与同事务内的 DML change 一起进入 ReorderBuffer。
第四步，在事务提交时，输出插件按 ReorderBuffer 中的顺序回调并输出。

### 15.3 为什么顺序可保证

顺序来源于三层：

* WAL 中的实际写入顺序
* ReorderBuffer 中的事务内 change 顺序
* output plugin 顺序输出

因此，顺序不是 apply 端推断出来的，而是在发布端执行时就已经固化进事务流。

---

## 16. DDL message 协议格式

新增逻辑复制消息类型：

```text
Byte1('D')
```

建议消息体格式：

```text
Byte1    'D'
Int32    proto_version
Int8     source
Int8     kind
Int32    flags
Int32    ddl_seqno
Int32    classid
Int32    objid
Int32    objsubid
Int32    relid
Int32    nspid
String   command_tag
String   object_identity
String   normalized_sql
Int32    npubs
String[] pubnames
```

### 16.1 为什么使用 pubnames 而不是 publication OID

publication OID 是发布端本地标识，订阅端不应依赖。
publication 名称更稳定，也更符合 subscription 本地配置的匹配模型。

---

## 17. 执行顺序详细说明

这一节是整个设计的核心。

### 17.1 总原则

订阅端执行顺序必须与发布端事务内顺序严格一致。

### 17.2 automatic DDL 与 DML 顺序

示例：

```sql
BEGIN;
CREATE TABLE t1(id int);
INSERT INTO t1 VALUES (1);
COMMIT;
```

发布端顺序：

1. `CREATE TABLE` 触发 `LogLogicalDDLMessage()`
2. `INSERT` 形成普通 DML change
3. `COMMIT`

订阅端必须按如下顺序执行：

1. BEGIN
2. DDL：CREATE TABLE
3. DML：INSERT
4. COMMIT

### 17.3 多条 automatic DDL 顺序

示例：

```sql
BEGIN;
CREATE TABLE t1(id int);
CREATE INDEX idx_t1_id ON t1(id);
COMMIT;
```

顺序必须为：

1. CREATE TABLE
2. CREATE INDEX

`ddl_seqno` 用于在事务内表达这一先后关系。

### 17.4 manual DDL 与 automatic DDL 混合顺序

示例：

```sql
BEGIN;
CREATE TABLE t1(id int);
SELECT pg_emit_logical_ddl('pub1',
  'CREATE VIEW public.v1 AS SELECT * FROM public.t1');
INSERT INTO t1 VALUES (1);
COMMIT;
```

订阅端必须执行：

1. CREATE TABLE
2. CREATE VIEW
3. INSERT
4. COMMIT

manual 只是入口不同，进入 WAL 后就是正式事务流的一部分。

### 17.5 apply 端不能自行重排

apply worker 只能按流顺序执行，不能根据对象依赖自行排序或延后执行。
因为：

* 正确顺序已经由 WAL 与 reorder buffer 保证
* 重排会破坏与发布端的一致性
* 可能引入难以发现的语义偏差

---

## 18. pgoutput 扩展设计

### 18.1 协议协商参数

在 `START_REPLICATION ... LOGICAL` 中增加：

```text
ddl=true|false
ddl_proto_version=1
```

语义为：

* 订阅端是否理解 DDL 消息
* 使用哪一版 DDL message 格式

### 18.2 输出过滤职责

`pgoutput` 在拿到 DDL message 后只做一层薄过滤：

```text
cmd.pubnames ∩ current_subscription_publications != ∅
```

命中则发送 `'D'` 消息，不命中则跳过。

automatic / manual 的对象匹配与 publication 校验应尽量前移到发布端完成；`pgoutput` 只负责基于 publication membership 做最终选择。

---

## 19. apply worker 设计

### 19.1 消息分发入口

在 `worker.c` 中增加：

```c
case LOGICAL_REP_MSG_DDL:
    apply_handle_ddl(s);
    break;
```

### 19.2 apply_handle_ddl

```c
static void
apply_handle_ddl(StringInfo s)
{
    LogicalDDLCommand *cmd;

    cmd = logicalrep_read_ddl(s);

    if (!MySubscription->subenableddl)
        return;

    if (cmd->source == REPL_DDL_SOURCE_MANUAL &&
        !MySubscription->subenableddlmanual)
        return;

    if (!subscription_accepts_ddl_kind(MySubscription, cmd->kind))
        return;

    if (ddl_already_applied(cmd))
        return;

    execute_replicated_ddl(cmd);
}
```

### 19.3 execute_replicated_ddl

执行要求：

* 在 apply worker 的远端事务上下文中执行
* 只执行 `normalized_sql`
* 采用受控 `search_path`
* 必要时刷新 relcache / syscache / inval 状态

---

## 20. 幂等与重复投递

建议使用如下去重键：

```text
(origin_id, xid, end_lsn, ddl_seqno)
```

`ddl_already_applied()` 至少应支持：

* 消息级幂等判断
* 在对象已存在且定义兼容时的保守跳过

一期可以先做最小能力，二期逐步增强。

---

## 21. 错误处理

automatic 与 manual 一旦进入复制流，错误处理语义完全一致。

若订阅端执行 DDL 失败：

* abort 当前远端事务
* apply worker 报错
* subscription 停止或按既有 disable-on-error 策略处理

原因在于，DDL 失败往往意味着 schema 已经分叉，自动跳过会让后续 DML 在错误 schema 上继续运行，风险更高。

---

## 22. 安全模型

### 22.1 automatic DDL

automatic DDL 本质上允许发布端通过复制流驱动订阅端执行受控 utility statement，因此应明确文档警告：只适用于强信任拓扑。

执行身份建议继续沿用 subscription owner 语义，不引入更复杂的“按对象 owner 切换”模型。

### 22.2 manual DDL

manual DDL 更敏感，因为它允许显式广播受控 DDL。建议：

* 调用权限仅限 superuser 或目标 publication owner
* 必须经过受控白名单校验
* 必须在文档中明确安全风险

---

## 23. 向后兼容与升级

### 23.1 默认关闭 DDL 复制

若 publication / subscription 未设置 DDL 参数，则默认行为与当前 PostgreSQL 一致：

* 不复制任何 DDL
* 不影响现有逻辑复制行为

### 23.2 协议兼容

若输出插件或订阅端未协商 `ddl=true` / `ddl_proto_version=1`，则发布端不应发送 `'D'` 消息，或应在连接建立时明确报不兼容错误，避免 silent mismatch。

---

## 24. 测试方案

### 24.1 automatic 基础测试

* CREATE TABLE + INSERT
* ALTER TABLE + UPDATE
* CREATE INDEX / DROP INDEX
* CREATE VIEW
* CREATE FUNCTION
* CREATE TRIGGER

### 24.2 manual 基础测试

* 手动发送 CREATE INDEX
* 手动发送 CREATE VIEW
* 手动发送 CREATE FUNCTION

### 24.3 混合顺序测试

```sql
BEGIN;
CREATE TABLE t1(id int);
SELECT pg_emit_logical_ddl('pub1',
  'CREATE VIEW public.v1 AS SELECT * FROM public.t1');
INSERT INTO t1 VALUES (1);
COMMIT;
```

验证订阅端执行顺序严格一致。

### 24.4 错误路径测试

* publication 不支持该 kind，manual 报错
* subscription 禁 manual，manual DDL 被拒收
* 订阅端对象冲突
* 权限不足导致 apply 失败
* 逻辑复制重启后幂等处理

---

## 25. Patch 切分建议

### Patch 1

catalog 与参数解析：

* `pg_publication`
* `pg_subscription`
* publication/subscription DDL 参数

### Patch 2

`LogicalDDLCommand` 与协议定义：

* `logicalddl.h`
* `logicalddl.c`
* `logicalproto.h`
* `proto.c`

### Patch 3

automatic DDL 捕获：

* `utility.c`
* `BuildAutomaticLogicalDDLCommand`

### Patch 4

manual DDL 广播：

* `pg_emit_logical_ddl`
* parse / validate / normalize / emit

### Patch 5

WAL / pgoutput / worker：

* `LogLogicalDDLMessage`
* `'D'` 消息
* `apply_handle_ddl`

### Patch 6

测试与文档：

* TAP / isolation tests
* SGML 文档补充

---

## 26. 源码改造点总表

关键改动文件建议包括：

* `src/include/catalog/pg_publication.h`
* `src/include/catalog/pg_subscription.h`
* `src/backend/commands/publicationcmds.c`
* `src/backend/commands/subscriptioncmds.c`
* `src/backend/tcop/utility.c`
* `src/backend/replication/logical/logicalddl.c`
* `src/include/replication/logicalddl.h`
* `src/backend/replication/pgoutput/pgoutput.c`
* `src/backend/replication/logical/proto.c`
* `src/include/replication/logicalproto.h`
* `src/backend/replication/logical/worker.c`

---

## 27. 结论

本设计的核心结论可以概括为四点：

第一，automatic DDL 与 manual DDL 是两条不同入口。
第二，二者统一转换为 `LogicalDDLCommand` 并写入 transactional logical message。
第三，DDL 不从普通物理 WAL record 反推，而是在执行路径主动生成高层逻辑消息。
第四，执行顺序由 WAL 顺序、ReorderBuffer 顺序和 output/apply 顺序天然保证，订阅端只需按流顺序执行。

这样，一期与二期可以被统一纳入同一个架构之下：
automatic 解决“系统应自动复制哪些 DDL”，manual 解决“用户现在要显式广播哪条 DDL”，而两者在进入复制流之后共享完全一致的复制语义和一致性边界。
