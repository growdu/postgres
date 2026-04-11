
# PostgreSQL 逻辑复制支持 DDL（二期详细设计）

---

## 当前实现状态（截至 2026-04-11）

| 模块 | 文档目标 | 当前代码状态 |
| --- | --- | --- |
| automatic 扩展对象类型 | schema/trigger/view/function/type/domain/rule/extension | 已实现 |
| publication/subscription `ddl` 过滤 | `pubddl/subddl` 位图与校验 | 已实现 |
| manual DDL（`pg_emit_logical_ddl`） | 手动广播入口 | 未实现（本文件为设计预留） |

---

## 1. 目标与范围

### 1.1 目标

在一期支持 `table/index` 自动DDL复制基础上，实现：

### （1）扩展 automatic DDL 能力

支持更多对象类型自动复制：

```text
schema / trigger / view / function / type / domain / rule
```

---

### （2）设计预留 manual DDL 广播能力（当前未实现）

提供 SQL 接口：

> 用户显式提交一条 DDL，由系统广播到指定 publication 的复制流中

---

### 1.2 非目标（明确不做）

```text
❌ 不实现 pending / 队列
❌ 不实现手动确认执行流程
❌ 不实现“延迟执行DDL”
❌ 不实现按 subscription 定向发送
```

---

## 2. 总体架构

---

## 2.1 两条入口

```text
                ┌──────────────────────┐
                │      用户 DDL        │
                └──────────┬───────────┘
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
┌───────────────┐                   ┌────────────────────┐
│ automatic DDL │                   │  manual DDL        │
│ ProcessUtility│                   │ SQL函数调用         │
└──────┬────────┘                   └────────┬───────────┘
       │                                     │
       └──────────────┬──────────────────────┘
                      │
          Build LogicalDDLCommand
                      │
          LogLogicalDDLMessage
                      │
                  WAL (logical message)
                      │
                logical decoding
                      │
                  pgoutput (D)
                      │
                 apply worker
                      │
             execute_replicated_ddl
```

---

## 2.2 核心设计原则

### 原则 1：automatic 与 manual 入口分离

| 类型        | 入口             | 控制方式     |
| --------- | -------------- | -------- |
| automatic | ProcessUtility | ddl 类型过滤 |
| manual    | SQL函数          | 用户显式指定   |

---

### 原则 2：统一复制链路

```text
automatic == manual
    ↓
LogicalDDLCommand
    ↓
WAL logical message
    ↓
pgoutput
    ↓
apply worker
```

---

### 原则 3：publication 作为唯一分发单位

```text
DDL → publication → subscription
```

不支持：

```text
DDL → subscription（❌）
```

---

## 3. Catalog 设计

---

## 3.1 pg_publication 扩展

新增字段：

```c
int32 pubddl;
```

---

### bitmask 定义

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

---

## 3.2 pg_subscription 扩展

新增字段：

```c
int32 subddl;                /* 接受的DDL类型 */
```

说明：

```text
当前代码仅包含 subddl。
subenableddl / subenableddlmanual 作为后续候选扩展，尚未落地。
```

---

## 4. 核心数据结构

---

## 4.1 DDL 类型

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

---

## 4.2 来源类型

```c
typedef enum ReplicatedDDLSource
{
    REPL_DDL_SOURCE_AUTOMATIC = 1,
    REPL_DDL_SOURCE_MANUAL    = 2
} ReplicatedDDLSource;
```

---

## 4.3 LogicalDDLCommand

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

---

## 5. Automatic DDL 设计

---

## 5.1 捕获入口

在：

```c
standard_ProcessUtility()
```

增加：

```c
cmd = BuildAutomaticLogicalDDLCommand(...);

if (cmd && should_publish_ddl(cmd))
    LogLogicalDDLMessage(cmd);
```

---

## 5.2 构造函数

```c
LogicalDDLCommand *
BuildAutomaticLogicalDDLCommand(...)
```

---

## 5.3 支持的 DDL

### 第一阶段（必须实现）

| 类型       | 示例              |
| -------- | --------------- |
| schema   | CREATE SCHEMA   |
| trigger  | CREATE TRIGGER  |
| view     | CREATE VIEW     |
| function | CREATE FUNCTION |

---

### 第二阶段

| 类型     |
| ------ |
| type   |
| domain |
| rule   |

---

### 不支持 automatic

```text
extension
```

---

## 5.4 过滤流程

```text
1. 是否DDL
2. 是否支持的kind
3. 是否允许automatic
4. publication.ddl是否包含该kind
5. 是否命中作用域
```

---

## 5.5 作用域规则

| kind     | FOR TABLE | FOR SCHEMA | FOR ALL |
| -------- | --------- | ---------- | ------- |
| table    | ✔         | ✔          | ✔       |
| index    | ✔         | ✔          | ✔       |
| trigger  | ✔（依附表）    | ✔          | ✔       |
| view     | ✖         | ✔          | ✔       |
| function | ✖         | ✖          | ✔       |
| schema   | ✖         | ✖          | ✔       |

---

## 6. Manual DDL 设计

---

> 本章为预留设计，当前代码尚未实现。

## 6.1 SQL接口

```sql
SELECT pg_emit_logical_ddl(
    publications => ARRAY['pub1'],
    ddl_sql      => 'CREATE VIEW public.v1 AS SELECT * FROM t'
);
```

---

## 6.2 函数定义

```c
Datum pg_emit_logical_ddl(PG_FUNCTION_ARGS);
```

---

## 6.3 执行流程

```text
1. 校验权限（superuser / publication owner）
2. parse ddl_sql
3. 识别 kind / object_identity
4. 生成 normalized_sql
5. 校验 publication 支持该 kind
6. 构造 LogicalDDLCommand
7. LogLogicalDDLMessage
```

---

## 6.4 安全限制

禁止：

```text
CREATE DATABASE
ALTER SYSTEM
ROLE
TABLESPACE
REPLICATION SLOT
```

---

## 6.5 publication 校验

```text
manual ddl kind ⊆ publication.pubddl
```

---

## 7. WAL 与 Logical Message

---

## 7.1 写入接口

```c
void LogLogicalDDLMessage(LogicalDDLCommand *cmd);
```

---

## 7.2 payload

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

---

## 7.3 事务顺序

```text
BEGIN
DDL#1
DDL#2
DML
COMMIT
```

---

## 8. pgoutput 扩展

---

## 8.1 新消息类型

```text
'D'  -- DDL
```

---

## 8.2 协议参数

```text
ddl=true|false
ddl_proto_version=1
```

---

## 8.3 过滤逻辑

```c
if (cmd.pubnames ∩ subscription.publications != ∅)
    send;
else
    skip;
```

---

## 9. Apply Worker

---

## 9.1 分发入口

```c
case LOGICAL_REP_MSG_DDL:
    apply_handle_ddl(s);
```

---

## 9.2 执行逻辑

```c
static void apply_handle_ddl(StringInfo s)
{
    cmd = logicalrep_read_ddl(s);

    if (!(cmd->kind & subddl))
        return;

    if (ddl_already_applied(cmd))
        return;

    execute_replicated_ddl(cmd);
}
```

当前实现说明：

```text
manual source 分支尚未落地；
执行过滤以 subddl 为主。
```

---

## 9.3 执行函数

```c
execute_replicated_ddl(cmd)
{
    set search_path
    exec normalized_sql
    invalidate cache
}
```

---

## 10. 幂等性

---

## 10.1 去重键

```text
(origin_id, xid, end_lsn, ddl_seqno)
```

---

## 10.2 执行前检查

```c
if (ddl_already_applied(cmd))
    skip;
```

---

## 11. 错误处理

---

## 11.1 统一策略

```text
automatic == manual
```

失败行为：

```text
1. abort 当前事务
2. apply worker 报错
3. subscription 停止
```

---

## 12. 测试方案

---

### automatic

* CREATE VIEW
* CREATE FUNCTION
* CREATE TRIGGER
* schema 过滤

---

### manual

* emit create index
* emit create view
* emit create function

---

### 混合

```sql
BEGIN;
CREATE TABLE t1(...);
SELECT pg_emit_logical_ddl(...);
INSERT INTO t1 VALUES (...);
COMMIT;
```

---

### 错误

* 权限不足
* SQL非法
* 对象冲突

---

## 13. Patch 拆分

---

### Patch 1

catalog + ddl bitmask

---

### Patch 2

LogicalDDLCommand + message

---

### Patch 3

automatic DDL 扩展

---

### Patch 4

manual DDL 函数

---

### Patch 5

pgoutput + worker

---

### Patch 6

测试 + 文档

---

# 14. 最终总结

---

## 核心模型

```text
automatic:
  系统决定“要不要复制”

manual:
  用户决定“复制什么”

两者：
  ↓
统一复制链路
```

---

## 最关键设计点

### 1️⃣ 不引入 pending（极大降低复杂度）

### 2️⃣ publication 是唯一分发单位

### 3️⃣ automatic / manual 入口分离，下游统一
