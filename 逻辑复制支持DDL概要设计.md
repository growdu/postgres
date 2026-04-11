下面给出一版**统一的总体设计文档（一期+二期+三期+四期整合）**。
文档同时覆盖：

* 已落地能力（当前代码）
* 规划能力（尚未实现）
* 分期演进路线

---

# PostgreSQL 逻辑复制支持 DDL（总体设计与分期路线）

## 当前实现状态（截至 2026-04-11）

| 阶段 | 目标 | 当前代码状态 |
| --- | --- | --- |
| 一期 | automatic DDL（table/index） | 已实现 |
| 二期 | automatic 扩展对象（type/function/domain/trigger/view/rule/schema/extension） | 已实现 |
| 二期（manual） | `pg_emit_logical_ddl` 手动广播 | 未实现（预留） |
| 三期 | initial schema sync（SchemaSyncWorker） | 未实现（预留） |
| 四期 | refresh 对象级 delta/cleanup | 未实现（预留） |

---

# 1. 摘要

本文提出 PostgreSQL 内建逻辑复制支持 DDL 的分期方案：

```text
一期：automatic DDL（table/index）
二期：扩展 automatic DDL（manual 预留）
三期：initial schema sync（启动一致性）
四期：publication 变更收敛（refresh 对象级扩展）
```

核心思想：

```text
DDL 作为逻辑复制的一等消息
通过 WAL logical message 进入复制流
与 DML 保持事务级顺序一致
```

目标上：

```text
initial schema sync + incremental DDL + refresh delta
形成完整闭环
```

---

# 2. 设计目标

---

## 2.1 核心目标

1. 支持 DDL 自动复制（automatic）
2. 预留 DDL 手动广播（manual）
3. 保证 DDL + DML 顺序一致
4. 预留 subscription 初始化 schema（initial sync）
5. 与 publication/subscription 模型一致
6. 不引入额外状态系统

---

## 2.2 非目标

```text
❌ 不引入 pending / queue
❌ 不支持延迟执行 DDL
❌ 不支持按 subscription 定向发送
❌ 不从 WAL redo 反推 DDL
```

---

# 3. 总体架构

---

## 3.1 三阶段统一模型

```text
                ┌─────────────────────────┐
                │   CREATE SUBSCRIPTION   │
                └──────────┬──────────────┘
                           │
                ┌──────────▼──────────┐
                │ Phase 3: Initial Sync│
                │ SchemaSyncWorker     │
                └──────────┬──────────┘
                           │
                ┌──────────▼──────────┐
                │ Phase 1/2: 增量复制  │
                │ WAL → decoding       │
                │ → pgoutput → apply   │
                └──────────────────────┘
```

---

## 3.2 双入口统一链路

```text
automatic DDL        manual DDL
(ProcessUtility)     (SQL函数)
        │                  │
        └──────┬───────────┘
               │
      LogicalDDLCommand
               │
      LogLogicalDDLMessage
               │
      WAL logical message
               │
      logical decoding
               │
      pgoutput 'D'
               │
      apply worker
```

---

## 3.3 核心原则

### 原则1：入口分离，下游统一

```text
automatic ≠ manual（入口不同）
automatic == manual（复制链路一致）
```

---

### 原则2：publication 为唯一分发单位

```text
DDL → publication → subscription
```

---

### 原则3：initial + incremental 闭环

```text
initial sync：解决起点一致
incremental：解决持续一致
```

---

# 4. 功能范围

---

## 4.1 一期

支持：

```text
table
index
```

---

## 4.2 二期

### automatic 扩展

```text
schema
trigger
view
function
type
domain
rule
```

---

### manual

```text
pg_emit_logical_ddl()
```

---

## 4.3 三期

```text
initial schema sync
```

---

# 5. 用户接口

---

## 5.1 publication

```sql
CREATE PUBLICATION pub
FOR ALL TABLES
WITH (ddl='table,index,view,function,trigger');
```

---

## 5.2 subscription（当前实现）

```sql
CREATE SUBSCRIPTION sub
CONNECTION '...'
PUBLICATION pub
WITH (
  ddl='table,index'
);
```

---

## 5.3 manual DDL（预留）

```sql
SELECT pg_emit_logical_ddl(
  publications => ARRAY['pub1'],
  ddl_sql => 'CREATE VIEW public.v1 AS SELECT * FROM t'
);
```

---

# 6. Catalog 设计

---

## 6.1 pg_publication

```c
int32 pubddl;
```

---

## 6.2 pg_subscription（当前实现）

```c
int32 subddl;
```

---

# 7. 核心数据结构

---

## 7.1 LogicalDDLCommand

```c
typedef struct LogicalDDLCommand
{
    ReplicableDDLKind kind;
    ReplicatedDDLSource source;

    Oid relid;
    Oid nspid;

    char *command_tag;
    char *normalized_sql;
    char *object_identity;

    uint32 ddl_seqno;

    int npubs;
    char **pubnames;
} LogicalDDLCommand;
```

---

# 8. automatic DDL 设计

---

## 8.1 入口

```c
standard_ProcessUtility()
```

---

## 8.2 核心流程

```text
识别DDL
→ 构造 LogicalDDLCommand
→ publication 过滤
→ 写 WAL message
```

---

## 8.3 BuildAutomaticLogicalDDLCommand

负责：

```text
1. parse tree 识别
2. kind 映射
3. object_identity 提取
4. normalized_sql 生成
5. publication 匹配
```

---

## 8.4 作用域规则

| 类型       | FOR TABLE | FOR SCHEMA | FOR ALL |
| -------- | --------- | ---------- | ------- |
| table    | ✔         | ✔          | ✔       |
| index    | ✔         | ✔          | ✔       |
| trigger  | ✔         | ✔          | ✔       |
| view     | ✖         | ✔          | ✔       |
| function | ✖         | ✖          | ✔       |
| schema   | ✖         | ✖          | ✔       |

---

# 9. manual DDL 设计（预留）

---

## 9.1 执行流程

```text
SQL函数
→ parse ddl_sql
→ 校验白名单
→ 构造 LogicalDDLCommand
→ LogLogicalDDLMessage
```

---

## 9.2 特点（目标）

```text
✔ 显式广播
✔ publication 绑定
✔ 不依赖 subscription
✔ 与 automatic 完全统一
```

---

# 10. WAL 写入机制

---

## 10.1 接口

```c
LogLogicalDDLMessage(cmd);
```

---

## 10.2 类型

```text
transactional logical message
prefix = "pg_ddl"
```

---

## 10.3 payload

```text
source
kind
ddl_seqno
normalized_sql
object_identity
pubnames
```

---

# 11. 逻辑解码机制

---

## 11.1 流程

```text
WAL → logical decoding → ReorderBuffer → pgoutput
```

---

## 11.2 核心点

```text
DDL message 与 DML 一起进入事务流
```

---

## 11.3 顺序来源

```text
WAL顺序 + ReorderBuffer顺序
```

---

# 12. 执行顺序

---

## 12.1 单事务

```text
BEGIN
DDL1
DDL2
DML
COMMIT
```

---

## 12.2 apply 执行

```text
严格按顺序执行
禁止重排
```

---

# 13. initial schema sync（Phase 3，预留）

---

## 13.1 目标（未实现）

```text
在 subscription 初始化时
构建 schema
```

---

## 13.2 执行流程

```text
CREATE SUBSCRIPTION
→ snapshot
→ SchemaSyncWorker
→ TablesyncWorker
→ ApplyWorker
```

---

## 13.3 SchemaSyncWorker（未实现）

---

### 输入

```text
publication
ddl bitmask
snapshot
```

---

### 步骤

```text
1. 枚举对象
2. 过滤
3. 生成 DDL
4. 拓扑排序
5. 执行
```

---

## 13.4 排序规则

```text
schema
→ table
→ type
→ function
→ view
→ trigger
→ index
```

---

## 13.5 范围控制

```text
由 publication.ddl 决定
```

---

# 14. snapshot 边界

---

```text
snapshot 前 → initial sync
snapshot 后 → WAL replication
```

---

# 15. pgoutput 扩展

---

## 15.1 新消息

```text
'D' = DDL
```

---

## 15.2 过滤

```text
pubnames ∩ subscription.publications
```

---

# 16. apply worker（当前实现）

---

## 16.1 处理逻辑

```c
if (!(kind ∈ subddl)) return;

execute_replicated_ddl();
```

---

# 17. 幂等性

---

```text
(origin_id, xid, lsn, ddl_seqno)
```

---

# 18. 错误处理

---

```text
DDL失败 → 事务回滚 → worker停止
```

---

# 19. 正确性保证

---

## ✔ 不丢DDL

initial + WAL 覆盖所有 DDL

---

## ✔ 不重复

initial ≠ WAL

---

## ✔ 顺序一致

WAL + reorder buffer 保证

---

## ✔ schema先于数据

SchemaSync → Tablesync → Apply

---

# 20. 安全模型

---

## automatic

```text
信任发布端
```

---

## manual

```text
仅 superuser / publication owner
```

---

# 21. Patch 切分

---

```text
Patch 1：catalog
Patch 2：LogicalDDLCommand
Patch 3：automatic（table）
Patch 4：initial sync（table）
Patch 5：pgoutput + worker
Patch 6：index
Patch 7：其他对象
Patch 8：manual
```

---

# 22. 最终总结

---

## 核心模型

```text
DDL replication =
  initial schema sync
  + incremental DDL (logical message)
```

---

## 三期闭环

```text
Phase 3：起点一致
Phase 1/2：持续一致
```

---

## 最关键设计点

### 1️⃣ DDL 进入 WAL 逻辑消息

### 2️⃣ publication 控制范围

### 3️⃣ automatic / manual 统一

### 4️⃣ initial sync 补齐体系

---

# 四期补充章节

# 23. Publication 变更与 Refresh 语义（四期规划，未实现）

本节定义当 publication 被删除、修改，或订阅端执行 `REFRESH PUBLICATION` 时的目标行为。该章节属于四期规划，当前代码尚未实现对象级 refresh 扩展。章节目标是把：

* initial schema sync
* 增量 DDL/DML 复制
* publication 路由变更

统一成一套可预期、可实现的规则。

---

## 23.1 设计原则

### 原则 1：publication 变更属于复制拓扑变更，不属于普通 DDL 复制对象

`DROP PUBLICATION` 仅删除 publication 元数据；官方文档明确说明 publication 没有依赖对象，因此 `CASCADE/RESTRICT` 实际上不起作用。([PostgreSQL][1])

因此在本设计中：

* publication / subscription 管理语句**不进入 DDL replication 范围**
* 不通过 `LogicalDDLCommand` 复制
* 不通过 `'D'` 消息发送到订阅端执行

---

### 原则 2：refresh 的本质是“订阅范围重对齐”

官方文档对 `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` 的定义是：从 publisher 获取缺失的表信息，开始复制新增到 publication 的表，并移除已不再属于 publication 的关系及其 table sync slots。([PostgreSQL][2])

在本设计中，这个语义要从“表级”扩展到“对象级”：

* 表：沿用现有 PostgreSQL 语义
* DDL 对象：扩展为对象级 delta schema sync

---

### 原则 3：refresh 只影响“后续复制范围”，不自动回滚已同步对象

本设计不把 publication 视为“订阅端 schema 的强制镜像定义”，而把它视为“后续哪些对象变化需要继续复制”的控制面。
因此：

* **新增进入范围的对象**：通过 refresh 补齐
* **移出范围的对象**：停止后续复制
* **本地已存在对象**：默认不自动 drop

---

## 23.2 删除 publication（Publisher 侧）

### 23.2.1 发布端行为

当执行：

```sql
DROP PUBLICATION pub1;
```

发布端行为为：

1. 删除 publication 元数据
2. 该操作**不进入 DDL replication**
3. 提交后，新事务不再命中该 publication
4. 删除前已经进入 WAL、并且已经归属到该 publication 的 DDL/DML，仍按原有 publication 归属继续复制

这里采用“事务边界生效”语义，也就是：

```text
publication 删除提交前 → 已归属消息仍有效
publication 删除提交后 → 新消息不再归属该 publication
```

---

### 23.2.2 订阅端行为

如果某个 subscription 仍然配置订阅了这个已不存在的 publication，则订阅端不应静默成功，而应在后续 refresh / 重连 / 元数据校验阶段报错并停止 worker。

这一点与 PostgreSQL 现有 `ALTER SUBSCRIPTION` 语义是兼容的：subscription 本地保存的是 publication 列表，而 `SET/ADD/DROP PUBLICATION` 用于修改该列表，默认还会触发 refresh。([PostgreSQL][2])

因此，推荐语义如下：

### 规则 A：不自动删除已同步对象

订阅端不会因为 publisher 删除了 publication，就自动执行：

* `DROP TABLE`
* `DROP VIEW`
* `DROP FUNCTION`
* `DROP INDEX`
* `DROP TRIGGER`

### 规则 B：停止后续接收该 publication 的变化

从 publication 删除提交点之后，属于该 publication 的后续变化不再产生。

### 规则 C：若 subscription 仍引用该 publication，则报错并停止

要求 DBA 显式修复订阅配置，例如：

```sql
ALTER SUBSCRIPTION sub1 DROP PUBLICATION pub1;
```

或：

```sql
ALTER SUBSCRIPTION sub1 SET PUBLICATION pub2;
```

---

## 23.3 修改 publication（Publisher 侧）

publication 修改主要有三类：

1. 修改 publication 的对象集合
   例如：`ADD TABLE` / `DROP TABLE`
2. 修改 publication 的 DDL 类型集合
   例如：`SET (ddl='table,index,view')`
3. 修改 subscription 订阅的 publication 列表
   例如：`SET/ADD/DROP PUBLICATION`

这些变化都不会通过 DDL replication 自动“通知订阅端重建 schema”，而是通过**订阅端 refresh**生效。

---

## 23.4 REFRESH PUBLICATION（Subscriber 侧）

### 23.4.1 现有 PostgreSQL 行为

官方文档说明：

* `REFRESH PUBLICATION` 会获取缺失的表信息
* 会开始复制新增到 publication 的表
* 会移除已不再属于 publication 的关系
* 还会移除对应的 table synchronization slots。([PostgreSQL][2])

因此，在现有 PostgreSQL 中，refresh 本质上已经是一次“订阅范围重对齐”。

---

### 23.4.2 本设计中的扩展语义（未实现）

在本设计下，`REFRESH PUBLICATION` 不应仅限于“表级 refresh”，而应扩展为：

```text
表级 refresh
+
对象级 delta schema sync
```

即：

### 第一层：表级 refresh

沿用现有 PostgreSQL 行为：

* 为新增表建立同步元数据
* 如 `copy_data = true`，复制表中既有数据
* 对移出 publication 的表，移除其同步元数据与 table sync slots。([PostgreSQL][2])

### 第二层：对象级 delta schema sync

对新增进入当前 publication.ddl 覆盖范围的 schema 对象：

* 枚举对象
* 过滤
* 生成 normalized DDL
* 按依赖顺序执行

这一步等价于把三期 `SchemaSyncWorker` 的能力复用到 refresh 上，但处理范围是“增量补齐”，而不是首次全量初始化。

---

## 23.5 refresh 对“新增对象”的处理

当 publication 范围扩大时，例如：

```sql
ALTER PUBLICATION pub1 ADD TABLE t2;
ALTER PUBLICATION pub1 SET (ddl='table,index,view');
ALTER SUBSCRIPTION sub1 ADD PUBLICATION pub2;
```

然后订阅端执行：

```sql
ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;
```

订阅端行为定义如下。

### 23.5.1 对表

沿用现有 PostgreSQL 行为：

* 新增表进入复制范围
* 如启用 `copy_data = true`，执行初始数据 copy
* 后续继续接收该表的 DML / DDL 增量。([PostgreSQL][2])

### 23.5.2 对 DDL 对象

执行一次对象级 delta schema sync，补齐当前 publication.ddl 范围内、但订阅端此前未纳入同步的对象。

例如 publication 从：

```text
ddl='table,index'
```

改为：

```text
ddl='table,index,view,function'
```

则 refresh 后，subscriber 应补齐：

* 已存在但此前未同步的 view
* 已存在但此前未同步的 function

否则这些对象若早于 refresh 已存在于 publisher，就永远无法靠增量 WAL 自动补齐。

---

## 23.6 refresh 对“移出对象”的处理

当 publication 范围缩小时，例如：

```sql
ALTER PUBLICATION pub1 DROP TABLE t1;
ALTER PUBLICATION pub1 SET (ddl='table');
ALTER SUBSCRIPTION sub1 DROP PUBLICATION pub2;
```

再执行 refresh。

推荐语义如下：

### 规则 A：停止后续复制

被移出范围的对象从 refresh 生效后，不再接收后续 DDL / DML 增量。

### 规则 B：不自动 drop 本地对象

subscriber 上已经存在的对象默认保留，不自动执行反向清理。

例如：

* 不自动 `DROP TABLE`
* 不自动 `DROP VIEW`
* 不自动 `DROP FUNCTION`

### 规则 C：refresh 的“删除语义”仅作用于同步关系，不作用于本地 schema 回滚

也就是说，refresh 调整的是：

```text
今后继续复制什么
```

而不是：

```text
把本地 schema 回退到 publication 当前集合的精确镜像
```

这样做的原因是：

1. 风险更低
2. 与当前 PostgreSQL 表级 refresh 行为更一致
3. 避免本地依赖对象被意外删除

---

## 23.7 refresh 与 initial schema sync 的关系

本设计把 subscription 生命周期中的 schema 同步分成两类：

### 1. Initial Schema Sync

在 `CREATE SUBSCRIPTION` 时执行，负责构建“起点一致性”。

### 2. Delta Schema Sync

在 `REFRESH PUBLICATION` 时执行，负责在 publication 范围变化后“追平新范围”。

二者共用相同能力：

* 对象枚举
* publication.ddl 过滤
* normalized DDL 生成
* 依赖排序
* DDL 执行

区别只在于：

* initial sync：针对 snapshot 前的完整初始集合
* refresh：针对“自上次同步以来新增进入范围”的差量集合

---

## 23.8 refresh 的执行顺序

推荐顺序如下：

### 对初次创建 subscription

```text
SchemaSyncWorker
→ TablesyncWorker
→ ApplyWorker
```

### 对 REFRESH PUBLICATION

```text
Delta Schema Sync
→ 表级 refresh / copy_data
→ ApplyWorker 继续增量复制
```

对于对象级 delta schema sync，仍建议使用与 initial sync 相同的依赖顺序：

```text
schema
→ table
→ type
→ function
→ view
→ trigger
→ index
```

这样可以最大化减少依赖缺失导致的失败。

---

## 23.9 典型场景定义

### 场景 1：删除 publication

发布端：

```sql
DROP PUBLICATION pub1;
```

订阅端：

* 不自动删除本地对象
* 若 subscription 仍引用 `pub1`，后续 refresh / worker 校验时报错并停止
* DBA 需显式修改 subscription 配置

---

### 场景 2：publication 增加了 `view` 支持

发布端：

```sql
ALTER PUBLICATION pub1 SET (ddl='table,index,view');
```

订阅端执行：

```sql
ALTER SUBSCRIPTION sub1 REFRESH PUBLICATION;
```

行为：

* 新增表按现有 refresh 语义处理
* 已存在的 view 补做 delta schema sync
* refresh 之后新发生的 view DDL 则走增量 WAL 复制

---

### 场景 3：publication 去掉 `view`

发布端：

```sql
ALTER PUBLICATION pub1 SET (ddl='table,index');
```

订阅端 refresh 后：

* 本地已有 view 保留
* 以后不再接收 view 相关 DDL
* 若 view 依赖该 publication 中表的后续变化，视为用户自行承担本地依赖维护责任

---

## 23.10 最终规则总结

### 删除 publication

* 不复制到订阅端
* 不自动解绑 subscription
* 不自动删除本地对象
* 若 subscription 仍引用它，应报错并要求 DBA 显式修复

### REFRESH PUBLICATION

* 表级行为沿用 PostgreSQL 现有语义：补新表、移除不再属于 publication 的表同步关系和同步槽。([PostgreSQL][2])
* DDL 对象级行为扩展为 delta schema sync：补新增进入范围的对象，停止移出范围对象的后续复制，但不自动 drop 本地对象

---

该节代表目标闭环模型（当前未完全落地）：

```text
CREATE SUBSCRIPTION → initial schema sync
运行期 WAL → incremental DDL/DML
publication 变化 → refresh + delta schema sync
```

这样“起点一致、持续一致、范围变化后的追平”三件事就都讲清楚了。

[1]: https://www.postgresql.org/docs/current/sql-droppublication.html "PostgreSQL: Documentation: 18: DROP PUBLICATION"
[2]: https://www.postgresql.org/docs/current/sql-altersubscription.html "PostgreSQL: Documentation: 18: ALTER SUBSCRIPTION"
