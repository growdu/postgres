

# PostgreSQL 逻辑复制 DDL 支持 —— 总体设计文档（最终版）

---

# 1. 设计目标

在 PostgreSQL 现有逻辑复制体系上扩展 DDL 同步能力，使系统具备：

1. 发布端自动捕获 DDL；
2. 将 DDL 转化为结构化消息；
3. 通过逻辑复制链路传输；
4. 订阅端按顺序执行 DDL；
5. 在增强阶段支持接收持久化、重试、审计及级联订阅。

核心原则：

> **DDL = 可复制的数据流（而不是隐式行为）**

---

# 2. 总体架构（四阶段模型）

本设计采用四阶段逐步演进模型：

| 阶段  | 名称              | 核心能力           | 是否必须 |
| --- | --------------- | -------------- | ---- |
| 设计1 | 目录模型与用户接口       | DDL 能力定义 + 消息表 | 必须   |
| 设计2 | 系统表逻辑复制通路       | 让消息能复制         | 必须   |
| 设计3 | 自动 DDL 同步（直接执行） | 最小闭环           | 必须   |
| 设计4 | 接收表与增强能力        | 状态机 / 重试 / 级联  | 可选增强 |

---

# 3. 核心设计原则

---

## 3.1 消息化设计

DDL 不直接同步执行，而是转化为消息：

```text
DDL → pg_publication_sync → logical replication → apply
```

---

## 3.2 单源主控模型（与 DML 一致）

与逻辑复制 DML 一致：

> 一张表的 schema 演化应来自唯一上游来源

---

## 3.3 顺序一致性原则（关键）

必须保证：

> **DDL 与 DML 在订阅端执行顺序与发布端一致**

---

## 3.4 数据面 / 控制面分离

| 层          | 职责   |
| ---------- | ---- |
| 数据面（设计2）   | 传输消息 |
| 控制面（设计3/4） | 执行消息 |

---

## 3.5 分阶段演进原则

* 设计3先实现最小闭环（不引入 recv）
* 设计4作为增强，不阻塞前面阶段

---

# 4. 用户接口设计（设计1）

---

## 4.1 扩展 publication

新增字段：

```text
pg_publication.pubddl int4
```

语义：

> publication 允许发布的 DDL 类型集合

---

## 4.2 扩展 subscription

新增字段：

```text
pg_subscription.subddl int4
```

语义：

> subscription 允许接收的 DDL 类型集合

---

## 4.3 SQL 接口

```sql
CREATE/ALTER PUBLICATION ... WITH (ddl='...')
CREATE/ALTER SUBSCRIPTION ... WITH (ddl='...')
```

---

## 4.4 支持 token

```text
table, index, trigger, view, rule,
schema, function, type, domain, extension, all
```

---

# 5. 核心数据模型（设计1）

---

## 5.1 系统表 `pg_publication_sync`

定义：

> 发布端 DDL 消息表（append-only）

---

## 5.2 表特性

* append-only
* 只存消息（Q/A/D）
* 不存状态
* 不存执行结果

---

## 5.3 消息类型

| 类型 | 含义                      |
| -- | ----------------------- |
| Q  | 普通 DDL                  |
| A  | ADD publication member  |
| D  | DROP publication member |

---

## 5.4 核心字段

| 字段               | 含义              |
| ---------------- | --------------- |
| pfsyncpubid      | 发布端 publication |
| pfsyncmsgtype    | Q/A/D           |
| pfsyncddl        | DDL 类型          |
| pfsyncddlsql     | SQL             |
| pfsyncsearchpath | 执行上下文           |
| pfsynclsn        | 顺序关键            |
| pfsyncts         | 时间              |

---

# 6. 设计2：系统表逻辑复制通路

---

## 6.1 目标

让 `pg_publication_sync` 能通过逻辑复制同步。

---

## 6.2 白名单机制

只允许：

```text
pg_publication_sync
```

---

## 6.3 WAL 放开

允许系统表进入逻辑解码。

---

## 6.4 publication 支持

* 允许 publishable
* 不允许 `ADD TABLE pg_publication_sync`
* 仅作为 `ddl` 的隐式成员

---

## 6.5 pgoutput 支持

* 支持 INSERT
* 必须做 tuple 级过滤：

```text
按 pfsyncpubid 路由
```

---

## 6.6 subscriber 支持

允许系统表作为复制目标。

---

# 7. 设计3：自动 DDL 同步（直接执行）

---

## 7.1 目标

形成最小闭环：

> 收到 DDL → 直接执行

---

## 7.2 核心原则

### 不引入 recv 表

* 不落中间表
* 不做异步执行

### 直接执行

* 收到即 apply

---

## 7.3 执行入口

```c
maybe_apply_publication_sync_message()
```

---

## 7.4 执行逻辑

### Q

* 恢复 search_path
* 执行 SQL

### A

* 加入订阅关系

### D

* 移除订阅关系

---

## 7.5 顺序保证

设计3依赖：

> **PG 原生 apply 顺序**

即：

* WAL 顺序 = apply 顺序
* DDL 与 DML 天然一致

---

## 7.6 错误语义

* 出错即停止
* 不提供状态机
* 不提供重试

---

## 7.7 限制

设计3不支持：

* 重试
* 审计
* 级联订阅
* 多来源冲突管理

---

# 8. DDL 与 DML 语义一致性

---

## 8.1 一致点

* 单源主控
* 顺序执行
* publication/subscription 模型一致
* 无冲突自动解决

---

## 8.2 收敛点（重要）

DDL 比 DML 更严格：

> 同一张表只允许一个来源域控制 schema

---

# 9. 设计4：接收表与增强能力（可选）

---

## 9.1 目标

解决：

* 接收持久化
* 执行状态跟踪
* 重试
* 审计
* 级联/循环订阅

---

## 9.2 新增表

```text
pg_publication_recv
```

---

## 9.3 与 sync 表区别

| 表                   | 作用      |
| ------------------- | ------- |
| pg_publication_sync | 发布端消息   |
| pg_publication_recv | 接收与执行状态 |

---

## 9.4 核心能力

### 1. 状态机

* R / A / S / F

### 2. 重试机制

### 3. 错误记录

### 4. 多来源支持

### 5. 级联订阅支持

---

## 9.5 顺序控制（增强）

引入：

> **DDL barrier**

保证：

* DDL 未执行 → 阻塞后续 DML

---

## 9.6 多来源限制

必须保证：

> 同一张表只允许一个 DDL 来源

---

## 9.7 与设计3关系

| 项      | 设计3 | 设计4 |
| ------ | --- | --- |
| recv 表 | ❌   | ✅   |
| 状态机    | ❌   | ✅   |
| 重试     | ❌   | ✅   |
| 级联     | ❌   | ✅   |

---

# 10. 端到端流程

---

## 设计3路径（最小闭环）

```text
DDL
 ↓
pg_publication_sync
 ↓
logical replication
 ↓
apply worker
 ↓
直接执行 DDL
```

---

## 设计4路径（增强）

```text
DDL
 ↓
pg_publication_sync
 ↓
logical replication
 ↓
pg_publication_recv
 ↓
状态机调度
 ↓
执行 DDL
```

---

# 11. 生命周期

---

## 11.1 sync 表

* append-only
* 支持 prune

---

## 11.2 recv 表（设计4）

* 可更新
* 保存执行状态

---

# 12. 总结

---

## 核心思想

> 用消息表表达 DDL，用逻辑复制传输，用 apply 执行。

---

## 四阶段能力

| 阶段  | 能力   |
| --- | ---- |
| 设计1 | 定义能力 |
| 设计2 | 传输能力 |
| 设计3 | 执行闭环 |
| 设计4 | 工程增强 |

---

## 一句话总结

> 设计3实现“DDL 能跑”，设计4实现“DDL 能用”。
