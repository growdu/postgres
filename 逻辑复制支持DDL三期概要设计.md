

# PostgreSQL 逻辑复制 DDL 三期（Initial Schema Sync）概要设计

---

# 1. 设计目标

---

## 1.1 问题定义

在现有一期 + 二期设计中：

```text
✔ 支持增量 DDL replication
✔ DDL 与 DML 顺序一致
✔ automatic + manual 统一链路
```

但缺失：

```text
❌ subscription 创建时 schema 初始化
```

即：

```text
发布端 schema ≠ 订阅端 schema
```

会导致：

* apply worker 执行 DML 失败（表不存在）
* DDL 顺序无法保证（依赖对象缺失）
* 用户必须手动 pg_dump schema

---

## 1.2 三期目标

三期目标是：

```text
在 subscription 初始化阶段
自动构建订阅端 schema
并与 publication.ddl 保持一致
```

---

## 1.3 核心目标拆解

### 目标1：schema bootstrap

```text
在复制开始前
保证订阅端具备必要 schema
```

---

### 目标2：与 publication.ddl 一致

```text
initial sync 的对象集合
== publication.ddl 决定
```

---

### 目标3：与增量 DDL 无缝衔接

```text
snapshot 前 DDL → initial sync
snapshot 后 DDL → logical replication
```

---

### 目标4：不破坏现有复制模型

```text
不引入：
❌ 新 replication protocol
❌ 新 WAL 类型
❌ 新状态机（pending 等）
```

---

# 2. 总体设计

---

## 2.1 三期在整体架构中的位置

```text
一期：automatic DDL（table/index）
二期：扩展 automatic + manual DDL
三期：initial schema sync（补齐启动阶段）
```

---

## 2.2 全流程统一模型

```text
CREATE SUBSCRIPTION
        ↓
[Phase 3] Initial Schema Sync
        ↓
[Phase 1/2] 增量复制（DDL + DML）
```

---

## 2.3 三期新增模块

新增：

```text
SchemaSyncWorker
```

职责：

```text
在 subscription 初始化时
构建订阅端 schema
```

---

# 3. 核心设计原则

---

## 原则1：initial sync ≠ logical replication

```text
initial sync：
  catalog → deparse → SQL → apply

incremental：
  WAL → logical decoding → apply
```

---

## 原则2：publication.ddl 是唯一控制源

```text
publication.ddl 决定：
  initial sync 同步什么
  incremental replication 同步什么
```

---

## 原则3：snapshot 边界清晰

```text
snapshot 前 → initial sync
snapshot 后 → WAL replication
```

---

## 原则4：不重复执行

```text
initial sync 不写 WAL
incremental DDL 才写 WAL
```

---

# 4. 执行流程设计

---

## 4.1 subscription 创建流程（扩展）

当前流程：

```text
CREATE SUBSCRIPTION
→ tablesync worker
→ copy data
→ apply worker
```

三期改为：

```text
CREATE SUBSCRIPTION
        ↓
获取 replication snapshot
        ↓
SchemaSyncWorker（新增）
        ↓
TablesyncWorker（copy data）
        ↓
ApplyWorker（增量复制）
```

---

## 4.2 时序图

```text
Publisher                        Subscriber

CREATE SUBSCRIPTION
      │
      ├── snapshot ------------------------→
      │
      │                SchemaSyncWorker:
      │                1. 拉取对象
      │                2. 生成DDL
      │                3. 执行DDL
      │
      │                TablesyncWorker:
      │                COPY table data
      │
      ├── WAL (DDL+DML) ------------------→
                       ApplyWorker:
                       按顺序执行
```

---

# 5. SchemaSyncWorker 设计

---

## 5.1 Worker 定位

```text
独立 background worker
类似 tablesync worker
```

---

## 5.2 输入

来自 subscription：

* publication 列表
* publication.ddl bitmask
* snapshot

---

## 5.3 输出

```text
在订阅端执行 DDL
构建 schema
```

---

## 5.4 执行步骤

---

### Step 1：获取 snapshot

```text
确保 schema 与数据 snapshot 一致
```

---

### Step 2：枚举对象

根据 publication.ddl：

```text
扫描 publisher catalog：
  pg_class
  pg_proc
  pg_trigger
  pg_type
  pg_namespace
```

---

### Step 3：对象过滤

```text
按 publication scope + ddl bitmask
```

---

### Step 4：生成 normalized_sql

复用：

```text
GenerateNormalizedDDL()
```

（与二期一致）

---

### Step 5：排序（关键）

必须按依赖顺序执行：

```text
1. schema
2. table
3. type
4. function
5. view
6. trigger
7. index
```

---

### Step 6：执行

```text
execute_replicated_ddl()
```

---

# 6. 同步对象范围

---

## 6.1 由 publication.ddl 控制

例如：

```sql
WITH (ddl='table,index')
```

---

## 6.2 initial sync 行为

```text
✔ CREATE TABLE
✔ CREATE INDEX
✖ VIEW / FUNCTION / TRIGGER
```

---

## 6.3 与增量保持一致

```text
initial sync 集合
==
incremental DDL 集合
```

---

# 7. snapshot 与 DDL 边界

---

## 7.1 定义

```text
snapshot = replication snapshot
```

---

## 7.2 规则

```text
DDL before snapshot → initial sync
DDL after snapshot → WAL replication
```

---

## 7.3 示例

```sql
CREATE TABLE t1;
-- snapshot here
ALTER TABLE t1 ADD COLUMN c2;
```

---

订阅端：

```text
initial sync → CREATE TABLE t1
WAL → ALTER TABLE t1 ADD COLUMN c2
```

---

# 8. 与 tablesync 的关系

---

## 8.1 执行顺序

```text
SchemaSyncWorker → TablesyncWorker
```

---

## 8.2 原因

```text
必须先有 table schema
才能 COPY 数据
```

---

## 8.3 不修改 tablesync 逻辑

```text
tablesync 只做数据复制
```

---

# 9. manual DDL 与 initial sync

---

## 9.1 场景

```text
initial sync 过程中
用户执行 pg_emit_logical_ddl()
```

---

## 9.2 规则

```text
manual DDL → 正常写 WAL
→ 等待 apply worker 执行
```

---

## 9.3 要求

```text
SchemaSyncWorker 必须先完成
```

否则：

```text
manual DDL 可能依赖不存在对象
```

---

# 10. 错误处理

---

## 10.1 initial sync 失败

```text
→ subscription 创建失败
```

---

## 10.2 对象冲突

例如：

```text
table 已存在
```

策略：

```text
STRICT（默认）：
  报错

未来可扩展：
  IF NOT EXISTS / replace
```

---

# 11. 与社区方向一致性

---

## 11.1 完全一致的点

* initial schema sync 必须存在
* publication.ddl 控制对象集合
* initial sync 与 incremental 一致
* snapshot 边界明确

---

## 11.2 优势

你的设计相比社区 patch：

```text
✔ unified DDL model（更干净）
✔ logical message 统一链路
✔ automatic + manual 一体化
```

---

## 11.3 风险点

```text
⚠ 需要解释为何不用 event trigger
⚠ 需要证明 normalized_sql 可行
```

---

# 12. Patch 拆分建议（三期）

---

## Patch 7

```text
SchemaSyncWorker 基础框架
```

---

## Patch 8

```text
catalog 枚举 + 对象过滤
```

---

## Patch 9

```text
DDL 生成 + 执行
```

---

## Patch 10

```text
与 tablesync / apply worker 集成
```

---

# 13. 总结

---

三期补齐后，整个体系变为：

```text
Phase 3：initial schema sync（启动一致性）
Phase 1/2：incremental DDL + DML（运行一致性）
```

---

## 核心闭环

```text
initial sync：
  解决“起点一致”

incremental：
  解决“持续一致”
```
