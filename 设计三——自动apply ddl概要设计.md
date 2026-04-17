

# 设计三：自动同步 DDL（直接执行版）

---

# 1. 设计目标

在设计1（目录模型）和设计2（系统表复制通路）基础上，实现 DDL 自动同步的**最小闭环能力**：

1. 发布端捕获 DDL 并写入 `pg_publication_sync`
2. 逻辑复制将消息同步到订阅端
3. 订阅端 apply worker **直接执行 DDL**
4. 保证 DDL 与 DML 的执行顺序与发布端一致

本设计的核心目标是：

> **在不引入接收表、不引入状态机的前提下，让 DDL 能“正确执行”。**

---

# 2. 设计范围

## 2.1 包含内容

* 发布端 DDL 捕获与消息写入
* `Q/A/D` 消息协议
* 订阅端 apply 执行路径
* DDL 与 DML 顺序一致性保证
* 错误处理与执行语义
* 对 subscription / publication 的影响

---

## 2.2 不包含内容

以下内容不属于设计3范围：

* 接收表（`pg_publication_recv`）
* 状态机（R/A/S/F）
* 重试机制
* 审计记录
* 级联/循环订阅控制
* 多来源调度

这些统一在设计4实现。

---

# 3. 核心设计原则

---

## 3.1 不落接收表（关键）

设计3明确：

> **订阅端不将消息写入中间表，而是收到后直接执行**

即：

```text
pg_publication_sync → apply → 执行
```

而不是：

```text
pg_publication_sync → recv表 → 再执行
```

---

## 3.2 复用 DML apply 顺序（核心优势）

设计3的最大优势是：

> **完全复用 PostgreSQL 原生逻辑复制的 apply 顺序**

也就是说：

* WAL 顺序 → logical decoding → apply 顺序
* DDL 与 DML 在同一流中推进
* 不需要额外排序或 barrier 机制

---

## 3.3 单线程执行（per subscription）

DDL 执行必须满足：

* 仅在 **leader apply worker** 执行
* 不允许 parallel apply worker 执行 DDL

---

## 3.4 与 DML 语义一致

DDL 执行遵循 DML apply 的核心语义：

* 按来源流顺序执行
* 出错即停止
* 不做冲突自动解决
* 不支持多源合并

---

# 4. 消息模型

---

## 4.1 消息来源

所有 DDL 消息来自：

```text
pg_publication_sync
```

---

## 4.2 消息类型

| 类型 | 含义               |
| -- | ---------------- |
| Q  | 普通 DDL           |
| A  | publication 成员新增 |
| D  | publication 成员移除 |

---

## 4.3 关键字段

| 字段                | 用途       |
| ----------------- | -------- |
| pfsyncmsgtype     | Q/A/D 分发 |
| pfsyncddl         | DDL 类型过滤 |
| pfsyncddlsql      | 执行 SQL   |
| pfsyncsearchpath  | 执行上下文    |
| pfsynctargettable | A/D 目标   |
| pfsynclsn         | 顺序保证     |

---

# 5. 发布端写入

---

## 5.1 Q 消息（DDL 捕获）

入口：

```c
CapturePublicationSyncDDL()
```

逻辑：

1. 捕获 DDL（ProcessUtility）
2. 找到命中的 publication
3. 对每个 publication 写一条 Q

---

## 5.2 A/D 消息（成员变更）

入口：

```c
insert_publication_sync_relation_message()
```

场景：

| 操作               | 消息      |
| ---------------- | ------- |
| ADD TABLE        | A       |
| DROP TABLE       | D       |
| DROP PUBLICATION | 展开为多个 D |

---

## 5.3 publication 粒度展开

一条 DDL：

```text
命中 N 个 publication → 写 N 条消息
```

---

# 6. 订阅端执行流程

---

## 6.1 执行入口

```c
maybe_apply_publication_sync_message()
```

---

## 6.2 调用时机

当 apply worker 收到：

```text
relation = pg_publication_sync
```

时调用。

---

## 6.3 前置条件

必须满足：

```text
1. 当前 worker 是 leader apply worker
2. subddl & pfsyncddl != 0
```

否则：

* 不执行
* 跳过

---

## 6.4 执行分发

### Q（DDL SQL）

执行流程：

1. 保存当前 search_path
2. 设置为 `pfsyncsearchpath`
3. 执行 `pfsyncddlsql`
4. 恢复 search_path

---

### A（加入订阅）

调用内部函数：

```c
ApplyPublicationSyncAddRelation()
```

作用：

* 将对象加入订阅同步范围

---

### D（移除订阅）

调用：

```c
ApplyPublicationSyncDropRelation()
```

作用：

* 将对象移出订阅同步范围

---

# 7. DDL 与 DML 顺序保证（核心）

---

## 7.1 顺序来源

顺序由：

```text
WAL 顺序（LSN）
```

决定。

---

## 7.2 顺序保证机制

设计3中：

> **DDL 和 DML 共用同一个 apply 顺序流**

即：

```text
WAL → decode → apply（单流）
```

因此天然保证：

* DDL 在 DML 前 → 先执行 DDL
* DML 在 DDL 前 → 先执行 DML

---

## 7.3 为什么不需要 barrier

因为：

* 没有 recv 表
* 没有异步调度
* 没有脱离 apply 主循环

所以：

> **顺序不会被打破**

---

## 7.4 顺序示例

### case 1

发布端：

```text
LSN=100 ALTER TABLE t ADD COLUMN a
LSN=110 INSERT INTO t(a)
```

订阅端：

```text
执行 ALTER
再执行 INSERT
```

---

### case 2

发布端：

```text
LSN=100 INSERT
LSN=120 ALTER TABLE DROP COLUMN
```

订阅端：

```text
先 INSERT
再 DROP COLUMN
```

---

# 8. 错误处理

---

## 8.1 基本原则

> **DDL 执行失败 = apply worker 停止**

---

## 8.2 错误类型

### Q

* SQL 执行失败

### A/D

* 目标对象不存在
* 状态冲突

---

## 8.3 行为

* 抛错
* 停止 subscription apply
* 依赖现有 PG 错误恢复机制

---

## 8.4 不提供

设计3不提供：

* 重试
* 跳过
* 状态记录

---

# 9. 约束与限制

---

## 9.1 单来源约束（重要）

> 同一张表只能由一个 subscription 管理 DDL

---

## 9.2 不支持多源合并

不允许：

* 多个数据库同时修改同一表 schema

---

## 9.3 不支持级联

设计3中：

* 不考虑循环订阅
* 不考虑转发

---

## 9.4 无审计能力

执行后：

* 无历史记录
* 无执行状态

---

# 10. 与设计4的关系

---

## 设计3

* 直接执行
* 无状态
* 无 recv 表
* 最小闭环

---

## 设计4（增强）

在设计3基础上提供：

* recv 表
* 状态机
* 重试
* 审计
* 多来源
* DDL barrier

---

# 11. 总结

---

## 设计三的核心价值

> **让 DDL 在逻辑复制中“正确执行”，并保证顺序一致。**

---

## 核心特点

| 特性   | 状态 |
| ---- | -- |
| 最小闭环 | ✅  |
| 顺序一致 | ✅  |
| 简单实现 | ✅  |
| 状态机  | ❌  |
| 重试   | ❌  |
| 级联   | ❌  |

---

## 一句话总结

> 设计三通过“直接执行”模型，将 DDL 嵌入现有 apply 顺序流，在不引入额外复杂度的前提下，实现与 DML 完全一致的执行语义。

