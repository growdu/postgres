# Apply Worker数据库连接方式分析

## 背景

在逻辑复制Apply Worker中执行DDL时，存在两种可能的实现方式：

1. **通过Portal内部执行** - 不建立额外连接，直接在Worker进程内部执行SQL
2. **通过libpq建立额外连接** - 建立新的数据库连接来执行SQL

本文档分析这两种方式的优劣，以及为什么**不推荐使用额外libpq连接**。

---

## 一、两种连接方式对比

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  方案A: Apply Worker + 额外libpq连接                                         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Apply Worker Process                                                │   │
│  │ │   │
│  │  ┌───────────────────┐         ┌───────────────────┐                 │   │
│  │  │ Replication连接   │         │ 额外libpq连接      │                 │   │
│  │  │ (walrcv)          │         │ (PQconnectdb)      │                 │   │
│  │  └───────────────────┘         └───────────────────┘                 │   │
│  │          │                         │                                │   │
│  └──────────┼─────────────────────────┼─────────────────────────────────┘   │
│             │                         │                                    │
│             ▼                         ▼                                    │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │              PostgreSQL Server                               │        │
│  │  ┌────────────┐  ┌────────────┐                               │        │
│  │  │ Backend #1 │  │ Backend #2 │  ← 两个独立的后端进程           │        │
│  │  │ (repl)     │  │ (libpq)    │                               │        │
│  │  └────────────┘  └────────────┘                               │        │
│  └─────────────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  方案B: Apply Worker使用Portal内部执行 (推荐)                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │  Apply Worker Process                                                │   │
│  │                                                                      │   │
│  │  ┌───────────────────┐                                               │   │
│  │  │ Replication连接   │ (walrcv) - 仅用于从Publisher接收消息            │   │
│  │  └───────────────────┘                                               │   │
│  │          │ │   │
│  │          │ 直接内部执行 (无需额外连接) │   │
│  │          ▼ │   │
│  │  ┌─────────────────────────────────────────────────────────────┐     │   │
│  │  │ exec_sql_via_portal()                                       │     │   │
│  │  │   ├── babelfishpg_tsql_raw_parser() ← TSQL解析              │     │   │
│  │  │   ├── pg_analyze_and_rewrite_fixedparams() ← BBF hooks     │     │   │
│  │  │   ├── PortalRun() → standard_ProcessUtility() ← DDL执行     │     │   │
│  │  └─────────────────────────────────────────────────────────────┘     │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、额外libpq连接的问题

### 2.1 事务可见性问题

**核心问题**：两个独立后端连接拥有不同的事务上下文。

```c
// 时间线示例：
// T1: Worker通过libpq连接执行 CREATE TABLE foo (id INT)
// T2: Worker通过内部Portal执行 INSERT INTO foo VALUES (1)
// T3: COMMIT

// 实际发生的情况：
// Connection 1 (libpq):  CREATE TABLE foo (id INT);  --隐式提交
// Connection 2 (internal): INSERT INTO foo VALUES (1); -- 另一事务
// COMMIT (各自的 COMMIT)

// 问题：
// - 如果T1的CREATE TABLE在事务A中
// - T2的INSERT在事务B中
// - 两者可能看到不同的数据状态
// - 甚至INSERT可能找不到刚创建的表（如果它在独立事务中）
```

### 2.2 BBF扩展兼容性问题

BBF扩展的hooks设计假设**单一backend执行上下文**：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  BBF hooks的预期执行路径                                                    │
│                                                                             │
│  SQL输入 ──→ babelfishpg_tsql_raw_parser()  ← TSQL解析器                  │
│              │                                                            │
│              ▼ │
│       pg_analyze_and_rewrite_fixedparams()  ← BBF analyzer hooks介入 │
│              │ │
│              ▼ │
│          pg_plan_queries()                                                │
│              │                                                            │
│              ▼                                                            │
│          PortalRun() │
│              │                                                            │
│              ▼                                                            │
│ standard_ProcessUtility()                                            │
│              │                                                            │
│              ▼                                                            │
│       ProcessUtility_hook (BBF) ← 正确触发 │
└─────────────────────────────────────────────────────────────────────────────┘
```

**libpq连接的问题**：

| 问题 | 说明 |
|------|------|
| **sql_dialect未设置** | libpq新连接的session可能未设置`sql_dialect = SQL_DIALECT_TSQL` |
| **Hooks未初始化** | BBF的`ProcessUtility_hook`可能未正确设置 |
| **GUC不一致** | BBF相关的GUC参数可能与主连接不同 |
| **触发器状态** |某些BBF内部状态存储在backend上下文中 |

### 2.3 锁和并发问题

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  双连接锁冲突风险                                                         │
│                                                                             │
│  Connection 1 (Replication)          Connection 2 (libpq)                  │
│       │                                    │                               │
│       ▼                                    ▼                               │
│  LOCK TABLE foo SHARE;                 LOCK TABLE foo SHARE;              │
│       │                                    │                               │
│       │◀────── 死锁 ──────────────────────┘                               │
│                                                                             │
│  解决方案：                                                                │
│  - 按固定顺序获取锁                                                       │
│  - 使用`LOCK TABLE ... NOWAIT`                                            │
│  - 但这增加了复杂性 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.4 资源消耗

|资源 | 额外libpq连接 | Portal内部执行 |
|------|--------------|----------------|
| **Backend进程** | 2个 | 1个 |
| **共享内存** | 2x MyProc + 2x buffers | 1x |
| **连接槽** | 消耗1个连接槽配额 | 不消耗 |
| **事务开销** | 跨连接事务同步复杂 | 无额外开销 |

### 2.5 代码复杂度

```c
// 使用额外libpq连接需要处理的边界情况：

// 1. 连接管理
PGconn *ddl_conn = PQconnectdb(conninfo);
if (PQstatus(ddl_conn) != CONNECTION_OK)
    ereport(ERROR, ...);
// 需要处理重连、心跳等

// 2. Session参数同步
PQexec(ddl_conn, "SET babelfishpg_tsql.sql_dialect = 'tsql'");
// 确保所有GUC与主连接一致

// 3. 事务同步
// 如何保证libpq连接和内部执行在同一个事务中？
// 可能的方案：
// - 两阶段提交（复杂）
// - 先DDL再DML在同一个事务（需要重构执行顺序）

// 4. 错误处理
// 两套错误处理机制
// 错误传播需要协调

// 5. 清理
PQfinish(ddl_conn);
```

---

## 三、Portal内部执行的优势

### 3.1 事务一致性

```c
// Portal内部执行 -同一事务上下文
StartTransactionCommand();

// 所有操作在同一事务中
exec_sql_via_portal("CREATE TABLE foo (id INT)");  // DDL
exec_sql_via_portal("INSERT INTO foo VALUES (1)"); // DML

//同一个COMMIT
CommitTransactionCommand();

// 结果：
// - DDL和DML在同一个事务中
// - 原子性得到保证
// - 快照一致
```

### 3.2 完整的BBF支持

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Portal内部执行 - BBF完整支持 │
│                                                                             │
│  sql_dialect = SQL_DIALECT_TSQL  ←已在Worker上下文中设置                   │
│                                                                             │
│  exec_sql_via_portal("CREATE TABLE foo ..."); │
│       │                                                                     │
│       ▼                                                                     │
│  babelfishpg_tsql_raw_parser()  ← TSQL解析器可用 │
│       │                                                                     │
│       ▼                                                                     │
│  pg_analyze_and_rewrite_fixedparams()  ← BBF analyzer hooks介入              │
│       │                                                                     │
│       ▼                                                                     │
│  PortalRun() → standard_ProcessUtility()                                     │
│       │                                                                     │
│       ▼                                                                     │
│  ProcessUtility_hook (BBF) ← 正确触发 ✓                                     │
│       │                                                                     │
│       ├── pltsql_createFunction()  ← CREATE FUNCTION处理 │
│       ├── create_bbf_db()           ← CREATE DATABASE处理                   │
│       └── ...其他BBF特定处理... │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.3 资源高效

- **单一Backend**：不需要额外的后端进程
- **共享事务上下文**：与主复制流共享同一个事务
- **无连接开销**：避免libpq连接的建立、认证开销

### 3.4 实现简洁

```c
// Portal方式 - 简洁的实现
static void
exec_sql_via_portal(const char *sql_text)
{
    List *parsetree_list;
    List *querytree_list;
    List *plantree_list;
    Portal portal;

    // 1. 解析
    parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);

    // 2. 分析重写 (BBF hooks介入)
    querytree_list = pg_analyze_and_rewrite_fixedparams(..., NULL, 0, NULL);

    // 3. 计划
    plantree_list = pg_plan_queries(querytree_list, sql_text, ...);

    // 4. Portal执行
    portal = CreatePortal("ddl_replayer", true, true);
    PortalDefineQuery(portal, NULL, sql_text, commandTag, plantree_list, NULL);
    PortalStart(portal, NULL, 0, InvalidSnapshot);
    PortalRun(portal, FETCH_ALL, true, DestNone, DestNone, &qc);
    PortalDrop(portal, false);
}
```

---

## 四、特殊场景分析

### 4.1 跨数据库操作

如果确实需要操作**不同数据库**：

|场景 | 推荐方案 |
|------|----------|
| 同实例多数据库 | 使用`dblink`或`PostgreSQL FDW` |
| 不同实例 | 在目标实例建立独立的Apply Worker |
| 只读操作 | 可以使用libpq只读连接，但不推荐用于DDL |

**结论**：跨数据库DDL应该通过独立的复制订阅实现，而不是在单个Worker中建立额外连接。

### 4.2 只读查询

对于**只读元数据查询**（如检查表是否存在），可以使用libpq连接，但：

1. 使用**只读事务** (`SET TRANSACTION READ ONLY`)
2. 保持连接**复用**，避免频繁建立/断开
3. **不要**用于DDL执行

---

## 五、方案对比总结

| 评估维度 | 额外libpq连接 | Portal内部执行 |
|----------|--------------|----------------|
| **事务一致性** | ❌ 跨连接事务复杂 | ✅ 同一事务 |
| **BBF兼容性** | ❌ Hooks可能不生效 | ✅ 完全兼容 |
| **锁管理** | ❌ 死锁风险 | ✅ 单一上下文 |
| **资源消耗** | ❌ 双倍资源 | ✅ 低开销 |
| **实现复杂度** | ❌ 需要管理双连接 | ✅ 简洁 |
| **错误处理** | ❌ 两套机制 | ✅ 统一 |
| **维护性** | ❌ 边界情况多 | ✅ 易维护 |

---

## 六、结论

**不推荐在Apply Worker中建立额外的libpq连接来执行DDL**，原因：

1. **事务不一致**：两个独立后端连接无法保证原子性和事务一致性
2. **BBF不兼容**：BBF扩展的hooks需要单一backend上下文
3. **资源浪费**：额外的backend进程和连接槽消耗
4. **复杂性增加**：连接管理、Session同步、事务协调等

**推荐方案**：使用Portal内部执行方式，它提供了：

- 完全的事务一致性保证
- 完整的BBF扩展支持
- 低资源消耗
- 简洁的实现

---

## 附录：相关代码位置

| 文件 | 说明 |
|------|------|
| `src/backend/replication/logical/worker.c` | Apply Worker主循环，`apply_dispatch()` |
| `src/backend/replication/logical/sql_replayer.c` | Portal执行核心函数（新增） |
| `src/backend/tcop/pquery.c` | `PortalRun()`, `PortalStart()` |
| `src/backend/tcop/utility.c` | `standard_ProcessUtility()` |
| `babelfish_extensions/.../hooks.c` | BBF的`ProcessUtility_hook` |
| `babelfish_extensions/.../pl_handler.c` | BBF的`bbf_ProcessUtility()` |