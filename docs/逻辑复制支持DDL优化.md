# PostgreSQL Fork子进程SQL执行方案

## 需求背景

在PostgreSQL后端fork一个进程，用于回放客户端的SQL。要求：
1. **不建立socket连接**：子进程作为server端进程，无法重用父进程的客户端socket
2. **与客户端执行SQL等价**：通过内部API执行SQL，模拟客户端的SQL执行流程
3. **支持BBF扩展**：需要支持TSQL语法和BBF特定DDL处理

---

## 一、技术方案选择

### 1.1候选方案对比

| 方案 | 描述 | 优点 | 缺点 |
|------|------|------|------|
| **SPI方式** | 使用Server Programming Interface执行SQL | 成熟稳定、有事务封装 | 需要编译成C函数调用、不够直接 |
| **Portal+Executor方式** | 直接调用内部执行路径 | 灵活、可控、与客户端执行路径一致 | 需要手动管理更多上下文 |
| **直接socket连接** | 子进程新建连接到数据库 | 实现简单 | 无法实现（本身就是server后端） |

### 1.2 选择Portal+Executor方式

选择理由：
- **与客户端执行路径一致**：最终都走到`PortalRun()`/`standard_ProcessUtility()`
- **支持BBF所有DDL**：所有DDL都经过`standard_ProcessUtility()`，BBF的`ProcessUtility_hook`会被触发
- **不需要BBF特定适配**：BBF hooks在标准流程中已集成

---

## 二、架构设计

### 2.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Parent Backend Process                                                     │
│  ┌─────────┐    ┌──────────────┐    ┌───────────────┐    ┌─────────────┐  │
│  │ Socket  │───▶│  Postgres    │───▶│   Parser      │───▶│  Executor   │  │
│  │ Handler │    │  Protocol │    │  /Analyzer │    │             │  │
│└─────────┘    └──────────────┘    └───────────────┘    └─────────────┘  │
│       │                                                                       │
│       │ fork()                                                              │
│       ▼                                                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  Child Backend (SQL Replayer)                                      │      │
│  │  - 无socket连接                                                     │      │
│  │  - 通过共享内存与PostgreSQL核心通信                                   │      │
│  │  - 调用InitPostgres() + Portal方式执行SQL │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────┘
                                   │
                                   │ 共享内存 (共享内存中的ProcArray、锁等)
                                   ▼
                    ┌──────────────────────────────┐
                    │   PostgreSQL Server Core │
                    │   (同一进程的不同后端)        │
                    └──────────────────────────────┘
```

### 2.2 Fork后子进程状态

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Fork后子进程面临的问题                                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│ │
│  问题 │  解决方案                                │
│  ───────────────────────────────────────────────────────────── │
│  MyProc在ProcArray中冲突 │  InitProcessPhase2()创建新的ProcArray项   │
│  事务状态未定义                 │  StartTransactionCommand()建立新事务     │
│  缓存状态不一致                 │  RelationCacheInitialize()重新初始化     │
│  快照未初始化                   │  GetTransactionSnapshot()获取新快照     │
│  父进程的socket在子进程无效 │  不需要socket，通过共享内存通信 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 三、Portal执行流程详解

### 3.1 标准PostgreSQL查询执行路径

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  exec_simple_query() 完整流程                                                 │
│                                                                 │
│  query_string │
│       │                                                                       │
│       ▼ │
│  ┌─────────────────┐                                                        │
│  │ pg_parse_query  │ ───→ List<RawStmt> │
│  └─────────────────┘                                                        │
│       │                                                                       │
│       ▼                                                                       │
│  ┌─────────────────────────────┐                                            │
│  │ pg_analyze_and_rewrite_ │ ───→ List<Query>                           │
│  │ fixedparams()               │     (parse analysis + rewrite)             │
│  └─────────────────────────────┘                                            │
│       │                                                                       │
│       ▼ │
│  ┌─────────────────┐                                                        │
│  │ pg_plan_queries │ ───→ List<PlannedStmt>                                │
│  └─────────────────┘                                                        │
│       │                                                                       │
│       ▼                                                                       │
│  ┌─────────────────┐                                                        │
│  │  CreatePortal   │                                                        │
│  └─────────────────┘                                                        │
│       │                                                                       │
│       ▼                                                                       │
│  ┌─────────────────┐                                                        │
│  │  PortalStart    │                                                        │
│  └─────────────────┘                                                        │
│       │                                                                       │
│       ▼                                                                       │
│  ┌─────────────────┐┌─────────────────┐                               │
│  │ PortalRun     │ ──▶ │ PortalRunUtility│ (DDL语句)                       │
│  │ │     │       或 │                               │
│  │                 │     │ PortalRunSelect │ (SELECT语句)                   │
│  └─────────────────┘ └─────────────────┘                               │
│       │                                                                       │
│       ▼                                                                       │
│  ┌─────────────────┐                                                        │
│  │ PortalDrop    │                                                        │
│└─────────────────┘                                                        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 DDL语句的执行路径

对于DDL语句（CREATE TABLE、ALTER TABLE等），执行路径如下：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  DDL语句执行路径 (PortalRun → PortalRunUtility → standard_ProcessUtility) │
│                                                                             │
│  PortalRun(portal, FETCH_ALL, ...)                                          │
│       │                                                                      │
│       ▼ │
│  ┌────────────────────┐                                                     │
│  │ ChoosePortalStrat- │                                                     │
│  │ egy()              │ → 返回 PORTAL_MULTI_QUERY (DDL不是SELECT)          │
│  └────────────────────┘                                                     │
│       │                                                                      │
│       ▼                                                                      │
│  ┌────────────────────┐                                                     │
│  │ PortalRunUtility()  │                                                     │
│  └────────────────────┘                                                     │
│       │                                                                      │
│       ▼ │
│  ┌────────────────────────┐                                                 │
│  │ standard_ProcessUtility() │                                              │
│  └────────────────────────┘                                                 │
│       │                                                                      │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────┐                │
│  │ ProcessUtility_hook (BBF扩展设置)                        │                │
│  │ │                │
│  │ bbf_ProcessUtility()                                   │                │
│  │   ├── CREATE FUNCTION → pltsql_createFunction()        │                │
│  │   ├── CREATE DATABASE → create_bbf_db()               │                │
│  │   ├── DROP DATABASE → drop_bbf_db()                   │                │
│  │   └── TransactionStmt → PLTsqlProcessTransaction()    │                │
│  └─────────────────────────────────────────────────────────┘                │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────┐                │
│  │ DDL实际执行 (tablecmds.c, indexcmds.c, ...) │                │
│  └─────────────────────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 四、BBF扩展兼容性分析

### 4.1 BBF扩展Hook架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  BBF扩展TSQL执行架构 │
│                                                                             │
│  ┌─────────────┐    ┌─────────────────┐    ┌──────────────────────────┐     │
│  │ TDS Protocol│───▶│ TSQL Parser      │───▶│ PostgreSQL Analyzer/Hook │     │
│  │ (pg_tds)    │    │ (babelfishpg_ │    │ (gram_hook.c)            │     │
│  │             │    │  tsql_raw_parser)│    │                          │     │
│  └─────────────┘└─────────────────┘    └──────────────────────────┘     │
│                                                    │                         │
│                                                    ▼                         │
│  ┌─────────────────────────────────────────────────────────────────────┐     │
│  │  PostgreSQL Executor / Portal / standard_ProcessUtility            │     │
│  │                                                                     │     │
│  │  ProcessUtility_hook (BBF: bbf_ProcessUtility)                    │     │
│  └─────────────────────────────────────────────────────────────────────┘     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 BBF Hook触发时机

| Hook类型 | 触发时机 | BBF处理函数 | Portal兼容性 |
|----------|----------|-------------|-------------|
| **Analyzer Hooks** | `pg_analyze_and_rewrite_fixedparams()` | `transform_tsql_select_statement()`<br>`pre_transform_insert()` | ✅ 兼容 |
| **ProcessUtility Hook** | `standard_ProcessUtility()` | `bbf_ProcessUtility()` | ✅ 兼容 |
| **Object Access Hook** | 对象访问时 | `bbf_object_access_hook()` | ✅ 兼容 |
| **Executor Hook** | `ExecutorStart/Run/End()` | `pltsql_ExecutorStart()`等 | ✅ 兼容 |

### 4.3 BBF DDL覆盖矩阵

| DDL类型 | 处理位置 | BBF特定处理 | Portal路径支持 |
|---------|----------|-------------|----------------|
| `CREATE TABLE` | `standard_ProcessUtility` | 表名映射、列处理 | ✅ 支持 |
| `ALTER TABLE` | `standard_ProcessUtility` | TSQL视图检查 | ✅ 支持 |
| `DROP TABLE` | `standard_ProcessUtility` | trigger清理 | ✅ 支持 |
| `CREATE INDEX` | `standard_ProcessUtility` | 无 | ✅ 支持 |
| `CREATE VIEW` | `standard_ProcessUtility` | 视图定义存储 | ✅ 支持 |
| `CREATE FUNCTION` | `bbf_ProcessUtility` | `pltsql_createFunction()` | ✅ 支持 |
| `CREATE PROCEDURE` | `bbf_ProcessUtility` | `pltsql_createFunction()` | ✅ 支持 |
| `CREATE DATABASE` | `bbf_ProcessUtility` | `create_bbf_db()` | ✅ 支持 |
| `DROP DATABASE` | `bbf_ProcessUtility` | `drop_bbf_db()` | ✅ 支持 |
| `CREATE TYPE` | `standard_ProcessUtility` | 无 | ✅ 支持 |
| `CREATE SCHEMA` | `standard_ProcessUtility` | 无 | ✅ 支持 |

### 4.4 BBF特定DDL处理详解

```c
// hooks.c:772 - BBF在ProcessUtility阶段的处理
pltsql_bbfCustomProcessUtility(ParseState *pstate, PlannedStmt *pstmt, ...)
{
    Node *parsetree = pstmt->utilityStmt;

    switch (nodeTag(parsetree))
    {
        case T_CreateFunctionStmt:
            return pltsql_createFunction(...);  // 函数创建
        case T_CreatedbStmt:
            if (sql_dialect == SQL_DIALECT_TSQL)
                return create_bbf_db(...);      // 数据库创建
        case T_DropdbStmt:
            if (sql_dialect == SQL_DIALECT_TSQL)
                return drop_bbf_db(...);        // 数据库删除
        case T_TransactionStmt:
            if (NestedTranCount > 0 || ...)
                return PLTsqlProcessTransaction(...);  // 事务处理
        default:
            return false;  // 其他DDL走标准流程
    }
}
```

---

## 五、子进程初始化流程

### 5.1 完整初始化序列

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  子进程SQL Replayer初始化流程 │
│                                                                             │
│  1. fork() 后继承父进程内存 (copy-on-write)                                 │
│       │                                                                      │
│       ▼ │
│  2. BackgroundWorkerInitializeConnection() │
│       │                                                                      │
│       ├── InitProcessPhase2() → 将子进程加入ProcArray(共享内存)     │
│       ├── pgstat_beinit()              → 初始化状态报告 │
│       ├── SharedInvalidationBackendInit() → 初始化共享失效管理器 │
│       ├── ProcSignalInit()             → 初始化进程信号                      │
│       ├── RegisterTimeout()            → 注册超时处理器 │
│       │ │
│       ├── StartupXLOG()                → 启动XLOG (如果不是postmaster子进程)│
│       │                                                                      │
│       ├── RelationCacheInitialize()    → 初始化Relation缓存                 │
│       ├── InitCatalogCache()           → 初始化Catalog缓存                  │
│       ├── InitPlanCache()              → 初始化计划缓存                     │
│       ├── EnablePortalManager()         → 启用Portal管理器                   │
│       │                                                                      │
│       ├── RelationCacheInitializePhase2() → 加载共享系统表                  │
│       │                                                                      │
│       └── StartTransactionCommand()    → 开始第一个事务                     │
│                                                                            │
│  3. 设置sql_dialect = SQL_DIALECT_TSQL  (启用BBF hooks)                      │
│                                                                            │
│  4. SQL执行就绪                                                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 5.2 关键函数解析

#### InitProcessPhase2 (postinit.c)

```c
/*
 * 将子进程加入ProcArray共享内存
 * 这是子进程成为可见数据库进程的关键步骤
 */
void
InitProcessPhase2(void)
{
    // 在ProcArray共享内存中分配新的ProcStruct
    // 设置MyProc/MyProcAddr指向新分配的结构
    //初始化事务相关状态
}
```

#### BackgroundWorkerInitializeConnection (bgworker.c)

```c
/*
 * Background Worker连接数据库的标准方式
 * 用于fork后的子进程初始化数据库连接
 */
void
BackgroundWorkerInitializeConnection(const char *dbname, const char *username, uint32 flags)
{
    InitPostgres(dbname, InvalidOid,    // 数据库名
                 username, InvalidOid,  // 用户名
                 init_flags,            // 初始化标志
                 NULL);
}
```

---

## 六、SQL执行实现方案

### 6.1 代码实现

```c
/**
 * SQL Replayer子进程主函数
 * 使用Portal方式执行SQL，不依赖socket连接
 */
void
sql_replayer_main(Datum main_arg)
{
    BackgroundWorkerStatus status;

    /* 阻塞信号，在fork后立即初始化 */
    BackgroundWorkerBlockSignals();
    BackgroundWorkerInitializeConnection(
        "mydb",           // 数据库名
        "postgres",       // 用户名
        0                 // flags
    );
    BackgroundWorkerUnblockSignals();

    /* 设置BBF方言为TSQL，启用BBF hooks */
    sql_dialect = SQL_DIALECT_TSQL;

    /* 进入主循环 */
    for (;;)
    {
        /* 检查是否收到停止信号 */
        if (GotSIGTERM)
        {
            status = BGW_TERMINATED;
            break;
        }

        /* 从队列获取SQL (共享内存队列) */
        char *sql = get_next_sql_from_queue();
        if (sql == NULL)
        {
            /* 无SQL可处理，睡眠等待 */
            pg_usleep(100000);  // 100ms
            continue;
        }

        /* 执行SQL */
        exec_sql_via_portal(sql);

        /* 释放SQL字符串 */
        pfree(sql);
    }

    /* 清理并退出 */
    proc_exit(0);
}

/**
 * 通过Portal方式执行SQL
 *流程：解析 → 分析重写 → 计划 → Portal执行
 */
static void
exec_sql_via_portal(const char *sql_text)
{
    List *parsetree_list;
    List *querytree_list;
    List *plantree_list;
    Portal portal;
    CommandTag commandTag;
    QueryCompletion qc;
    DestReceiver *receiver;

    /* 开始事务 */
    StartTransactionCommand();

    /* 1. 解析SQL - 使用BBF的TSQL解析器 */
    parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
    if (parsetree_list == NIL)
    {
        ereport(WARNING, errmsg("Parse failed for: %s", sql_text));
        RollbackTransactionCommand();
        return;
    }

    /* 2. 分析和重写 - BBF analyzer hooks在此阶段介入 */
    querytree_list = pg_analyze_and_rewrite_fixedparams(
        linitial_node(RawStmt, parsetree_list),
        sql_text,
        NULL,      // paramTypes
        0,         // numParams
        NULL // queryEnv
    );

    /* 3. 生成计划 */
    plantree_list = pg_plan_queries(
        querytree_list,
        sql_text,
        CURSOR_OPT_PARALLEL_OK,
        NULL       // boundParams
    );

    /* 4. 创建Portal */
    portal = CreatePortal("sql_replayer", true, true);
    portal->visible = false;

    /* 5. 定义查询到Portal */
    commandTag = CreateCommandTag(linitial_node(PlannedStmt, plantree_list)->utilityStmt);
    PortalDefineQuery(
        portal,
        NULL,           // stmts
        sql_text,
        commandTag,
        plantree_list,
        NULL            // queryEnv
    );

    /* 6. 启动Portal */
    PortalStart(portal, NULL, 0, InvalidSnapshot);

    /* 7. 创建目标接收器 */
    receiver = CreateDestReceiver(DestNone);
    receiver->rDestroy(receiver);

    /* 8. 执行Portal */
    (void) PortalRun(
        portal,
        FETCH_ALL,
        true,           // isTopLevel
        receiver,
        receiver,
        &qc
    );

    /* 9. 清理Portal */
    PortalDrop(portal, false);

    /* 提交事务 */
    CommitTransactionCommand();
}
```

### 6.2 关键适配点

| 步骤 | 标准PG流程 | BBF适配 | 说明 |
|------|-----------|---------|------|
| **解析** | `pg_parse_query()` | `babelfishpg_tsql_raw_parser()` | TSQL语法解析器 |
| **分析重写** | `pg_analyze_and_rewrite_fixedparams()` | BBF analyzer hooks | 表名映射、列转换等 |
| **计划** | `pg_plan_queries()` | 无需特殊处理 | 标准计划生成 |
| **执行** | `PortalRun()` | BBF ProcessUtility hook | DDL特殊处理 |

### 6.3 方言设置

```c
/* 设置sql_dialect为TSQL以启用BBF特定功能 */
sql_dialect = SQL_DIALECT_TSQL;

/* 如果需要，也可以通过GUC设置 */
set_config_option(
    "babelfishpg_tsql.sql_dialect",
    "tsql",
    GUC_CONTEXT_SESSION,
    true
);
```

---

## 七、事务与快照管理

### 7.1 事务处理

```c
/*
 * 事务边界管理
 */
static void
exec_sql_via_portal(const char *sql_text)
{
    /* 每个SQL语句作为独立事务执行 */
    StartTransactionCommand();
    // ... SQL执行 ...
    CommitTransactionCommand();
}

/*
 * 批量SQL执行 (可选)
 */
static void
exec_batch_via_portal(const char *sql_batch)
{
    StartTransactionCommand();

    /* 解析多个语句 */
    List *parsetree_list = pg_parse_query(sql_batch);
    ListCell *lc;
    foreach(lc, parsetree_list)
    {
        RawStmt *parsetree = lfirst_node(RawStmt, lc);
        /* 执行每个语句 */
        exec_single_statement(parsetree, sql_batch);
    }

    CommitTransactionCommand();
}
```

### 7.2 快照管理

```c
/*
 * PortalStart中自动处理快照
 * 在PORTAL_ONE_SELECT策略下会自动调用GetTransactionSnapshot()
 */
PortalStart(Portal portal, ParamListInfo params, int eflags, Snapshot snapshot)
{
    // ...
    if (snapshot)
        PushActiveSnapshot(snapshot);
    else
        PushActiveSnapshot(GetTransactionSnapshot());  // 自动获取事务快照
    // ...
}
```

---

## 八、错误处理与恢复

### 8.1 错误处理

```c
static void
exec_sql_via_portal(const char *sql_text)
{
    MemoryContext oldcontext;
    MemoryContext per_sql_context;

    /* 创建独立内存上下文 */
    per_sql_context = AllocSetContextCreate(
        CurrentMemoryContext,
        "SQL Replayer per-stmt",
        ALLOCSET_DEFAULT_SIZES
    );
    oldcontext = MemoryContextSwitchTo(per_sql_context);

    /* 使用PG_TRY保护执行 */
    PG_TRY();
    {
        /* 执行SQL */
        // ... 执行逻辑 ...
    }
    PG_CATCH();
    {
        /* 错误处理 */
        FlushErrorState();
        ereport(LOG,
            errmsg("SQL execution failed: %s", sql_text));
        RollbackTransactionCommand();
    }
    PG_END_TRY();

    /* 清理内存上下文 */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(per_sql_context);
}
```

### 8.2 中断处理

```c
/* 在循环中检查中断 */
for (;;)
{
    CHECK_FOR_INTERRUPTS();  // 检查SIGINT、SIGTERM等

    char *sql = get_next_sql_from_queue();
    if (sql != NULL)
    {
        exec_sql_via_portal(sql);
        pfree(sql);
    }
}
```

---

## 九、已知限制与注意事项

### 9.1 Session级状态

| 问题 | 影响 | 解决方案 |
|------|------|----------|
| **GUC设置** | 某些BBF GUC需要在session开始时设置 | 子进程初始化后设置`sql_dialect`等GUC |
| **Session锁** | fork后锁状态需要重新获取 | 使用`LockAcquire`重新获取需要的锁 |
| **Prepared Statement** | 需要重新prepare | 在子进程中重新创建 |

### 9.2 Temp Table处理

BBF对temp table使用OID缓冲机制，子进程中需要确保：

```c
/* 重新初始化temp table OID缓冲 */
if (temp_oid_buffer_size > 0)
{
    /* OID缓冲在fork后需要重新初始化 */
    BUFFER_START_TO_OID = InvalidOid;
}
```

### 9.3 事务嵌套

BBF支持嵌套事务（`NestedTranCount`），子进程初始化时该值为0，行为正确。

---

## 十、总结

### 10.1 方案优势

1. **与客户端执行等价**：最终都经过相同的执行路径
2. **完整BBF支持**：所有DDL经过`standard_ProcessUtility()` + BBF hooks
3. **无socket依赖**：通过共享内存通信，适合fork场景
4. **事务隔离**：每个SQL独立事务执行

### 10.2 实施步骤

```
1. fork()后子进程调用BackgroundWorkerInitializeConnection()
2. 设置sql_dialect = SQL_DIALECT_TSQL
3. 实现exec_sql_via_portal()函数
4. 实现错误处理和恢复机制
5. 集成到逻辑复制DDL传播流程
```

### 10.3 代码位置建议

```
新增文件: src/backend/replication/logical/sql_replayer.c
          src/backend/replication/logical/sql_replayer.h
修改文件: src/backend/replication/logical/worker.c (集成点)
```

---

## 十一、集成到逻辑复制Apply Worker设计

### 11.1 现有架构分析

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ 逻辑复制Apply Worker消息处理流程                                            │
│ │
│  walrcv_receive() ← 从Publisher接收消息 │
│         │                                                                     │
│         ▼                                                                     │
│  apply_dispatch(s) ←消息分发 │
│         │                                                                     │
│         ├── 'B' → apply_handle_begin()      ← 事务开始                        │
│         ├── 'C' → apply_handle_commit() ← 事务提交                        │
│         ├── 'I' → apply_handle_insert()     ← INSERT操作                      │
│         ├── 'U' → apply_handle_update()     ← UPDATE操作                      │
│         ├── 'D' → apply_handle_delete()     ← DELETE操作                      │
│         ├── 'T' → apply_handle_truncate()   ← TRUNCATE操作                    │
│         ├── 'R' → apply_handle_relation()   ← Relation定义 │
│         └── ... │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

**现有消息类型** (`LogicalRepMsgType`):
- `LOGICAL_REP_MSG_BEGIN` ('B') - 事务开始
- `LOGICAL_REP_MSG_COMMIT` ('C') - 事务提交
- `LOGICAL_REP_MSG_INSERT` ('I') - INSERT
- `LOGICAL_REP_MSG_UPDATE` ('U') - UPDATE
- `LOGICAL_REP_MSG_DELETE` ('D') - DELETE
- `LOGICAL_REP_MSG_TRUNCATE` ('T') - TRUNCATE
- `LOGICAL_REP_MSG_RELATION` ('R') - 表结构定义
- 等等...

**注意**：当前逻辑复制**不支持DDL消息类型**。

### 11.2 DDL集成架构设计

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  扩展后的逻辑复制DDL处理流程                                                  │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  Publisher端 │      │
│  │ │      │
│  │  standard_ProcessUtility()                                         │      │
│  │         │                                                            │      │
│  │         ├── 检测到DDL语句 (CREATE/ALTER/DROP)                        │      │
│  │         │ │      │
│  │         ▼ │      │
│  │  ┌──────────────────────────────────────────────────────────────┐   │      │
│  │  │ DDL Capture (在standard_ProcessUtility中)                    │   │      │
│  │  │  - 判断sql_dialect = TSQL                                     │   │      │
│  │  │  - 将DDL SQL字符串序列化 │   │      │
│  │  │  - 写入reorderbuffer或直接发送                                 │   │      │
│  │  └──────────────────────────────────────────────────────────────┘   │      │
│  │ │                                                            │      │
│  │         ▼                                                            │      │
│  │  发送 'X' (DDL) 消息类型 ─────────────────────────────────────────▶│ │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  Subscriber端                                                        │      │
│  │                                                                     │      │
│  │ ◀───────────────────────────────────────────────────────────── 'X' │ │
│  │ │                                                            │      │
│  │         ▼ │      │
│  │  apply_handle_ddl(s)                                                 │      │
│  │         │                                                            │      │
│  │         ▼ │      │
│  │  exec_sql_via_portal(ddl_sql)  ← Portal方式执行DDL │      │
│  │         │                                                            │      │
│  │         ├── babelfishpg_tsql_raw_parser()  ← TSQL解析                │      │
│  │         ├── pg_analyze_and_rewrite_fixedparams() ← BBF hooks介入     │      │
│  │         ├── pg_plan_queries()                                        │      │
│  │         └── PortalRun() → standard_ProcessUtility() → DDL执行       │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.3 协议扩展设计

#### 11.3.1 新增消息类型

```c
// src/include/replication/logicalproto.h

typedef enum LogicalRepMsgType
{
    // ... 现有类型 ...
    LOGICAL_REP_MSG_DDL = 'X',  // 新增：DDL消息
} LogicalRepMsgType;
```

#### 11.3.2 DDL消息结构

```c
// DDL消息格式
struct LogicalRepDDLData
{
    int32 nbytes;              // DDL SQL字符串长度
    char    schema[NAMEDATALEN]; // 目标schema名称
    char    sql[];               // DDL SQL字符串 (变长)
};
```

### 11.4 Publisher端实现

#### 11.4.1捕获点选择

在`standard_ProcessUtility`中捕获DDL语句：

```c
// src/backend/tcop/utility.c

static void
standard_ProcessUtility(PlannedStmt *pstmt, ...)
{
    Node *parsetree = pstmt->utilityStmt;

    // 检测DDL类型
    switch (nodeTag(parsetree))
    {
        case T_CreateStmt: // CREATE TABLE/INDEX/etc
        case T_AlterTableStmt:     // ALTER TABLE
        case T_DropStmt:           // DROP TABLE/etc
        case T_CreateSchemaStmt:   // CREATE SCHEMA
        case T_CreateFunctionStmt: // CREATE FUNCTION
        case T_ViewStmt:           // CREATE VIEW
        // ... 其他DDL类型
        {
            // 如果启用了DDL复制且为TSQL方言
            if (sql_dialect == SQL_DIALECT_TSQL && ddl_replication_enabled)
            {
                // 序列化DDL并发送
                send_ddl_via_logical_replication(parsetree, queryString);
            }
            break;
        }
    }

    // 继续标准处理
    // ...
}
```

#### 11.4.2 发送DDL消息

```c
// src/backend/replication/logical/message.c 或 reorderbuffer.c

void
send_ddl_via_logical_replication(Node *parsetree, const char *queryString)
{
    StringInfoData s;
    MemoryContext oldcontext;

    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&s);

    // 写入消息类型
    pq_sendbyte(&s, LOGICAL_REP_MSG_DDL);

    // 写入schema信息 (从parsetree中提取)
    const char *schema = get_ddl_schema_name(parsetree);
    pq_sendstring(&s, schema ? schema : "");

    // 写入DDL SQL
    pq_sendstring(&s, queryString);

    // 通过wal sender发送
    LogicalRepMsgSend(&s);

    MemoryContextSwitchTo(oldcontext);
}
```

### 11.5 Subscriber端实现

#### 11.5.1消息处理函数

```c
// src/backend/replication/logical/worker.c

/*
 * 处理接收到的DDL消息
 */
static void
apply_handle_ddl(StringInfo s)
{
    char *schema;
    char       *ddl_sql;
    MemoryContext oldcontext;
    MemoryContext ddl_context;

    /* 创建独立内存上下文处理DDL */
    ddl_context = AllocSetContextCreate(
        CurrentMemoryContext,
        "DDL apply context",
        ALLOCSET_DEFAULT_SIZES
    );
    oldcontext = MemoryContextSwitchTo(ddl_context);

    /* 读取schema和DDL SQL */
    schema = pq_getmsgstring(s);
    ddl_sql = pq_getmsgstring(s);

    /* 验证消息完整性 */
    if (ddl_sql == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
             errmsg("invalid DDL message: missing SQL text")));

    /* 开始事务 */
    StartTransactionCommand();

    /* 可选：设置schema上下文 */
    if (schema && schema[0] != '\0')
    {
        PushActiveSnapshot(GetTransactionSnapshot());
        /* 设置当前schema */
        // set_current_schema(schema);
        PopActiveSnapshot();
    }

    /* 使用Portal方式执行DDL */
    exec_sql_via_portal(ddl_sql);

    /* 提交事务 */
    CommitTransactionCommand();

    /* 清理 */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextDelete(ddl_context);
}
```

#### 11.5.2 注册消息处理

```c
// apply_dispatch()中添加新消息类型处理

switch (action)
{
    // ... 现有处理 ...

    case LOGICAL_REP_MSG_DDL:
        apply_handle_ddl(s);
        break;

    default:
        ereport(ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
             errmsg("invalid logical replication message type %d", action)));
}
```

### 11.6 exec_sql_via_portal实现

```c
// src/backend/replication/logical/sql_replayer.c

/*
 * 通过Portal方式执行SQL (支持DDL和BBF扩展)
 */
static void
exec_sql_via_portal(const char *sql_text)
{
    List *parsetree_list;
    List       *querytree_list;
    List       *plantree_list;
    Portal      portal;
    CommandTag  commandTag;
    QueryCompletion qc;
    DestReceiver *receiver;

    /* 使用PG_TRY保护执行 */
    PG_TRY();
    {
        /* 1. 解析SQL - 使用BBF的TSQL解析器(如果可用) */
#ifdef EXTERANAL_BABELFISH
        if (sql_dialect == SQL_DIALECT_TSQL)
            parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
        else
#endif
            parsetree_list = pg_parse_query(sql_text);

        if (parsetree_list == NIL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("parse failed for: %s", sql_text)));

        /* 2. 分析和重写 - BBF analyzer hooks在此阶段介入 */
        querytree_list = pg_analyze_and_rewrite_fixedparams(
            linitial_node(RawStmt, parsetree_list),
            sql_text,
            NULL, 0, NULL);

        /* 3. 生成计划 */
        plantree_list = pg_plan_queries(
            querytree_list,
            sql_text,
            CURSOR_OPT_PARALLEL_OK,
            NULL);

        /* 4. 创建Portal */
        portal = CreatePortal("ddl_replayer", true, true);
        portal->visible = false;

        /* 5. 定义查询到Portal */
        commandTag = CreateCommandTag(
            linitial_node(PlannedStmt, plantree_list)->utilityStmt);
        PortalDefineQuery(
            portal,
            NULL,
            sql_text,
            commandTag,
            plantree_list,
            NULL);

        /* 6. 启动Portal */
        PortalStart(portal, NULL, 0, InvalidSnapshot);

        /* 7. 创建目标接收器 (DDL不需要结果) */
        receiver = CreateDestReceiver(DestNone);

        /* 8. 执行Portal */
        (void) PortalRun(
            portal,
            FETCH_ALL,
            true,   // isTopLevel
            receiver,
            receiver,
            &qc);

        /* 9. 清理 */
        receiver->rDestroy(receiver);
        PortalDrop(portal, false);
    }
    PG_CATCH();
    {
        /* 错误处理 */
        FlushErrorState();
        ereport(ERROR,
            (errmsg("DDL execution failed: %s", sql_text)));
    }
    PG_END_TRY();
}
```

### 11.7 集成点汇总

| 文件 | 修改内容 | 说明 |
|------|----------|------|
| `src/include/replication/logicalproto.h` | 添加`LOGICAL_REP_MSG_DDL = 'X'` | 新消息类型 |
| `src/backend/tcop/utility.c` | 在`standard_ProcessUtility`中添加DDL捕获 | Publisher端捕获点 |
| `src/backend/replication/logical/message.c` | 添加`send_ddl_via_logical_replication()` | DDL消息发送 |
| `src/backend/replication/logical/worker.c` | 添加`apply_handle_ddl()`并注册到`apply_dispatch()` | Subscriber端处理 |
| `src/backend/replication/logical/sql_replayer.c` | 新增文件，实现`exec_sql_via_portal()` | SQL执行核心函数 |

### 11.8 事务边界处理

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│  DDL消息的事务边界处理                                                        │
│                                                                               │
│  Publisher端:                                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │ 'B' (BEGIN)                                                        │      │
│  │ │                                                                │      │
│  │    ▼                                                                │      │
│  │ 'X' (DDL: CREATE TABLE foo ...) ──────────────────────────────────▶│      │
│  │    │                                                                │      │
│  │    ▼                                                                │      │
│  │ 'C' (COMMIT)                                                       │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                               │
│  问题: DDL在事务内执行可能有限制 │
│  解决: 如果DDL不能嵌套在事务内，可以: │
│        1. DDL消息在BEGIN/COMMIT之外单独发送                                    │
│        2. 或者Subscriber端在执行DDL前自动切割事务 │
│                                                                               │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 11.9 错误处理策略

|错误类型 | 处理策略 | 说明 |
|----------|----------|------|
| **语法错误** | 记录错误日志，继续处理 | DDL语法错误可能是 Publisher和Subscriber版本不兼容 |
| **对象不存在** | 如果`IF EXISTS`，忽略；否则报错 | DROP操作常见 |
| **对象已存在** | 如果`IF NOT EXISTS`，忽略；否则报错 | CREATE操作常见 |
| **权限不足** | 报错并暂停复制 | 需要人工干预 |
| **死锁** | 重试3次后报错 | 并发DDL可能发生 |

### 11.10 性能考虑

1. **批量DDL合并**：多个连续DDL可以合并为一个消息
2. **异步执行**：DDL执行不阻塞后续DML消息处理
3. **内存上下文**：每个DDL使用独立内存上下文，避免内存泄漏

---

## 附录A：关键函数索引

| 函数 | 位置 | 说明 |
|------|------|------|
| `babelfishpg_tsql_raw_parser` | babelfishpg_tsql | TSQL解析器 |
| `pg_parse_query` | tcop/postgres.c | 标准PG解析器 |
| `pg_analyze_and_rewrite_fixedparams` | tcop/postgres.c | 分析重写 |
| `pg_plan_queries` | optimizer/plan/planner.c | 计划生成 |
| `CreatePortal` | tcop/pquery.c | 创建Portal |
| `PortalStart` | tcop/pquery.c | 启动Portal |
| `PortalRun` | tcop/pquery.c | 执行Portal |
| `PortalDrop` | tcop/pquery.c | 销毁Portal |
| `standard_ProcessUtility` | tcop/utility.c | 标准Utility处理 |
| `BackgroundWorkerInitializeConnection` | postmaster/bgworker.c | Worker初始化 |
| `InitPostgres` | utils/init/postinit.c | Postgres初始化 |

---

## 附录B：Portal策略选择

```
ChoosePortalStrategy() 根据语句类型选择策略:

┌─────────────────────────────────────────────────────────────────┐
│  list_length(stmts) == 1                                        │
├─────────────────────────────────────────────────────────────────┤
│  Node类型 = Query │
│    ├── commandType == CMD_SELECT │
│    │   ├── hasModifyingCTE → PORTAL_ONE_MOD_WITH              │
│    │   └── 无 → PORTAL_ONE_SELECT                              │
│    └── commandType == CMD_UTILITY                             │
│        ├── UtilityReturnsTuples → PORTAL_UTIL_SELECT          │
│        └── 其他 → PORTAL_MULTI_QUERY                          │
│                                                                  │
│  Node类型 = PlannedStmt                                         │
│    └── (同上)                                                   │
└─────────────────────────────────────────────────────────────────┘
```

DDL语句（`commandType == CMD_UTILITY`）选择`PORTAL_MULTI_QUERY`，最终调用`PortalRunUtility()`执行。