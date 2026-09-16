# Apply Worker 与 BBF Backend 执行 DDL 对比分析

## 一、问题背景

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  核心问题：Apply Worker 执行 TSQL DDL 时，BBF Hooks 是否可用？              │
│                                                                             │
│  BBF Backend: │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  TDS连接 → TSQL Parser → gram_hook → PostgreSQL Executor           │      │
│  │                   ↑ │      │
│  │ BBF hooks 已设置 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  Apply Worker:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  Portal → TSQL Parser → ??? → PostgreSQL Executor                   │      │
│  │                       ↑                                              │      │
│  │                  BBF hooks 是否可用？                               │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 二、BBF Backend 执行 DDL 完整流程

### 2.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  BBF Backend 执行 DDL 完整流程 │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  1. PostgreSQL Backend 启动                                       │      │
│  │     postmaster → fork() → Backend进程                             │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  2. 共享库加载阶段 │      │
│  │     shared_preload_libraries 中的 BBF 扩展被加载                    │      │
│  │     - babelfishpg_tsql.so │      │
│  │     - _PG_init() 被调用 │      │
│  │     - Hooks 被注册到全局变量 │      │
│  │       • gram_hook = bbf_parse_analyze                              │      │
│  │       • ProcessUtility_hook = bbf_ProcessUtility                   │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  3. InitPostgres 阶段                                               │      │
│  │     -初始化数据库连接                                              │      │
│  │     - 设置 session上下文                                          │      │
│  │     -初始化 BBF 特定状态                                           │      │
│  │       • sql_dialect = SQL_DIALECT_TSQL (由客户端设置)              │      │
│  │       • search_path 配置 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  4. TDS协议处理 (pg_tds)                                           │      │
│  │     - 接收客户端 TDS 协议数据包 │      │
│  │     - 解析 TDS header │      │
│  │     - 提取 SQL 文本 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  5. TSQL 解析 (babelfishpg_tsql_raw_parser) │      │
│  │     - 使用 TSQL 词法分析器                                          │      │
│  │     - 使用 TSQL 语法规则 │      │
│  │     - 生成 RawStmt │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  6. gram_hook 介入 (关键！)                                         │      │
│  │     - bbf_parse_analyze() 被调用                                    │      │
│  │     - TSQL schema → PG schema 映射                                   │      │
│  │       • dbo.test → public.test │      │
│  │     - 表名/列名转换 │      │
│  │     - 数据类型转换                                                   │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  7. 分析与重写 (pg_analyze_and_rewrite_fixedparams) │      │
│  │     - BBF analyzer hooks 介入 │      │
│  │     - 生成 Query │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  8. 计划生成 (pg_plan_queries)                                      │      │
│  │     - 生成 PlannedStmt                                               │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  9. Portal 执行                                                     │      │
│  │     - PortalRun() → PortalRunUtility()                              │      │
│  │     - standard_ProcessUtility() │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  10. ProcessUtility_hook 介入 (关键！)                               │      │
│  │      - bbf_ProcessUtility() 被调用                                  │      │
│  │      - 处理 BBF 特定 DDL: │      │
│  │        • CREATE DATABASE → create_bbf_db()                          │      │
│  │        • CREATE FUNCTION → pltsql_createFunction()                 │      │
│  │        • DROP DATABASE → drop_bbf_db()                              │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  11. 实际 DDL 执行                                                  │      │
│  │      - standard_ProcessUtility() 执行实际的 DDL                    │      │
│  │      - 在 PostgreSQL 中创建对象 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 关键代码路径

```c
// BBF Backend 关键代码路径

// 1. 扩展加载 (_PG_init)
void _PG_init(void)
{
    // 注册 hooks
    gram_hook = bbf_parse_analyze;
    ProcessUtility_hook = bbf_ProcessUtility;
}

// 2. 客户端设置 sql_dialect
// (通过 TDS协议，客户端发送 "SET sql_dialect = tsql")

// 3. SQL 执行
exec_simple_query(sql_text)
{
    parsetree_list = babelfishpg_tsql_raw_parser(sql_text);
    //       ↑
    // gram_hook 在这里被调用

    querytree_list = pg_analyze_and_rewrite_fixedparams(...);
    plantree_list = pg_plan_queries(...);
    PortalRun(...);
    //       ↓
    //   ProcessUtility_hook 在这里被调用
}
```

---

## 三、Apply Worker 执行 DDL 当前流程

### 3.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 执行 DDL 当前流程 (有问题)                                 │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  1. Background Worker 启动                                         │      │
│  │     postmaster → fork() → Background Worker 进程                    │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  2. 共享库加载阶段                                                   │      │
│  │     - shared_preload_libraries 中的 BBF 扩展被加载 │      │
│  │     - hooks 被注册                                                   │      │
│  │    ⚠️ 但这里没有客户端连接，不知道 sql_dialect                 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  3. InitializeLogRepWorker() │      │
│  │     - BackgroundWorkerInitializeConnectionByOid()                    │      │
│  │     - SetConfigOption("search_path", "")                           │      │
│  │     ⚠️ 没有设置 sql_dialect │      │
│  │     ⚠️ BBF hooks 状态未知 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  4. 接收逻辑复制 DDL 消息                                            │      │
│  │     - 从 Publisher 接收原始 TSQL DDL │      │
│  │     -存入内存 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  5. exec_sql_via_portal()                                           │      │
│  │     - babelfishpg_tsql_raw_parser(sql_text)                         │      │
│  │      ⚠️ 如果 sql_dialect 未设置，可能用错解析器                   │      │
│  │     - pg_analyze_and_rewrite_fixedparams(...) │      │
│  │       ⚠️ BBF analyzer hooks 可能不触发                            │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  6. PortalRun() → standard_ProcessUtility()                          │      │
│  │    ⚠️ ProcessUtility_hook 可能不触发 BBF 特定处理 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 当前 Apply Worker 初始化代码

```c
// src/backend/replication/logical/worker.c

void
InitializeLogRepWorker(void)
{
    /* 设置 session_replication_role */
    SetConfigOption("session_replication_role", "replica",
                     PGC_SUSET, PGC_S_OVERRIDE);

    /* 连接数据库 */
    BackgroundWorkerInitializeConnectionByOid(
        MyLogicalRepWorker->dbid,
        MyLogicalRepWorker->userid,
        0);

    /*
     * 设置安全的 search_path
     * ⚠️ 问题：这里清空了 search_path，没有设置 BBF 特定的路径
     */
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    /* 加载订阅信息 */
    // ...
}
```

### 3.3 问题分析

| 问题 | BBF Backend | Apply Worker |
|------|-------------|-------------|
| **sql_dialect** | 客户端通过 TDS 协议设置 | ❌ 未设置 |
| **search_path** | BBF 默认值 (含 dbo) | ❌ 清空 |
| **gram_hook** | ✅ 已设置 |⚠️ 可能未正确调用 |
| **ProcessUtility_hook** | ✅ 已设置 | ⚠️ 可能未正确调用 |
| **schema mapping** | 自动 (dbo→public) | ❌ 不工作 |

---

## 四、Apply Worker + BBF 正确流程

### 4.1 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker + BBF 正确流程                                               │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  1. Background Worker 启动                                         │      │
│  │     postmaster → fork() → Background Worker 进程                    │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  2. 共享库加载阶段                                                   │      │
│  │     - BBF 扩展加载 │      │
│  │     - hooks 被注册到全局变量                                         │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  3. InitializeLogRepWorker() (现有代码)                             │      │
│  │     - BackgroundWorkerInitializeConnectionByOid()                    │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  4. InitializeBBFContext() (新增！)                                │      │
│  │     - set_config_option("sql_dialect", "tsql")                     │      │
│  │     - set_config_option("search_path", "dbo, public, pg_catalog")  │      │
│  │     - verify_bbf_hooks()                                           │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  5. 接收逻辑复制 DDL 消息 │      │
│  │     - 原始 TSQL DDL                                                  │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  6. exec_sql_via_portal()                                           │      │
│  │     - babelfishpg_tsql_raw_parser(sql_text)  ← BBF 解析器           │      │
│  │     - gram_hook 自动介入 (sql_dialect=TSQL)                         │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  7. PortalRun() → standard_ProcessUtility() │      │
│  │     - ProcessUtility_hook 触发 BBF 特定处理 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                              │                                              │
│                              ▼                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  8. DDL 执行成功 │      │
│  │ - PostgreSQL 中创建正确的对象                                     │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 新增 InitializeBBFContext 函数

```c
/*
 * InitializeBBFContext - 初始化 BBF 上下文
 *
 * 在 InitializeLogRepWorker() 之后调用
 * 设置 Apply Worker 执行 TSQL DDL 所需的 BBF 环境
 */
static void
InitializeBBFContext(void)
{
#ifdef EXTERNAL_BABELFISH
    const char *dbname;

    /*
     * 1. 设置 sql_dialect 为 TSQL
     * 这是启用 BBF hooks 的关键开关
     */
    set_config_option(
        "babelfishpg_tsql.sql_dialect",
        "tsql",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /*
     * 2. 设置 search_path 以支持 BBF schema 映射
     * dbo 在前，BBF 会将 dbo 映射到 public
     */
    set_config_option(
        "search_path",
        "dbo, public, pg_catalog",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /*
     * 3. 设置 BBF 数据库名
     */
    dbname = get_database_name(MyLogicalRepWorker->dbid);
    if (dbname)
    {
        set_config_option(
            "babelfishpg_tsql.database_name",
            dbname,
            PGC_USERSET,
            GUC_CONTEXT_SESSION
        );
    }

    /*
     * 4. 验证 BBF hooks 是否正确设置
     */
    verify_bbf_hooks();
#else
    ereport(DEBUG1,
        (errmsg("BBF not compiled in this build")));
#endif
}

/*
 * verify_bbf_hooks - 验证 BBF hooks 状态
 */
static void
verify_bbf_hooks(void)
{
#ifdef EXTERNAL_BABELFISH
    if (gram_hook == NULL)
        ereport(WARNING,
            (errmsg("gram_hook is NULL - BBF parser hooks not active")));
    else
        ereport(DEBUG1,
            (errmsg("BBF gram_hook is set")));

    if (ProcessUtility_hook == NULL)
        ereport(WARNING,
            (errmsg("ProcessUtility_hook is NULL - BBF DDL hooks not active")));
    else
        ereport(DEBUG1,
            (errmsg("BBF ProcessUtility_hook is set")));

    if (sql_dialect != SQL_DIALECT_TSQL)
        ereport(WARNING,
            (errmsg("sql_dialect is not TSQL - BBF features may not work")));
#endif
}
```

### 4.3 修改 ApplyWorkerMain

```c
void
ApplyWorkerMain(Datum main_arg)
{
    int worker_slot = DatumGetInt32(main_arg);

    InitializingApplyWorker = true;
    SetupApplyOrSyncWorker(worker_slot);

    /*
     * 新增：初始化 BBF 上下文
     * 在数据库连接建立后、开始处理消息前调用
     */
    InitializeBBFContext();

    InitializingApplyWorker = false;

    run_apply_worker();
    proc_exit(0);
}
```

---

## 五、exec_sql_via_portal 详细实现

### 5.1 完整代码

```c
/*
 * exec_sql_via_portal - 通过 Portal 执行 SQL
 *
 * 支持 TSQL DDL 的完整执行流程：
 * 1. 提取 schema 名称
 * 2. 自动创建缺失的 schema (如果是 CREATE TABLE 等)
 * 3. 使用 BBF 解析器解析
 * 4. 通过 Portal 执行
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
    char *schema_name = NULL;

    /* 开始事务 */
    StartTransactionCommand();

    PG_TRY();
    {
        /*
         * Step 1: 从原始 SQL 提取 schema 名称
         * 例如: "CREATE TABLE test.test1" → "test"
         */
        schema_name = extract_schema_from_sql(sql_text);

        /*
         * Step 2: 检查 schema 是否存在，如不存在则创建
         */
        if (schema_name != NULL && !schema_exists(schema_name))
        {
            if (is_create_object_ddl(sql_text))
            {
                char *create_schema_sql;

                create_schema_sql = psprintf("CREATE SCHEMA \"%s\"",
                    schema_name);
                exec_single_ddl(create_schema_sql);
                pfree(create_schema_sql);

                ereport(LOG,
                    (errmsg("created schema \"%s\" for DDL replication",
                        schema_name)));
            }
        }

        /*
         * Step 3: 解析 SQL
         * 关键：使用 BBF TSQL 解析器
         */
#ifdef EXTERNAL_BABELFISH
        if (sql_dialect == SQL_DIALECT_TSQL)
        {
            parsetree_list = babelfishpg_tsql_raw_parser(sql_text,
                RAW_PARSE_DEFAULT);
        }
        else
#endif
        {
            parsetree_list = pg_parse_query(sql_text);
        }

        if (parsetree_list == NIL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("parse failed for: %s", sql_text)));

        /*
         * Step 4: 分析和重写
         * BBF analyzer hooks 在此阶段介入
         */
        querytree_list = pg_analyze_and_rewrite_fixedparams(
            linitial_node(RawStmt, parsetree_list),
            sql_text,
            NULL, 0, NULL
        );

        /*
         * Step 5: 生成计划
         */
        plantree_list = pg_plan_queries(
            querytree_list,
            sql_text,
            CURSOR_OPT_PARALLEL_OK,
            NULL
        );

        /*
         * Step 6: 创建 Portal
         */
        portal = CreatePortal("ddl_replayer", true, true);
        portal->visible = false;

        /*
         * Step 7: 定义查询到 Portal
         */
        commandTag = CreateCommandTag(
            linitial_node(PlannedStmt, plantree_list)->utilityStmt);
        PortalDefineQuery(
            portal,
            NULL,
            sql_text,
            commandTag,
            plantree_list,
            NULL
        );

        /*
         * Step 8: 启动 Portal
         */
        PortalStart(portal, NULL, 0, InvalidSnapshot);

        /*
         * Step 9: 创建目标接收器
         */
        DestReceiver *receiver = CreateDestReceiver(DestNone);

        /*
         * Step 10: 执行 Portal
         *关键：PortalRun → PortalRunUtility → standard_ProcessUtility
         *       → ProcessUtility_hook (BBF)
         */
        (void) PortalRun(
            portal,
            FETCH_ALL,
            true,   // isTopLevel
            receiver,
            receiver,
            &qc
        );

        /* 清理 */
        receiver->rDestroy(receiver);
        PortalDrop(portal, false);
    }
    PG_CATCH();
    {
        FlushErrorState();
        RollbackTransactionCommand();
        if (schema_name) pfree(schema_name);
        return;
    }
    PG_END_TRY();

    CommitTransactionCommand();
    if (schema_name) pfree(schema_name);
}

/*
 * exec_single_ddl - 执行单条 DDL 语句
 */
static void
exec_single_ddl(const char *ddl_sql)
{
    List *parsetree_list;
    List *querytree_list;
    List *plantree_list;
    Portal portal;
    CommandTag commandTag;
    QueryCompletion qc;
    DestReceiver *receiver;

    parsetree_list = pg_parse_query(ddl_sql);

    querytree_list = pg_analyze_and_rewrite_fixedparams(
        linitial_node(RawStmt, parsetree_list),
        ddl_sql, NULL, 0, NULL);

    plantree_list = pg_plan_queries(querytree_list, ddl_sql,
        CURSOR_OPT_PARALLEL_OK, NULL);

    portal = CreatePortal("ddl_prep", true, true);
    commandTag = CreateCommandTag(
        linitial_node(PlannedStmt, plantree_list)->utilityStmt);
    PortalDefineQuery(portal, NULL, ddl_sql, commandTag, plantree_list, NULL);
    PortalStart(portal, NULL, 0, InvalidSnapshot);

    receiver = CreateDestReceiver(DestNone);
    (void) PortalRun(portal, FETCH_ALL, true, receiver, receiver, &qc);
    receiver->rDestroy(receiver);
    PortalDrop(portal, false);
}

/*
 * extract_schema_from_sql - 从 SQL 中提取 schema 名称
 */
static char *
extract_schema_from_sql(const char *sql_text)
{
    const char *p = sql_text;
    const char *dot;
    size_t schema_len;
    char *schema_name;

    /* 跳过开头的空格 */
    while (*p && isspace(*p)) p++;

    /* 跳过 SQL 关键词 (CREATE, DROP, ALTER, SELECT, etc) */
    while (*p && !isspace(*p)) p++;
    while (*p && isspace(*p)) p++;

    /* 跳过对象类型关键词 (TABLE, INDEX, VIEW, SCHEMA, etc) */
    while (*p && !isspace(*p)) p++;
    while (*p && isspace(*p)) p++;

    /* 检查是否有 schema.table 格式 */
    dot = strchr(p, '.');
    if (dot == NULL)
        return NULL;

    schema_len = dot - p;
    schema_name = palloc(schema_len + 1);
    strncpy(schema_name, p, schema_len);
    schema_name[schema_len] = '\0';

    return schema_name;
}

/*
 * schema_exists - 检查 schema 是否存在
 */
static bool
schema_exists(const char *schema_name)
{
    return OidIsValid(get_namespace_oid(schema_name, true));
}

/*
 * is_create_object_ddl - 判断是否是创建对象的 DDL
 */
static bool
is_create_object_ddl(const char *sql_text)
{
    return (strncasecmp(sql_text, "CREATE ", 7) == 0 ||
            strncasecmp(sql_text, "ALTER ", 6) == 0);
}
```

---

## 六、BBF Backend 与 Apply Worker 执行对比

### 6.1 执行环境对比

| 方面 | BBF Backend | Apply Worker |
|------|-------------|--------------|
| **进程类型** | Backend 进程 | Background Worker 进程 |
| **客户端连接** | TDS 协议连接 | 无客户端连接 |
| **sql_dialect 设置** | 客户端通过 TDS 设置 | 需要手动设置 |
| **search_path** | BBF 默认配置 | 需要手动设置 |
| **gram_hook** | ✅ 已初始化 | 需要验证 |
| **ProcessUtility_hook** | ✅ 已初始化 | 需要验证 |
| **schema mapping** | 自动工作 | 需要配置后工作 |

### 6.2 DDL 执行路径对比

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  BBF Backend 执行 DDL                                                      │
│                                                                             │
│  SQL: CREATE TABLE dbo.test (id INT); │
│       │                                                                     │
│       ▼                                                                     │
│  babelfishpg_tsql_raw_parser() → RawStmt                                  │
│       │                                                                     │
│       ▼                                                                     │
│  gram_hook (bbf_parse_analyze) → schema映射: dbo → public               │
│       │                                                                     │
│       ▼                                                                     │
│  Query → PlannedStmt (utilityStmt = CreateStmt)                          │
│       │                                                                     │
│       ▼                                                                     │
│  PortalRun() → standard_ProcessUtility()                                  │
│       │                                                                     │
│       ▼                                                                     │
│  ProcessUtility_hook (bbf_ProcessUtility) │
│       │                                                                     │
│       ▼                                                                     │
│  执行: CREATE TABLE public.test                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 执行 DDL                                                     │
│                                                                             │
│  SQL: CREATE TABLE dbo.test (id INT);                                     │
│       │                                                                     │
│       ▼                                                                     │
│  InitializeBBFContext() → sql_dialect = TSQL, search_path = "dbo,..." │
│       │                                                                     │
│       ▼                                                                     │
│  babelfishpg_tsql_raw_parser() → RawStmt                                  │
│       │                                                                     │
│       ▼                                                                     │
│  gram_hook (同上) → schema 映射: dbo → public                            │
│       │                                                                     │
│       ▼                                                                     │
│  Query → PlannedStmt                                                      │
│       │                                                                     │
│       ▼                                                                     │
│  PortalRun() → standard_ProcessUtility()                                   │
│       │                                                                     │
│       ▼                                                                     │
│  ProcessUtility_hook (bbf_ProcessUtility)                                 │
│       │                                                                     │
│       ▼                                                                     │
│  执行: CREATE TABLE public.test                                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 6.3 关键差异点

| 差异点 | BBF Backend | Apply Worker |
|--------|-------------|--------------|
| **初始化来源** | 扩展加载 + 客户端设置 | 手动调用 InitializeBBFContext() |
| **sql_dialect** | 自动设置为 TSQL | 需要显式设置 |
| **执行顺序保证** | 客户端保证 | 按消息顺序执行 |
| **事务边界** | 客户端控制 | Apply Worker 控制 |
| **错误处理** | 客户端负责 | 应用到 DDL 错误策略 |

---

## 七、完整的 Apply Worker 初始化流程

### 7.1 流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker + BBF 完整初始化流程                                         │
│                                                                             │
│  ApplyWorkerMain(Datum main_arg)                                          │
│       │ │
│       ▼                                                                      │
│  SetupApplyOrSyncWorker(worker_slot)                                      │
│       │                                                                      │
│       ├── logicalrep_worker_attach(worker_slot) │
│       │                                                                      │
│       ├── pqsignal(SIGHUP, SIGTERM, ...)                                   │
│       │                                                                      │
│       ├── BackgroundWorkerUnblockSignals()                                  │
│       │                                                                      │
│       └── InitializeLogRepWorker()                                          │
│                │                                                           │
│                ├── SetConfigOption("session_replication_role", "replica")  │
│                ├── BackgroundWorkerInitializeConnectionByOid()            │
│                └── SetConfigOption("search_path", "") │
│                                                                             │
│  InitializeBBFContext()  ← 新增                                             │
│       │ │
│       ├── set_config_option("sql_dialect", "tsql") │
│       │                                                                      │
│       ├── set_config_option("search_path", "dbo, public, pg_catalog")     │
│       │                                                                      │
│       ├── set_config_option("babelfishpg_tsql.database_name", ...) │
│       │                                                                      │
│       └── verify_bbf_hooks()                                              │
│                                                                             │
│  run_apply_worker()                                                         │
│       │                                                                      │
│       ├──接收 DDL 消息                                                    │
│       └── exec_sql_via_portal(ddl_sql)                                     │
│                │                                                           │
│                ├── extract_schema_from_sql() │
│                ├── schema_exists() → create if not │
│                ├── babelfishpg_tsql_raw_parser()                          │
│                ├── gram_hook 介入                                          │
│                └── PortalRun() → ProcessUtility_hook                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 修改的文件

```
修改文件: src/backend/replication/logical/worker.c

新增函数:
- InitializeBBFContext()
- verify_bbf_hooks()
- exec_sql_via_portal()
- exec_single_ddl()
- extract_schema_from_sql()
- schema_exists()
- is_create_object_ddl()

修改函数:
- ApplyWorkerMain() - 添加 InitializeBBFContext() 调用
```

---

## 八、结论

### 8.1 BBF Backend 与 Apply Worker 的核心区别

| 方面 | BBF Backend | Apply Worker |
|------|-------------|--------------|
| **初始化方式** | 自动 (扩展加载 + 客户端设置) | **手动** (调用 InitializeBBFContext) |
| **sql_dialect** |客户端通过 TDS 设置 | **必须显式设置** |
| **search_path** | BBF 默认配置 | **必须显式设置** |
| **hooks 调用** | 自动触发 | **依赖 sql_dialect 设置** |

### 8.2 Apply Worker 需要显式处理的原因

1. **Apply Worker 是 Background Worker**：不是通过 TDS 协议连接启动的，没有客户端来设置 `sql_dialect`
2. **需要模拟 BBF 上下文**：必须手动设置 `sql_dialect = TSQL` 来启用 BBF hooks
3. **需要配置 search_path**：BBF 的 schema 映射依赖于正确的 `search_path`

### 8.3 解决方案

在 `ApplyWorkerMain()` 中，`run_apply_worker()` 之前调用 `InitializeBBFContext()`，设置：
- `sql_dialect = tsql` - 启用 BBF hooks
- `search_path = dbo, public, pg_catalog` - 支持 schema 映射
- `babelfishpg_tsql.database_name` - BBF 数据库名

这样 Apply Worker 就能像 BBF Backend 一样正确执行 TSQL DDL。

---

## 附录：相关代码位置

| 文件 | 说明 |
|------|------|
| `src/backend/replication/logical/worker.c` | Apply Worker 主文件 |
| `src/backend/postmaster/bgworker.c` | Background Worker 初始化 |
| `src/backend/tcop/utility.c` | ProcessUtility 和 hooks |
| `src/backend/parser/parser.c` | raw_parser 实现 |
| `src/backend/replication/logical/slotsync.c` | slotsync Worker 参考 |