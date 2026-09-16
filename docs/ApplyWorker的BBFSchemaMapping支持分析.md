# Apply Worker 的 BBF Schema Mapping 支持分析

## 一、问题背景（修正）

### 1.1 核心前提

**关键事实**：Apply Worker 收到的 DDL 是**原始 TSQL 格式**，例如：

```
CREATE TABLE dbo.test (id INT);
CREATE PROCEDURE dbo.sp_test AS SELECT * FROM t;
```

**不是**已经转换成 PG 格式的 DDL。

### 1.2核心问题

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  核心问题: Apply Worker 如何正确解析和执行 TSQL DDL │
│                                                                             │
│  Apply Worker 收到: │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  CREATE TABLE dbo.test (id INT);  ← 原始 TSQL 语法                    │      │
│  │    schema: dbo                                                       │      │
│  │    语法: TSQL (SQL Server 风格)                                       │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│           │                                                                     │
│           ▼                                                                     │
│  Apply Worker 需要: │
│           │                                                                     │
│           ├── 1. 解析 TSQL 语法 → babelfishpg_tsql_raw_parser()              │
│           ├── 2. Schema 映射 dbo → public → gram_hook                       │
│           └── 3. 执行转换后的 PG DDL → standard_ProcessUtility()           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.3 与之前的差异

| 场景 | 之前理解 | 实际情况 |
|------|----------|----------|
| DDL 格式 | PG 格式 (`public.test`) | **TSQL 格式** (`dbo.test`) |
| 解析器 | `pg_parse_query()`即可 | **必须用** `babelfishpg_tsql_raw_parser()` |
| gram_hook | 可能不触发 | **会正确触发** |
| schema 映射 | 需要逆向推断 | **正向映射** dbo → public |

---

## 二、TSQL DDL 的 Schema Mapping 流程

### 2.1 完整执行流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 执行 TSQL DDL 的完整流程 │
│                                                                             │
│  1. 接收 DDL 消息                                                            │
│       │ │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  DDL: CREATE TABLE dbo.test (id INT);                              │      │
│  │       - 原始 TSQL 语法                                               │      │
│  │       - schema: dbo │      │
│  │       -目标: SQL Server 表 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  2. 解析阶段 (babelfishpg_tsql_raw_parser) │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT) │      │
│  │       │ │      │
│  │       ▼ │      │
│  │  gram_hook 介入 │      │
│  │       │ │      │
│  │       ├── 检测到 schema: dbo                                          │      │
│  │       ├── 查询 schema映射表                                          │      │
│  │       └── dbo → public转换 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  3. 分析重写阶段 (pg_analyze_and_rewrite_fixedparams)                       │
│       │ │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  BBF analyzer hooks 介入 │      │
│  │       │ │      │
│  │       ├── 表名解析: dbo.test → public.test                            │      │
│  │       ├── 列名映射 (如果需要)                                         │      │
│  │       └── 类型转换 (如果需要)                                          │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  4.计划阶段 (pg_plan_queries)                                             │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  生成 PlannedStmt                                                   │      │
│  │       │ │      │
│  │       └── utilityStmt = CreateStmt (CREATE TABLE public.test ...) │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  5. 执行阶段 (PortalRun → standard_ProcessUtility) │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  ProcessUtility_hook (BBF)                                          │      │
│  │       │ │      │
│  │       ├── DDL 类型判断 │      │
│  │       └── 执行 BBF 特定处理 (如果需要)                                │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  6. 结果 │
│       │                                                                      │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  在 PostgreSQL 中创建: public.test │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 BBF Schema Mapping 转换表

| TSQL Schema | PG Schema | 说明 |
|-------------|-----------|------|
| `dbo` | `public` | 默认数据库所有者 |
| `guest` | `guest` | 访客 schema |
| `INFORMATION_SCHEMA` | `pg_catalog` | 信息 schema |
| `sys` | `pg_catalog` | 系统 schema |

---

## 三、Apply Worker 必须满足的条件

### 3.1 必需的 BBF 环境配置

```c
void
ApplyWorkerInitializeBBF(const char *dbname, const char *username)
{
    /* 1. 初始化数据库连接 */
    BackgroundWorkerInitializeConnection(dbname, username, 0);

    /* 2. 设置 BBF TSQL 方言 - 这是最关键的配置 */
#ifdef EXTERNAL_BABELFISH
    sql_dialect = SQL_DIALECT_TSQL;  // ←启用 BBF hooks
#endif

    /* 3. 设置 search_path - 包含 dbo 以便 BBF 正确映射 */
    set_config_option(
        "search_path",
        "dbo, public, pg_catalog",  // dbo 在前，BBF 会映射到 public
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );
}
```

### 3.2 必需的 Hooks 状态

| Hook | 状态 | 作用 |
|------|------|------|
| `gram_hook` | **必须已设置** | TSQL 解析阶段的 schema 转换 |
| `ProcessUtility_hook` | **必须已设置** | DDL 执行阶段的 BBF 处理 |
| `sql_dialect` | **必须为 TSQL** | 触发 BBF 特定逻辑的开关 |

### 3.3 验证 BBF 初始化

```c
static void
verify_bbf_initialization(void)
{
#ifdef EXTERNAL_BABELFISH
    /*验证 sql_dialect */
    if (sql_dialect != SQL_DIALECT_TSQL)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("Apply Worker must run with sql_dialect = TSQL")));

    /* 验证 gram_hook */
    if (gram_hook == NULL)
        ereport(ERROR,
            (errcode(ERRCODE_INTERNAL_ERROR),
             errmsg("BBF gram_hook is not set - Babelfish may not be loaded")));

    /* 验证 search_path 包含 dbo */
    {
        const char *path = GetConfigOption("search_path", false, false);
        if (path == NULL || strstr(path, "dbo") == NULL)
            ereport(WARNING,
                (errmsg("search_path does not contain dbo - schema mapping may fail")));
    }
#endif
}
```

---

## 四、exec_sql_via_portal 的正确实现

### 4.1 关键代码

```c
static void
exec_sql_via_portal(const char *sql_text)
{
    List *parsetree_list;
    List *querytree_list;
    List *plantree_list;
    Portal portal;
    CommandTag commandTag;
    QueryCompletion qc;

    /* 开始事务 */
    StartTransactionCommand();

    PG_TRY();
    {
        /*
         * 关键步骤: 使用 BBF TSQL 解析器
         *
         * 当 sql_dialect = SQL_DIALECT_TSQL 时:
         * - babelfishpg_tsql_raw_parser() 会使用 TSQL 语法解析
         * - gram_hook 会介入进行 schema 转换 (dbo → public)
         */
#ifdef EXTERNAL_BABELFISH
        if (sql_dialect == SQL_DIALECT_TSQL)
        {
            /* 使用 BBF TSQL 解析器 */
            parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
        }
        else
#endif
        {
            /* Fallback 到标准 PG 解析器 (可能无法处理 TSQL 语法) */
            parsetree_list = pg_parse_query(sql_text);
        }

        if (parsetree_list == NIL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("parse failed for: %s", sql_text)));

        /*
         * 分析重写阶段
         * BBF analyzer hooks 在此阶段介入，进行:
         * - 表名解析
         * - 列名映射
         * - 类型转换
         */
        querytree_list = pg_analyze_and_rewrite_fixedparams(
            linitial_node(RawStmt, parsetree_list),
            sql_text,
            NULL, 0, NULL
        );

        /* 计划生成 */
        plantree_list = pg_plan_queries(
            querytree_list,
            sql_text,
            CURSOR_OPT_PARALLEL_OK,
            NULL
        );

        /* 创建 Portal */
        portal = CreatePortal("ddl_replayer", true, true);
        portal->visible = false;

        /* 定义查询 */
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

        /* 启动 Portal */
        PortalStart(portal, NULL, 0, InvalidSnapshot);

        /* 创建目标接收器 */
        DestReceiver *receiver = CreateDestReceiver(DestNone);

        /* 执行 Portal */
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
        ereport(ERROR,
            (errmsg("DDL execution failed: %s", sql_text)));
    }
    PG_END_TRY();

    /* 提交事务 */
    CommitTransactionCommand();
}
```

### 4.2 关键点说明

| 步骤 | 关键调用 | 说明 |
|------|----------|------|
| **解析** | `babelfishpg_tsql_raw_parser()` | TSQL 语法解析，触发 `gram_hook` |
| **分析重写** | `pg_analyze_and_rewrite_fixedparams()` | BBF analyzer hooks 介入 |
| **执行** | `PortalRun()` → `standard_ProcessUtility()` | 触发 `ProcessUtility_hook` |

---

## 五、TSQL DDL 覆盖范围

### 5.1 支持的 DDL 类型

| DDL 类型 | TSQL 语法示例 | gram_hook 触发 | ProcessUtility_hook 触发 |
|----------|---------------|----------------|-------------------------|
| `CREATE TABLE` | `CREATE TABLE dbo.test (id INT);` | ✅ | ✅ |
| `ALTER TABLE` | `ALTER TABLE dbo.test ADD col INT;` | ✅ | ✅ |
| `DROP TABLE` | `DROP TABLE dbo.test;` | ✅ | ✅ |
| `CREATE INDEX` | `CREATE INDEX idx ON dbo.test(id);` | ✅ | ✅ |
| `CREATE VIEW` | `CREATE VIEW dbo.v AS SELECT * FROM t;` | ✅ | ✅ |
| `CREATE TRIGGER` | `CREATE TRIGGER dbo.tr ON dbo.test;` | ✅ | ✅ |
| `CREATE PROCEDURE` | `CREATE PROCEDURE dbo.sp AS SELECT 1;` | ✅ | ✅ |
| `CREATE FUNCTION` | `CREATE FUNCTION dbo.f() RETURNS INT;` | ✅ | ✅ |
| `CREATE SCHEMA` | `CREATE SCHEMA dbo;` | ✅ | ✅ |
| `DROP DATABASE` | `DROP DATABASE mydb;` | N/A | ✅ (BBF 特殊处理) |

### 5.2 Schema Mapping详情

```c
/*
 * gram_hook 中的 schema 转换逻辑 (伪代码)
 */
Node *
gram_hook_parse_analyze(
    const char *sql_text,
    RawStmt *raw_stmt,
    ...)
{
    if (sql_dialect == SQL_DIALECT_TSQL)
    {
        switch (nodeTag(raw_stmt->stmt))
        {
            case T_CreateStmt:
            {
                CreateStmt *stmt = (CreateStmt *) raw_stmt->stmt;
                List *relation = stmt->relation;

                /*
                 * 遍历每个表名，检查 schema
                 */
                foreach(lc, relation)
                {
                    RangeVar *rv = lfirst(lc);

                    /*
                     * 如果 schema 名为 "dbo"，转换为 "public"
                     */
                    if (rv->schemaname &&
                        strcmp(rv->schemaname, "dbo") == 0)
                    {
                        rv->schemaname = "public";
                    }
                }
                break;
            }
            // ... 其他 DDL 类型 ...
        }
    }

    /* 调用原始的 parse_analyze */
    return original_parse_analyze(sql_text, raw_stmt, ...);
}
```

---

## 六、潜在问题与解决方案

### 6.1 问题1: BBF 扩展未加载

**问题**：如果 Babelfish 扩展未加载，`babelfishpg_tsql_raw_parser`不可用。

**解决方案**：

```c
static List *
safe_parse_sql(const char *sql_text)
{
    List *parsetree_list = NIL;

#ifdef EXTERNAL_BABELFISH
    if (sql_dialect == SQL_DIALECT_TSQL)
    {
        /* BBF 已编译入，尝试使用 TSQL 解析器 */
        if (babelfishpg_tsql_raw_parser != NULL)
        {
            parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
        }
        else
        {
            ereport(ERROR,
                (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                 errmsg("TSQL dialect requested but BBF parser not available")));
        }
    }
    else
#endif
    {
        parsetree_list = pg_parse_query(sql_text);
    }

    return parsetree_list;
}
```

### 6.2 问题2: search_path 不包含 dbo

**问题**：`search_path` 中没有 `dbo`，导致 BBF 无法正确进行 schema 映射。

**解决方案**：

```c
static void
ensure_search_path_for_bbf(void)
{
    const char *current_path;
    char *new_path;

    current_path = GetConfigOption("search_path", false, false);

    /* 检查是否包含 dbo */
    if (current_path == NULL || strstr(current_path, "dbo") == NULL)
    {
        /* 在开头添加 dbo */
        new_path = psprintf("dbo, %s", current_path);
        set_config_option("search_path", new_path, PGC_USERSET, GUC_CONTEXT_SESSION);
        pfree(new_path);
    }
}
```

### 6.3 问题3: gram_hook 未触发 schema 转换

**问题**：gram_hook 可能由于某种原因未正确触发。

**诊断方法**：

```c
static void
diagnose_gram_hook(void)
{
#ifdef EXTERNAL_BABELFISH
    /* 检查 gram_hook 是否设置 */
    if (gram_hook == NULL)
    {
        ereport(WARNING,
            (errmsg("gram_hook is NULL - BBF parser hooks not active")));
        return;
    }

    /* 检查当前 sql_dialect */
    if (sql_dialect != SQL_DIALECT_TSQL)
    {
        ereport(WARNING,
            (errmsg("sql_dialect is not TSQL - gram_hook may not trigger")));
        return;
    }

    ereport(LOG,
        (errmsg("gram_hook is properly configured")));
#endif
}
```

---

## 七、完整的 Apply Worker 初始化流程

### 7.1 初始化序列

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker BBF 初始化完整流程 │
│                                                                             │
│  1. Background Worker启动                                                   │
│       │                                                                      │
│       ▼                                                                      │
│  2. BackgroundWorkerInitializeConnection() │
│       │                                                                      │
│       ├── InitProcessPhase2()        → 加入 ProcArray                        │
│       ├── pgstat_beinit()            → 统计初始化 │
│       ├── SharedInvalidationBackendInit() → 失效管理初始化 │
│       ├── RelationCacheInitialize()   → Relation缓存初始化                   │
│       ├── InitCatalogCache()         → Catalog 缓存初始化                     │
│       └── StartTransactionCommand()  → 开始第一个事务                        │
│                                                                             │
│  3. 设置 BBF 环境 │
│       │                                                                      │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  sql_dialect = SQL_DIALECT_TSQL                                      │      │
│  │       └── 启用 BBF hooks                                              │      │
│  │                                                                      │      │
│  │  search_path = "dbo, public, pg_catalog"                            │      │
│  │       └── 确保 dbo 在前，支持 schema映射                             │      │
│  │                                                                      │      │
│  │  set_config_option("babelfishpg_tsql.database_name", dbname)        │      │
│  │       └── 设置 BBF 数据库名                                           │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  4. 验证 BBF 初始化                                                          │
│       │                                                                      │
│       ▼                                                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  verify_bbf_initialization()                                       │      │
│  │       ├── 检查 gram_hook != NULL                                    │      │
│  │       ├── 检查 ProcessUtility_hook != NULL │      │
│  │       ├── 检查 sql_dialect == TSQL                                  │      │
│  │       └── 检查 search_path 包含 dbo                                 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  5. 进入主循环                                                              │
│       │                                                                      │
│       ▼ │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  for (;;) │      │
│  │  { │      │
│  │      message = walrcv_receive(); │      │
│  │      if (message.type == DDL)                                      │      │
│  │          exec_sql_via_portal(message.sql);  ← TSQL DDL 执行 │      │
│  │      else │      │
│  │          apply_handle_xxx(message);  ← 其他消息处理                 │      │
│  │  }                                                                  │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 7.2 完整初始化代码

```c
void
ApplyWorkerMain(Datum main_arg)
{
    BackgroundWorkerStatus status;

    /* 阻塞信号 */
    BackgroundWorkerBlockSignals();

    /* 初始化数据库连接 */
    BackgroundWorkerInitializeConnection(
        dbname,           /* 数据库名 */
        username,         /* 用户名 */
        0                 /* flags */
    );
    BackgroundWorkerUnblockSignals();

    /* 设置 BBF 环境 */
#ifdef EXTERNAL_BABELFISH
    /* 启用 TSQL 方言 */
    sql_dialect = SQL_DIALECT_TSQL;

    /* 设置 search_path 以支持 schema 映射 */
    set_config_option(
        "search_path",
        "dbo, public, pg_catalog",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /* 设置 BBF 数据库名 */
    set_config_option(
        "babelfishpg_tsql.database_name",
        dbname,
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /* 验证 BBF 初始化状态 */
    verify_bbf_initialization();
#else
    /* 非 BBF 环境 */
    set_config_option(
        "search_path",
        "public, pg_catalog",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );
#endif

    /* 进入主循环 */
    for (;;)
    {
        CHECK_FOR_INTERRUPTS();

        if (GotSIGTERM)
        {
            status = BGW_TERMINATED;
            break;
        }

        /*接收 WAL 消息 */
        bytea *message = walrcv_receive(&timeout);
        if (message == NULL)
            continue;

        /* 处理消息 */
        switch (message->type)
        {
            case LOGICAL_REP_MSG_DDL:
                /* TSQL DDL - 通过 Portal 执行 */
                exec_sql_via_portal(message->sql);
                break;

            case LOGICAL_REP_MSG_INSERT:
            case LOGICAL_REP_MSG_UPDATE:
            case LOGICAL_REP_MSG_DELETE:
                /* DML 操作 */
                apply_handle_dml(message);
                break;

            case LOGICAL_REP_MSG_BEGIN:
                apply_handle_begin(message);
                break;

            case LOGICAL_REP_MSG_COMMIT:
                apply_handle_commit(message);
                break;

            default:
                ereport(WARNING,
                    (errmsg("unknown message type: %d", message->type)));
                break;
        }
    }

    proc_exit(0);
}
```

---

## 八、结论

### 8.1 Apply Worker 支持 BBF Schema Mapping 的条件

| 条件 | 必须性 | 说明 |
|------|--------|------|
| **sql_dialect = SQL_DIALECT_TSQL** | ✅ 必须 | 启用 BBF hooks 的开关 |
| **search_path 包含 dbo** | ✅ 必须 | BBF schema 映射依赖 |
| **babelfishpg_tsql_raw_parser 可用** | ✅ 必须 | TSQL DDL 解析 |
| **gram_hook 已设置** | ✅ 必须 | schema 转换 (dbo → public) |
| **ProcessUtility_hook 已设置** | ✅ 必须 | DDL 执行时的 BBF 处理 |

### 8.2 关键结论

1. **收到的是原始 TSQL DDL**，所以 `babelfishpg_tsql_raw_parser()` 能正确解析
2. **gram_hook 会正确触发** schema 转换 (dbo → public)
3. **执行流程与 TSQL 客户端直接执行相同**，都能正确支持 BBF 特定 DDL

### 8.3 架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 执行 TSQL DDL 架构图 │
│                                                                             │
│  Publisher Subscriber │
│  ───────── ──────────                              │
│                                                                 │
│  TSQL Client Apply Worker                            │
│       │                                  │                                   │
│       ▼                                  │                                   │
│  ┌─────────────┐                        │                                   │
│  │ babelfish- │                        │                                   │
│  │ pg_tsql_ │ ──── logical rep ────▶ │ │
│  │ raw_parser  │  DDL (TSQL 原文)        │                                   │
│  └─────────────┘                        │                                   │
│       │                                  ▼                                   │
│       │                          ┌─────────────────┐                      │
│       │                          │ exec_sql_via_ │                      │
│       │                          │ portal() │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       │                                  ▼                                   │
│       │                         ┌─────────────────┐                      │
│       │                          │ babelfishpg_   │                      │
│       │                          │ tsql_raw_ │                      │
│       │                          │ parser()       │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       │                                  ▼                                   │
│       │                         ┌─────────────────┐                      │
│       │                          │ gram_hook 触发   │                      │
│       │                          │ dbo → public    │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       │                                  ▼                                   │
│       │                         ┌─────────────────┐                      │
│       │                          │ pg_analyze_and_ │                      │
│       │                          │ rewrite_ │                      │
│       │                          │ fixedparams()   │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       │                                  ▼                                   │
│       │                          ┌─────────────────┐                      │
│       │                          │ standard_       │                      │
│       │                          │ ProcessUtility │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       │                                  ▼                                   │
│       │                         ┌─────────────────┐                      │
│       │                          │ CREATE TABLE   │                      │
│       │                          │ public.test    │                      │
│       │                          └─────────────────┘                      │
│       │                                  │                                   │
│       ▼                                  ▼                                   │
│  public.test ◀────────────────────────────┘                                   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 附录：相关文件

| 文件 | 说明 |
|------|------|
| `src/backend/replication/logical/worker.c` | Apply Worker 主循环 |
| `src/backend/parser/parser.c` | `raw_parser()` 实现 |
| `src/backend/tcop/utility.c` | `standard_ProcessUtility()` |
| `src/backend/catalog/namespace.c` | `search_path` 处理 |
| Babelfish 扩展 | `babelfishpg_tsql_raw_parser()`, `gram_hook` 实现 |