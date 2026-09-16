# Apply Worker 获取 BBF 上下文方案分析

## 一、BBF Background Worker 初始化模式

### 1.1 标准 Background Worker 初始化流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  PostgreSQL Background Worker 初始化标准流程                              │
│                                                                             │
│  postmaster                                                                │
│     │                                                                      │
│     ├── fork() → Background Worker进程                                   │
│     │                                                                      │
│     ▼ │
│  BackendStartup() (bgworker.c)                                              │
│     │                                                                      │
│     ├── InitProcess() → 创建 PGPROC 结构                                   │
│     │                                                                      │
│     ├── BaseInit() → 基础初始化                                            │
│     │                                                                      │
│     ▼ │
│  bgworker_main() │
│     │                                                                      │
│     ├── LookupBackgroundWorkerFunction() → 查找入口函数                    │
│     │                                                                      │
│     └── 调用 worker入口函数 (如 ApplyWorkerMain)                         │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Apply Worker 初始化流程 (现有实现)

```c
// src/backend/replication/logical/worker.c

// ApplyWorkerMain - Apply Worker入口函数
void
ApplyWorkerMain(Datum main_arg)
{
    int worker_slot = DatumGetInt32(main_arg);

    InitializingApplyWorker = true;
    SetupApplyOrSyncWorker(worker_slot);  // ← 核心初始化
    InitializingApplyWorker = false;

    run_apply_worker();
    proc_exit(0);
}

// SetupApplyOrSyncWorker - 设置 Apply 或 Tablesync Worker
void
SetupApplyOrSyncWorker(int worker_slot)
{
    logicalrep_worker_attach(worker_slot);  // 关联到共享内存中的 worker slot

    /* 设置信号处理 */
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    BackgroundWorkerUnblockSignals();

    /* 初始化统计信息 */
    MyLogicalRepWorker->last_send_time =
    MyLogicalRepWorker->last_recv_time = GetCurrentTimestamp();

    load_file("libpqwalreceiver", false);

    InitializeLogRepWorker();  // ← 数据库连接初始化
}

// InitializeLogRepWorker - 核心初始化函数
void
InitializeLogRepWorker(void)
{
    /* 运行为 replica session replication role */
    SetConfigOption("session_replication_role", "replica",
                     PGC_SUSET, PGC_S_OVERRIDE);

    /* 连接数据库 */
    BackgroundWorkerInitializeConnectionByOid(
        MyLogicalRepWorker->dbid,
        MyLogicalRepWorker->userid,
        0);

    /*
     * 设置安全的 search_path，防止恶意用户重定向代码
     */
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    /* 加载订阅信息到持久内存上下文 */
    ApplyContext = AllocSetContextCreate(...);
    StartTransactionCommand();
    // ... 加载订阅信息 ...
}
```

---

## 二、Apply Worker 获取 BBF 上下文的必要性

### 2.1 问题分析

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 执行 TSQL DDL 的问题                                           │
│                                                                             │
│  现有 Apply Worker 初始化: │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  InitializeLogRepWorker() │      │
│  │       │                                                              │      │
│  │       ├── BackgroundWorkerInitializeConnectionByOid() ← 连接数据库   │      │
│  │       │                                                              │      │
│  │       ├── SetConfigOption("search_path", "", ...) ← 清空路径        │      │
│  │       │                                                              │      │
│  │       └── 加载订阅信息                                               │      │
│  │                                                                      │      │
│  │  问题:                                                              │      │
│  │  - sql_dialect 未设置 (默认为 PG) │      │
│  │  - search_path 为空                                                   │      │
│  │  - BBF hooks 未初始化 │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
│  结果:                                                                      │
│  - 无法正确解析 TSQL 语法                                                    │
│  - gram_hook 不会触发                                                       │
│  - dbo → public schema映射失败 │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 需要的 BBF 上下文

| BBF 上下文组件 | 作用 | 设置方式 |
|---------------|------|----------|
| `sql_dialect = SQL_DIALECT_TSQL` | 启用 BBF hooks | GUC 设置 |
| `search_path` | 包含 dbo 以支持 schema 映射 | GUC 设置 |
| `babelfishpg_tsql.database_name` | BBF 数据库名 | GUC 设置 |
| `gram_hook` | TSQL 解析器的 schema 转换 | BBF 扩展设置 |
| `ProcessUtility_hook` | DDL 执行时的 BBF 处理 | BBF 扩展设置 |

---

## 三、BBF Background Worker 初始化模式设计

### 3.1 借鉴 slotsync Worker 的初始化模式

```c
// slotsync.c 中的模式
void
ReplSlotSyncWorkerMain(const void *startup_data, size_t startup_data_len)
{
    MyBackendType = B_SLOTSYNC_WORKER;
    init_ps_display(NULL);

    InitProcess(); // 创建 PGPROC
    BaseInit();             // 基础初始化

    /* 设置信号 */
    pqsignal(SIGHUP, SignalHandlerForConfigReload);
    pqsignal(SIGTERM, die);
    // ...

    /* 设置安全的 search_path */
    SetConfigOption("search_path", "", PGC_SUSET, PGC_S_OVERRIDE);

    /* 连接数据库 */
    InitPostgres(dbname, InvalidOid, NULL, InvalidOid, 0, NULL);

    SetProcessingMode(NormalProcessing);

    // 主循环
}
```

### 3.2 Apply Worker BBF 初始化方案

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker BBF 上下文初始化方案                                         │
│                                                                             │
│  方案A: 修改现有 InitializeLogRepWorker() │
│  ─────────────────────────────────────────────────────────                  │
│  在现有函数中添加 BBF 特定初始化 │
│                                                                             │
│  方案B: 新增 InitializeLogRepWorkerBBF()                                   │
│  ─────────────────────────────────────────────────────────                  │
│  在调用 InitializeLogRepWorker()之后调用 BBF 特定初始化                   │
│                                                                             │
│  方案C: 模块化初始化 (推荐)                                                │
│  ─────────────────────────────────────────────────────────                  │
│  ┌─────────────────────────────────────────────────────────────────────┐      │
│  │  InitializeLogRepWorker() │      │
│  │       │                                                              │      │
│  │       ├── BackgroundWorkerInitializeConnectionByOid()               │      │
│  │       ├── SetConfigOption("search_path", "")                        │      │
│  │       └── 加载订阅信息                                               │      │
│  │                                                                      │      │
│  │  InitializeBBFContext()  ← 新增                                      │      │
│  │       │                                                              │      │
│  │       ├── set_config_option("sql_dialect", "tsql")                   │      │
│  │       ├── set_config_option("search_path", "dbo, public, pg_catalog")│      │
│  │       ├── set_config_option("babelfishpg_tsql.database_name", ...)   │      │
│  │       └── verify_bbf_hooks()                                         │      │
│  └─────────────────────────────────────────────────────────────────────┘      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 四、详细实现方案

### 4.1 核心初始化函数

```c
/*
 * InitializeBBFContext - 初始化 BBF 上下文
 *
 * 在 InitializeLogRepWorker() 之后调用
 */
static void
InitializeBBFContext(void)
{
    const char *dbname;

#ifdef EXTERNAL_BABELFISH
    /* 检查 BBF 是否已加载 */
    if (!process_babelfish_extensions())
    {
        ereport(WARNING,
            (errmsg("Babelfish extensions not loaded, TSQL DDL replication will not work")));
        return;
    }

    /*
     * 设置 sql_dialect 为 TSQL
     * 这是启用 BBF hooks 的关键开关
     */
    set_config_option(
        "babelfishpg_tsql.sql_dialect",
        "tsql",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /*
     * 设置 search_path 以支持 BBF schema 映射
     * dbo 在前，BBF 会将其映射到 public
     */
    set_config_option(
        "search_path",
        "dbo, public, pg_catalog",
        PGC_USERSET,
        GUC_CONTEXT_SESSION
    );

    /*
     * 获取数据库名并设置 BBF 数据库名
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
     * 验证 BBF hooks 是否正确设置
     */
    verify_bbf_hooks();

#else
    /* 非 BBF 环境 */
    ereport(DEBUG1,
        (errmsg("BBF not compiled in, TSQL DDL replication disabled")));
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

### 4.2 修改 ApplyWorkerMain

```c
void
ApplyWorkerMain(Datum main_arg)
{
    int worker_slot = DatumGetInt32(main_arg);

    InitializingApplyWorker = true;
    SetupApplyOrSyncWorker(worker_slot);

    /*
     * BBF 上下文初始化
     * 在数据库连接建立后、开始处理消息前调用
     */
    InitializeBBFContext();

    InitializingApplyWorker = false;

    run_apply_worker();
    proc_exit(0);
}
```

### 4.3 完整的初始化流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker + BBF 完整初始化流程 │
│                                                                             │
│  ApplyWorkerMain(Datum main_arg) │
│       │                                                                      │
│       ▼ │
│  SetupApplyOrSyncWorker(worker_slot) │
│       │                                                                      │
│       ├── logicalrep_worker_attach(worker_slot) │
│       ├── pqsignal(SIGHUP, SIGTERM, ...) │
│       ├── BackgroundWorkerUnblockSignals()                                  │
│       └── InitializeLogRepWorker() │
│                │                                                           │
│                ├── SetConfigOption("session_replication_role", "replica") │
│                ├── BackgroundWorkerInitializeConnectionByOid()             │
│                ├── SetConfigOption("search_path", "") │
│                └──加载订阅信息                                             │
│                                                                             │
│  InitializeBBFContext()  ← 新增 │
│       │ │
│       ├── set_config_option("sql_dialect", "tsql") │
│       ├── set_config_option("search_path", "dbo, public, pg_catalog")      │
│       ├── set_config_option("babelfishpg_tsql.database_name", dbname)     │
│       └── verify_bbf_hooks()                                                │
│                                                                             │
│  run_apply_worker() │
│       │                                                                      │
│       └── 处理逻辑复制消息 (包括 TSQL DDL) │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 五、条件编译处理

### 5.1 使用 #ifdef EXTERNAL_BABELFISH

```c
/*
 * BBF 支持的条件编译
 *
 * 在编译 PostgreSQL 时，如果包含了 Babelfish 扩展，
 * 会定义 EXTERNAL_BABELFISH 宏
 */

#ifdef EXTERNAL_BABELFISH
#include<babelfish/headers.h>  // BBF 相关的头文件

static void
InitializeBBFContext(void)
{
    /* BBF 特定的初始化 */
    sql_dialect = SQL_DIALECT_TSQL;
    set_config_option(...);
    verify_bbf_hooks();
}

#else

/* 非 BBF 环境下的空实现 */
static void
InitializeBBFContext(void)
{
    /* 不做任何事 */
    ereport(DEBUG1,
        (errmsg("BBF not available in this build")));
}

#endif
```

### 5.2 编译配置 (meson.build)

```python
# src/backend/replication/logical/meson.build
# 添加条件编译支持

if get_option('babelfish')
    conf_data.set('EXTERNAL_BABELFISH', true)
    user_opts += [ '--with-babelfish' ]
endif
```

---

## 六、search_path 设置的详细分析

### 6.1 BBF Schema 映射规则

| SQL Server Schema | PostgreSQL Schema | search_path 位置 |
|------------------|------------------|------------------|
| `dbo` | `public` | 必须在前 |
| `guest` | `guest` | 可选 |
| `INFORMATION_SCHEMA` | `pg_catalog` | 可选 |
| `sys` | `pg_catalog` | 可选 |

### 6.2 search_path 设置策略

```c
/*
 * search_path 设置的三种策略
 */

/*
 * 策略1: 仅 BBF 环境 (推荐)
 * 只在 BBF 环境下设置 dbo
 */
if (is_bbf_environment())
{
    set_config_option("search_path", "dbo, public, pg_catalog", ...);
}

/*
 * 策略2: 保留原有 + BBF
 * 在原有 search_path 基础上添加 dbo
 */
static void
setup_search_path_for_bbf(void)
{
    const char *current_path = GetConfigOption("search_path", false, false);

    if (strstr(current_path, "dbo") == NULL)
    {
        char *new_path = psprintf("dbo, %s", current_path);
        set_config_option("search_path", new_path, PGC_USERSET, GUC_CONTEXT_SESSION);
        pfree(new_path);
    }
}

/*
 * 策略3: 始终设置为 BBF 默认值
 * 适用于纯 BBF 环境
 */
set_config_option("search_path", "dbo, public, pg_catalog", PGC_USERSET, GUC_CONTEXT_SESSION);
```

---

## 七、错误处理与恢复

### 7.1 BBF 初始化失败的处理

```c
static void
InitializeBBFContext(void)
{
#ifdef EXTERNAL_BABELFISH
    /* 检查 BBF 是否可用 */
    if (!process_babelfish_extensions())
    {
        ereport(WARNING,
            (errmsg("Babelfish extensions not loaded")));
        return;
    }

    PG_TRY();
    {
        /* 设置 BBF 上下文 */
        sql_dialect = SQL_DIALECT_TSQL;
        set_config_option("babelfishpg_tsql.sql_dialect", "tsql", ...);
        set_config_option("search_path", "dbo, public, pg_catalog", ...);
        set_config_option("babelfishpg_tsql.database_name", dbname, ...);

        /* 验证 hooks */
        verify_bbf_hooks();
    }
    PG_CATCH();
    {
        /* BBF 初始化失败，回退到非 BBF 模式 */
        FlushErrorState();
        ereport(WARNING,
            (errmsg("BBF context initialization failed, continuing without BBF support")));
    }
    PG_END_TRY();
#endif
}
```

### 7.2 BBF hooks验证

```c
static void
verify_bbf_hooks(void)
{
#ifdef EXTERNAL_BABELFISH
    bool hooks_ok = true;

    if (gram_hook == NULL)
    {
        ereport(WARNING, errmsg("gram_hook is NULL"));
        hooks_ok = false;
    }

    if (ProcessUtility_hook == NULL)
    {
        ereport(WARNING, errmsg("ProcessUtility_hook is NULL"));
        hooks_ok = false;
    }

    if (sql_dialect != SQL_DIALECT_TSQL)
    {
        ereport(WARNING,
            (errmsg("sql_dialect is %d, expected %d (TSQL)",
                sql_dialect, SQL_DIALECT_TSQL)));
        hooks_ok = false;
    }

    if (!hooks_ok)
    {
        ereport(WARNING,
            (errmsg("BBF hooks not properly configured, TSQL DDL may not work correctly")));
    }
    else
    {
        ereport(DEBUG1,
            (errmsg("BBF context initialized successfully")));
    }
#endif
}
```

---

## 八、与其他 Worker 的对比

### 8.1 各 Worker 初始化的比较

| Worker 类型 | 数据库连接 | search_path | 特殊 GUC |
|------------|-----------|-------------|-----------|
| **slotsync** | `InitPostgres()` | 清空 | 无 |
| **apply** | `BackgroundWorkerInitializeConnectionByOid()` | 清空 | `session_replication_role=replica` |
| **apply+BBF** | 同上 | `dbo, public, pg_catalog` | `sql_dialect=tsql` |

### 8.2 初始化顺序对比

```
slotsync Worker:
1. InitProcess()
2. BaseInit()
3. SetConfigOption("search_path", "")
4. InitPostgres(dbname, ...)

apply Worker:
1. logicalrep_worker_attach()
2. SetupApplyOrSyncWorker()
3. SetConfigOption("session_replication_role", "replica")
4. BackgroundWorkerInitializeConnectionByOid()
5. SetConfigOption("search_path", "")

apply Worker + BBF:
1. logicalrep_worker_attach()
2. SetupApplyOrSyncWorker()
3. SetConfigOption("session_replication_role", "replica")
4. BackgroundWorkerInitializeConnectionByOid()
5. SetConfigOption("search_path", "")
6. InitializeBBFContext()  ← 新增
   ├── set_config_option("sql_dialect", "tsql")
   ├── set_config_option("search_path", "dbo, public, pg_catalog")
   ├── set_config_option("babelfishpg_tsql.database_name", ...)
   └── verify_bbf_hooks()
```

---

## 九、结论与建议

### 9.1 推荐方案

采用**方案C (模块化初始化)**，即新增 `InitializeBBFContext()` 函数，在 `InitializeLogRepWorker()` 之后调用。

### 9.2 实现要点

1. **条件编译**: 使用 `#ifdef EXTERNAL_BABELFISH`保护 BBF 相关代码
2. **GUC 设置**:
   - `sql_dialect = tsql` - 启用 BBF hooks
   - `search_path = dbo, public, pg_catalog` - 支持 schema 映射
3. **验证机制**: 验证 BBF hooks 是否正确设置
4. **错误处理**: BBF 初始化失败时回退到非 BBF 模式

### 9.3 代码位置

```
修改文件: src/backend/replication/logical/worker.c
新增函数: InitializeBBFContext(), verify_bbf_hooks()
修改位置: ApplyWorkerMain() 中，在 SetupApplyOrSyncWorker() 之后调用
```

---

## 附录：相关代码位置

| 文件 | 说明 |
|------|------|
| `src/backend/replication/logical/worker.c` | Apply Worker 主文件 |
| `src/backend/postmaster/bgworker.c` | Background Worker 初始化 |
| `src/backend/replication/logical/slotsync.c` | slotsync Worker 参考 |
| `src/include/postmaster/bgworker.h` | Background Worker 接口定义 |