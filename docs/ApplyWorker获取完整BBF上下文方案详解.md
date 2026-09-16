# Apply Worker 获取完整 BBF 上下文方案详解

## 文档目的

本文档详细描述 Apply Worker（逻辑复制订阅端 worker）在执行 TSQL DDL 时如何获取完整的 Babelfish (BBF) 上下文，实现对 TSQL 语法和语义的正确支持。

---

## 一、完整 BBF 上下文组件清单

### 1.1 必需组件

| 组件 | 类型 | 设置方式 | 作用 |
|------|------|---------|------|
| `sql_dialect` | `int` | `sql_dialect = SQL_DIALECT_TSQL` | 启用 BBF hooks |
| `search_path` | `string` | `set_config_option("search_path", "dbo, public, pg_catalog", ...)` | 支持 dbo → public 映射 |
| `gram_hook` | `gram_hook_type` | BBF 自动设置 | TSQL 解析器 |
| `ProcessUtility_hook` | `ProcessUtility_hook_type` | BBF 自动设置 | DDL 处理 |

### 1.2 推荐组件

| 组件 | 类型 | 设置方式 | 作用 |
|------|------|---------|------|
| `babelfishpg_tsql.database_name` | `string` | GUC | BBF 数据库名 |
| `babelfishpg_tsql.schema_mapping` | `string` | GUC | Schema 映射配置 |
| `session_replication_role` | `string` | `set_config_option("session_replication_role", "replica", ...)` | 复制角色 |

### 1.3 可选组件（高级）

| 组件 | 类型 | 设置方式 | 作用 |
|------|------|---------|------|
| `pltsql_logical_repl_apply_mode` | `bool` | 新增 | BBF apply 模式标志 |
| `babelfishpg_tsql.statement_timeout` | `int` | GUC | 语句超时设置 |

---

## 二、方案总览

|方案 | 复杂度 | 改动范围 | 风险 | 推荐程度 |
|------|--------|---------|------|---------|
| **A. 手动设置 BBF GUC** | 低 | 小 | 中 | ⭐⭐⭐⭐ |
| **B. 消息驱动上下文恢复** | 中 | 中 | 低 | ⭐⭐⭐⭐⭐ |
| **C. 模拟 TDS 连接状态** | 高 | 大 | 高 | ⭐⭐ |
| **D. 复制 Parallel Worker 模式** | 中 | 中 | 中 | ⭐⭐⭐ |

---

## 三、方案 A 详细实现：手动设置 BBF GUC

### 3.1 核心思想

在 `ApplyWorkerMain()` 中，`SetupApplyOrSyncWorker()` 之后手动设置 BBF 相关的 GUC。

### 3.2 新增头文件：worker_bbf.h

```c
/*
 * worker_bbf.h - Apply Worker BBF Context 初始化接口
 *
 * 在 PostgreSQL 逻辑复制 worker 中初始化 Babelfish 上下文
 */

#ifndef WORKER_BBF_H
#define WORKER_BBF_H

#include "postgres.h"

/* 条件编译标志 */
#ifdef EXTERNAL_BABELFISH
#include "babelfish/headers.h"
#endif

/*
 * InitializeBBFContext - 初始化 BBF 上下文
 *
 * 在 InitializeLogRepWorker() 之后调用
 *
 * 失败时：记录 WARNING 日志，继续执行（非致命）
 */
extern void InitializeBBFContext(void);

/*
 * VerifyBBFHooks - 验证 BBF hooks 状态
 *
 * 用于调试和诊断
 */
extern void VerifyBBFHooks(void);

/*
 * IsBBFContextInitialized - 检查 BBF 上下文是否已初始化
 */
extern bool IsBBFContextInitialized(void);

#endif /* WORKER_BBF_H */
```

### 3.3 核心实现：worker_bbf.c

```c
/*
 * worker_bbf.c - Apply Worker BBF Context 初始化实现
 *
 * 负责在 Apply Worker 中初始化 Babelfish 上下文
 */

#include "postgres.h"

#include "replication/logical/worker.h"
#include "replication/logical/slotsync.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/elog.h"
#include "utils/ps_status.h"

#ifdef EXTERNAL_BABELFISH
#include "babelfish/headers.h"
#include "catalog.h"
#include "multidb.h"
#include "session.h"
#endif

/* 全局标志：BBF 上下文是否已初始化 */
static bool bbf_context_initialized = false;

/*
 * InitializeBBFContext - 初始化 BBF 上下文
 *
 * 调用时机：SetupApplyOrSyncWorker() 之后，run_apply_worker() 之前
 */
void
InitializeBBFContext(void)
{
#ifdef EXTERNAL_BABELFISH
    const char *dbname;
    bool success = true;

    ereport(LOG, errmsg("Initializing BBF context for Apply Worker"));

    /* 1. 检查 BBF 是否已加载 */
    if (!process_babelfish_extensions())
    {
        ereport(WARNING,
            (errmsg("Babelfish extensions not loaded, TSQL DDL replication disabled")));
        return;
    }

    /* 2. 设置 sql_dialect 为 TSQL */
    if (!set_config_option("babelfishpg_tsql.sql_dialect", "tsql",
                           PGC_USERSET, GUC_CONTEXT_SESSION, GUC_ACTION_SAVE,
                           true, 0, false))
    {
        ereport(WARNING, (errmsg("Failed to set sql_dialect to TSQL")));
        success = false;
    }
    else
    {
        ereport(DEBUG1, (errmsg("sql_dialect set to TSQL")));
    }

    /* 3. 设置 search_path 以支持 BBF schema 映射 */
    if (!set_config_option("search_path", "dbo, public, pg_catalog",
                           PGC_USERSET, GUC_CONTEXT_SESSION, GUC_ACTION_SAVE,
                           true, 0, false))
    {
        ereport(WARNING, (errmsg("Failed to set search_path")));
        success = false;
    }
    else
    {
        ereport(DEBUG1, (errmsg("search_path set to dbo, public, pg_catalog")));
    }

    /* 4. 获取并设置 BBF 数据库名 */
    dbname = get_database_name(MyLogicalRepWorker->dbid);
    if (dbname == NULL)
    {
        ereport(WARNING, (errmsg("Failed to get database name for dbid %d",
                                 MyLogicalRepWorker->dbid)));
        success = false;
    }
    else
    {
        if (!set_config_option("babelfishpg_tsql.database_name", dbname,
                               PGC_USERSET, GUC_CONTEXT_SESSION, GUC_ACTION_SAVE,
                               true, 0, false))
        {
            ereport(WARNING, (errmsg("Failed to set babelfishpg_tsql.database_name to %s",
                                     dbname)));
            success = false;
        }
        else
        {
            ereport(DEBUG1, (errmsg("babelfishpg_tsql.database_name set to %s", dbname)));
        }
    }

    /* 5. 验证 BBF hooks */
    if (success)
    {
        VerifyBBFHooks();
        bbf_context_initialized = true;
    }

#else
    ereport(DEBUG1, (errmsg("BBF not compiled in, TSQL DDL replication disabled")));
#endif
}

/*
 * VerifyBBFHooks - 验证 BBF hooks 状态
 *
 * 用于诊断 BBF 是否正确加载
 */
void
VerifyBBFHooks(void)
{
#ifdef EXTERNAL_BABELFISH
    bool hooks_ok = true;

    /* 检查 gram_hook */
    if (gram_hook == NULL)
    {
        ereport(WARNING,
            (errmsg("gram_hook is NULL - BBF parser hooks not active")));
        hooks_ok = false;
    }
    else
    {
        ereport(DEBUG1, (errmsg("BBF gram_hook is set")));
    }

    /* 检查 ProcessUtility_hook */
    if (ProcessUtility_hook == NULL)
    {
        ereport(WARNING,
            (errmsg("ProcessUtility_hook is NULL - BBF DDL hooks not active")));
        hooks_ok = false;
    }
    else
    {
        ereport(DEBUG1, (errmsg("BBF ProcessUtility_hook is set")));
    }

    /* 检查 sql_dialect */
    if (sql_dialect != SQL_DIALECT_TSQL)
    {
        ereport(WARNING,
            (errmsg("sql_dialect is %d, expected %d (TSQL)",
                    sql_dialect, SQL_DIALECT_TSQL)));
        hooks_ok = false;
    }
    else
    {
        ereport(DEBUG1, (errmsg("sql_dialect is TSQL")));
    }

    if (hooks_ok)
    {
        ereport(LOG, (errmsg("BBF context initialized successfully")));
    }
    else
    {
        ereport(WARNING,
            (errmsg("BBF hooks not properly configured, TSQL DDL may not work correctly")));
    }

#endif
}

/*
 * IsBBFContextInitialized - 检查 BBF 上下文是否已初始化
 */
bool
IsBBFContextInitialized(void)
{
    return bbf_context_initialized;
}
```

### 3.4 修改 worker.c：集成 BBF 上下文初始化

```c
/*
 * src/backend/replication/logical/worker.c
 *
 * 修改 ApplyWorkerMain() 函数
 */

#include "worker_bbf.h"  // 新增

void
ApplyWorkerMain(Datum main_arg)
{
    int worker_slot = DatumGetInt32(main_arg);

    /* 设置进程名称 */
    init_ps_display("logical replication worker", "", "", "");

    InitializingApplyWorker = true;

    /* 原有初始化流程 */
    SetupApplyOrSyncWorker(worker_slot);

    /* ===== 新增：BBF 上下文初始化 ===== */
    InitializeBBFContext();
    /* ================================== */

    InitializingApplyWorker = false;

    /* 检查 BBF 上下文是否成功初始化 */
    if (IsBBFContextInitialized())
    {
        ereport(LOG, (errmsg("Apply Worker BBF context ready")));
    }

    run_apply_worker();
    proc_exit(0);
}
```

### 3.5 条件编译处理

```c
/*
 * 在 worker.c 开头添加
 */

/* BBF 支持检测 */
#ifdef EXTERNAL_BABELFISH
#include <babelfish/headers.h>
extern bool process_babelfish_extensions(void);
extern int sql_dialect;
extern gram_hook_type gram_hook;
extern ProcessUtility_hook_type ProcessUtility_hook;
#define SQL_DIALECT_TSQL 1
#define SQL_DIALECT_PG 0
#endif

/*
 * 或者创建一个包装头文件 bbf_compat.h
 */

/* bbf_compat.h */
#ifndef BBF_COMPAT_H
#define BBF_COMPAT_H

#ifdef EXTERNAL_BABELFISH
#include <babelfish/headers.h>
#include <catalog.h>
#include <multidb.h>
#include <session.h>

/* BBF 类型和函数声明 */
extern bool process_babelfish_extensions(void);
extern int sql_dialect;
extern gram_hook_type gram_hook;
extern ProcessUtility_hook_type ProcessUtility_hook;

#define SQL_DIALECT_TSQL 1
#define SQL_DIALECT_PG 0
#else
/* 非 BBF 环境下的存根定义 */
#define sql_dialect 0
#define gram_hook NULL
#define ProcessUtility_hook NULL
static inline bool process_babelfish_extensions(void) { return false; }
#endif

#endif /* BBF_COMPAT_H */
```

---

## 四、方案 B 详细实现：消息驱动上下文恢复

### 4.1 核心思想

根据 DDL 消息中携带的上下文信息，动态恢复 BBF 语义。这是最符合 Babelfish 设计意图的方案。

### 4.2 消息输入结构体

```c
/*
 * bbf_ddl_replicate.h - BBF DDL 复制消息接口
 *
 * 定义 DDL 消息的结构和解析函数
 */

#ifndef BBF_DDL_REPLICATE_H
#define BBF_DDL_REPLICATE_H

#include "postgres.h"
#include "fmgr.h"
#include "nodes/parsenodes.h"

/*
 * BbfLogicalDdlApplyInput - DDL Apply 输入结构
 *
 * 从 DDL 消息中提取的上下文信息
 */
typedef struct BbfLogicalDdlApplyInput
{
    const char *db_mode;           /* 数据库模式: "tsql" 或 "postgres" */
    const char *logical_db_name;   /* 目标 logical database 名称 */
    const char *search_path;       /* 应用时的 search_path */
    const char *ddl_sql;          /* 原始 DDL SQL 语句 */
    const char *origin_lsn;       /* 原始 LSN（用于诊断） */
    Oid         db_oid;           /* 目标数据库 OID */
    Oid         user_oid;         /* 执行用户 OID */
} BbfLogicalDdlApplyInput;

/*
 * BbfDdlApplyResult - DDL Apply 执行结果
 */
typedef struct BbfDdlApplyResult
{
    bool        success;          /* 是否成功 */
    const char *error_message;    /* 错误信息（如果失败） */
    int         error_code;       /* 错误码 */
    const char *ddl_sql;          /* 执行的 DDL */
    const char *normalized_sql;    /* 规范化后的 SQL（用于日志） */
} BbfDdlApplyResult;

/*
 * DbMode - 支持的数据库模式
 */
typedef enum DbMode
{
    DB_MODE_PG,       /* PostgreSQL 模式 */
    DB_MODE_TSQL,     /* T-SQL 模式 (Babelfish) */
    DB_MODE_UNKNOWN   /* 未知模式 */
} DbMode;

#endif /* BBF_DDL_REPLICATE_H */
```

### 4.3 核心接口实现

```c
/*
 * bbf_ddl_replicate.c - BBF DDL 复制核心实现
 */

#include "postgres.h"
#include "bbf_ddl_replicate.h"
#include "worker_bbf.h"

#ifdef EXTERNAL_BABELFISH
#include "catalog.h"
#include "multidb.h"
#include "session.h"
#include "pltsql.h"
#endif

/* 保存的上下文（用于恢复） */
static struct SavedBBFContext
{
    int         saved_sql_dialect;
    char       *saved_search_path;
    char       *saved_database_name;
    bool        in_apply_mode;
} saved_bbf_context = {0};

/*
 * bbf_resolve_apply_sql_dialect - 根据 db_mode 解析 sql_dialect
 */
static int
bbf_resolve_apply_sql_dialect(const char *db_mode)
{
    if (db_mode == NULL)
        return SQL_DIALECT_PG;  /* 默认使用 PG 模式 */

    if (strcmp(db_mode, "tsql") == 0 || strcmp(db_mode, "babelfish") == 0)
        return SQL_DIALECT_TSQL;
    else if (strcmp(db_mode, "postgres") == 0 || strcmp(db_mode, "pg") == 0)
        return SQL_DIALECT_PG;
    else
        ereport(WARNING,
            (errmsg("Unknown db_mode: %s, using PG dialect", db_mode)));

    return SQL_DIALECT_PG;
}

/*
 * bbf_is_supported_apply_mode - 检查是否支持该数据库模式
 */
static bool
bbf_is_supported_apply_mode(const char *db_mode)
{
    if (db_mode == NULL)
        return false;

    return (strcmp(db_mode, "tsql") == 0 ||
            strcmp(db_mode, "babelfish") == 0 ||
            strcmp(db_mode, "postgres") == 0 ||
            strcmp(db_mode, "pg") == 0);
}

/*
 * bbf_enter_logical_ddl_apply_mode - 进入 BBF DDL apply 模式
 *
 * 保存当前上下文，设置 apply 模式标志
 */
void
bbf_enter_logical_ddl_apply_mode(void)
{
#ifdef EXTERNAL_BABELFISH
    saved_bbf_context.saved_sql_dialect = sql_dialect;
    saved_bbf_context.saved_search_path = pstrdup(GetConfigOption("search_path", false, false));
    saved_bbf_context.saved_database_name = pstrdup(GetConfigOption("babelfishpg_tsql.database_name", false, false));
    saved_bbf_context.in_apply_mode = true;

    ereport(DEBUG1, (errmsg("Entered BBF DDL apply mode")));
#endif
}

/*
 * bbf_leave_logical_ddl_apply_mode - 离开 BBF DDL apply 模式
 *
 * 恢复之前保存的上下文
 */
void
bbf_leave_logical_ddl_apply_mode(void)
{
#ifdef EXTERNAL_BABELFISH
    if (!saved_bbf_context.in_apply_mode)
        return;

    /* 恢复 sql_dialect */
    sql_dialect = saved_bbf_context.saved_sql_dialect;

    /* 恢复 search_path */
    set_config_option("search_path", saved_bbf_context.saved_search_path,
                      PGC_USERSET, GUC_CONTEXT_SESSION);

    /* 恢复 database_name */
    if (saved_bbf_context.saved_database_name)
    {
        set_config_option("babelfishpg_tsql.database_name",
                          saved_bbf_context.saved_database_name,
                          PGC_USERSET, GUC_CONTEXT_SESSION);
    }

    /* 清理保存的上下文 */
    if (saved_bbf_context.saved_search_path)
        pfree(saved_bbf_context.saved_search_path);
    if (saved_bbf_context.saved_database_name)
        pfree(saved_bbf_context.saved_database_name);

    saved_bbf_context.in_apply_mode = false;
    ereport(DEBUG1, (errmsg("Left BBF DDL apply mode")));
#endif
}

/*
 * bbf_restore_apply_database_context - 恢复目标数据库上下文
 *
 * 设置目标 logical database 和 search_path
 */
void
bbf_restore_apply_database_context(const char *logical_db_name,
                                     const char *search_path)
{
#ifdef EXTERNAL_BABELFISH
    Oid db_oid;

    if (logical_db_name == NULL)
    {
        ereport(WARNING, (errmsg("logical_db_name is NULL")));
        return;
    }

    /* 切换到目标数据库 */
    db_oid = get_database_oid(logical_db_name, false);
    if (!OidIsValid(db_oid))
    {
        ereport(ERROR,
            (errcode(ERRCODE_UNDEFINED_DATABASE),
             errmsg("Database %s does not exist", logical_db_name)));
    }

    /* 使用 BackgroundWorkerInitializeConnectionByOid 连接数据库 */
    /* 注意：这需要在事务中执行 */
    BackgroundWorkerInitializeConnectionByOid(db_oid, InvalidOid, 0);

    /* 设置 search_path */
    if (search_path != NULL)
    {
        set_config_option("search_path", search_path,
                          PGC_USERSET, GUC_CONTEXT_SESSION);
    }
    else
    {
        /* 默认使用 BBF 的 search_path */
        set_config_option("search_path", "dbo, public, pg_catalog",
                          PGC_USERSET, GUC_CONTEXT_SESSION);
    }

    ereport(DEBUG1, (errmsg("Restored apply context: db=%s, search_path=%s",
                            logical_db_name,
                            search_path ? search_path : "default")));
#endif
}

/*
 * bbf_reset_apply_database_context - 重置数据库上下文
 *
 * 恢复到默认状态
 */
void
bbf_reset_apply_database_context(void)
{
#ifdef EXTERNAL_BABELFISH
    /* 恢复到默认 search_path */
    set_config_option("search_path", "dbo, public, pg_catalog",
                      PGC_USERSET, GUC_CONTEXT_SESSION);

    ereport(DEBUG1, (errmsg("Reset apply database context")));
#endif
}

/*
 * bbf_build_apply_parsetree - 构建 parser tree
 *
 * 根据 sql_dialect 选择解析器
 */
static List *
bbf_build_apply_parsetree(const char *ddl_sql, int sql_dialect)
{
    List *parsetree_list = NIL;

    if (ddl_sql == NULL || strlen(ddl_sql) == 0)
    {
        ereport(WARNING, (errmsg("Empty DDL SQL")));
        return NIL;
    }

#ifdef EXTERNAL_BABELFISH
    if (sql_dialect == SQL_DIALECT_TSQL)
    {
        /* 使用 BBF TSQL 解析器 */
        parsetree_list = babelfishpg_tsql_raw_parser(ddl_sql, RAW_PARSE_DEFAULT);
    }
    else
#endif
    {
        /* 使用 PostgreSQL 标准解析器 */
        parsetree_list = pg_parse_query(ddl_sql);
    }

    if (parsetree_list == NIL)
    {
        ereport(ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg("Parse failed for: %s", ddl_sql)));
    }

    return parsetree_list;
}

/*
 * bbf_apply_logical_ddl_message - 执行 DDL 消息
 *
 * 核心执行函数
 */
BbfDdlApplyResult
bbf_apply_logical_ddl_message(BbfLogicalDdlApplyInput *input)
{
    BbfDdlApplyResult result = {0};
    List *parsetree_list = NIL;
    int target_dialect;

#ifdef EXTERNAL_BABELFISH
    /* 参数校验 */
    if (input == NULL || input->ddl_sql == NULL)
    {
        result.success = false;
        result.error_message = "Invalid input";
        result.error_code = ERRCODE_INTERNAL_ERROR;
        return result;
    }

    /* 检查是否支持该模式 */
    if (!bbf_is_supported_apply_mode(input->db_mode))
    {
        result.success = false;
        result.error_message = psprintf("Unsupported db_mode: %s",
                                        input->db_mode ? input->db_mode : "NULL");
        result.error_code = ERRCODE_INVALID_PARAMETER_VALUE;
        return result;
    }

    PG_TRY();
    {
        /* 1. 进入 BBF apply 模式 */
        bbf_enter_logical_ddl_apply_mode();

        /* 2. 恢复数据库上下文 */
        bbf_restore_apply_database_context(input->logical_db_name,
                                           input->search_path);

        /* 3. 解析 DDL */
        target_dialect = bbf_resolve_apply_sql_dialect(input->db_mode);
        parsetree_list = bbf_build_apply_parsetree(input->ddl_sql,
                                                  target_dialect);

        /* 4. 执行 DDL（通过 ProcessUtility） */
        // ... 执行逻辑 ...

        /* 5. 清理上下文 */
        bbf_reset_apply_database_context();
        bbf_leave_logical_ddl_apply_mode();

        result.success = true;
        result.ddl_sql = input->ddl_sql;
    }
    PG_CATCH();
    {
        FlushErrorState();

        result.success = false;
        result.error_message = psprintf("Failed to apply DDL: %s",
                                        input->ddl_sql);
        result.error_code = ERRCODE_INTERNAL_ERROR;

        /* 确保上下文被清理 */
        bbf_reset_apply_database_context();
        bbf_leave_logical_ddl_apply_mode();
    }
    PG_END_TRY();

#else
    result.success = false;
    result.error_message = "BBF not compiled in";
    result.error_code = ERRCODE_FEATURE_NOT_SUPPORTED;
#endif

    return result;
}

/*
 * bbf_in_tsql_semantic_context - 判断是否在 TSQL 语义上下文中
 *
 * 替代 IS_TDS_CONN() 的抽象函数
 * 当处于 BBF apply 模式时也返回 true
 */
bool
bbf_in_tsql_semantic_context(void)
{
#ifdef EXTERNAL_BABELFISH
    /* 方式1：直接检查 sql_dialect */
    if (sql_dialect == SQL_DIALECT_TSQL)
        return true;

    /* 方式2：检查是否在 BBF apply 模式 */
    if (saved_bbf_context.in_apply_mode)
        return true;

    /* 方式3：检查是否为 TDS 连接 */
    if (IS_TDS_CONN())
        return true;

    return false;
#else
    return false;
#endif
}
```

### 4.4 Q 消息完整执行流程

```c
/*
 * handle_bbf_ddl_q_message - 处理 BBF DDL Q 消息
 *
 * 完整的执行流程
 */
void
handle_bbf_ddl_q_message(const char *ddl_sql, const char *db_mode,
                        const char *logical_db_name, const char *search_path)
{
    BbfLogicalDdlApplyInput input;
    BbfDdlApplyResult result;

    /* 构建输入结构 */
    memset(&input, 0, sizeof(input));
    input.db_mode = db_mode;
    input.logical_db_name = logical_db_name;
    input.search_path = search_path;
    input.ddl_sql = ddl_sql;

    ereport(LOG,
        (errmsg("Processing BBF DDL: db_mode=%s, db=%s, sql=%s",
                db_mode ? db_mode : "NULL",
                logical_db_name ? logical_db_name : "NULL",
                ddl_sql)));

    /* 执行 DDL */
    result = bbf_apply_logical_ddl_message(&input);

    if (!result.success)
    {
        ereport(ERROR,
            (errcode(result.error_code),
             errmsg("DDL apply failed: %s, sql: %s",
                    result.error_message, ddl_sql)));
    }

    ereport(LOG,
        (errmsg("BBF DDL applied successfully: %s", ddl_sql)));
}
```

---

## 五、方案 C 详细实现：模拟 TDS 连接状态

### 5.1 核心修改：新增判断宏

```c
/*
 * pltsql.h - 新增判断宏
 *
 * 在现有 IS_TDS_CONN() 基础上新增更通用的判断
 */

/* 原有定义 */
#define IS_TDS_CONN() (MyProcPort && MyProcPort->is_tds_conn)

/***** 新增定义 *****/

/*
 * IS_BABELFISH_CONNECTION - 判断是否为 Babelfish 连接
 *
 * 包括 TDS 连接和 BBF apply worker
 */
#define IS_BABELFISH_CONNECTION() \
    (IS_TDS_CONN() || IsBabelfishApplyWorker())

/*
 * IS_TDS_SEMANTIC_CONTEXT - 判断是否在 T-SQL 语义上下文中
 *
 * 用于替代 IS_TDS_CONN() 的更通用版本
 */
#define IS_TSQL_SEMANTIC_CONTEXT() \
    (sql_dialect == SQL_DIALECT_TSQL || \
     (IsBabelfishApplyWorker() && bbf_in_apply_mode()))

/*
 * IsBabelfishApplyWorker - 判断是否为 Babelfish Apply Worker
 */
extern bool IsBabelfishApplyWorker(void);

/*
 * bbf_in_apply_mode - 判断是否处于 BBF apply 模式
 */
extern bool bbf_in_apply_mode(void);
```

### 5.2 Apply Worker 模式检测

```c
/*
 * worker_bbf.c - 新增 Apply Worker 模式检测
 */

/* 全局变量 */
static bool bbf_apply_mode = false;

/*
 * IsBabelfishApplyWorker - 判断是否为 BBF Apply Worker
 *
 * 通过检查 backend type 和 sql_dialect 来判断
 */
bool
IsBabelfishApplyWorker(void)
{
    /*
     * MyBackendType 是 PostgreSQL 内部变量
     * Apply Worker 的类型为 B_APPLY_WORKER
     */
    return (MyBackendType == B_APPLY_WORKER &&
            sql_dialect == SQL_DIALECT_TSQL);
}

/*
 * bbf_in_apply_mode - 判断是否处于 BBF apply 模式
 */
bool
bbf_in_apply_mode(void)
{
    return bbf_apply_mode;
}

/*
 * bbf_set_apply_mode - 设置 BBF apply 模式
 */
void
bbf_set_apply_mode(bool enable)
{
    bbf_apply_mode = enable;
}
```

### 5.3 修改 hooks.c 中的判断点

```c
/*
 * hooks.c - 修改 IS_TDS_CONN() 调用点
 *
 * 将 if (sql_dialect == SQL_DIALECT_TSQL && IS_TDS_CONN())
 * 修改为 if (sql_dialect == SQL_DIALECT_TSQL && IS_TSQL_SEMANTIC_CONTEXT())
 */

/* 修改前 */
if (sql_dialect != SQL_DIALECT_TSQL || !IS_TDS_CONN())
{
    /* 只有 TDS 连接才执行 */
}

/* 修改后 */
if (sql_dialect != SQL_DIALECT_TSQL || !IS_TSQL_SEMANTIC_CONTEXT())
{
    /* TDS 连接或 BBF Apply Worker 都执行 */
}
```

---

## 六、错误处理与恢复机制

### 6.1 错误处理策略

```c
/*
 * 错误处理的三层策略
 */

/* 1. BBF 上下文初始化失败 - 警告但继续 */
if (!InitializeBBFContext())
{
    ereport(WARNING,
        (errmsg("BBF context initialization failed, TSQL DDL will not be applied")));
    /* 继续运行，但不执行 TSQL DDL */
}

/* 2. 单条 DDL 执行失败 - 记录并跳过 */
PG_TRY();
{
    bbf_apply_logical_ddl_message(&input);
}
PG_CATCH();
{
    /* 记录错误 */
    ereport(ERROR,
        (errcode(ERRCODE_INTERNAL_ERROR),
         errmsg("DDL apply failed: %s, skipping", input.ddl_sql)));
    /* 可选：写入错误表供后续诊断 */
    record_ddl_failure(&input, &edata);
}
PG_END_TRY();

/* 3. 严重错误 - 回退整个事务 */
PG_TRY();
{
    /* 执行 DDL */
}
PG_CATCH();
{
    /* 回退事务 */
    HatalmqRollback();
    /* 重新抛出错误 */
    PG_RE_THROW();
}
PG_END_TRY();
```

### 6.2 上下文恢复的 PG_TRY 模式

```c
/*
 * 安全的上下文切换模式
 *
 * 使用 PG_TRY/PG_FINALLY 确保上下文始终被恢复
 */
void
safe_bbf_context_switch(BbfLogicalDdlApplyInput *input)
{
    /* 保存当前上下文 */
    SavedBBFContext saved;
    saved.saved_sql_dialect = sql_dialect;
    saved.saved_search_path = pstrdup(GetConfigOption("search_path", false, false));
    saved.saved_database_name = pstrdup(GetConfigOption("babelfishpg_tsql.database_name", false, false));

    PG_TRY();
    {
        /* 设置新上下文 */
        sql_dialect = bbf_resolve_apply_sql_dialect(input->db_mode);
        set_config_option("search_path", input->search_path, ...);

        /* 执行 DDL */
        exec_ddl(input->ddl_sql);
    }
    PG_FINALLY();
    {
        /* 始终恢复上下文 */
        sql_dialect = saved.saved_sql_dialect;
        set_config_option("search_path", saved.saved_search_path, ...);

        /* 清理 */
        pfree(saved.saved_search_path);
        pfree(saved.saved_database_name);
    }
    PG_END_TRY();
}
```

---

## 七、集成点汇总

### 7.1 需要修改的 PostgreSQL 文件

| 文件 | 修改内容 |
|------|---------|
| `src/backend/replication/logical/worker.c` | 调用 `InitializeBBFContext()` |
| `src/backend/replication/logical/meson.build` | 添加条件编译配置 |

### 7.2 需要新增的文件

| 文件 | 内容 |
|------|------|
| `src/backend/replication/logical/worker_bbf.h` | BBF 上下文初始化接口 |
| `src/backend/replication/logical/worker_bbf.c` | BBF 上下文初始化实现 |
| `src/backend/replication/logical/bbf_ddl_replicate.h` | DDL 复制消息接口 |
| `src/backend/replication/logical/bbf_ddl_replicate.c` | DDL 复制核心实现 |

### 7.3 需要修改的 Babelfish 文件

| 文件 | 修改内容 |
|------|---------|
| `contrib/babelfishpg_tsql/src/pltsql.h` | 新增 `IS_TSQL_SEMANTIC_CONTEXT()` 宏 |
| `contrib/babelfishpg_tsql/src/hooks.h` | 新增 `bbf_in_tsql_semantic_context()` 声明 |

---

## 八、与现有实现的对比

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

apply Worker + BBF (方案A):
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

apply Worker + BBF (方案B - 消息驱动):
1-5. 同上
6. bbf_enter_logical_ddl_apply_mode()  ← 每条消息执行时
7. bbf_restore_apply_database_context(...)
8. bbf_build_apply_parsetree(...)
9. 执行 DDL
10. bbf_leave_logical_ddl_apply_mode()
```

---

## 九、风险评估与缓解

| 风险 | 影响 | 缓解措施 |
|------|------|---------|
| `sql_dialect` 设置失败 | TSQL DDL 无法解析 | 添加验证逻辑，失败时记录警告 |
| `IS_TDS_CONN()` 返回 false | 部分 BBF 代码路径不执行 | 新增 `IS_TSQL_SEMANTIC_CONTEXT()` 抽象 |
| 上下文恢复失败 | 污染后续 DDL 执行 | 使用 PG_TRY/PG_FINALLY 确保恢复 |
| BBF 未加载 | 代码崩溃 | 使用 `#ifdef EXTERNAL_BABELFISH` 条件编译 |
| 内存泄漏 | 长期运行内存增长 | 在 `saved_bbf_context` 使用后及时 pfree |

---

## 十、推荐实现路径

**短期**：采用方案 A（手动设置 BBF GUC）
- 在 `ApplyWorkerMain()` 中添加 `InitializeBBFContext()` 调用
- 使用 `#ifdef EXTERNAL_BABELFISH` 条件编译
- 改动小，风险低，可快速验证

**长期**：采用方案 B（消息驱动上下文恢复）
- 在 DDL 消息中携带完整上下文信息
- 实现 `bbf_in_tsql_semantic_context()` 抽象
- 支持 PG/TSQL 混合模式
- 最符合 Babelfish 设计意图

---

## 附录：相关代码位置

| 文件 | 说明 |
|------|------|
| `src/backend/replication/logical/worker.c` | Apply Worker 主文件 |
| `src/backend/replication/logical/slotsync.c` | slotsync Worker 参考 |
| `src/backend/postmaster/bgworker.c` | Background Worker 初始化 |
| `src/include/postmaster/bgworker.h` | Background Worker 接口定义 |
| `contrib/babelfishpg_tsql/src/pltsql.h` | BBF 主头文件 |
| `contrib/babelfishpg_tsql/src/hooks.c` | BBF hooks 实现 |
| `contrib/babelfishpg_tsql/src/bbf_parallel_query.c` | Parallel Worker BBF 上下文参考 |

---

## 附录 B：Babelfish Schema 映射机制详解

### B.1 核心概念

Babelfish 通过**逻辑 schema 名**和**物理 schema 名**的双向映射，实现 SQL Server 的 `dbo` 到 PostgreSQL 的 `public` 的转换。

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     Babelfish Schema 映射架构                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  [客户端]  CREATE TABLE dbo.table1                                         │
│                │                                                             │
│                ▼                                                             │
│  [Logical Schema Name]     "dbo" (SQL Server 语法)                          │
│                │                                                             │
│                ▼                                                             │
│  [Schema Mapping Layer]  get_physical_schema_name(db, "dbo")               │
│                │                                                             │
│                ▼                                                             │
│  [Physical Schema Name]   "mydb_dbo" (多数据库模式) 或 "dbo" (单数据库模式)   │
│                │                                                             │
│                ▼                                                             │
│  [PostgreSQL]            CREATE TABLE mydb_dbo.table1                      │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### B.2 关键函数

#### B.2.1 get_physical_schema_name()

将逻辑 schema 名转换为物理 schema 名。

```c
// contrib/babelfishpg_tsql/src/multidb.c

/*
 * get_physical_schema_name - 获取物理 schema 名
 *
 * 参数:
 *   - db_name: 数据库名 (如 "mydb")
 *   - schema_name: 逻辑 schema 名 (如 "dbo", "guest")
 *
 * 返回: 物理 schema 名
 *
 * 映射规则:
 *   - 共享 schema (sys, pg_catalog, information_schema_tsql, public): 直接返回
 *   - SINGLE_DB 模式: 直接返回 schema_name
 *   - 多数据库模式: 返回 "dbname_schema" 格式 (如 "mydb_dbo")
 */
char *
get_physical_schema_name(char *db_name, const char *schema_name)
{
    return get_physical_schema_name_by_mode(db_name, schema_name, get MigrationMode());
}

/*
 * get_physical_schema_name_by_mode - 根据迁移模式获取物理 schema 名
 */
char *
get_physical_schema_name_by_mode(char *db_name, const char *schema_name, MigrationMode mode)
{
    static char result[MAX_BBF_NAMEDATALEND];
    const char *name;

    /* 1. 检查是否为共享 schema */
    if (is_shared_schema(schema_name))
        return (char *) schema_name;

    /* 2. 根据迁移模式处理 */
    if (SINGLE_DB == mode)
    {
        /* 单数据库模式: 不添加前缀 */
        return (char *) schema_name;
    }
    else
    {
        /* 多数据库模式: 添加 db_ 前缀 */
        snprintf(result, MAX_BBF_NAMEDATALEND, "%s_%s", db_name, schema_name);
        return result;
    }
}
```

#### B.2.2 get_logical_schema_name()

将物理 schema 名反向转换为逻辑 schema 名。

```c
// contrib/babelfishpg_tsql/src/catalog.c

/*
 * get_logical_schema_name - 获取逻辑 schema 名
 *
 * 通过查询 pg_namespace 表，找到与物理 schema 名对应的逻辑名
 *
 * 参数:
 *   - physical_schema_name: 物理 schema 名 (如 "mydb_dbo")
 *   - missingOk: 未找到时是否返回 NULL
 *
 * 返回: 逻辑 schema 名 (如 "dbo")，未找到时根据 missingOk 决定行为
 */
const char *
get_logical_schema_name(const char *physical_schema_name, bool missingOk)
{
    const char *logical_name;
    HeapTuple tuple;
    Relation nsrel;
    ScanKeyData skey[1];
    SysScanDesc ssd;

    /* 打开 pg_namespace 系统表 */
    nsrel = table_open(NamespaceRelationId, AccessShareLock);

    /* 构建扫描键: nspname = physical_schema_name */
    ScanKeyInit(&skey[0],
                Anum_pg_namespace_nspname,
                BTEqualStrategyNumber, F_NAMEEQ,
                CStringGetDatum(physical_schema_name));

    /* 执行扫描 */
    ssd = systable_beginscan(nsrel, NamespaceNameIndexId, true,
                              NULL, 1, skey);

    tuple = systable_getnext(ssd);

    if (!HeapTupleIsValid(tuple))
    {
        systable_endscan(ssd);
        table_close(nsrel, AccessShareLock);

        if (missingOk)
            return NULL;
        else
            ereport(ERROR, ...);
    }

    /* 获取 logical_schema_name 属性 */
    logical_name = pstrdup(heap_getattr(tuple, Anum_pg_namespace_nspname,
                                        RelationGetDescr(nsrel), NULL));

    systable_endscan(ssd);
    table_close(nsrel, AccessShareLock);

    return logical_name;
}
```

#### B.2.3 is_shared_schema()

判断是否为共享 schema。

```c
// contrib/babelfishpg_tsql/src/multidb.c

/*
 * is_shared_schema - 检查是否为共享 schema
 *
 * 共享 schema 包括:
 *   - sys (系统表和对象)
 *   - pg_catalog (PostgreSQL 系统)
 *   - information_schema (SQL 标准信息模式)
 *   - information_schema_tsql (TSQL 信息模式)
 *   - public (PostgreSQL 默认 schema)
 *   - guest (SQL Server 可选 guest 访问)
 */
bool
is_shared_schema(const char *name)
{
    if (name == NULL)
        return false;

    /* 使用静态共享 schema 列表进行匹配 */
    static const char *shared_schemas[] = {
        "sys",
        "pg_catalog",
        "information_schema",
        "information_schema_tsql",
        "public",
        "guest",
        NULL
    };

    for (int i = 0; shared_schemas[i] != NULL; i++)
    {
        if (strcmp(name, shared_schemas[i]) == 0)
            return true;
    }

    return false;
}
```

### B.3 迁移模式与 Schema 映射

#### B.3.1 SINGLE_DB 模式

```sql
-- 数据库: mydb
-- 迁移模式: SINGLE_DB

-- 客户端 SQL
CREATE TABLE dbo.table1 (id INT);

-- 物理存储
-- schema: dbo (未添加前缀)
-- 完整表名: mydb.dbo.table1
```

#### B.3.2 MULTI_DB 模式

```sql
-- 数据库: mydb
-- 迁移模式: MULTI_DB

-- 客户端 SQL
CREATE TABLE dbo.table1 (id INT);

-- 物理存储
-- schema: mydb_dbo (添加了 db_ 前缀)
-- 完整表名: mydb.mydb_dbo.table1
```

#### B.3.3 模式对比表

| SQL Server Schema | SINGLE_DB 物理名 | MULTI_DB 物理名 | 共享 |
|------------------|-----------------|----------------|------|
| `dbo` | `dbo` | `mydb_dbo` | 否 |
| `guest` | `guest` | `mydb_guest` | 否 |
| `sys` | `sys` | `sys` | 是 |
| `INFORMATION_SCHEMA` | `information_schema` | `information_schema` | 是 |
| `pg_catalog` | `pg_catalog` | `pg_catalog` | 是 |
| `public` | `public` | `public` | 是 |

### B.4 Schema 映射在 Apply Worker 中的应用

#### B.4.1 问题场景

当 Apply Worker 收到如下 DDL 时：

```sql
CREATE TABLE dbo.orders (id INT, name VARCHAR(50));
```

需要正确映射到物理 schema 名。

#### B.4.2 应用策略

```c
/*
 * bbf_resolve_target_schema - 解析目标物理 schema
 *
 * 参数:
 *   - logical_schema: 逻辑 schema 名 (如 "dbo")
 *   - db_name: 数据库名
 *
 * 返回: 物理 schema 名
 */
const char *
bbf_resolve_target_schema(const char *logical_schema, const char *db_name)
{
    MigrationMode mode;

#ifdef EXTERNAL_BABELFISH
    /* 获取当前迁移模式 */
    mode = get_migration_mode();

    /* 使用 Babelfish 的映射函数 */
    return get_physical_schema_name_by_mode(db_name, logical_schema, mode);
#else
    /* 非 BBF 环境: 直接返回原 schema 名 */
    return logical_schema;
#endif
}

/*
 * bbf_extract_and_resolve_schema - 从 DDL 中提取 schema 并解析
 *
 * 用于 DDL 中未显式指定 schema 的情况
 * 根据 search_path 中的第一个 schema 作为默认 schema
 */
const char *
bbf_extract_and_resolve_schema(const char *ddl_sql, const char *db_name)
{
    const char *schema = NULL;
    char *resolved_schema;
    const char *search_path;
    const char *first_schema;

    /* 1. 尝试从 DDL 中提取 schema */
    schema = extract_schema_from_ddl(ddl_sql);
    if (schema != NULL)
    {
        /* DDL 中显式指定了 schema */
        return bbf_resolve_target_schema(schema, db_name);
    }

    /* 2. 从 search_path 获取第一个 schema */
    search_path = GetConfigOption("search_path", false, false);
    first_schema = extract_first_schema_from_search_path(search_path);

    if (first_schema == NULL)
        first_schema = "dbo";  /* 默认值 */

    /* 3. 解析为物理 schema 名 */
    resolved_schema = (char *) bbf_resolve_target_schema(first_schema, db_name);

    return resolved_schema;
}
```

#### B.4.3 非 dbo 默认 schema 处理

当默认 schema 不是 `dbo` 时，需要动态调整 search_path：

```c
/*
 * bbf_adjust_search_path_for_default_schema - 调整 search_path 以支持非 dbo 默认 schema
 *
 * 在某些 BBF 配置中，默认 schema 可能不是 dbo
 * 需要将默认 schema 放在 search_path 首位
 */
void
bbf_adjust_search_path_for_default_schema(const char *default_schema)
{
    const char *current_path;
    char *new_path;
    const char *dbname;

    /* 获取当前 search_path */
    current_path = GetConfigOption("search_path", false, false);

    /* 如果 default_schema 已在 search_path 首位，无需调整 */
    if (starts_with(current_path, default_schema))
        return;

    /* 构建新的 search_path: default_schema 在前，后续是 dbo, public, pg_catalog */
    dbname = get_database_name(MyLogicalRepWorker->dbid);

    if (strcmp(default_schema, "dbo") != 0)
    {
        /* 默认 schema 不是 dbo，需要调整顺序 */
        new_path = psprintf("%s, dbo, public, pg_catalog", default_schema);
    }
    else
    {
        /* 默认 schema 是 dbo，使用标准 BBF search_path */
        new_path = psprintf("dbo, public, pg_catalog");
    }

    set_config_option("search_path", new_path,
                      PGC_USERSET, GUC_CONTEXT_SESSION);

    pfree(new_path);

    ereport(DEBUG1,
            (errmsg("Adjusted search_path for default schema: %s",
                    default_schema)));
}
```

### B.5 Schema 映射与 DDL 执行

#### B.5.1 CREATE TABLE 场景

```sql
-- 输入 DDL (客户端)
CREATE TABLE dbo.orders (id INT);

-- Apply Worker 处理流程:
-- 1. 解析 SQL，获取目标 schema: "dbo"
-- 2. 调用 get_physical_schema_name("mydb", "dbo")
-- 3. 获取物理 schema: "mydb_dbo" (MULTI_DB 模式)
-- 4. 执行: CREATE TABLE mydb_dbo.orders (id INT)
```

#### B.5.2 CREATE SCHEMA 场景

```sql
-- 输入 DDL (客户端)
CREATE SCHEMA myschema;

-- Apply Worker 处理流程:
-- 1. 解析 SQL，获取目标 schema: "myschema"
-- 2. 调用 get_physical_schema_name("mydb", "myschema")
-- 3. 获取物理 schema: "mydb_myschema" (MULTI_DB 模式)
-- 4. 执行: CREATE SCHEMA mydb_myschema

-- 注意: 需要先检查物理 schema 是否已存在
-- 如果 mydb_myschema 已存在，可能需要映射到现有物理 schema
```

#### B.5.3 未指定 schema 的 DDL

```sql
-- 输入 DDL (客户端)
CREATE TABLE users (id INT);

-- Apply Worker 处理流程:
-- 1. 解析 SQL，未指定 schema
-- 2. 从 search_path 获取第一个 schema: "dbo"
-- 3. 调用 get_physical_schema_name("mydb", "dbo")
-- 4. 获取物理 schema: "mydb_dbo"
-- 5. 执行: CREATE TABLE mydb_dbo.users (id INT)
```

### B.6 验证 Schema 映射正确性

```c
/*
 * bbf_verify_schema_mapping - 验证 schema 映射是否正确
 *
 * 用于调试和诊断
 */
void
bbf_verify_schema_mapping(const char *logical_schema,
                          const char *expected_physical)
{
    const char *actual_physical;
    const char *dbname;
    MigrationMode mode;

    dbname = GetConfigOption("babelfishpg_tsql.database_name", false, false);
    mode = get_migration_mode();

    actual_physical = get_physical_schema_name_by_mode(dbname, logical_schema, mode);

    if (strcmp(actual_physical, expected_physical) == 0)
    {
        ereport(DEBUG1,
                (errmsg("Schema mapping OK: %s -> %s (mode=%d)",
                        logical_schema, actual_physical, mode)));
    }
    else
    {
        ereport(WARNING,
                (errmsg("Schema mapping mismatch: %s -> %s (expected %s, mode=%d)",
                        logical_schema, actual_physical, expected_physical, mode)));
    }
}
```

### B.7 小结

| 功能 | 函数 | 文件 |
|------|------|------|
| 逻辑→物理 | `get_physical_schema_name()` | multidb.c |
| 物理→逻辑 | `get_logical_schema_name()` | catalog.c |
| 共享 schema 判断 | `is_shared_schema()` | multidb.c |
| 迁移模式获取 | `get_migration_mode()` | multidb.c |

**Apply Worker 中使用 schema 映射的关键点**：

1. **MULTI_DB 模式**：所有非共享 schema 都需要添加 `db_` 前缀
2. **SINGLE_DB 模式**：schema 名保持原样
3. **共享 schema**：始终不添加前缀（sys, pg_catalog, information_schema, public）
4. **search_path**：应包含 `dbo` 以支持默认 schema 映射