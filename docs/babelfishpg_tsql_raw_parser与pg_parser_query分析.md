# babelfishpg_tsql_raw_parser 与 pg_parser_query 对比分析

## 一、函数概述

### 1.1 pg_parser_query (PostgreSQL 标准解析器)

```c
// src/backend/tcop/postgres.c
List *
pg_parse_query(const char *query_string)
{
    List *raw_parsetree_list;
    raw_parsetree_list = raw_parser(query_string, RAW_PARSE_DEFAULT);
    // ...
    return raw_parsetree_list;
}
```

```c
// src/include/parser/parser.h
typedef enum
{
    RAW_PARSE_DEFAULT = 0,        // 标准SQL解析
    RAW_PARSE_TYPE_NAME,          // 类型名解析
    RAW_PARSE_PLPGSQL_EXPR,       // PL/pgSQL表达式
    RAW_PARSE_PLPGSQL_ASSIGN1,     // PL/pgSQL赋值
    RAW_PARSE_PLPGSQL_ASSIGN2,
    RAW_PARSE_PLPGSQL_ASSIGN3,
} RawParseMode;

// 主入口
extern List *raw_parser(const char *str, RawParseMode mode);
```

### 1.2 babelfishpg_tsql_raw_parser (Babelfish TSQL解析器)

**注意**: `babelfishpg_tsql_raw_parser` 是 **Babelfish 扩展** 提供的函数，不在 PostgreSQL 核心代码中。其实际实现位于 Babelfish 扩展中。

```c
// 预期签名 (来自Babelfish扩展)
List *
babelfishpg_tsql_raw_parser(const char *str, RawParseMode mode);
```

---

## 二、核心区别对比

| 维度 | pg_parse_query / raw_parser | babelfishpg_tsql_raw_parser |
|------|----------------------------|----------------------------|
| **所属模块** | PostgreSQL 核心 | Babelfish 扩展 |
| **语法支持** | 标准 PostgreSQL SQL | T-SQL (SQL Server 语法) |
| **入口文件** | `src/backend/parser/parser.c` | Babelfish 扩展 |
| **调用方式** | 直接调用 | 通过扩展 hooks 间接调用 |
| **依赖** | 无 | 需要 Babelfish 扩展已加载 |
| **可用性** | 始终可用 | 仅在启用 Babelfish 时可用 |
| **GUC控制** | 无 | `babelfishpg_tsql.sql_dialect = 'tsql'` |

---

## 三、调用路径分析

### 3.1 pg_parse_query 完整调用链

```
exec_simple_query()
    │
    ▼
pg_parse_query(query_string)          // src/backend/tcop/postgres.c:604
    │
    ▼
raw_parser(query_string, RAW_PARSE_DEFAULT)  // src/backend/parser/parser.c:42
    │
    ├── scanner_init()                  // 初始化flex词法分析器
    ├── parser_init()                   // 初始化bison语法分析器
    └── base_yyparse()                  // 执行解析
    │
    ▼
返回 List<RawStmt> // 原始语法树
```

### 3.2 babelfishpg_tsql_raw_parser 调用链

```
exec_sql_via_portal()
    │
    ├── sql_dialect = SQL_DIALECT_TSQL  // 设置方言GUC
    │
    ▼
babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT)
    │
    ├── TSQLLexerInit()                // 初始化TSQL词法
    ├── TSQLParserInit()               // 初始化TSQL语法
    └── TSQL_yyparse() // TSQL解析
    │
    ▼
返回 List<RawStmt>                     // TSQL语法的原始语法树
```

---

## 四、Apply Worker 调用可行性分析

### 4.1 直接调用 babelfishpg_tsql_raw_parser 的条件

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Apply Worker 直接调用 babelfishpg_tsql_raw_parser 的前提条件            │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Babelfish 扩展已加载                                                  │
│     - shared_preload_libraries 包含 babelfishpg_tsql                     │
│     - 扩展的 DLL/SO 已加载到 PostgreSQL 进程                              │
│                                                                           │
│  2. BBF Hooks 已正确初始化                                               │
│     - gram_hook 已设置                                                    │
│     - ProcessUtility_hook 已设置 │
│     - sql_dialect 已设置为 SQL_DIALECT_TSQL                               │
│                                                                           │
│  3. Session上下文正确 │
│     - search_path 已设置                                                  │
│     - 当前数据库正确 │
│     - 用户权限正确 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 可行性分析结论

| 调用方式 | 可行性 | 说明 |
|----------|--------|------|
| **直接调用 babelfishpg_tsql_raw_parser** | ⚠️ 有条件可行 | 仅当 Babelfish 扩展已加载时可用 |
| **通过 PG扩展 hooks 间接调用** | ✅ 推荐 | 通过 sql_dialect 自动路由到 BBF 解析器 |
| **fallback 到 pg_parse_query** | ✅ 始终可用 | 当 BBF 不可用时的保底方案 |

### 4.3 推荐实现模式

```c
static List *
exec_parse_sql(const char *sql_text)
{
    List *parsetree_list;

    /* 方式1: 如果启用了TSQL方言，使用BBF解析器 */
    if (sql_dialect == SQL_DIALECT_TSQL)
    {
#ifdef EXTERNAL_BABELFISH
        parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
#else
        /* BBF未编译入，fallback到标准解析器（但可能无法正确解析TSQL语法） */
        parsetree_list = pg_parse_query(sql_text);
#endif
    }
    /* 方式2: 标准PostgreSQL方言 */
    else
    {
        parsetree_list = pg_parse_query(sql_text);
    }

    return parsetree_list;
}
```

---

## 五、关键差异：语法支持范围

### 5.1 pg_parse_query (PostgreSQL 语法)

支持的 DDL：
- `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`
- `CREATE INDEX`, `DROP INDEX`, `REINDEX`
- `CREATE SCHEMA`, `DROP SCHEMA`
- `CREATE VIEW`, `DROP VIEW`
- `CREATE SEQUENCE`, `DROP SEQUENCE`
- `CREATE TYPE`, `DROP TYPE`
- 等等...

### 5.2 babelfishpg_tsql_raw_parser (TSQL 语法)

除支持上述外，还支持：
- `CREATE TABLE` (TSQL 扩展语法，如 `INT IDENTITY(1,1)`)
- `CREATE TRIGGER` (TSQL 语法)
- `CREATE PROCEDURE` (TSQL 存储过程语法)
- `CREATE FUNCTION` (TSQL 函数语法)
- `sp_xxx` 存储过程语法
- `SELECT INTO` 语法
- `DELETE FROM table WHERE`简写
- TSQL特有的数据类型 (`DATETIME`, `NVARCHAR(max)` 等)

### 5.3 兼容性矩阵

| DDL 类型 | pg_parse_query | babelfishpg_tsql_raw_parser |
|----------|----------------|---------------------------|
| `CREATE TABLE t (id INT)` | ✅ 支持 | ✅ 支持 |
| `CREATE TABLE t (id INT IDENTITY(1,1))` | ❌语法错误 | ✅ 支持 |
| `CREATE TRIGGER tr ON t` | ❌ 语法错误 | ✅ 支持 |
| `CREATE PROC p AS SELECT` | ❌ 语法错误 | ✅ 支持 |
| `SELECT INTO FROM` | ❌ 语法错误 | ✅ 支持 |
| `DROP TABLE IF EXISTS` | ✅ 支持 | ✅ 支持 |

---

## 六、GUC 和 Hooks 依赖

### 6.1 sql_dialect 设置

```c
/* 设置sql_dialect为TSQL以启用BBF特定功能 */
sql_dialect = SQL_DIALECT_TSQL;

/* 或者通过GUC */
set_config_option(
    "babelfishpg_tsql.sql_dialect",
    "tsql",
    GUC_CONTEXT_SESSION,
    true
);
```

### 6.2必需的 Hooks

| Hook 名称 | 作用 | 必需性 |
|-----------|------|--------|
| **gram_hook** | 拦截语法分析，注入 TSQL 规则 | 是 |
| **ProcessUtility_hook** | 拦截 DDL 执行，执行 BBF 特定处理 | 是 |
| **object_access_hook** | 对象访问时的 BBF 特定处理 | 可选 |
| **ExecutorStart_hook** | 执行器启动时的 BBF 初始化 | 可选 |

### 6.3 Apply Worker 中的初始化

Apply Worker 是一个 Background Worker，它在初始化时需要：

```c
void
ApplyWorkerMain(Datum main_arg)
{
    /* 1. 初始化数据库连接 */
    BackgroundWorkerInitializeConnection(dbname, username, 0);

    /* 2. 设置TSQL方言 (如果BBF已加载) */
    if (process_tsql_dialect)
        sql_dialect = SQL_DIALECT_TSQL;

    /* 3. 设置search_path等GUC */
    // ...

    /* 4. 进入主循环 */
    for (;;)
    {
        // 处理复制消息
    }
}
```

---

## 七、风险评估

### 7.1 当 Babelfish 未加载时的风险

| 风险 | 影响 | 缓解措施 |
|------|------|----------|
| 调用未定义的 `babelfishpg_tsql_raw_parser` | **崩溃** | 使用 `#ifdef EXTERNAL_BABELFISH` 条件编译 |
| TSQL 语法被 PostgreSQL 解析器拒绝 | **解析失败** | fallback 到 `pg_parse_query`，但可能丢失语义 |
| 事务回滚但部分DDL已执行 | **数据不一致** | 使用 `PG_TRY/PG_CATCH` 保护 |

### 7.2 代码示例：安全调用模式

```c
static List *
safe_parse_sql(const char *sql_text, bool is_tsql_dialect)
{
    List *parsetree_list = NIL;

    /* 使用独立的内存上下文 */
    MemoryContext oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    PG_TRY();
    {
        if (is_tsql_dialect)
        {
#ifdef EXTERNAL_BABELFISH
            /* 使用BBF TSQL解析器 */
            parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
#else
            /* BBF未编译入，使用标准解析器（可能解析失败） */
            ereport(WARNING,
                errmsg("BBF not compiled in, using standard parser"));
            parsetree_list = pg_parse_query(sql_text);
#endif
        }
        else
        {
            /* 标准PostgreSQL解析器 */
            parsetree_list = pg_parse_query(sql_text);
        }

        if (parsetree_list == NIL)
            ereport(ERROR,
                (errcode(ERRCODE_SYNTAX_ERROR),
                 errmsg("parse failed for: %s", sql_text)));
    }
    PG_CATCH();
    {
        FlushErrorState();
        parsetree_list = NIL;
    }
    PG_END_TRY();

    MemoryContextSwitchTo(oldcontext);
    return parsetree_list;
}
```

---

## 八、结论

### 8.1 Apply Worker 是否可以直接调用 babelfishpg_tsql_raw_parser？

**答案：可以，但需要满足以下条件：**

1. **编译条件**: 代码需要使用 `#ifdef EXTERNAL_BABELFISH` 条件编译
2. **运行时条件**: 必须检测 Babelfish 扩展是否已加载
3. **初始化条件**: `sql_dialect` 必须设置为 `SQL_DIALECT_TSQL`
4. **Fallback条件**: 需要准备 fallback 到 `pg_parse_query` 的备选方案

### 8.2 推荐方案

```c
/*
 * exec_sql_via_portal 中的解析选择逻辑
 */
static void
exec_sql_via_portal(const char *sql_text)
{
    List *parsetree_list;

    /* 关键：根据sql_dialect选择解析器 */
    if (sql_dialect == SQL_DIALECT_TSQL && process_tsql_dialect)
        parsetree_list = babelfishpg_tsql_raw_parser(sql_text, RAW_PARSE_DEFAULT);
    else
        parsetree_list = pg_parse_query(sql_text);

    // ... 后续分析重写、执行流程
}
```

### 8.3 关键文件

| 文件 |作用 |
|------|------|
| `src/backend/parser/parser.c` | `raw_parser()` 实现，PG 标准解析器 |
| `src/backend/tcop/postgres.c` | `pg_parse_query()` 入口 |
| `src/include/parser/parser.h` | `RawParseMode` 枚举定义 |
| `src/include/tcop/tcopprot.h` | `pg_analyze_and_rewrite_fixedparams`声明 |
| Babelfish扩展 | `babelfishpg_tsql_raw_parser()` 实现 |

---

## 附录：raw_parser 源码关键逻辑

```c
// src/backend/parser/parser.c:42
List *
raw_parser(const char *str, RawParseMode mode)
{
    core_yyscan_t yyscanner;
    base_yy_extra_type yyextra;
    int yyresult;

    /* 初始化flex扫描器 */
    yyscanner = scanner_init(str, &yyextra.core_yy_extra,
                             &ScanKeywords, ScanKeywordTokens);

    /* 根据RawParseMode设置lookahead */
    if (mode == RAW_PARSE_DEFAULT)
        yyextra.have_lookahead = false;
    else
    {
        static const int mode_token[] = {
            [RAW_PARSE_DEFAULT] = 0,
            [RAW_PARSE_TYPE_NAME] = MODE_TYPE_NAME,
            [RAW_PARSE_PLPGSQL_EXPR] = MODE_PLPGSQL_EXPR,
            // ...
        };
        yyextra.have_lookahead = true;
        yyextra.lookahead_token = mode_token[mode];
        // ...
    }

    /* 初始化bison解析器 */
    parser_init(&yyextra);

    /* 执行解析 */
    yyresult = base_yyparse(yyscanner);

    /* 清理 */
    scanner_finish(yyscanner);

    if (yyresult)  /* 解析失败 */
        return NIL;

    return yyextra.parsetree;  /* 返回语法树列表 */
}
```