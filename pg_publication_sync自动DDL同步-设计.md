# 设计3：利用逻辑复制同步 `pg_publication_sync`，并 apply DDL SQL 实现自动 DDL 同步

更新时间：2026-04-17

对应测试文档：  
[pg_publication_sync自动DDL同步-测试验证步骤.md](/Users/growduduan/cwork/postgresql-2/pg_publication_sync自动DDL同步-测试验证步骤.md)

## 1. 目标

基于 `pg_publication_sync` 实现自动 DDL 同步闭环：

1. 发布端统一捕获 DDL 并写入对象消息行。
2. 订阅端接收后按消息类型执行。
3. 支持 `Q/A/D` 三类消息，覆盖 DDL 执行与 publication 成员变更。

## 2. 发布端 DDL 捕获

主入口：`src/backend/tcop/utility.c` 的 `ProcessUtility` 路径。

核心函数：`CapturePublicationSyncDDL(PlannedStmt *pstmt, const char *queryString)`。

捕获内容：

1. `message_type='Q'`
2. `ddl_str`：原始 SQL 文本（按语句位置截取）
3. `search_path`：发布端执行上下文
4. `target_table`：目标对象名（可为空）
5. `publication_list`：命中的 publication 名单
6. `pfsyncddl`：语句对应 DDL mask

## 3. 发布端过滤规则

一条 DDL 只有在同时命中以下条件时才入表：

1. publication 已开启 `WITH (ddl=...)`。
2. 当前语句 `ddlmask` 被 publication 的 `pubddl` 包含。
3. 对象范围命中 publication（`FOR TABLE` / `FOR TABLES IN SCHEMA` / `FOR ALL TABLES`）。
4. 作用域类型限制命中：
   * `FOR TABLE`：仅允许 `table/index`
   * `FOR TABLES IN SCHEMA`、`FOR ALL TABLES`：允许  
     `table/index/trigger/view/rule/schema/function/type/domain/extension`

## 4. publication 成员变更消息（A/D）

入口：`src/backend/commands/publicationcmds.c`。

函数：`insert_publication_sync_relation_message(pubid, relid, message_type)`。

触发场景：

1. `ALTER PUBLICATION ... ADD TABLE` -> `message_type='A'`
2. `ALTER PUBLICATION ... DROP TABLE` -> `message_type='D'`
3. `DROP PUBLICATION` -> 对关联关系发 `D`

消息载荷：

1. `target_table`：schema-qualified 表名
2. `publication_list`：当前 publication 名
3. `pfsyncddl`：固定 `PUBDDL_TABLE`

## 5. 订阅端 apply 分发

入口：`src/backend/replication/logical/worker.c`。

函数：`maybe_apply_publication_sync_message(...)`。

执行前置条件：

1. 仅 leader apply worker 执行。
2. 本地关系必须是 `pg_publication_sync`。
3. 仅处理 `pfsynckind='o'` 对象消息行。
4. 行需通过 `publication_sync_row_matches_subscription()` 过滤：
   * `subddl & pfsyncddl != 0`
   * `publication_list` 与 subscription 的 publication 集有交集

## 6. 消息语义

### 6.1 `Q`：执行 DDL SQL

函数：`apply_publication_sync_message_q(...)`。

行为：

1. 校验 `ddl_str`、`search_path` 非空。
2. 暂存当前 `search_path`，切换到捕获值。
3. 用 parse-tree 模式执行 SQL（非 SPI 字符串直执行）：
   * `pg_parse_query`
   * analyze/rewrite/plan
   * `ProcessUtility`
4. 执行后恢复原 `search_path`。

附加行为：

1. schema/all-table 场景下，`CREATE TABLE` 后自动写入 `pg_subscription_rel`（READY），保证后续 DML 可立即 apply。

### 6.2 `A`：加入订阅关系映射

函数：`apply_publication_sync_message_a(...)`。

行为：

1. 解析 `target_table` 为关系 OID。
2. 若 `pg_subscription_rel` 无映射，插入 READY 状态。

### 6.3 `D`：移除订阅关系映射

函数：`apply_publication_sync_message_d(...)`。

行为：

1. 解析 `target_table`，若表仍存在则查映射。
2. 若映射存在，从 `pg_subscription_rel` 移除。

## 7. 错误处理策略

1. 未知 `message_type` 显式报错：`Supported message types are Q/A/D.`。
2. `Q` 严格校验 `ddl_str/search_path`，缺失即错误。
3. `A/D` 严格校验 `target_table`，缺失即错误。
4. DDL 执行失败会中断当前 apply 事务，避免半成功状态。

## 8. 边界说明

1. 自动 DDL 同步仅通过 `pg_publication_sync` 消息驱动。
2. 过滤结果由 `pubddl + subddl + publication_list + publication scope` 共同决定。
3. 该设计不试图替代全量 schema diff 工具；它是基于变更流的在线同步机制。
