# 设计3：利用逻辑复制同步 `pg_publication_sync` 并自动 apply DDL

更新时间：2026-04-17

对应测试文档：  
[pg_publication_sync自动DDL同步-测试验证步骤.md](/Users/growduduan/cwork/postgresql-2/pg_publication_sync自动DDL同步-测试验证步骤.md)

## 1. 目标

实现自动 DDL 同步闭环：

1. 发布端捕获 DDL 并写消息。
2. 逻辑复制同步 `pg_publication_sync` 行。
3. 订阅端按消息类型 apply。

## 2. 消息原则

1. 表只存消息行（`Q/A/D`），不存配置行。
2. 普通 `Q` 消息按 publication 粒度展开写入：
   * 一条 DDL 命中 N 个 publication，就写 N 条 `Q`。
   * 每条消息独立 `pfsyncpubid`。
3. `A/D` 也按 publication 粒度写入。

## 3. 发布端写入

### 3.1 `Q` 写入（`ProcessUtility`）

入口：`CapturePublicationSyncDDL()`。

过滤命中 publication 后，不再拼 `publication_list`，而是对每个命中 publication 单独插入一条 `Q`：

1. `pfsyncpubid`：当前 publication OID
2. `pfsyncmsgtype='Q'`
3. `pfsyncddlsql`：原始 SQL
4. `pfsyncsearchpath`：捕获时 search_path
5. `pfsynctargettable`：可选
6. `pfsynclsn/pfsyncts`：写入时位点与时间

### 3.2 `A/D` 写入（publication 变更）

入口：`insert_publication_sync_relation_message()`。

1. `ALTER PUBLICATION ... ADD TABLE` -> `A`
2. `ALTER PUBLICATION ... DROP TABLE` -> `D`
3. `DROP PUBLICATION` -> 对关联表写 `D`

## 4. 发布端发送过滤（关键）

在 `pgoutput_change()` 对 `pg_publication_sync` 做行级路由：

1. 读取消息行 `pfsyncpubid`
2. 仅当 `pfsyncpubid` 属于当前订阅请求的 publication 集时才发送

结果：订阅端无需再解析 `publication_list` 做 publication 交集判断。

## 5. 订阅端 apply

入口：`maybe_apply_publication_sync_message()`。

前置条件：

1. 仅 leader apply worker 执行
2. 关系是 `pg_publication_sync`
3. `subddl & pfsyncddl != 0`

分发：

1. `Q`：以 parse-tree 路径执行 `pfsyncddlsql`，并恢复 `pfsyncsearchpath`
2. `A`：将 `pfsynctargettable` 加入 `pg_subscription_rel`
3. `D`：将 `pfsynctargettable` 从 `pg_subscription_rel` 移除

## 6. 错误与边界

1. 未知消息类型报错（当前仅支持 `Q/A/D`）
2. `Q` 缺 `pfsyncddlsql/pfsyncsearchpath` 报错
3. `A/D` 缺 `pfsynctargettable` 报错

## 7. 历史清理

提供函数：

```sql
select pg_publication_sync_prune();
```

策略：删除满足 `row_lsn < min(logical slot restart_lsn)` 且早于时间窗口的历史消息。
