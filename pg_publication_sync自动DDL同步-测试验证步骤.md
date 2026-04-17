# 测试3：`pg_publication_sync` 自动 DDL 同步验证步骤

对应设计文档：  
[pg_publication_sync自动DDL同步-设计.md](/Users/growduduan/cwork/postgresql-2/pg_publication_sync自动DDL同步-设计.md)

## 1. 目标

验证执行层新模型：

1. 表内仅 `Q/A/D` 消息；
2. `Q` 按 publication 粒度展开写入；
3. 发布端按 `pfsyncpubid` 路由发送；
4. 订阅端 apply 不依赖 `publication_list`。

## 2. 前置条件

1. publisher/subscriber 两节点；
2. 当前分支重新 `initdb`；
3. 两端均有基础 schema 与基础表。

## 3. 用例

### 3.1 `Q` apply + search_path

发布端：

```sql
CREATE TABLE auto_ddl.base_q(id int primary key);
CREATE PUBLICATION pub_auto_q FOR TABLE auto_ddl.base_q WITH (ddl='table');
SET search_path = auto_ddl;
CREATE TABLE q_sync_t(id int primary key);
```

订阅端：

```sql
CREATE TABLE auto_ddl.base_q(id int primary key);
CREATE SUBSCRIPTION sub_auto_q ... PUBLICATION pub_auto_q WITH (copy_data=false, ddl='table');

SELECT to_regclass('auto_ddl.q_sync_t') IS NOT NULL;
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype='Q'
   AND pfsyncsearchpath='auto_ddl'
   AND position('CREATE TABLE q_sync_t' in pfsyncddlsql) > 0;
```

预期：DDL 被执行，且消息落表。

### 3.2 `Q` fan-out（同一 DDL 命中多个 publication）

发布端：

```sql
CREATE TABLE auto_ddl.fanout_base(id int primary key);
CREATE PUBLICATION pub_fanout_a FOR TABLE auto_ddl.fanout_base WITH (ddl='table');
CREATE PUBLICATION pub_fanout_b FOR TABLE auto_ddl.fanout_base WITH (ddl='table');
CREATE TABLE auto_ddl.fanout_t(id int);
```

发布端验证：

```sql
SELECT p.pubname, count(*)
  FROM pg_publication_sync s
  JOIN pg_publication p ON p.oid = s.pfsyncpubid
 WHERE s.pfsyncmsgtype='Q'
   AND position('CREATE TABLE auto_ddl.fanout_t' in s.pfsyncddlsql) > 0
 GROUP BY p.pubname
 ORDER BY p.pubname;
```

预期：`pub_fanout_a/pub_fanout_b` 各 1 条。

### 3.3 发布端按 `pfsyncpubid` 路由（订阅端不看 publication_list）

订阅端只订阅 `pub_fanout_a`：

```sql
CREATE SUBSCRIPTION sub_fanout_a ... PUBLICATION pub_fanout_a WITH (copy_data=false, ddl='table');
```

发布端再执行一条命中 A/B 的 DDL 后，订阅端验证：

```sql
SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsyncmsgtype='Q'
   AND position('CREATE TABLE auto_ddl.fanout_t2' in pfsyncddlsql) > 0;
```

预期：仅 1 条（只收到 pub_fanout_a 的消息）。

### 3.4 `A/D` apply

发布端：

```sql
CREATE TABLE auto_ddl.ad_base(id int primary key);
CREATE TABLE auto_ddl.ad_target(id int primary key, v text);
CREATE PUBLICATION pub_auto_ad FOR TABLE auto_ddl.ad_base WITH (ddl='table');

ALTER PUBLICATION pub_auto_ad ADD TABLE auto_ddl.ad_target;
ALTER PUBLICATION pub_auto_ad DROP TABLE auto_ddl.ad_target;
```

订阅端验证：

```sql
SELECT count(*) FROM pg_publication_sync WHERE pfsyncmsgtype='A' AND pfsynctargettable='auto_ddl.ad_target';
SELECT count(*) FROM pg_publication_sync WHERE pfsyncmsgtype='D' AND pfsynctargettable='auto_ddl.ad_target';
```

预期：`A`、`D` 各至少 1 条，且订阅关系映射随之增删。

### 3.5 历史清理

发布端：

```sql
SELECT pg_publication_sync_prune();
```

预期：返回 `int8` 删除计数，且不会删除仍处于逻辑槽保留窗口内的消息。
