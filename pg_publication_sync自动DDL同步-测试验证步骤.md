# 测试3：`pg_publication_sync` 自动 DDL 同步验证步骤

对应设计文档：  
[pg_publication_sync自动DDL同步-设计.md](/Users/growduduan/cwork/postgresql-2/pg_publication_sync自动DDL同步-设计.md)

## 1. 目标

独立验证自动 DDL 同步执行层：

1. `Q` 消息：订阅端执行 `ddl_str`。
2. `A/D` 消息：订阅端维护 `pg_subscription_rel`。
3. 过滤规则：`pubddl + subddl + publication_list + scope` 生效。

## 2. 前置条件

1. publisher 与 subscriber 两节点已就绪。
2. 两端均创建测试 schema：`auto_ddl`。
3. 订阅端已具备与发布端同名基础表（用于 FOR TABLE publication）。

## 3. 验证步骤

### 3.1 `Q` 消息与 `search_path` 恢复

发布端：

```sql
CREATE TABLE auto_ddl.base_q(id int primary key);
CREATE PUBLICATION pub_auto_q
FOR TABLE auto_ddl.base_q
WITH (ddl = 'table');
```

订阅端：

```sql
CREATE TABLE auto_ddl.base_q(id int primary key);
CREATE SUBSCRIPTION sub_auto_q
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_auto_q
WITH (copy_data = false, ddl = 'table');
```

发布端触发 DDL：

```sql
SET search_path = auto_ddl;
CREATE TABLE q_sync_t(id int primary key);
```

订阅端验证：

```sql
SELECT to_regclass('auto_ddl.q_sync_t') IS NOT NULL AS ddl_applied;

SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND message_type = 'Q'
   AND search_path = 'auto_ddl'
   AND position('CREATE TABLE q_sync_t' in ddl_str) > 0;
```

预期：

1. `ddl_applied = true`。
2. 存在 `Q` 记录，且 `search_path` 被正确记录。

### 3.2 `A/D` 消息驱动 subscription 成员映射

发布端：

```sql
CREATE TABLE auto_ddl.ad_base(id int primary key);
CREATE TABLE auto_ddl.ad_target(id int primary key, v text);

CREATE PUBLICATION pub_auto_ad
FOR TABLE auto_ddl.ad_base
WITH (ddl = 'table,index');
```

订阅端：

```sql
CREATE TABLE auto_ddl.ad_base(id int primary key);
CREATE TABLE auto_ddl.ad_target(id int primary key, v text);

CREATE SUBSCRIPTION sub_auto_ad
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_auto_ad
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行：

```sql
INSERT INTO auto_ddl.ad_target VALUES (1, 'before-add');

ALTER PUBLICATION pub_auto_ad ADD TABLE auto_ddl.ad_target;
INSERT INTO auto_ddl.ad_target VALUES (2, 'after-add');

ALTER PUBLICATION pub_auto_ad DROP TABLE auto_ddl.ad_target;
INSERT INTO auto_ddl.ad_target VALUES (3, 'after-drop');
```

订阅端验证：

```sql
SELECT count(*) FROM auto_ddl.ad_target WHERE id = 1; -- 预期 0
SELECT count(*) FROM auto_ddl.ad_target WHERE id = 2; -- 预期 1
SELECT count(*) FROM auto_ddl.ad_target WHERE id = 3; -- 预期 0

SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND target_table = 'auto_ddl.ad_target'
   AND message_type IN ('A', 'D');

SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE s.subname = 'sub_auto_ad'
   AND n.nspname = 'auto_ddl'
   AND c.relname = 'ad_target';
```

预期：

1. 仅 `id=2` 被复制。
2. 有 `A` 和 `D` 消息记录。
3. 最终 `pg_subscription_rel` 中 `ad_target` 映射被移除。

### 3.3 过滤规则验证（scope + ddl + publication_list）

发布端：

```sql
CREATE TABLE auto_ddl.scope_tbl(id int primary key);
CREATE TABLE auto_ddl.scope_tbl2(id int primary key);

CREATE PUBLICATION pub_scope_tbl
FOR TABLE auto_ddl.scope_tbl
WITH (ddl = 'table');

CREATE PUBLICATION pub_scope_idx
FOR TABLE auto_ddl.scope_tbl
WITH (ddl = 'index');
```

订阅端：

```sql
CREATE TABLE auto_ddl.scope_tbl(id int primary key);
CREATE TABLE auto_ddl.scope_tbl2(id int primary key);

CREATE SUBSCRIPTION sub_scope_tbl
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_scope_tbl
WITH (copy_data = false, ddl = 'table');
```

发布端执行：

```sql
ALTER TABLE auto_ddl.scope_tbl ADD COLUMN c1 int;
ALTER TABLE auto_ddl.scope_tbl2 ADD COLUMN c2 int;
```

订阅端验证：

```sql
SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'auto_ddl.scope_tbl'::regclass
     AND attname = 'c1' AND NOT attisdropped
) AS scope_tbl_applied;

SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'auto_ddl.scope_tbl2'::regclass
     AND attname = 'c2' AND NOT attisdropped
) AS scope_tbl2_applied;

SELECT coalesce(bool_or(position('pub_scope_idx' in publication_list) > 0), false)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND position('ALTER TABLE auto_ddl.scope_tbl ADD COLUMN c1' in ddl_str) > 0;
```

预期：

1. `scope_tbl_applied = true`。
2. `scope_tbl2_applied = false`（publication_list 无交集）。
3. 第三条查询返回 `false`（index-only publication 不捕获 table DDL）。

## 4. 清理

发布端：

```sql
DROP PUBLICATION IF EXISTS pub_auto_q;
DROP PUBLICATION IF EXISTS pub_auto_ad;
DROP PUBLICATION IF EXISTS pub_scope_tbl;
DROP PUBLICATION IF EXISTS pub_scope_idx;
DROP SCHEMA IF EXISTS auto_ddl CASCADE;
```

订阅端：

```sql
DROP SUBSCRIPTION IF EXISTS sub_auto_q;
DROP SUBSCRIPTION IF EXISTS sub_auto_ad;
DROP SUBSCRIPTION IF EXISTS sub_scope_tbl;
DROP SCHEMA IF EXISTS auto_ddl CASCADE;
```

