# 逻辑复制 DDL 同步改动测试验证步骤

本文覆盖本次改动涉及的三类验证：

1. catalog 与参数解析（本地单节点）
2. 备份恢复回放（`pg_dump`）
3. subscription 参数落盘（双节点最小联调）

---

## 1. 前置准备

1. 重新 `initdb`（本次修改了 system catalog，`catversion` 已提升）
2. 建议启用 `wal_level = logical`
3. 使用 superuser 执行以下 SQL

---

## 2. 回归测试（自动）

### 2.1 publication 回归

```bash
rtk make -C src/test/regress check TESTS=publication
```

验证点：

* `CREATE/ALTER PUBLICATION ... WITH (ddl=...)` 语法可用
* `pg_publication.pubddl` 正确写入
* `pg_publication_sync` 生成并更新 publication 级默认项
* DDL 对象捕获采用“单条 DDL 对应单条 `pfsynckind='o'`”模型
* 非法 `ddl` token 与重复 `ddl` 参数报错

### 2.2 pg_dump 回归

```bash
rtk make -C src/bin/pg_dump check
```

验证点：

* dump 输出包含 publication 的 `ddl` 选项
* dump 输出包含 subscription 的 `ddl` 选项
* 恢复后 `pubddl/subddl` 保持一致

---

## 3. 单节点 SQL 验证（手工）

```sql
CREATE TABLE pub_ddl_test_tbl(id int primary key, v text);
CREATE PUBLICATION pub_ddl_test
FOR TABLE pub_ddl_test_tbl
WITH (ddl = 'table,index,trigger');

SELECT pubname, pubddl
FROM pg_publication
WHERE pubname = 'pub_ddl_test';

SELECT s.pfsynckind, s.pfsyncenabled, s.pfsyncddl
FROM pg_publication_sync s
JOIN pg_publication p ON p.oid = s.pfsyncpubid
WHERE p.pubname = 'pub_ddl_test';

ALTER PUBLICATION pub_ddl_test
SET (ddl = 'view,function');

SELECT pubname, pubddl
FROM pg_publication
WHERE pubname = 'pub_ddl_test';

SELECT s.pfsynckind, s.pfsyncenabled, s.pfsyncddl
FROM pg_publication_sync s
JOIN pg_publication p ON p.oid = s.pfsyncpubid
WHERE p.pubname = 'pub_ddl_test';

-- 错误校验
CREATE PUBLICATION pub_ddl_bad FOR TABLE pub_ddl_test_tbl WITH (ddl = 'table,not_exist_kind');
CREATE PUBLICATION pub_ddl_dup FOR TABLE pub_ddl_test_tbl WITH (ddl = 'table', ddl = 'index');
```

预期：

* `pubddl` 从 `11`（table+index+trigger）更新为 `144`（view+function）
* `pg_publication_sync` 的 publication 级条目同步更新
* 非法 token 与重复参数均报错

### 3.1 单条 DDL 仅写一条对象记录（含 publication_list）

```sql
CREATE TABLE ddl_obj_once(id int);
CREATE PUBLICATION pub_ddl_obj_a FOR TABLE ddl_obj_once WITH (ddl = 'table,index');
CREATE PUBLICATION pub_ddl_obj_b FOR TABLE ddl_obj_once WITH (ddl = 'table');

ALTER TABLE ddl_obj_once ADD COLUMN c1 int;

SELECT count(*) AS ddl_obj_rows
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND position('ALTER TABLE ddl_obj_once' in ddl_str) > 0;

SELECT coalesce(bool_or(position('pub_ddl_obj_a' in publication_list) > 0), false) AS has_pub_a,
       coalesce(bool_or(position('pub_ddl_obj_b' in publication_list) > 0), false) AS has_pub_b
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND position('ALTER TABLE ddl_obj_once' in ddl_str) > 0;

DROP PUBLICATION pub_ddl_obj_a, pub_ddl_obj_b;
DROP TABLE ddl_obj_once;
```

预期：

* `ddl_obj_rows = 1`；
* `has_pub_a = true` 且 `has_pub_b = true`；
* 即单条 DDL 只落一条 `pg_publication_sync` 记录，`publication_list` 记录关联 publication 名称集合。

---

## 4. 双节点 subscription 验证（手工）

> 用两个实例 `publisher` 与 `subscriber` 进行最小联调即可。

### 4.1 publisher

```sql
CREATE TABLE t1(id int primary key);
CREATE PUBLICATION pub1 FOR TABLE t1 WITH (ddl = 'table,index');
SELECT pubname, pubddl FROM pg_publication WHERE pubname = 'pub1';
```

### 4.2 subscriber

```sql
CREATE SUBSCRIPTION sub1
CONNECTION 'host=127.0.0.1 port=<publisher_port> dbname=<db> user=<user> password=<pwd>'
PUBLICATION pub1
WITH (connect = false, ddl = 'table,index');

SELECT subname, subddl
FROM pg_subscription
WHERE subname = 'sub1';

ALTER SUBSCRIPTION sub1
SET (ddl = 'view,function');

SELECT subname, subddl
FROM pg_subscription
WHERE subname = 'sub1';
```

预期：

* `subddl` 首次落盘后为 `3`（table+index）
* `ALTER` 后更新为 `144`（view+function）

### 4.3 订阅先建立后验证 `pg_publication_sync` 增量同步

> 目标：覆盖“订阅已在线，后续 `pg_publication_sync` 新记录必须可见”的场景。

publisher：

```sql
CREATE TABLE t_sys_late(id int primary key, v text);
CREATE PUBLICATION pub_sys_late FOR TABLE t_sys_late WITH (ddl = 'table,index');
```

subscriber：

```sql
CREATE SUBSCRIPTION sub_sys_late
CONNECTION 'host=127.0.0.1 port=<publisher_port> dbname=<db> user=<user> password=<pwd>'
PUBLICATION pub_sys_late
WITH (copy_data = false);
```

publisher（订阅创建后触发）：

```sql
ALTER TABLE t_sys_late ADD COLUMN c2 int;
```

subscriber：

```sql
SELECT p.pubname, s.pfsynckind, s.message_type, s.ddl_str
  FROM pg_publication_sync s
  LEFT JOIN pg_publication p ON p.oid = s.pfsyncpubid
 WHERE (s.pfsynckind = 'p' AND p.pubname = 'pub_sys_late')
    OR (s.pfsynckind = 'o'
        AND position('pub_sys_late' in coalesce(s.publication_list, '')) > 0
        AND position('t_sys_late' in coalesce(s.ddl_str, '')) > 0)
 ORDER BY s.pfsynckind, s.oid;
```

预期：

* 能看到 `pub_sys_late` 的 `pfsynckind='p'` 与 `pfsynckind='o'` 记录；
* 说明订阅建立后新增的 `pg_publication_sync` 数据可以实时同步。

---

## 5. 备份恢复验证（手工）

```bash
rtk pg_dump -d <db> -s -f /tmp/ddl_sync.sql
rtk rg "CREATE PUBLICATION|CREATE SUBSCRIPTION|ddl =" /tmp/ddl_sync.sql
```

预期：

* publication/subscription 的建表语句中都包含 `ddl = '...'`
* 还原后 `pg_publication.pubddl` 与 `pg_subscription.subddl` 一致
