# 逻辑复制 DDL 复杂场景集成测试用例

## 1. 测试目标

本文件用于验证“逻辑复制支持 DDL 自动同步”在复杂场景下的稳定性，重点覆盖：

* 分区表创建/删除链路；
* 同一事务中 DDL + DML 的顺序依赖；
* 复杂 `ALTER TABLE` 语句链；
* publication 成员变更（`A/D`）与后续 DML 的衔接；
* 失败事务回滚下的一致性。

---

## 2. 前置条件

1. 发布端与订阅端均使用当前分支构建。
2. 发布端 `wal_level=logical`，并已创建复制用户。
3. 已启用本分支 DDL 同步能力（`WITH (ddl=...)`）。
4. 建议使用独立数据库执行，避免历史对象干扰。
5. 每个“发布端执行”步骤后，需等待订阅端追平再做断言。

---

## 3. 通用初始化

### 3.1 发布端

```sql
CREATE SCHEMA IF NOT EXISTS it_ddl;
CREATE TABLE IF NOT EXISTS it_ddl.base_tbl(id int primary key, v text);
```

### 3.2 订阅端

```sql
CREATE SCHEMA IF NOT EXISTS it_ddl;
CREATE TABLE IF NOT EXISTS it_ddl.base_tbl(id int primary key, v text);
```

---

## 4. 复杂场景用例

## 4.1 IT-D01 分区表创建/删除与数据同步

目标：验证分区表及分区对象的 DDL `Q` apply，以及后续 DML。

发布端：

```sql
CREATE PUBLICATION pub_it_part
FOR TABLES IN SCHEMA it_ddl
WITH (ddl = 'table,index');
```

订阅端：

```sql
CREATE SUBSCRIPTION sub_it_part
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_part
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行复杂 DDL + DML：

```sql
CREATE TABLE it_ddl.orders(
    id bigint,
    dt date NOT NULL,
    amt numeric(10,2),
    PRIMARY KEY (id, dt)
) PARTITION BY RANGE (dt);

CREATE TABLE it_ddl.orders_2024 PARTITION OF it_ddl.orders
FOR VALUES FROM ('2024-01-01') TO ('2025-01-01');

CREATE TABLE it_ddl.orders_2025 PARTITION OF it_ddl.orders
FOR VALUES FROM ('2025-01-01') TO ('2026-01-01');

CREATE INDEX idx_orders_amt ON it_ddl.orders (amt);

INSERT INTO it_ddl.orders VALUES
(1, '2024-06-01', 10.00),
(2, '2025-06-01', 20.00);

DROP TABLE it_ddl.orders_2024;
```

订阅端验证：

```sql
SELECT to_regclass('it_ddl.orders') IS NOT NULL AS parent_exists;
SELECT to_regclass('it_ddl.orders_2025') IS NOT NULL AS p2025_exists;
SELECT to_regclass('it_ddl.orders_2024') IS NULL AS p2024_dropped;
SELECT count(*) FROM it_ddl.orders;
```

预期：

* 分区父表与保留分区存在；
* 已删除分区在订阅端不存在；
* 可查询到同步后的数据（至少 1 行）。

---

## 4.2 IT-D02 同一事务内 `ALTER TABLE` + `INSERT` 顺序依赖

目标：验证事务内先 DDL 后 DML，订阅端按顺序应用。

发布端准备：

```sql
CREATE TABLE it_ddl.tx_dep_t(id int primary key, v text);
CREATE PUBLICATION pub_it_tx_dep
FOR TABLE it_ddl.tx_dep_t
WITH (ddl = 'table,index');
```

订阅端准备：

```sql
CREATE TABLE it_ddl.tx_dep_t(id int primary key, v text);
CREATE SUBSCRIPTION sub_it_tx_dep
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_tx_dep
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行：

```sql
BEGIN;
ALTER TABLE it_ddl.tx_dep_t ADD COLUMN c1 int DEFAULT 0;
INSERT INTO it_ddl.tx_dep_t(id, v, c1) VALUES (1, 'ok', 100);
COMMIT;
```

订阅端验证：

```sql
SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'it_ddl.tx_dep_t'::regclass
     AND attname = 'c1' AND NOT attisdropped
) AS c1_exists;

SELECT id, v, c1 FROM it_ddl.tx_dep_t WHERE id = 1;
```

预期：

* 列 `c1` 存在；
* 行 `(1, 'ok', 100)` 可见。

---

## 4.3 IT-D03 同一事务内 `CREATE TABLE` + `INSERT`（Schema/All Tables 场景）

目标：验证事务内新建表后立刻写入，订阅端自动纳管并应用 DML。

发布端：

```sql
CREATE PUBLICATION pub_it_tx_create
FOR TABLES IN SCHEMA it_ddl
WITH (ddl = 'table,index');
```

订阅端：

```sql
CREATE SUBSCRIPTION sub_it_tx_create
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_tx_create
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行：

```sql
BEGIN;
CREATE TABLE it_ddl.tx_new_t(id int primary key, v text);
INSERT INTO it_ddl.tx_new_t VALUES (1, 'new-in-tx');
COMMIT;
```

订阅端验证：

```sql
SELECT to_regclass('it_ddl.tx_new_t') IS NOT NULL AS rel_exists;
SELECT count(*) FROM it_ddl.tx_new_t;

SELECT count(*)
  FROM pg_subscription_rel sr
  JOIN pg_subscription s ON s.oid = sr.srsubid
  JOIN pg_class c ON c.oid = sr.srrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE s.subname = 'sub_it_tx_create'
   AND n.nspname = 'it_ddl'
   AND c.relname = 'tx_new_t';
```

预期：

* 新表存在且数据已同步；
* `pg_subscription_rel` 中有映射记录。

---

## 4.4 IT-D04 复杂 `ALTER TABLE` 语句链

目标：覆盖 rename/type/constraint/index 等组合变更。

发布端准备：

```sql
CREATE TABLE it_ddl.alter_chain_t(
    id int primary key,
    v text
);
INSERT INTO it_ddl.alter_chain_t VALUES (1, '123');

CREATE PUBLICATION pub_it_alter_chain
FOR TABLE it_ddl.alter_chain_t
WITH (ddl = 'table,index');
```

订阅端准备：

```sql
CREATE TABLE it_ddl.alter_chain_t(
    id int primary key,
    v text
);
INSERT INTO it_ddl.alter_chain_t VALUES (1, '123');

CREATE SUBSCRIPTION sub_it_alter_chain
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_alter_chain
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行：

```sql
BEGIN;
ALTER TABLE it_ddl.alter_chain_t ADD COLUMN c_raw text;
ALTER TABLE it_ddl.alter_chain_t RENAME COLUMN v TO v_old;
ALTER TABLE it_ddl.alter_chain_t ADD COLUMN v int;
UPDATE it_ddl.alter_chain_t SET v = v_old::int;
ALTER TABLE it_ddl.alter_chain_t ADD CONSTRAINT alter_chain_v_chk CHECK (v > 0);
CREATE INDEX idx_alter_chain_v ON it_ddl.alter_chain_t(v);
COMMIT;
```

订阅端验证：

```sql
SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'it_ddl.alter_chain_t'::regclass
     AND attname = 'v_old' AND NOT attisdropped
) AS has_v_old;

SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'it_ddl.alter_chain_t'::regclass
     AND attname = 'v' AND NOT attisdropped
) AS has_v_new;

SELECT conname
  FROM pg_constraint
 WHERE conrelid = 'it_ddl.alter_chain_t'::regclass
   AND conname = 'alter_chain_v_chk';

SELECT indexname
  FROM pg_indexes
 WHERE schemaname = 'it_ddl'
   AND tablename = 'alter_chain_t'
   AND indexname = 'idx_alter_chain_v';
```

预期：

* 列重命名、生效列、约束、索引在订阅端均存在；
* 变更顺序正确，无 apply 中断。

---

## 4.5 IT-D05 publication 成员变更（A/D）与 DML 连续性

目标：验证 `ALTER PUBLICATION ADD/DROP TABLE` 触发 `A/D`，并影响后续 DML。

发布端准备：

```sql
CREATE TABLE it_ddl.ad_t(id int primary key, v text);
CREATE PUBLICATION pub_it_ad
FOR TABLE it_ddl.base_tbl
WITH (ddl = 'table,index');
```

订阅端准备：

```sql
CREATE TABLE it_ddl.ad_t(id int primary key, v text);
CREATE SUBSCRIPTION sub_it_ad
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_ad
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行与验证：

```sql
-- 未加入 publication 前：不应同步
INSERT INTO it_ddl.ad_t VALUES (1, 'before-add');

ALTER PUBLICATION pub_it_ad ADD TABLE it_ddl.ad_t;
INSERT INTO it_ddl.ad_t VALUES (2, 'after-add');

ALTER PUBLICATION pub_it_ad DROP TABLE it_ddl.ad_t;
INSERT INTO it_ddl.ad_t VALUES (3, 'after-drop');
```

订阅端验证：

```sql
SELECT count(*) FROM it_ddl.ad_t WHERE id = 1; -- 预期 0
SELECT count(*) FROM it_ddl.ad_t WHERE id = 2; -- 预期 1
SELECT count(*) FROM it_ddl.ad_t WHERE id = 3; -- 预期 0

SELECT count(*)
  FROM pg_publication_sync
 WHERE pfsynckind = 'o'
   AND target_table = 'it_ddl.ad_t'
   AND message_type IN ('A', 'D');
```

预期：

* `id=2` 被同步，`id=1`/`id=3` 不同步；
* 能看到 `A`/`D` 记录；
* drop 后订阅关系映射被移除。

---

## 4.6 IT-D06 失败事务回滚一致性（DDL+DML 混合）

目标：验证事务失败时，不会留下半应用结果。

发布端准备：

```sql
CREATE TABLE it_ddl.tx_rollback_t(id int primary key, v text);
CREATE PUBLICATION pub_it_rb
FOR TABLE it_ddl.tx_rollback_t
WITH (ddl = 'table,index');
```

订阅端准备：

```sql
CREATE TABLE it_ddl.tx_rollback_t(id int primary key, v text);
CREATE SUBSCRIPTION sub_it_rb
CONNECTION 'host=... port=... dbname=... user=... password=...'
PUBLICATION pub_it_rb
WITH (copy_data = false, ddl = 'table,index');
```

发布端执行（故意触发失败）：

```sql
BEGIN;
ALTER TABLE it_ddl.tx_rollback_t ADD COLUMN c_fail int;
INSERT INTO it_ddl.tx_rollback_t VALUES (1, 'ok');
INSERT INTO it_ddl.tx_rollback_t VALUES (1, 'dup'); -- 主键冲突，事务失败
COMMIT;
```

订阅端验证：

```sql
SELECT EXISTS (
  SELECT 1 FROM pg_attribute
   WHERE attrelid = 'it_ddl.tx_rollback_t'::regclass
     AND attname = 'c_fail' AND NOT attisdropped
) AS c_fail_exists;

SELECT count(*) FROM it_ddl.tx_rollback_t;
```

预期：

* `c_fail_exists = false`；
* 表中无新增行；
* 不应出现该失败事务对应的有效对象级变更结果。

---

## 5. 结果判定建议

通过标准：

1. 所有用例在订阅端无 apply worker 异常退出；
2. 对象结构与数据结果满足每个用例预期；
3. `pg_publication_sync` 中 `Q/A/D` 与操作语义一致；
4. `A/D` 场景下，`pg_subscription_rel` 映射变化与 DML 结果一致。

---

## 6. 清理脚本（建议）

发布端：

```sql
DROP PUBLICATION IF EXISTS pub_it_part;
DROP PUBLICATION IF EXISTS pub_it_tx_dep;
DROP PUBLICATION IF EXISTS pub_it_tx_create;
DROP PUBLICATION IF EXISTS pub_it_alter_chain;
DROP PUBLICATION IF EXISTS pub_it_ad;
DROP PUBLICATION IF EXISTS pub_it_rb;

DROP SCHEMA IF EXISTS it_ddl CASCADE;
```

订阅端：

```sql
DROP SUBSCRIPTION IF EXISTS sub_it_part;
DROP SUBSCRIPTION IF EXISTS sub_it_tx_dep;
DROP SUBSCRIPTION IF EXISTS sub_it_tx_create;
DROP SUBSCRIPTION IF EXISTS sub_it_alter_chain;
DROP SUBSCRIPTION IF EXISTS sub_it_ad;
DROP SUBSCRIPTION IF EXISTS sub_it_rb;

DROP SCHEMA IF EXISTS it_ddl CASCADE;
```
