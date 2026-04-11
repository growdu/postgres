# 逻辑复制 DDL 测试说明

## 1. 测试目标

验证以下核心能力：

1. DDL 能否自动同步到订阅端。
2. DDL 与 DML 同事务复制是否顺序正确。
3. 出现问题时能否快速定位为“发布端未发送”或“订阅端未接收/未应用”。
4. `type/function/domain/trigger/view/rule/schema/extension` 是否可被正确过滤和同步。

---

## 2. 测试环境

建议本机双实例：

- 发布端：`127.0.0.1:55432`
- 订阅端：`127.0.0.1:55433`

前置参数（至少发布端）：

- `wal_level = logical`
- `max_replication_slots >= 4`
- `max_wal_senders >= 4`

---

## 3. 基础校验

### 3.1 catalog 字段校验

```sql
SELECT attname, atttypid::regtype
FROM pg_attribute
WHERE attrelid = 'pg_publication'::regclass
  AND attname = 'pubddl';
```

预期：`pubddl` 类型是 `integer`（`int4`）。

```sql
SELECT attname, atttypid::regtype
FROM pg_attribute
WHERE attrelid = 'pg_subscription'::regclass
  AND attname = 'subddl';
```

预期：`subddl` 类型是 `integer`（`int4`）。

---

## 4. 场景一：DDL 自动同步

### 4.1 准备

发布端：

```sql
DROP TABLE IF EXISTS public.t_base CASCADE;
CREATE TABLE public.t_base(id int primary key, note text);

DROP PUBLICATION IF EXISTS pub_ddl;
CREATE PUBLICATION pub_ddl
FOR ALL TABLES
WITH (
  publish = 'insert,update,delete,truncate',
  ddl = 'table,index,type,function,domain,trigger,view,rule,schema,extension'
);
```

订阅端：

```sql
DROP SUBSCRIPTION IF EXISTS sub_ddl;
CREATE SUBSCRIPTION sub_ddl
CONNECTION 'host=127.0.0.1 port=55432 dbname=postgres user=postgres'
PUBLICATION pub_ddl
WITH (
  copy_data=false,
  create_slot=true,
  enabled=true,
  ddl='table,index,type,function,domain,trigger,view,rule,schema,extension'
);
```

### 4.2 执行

发布端：

```sql
CREATE TABLE public.t_ddl_auto(id int primary key, note text);
```

订阅端验证：

```sql
SELECT to_regclass('public.t_ddl_auto') IS NOT NULL AS ddl_synced;
```

预期：`ddl_synced = t`。

---

## 5. 场景二：DDL + DML 同事务

### 5.1 执行

发布端：

```sql
BEGIN;
CREATE TABLE public.t_mix(id int primary key, note text);
INSERT INTO public.t_mix VALUES (1, 'from same tx');
INSERT INTO public.t_base VALUES (1, 'base row');
COMMIT;
```

### 5.2 验证

订阅端：

```sql
SELECT to_regclass('public.t_mix') IS NOT NULL AS mix_exists;
SELECT count(*) AS mix_cnt FROM public.t_mix;
SELECT count(*) AS base_cnt FROM public.t_base WHERE id = 1;
```

预期：

- `mix_exists = t`
- `mix_cnt = 1`
- `base_cnt = 1`

如果三项都满足，说明 DDL 自动同步和 DDL+DML 混合事务复制路径正常。

---

## 6. 场景三：DDL 过滤与兼容性

### 6.1 publication 只开放 table，index 不应同步

发布端：

```sql
ALTER PUBLICATION pub_ddl SET (ddl = 'table');
DROP TABLE IF EXISTS public.t_filter;
CREATE TABLE public.t_filter(id int primary key, note text);
CREATE INDEX idx_t_filter_note ON public.t_filter(note);
```

订阅端验证：

```sql
SELECT to_regclass('public.t_filter') IS NOT NULL AS table_ok;
SELECT to_regclass('public.idx_t_filter_note') IS NOT NULL AS index_ok;
```

预期：

- `table_ok = t`
- `index_ok = f`

### 6.2 subscription 请求超集应报错

当 publication 为 `ddl='table'` 时，在订阅端执行：

```sql
ALTER SUBSCRIPTION sub_ddl SET (ddl = 'table,index');
```

预期：报错，提示 subscription 请求了 publication 未提供的 DDL 类型。

---

## 7. 场景四：新增对象类型冒烟验证

发布端（单事务内执行，便于检查顺序）：

```sql
BEGIN;
CREATE SCHEMA IF NOT EXISTS s_ddl;
CREATE TYPE s_ddl.status_t AS ENUM ('ok','bad');
CREATE DOMAIN s_ddl.email_t AS text CHECK (position('@' in VALUE) > 1);
CREATE FUNCTION s_ddl.f_add(a int, b int) RETURNS int
LANGUAGE SQL
AS $$ SELECT a + b $$;
CREATE TABLE s_ddl.t_obj (id int primary key, v int);
CREATE VIEW s_ddl.v_obj AS SELECT id, v FROM s_ddl.t_obj;
CREATE RULE r_obj_ins AS ON INSERT TO s_ddl.v_obj DO INSTEAD
  INSERT INTO s_ddl.t_obj(id, v) VALUES (NEW.id, NEW.v);
CREATE FUNCTION s_ddl.tr_set_v() RETURNS trigger
LANGUAGE plpgsql
AS $$ BEGIN NEW.v := COALESCE(NEW.v, 0); RETURN NEW; END $$;
CREATE TRIGGER tr_obj_bi
BEFORE INSERT ON s_ddl.t_obj
FOR EACH ROW EXECUTE FUNCTION s_ddl.tr_set_v();
COMMIT;
```

订阅端验证（按对象类型核对）：

```sql
SELECT to_regnamespace('s_ddl') IS NOT NULL AS schema_ok;
SELECT to_regtype('s_ddl.status_t') IS NOT NULL AS type_ok;
SELECT to_regtype('s_ddl.email_t') IS NOT NULL AS domain_ok;
SELECT to_regprocedure('s_ddl.f_add(int,int)') IS NOT NULL AS function_ok;
SELECT to_regclass('s_ddl.v_obj') IS NOT NULL AS view_ok;
SELECT EXISTS (
  SELECT 1 FROM pg_rewrite r
  JOIN pg_class c ON c.oid = r.ev_class
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 's_ddl' AND c.relname = 'v_obj' AND r.rulename = 'r_obj_ins'
) AS rule_ok;
SELECT EXISTS (
  SELECT 1 FROM pg_trigger t
  JOIN pg_class c ON c.oid = t.tgrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = 's_ddl' AND c.relname = 't_obj' AND t.tgname = 'tr_obj_bi'
) AS trigger_ok;
```

`extension` 建议单独验证（取决于测试环境是否安装扩展包）：

```sql
-- 发布端
CREATE EXTENSION IF NOT EXISTS hstore;
-- 订阅端
SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'hstore') AS extension_ok;
```

---

## 8. 问题定位：发布端没发还是订阅端没收

### 8.1 先看配置是否匹配

发布端：

```sql
SELECT pubname, pubddl FROM pg_publication WHERE pubname = 'pub_ddl';
```

订阅端：

```sql
SELECT subname, subddl FROM pg_subscription WHERE subname = 'sub_ddl';
```

### 8.2 再看发布端逻辑槽是否出现 DDL message（可选）

```sql
SELECT lsn, xid, encode(data, 'escape') AS msg
FROM pg_logical_slot_peek_binary_changes(
  'sub_ddl',
  NULL,
  NULL,
  'proto_version', '1',
  'publication_names', 'pub_ddl'
);
```

观察点：输出中应包含 DDL message（prefix 如 `pg_ddl_table`、`pg_ddl_view`、`pg_ddl_type`、`pg_ddl_extension`）。

### 8.3 发布端已发送但订阅端无结果

重点查订阅端：

1. apply worker 是否在运行（`pg_stat_subscription`）。
2. 日志是否出现 DDL 执行错误（权限、对象已存在、schema 不匹配）。

---

## 9. 清理步骤

订阅端：

```sql
DROP SUBSCRIPTION IF EXISTS sub_ddl;
```

发布端：

```sql
DROP PUBLICATION IF EXISTS pub_ddl;
DROP TABLE IF EXISTS public.t_mix;
DROP TABLE IF EXISTS public.t_ddl_auto;
DROP TABLE IF EXISTS public.t_filter;
DROP TABLE IF EXISTS public.t_base;
DROP SCHEMA IF EXISTS s_ddl CASCADE;
```

如果是临时实例，还可停止并清理数据目录：

```bash
pg_ctl -D /tmp/pgddl-pub -m immediate stop
pg_ctl -D /tmp/pgddl-sub -m immediate stop
rm -rf /tmp/pgddl-pub /tmp/pgddl-sub
```
