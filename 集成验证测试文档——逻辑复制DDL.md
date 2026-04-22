# 集成验证测试文档——逻辑复制 DDL（Linux 可重复执行版）

## 1. 目标与范围

本文档用于在 Linux 环境对“逻辑复制 DDL + DML 联动”进行可重复回归，重点覆盖：

1. 发布范围：`FOR TABLE` / `FOR TABLES IN SCHEMA` / `FOR ALL TABLES`
2. `WITH (ddl=...)` 多取值（含单值、组合值、`all`）
3. 复杂 SQL（多动作 `ALTER`、`CREATE FUNCTION`、`TYPE/DOMAIN/EXTENSION`、分区、引用标识符等）
4. DDL 与 DML 同事务、执行顺序、失败重试/阻塞行为
5. 多订阅订阅同一 publication 的并发场景
6. SQL 查询接口可读性与状态一致性（`pg_publication` / `pg_subscription` / `pg_publication_sync` / `pg_stat_subscription`）

---

## 2. Linux 前置条件

1. 可执行文件：`initdb`、`pg_ctl`、`psql`、`createdb`、`dropdb`
2. Shell：`bash`
3. 若 PostgreSQL 使用非系统库目录，设置：
   - `export LD_LIBRARY_PATH=<pg_lib_dir>:$LD_LIBRARY_PATH`
4. 测试用户可在 `/tmp` 下创建目录

说明：本文档不依赖 macOS 路径，不依赖当前开发机固定目录。

---

## 3. 参数化环境变量（建议直接复制执行）

```bash
set -euo pipefail

export PG_BIN="${PG_BIN:-$(dirname "$(command -v psql)")}"
export PG_SUPERUSER="${PG_SUPERUSER:-postgres}"

export PUB_HOST="${PUB_HOST:-127.0.0.1}"
export SUB_HOST="${SUB_HOST:-127.0.0.1}"
export PUB_PORT="${PUB_PORT:-55431}"
export SUB_PORT="${SUB_PORT:-55432}"

export PUB_DB="${PUB_DB:-pubdb}"
export SUB_DB="${SUB_DB:-subdb}"

export PUB_DATA="${PUB_DATA:-/tmp/pgddl_it_pub}"
export SUB_DATA="${SUB_DATA:-/tmp/pgddl_it_sub}"

export PUB_CONNINFO="${PUB_CONNINFO:-host=$PUB_HOST port=$PUB_PORT dbname=$PUB_DB user=$PG_SUPERUSER}"

pub_psql() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PUB_HOST" -p "$PUB_PORT" -U "$PG_SUPERUSER" -d "$PUB_DB" "$@"
}

sub_psql() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SUB_HOST" -p "$SUB_PORT" -U "$PG_SUPERUSER" -d "$SUB_DB" "$@"
}

pub_admin() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PUB_HOST" -p "$PUB_PORT" -U "$PG_SUPERUSER" -d postgres "$@"
}

sub_admin() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SUB_HOST" -p "$SUB_PORT" -U "$PG_SUPERUSER" -d postgres "$@"
}
```

---

## 4. 初始化与销毁

### 4.1 初始化双实例（单机双端口）

```bash
set -euo pipefail

"$PG_BIN/pg_ctl" -D "$PUB_DATA" -m immediate stop >/dev/null 2>&1 || true
"$PG_BIN/pg_ctl" -D "$SUB_DATA" -m immediate stop >/dev/null 2>&1 || true
rm -rf "$PUB_DATA" "$SUB_DATA"

"$PG_BIN/initdb" -D "$PUB_DATA" -U "$PG_SUPERUSER" -A trust >/dev/null
"$PG_BIN/initdb" -D "$SUB_DATA" -U "$PG_SUPERUSER" -A trust >/dev/null

cat >>"$PUB_DATA/postgresql.conf" <<CFG
listen_addresses='*'
port=$PUB_PORT
wal_level=logical
max_replication_slots=64
max_wal_senders=64
max_logical_replication_workers=64
max_worker_processes=128
CFG

cat >>"$SUB_DATA/postgresql.conf" <<CFG
listen_addresses='*'
port=$SUB_PORT
max_logical_replication_workers=64
max_worker_processes=128
CFG

"$PG_BIN/pg_ctl" -D "$PUB_DATA" -l "$PUB_DATA/server.log" start >/dev/null
"$PG_BIN/pg_ctl" -D "$SUB_DATA" -l "$SUB_DATA/server.log" start >/dev/null

pub_admin -c "DROP DATABASE IF EXISTS $PUB_DB;"
pub_admin -c "CREATE DATABASE $PUB_DB;"
sub_admin -c "DROP DATABASE IF EXISTS $SUB_DB;"
sub_admin -c "CREATE DATABASE $SUB_DB;"
```

### 4.2 每轮用例前的对象清理

Publisher:

```sql
DROP PUBLICATION IF EXISTS pub_ft CASCADE;
DROP PUBLICATION IF EXISTS pub_fs CASCADE;
DROP PUBLICATION IF EXISTS pub_fa CASCADE;

DROP SCHEMA IF EXISTS d1 CASCADE;
DROP SCHEMA IF EXISTS d2 CASCADE;
DROP SCHEMA IF EXISTS case_s CASCADE;
DROP SCHEMA IF EXISTS part_s CASCADE;
DROP SCHEMA IF EXISTS ext_s CASCADE;

DROP TABLE IF EXISTS public.t_base CASCADE;
DROP TABLE IF EXISTS public.t_extra CASCADE;
DROP TABLE IF EXISTS public.tx_same CASCADE;
DROP TABLE IF EXISTS public.tx_order CASCADE;
```

Subscriber:

```sql
DROP SUBSCRIPTION IF EXISTS sub_ft;
DROP SUBSCRIPTION IF EXISTS sub_fs;
DROP SUBSCRIPTION IF EXISTS sub_fa;
DROP SUBSCRIPTION IF EXISTS sub_fa_2;

DROP SCHEMA IF EXISTS d1 CASCADE;
DROP SCHEMA IF EXISTS d2 CASCADE;
DROP SCHEMA IF EXISTS case_s CASCADE;
DROP SCHEMA IF EXISTS part_s CASCADE;
DROP SCHEMA IF EXISTS ext_s CASCADE;

DROP TABLE IF EXISTS public.t_base CASCADE;
DROP TABLE IF EXISTS public.t_extra CASCADE;
DROP TABLE IF EXISTS public.tx_same CASCADE;
DROP TABLE IF EXISTS public.tx_order CASCADE;
```

---

### 4.3 用例隔离策略（避免相互干扰）

结论：**会互相干扰**。  
主要干扰源包括：残留 publication/subscription、复制槽、`pg_subscription_rel` 状态、同名对象、上一个用例遗留的 worker 运行状态。

建议分两档执行：

1. 严格隔离（推荐用于缺陷复现、CI）：每个用例前执行第 4.1 章全量重置（重建实例+数据库）。
2. 快速隔离（推荐用于本地批量回归）：每个用例前至少执行第 4.2 章对象清理，并检查复制槽已回收。

复制槽检查（Publisher）：

```sql
SELECT slot_name, active
FROM pg_replication_slots
ORDER BY slot_name;
```

若存在残留逻辑槽（异常中断时可能出现），先在 Subscriber 删除 subscription，再在 Publisher 手工清理槽后再继续下一用例。

推荐最小流程（每个用例）：

1. 执行 4.2（Publisher + Subscriber）
2. 检查 `pg_replication_slots` 无异常残留
3. 开始当前用例

---

## 5. `ddl` 取值清单（按代码能力）

支持值：

1. `table`
2. `index`
3. `trigger`
4. `view`
5. `rule`
6. `schema`
7. `function`
8. `type`
9. `domain`
10. `extension`
11. `all`（以上全集）

推荐组合回归集（最小但高覆盖）：

1. `table`
2. `schema`
3. `index,trigger`
4. `function,type,domain`
5. `extension`
6. `table,index,trigger,schema,function,type,domain,extension`
7. `all`

---

## 6. 发布范围 × DDL 组合矩阵

| 编号 | 发布范围 | ddl 取值 | 预期 |
|---|---|---|---|
| M1 | `FOR TABLE` | `table` | 目标表 DDL 与后续 DML 都可正常推进 |
| M2 | `FOR TABLE` | `index,trigger` | 索引/触发器 DDL 生效，DML 不回归 |
| M3 | `FOR TABLE` | `all` | 全类型开关可读且行为稳定 |
| M4 | `FOR TABLES IN SCHEMA` | `table` | 新建 schema 内表自动纳入 |
| M5 | `FOR TABLES IN SCHEMA` | `schema,function,type,domain` | schema 相关依赖链路可执行 |
| M6 | `FOR TABLES IN SCHEMA` | `all` | schema 场景全量开关稳定 |
| M7 | `FOR ALL TABLES` | `table` | 全库新增表自动进入复制 |
| M8 | `FOR ALL TABLES` | `schema` | 先建 schema 再建表不报错 |
| M9 | `FOR ALL TABLES` | `all` | 最大范围下无冲突、接口可观测 |

---

## 7. 通用建链模板

### 7.1 Publisher 建 publication

按场景替换：

```sql
CREATE PUBLICATION pub_ft FOR TABLE public.t_base WITH (ddl='table');
```

```sql
CREATE PUBLICATION pub_fs FOR TABLES IN SCHEMA d1 WITH (ddl='all');
```

```sql
CREATE PUBLICATION pub_fa FOR ALL TABLES WITH (ddl='all');
```

### 7.2 Subscriber 建 subscription

```sql
CREATE SUBSCRIPTION sub_ft
CONNECTION :'pub_conn'
PUBLICATION pub_ft
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

执行方式（示例）：

```bash
sub_psql -v pub_conn="$PUB_CONNINFO" -c "CREATE SUBSCRIPTION sub_ft CONNECTION :'pub_conn' PUBLICATION pub_ft WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');"
```

### 7.3 状态检查（每个场景都跑）

Publisher:

```sql
SELECT slot_name, active, confirmed_flush_lsn
FROM pg_replication_slots
ORDER BY slot_name;
```

Subscriber:

```sql
SELECT subname, pid, relid, received_lsn, latest_end_lsn
FROM pg_stat_subscription
ORDER BY subname, relid;
```

---

## 8. 核心场景（按矩阵执行）

## 8.1 M1：`FOR TABLE` + `ddl='table'`

Publisher:

```sql
CREATE TABLE public.t_base(id int primary key, v text);
CREATE PUBLICATION pub_ft FOR TABLE public.t_base WITH (ddl='table');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_ft
CONNECTION :'pub_conn'
PUBLICATION pub_ft
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

Publisher:

```sql
ALTER TABLE public.t_base ADD COLUMN v2 text;
INSERT INTO public.t_base VALUES (1, 'a', 'b');
```

Subscriber 验证：

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name='t_base'
ORDER BY ordinal_position;

SELECT * FROM public.t_base ORDER BY id;
```

通过标准：`v2` 存在，`id=1` 行可见。

## 8.2 M2：`FOR TABLE` + `ddl='index,trigger'`

Publisher:

```sql
CREATE TABLE public.t_base(id int primary key, v text);
CREATE PUBLICATION pub_ft FOR TABLE public.t_base WITH (ddl='index,trigger');

CREATE FUNCTION public.f_t_base_trg() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.v := upper(NEW.v);
  RETURN NEW;
END$$;

CREATE TRIGGER trg_t_base_up
BEFORE INSERT ON public.t_base
FOR EACH ROW EXECUTE FUNCTION public.f_t_base_trg();

CREATE INDEX t_base_v_idx ON public.t_base(v);
INSERT INTO public.t_base VALUES (2, 'abc');
```

Subscriber 验证：

```sql
SELECT indexname
FROM pg_indexes
WHERE schemaname='public' AND tablename='t_base'
ORDER BY indexname;

SELECT tgname
FROM pg_trigger t
JOIN pg_class c ON c.oid=t.tgrelid
JOIN pg_namespace n ON n.oid=c.relnamespace
WHERE NOT t.tgisinternal AND n.nspname='public' AND c.relname='t_base'
ORDER BY tgname;

SELECT * FROM public.t_base ORDER BY id;
```

通过标准：索引和触发器元数据可见，DML 正常复制。

## 8.3 M3：`FOR TABLE` + `ddl='all'`

Publisher:

```sql
ALTER PUBLICATION pub_ft SET (ddl='all');
```

Subscriber:

```sql
ALTER SUBSCRIPTION sub_ft SET (ddl='all');
```

接口验证：

```sql
SELECT pubname, pubddl, pg_get_ddl_options(pubddl)
FROM pg_publication WHERE pubname='pub_ft';

SELECT subname, subddl, pg_get_ddl_options(subddl)
FROM pg_subscription WHERE subname='sub_ft';
```

通过标准：可读化字符串与设置一致。

## 8.4 M4：`FOR TABLES IN SCHEMA` + `ddl='table'`

Publisher:

```sql
CREATE SCHEMA d1;
CREATE PUBLICATION pub_fs FOR TABLES IN SCHEMA d1 WITH (ddl='table');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fs
CONNECTION :'pub_conn'
PUBLICATION pub_fs
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

Publisher:

```sql
CREATE TABLE d1.s_t1(id int primary key, v text);
INSERT INTO d1.s_t1 VALUES (1, 'd1');
```

Subscriber 验证：

```sql
SELECT to_regclass('d1.s_t1');
SELECT * FROM d1.s_t1 ORDER BY id;
```

## 8.5 M5：`FOR TABLES IN SCHEMA` + `ddl='schema,function,type,domain'`

Publisher:

```sql
ALTER PUBLICATION pub_fs SET (ddl='schema,function,type,domain');

CREATE DOMAIN d1.email_t AS text CHECK (position('@' in value) > 1);
CREATE TYPE d1.status_t AS ENUM ('new','done');

CREATE FUNCTION d1.norm_email(text) RETURNS d1.email_t
LANGUAGE sql AS $$ SELECT lower($1)::d1.email_t $$;

CREATE TABLE d1.s_t2(
  id int primary key,
  email d1.email_t,
  st d1.status_t DEFAULT 'new'
);

INSERT INTO d1.s_t2 VALUES (1, d1.norm_email('A@B.COM'), 'done');
```

Subscriber 验证：

```sql
SELECT to_regtype('d1.email_t');
SELECT to_regtype('d1.status_t');
SELECT to_regprocedure('d1.norm_email(text)');
SELECT * FROM d1.s_t2 ORDER BY id;
```

## 8.6 M6：`FOR TABLES IN SCHEMA` + `ddl='all'`

Publisher:

```sql
ALTER PUBLICATION pub_fs SET (ddl='all');
```

Subscriber:

```sql
ALTER SUBSCRIPTION sub_fs SET (ddl='all');
```

然后执行第 11 章复杂 SQL 场景中的任意 2 组，作为覆盖抽样。

## 8.7 M7：`FOR ALL TABLES` + `ddl='table'`

Publisher:

```sql
CREATE PUBLICATION pub_fa FOR ALL TABLES WITH (ddl='table');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fa
CONNECTION :'pub_conn'
PUBLICATION pub_fa
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

Publisher:

```sql
CREATE TABLE public.t_extra(id int primary key, v text);
INSERT INTO public.t_extra VALUES (1, 'all-table');
```

Subscriber 验证：

```sql
SELECT to_regclass('public.t_extra');
SELECT * FROM public.t_extra ORDER BY id;
```

## 8.8 M8：`FOR ALL TABLES` + `ddl='schema'`

Publisher:

```sql
ALTER PUBLICATION pub_fa SET (ddl='schema');
CREATE SCHEMA d2;
CREATE TABLE d2.a1(id int primary key, v text);
INSERT INTO d2.a1 VALUES (1, 'schema-chain');
```

Subscriber 验证：

```sql
SELECT to_regclass('d2.a1');
SELECT * FROM d2.a1 ORDER BY id;
```

通过标准：先建 schema 再建表链路不因缺 schema 报错。

## 8.9 M9：`FOR ALL TABLES` + `ddl='all'`

Publisher:

```sql
ALTER PUBLICATION pub_fa SET (ddl='all');
```

Subscriber:

```sql
ALTER SUBSCRIPTION sub_fa SET (ddl='all');
```

然后执行第 10、11、12 章全部场景。

---

## 9. DDL 与 DML 行为专项

## 9.1 同事务：`DDL + DML`

Publisher:

```sql
BEGIN;
CREATE TABLE public.tx_same(id int primary key, v text);
INSERT INTO public.tx_same VALUES (1, 'same-tx');
COMMIT;
```

Subscriber 验证：

```sql
SELECT to_regclass('public.tx_same');
SELECT * FROM public.tx_same ORDER BY id;
```

## 9.2 顺序：先 DDL 后 DML

Publisher:

```sql
CREATE TABLE public.tx_order(id int primary key, v1 text);

BEGIN;
ALTER TABLE public.tx_order ADD COLUMN v2 text;
INSERT INTO public.tx_order(id, v1, v2) VALUES (2, 'after-ddl', 'ok');
COMMIT;
```

Subscriber 验证：

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema='public' AND table_name='tx_order'
ORDER BY ordinal_position;

SELECT * FROM public.tx_order ORDER BY id;
```

通过标准：无“列不存在”错误，`v2='ok'`。

## 9.3 失败后重试/阻塞观察（人工判定）

目标：验证 DML 依赖 DDL 时，行为是“重试等待”还是“直接报错终止”。

建议步骤：

1. 人为在订阅端制造临时执行失败条件（例如权限或锁冲突）。
2. 在发布端发送依赖 DDL 的 DML。
3. 观察 `pg_stat_subscription`、worker 日志、恢复后是否可继续推进。

记录项：是否自动恢复、是否需要人工干预、是否出现永久卡死。

---

## 10. 分区表专项

Publisher:

```sql
CREATE SCHEMA part_s;

CREATE TABLE part_s.orders (
  id bigint NOT NULL,
  bucket int NOT NULL,
  note text,
  PRIMARY KEY(id, bucket)
) PARTITION BY LIST (bucket);

CREATE TABLE part_s.orders_p1 PARTITION OF part_s.orders FOR VALUES IN (1);
CREATE TABLE part_s.orders_p2 PARTITION OF part_s.orders FOR VALUES IN (2);

INSERT INTO part_s.orders VALUES (1001,1,'p1-row'), (1002,2,'p2-row');

CREATE TABLE part_s.orders_p3 (LIKE part_s.orders INCLUDING ALL);
ALTER TABLE part_s.orders ATTACH PARTITION part_s.orders_p3 FOR VALUES IN (3);
INSERT INTO part_s.orders VALUES (1003,3,'p3-row');
```

Subscriber 验证：

```sql
SELECT to_regclass('part_s.orders') AS root_rel,
       to_regclass('part_s.orders_p1') AS p1_rel,
       to_regclass('part_s.orders_p2') AS p2_rel,
       to_regclass('part_s.orders_p3') AS p3_rel;

SELECT id, bucket, note FROM part_s.orders ORDER BY id;
```

---

## 11. 复杂 SQL / 特殊语句专项

## 11.1 引用标识符 + 生成列 + 约束

Publisher:

```sql
CREATE SCHEMA IF NOT EXISTS "CaseSchema";

CREATE TABLE "CaseSchema"."ComplexTable" (
  id bigint generated always as identity PRIMARY KEY,
  raw numeric(20,6) NOT NULL,
  normalized numeric(20,6) generated always as (raw / 1000.0) stored,
  tag text DEFAULT 'N/A',
  created_at timestamptz DEFAULT now(),
  CONSTRAINT raw_nonneg CHECK (raw >= 0)
);

INSERT INTO "CaseSchema"."ComplexTable"(raw, tag)
VALUES (12345.000000, 'ok');
```

## 11.2 多动作 `ALTER TABLE`

Publisher:

```sql
ALTER TABLE "CaseSchema"."ComplexTable"
  ADD COLUMN extra jsonb DEFAULT '{}'::jsonb,
  ADD COLUMN note text;

ALTER TABLE "CaseSchema"."ComplexTable"
  RENAME COLUMN tag TO tag_new;

UPDATE "CaseSchema"."ComplexTable"
SET extra='{"k":"v"}'::jsonb, note='migrated'
WHERE id=1;
```

## 11.3 视图 / 规则

Publisher:

```sql
CREATE VIEW "CaseSchema".v_complex AS
SELECT id, tag_new, (raw > 1000) AS gt_1000
FROM "CaseSchema"."ComplexTable";

CREATE RULE v_complex_ins AS
ON INSERT TO "CaseSchema".v_complex DO INSTEAD
INSERT INTO "CaseSchema"."ComplexTable"(raw, tag_new)
VALUES (NEW.id * 1.0, NEW.tag_new);
```

## 11.4 函数 / 类型 / 域

Publisher:

```sql
CREATE TYPE "CaseSchema".mood AS ENUM ('happy', 'sad');
CREATE DOMAIN "CaseSchema".nn_text AS text CHECK (length(value) > 0);

CREATE FUNCTION "CaseSchema".f_make(v "CaseSchema".nn_text)
RETURNS "CaseSchema".mood
LANGUAGE plpgsql
AS $$
BEGIN
  IF v = 'x' THEN
    RETURN 'sad';
  END IF;
  RETURN 'happy';
END;
$$;
```

## 11.5 扩展（可选，依赖安装）

Publisher:

```sql
CREATE SCHEMA IF NOT EXISTS ext_s;
CREATE EXTENSION IF NOT EXISTS hstore WITH SCHEMA ext_s;

CREATE TABLE ext_s.ext_t(id int primary key, attrs ext_s.hstore);
INSERT INTO ext_s.ext_t VALUES (1, 'k=>v');
```

## 11.6 `search_path` 与未限定名

Publisher:

```sql
SET search_path TO "CaseSchema", public;
CREATE TABLE sp_rel(id int primary key, v text);
INSERT INTO sp_rel VALUES (1, 'sp');
RESET search_path;
```

Subscriber 统一验证：

```sql
SELECT to_regclass('"CaseSchema"."ComplexTable"');
SELECT to_regclass('"CaseSchema".v_complex');
SELECT to_regprocedure('"CaseSchema".f_make("CaseSchema".nn_text)');
SELECT to_regtype('"CaseSchema".mood');
SELECT to_regtype('"CaseSchema".nn_text');
SELECT to_regclass('"CaseSchema".sp_rel');

SELECT id, raw, normalized, tag_new, extra, note
FROM "CaseSchema"."ComplexTable"
ORDER BY id;
```

---

## 12. 多订阅同 publication 并发场景（重点）

目标：验证同一 publication 被多个 subscription 订阅时，不发生同步冲突。

步骤：

1. 在 Subscriber 创建第二个订阅：

```sql
CREATE SUBSCRIPTION sub_fa_2
CONNECTION :'pub_conn'
PUBLICATION pub_fa
WITH (create_slot=true, enabled=true, copy_data=false, ddl='all');
```

2. Publisher 连续执行：

```sql
CREATE SCHEMA IF NOT EXISTS d2;
CREATE TABLE d2.multi_sub_t(id int primary key, v text);
CREATE INDEX multi_sub_t_v_idx ON d2.multi_sub_t(v);
INSERT INTO d2.multi_sub_t VALUES (1, 'a'), (2, 'b');
```

3. Subscriber 验证：

```sql
SELECT subname, pid, relid, received_lsn, latest_end_lsn
FROM pg_stat_subscription
WHERE subname IN ('sub_fa', 'sub_fa_2')
ORDER BY subname, relid;

SELECT * FROM d2.multi_sub_t ORDER BY id;
SELECT indexname FROM pg_indexes
WHERE schemaname='d2' AND tablename='multi_sub_t'
ORDER BY indexname;
```

通过标准：

1. 两个订阅都存活，无异常退出
2. 对象与数据一致
3. 无明显冲突日志（重复 apply 致命错误、catalog 冲突等）

---

## 13. SQL 接口验证（每轮必做）

Publisher:

```sql
SELECT pubname, puballtables, pubddl, pg_get_ddl_options(pubddl) AS pubddl_opts
FROM pg_publication
ORDER BY pubname;

SELECT pfsyncobjid,
       pfsyncpubid,
       pfsyncsubid,
       pfsyncmsgtype,
       pfsynctargettable,
       pfsyncddl,
       pg_get_ddl_options(pfsyncddl) AS pfsyncddl_opts
FROM pg_catalog.pg_publication_sync
ORDER BY pfsyncobjid DESC
LIMIT 200;
```

Subscriber:

```sql
SELECT subname, subenabled, subddl, pg_get_ddl_options(subddl) AS subddl_opts
FROM pg_subscription
ORDER BY subname;

SELECT subname, pid, relid, received_lsn, latest_end_lsn
FROM pg_stat_subscription
ORDER BY subname, relid;
```

---

## 14. 异常与边界回归建议

1. `ddl` 非法值：`WITH (ddl='table,unknown')` 应报错
2. `FOR TABLE` publication 指向不存在表：应按当前实现返回明确错误
3. 订阅端目标对象不存在时：验证是否由 DDL 同步补齐后恢复 DML
4. 先 `CREATE SCHEMA` 再 `CREATE TABLE`（`FOR ALL TABLES`）：必须稳定
5. 大事务（DDL + 批量 DML）下 apply worker 稳定性
6. 回滚事务中的 DDL 不应在订阅端落地
7. `ALTER PUBLICATION ... SET (ddl=...)` 动态调整后，行为应立即可观测

---

## 15. 清理（测试结束）

```bash
set -euo pipefail

sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_ft;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fs;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fa;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fa_2;"

"$PG_BIN/pg_ctl" -D "$PUB_DATA" -m immediate stop >/dev/null 2>&1 || true
"$PG_BIN/pg_ctl" -D "$SUB_DATA" -m immediate stop >/dev/null 2>&1 || true
rm -rf "$PUB_DATA" "$SUB_DATA"
```

---

## 16. 推荐执行节奏

1. 跑第 4 章初始化
2. 每个用例执行前先跑第 4.2 章清理（关键用例建议直接跑 4.1）
3. 按第 8 章矩阵执行 M1 ~ M9
4. 执行第 9、10、11、12 章专项
5. 执行第 13 章接口核对
6. 执行第 14 章边界回归
7. 执行第 15 章清理

按以上顺序可在 Linux 环境稳定复现，并支持后续自动化脚本化。
