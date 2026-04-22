# TC_SCOPE_FOR_SCHEMA_DDL_TABLE

- Init Policy: `FULL_INIT`
- Category: `for_scope`
- Scope: `FOR TABLES IN SCHEMA`
- DDL Option: `table`

## Purpose

验证 schema 范围发布中，新表自动纳入复制并完成 DDL + DML 联动。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE SCHEMA d1;
CREATE PUBLICATION pub_fs FOR TABLES IN SCHEMA d1 WITH (ddl='table');
CREATE TABLE d1.s_t1(id int primary key, v text);
INSERT INTO d1.s_t1 VALUES (1, 'd1');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fs CONNECTION :'pub_conn'
PUBLICATION pub_fs
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

## Expected

1. 订阅端 `d1.s_t1` 存在
2. 数据同步成功

