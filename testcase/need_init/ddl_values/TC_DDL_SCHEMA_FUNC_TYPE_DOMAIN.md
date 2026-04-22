# TC_DDL_SCHEMA_FUNC_TYPE_DOMAIN

- Init Policy: `FULL_INIT`
- Category: `ddl_values`
- Scope: `FOR TABLES IN SCHEMA`
- DDL Option: `schema,function,type,domain`

## Purpose

验证非 table/index 组合 DDL 的捕获与应用（schema/function/type/domain）。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE SCHEMA d1;
CREATE PUBLICATION pub_fs FOR TABLES IN SCHEMA d1
WITH (ddl='schema,function,type,domain');
CREATE DOMAIN d1.email_t AS text CHECK (position('@' in value) > 1);
CREATE TYPE d1.status_t AS ENUM ('new','done');
CREATE FUNCTION d1.norm_email(text) RETURNS d1.email_t
LANGUAGE sql AS $$ SELECT lower($1)::d1.email_t $$;
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fs CONNECTION :'pub_conn'
PUBLICATION pub_fs
WITH (create_slot=true, enabled=true, copy_data=false, ddl='schema,function,type,domain');
```

## Expected

1. 订阅端可解析 `d1.email_t`、`d1.status_t`、`d1.norm_email(text)`
2. 无 apply worker 异常退出

