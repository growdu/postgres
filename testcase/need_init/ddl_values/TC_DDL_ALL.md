# TC_DDL_ALL

- Init Policy: `FULL_INIT`
- Category: `ddl_values`
- Scope: `FOR ALL TABLES`
- DDL Option: `all`

## Purpose

验证 `ddl=all` 位图在发布端/订阅端接口与行为一致性。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE PUBLICATION pub_fa FOR ALL TABLES WITH (ddl='all');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fa CONNECTION :'pub_conn'
PUBLICATION pub_fa
WITH (create_slot=true, enabled=true, copy_data=false, ddl='all');
```

接口核对：

```sql
SELECT pubname, pubddl, pg_get_ddl_options(pubddl)
FROM pg_publication
WHERE pubname='pub_fa';

SELECT subname, subddl, pg_get_ddl_options(subddl)
FROM pg_subscription
WHERE subname='sub_fa';
```

## Expected

1. `pg_get_ddl_options` 结果包含所有类别
2. 后续复杂 DDL 可在同策略下扩展执行

