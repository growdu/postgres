# TC_SCOPE_FOR_ALL_DDL_TABLE

- Init Policy: `FULL_INIT`
- Category: `for_scope`
- Scope: `FOR ALL TABLES`
- DDL Option: `table`

## Purpose

验证全库范围下新增表自动进入逻辑复制，且 DDL 不因前序状态失败。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE PUBLICATION pub_fa FOR ALL TABLES WITH (ddl='table');
CREATE TABLE public.t_extra(id int primary key, v text);
INSERT INTO public.t_extra VALUES (1, 'all-table');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_fa CONNECTION :'pub_conn'
PUBLICATION pub_fa
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

## Expected

1. 订阅端 `public.t_extra` 存在
2. 数据同步成功

