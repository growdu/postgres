# TC_DDL_TABLE_INDEX

- Init Policy: `FULL_INIT`
- Category: `ddl_values`
- Scope: `FOR TABLE`
- DDL Option: `table,index`

## Purpose

验证表类 DDL 与索引类 DDL 同时开启时的联动行为。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE TABLE public.t_base(id int primary key, v text);
CREATE PUBLICATION pub_ft FOR TABLE public.t_base WITH (ddl='table,index');
ALTER TABLE public.t_base ADD COLUMN v2 text;
CREATE INDEX t_base_v_idx ON public.t_base(v);
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_ft CONNECTION :'pub_conn'
PUBLICATION pub_ft
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table,index');
```

## Expected

1. 订阅端新增列 `v2` 可见
2. 订阅端索引 `t_base_v_idx` 可见

