# TC_SCOPE_FOR_TABLE_DDL_TABLE

- Init Policy: `FULL_INIT`
- Category: `for_scope`
- Scope: `FOR TABLE`
- DDL Option: `table`

## Purpose

验证 `FOR TABLE` 场景下，仅表类 DDL 被捕获并驱动后续 DML 正常复制。

## Pre-steps

1. 执行 `../../bin/full_init.sh`

## Steps

Publisher:

```sql
CREATE TABLE public.t_base(id int primary key, v text);
CREATE PUBLICATION pub_ft FOR TABLE public.t_base WITH (ddl='table');
ALTER TABLE public.t_base ADD COLUMN v2 text;
INSERT INTO public.t_base VALUES (1, 'a', 'b');
```

Subscriber:

```sql
CREATE SUBSCRIPTION sub_ft CONNECTION :'pub_conn'
PUBLICATION pub_ft
WITH (create_slot=true, enabled=true, copy_data=false, ddl='table');
```

## Expected

1. 订阅端存在 `public.t_base.v2`
2. 数据 `id=1` 可见

