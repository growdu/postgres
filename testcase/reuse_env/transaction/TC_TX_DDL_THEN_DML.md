# TC_TX_DDL_THEN_DML

- Init Policy: `FAST_CLEANUP`
- Category: `transaction`
- Scope: `N/A`
- DDL Option: `table`

## Purpose

验证同一事务中先 DDL 后 DML 的执行顺序是否正确（避免订阅端列不存在错误）。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
CREATE TABLE public.tx_order(id int primary key, v1 text);
BEGIN;
ALTER TABLE public.tx_order ADD COLUMN v2 text;
INSERT INTO public.tx_order(id, v1, v2) VALUES (2, 'after-ddl', 'ok');
COMMIT;
```

## Expected

1. 订阅端 `v2` 列存在
2. `id=2` 行 `v2='ok'`

