# TC_TX_SAME_TRANSACTION

- Init Policy: `FAST_CLEANUP`
- Category: `transaction`
- Scope: `N/A`
- DDL Option: `table`

## Purpose

验证 DDL 与 DML 同事务提交时，订阅端能正确完成建表与数据应用。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
BEGIN;
CREATE TABLE public.tx_same(id int primary key, v text);
INSERT INTO public.tx_same VALUES (1, 'same-tx');
COMMIT;
```

## Expected

1. 订阅端 `public.tx_same` 存在
2. 数据 `id=1` 存在

