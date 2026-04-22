# TC_DDL_MULTI_OBJECT_DROP

- Init Policy: `FAST_CLEANUP`
- Category: `ddl_scenarios`
- Scope: `N/A`
- DDL Option: `table`

## Purpose

验证多对象 `DROP` 语句（如 `DROP TABLE a, b`）下，DDL 捕获与生命周期维护是否存在漏项。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
CREATE TABLE public.drop_a(id int primary key);
CREATE TABLE public.drop_b(id int primary key);
DROP TABLE public.drop_a, public.drop_b;
```

## Expected

1. 两个对象都被删除
2. 订阅端不应只处理首个对象而留下残留对象/状态

## Note

该用例用于持续追踪多对象 DROP 场景回归风险。

