# <CASE_ID>

- Init Policy: `FULL_INIT | FAST_CLEANUP`
- Category: `<for_scope|ddl_values|partition|transaction|ddl_scenarios>`
- Scope: `<FOR TABLE | FOR TABLES IN SCHEMA | FOR ALL TABLES | N/A>`
- DDL Option: `<table/index/.../all>`

## Purpose

一句话说明该用例验证目标。

## Pre-steps

1. 执行 `../../bin/full_init.sh` 或 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

列出发布端和订阅端操作 SQL。

## Expected

1. 元数据行为
2. 数据行为
3. 无异常 worker 终止/卡死

## Observe

```sql
SELECT subname, pid, relid, received_lsn, latest_end_lsn
FROM pg_stat_subscription
ORDER BY subname, relid;
```

