# TC_DDL_VIEW_RULE

- Init Policy: `FAST_CLEANUP`
- Category: `ddl_scenarios`
- Scope: `N/A`
- DDL Option: `view,rule`

## Purpose

验证 `view/rule` 场景下 DDL 捕获与应用的完整性。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
CREATE SCHEMA case_s;
CREATE TABLE case_s.t1(id int primary key, v text);
CREATE VIEW case_s.v1 AS SELECT id, v FROM case_s.t1;
CREATE RULE v1_ins AS
ON INSERT TO case_s.v1 DO INSTEAD
INSERT INTO case_s.t1(id, v) VALUES (NEW.id, NEW.v);
```

## Expected

1. 订阅端存在 `case_s.v1`
2. 订阅端规则对象可见

