# TC_DDL_QUOTED_IDENTIFIER

- Init Policy: `FAST_CLEANUP`
- Category: `ddl_scenarios`
- Scope: `N/A`
- DDL Option: `table`

## Purpose

验证带引号标识符（大小写敏感）在 DDL 同步与目标对象解析中的稳定性。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
CREATE SCHEMA "CaseSchema";
CREATE TABLE "CaseSchema"."MyTable"(
  "ID" int primary key,
  "Value" text
);
INSERT INTO "CaseSchema"."MyTable" VALUES (1, 'ok');
DROP TABLE "CaseSchema"."MyTable";
```

## Expected

1. 创建阶段订阅端对象可见
2. DROP 阶段对象可正确删除，无大小写折叠导致的残留

