# TC_DDL_TABLESPACE_MISMATCH

- Init Policy: `FAST_CLEANUP`
- Category: `ddl_scenarios`
- Scope: `N/A`
- DDL Option: `table,index`

## Purpose

验证发布端与订阅端 tablespace 环境不一致时的当前表现与容错策略。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`
3. 仅在发布端准备 tablespace（例如路径 `/data/tbs1`），订阅端不创建同名 tablespace

## Steps

Publisher:

```sql
CREATE TABLE public.tbs_case(id int primary key, v text) TABLESPACE tbs1;
CREATE INDEX tbs_case_v_idx ON public.tbs_case(v) TABLESPACE tbs1;
```

## Expected

1. 若订阅端不存在 `tbs1`，相关 DDL 在订阅端会失败并阻塞该事务推进
2. 补齐订阅端同名 tablespace 后可继续重放

## Note

建议在跨环境部署前建立 tablespace 映射清单并预创建。

