# CASE MATRIX

| Case ID | Init Policy | Category | Focus |
|---|---|---|---|
| TC_SCOPE_FOR_TABLE_DDL_TABLE | FULL_INIT | for_scope | `FOR TABLE` + `ddl=table` |
| TC_SCOPE_FOR_SCHEMA_DDL_TABLE | FULL_INIT | for_scope | `FOR TABLES IN SCHEMA` + `ddl=table` |
| TC_SCOPE_FOR_ALL_DDL_TABLE | FULL_INIT | for_scope | `FOR ALL TABLES` + `ddl=table` |
| TC_DDL_TABLE_INDEX | FULL_INIT | ddl_values | `ddl=table,index` |
| TC_DDL_SCHEMA_FUNC_TYPE_DOMAIN | FULL_INIT | ddl_values | `ddl=schema,function,type,domain` |
| TC_DDL_ALL | FULL_INIT | ddl_values | `ddl=all` |
| TC_PARTITION_ATTACH | FAST_CLEANUP | partition | 分区表 + attach partition |
| TC_TX_SAME_TRANSACTION | FAST_CLEANUP | transaction | DDL + DML 同事务 |
| TC_TX_DDL_THEN_DML | FAST_CLEANUP | transaction | 先 DDL 后 DML 顺序 |
| TC_DDL_QUOTED_IDENTIFIER | FAST_CLEANUP | ddl_scenarios | quoted 标识符场景 |
| TC_DDL_VIEW_RULE | FAST_CLEANUP | ddl_scenarios | view/rule 场景 |
| TC_DDL_MULTI_OBJECT_DROP | FAST_CLEANUP | ddl_scenarios | 多对象 DROP 场景 |
| TC_DDL_TABLESPACE_MISMATCH | FAST_CLEANUP | ddl_scenarios | 发布订阅端 tablespace 路径不一致 |

## 执行规则

1. `FULL_INIT` 用例前执行 `bin/full_init.sh`
2. `FAST_CLEANUP` 用例前执行 `bin/fast_cleanup.sh && bin/check_slots.sh`
3. 用例执行完成后记录：
   - SQL 结果
   - `pg_stat_subscription`
   - `pg_replication_slots`
   - 关键日志（失败时）

