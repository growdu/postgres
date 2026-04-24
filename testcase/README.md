# logical replication DDL testcase

本目录用于替代“单一大文档”的测试执行方式，目标是：

1. 按是否需要初始化环境划分用例，避免互相干扰。
2. 按测试范围划分用例（`for` 范围、`ddl` 取值、分区表、同事务、不同 DDL 场景）。
3. 支持 Linux 可重复执行。

## 目录结构

```text
testcase/
  bin/                        # 环境初始化/清理脚本
  need_init/                  # 需要全量初始化环境的用例
    for_scope/
    ddl_values/
  reuse_env/                  # 可复用实例，仅做快速清理的用例
    partition/
    transaction/
    ddl_scenarios/
  template/
  CASE_MATRIX.md              # 总用例清单
```

## 初始化策略

1. `FULL_INIT`
   - 执行 `bin/full_init.sh`
   - 适合 `for` 范围和 `ddl` 取值矩阵类用例（避免前置状态影响）
2. `FAST_CLEANUP`
   - 执行 `bin/fast_cleanup.sh`
   - 然后执行 `bin/check_slots.sh`
   - 适合同一功能域内的增量验证（分区、事务、复杂 DDL）

## 推荐执行顺序

1. 先执行 `need_init/for_scope`
2. 再执行 `need_init/ddl_values`
3. 再执行 `reuse_env/partition`
4. 再执行 `reuse_env/transaction`
5. 最后执行 `reuse_env/ddl_scenarios`

## 快速开始

```bash
cd testcase
./bin/full_init.sh
# 按 CASE_MATRIX.md 执行对应用例
```

## 最小回归脚本

```bash
cd testcase
./bin/min_regress.sh
```

覆盖点：
1. `FOR ALL TABLES` / `FOR TABLES IN SCHEMA` 且 `ddl` 未包含 `table` 时仅提示（NOTICE）。
2. 订阅端缺表时只暂停该远端表，不影响其他表继续 apply。
3. 收到该表新的 `RELATION` 元数据后可恢复该表 apply。
4. 多对象 `DROP` 混合范围（部分在发布范围内、部分不在）时跳过捕获并告警；全在范围内时正常捕获，并写入编码后的 `pfsynctargetlist`。
5. 多订阅并发回放同一 publication DDL 时（`schema/table/index`），重复对象错误会被忽略，worker 不崩溃循环。
6. 缺表进入 pause 后，后续 `Q` 消息成功创建该表可恢复该表的 DML apply（无需额外刷新）。
7. `FOR TABLES IN SCHEMA` 场景执行 `ALTER SCHEMA` 会产生显式 warning（提示当前 FOR 范围不自动同步该类 DDL）。
