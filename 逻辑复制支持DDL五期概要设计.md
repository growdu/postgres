# PostgreSQL 逻辑复制 DDL 五期设计文档

# —— 运行面能力完善（拆分为 5A/5B，设计方案）

---

## 当前实现状态（截至 2026-04-11）

| 子阶段 | 模块 | 文档目标 | 当前代码状态 |
| --- | --- | --- | --- |
| 五期A（前置） | 错误模型 | `fatal/retryable` 分层与处置策略 | 未实现 |
| 五期A（前置） | 基础观测 | 统一关键日志字段与链路定位 | 未实现 |
| 五期A（前置） | 同步模式（STRICT） | 默认阻断语义与审计要求 | 未实现 |
| 五期B（后置） | 状态管理（核心） | 订阅级/事件级状态体系 | 未实现 |
| 五期B（后置） | 恢复能力 | skip/retry/resync 编排与操作手册 | 未实现 |
| 五期B（后置） | 同步模式（RELAXED） | 可忽略错误边界与降级策略 | 未实现 |
| 五期B（后置） | 完整观测 | 运维查询与端到端诊断收敛 | 未实现 |

---

# 1. 目标与定位

五期不改变 DDL 复制主链路（`ProcessUtility -> WAL logical message -> pgoutput -> apply worker`），只完善运行面治理能力。

```text
可观测
可恢复
可运营
```

为避免三期/四期在缺少治理能力时放大故障面，五期拆分为：

```text
5A（前置）：最小运行保障，先做
5B（后置）：完整运行治理，后做
```

推荐实施顺序：

```text
1 -> 2 -> 5A -> 3 -> 4 -> 5B
```

---

# 2. 范围定义（5A / 5B）

## 2.1 五期A（前置，进入三期前必须完成）

1. 错误模型最小集：
   `fatal/retryable` 二分与统一处置
2. 基础观测最小集：
   关键字段统一与日志链路可追踪
3. `STRICT` 默认语义：
   DDL apply 出错即阻断，禁止隐式降级

## 2.2 五期B（后置，四期后收敛）

1. 状态管理（核心）：
   订阅级/事件级状态模型落地
2. 恢复能力编排：
   `retry -> skip -> resync` 标准化流程
3. `RELAXED` 模式：
   仅允许可证明幂等错误降级 warning
4. 完整观测能力：
   运行态指标、审计与运维查询闭环

## 2.3 非目标

```text
❌ 不新增 DDL 复制对象类型
❌ 不改变 publication/subscription 基本模型
❌ 不重写 WAL 解码/传输协议
```

---

# 3. 五期A 设计（前置）

## 3.1 错误模型最小集

* 致命错误：权限不足、依赖缺失、语义冲突、解析失败
* 可恢复错误：连接中断、锁等待超时、瞬时资源不足

处置原则：

* 默认 fail-fast
* 所有失败必须携带：`SQLSTATE + object_identity + lsn`

## 3.2 基础观测最小集

统一关键字段：

```text
subname, kind, object_identity, xid, lsn, sync_mode, result, sqlstate
```

最小诊断路径：

* 发布端可看到捕获/发出
* 订阅端可看到接收/执行/失败
* 运维可用 `xid + lsn + object_identity` 串联定位

## 3.3 STRICT 默认语义

* 任一 DDL apply 错误即中断
* 阻断后续复制，优先暴露不一致
* 禁止自动降级为 RELAXED

---

# 4. 五期B 设计（后置）

## 4.1 状态管理（核心）

采用最小状态模型，不新增业务队列系统表：

* 订阅级状态：`RUNNING / BLOCKED / RETRYING`
* 事件级状态：`RECEIVED / FILTERED / APPLIED / FAILED / SKIPPED`

优先复用现有机制：

* `pg_subscription.subenabled`
* `disableonerr`
* `ALTER SUBSCRIPTION ... SKIP (lsn=...)`
* `pg_stat_subscription`
* `pg_replication_origin_status`

## 4.2 恢复能力（skip/retry/resync）

三类动作：

1. `retry`：修复环境后重启 worker 重试
2. `skip`：确认可跳过后执行 `ALTER SUBSCRIPTION ... SKIP (lsn=...)`
3. `resync`：执行 `ALTER SUBSCRIPTION ... REFRESH PUBLICATION` 收敛差异

建议流程：

```text
先 retry
再 skip（人工确认）
最后 resync（必要时配合 copy_data）
```

## 4.3 RELAXED 模式（显式可选）

* 仅允许“可证明幂等”的已存在类错误降级 warning
* 权限/依赖/语义冲突仍按致命处理
* 必须保留完整审计日志

---

# 5. 阶段门禁（Gating）

## 5.1 进入三期前（必须满足）

```text
G1: 错误模型最小集生效
G2: 基础观测字段齐全
G3: STRICT 默认模式可验证
```

## 5.2 进入五期B前（建议满足）

```text
G4: 三期 initial sync 可稳定回归
G5: 四期 refresh 场景已覆盖核心回归
```

---

# 6. 测试覆盖建议

## 6.1 五期A（前置）

1. 致命错误阻断复制
2. 可恢复错误可重试推进
3. 日志字段完整性检查
4. STRICT 下冲突 DDL 必阻断

## 6.2 五期B（后置）

1. 状态轨迹：`RECEIVED -> APPLIED/FILTERED/FAILED`
2. `retry -> skip -> resync` 流程回归
3. RELAXED 仅放行可忽略幂等错误
4. 运维定位从告警到 SQLSTATE/LSN 的端到端演练

---

# 7. 分期关系

```text
Phase 1/2：DDL 增量复制能力
Phase 5A：最小运行保障（前置）
Phase 3：initial schema sync
Phase 4：refresh 对象级收敛
Phase 5B：完整运行治理
```

五期拆分后，可以在不推迟三期/四期功能建设的前提下，把关键运行风险前置消减。
