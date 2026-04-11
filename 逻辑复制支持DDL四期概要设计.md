

# PostgreSQL 逻辑复制 DDL 四期设计文档

# —— Publication 变更与对象级 Refresh（设计方案，未实现）

---

## 当前实现状态（截至 2026-04-11）

| 模块 | 文档目标 | 当前代码状态 |
| --- | --- | --- |
| REFRESH 对象级 delta schema sync | 新增对象自动补齐 | 未实现 |
| 移出范围对象治理 | 停止复制 + 可选 cleanup | 未实现 |
| `pg_subscription_rel_ext` | 对象级状态跟踪系统表 | 未实现 |
| `drop_missing_objects` 参数 | refresh 可选删除策略 | 未实现 |

---

# 1. 摘要

四期目标是：

```text
让 publication 变更在订阅端可正确收敛
```

前置假设（推荐）：在进入四期前，已完成五期A最小运行保障（错误模型/基础观测/STRICT）。

目标模型：

```text
REFRESH PUBLICATION =
  表级 refresh
  + 对象级 delta schema sync
  + 移出范围对象的停止复制
  + 可选清理策略
```

最终形成：

```text
initial sync（起点一致）
+ refresh（范围变化一致）
+ incremental（持续一致）
```

目标是形成闭环。

---

# 2. 设计目标

---

## 2.1 功能目标

### 目标1：publication 变更可收敛

```text
publisher 修改 publication
→ subscriber refresh 后状态一致
```

---

### 目标2：支持对象级 delta schema sync

```text
新增进入范围的对象 → 自动补齐
```

---

### 目标3：支持范围缩小时的正确行为

```text
移出范围对象：
  ✔ 停止复制
  ✔ 不自动删除（默认）
```

---

### 目标4：提供可选清理策略

```text
WITH (drop_missing_objects=true)
```

---

## 2.2 非目标

```text
❌ 不自动跨 subscription 传播 publication 变更
❌ 不做实时自动 refresh（仍需显式触发）
```

---

# 3. 总体架构

---

## 3.1 refresh 扩展模型

```text
REFRESH PUBLICATION
        ↓
RefreshPlanner
        ↓
┌────────────────────────────┐
│ 1. 表级 refresh             │
│ 2. 对象级 delta sync        │
│ 3. 移出对象处理             │
└────────────────────────────┘
```

---

## 3.2 新增核心组件

### 1️⃣ RefreshPlanner

负责：

```text
计算：
  新增对象
  移出对象
  未变化对象
```

---

### 2️⃣ DeltaSchemaSync（复用 SchemaSyncWorker）

```text
mode = REFRESH_DELTA
```

---

### 3️⃣ ReplicationObjectState（新增系统表）

用于记录：

```text
对象复制状态
```

---

# 4. 系统表设计（新增）

---

## 4.1 pg_subscription_rel_ext（扩展）

用于扩展 tracking：

```c
typedef struct SubscriptionObjectState
{
    Oid subid;
    Oid objid;
    char objtype;  // table/view/function...
    bool is_active;
    XLogRecPtr last_synced_lsn;
}
```

---

## 4.2 作用

用于：

```text
✔ 判断是否已同步
✔ 判断是否移出范围
✔ 支持 delta sync
✔ 支持 cleanup
```

---

# 5. REFRESH 执行流程

---

## 5.1 顶层流程

```text
ALTER SUBSCRIPTION ... REFRESH PUBLICATION
        ↓
RefreshPlanner
        ↓
执行三阶段：

1. 表级 refresh
2. delta schema sync
3. 移出对象处理
```

---

## 5.2 RefreshPlanner

---

### 输入

```text
publication 当前状态
subscription 当前状态
pg_subscription_rel_ext
```

---

### 输出

```text
新增对象集合
移出对象集合
未变化集合
```

---

## 5.3 对象分类

```text
新增：new_set - old_set
移出：old_set - new_set
保留：old_set ∩ new_set
```

---

# 6. 新增对象处理（Delta Schema Sync）

---

## 6.1 触发条件

```text
对象 ∈ 新范围
且 未同步
```

---

## 6.2 执行流程

```text
枚举对象
→ 生成 normalized_sql
→ 依赖排序
→ 执行
→ 标记已同步
```

---

## 6.3 排序规则

```text
schema
→ table
→ type
→ function
→ view
→ trigger
→ index
```

---

## 6.4 示例

```text
publication:
  ddl='table'

→ ddl='table,view'
```

refresh 后：

```text
补执行 CREATE VIEW
```

---

# 7. 表级 refresh（复用现有 PG）

---

## 7.1 行为

```text
✔ 新表 → copy_data
✔ 移除表 → 删除 sync slot
```

---

## 7.2 不修改逻辑

完全复用 PostgreSQL 现有实现。

---

# 8. 移出范围对象处理

---

## 8.1 默认策略（安全）

```text
✔ 停止复制
✔ 保留本地对象
```

---

## 8.2 行为

```text
is_active = false
```

---

## 8.3 示例

```text
publication:
  ddl='table,view'

→ ddl='table'
```

订阅端：

```text
view 保留
但不再接收更新
```

---

# 9. 可选清理策略（高级）

---

## 9.1 参数

```sql
ALTER SUBSCRIPTION ... REFRESH PUBLICATION
WITH (drop_missing_objects = true);
```

---

## 9.2 行为

```text
对移出对象执行 DROP
```

---

## 9.3 安全策略

```text
默认关闭
```

---

## 9.4 冲突处理

```text
依赖存在 → 报错
```

---

# 10. 删除 publication

---

## 10.1 行为

```text
不复制
不自动解绑
```

---

## 10.2 订阅端

```text
publication 不存在 → 报错 → worker 停止
```

---

# 11. 与 initial sync 的关系

---

## 11.1 统一模型

```text
Initial Sync = FULL SYNC
Refresh       = DELTA SYNC
```

---

## 11.2 复用能力

```text
SchemaSyncWorker
```

---

# 12. 与 WAL 复制关系

---

## 12.1 边界

```text
refresh 不写 WAL
```

---

## 12.2 增量

```text
refresh 后 → WAL 接管
```

---

# 13. 幂等性

---

## 13.1 判断

```text
objid + kind + subid
```

---

## 13.2 避免重复执行

```text
已同步 → skip
```

---

# 14. 错误处理

---

## 14.1 delta sync 失败

```text
→ refresh 失败
```

---

## 14.2 cleanup 失败

```text
→ 停止
```

---

# 15. 并发控制

---

## 15.1 refresh vs apply

```text
refresh 独占
```

---

## 15.2 锁

```text
subscription-level lock
```

---

# 16. patch 拆分建议

---

## Patch 10

```text
RefreshPlanner
```

---

## Patch 11

```text
delta schema sync
```

---

## Patch 12

```text
object state tracking
```

---

## Patch 13

```text
cleanup support
```

---

# 17. 最终模型总结

---

## 三阶段一致性

```text
initial sync → 起点一致
refresh      → 范围一致
incremental  → 持续一致
```

---

## 一句话总结

```text
REFRESH =
  reconcile(subscription_state, publication_state)
```

---

# 18. 关键价值

---

```text
✔ 完整闭环
✔ 可扩展
✔ 与 PG 语义一致
✔ 可分阶段提交
```

---

# 最后结论

👉 四期完成后，你的设计将从：

```text
“DDL 可以复制”
```

升级为：

```text
“schema 生命周期完全受 logical replication 管控”
```
