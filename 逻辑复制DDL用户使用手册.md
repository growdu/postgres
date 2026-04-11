# 逻辑复制 DDL 用户使用手册

## 1. 适用范围

本文档对应当前分支中的“逻辑复制支持 DDL”实现，目标是让 DDL 与 DML 一起通过逻辑复制自动同步。

当前已支持的 DDL 类型（`ddl` 选项）：

- `table`
- `index`
- `type`
- `function`
- `domain`
- `trigger`
- `view`
- `rule`
- `schema`
- `extension`

当前 `ddl` 默认值：

- publication：`none`（即不发送 DDL）
- subscription：`none`（即不接收 DDL）

只有 publication 和 subscription 两端都开启了匹配的 `ddl` 位图，DDL 才会真正被同步并执行。

---

## 2. 功能行为说明

### 2.1 发送与接收链路

1. 发布端在 `ProcessUtility` 成功执行后捕获可复制 DDL。
2. DDL 作为事务型 logical message（带专用 prefix）写入逻辑复制流。
3. `pgoutput` 根据 publication 的 `pubddl` 过滤后发送。
4. 订阅端 apply worker 根据 subscription 的 `subddl` 过滤后执行 SQL。

### 2.2 顺序保证

- DDL 与同事务内 DML 按提交顺序进入同一逻辑复制流。
- 在“同一事务先建表再插入”场景下，订阅端会先执行 DDL，再应用对应 DML。

### 2.3 DDL 兼容关系校验

创建/修改 subscription 时会校验：

- `subscription.ddl` 必须是所有目标 publication 的 `pubddl` 并集子集
- 若不满足会报错，阻止错误配置落库

---

## 3. 快速使用

### 3.1 发布端配置示例

```sql
-- 示例表
CREATE TABLE public.t_base(id int primary key, note text);

-- 发布 DML + DDL（示例：全部支持类型）
CREATE PUBLICATION pub_ddl
FOR ALL TABLES
WITH (
  publish = 'insert,update,delete,truncate',
  ddl = 'table,index,type,function,domain,trigger,view,rule,schema,extension'
);
```

### 3.2 订阅端配置示例

```sql
CREATE SUBSCRIPTION sub_ddl
CONNECTION 'host=127.0.0.1 port=55432 dbname=postgres user=postgres'
PUBLICATION pub_ddl
WITH (
  copy_data = false,
  create_slot = true,
  enabled = true,
  ddl = 'table,index,type,function,domain,trigger,view,rule,schema,extension'
);
```

### 3.3 修改已存在对象

```sql
ALTER PUBLICATION pub_ddl SET (ddl = 'table,view,type');
ALTER SUBSCRIPTION sub_ddl SET (ddl = 'table,view,type');
```

---

## 4. 观测与排查

### 4.1 查看两端 DDL 配置

```sql
SELECT pubname,
       pubddl,
       (pubddl & 1) <> 0 AS has_table,
       (pubddl & 2) <> 0 AS has_index,
       (pubddl & 4) <> 0 AS has_type,
       (pubddl & 8) <> 0 AS has_function,
       (pubddl & 16) <> 0 AS has_domain,
       (pubddl & 32) <> 0 AS has_trigger,
       (pubddl & 64) <> 0 AS has_view,
       (pubddl & 128) <> 0 AS has_rule,
       (pubddl & 256) <> 0 AS has_schema,
       (pubddl & 512) <> 0 AS has_extension
FROM pg_publication
ORDER BY 1;
```

```sql
SELECT subname,
       subddl,
       (subddl & 1) <> 0 AS has_table,
       (subddl & 2) <> 0 AS has_index,
       (subddl & 4) <> 0 AS has_type,
       (subddl & 8) <> 0 AS has_function,
       (subddl & 16) <> 0 AS has_domain,
       (subddl & 32) <> 0 AS has_trigger,
       (subddl & 64) <> 0 AS has_view,
       (subddl & 128) <> 0 AS has_rule,
       (subddl & 256) <> 0 AS has_schema,
       (subddl & 512) <> 0 AS has_extension
FROM pg_subscription
ORDER BY 1;
```

### 4.2 先判断“发布端没发”还是“订阅端没收”

推荐顺序：

1. 先确认 publication/subscription 的 `ddl` 配置匹配。
2. 再看发布端复制槽消息（是否出现 `MESSAGE`，prefix 如 `pg_ddl_table`、`pg_ddl_view`、`pg_ddl_type`）。
3. 若发布端已发送，再检查订阅端 worker 日志是否执行/报错。

---

## 5. 使用注意事项

### 5.1 关于对象范围

- `ddl='table,index,type,function,domain,trigger,view,rule,schema,extension'` 表示“允许发送/接收哪些 DDL 类型”，不改变 publication 对数据对象本身的可见范围。
- 若使用 `FOR TABLE ...`，只会复制被纳入 publication 的表数据；新建表是否复制，取决于 publication 范围（例如 `FOR ALL TABLES` 更适合自动跟随新表）。

### 5.2 关于 search_path

- 订阅端应用 DDL 时使用稳定 search_path：`public, pg_catalog`。
- 建议在 DDL 中显式写 schema（如 `public.t1`），避免依赖会话级 search_path 导致两端行为不一致。

### 5.3 当前阶段限制

- 当前支持 `table/index/type/function/domain/trigger/view/rule/schema/extension`。
- 不在上述清单内的对象类型不会通过该机制自动同步。

---

## 6. 常见错误

### 6.1 subscription 请求的 DDL 超出 publication 提供能力

现象：创建或修改 subscription 报参数错误（`ddl` 不兼容）。

处理：

1. 降低 subscription `ddl`（例如只保留 `table`）。
2. 或提升 publication `ddl`（例如改为 `table,index,view,type`）。

### 6.2 DDL 同步了，但新表 DML 没同步

优先检查：

1. publication 范围是否包含该新表（`FOR ALL TABLES` / `FOR TABLES IN SCHEMA` / `FOR TABLE`）。
2. 订阅端是否已经收到对应 RELATION/变更消息。

---

## 7. 推荐最小实践

1. 先用 `FOR ALL TABLES + ddl='table,index,type,function,domain,trigger,view,rule,schema,extension'` 验证链路通畅。
2. 再按业务收敛 publication 范围。
3. DDL 与对象名尽量 schema-qualified。
4. 每次调整 `ddl` 选项后立即跑一轮“DDL + DML 混合事务”回归测试。
