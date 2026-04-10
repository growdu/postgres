下面给你一版评审版设计文档，适合内部评审、方案会、Patch 立项评审。

PostgreSQL 逻辑复制支持 DDL 设计评审文档
1. 文档目的
本文档用于评审 PostgreSQL 逻辑复制 DDL 自动同步方案，明确：
	•	目标与范围
	•	用户接口
	•	系统表设计
	•	发布/订阅处理流程
	•	关键约束与风险
	•	分阶段实施建议
现有 PostgreSQL publication 只定义表级变化的发布范围，官方文档也明确写明“DDL operations are not published”，因此本方案属于对现有逻辑复制能力的扩展。(PostgreSQL)

2. 背景与现状
当前 PostgreSQL 逻辑复制基于 publication/subscription 模型：
	•	publication 定义当前数据库中哪些表的数据变化会被发布。(PostgreSQL)
	•	subscription 按 publication 名称订阅远端变化。(PostgreSQL)
	•	逻辑复制协议按事务顺序传输消息。(PostgreSQL)
但官方文档明确说明，当前 publication 不发布 DDL。(PostgreSQL)
这带来的直接问题是：
	•	用户需要手工保持发布端/订阅端 schema 一致
	•	DDL 与 DML 顺序难以人工保证
	•	运维复杂度高，容易出现“表结构未同步但数据已到达”的错误

3. 设计目标
本方案目标如下：
	1	用户执行普通 DDL 时，由内核自动捕获；
	2	自动判断该 DDL 是否应被 publication 发布；
	3	以系统表 pg_publication_sync 记录 DDL 事件；
	4	通过逻辑复制链路将该事件发送给订阅端；
	5	订阅端按 subscription 配置自动执行对应 DDL；
	6	保证事务级顺序，尽量使 DDL 先于同事务后续 DML 被应用；
	7	不要求用户手工调用额外同步函数。

4. 非目标
一期明确不包含以下目标：
	•	所有对象类型的 DDL 全覆盖
	•	通用 SQL 任意重放
	•	复杂跨版本语法自动兼容
	•	自动冲突修复
	•	审批式手动执行流程
	•	tablesync worker 对 DDL 的处理

5. 总体设计概述
5.1 设计思路
方案采用：
自动捕获 DDL → 写入系统表 pg_publication_sync → 逻辑复制链路特判放行 → 输出阶段转 DDL 消息 → 订阅端执行
核心原因是：
	•	publication/subscription 现有模型本身适合表达“发布哪些变更、订阅哪些 publication”；subscription 也是按 publication 名称建立的。(PostgreSQL)
	•	逻辑复制协议天然具备事务边界与顺序语义。(PostgreSQL)
	•	普通表方案在升级兼容、权限和语义归属上不够内核化，因此采用系统表承载更合适。
5.2 关键原则
	•	pg_publication_sync 是系统表，仅发布端持久化
	•	订阅端不物化该系统表
	•	跨节点必须直接解释的字段，全部使用文本/协议稳定格式，不使用本地 OID
	•	输出阶段将系统表 tuple 升格为 DDL 消息，避免订阅端按普通 tuple apply

6. 术语定义
Publication
publication 是当前数据库中的一个命名对象集合，用于定义哪些表的数据变化会被逻辑复制发布。(PostgreSQL)
Subscription
subscription 通过连接远端 publication 名称列表来订阅逻辑复制流。(PostgreSQL)
DDL 事件
被捕获并可通过逻辑复制传播的 schema 变更事件，如 CREATE TABLE、CREATE INDEX。
pg_publication_sync
新增系统表，用于在发布端持久化 DDL 事件。
LogicalDDLCommand
DDL 的统一中间结构，用于发布端建模、系统表落地、输出消息转换和订阅端 apply。

7. 用户接口设计
7.1 CREATE PUBLICATION 扩展
现有 CREATE PUBLICATION 支持 FOR TABLE、FOR ALL TABLES、FOR TABLES IN SCHEMA，以及 WITH (...) 参数。(PostgreSQL)
新增参数：
WITH (ddl = 'table,index')
支持值
	•	table
	•	index
	•	type
	•	function
	•	domain
	•	trigger
	•	view
	•	rule
	•	schema
	•	extension
	•	all
	•	none
一期支持
仅支持：
	•	table
	•	index
约束
	•	FOR TABLE：只允许 table,index
	•	FOR TABLES IN SCHEMA：只允许 table,index
	•	FOR ALL TABLES：一期也仅开放 table,index

7.2 CREATE SUBSCRIPTION 扩展
现有 CREATE SUBSCRIPTION 通过 publication 名称列表定义订阅关系。(PostgreSQL)
新增参数：
WITH (ddl = 'table,index')
约束
建立 subscription 时校验：
subscription.ddl ⊆ union(publication.ddl)
否则报错。

7.3 用户行为示例
发布端
CREATE PUBLICATION pub1
FOR ALL TABLES
WITH (ddl = 'table,index');
订阅端
CREATE SUBSCRIPTION sub1
CONNECTION 'host=... dbname=...'
PUBLICATION pub1
WITH (ddl = 'table,index');
用户执行
CREATE TABLE public.t1(id int);
CREATE INDEX idx_t1_id ON public.t1(id);
无需额外调用同步函数。

8. 系统表设计
8.1 设计定位
pg_publication_sync 是系统表，用于：
	•	发布端：持久化 DDL 事件
	•	输出阶段：作为 DDL 消息源
	•	订阅端：不落地、不持久化

8.2 字段设计
字段
类型
说明
lsn
pg_lsn
参考 WAL 位点
xid
xid8
顶层事务 ID
ddl_seqno
int4
同事务内 DDL 顺序号
ts
timestamptz
事件时间
message_type
"char"
Q/A/D
ddl_kind
text[]
DDL 类型数组
publication
text[]
publication 名称数组
object_identity
text
对象身份标识
target_table
text
目标表，schema-qualified
command_tag
text
命令标签
message_data
text
规范化 SQL
message_extra
jsonb
扩展信息

8.3 字段设计原则
publication
必须使用 text[]，不能使用 Oid[]。
原因：
	•	subscription 按 publication 名称建立，而不是按 OID。(PostgreSQL)
	•	OID 仅在发布端本库内部有效，订阅端无法解释。
message_data
存储规范化 SQL，而不是原始 SQL 文本。目的是降低 search_path、命名歧义等问题。
xid + ddl_seqno
用于保证同一事务内多条 DDL 的顺序。
object_identity
用于幂等、错误定位、冲突诊断。

8.4 典型记录示例
{
  "lsn": "0/16B6D50",
  "xid": 754321,
  "ddl_seqno": 1,
  "ts": "2026-04-10 10:30:00+08",
  "message_type": "Q",
  "ddl_kind": ["table"],
  "publication": ["pub1"],
  "object_identity": "public.t1",
  "target_table": "public.t1",
  "command_tag": "CREATE TABLE",
  "message_data": "CREATE TABLE public.t1 (id integer)",
  "message_extra": {
    "server_version_num": 180000,
    "syntax": "postgres",
    "normalized": true
  }
}

9. 中间结构设计
建议引入统一中间结构 LogicalDDLCommand：
typedef struct LogicalDDLCommand
{
    ReplicableDDLKind kind;

    Oid         relid;
    Oid         nspid;

    char       *command_tag;
    char       *query_string;
    char       *normalized_sql;
    char       *object_identity;
    char       *target_table;

    List       *publication_names;
    List       *ddl_kind_names;

    uint64      xid;
    uint32      ddl_seqno;
    TimestampTz ts;
    XLogRecPtr  lsn;

    Jsonb      *extra;
} LogicalDDLCommand;
作用
作为发布端和订阅端之间统一的数据模型，支持：
	•	DDL 解析
	•	系统表写入
	•	系统表读取
	•	输出消息序列化
	•	订阅端执行

10. 发布端处理流程
10.1 自动捕获入口
发布端在 ProcessUtility 路径自动识别可复制 DDL。
主流程
进入 ProcessUtility
-> 判断是否属于可复制 DDL
-> 执行真实 DDL
-> 若执行成功，构造 LogicalDDLCommand
-> 写入 pg_publication_sync
关键要求
	•	先执行 DDL，后写同步记录
	•	同一事务内完成
	•	若事务回滚，同步记录也回滚

10.2 命中 publication 计算
发布端根据：
	•	FOR TABLE
	•	FOR TABLES IN SCHEMA
	•	FOR ALL TABLES
以及 publication 的 ddl 配置，计算该 DDL 命中的 publication 名称列表。
publication 本身就是定义表变化发布范围的对象；FOR ALL TABLES 和 FOR TABLES IN SCHEMA 都表示未来表变化的作用域。(PostgreSQL)

10.3 写系统表
发布端调用内部函数：
PublicationSyncInsert(&cmd);
将 LogicalDDLCommand 映射为 pg_publication_sync tuple 并插入系统表。

10.4 循环抑制
必须维护内部状态：
bool in_ddl_replay;
当当前会话处于“订阅端 apply 触发的 DDL replay”状态时，自动捕获逻辑必须跳过写表，避免循环复制。

11. 发布端输出流程
11.1 系统表放行
由于逻辑复制默认不处理普通系统表，需要对 pg_publication_sync 做唯一特判放行。
原则
	•	其它系统表：继续忽略
	•	pg_publication_sync：允许解码输出

11.2 输出阶段转换
不建议把 pg_publication_sync 作为普通 tuple 直接发给订阅端。 建议在输出阶段将其转换为 DDL 消息。
处理逻辑
decode 得到对 pg_publication_sync 的 INSERT
-> 读取 tuple
-> 转为 LogicalDDLCommand
-> 写 DDL message
-> 发送订阅端

11.3 消息策略
一期推荐复用已有 logical replication MESSAGE 通路，而不是一开始就定义全新协议消息。
逻辑复制协议本身是按事务顺序传输消息的。(PostgreSQL)
优点：
	•	改动较小
	•	验证更快
	•	与当前 output plugin 的消息通路更一致

12. 订阅端处理流程
12.1 接收路径
订阅端 apply worker 收到 DDL message 后：
读消息
-> 反序列化为 LogicalDDLCommand
-> 判断是否属于当前 subscription
-> 执行 normalized_sql

12.2 过滤规则
按以下顺序判断：
1. publication 匹配
判断消息中的 publication text[] 与当前 subscription 所配置的 publication 名称列表是否相交。subscription 本来就是按 publication 名称订阅。(PostgreSQL)
2. ddl_kind 匹配
判断当前 subscription 是否启用了该类型。
3. 可选幂等检查
用 object_identity、xid、ddl_seqno 做辅助判断。

12.3 执行策略
一期直接执行 message_data，即规范化 SQL。
执行要求
	•	使用 schema-qualified SQL
	•	不依赖订阅端 search_path
	•	以 subscription owner 身份执行

12.4 失败策略
DDL apply 失败时：
	•	当前复制事务失败
	•	worker 停止或按现有错误策略禁用订阅
	•	默认不自动跳过

13. 顺序与事务一致性
逻辑复制协议本身定义了事务边界与顺序流。(PostgreSQL)
因此，对于：
BEGIN;
CREATE TABLE t1(id int);
INSERT INTO t1 VALUES (1);
COMMIT;
期望顺序为：
	1	发布端执行 DDL
	2	发布端写 pg_publication_sync
	3	同事务继续产生 DML
	4	输出阶段先发 DDL message
	5	订阅端先执行 DDL
	6	再 apply 同事务后续 DML
这样可最大程度保证“先有表结构，再有数据”。

14. 错误处理与幂等
14.1 发布端
	•	DDL 执行失败：不写 pg_publication_sync
	•	写系统表失败：事务失败
14.2 订阅端
	•	DDL apply 失败：停止当前复制进程
	•	默认不自动跳过
14.3 幂等
一期仅做最小幂等检查，例如：
	•	若 CREATE TABLE public.t1 时本地已存在兼容表，可选择跳过或报错
	•	保守策略默认是冲突即报错

15. 清理与维护
pg_publication_sync 只在发布端持久化，会持续增长，因此必须清理。
一期方案
提供：
SELECT pg_publication_sync_prune();
按各 subscription 的最小确认进度清理历史记录。
后续方案
可演进为自动后台清理。

16. 风险与约束
风险 1：SQL replay 仍有环境依赖
缓解方式：
	•	仅支持一期对象类型
	•	强制 normalized_sql
	•	尽量 schema-qualified
风险 2：系统表特判增加链路复杂度
缓解方式：
	•	仅单表特判
	•	输出阶段立即转 message
	•	订阅端不物化系统表
风险 3：循环复制
缓解方式：
	•	in_ddl_replay 强约束
风险 4：冲突与幂等不足
缓解方式：
	•	一期保守报错
	•	后续增强去重与兼容检查

17. 分阶段实施建议
Patch 1
	•	pg_publication / pg_subscription 增加 ddl 配置
	•	语法支持与校验
Patch 2
	•	新增系统表 pg_publication_sync
	•	中间结构 LogicalDDLCommand
Patch 3
	•	ProcessUtility 自动捕获 table/index DDL
	•	写 pg_publication_sync
Patch 4
	•	logical decoding 放行 pg_publication_sync
	•	输出阶段转 DDL message
Patch 5
	•	订阅端 DDL message apply
	•	replay 抑制
	•	基础错误处理
Patch 6
	•	prune
	•	TAP/隔离测试
	•	文档

18. 测试矩阵
接口类
	•	publication ddl 参数合法/非法
	•	subscription ddl 与 publication ddl 的子集关系校验
捕获类
	•	CREATE TABLE
	•	ALTER TABLE
	•	CREATE INDEX
	•	DROP TABLE
	•	DROP INDEX
顺序类
	•	CREATE TABLE + INSERT
	•	CREATE TABLE + CREATE INDEX
	•	同事务多 DDL
过滤类
	•	publication 不命中时不发送
	•	subscription 不命中时不执行
错误类
	•	发布端 DDL 失败不写同步表
	•	订阅端 apply 失败停订阅
递归类
	•	replay DDL 不再次写 pg_publication_sync
清理类
	•	pg_publication_sync_prune() 正常回收

19. 结论
该方案通过引入系统表 pg_publication_sync，在不大幅重写现有逻辑复制框架的前提下，为 PostgreSQL 增加 DDL 自动同步能力。
它的定位是：
	•	一期工程可落地
	•	内核风格相对清晰
	•	为后续“结构化 DDL + 更纯粹协议化消息”演进留出空间
