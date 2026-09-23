# pg_lrstat

Logical replication LSN and rate statistics.

- [USER_MANUAL.md](USER_MANUAL.md) — 用户使用手册：安装、配置（GUC）、全部视图、故障排查
- [BEST_PRACTICES.md](BEST_PRACTICES.md) — 逻辑复制性能分析最佳实践：巡检 SQL、瓶颈定位、容量规划
- [DESIGN.md](DESIGN.md) — 设计文档（指标体系、架构、算法）
- [TEST_REPORT.md](TEST_REPORT.md) — 测试验证报告（用例与视图执行结果）

A background worker samples the logical replication pipeline into
shared memory ring buffers; SQL views derive rates at query time.
Deploy on the publisher, the subscriber, or both.  On the subscriber
it also polls each subscription's publisher (reusing the subscription
conninfo), so `pg_lrstat_overall` shows both ends without installing
anything on the publisher.

## Quick start

    # postgresql.conf
    shared_preload_libraries = 'pg_lrstat'

    CREATE EXTENSION pg_lrstat;
    SELECT * FROM pg_lrstat_info;        -- loaded / health / config
    SELECT * FROM pg_lrstat_overall;     -- both ends, one row per subscription

## Tests

`make check` / `meson test postgresql:pg_lrstat/regress` runs the
deterministic regression suite (the server is started with
shared_preload_libraries via --temp-config).  TAP 002 runs a real
publisher/subscriber pair and needs Perl IPC::Run.  See TEST_REPORT.md
for local-macOS caveats (install-name patching without an installed
prefix).
