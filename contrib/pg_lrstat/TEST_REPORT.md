# pg_lrstat 测试验证报告

自动化验证：TAP 测试 `t/001_tablesync.pl`（`meson test pg_lrstat/001_tablesync`，需 `-Dtap_tests=enabled`），26 项断言，覆盖 table sync worker 全生命周期、按名导出、Evidence 佐证表、未知会话报错。

以下为 2026-09-29 双节点集群（pub+sub，逻辑订阅，采样 1s）的人工验证记录。

## 场景

`lrstat_start(true)`（自动名 report_verify 场景沿用自定义名仅便于文中引用；start/stop 均无参数）→ 8 个独立 UPDATE 事务（101 行 × 28KB，间隔 0.7s）→ `lrstat_stop()`。会话窗口 11 秒，发布端实际生成 WAL 0.63 MB（LSN 实测 0/8EE96F0→0/8F8AFC8）。

## 六张视图逐字段核验

| 视图 | 会话窗口行数 | 核验要点 | 结果 |
| --- | --- | --- | --- |
| info | 1 | 会话状态/起止/归档列表（10 个历史会话名） | ✅ |
| send_stat | 11（每轮一行） | retained_wal 0.1→0.6MB 随负载增长；水位逐轮推进；负载轮速率 0.071~0.145、空闲轮 0；flush_lag 在提交轮跳 0.40s | ✅ |
| recv_stat | 12（每轮一行） | received==applied 同步推进；速率负载轮 0.071~0.152；首行 apply_blocked=t（启动积压）次轮恢复 | ✅ |
| cluster_stat | 12（配对每轮一行） | remote_state=ok 全程；26:01 轮瞬时 bottleneck=send（当轮 gen>snd 且有未发送积压）次轮恢复 none | ✅ |
| send_history | 11 × 11 字段 | 7 个 LSN 水位 + spill/stream；confirmed 落后 sent（确认延迟） | ✅ |
| recv_history | 12 × 5 字段 | applied/received 同步；local_wal 独立推进 | ✅ |

## 报告准确性核对（可验证性）

| 核对项 | 报告值 | 独立核算 | 结果 |
| --- | --- | --- | --- |
| apply_avg | 0.0518 | history 窗口函数手算 = 0.0518 | ✅ 逐位一致 |
| 端到端闭合 | — | recv applied 差 0.63MB == pub LSN 实测 0.63MB | ✅ |
| 50G / 100G / 200G 推算 | 274.49h / 548.99h / 1097.98h | 51200/0.051813/3600=274.5h；严格 1:2:4 倍 | ✅ |
| 逐轮速率 | recv_stat apply_mbps | history lag() 手算同 ts 行 2.2231 == 2.2231（另一次负载，4 位小数） | ✅ |
| HTML Targets 卡 | recv 0.05 / apply 0.05, last LSN 0/8F8AFC8 | 与视图末轮水位一致 | ✅ |
| Evidence 区 | 四表 42 行 | send/recv history + send/recv rate 与原始数据一致 | ✅ |

## 附加验证

- **持久化路径**：核对中途实例重启（内存环形清空）后 `lrstat_export('report_verify')` 仍从 persist 文件完整重建报告 ✅
- **修复记录**：JSON 速率字段由 2 位升至 4 位小数——2 位时外部复核容量推算存在舍入歧义（0.05 vs 内部 0.051813）。

## 已知边界（非缺陷）

- 大事务期间 applied 停在提交边界、提交时一次跳变——内核语义，速率按提交时刻归属。
- RSEND 轮询样本的 state/sender_pid 为空（远端查询不含），发送端直连部署时 SendStat 有完整值。
- stat 三视图读内存环形，实例重启后为空；persist 会话经 `lrstat_export(name)` 从文件重建。
