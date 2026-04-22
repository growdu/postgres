# TC_PARTITION_ATTACH

- Init Policy: `FAST_CLEANUP`
- Category: `partition`
- Scope: `N/A`
- DDL Option: `table` 或 `all`

## Purpose

验证分区表根表、子分区、后续 `ATTACH PARTITION` 的 DDL+DML 行为。

## Pre-steps

1. 执行 `../../bin/fast_cleanup.sh`
2. 执行 `../../bin/check_slots.sh`

## Steps

Publisher:

```sql
CREATE SCHEMA part_s;
CREATE TABLE part_s.orders (
  id bigint NOT NULL,
  bucket int NOT NULL,
  note text,
  PRIMARY KEY(id, bucket)
) PARTITION BY LIST (bucket);

CREATE TABLE part_s.orders_p1 PARTITION OF part_s.orders FOR VALUES IN (1);
CREATE TABLE part_s.orders_p2 PARTITION OF part_s.orders FOR VALUES IN (2);
CREATE TABLE part_s.orders_p3 (LIKE part_s.orders INCLUDING ALL);
ALTER TABLE part_s.orders ATTACH PARTITION part_s.orders_p3 FOR VALUES IN (3);
INSERT INTO part_s.orders VALUES (1001,1,'p1'), (1002,2,'p2'), (1003,3,'p3');
```

## Expected

1. 订阅端根表及分区对象存在
2. 三条数据均可查询

