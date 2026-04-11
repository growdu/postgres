1.使用root用户执行如下编译

```shell

meson setup build \
  -Dssl=openssl \
  -Dlibxml=enabled \
  -Dlibxslt=enabled \
  -Dzlib=enabled \
  --prefix=`pwd`/debug 

```

执行生成：

```shell
meson compile -C build
```

编译完成后安装：

```shell
meson install -C build
```

2. 执行如下命令拷贝二进制到/tmp

```shell
/tmp/debug/bin/pg_ctl -D /tmp/debug/bin/pub_data/ stop
/tmp/debug/bin/pg_ctl -D /tmp/debug/bin/scrib_data/ stop
rm -rf /tmp/debug
cp -r debug /tmp/
chown dys /tmp/debug -R
```

后续操作都需要切换到dys用户执行。

3. 初始化发布端和订阅端数据库

```shell
cd /tmp/debug/bin/
/tmp/debug/bin/initdb -D pub_data -A trust
/tmp/debug/bin/initdb -D scrib_data -A trust
```

4.修改数据库端口和wal_level

```shell
echo "port=12345" >> /tmp/debug/bin/pub_data/postgresql.conf
echo "wal_level='logical'" >> /tmp/debug/bin/pub_data/postgresql.conf

echo "port=12346" >> /tmp/debug/bin/scrib_data/postgresql.conf
echo "wal_level='logical'" >> /tmp/debug/bin/scrib_data/postgresql.conf
```

5.  启动数据库

```shell
/tmp/debug/bin/pg_ctl -D /tmp/debug/bin/pub_data/ -l logfile start
/tmp/debug/bin/pg_ctl -D /tmp/debug/bin/scrib_data/ -l logfile1 start
```

6. 分别连接发布端和订阅端创建users表

```shell
/tmp/debug/bin/psql -d postgres -p 12345
```

```sql
create table users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) not NULL,
    email VARCHAR(255) UNIQUE
);
```

```shell
/tmp/debug/bin/psql -d postgres -p 12346
```

```sql
create table users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) not NULL,
    email VARCHAR(255) UNIQUE
);
```

7. 创建发布者

```shell
/tmp/debug/bin/psql -d postgres -p 12345
```

```sql
create publication ddl_test_pub for table users with (ddl ='table,index');
```

8. 创建订阅

```shell
/tmp/debug/bin/psql -d postgres -p 12346
```

```sql
create subscription ddl_test_sub 
connection 'host=localhost port=12345 dbname=postgres user=dys'
publication ddl_test_pub
with (
    copy_data = true,
    enabled = true,
    ddl = 'table,index',
    streaming = on,
    binary = false
);
```

9.在发布端插入数据

```shell
/tmp/debug/bin/psql -d postgres -p 12345
```


```sql
insert into users(name,email) values
('tom','tom@qq.com');
```


10. 在发布端更新表结构

```shell
/tmp/debug/bin/psql -d postgres -p 12345
```


```sql
alter table users add column phone VARCHAR(20);
```

11. 在发布端插入数据

```shell
/tmp/debug/bin/psql -d postgres -p 12345
```


```sql
insert into users(name,email,phone) values
('alice','test@qq.com','123-456-789'),
('bob','bob@qq.com','123-456-788');
```

12. 观察订阅端数据是否同步，包括列是否增加，插入数据是否一致。
