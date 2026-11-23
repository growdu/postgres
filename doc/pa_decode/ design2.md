并行解码方案设计

背景
我们已经在v3里成功实现了pg的stream协议，当前架构大概这样子：

特点：通过stream prorocol实现发布端和订阅端完全解耦。发布和订阅可以不改任何代码的情况下任意组合，互相校验正确性
瓶颈分析
1.串行解码+串行apply（v3主线）
buffer chage在事务commit前缓存，等到commit时，从缓存文件（或者内存）里出出来，一条一条发送订阅端apply。订阅端不光解码是串行的，apply也是串行的，v3只有10M/s，pg大概有30M/s
2.串行解码+并行apply（pg16引入，v3正在做）
buffer change在内存里缓存，当内存装不下时，通过stream协议把该事务的一部分buffer change发送出去，订阅端起一个并行worker处理这个消息。到了这个架构，v3大约20-30M/s，pg大约50-60M/s（pg用FPI，有可能是虚高）。
这个架构的瓶颈：stream的粒度越小，订阅端并行度越高，速度理论上越快。同时，stream粒度越小，解码CPU开销越大
●stream粒度越小，解码端单核CPU成为瓶颈（解码端频繁的start transacion/stop transaction，开销巨大）
●stream粒度越大，解码端CPU不是瓶颈，但订阅端没法提升并行度，订阅成为瓶颈（基本都在等commit order)
3.并行解码+并行apply（正在开发）
原理：既然串行解码的CPU的瓶颈，做成并行，解除单核CPU瓶颈就，整个逻辑复制吞吐就可以上去了
实测效果：v3做到50M/s左右，解码端完全不是瓶颈，瓶颈转移到v3的IUD执行慢。如果订阅端只接收消息不做apply，实测解码端可以做到80M/s-90M/s以上，紧跟xlog生成速度，从CPU负载来看，解码远没到达瓶颈
og并行解码方案？
本节讲一下为什么不用og并行解码的方案，该方案的原理我方案不再赘述，详细可以参考：https://doc.weixin.qq.com/doc/w3_ASkAvQbiAGYCNWX4za2BqTEqZwIdw?scode=AHUAfwdSAA8tVhL6a1ASkAvQbiAGY 

考虑一个事务做了以下序列：insert1；update2；delete3；alter table；insert5；commit；假设有三个并行解码worker，则各个worker接收消息的顺序以下：

不需要太复杂的分析就能得到这个架构最致命的问题：消息被打散到各个worker，没有事务上下文的概念，而快照、invalid msg、toast、spec insert，cid、cmin/cmax、父子事务等消息的处理，高度依赖解码顺序和事务上下文的实现
考虑几个实际问题：
1、update2假如是个toast，toast被分片到各种worker，怎么重新组装起来？没法组装，只能在reader处完成，reader很快成为瓶颈
2、insert5前面做了DDL，读系统表时，CID要+1，怎么算出系统表的cmin/cmax的值？还是没法算，只能在reader处完成，再把上下文打包发给worker，reader成为瓶颈
3、insert5前面做了DDL，需要invalid msg刷新relcach，这个架构下还是没法正确处理invalid msg，于是worker每解码一个消息，就要清空掉所有relcache和syscache，系统表缓存几乎失效
4、解码虽是并行，但本质还是等到commit时，一条一条发给订阅端，订阅端为了保证commit order没法并行，速度提升有限
5、没法提前感知事务已经结束。假设这个事务最终abort，我们在解码第一个insert1的时候即可以把整个事务上下文标记为abort，后面来的buffer chage立即丢弃

长存项目为了提升订阅端的并发，把commit时才发送出去，修改成看到buffer change立即发出去，这又引入了新的问题：
unique索引做insert，事务最终abort。直接把insert发出去订阅端，会引发订阅端唯一键冲突。由于没有事务上下文的概念，没法给这个事务标记为’abort‘

上面的问题，在OG方案里都有一些方法解决，或者绕过去，但实现非常难看，架构变形，BUG非常多，最关键一点：每改一行代码，订阅端都得跟着适配，output插件也要跟着适配，工作量巨大。不像pg的实现，通过stream通讯协议实现两端完全解码，一端根本不关键另一端怎么实现，两端都只关心自己怎么跟stream协议对接
新方案
新方案非常简单直接：woker按top_xid的维度拆分

●reader：按top_xid来分发事务到指定worker
●worker：只看到部分top_xid，跟其它woker完全隔离，可以看到本top_xid完整的事务上下文----代码完全复用串行解码，几乎不需要入侵串行解码的实现，相当于一个串行解码的worker
●walsender：把最终报文发送出去，并保证commit order
原理
考虑两个不同事务，修改同一个tuple，生成以下xlog序列：
t1 update; t1 commit; t2 update; t2 commit;
reader负责把这两组事务，按top_xid分发到不同worker，woker看到的消息序列：
●t1 update；t1 commit；
●t2 update;  t2 commit；
worker完全复用串行解码的流程，取快照，然后把update解码出来，转发给walsender
walsender先发t1，再发t2 ，保证commit order

每个worker只处理一部分事务，woker可以按CPU水平扩展！

实现细节
原理非常简单，本节讲一下具体怎么做到上面的效果
快照
这个方案核心之一，就是让worker取到正确的快照。解答这个问题，我看看看快照里有什么
●xmin：同MVCC的xmin，通过解码running_xact得到
●xmax：同MVCC的xmax，也是通过解码running_xact得到
●xip：历史快照的xip，是修改过系统表（历史快照只用来读系统表，其它不改修改系统表的事务完全不用管），且commit的事务
正在运行的事务：在xmin和xmax范围内，xip是已commit事务，做集合差，即可得到正在运行的事务（跟MVCC语义一样）
结总核心就两点：
1.running_xact需要对所有worker广播，让每个worker正确推进xmin、xmax取值
2.修改过系统表，且commit事务，把它广播到所有woker，woker就能拿到正确的xip（事务里看到NEW_CID、INVALID_MSG这两个xlog，说明它改过系统表，reader标记一下，在它commit时，把commit信息广播出去，commit 里携带了invalid msg，invald msg的问题也解决了）
snapshotbuild的序列化和restore
直接给结论，我们在CONSISTENT状态后才启动并行解码，不涉及。worker看到广播的running_xact消息，直接得初始状态的xmin、xmax、xip，而我们总是restart_lsn开始解码的，restart_lsn又总是running_xact纪录。所以woker看到restart_lsn就正确完成初始化了

由于我们不能在woker里做snapshotbuild的序列化和restore，因此，tablesync阶段，都无法用并行解码（这里跟原来一样，一个tablesync对应一个walsender）

catalog_xmin、restart_lsn的推进
复制槽属于walsender，woker里没有真正的复制槽，woker收到广播的running_xact，更新本地的shapshotbuild状态，然后转发给walsenderwalsender收到所有woker发来的running_xact，说明所有woker的进度已经过了该水位，更新candidate_xmin_lsn、candidate_catalog_xmin，后续就和串行一样了。细节可以参考：https://doc.weixin.qq.com/doc/w3_ASkAvQbiAGYCNcQ7w00kOQg0yOUQM?scode=AHUAfwdSAA8jUNpecoASkAvQbiAGY 

commit order
本设计核心之一，考虑有两个事务产生序列：t2 delete; t1 update; t1 abort; t2 update; t2 commit，其中：
1.t2 delete 修改一个跟t1没有交集的tuple
2.t1 update，t2 update，修改的是同一个tuple

两个worker分别解码，发送到walsender的消息队列。我们看到abort、commit时，增加一种特殊的屏障标记，叫commit_sync。

状态1：
此时walsender看到队列长这样：
t1 update; t1 abort; commit_sync1; commit_sync2;
t2 delete；commit_sync1; t2 update; t2 commit; commit_sync2;

状态2：
t2 delete和t1事务完全没有交集，谁先发谁后发没有关系，我们假设t1 update先发、t2 delete跟随，这时队列状态如下：
 t1 abort; commit_sync1; commit_sync2;
commit_sync1; t2 update; t2 commit; commit_sync2;

状态3：
这时我们看到commit syn1，必须让commit_sync前面的xlog先发走（如果有多个xlog，谁先谁后，没有关系，因为它们不产生交集，同情况1是一样的），此时我们看到队列长这样：
commit_sync1; commit_sync2;
commit_sync1; t2 update; t2 commit; commit_sync2;

此时，队首是两个同相的commit_sync，直接把它弹走。重新回到状态1、2、3其中之一，此时队列长这样：
commit_sync2;
 t2 update; t2 commit; commit_sync2;

就这样一直循环下去

我们在订阅端将会看到这样的序列：
t1 update；t2 delete；t1 abort; t2 update; t2 commit; 或
t2 delete；t1 update；t1 abort; t2 update; t2 commit;       ----(t2 delete 、t1 update没有交集，谁先谁后没有关系，它们可以并发)

最关键的地方：t1 abort把 t1 update、 t2 update 这两个有交集的操作隔开，保证了commit order，不会因为乱序产生死锁和数据不一致

stream流
在stream协议里
●stream start、stream stop间，是同一个事务的一连串消息
●begin; commit间，也是同一个事务的一连串消息
上一章，我们为了简化讨论，把stream流用了一个updatet简化操作代替了，实际上，stream协议里，不会单独一个update/deleteI消息，都是连续的stream流，在stream期间不能有其它事务穿插。真实的消息队列长这样（commit order算法不变）：
stream1; t1 abort; commit_sync1; commit_sync2;
stream2；commit_sync1; stream2; t2 commit; commit_sync2;




