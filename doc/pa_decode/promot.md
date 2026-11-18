当前的设计文档还存在多个问题，需要进行重新设计：1。修改只应该集中在发布端，不能动订阅端的机制；2。发布端并行解码进程应该分为walreader进程、decode                        
  worker进程、发送进程（walsender），各进程之间的关系为：walsender是主进程，负责整体流程，包括启动wal'reader、decode                                                       
  worker、发送和推进lsn，walreader进程负责读取wal并推动snapbuild达到consistent，然后将wal分发给decode worker，同时把提交顺序上报给walsender，decode                        
  worker解码完成后通知walsender可以发送，walsender需要保证commit的提交顺序（即commit/abort/cancel的顺序需要保证）；3。这里面最核心的点就是：要保证提交顺序，事务stream的时 
  候要尽快发送来提升性能，进程间通信调度应尽量减少内存拷贝，不要让调度成为新的性能瓶颈，缓存实效，系统表变更需要及时通知到decode                                           
  worker；4。保证顺序有两种方案：一种是commit barrier，另外一种是fifo队列（按照wal读取到的commit顺序排队），需要对比这两种方案的优劣； 根据这些信息进行补充完善 