# 实验四：日志和恢复

第四个实验是在数据库系统中实现日志记录和恢复机制。

## 任务一：日志管理器 & 任务二：系统恢复

+ `include/logging/log_record.h`
  + 日志记录的基本结构
+ `logging/log_manager.cpp和logging/log_manager.h`
  + 刷新日志记录到磁盘
+ `table/table_page.cpp和include/logging/table_page.h`
  + 以元组为单位，构造日志项，并通过log_manager写入日志
+ `concurrency/transaction_manager.cpp和include/concurrency/transaction_manager.h`
  + 更新事务的lsn
+ `logging/log_recovey.cpp和include/logging/log_recovey.h`
  + 反序列化日志(即分析阶段)，重做、撤销
  
  
## 总结
第四次实验虽然改的文件多，但量并不大，比起上一次实验的并发控制简单多了。

虽然比上次实验简单，但是这次实验也有一些难点，首当其冲的就是log_manager，因为要实现后台线程和线程同步。

我采用的是最直观但不怎么简洁的办法，用条件变量控制线程，再用一个原子布尔变量来判断后台线程是否在运行。

任务二只要理解恢复算法就能很快写出来了。
