# 实验三：并发控制

## 任务一：Lock Manager
为了确保正确交叉事务的操作，DBMS将使用锁管理器（LM）来控制何时允许事务访问数据项。

LM的基本思想是它维护一个关于活动事务当前持有的锁的内部数据结构。

然后，事务在允许访问数据项之前向LM发出锁定请求。LM将授予对调用事务的锁定，阻止该事务或中止它。

此任务要求您实现支持两阶段锁定（2PL）和严格两阶段锁定（S2PL）的元组级LM 。

## 知识笔记
### Two-phase locking - 2PL
简单来说就是，一个事务分为两个阶段，上升阶段(获取锁)和下降阶段(释放锁)，在上升阶段不能释放锁，在下降阶段不能获得锁。

这样就保证了冲突可串行化，但是不能保证不会发生死锁，所以我门采用下面的方法来预防死锁。

### Strict Two-phase locking - S2PL
在2PL的基础上，还要求事务持有的所有排他锁必须在事务提交后方可释放。

这个要求保证未提交事务所写的任何数据在该事务提交之前均以排它锁方式加锁，防止其他事务读这些数据。

### wait-die
Lock Manager 将实施WAIT-DIE策略以预防死锁
+ 该方案是基于非剥夺方法。当进程Pi请求的资源正被进程Pj占有时，只有当Pi的时间戳比进程Pj的时间戳小时，
即Pi比Pj老时，Pi才能等待。否则Pi被卷回（roll-back），即死亡。
+ 一个进程死亡后会释放他所占用的所有资源。在这里假定死亡的进程将带着同样的时间戳重新运行。
+ 由于具有较小时间戳的进程才等待具有较大时间戳的进程，因此很显然死锁不会发生。当进程在等待特定的资源时，不会释放资源。
+ 一旦一个进程释放一个资源，与这个资源相联系的等待队列中的一个进程将被激活。


### Lock Manager
锁管理器可以实现为一个过程，它从事务接受消息并反馈消息。锁管理器过程针对锁请求消息返回授予锁消息，或者要求事务回滚的消息。
解锁消息只需要得到一个确认回答，但可能引发为其他等待事务的授予锁消息。

锁管理器为目前已加锁的每个数据项维护一个链表，每一个请求为链表中一条记录，按请求到达的顺序排序。

它使用一个以数据项名称为索引的散列表来查找链表中的数据项，叫锁表。

![](https://github.com/liu-jianhao/CMU-15-445/blob/master/Lab3-Concurrency-Control/%E9%94%81%E8%A1%A8.png)

上图包含五个不同数据项的锁。已授予锁的事务用深色阴影方块表示，等待授予锁的事务用浅色方块。

除了图上看到的，锁表还应当维护一个基于事务表示符的索引，这样它可以有效地确定给定事务持有的锁集。


锁管理器处理请求：
+ 当一条请求消息到达时，如果相应数据项的链表存在，在该链表末尾增加一个记录；否则，新建一个包含该请求记录的链表。
+ 收到一个事务的解锁消息是，它将与该事务相对应的数据项链表中的记录删除，然后检查随后的记录，如果有，检查该请求能否被授权
+ 如果一个事务中止，锁管理器删除该事务产生的正在等待加锁的所有请求。
这个算法保证了锁请求无饿死现象，因为在先前接受到的请求正在等待加锁时，后来者不可能获得授权。

## 任务一调试
任务一还算比较简单，主要先将锁表的数据结构想好，之后的操作基本上就水到渠成了，但毕竟是多线程的并发，难免会有问题。
```
test/lock_manager_test
Running main() from gmock_main.cc
[==========] Running 9 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 9 tests from LockManagerTest
[ RUN      ] LockManagerTest.BasicTest
[       OK ] LockManagerTest.BasicTest (1 ms)
[ RUN      ] LockManagerTest.BasicShareTest
[       OK ] LockManagerTest.BasicShareTest (100 ms)
[ RUN      ] LockManagerTest.BasicExclusiveTest
[       OK ] LockManagerTest.BasicExclusiveTest (101 ms)
[ RUN      ] LockManagerTest.BasicUpdateTest
^C
```
能通过三个测试，但是最后一样还是卡住了。

找了快一个小时，终于找到了：
```cpp
if(it == lock_table_[rid].list.begin() || it->mode == LockMode::EXCLUSIVE)
{
  cond.notify_all();
}

lock_table_[rid].list.erase(it);
```
这个是改过之后的，之前错误的代码是将erase语句写在了其上面的if语句之前，因为erase会让迭代器失效，所以后面的操作肯定就是错误的了。

然后就通过测试了：
```
[==========] Running 9 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 9 tests from LockManagerTest
[ RUN      ] LockManagerTest.BasicTest
[       OK ] LockManagerTest.BasicTest (0 ms)
[ RUN      ] LockManagerTest.BasicShareTest
[       OK ] LockManagerTest.BasicShareTest (101 ms)
[ RUN      ] LockManagerTest.BasicExclusiveTest
[       OK ] LockManagerTest.BasicExclusiveTest (100 ms)
[ RUN      ] LockManagerTest.BasicUpdateTest
[       OK ] LockManagerTest.BasicUpdateTest (101 ms)
[ RUN      ] LockManagerTest.BasicTest1
[       OK ] LockManagerTest.BasicTest1 (100 ms)
[ RUN      ] LockManagerTest.BasicTest2
[       OK ] LockManagerTest.BasicTest2 (200 ms)
[ RUN      ] LockManagerTest.DeadlockTest1
[       OK ] LockManagerTest.DeadlockTest1 (0 ms)
[ RUN      ] LockManagerTest.DeadlockTest2
[       OK ] LockManagerTest.DeadlockTest2 (1 ms)
[ RUN      ] LockManagerTest.DeadlockTest3
[       OK ] LockManagerTest.DeadlockTest3 (500 ms)
[----------] 9 tests from LockManagerTest (1104 ms total)

[----------] Global test environment tear-down
[==========] 9 tests from 1 test case ran. (1104 ms total)
[  PASSED  ] 9 tests.
```

## 任务二：并发索引

### 提示
+ 不能使用全局范围锁存器来保护你的数据结构，也就是说，无法锁定整个索引，只能在操作完成时解锁锁存器。
+ 提供了读写锁存器的实现`src/include/common/rwmutex.h`。并且已经在文件下添加了辅助函数来获取和释放`latch`(`src/include/page / page.h`)
+ 必须使用名为`transaction`的传入指针参数。它提供了在遍历B +树时存储已获取锁存页面的方法，以及存储在`Remove`操作期间删除的页面的方法。我们的建议是仔细查看`FindLeafPage()`B+树中的方法，你可能想改变以前的实现并在这个特定方法中添加`latch crabbing`的逻辑。
+　缓冲池管理器中`FetchPage`的返回值是一个指向`Page`的实例`src/include/page/page.h`的指针。你可以获得一个Page的锁，但你不能获得B+Tree节点上的锁（既不是内部节点也不是叶子节点）

### 常见的问题
+ 仔细考虑`UnpinPage(page_id, is_dirty)`缓冲池管理器类中的`UnLock()`方法与页面类中的方法之间的顺序和关系。在从缓冲池取消固定同一页面之前，必须释放该页面上的锁存器。
+ 如果你正确地实现并发B+树索引，那么每个线程将始终从根到底获取锁存器。释放闩锁时，请确保遵循相同的顺序（也称为从根到下）以避免可能的死锁情况。
+ 其中一个例子是插入和删除时，成员变量`root_page_id（src/include/index/b_plus_tree.h）`也将被更新。您有责任防止此共享变量的并发更新（提示：在B +树索引中添加一个抽象层，您可以使用std::mutex来保护此变量）


## 任务二调试
前面两次实验都需要花很多时间来调试，这次也不例外。。。
```shell
test/b_plus_tree_concurrent_test 
Running main() from gmock_main.cc
[==========] Running 6 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 6 tests from BPlusTreeConcurrentTest
[ RUN      ] BPlusTreeConcurrentTest.InsertTest1
[1]    1189 segmentation fault  test/b_plus_tree_concurrent_test
```
用`valgrand`检查得到下面结果(部分)：
```
==25834==    at 0x4F47E4A: cmudb::BPlusTreeInternalPage<cmudb::GenericKey<8ul>, int, cmudb::GenericComparator<8ul> >::Lookup(cmudb::GenericKey<8ul> const&, cmudb::GenericComparator<8ul> const&) const (in /home/liu/Desktop/CMU/CMU-15-445/Lab/build/lib/libvtable.so)
```
可以看到是在之前内部节点页面中的`Lookup`函数出了问题

检查一遍`Lookup`函数，猜测应该是没有处理边界条件，加上之后，再次编译运行
```shell
test/b_plus_tree_concurrent_test
Running main() from gmock_main.cc
[==========] Running 6 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 6 tests from BPlusTreeConcurrentTest
[ RUN      ] BPlusTreeConcurrentTest.InsertTest1
b_plus_tree_concurrent_test: /home/liu/Desktop/CMU/CMU-15-445/Lab/src/index/b_plus_tree.cpp:670: cmudb::BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* cmudb::BPlusTree<KeyType, ValueType, KeyComparator>::FindLeafPage(const KeyType&, bool, cmudb::Operation, cmudb::Transaction*) [with KeyType = cmudb::GenericKey<8>; ValueType = cmudb::RID; KeyComparator = cmudb::GenericComparator<8>]: Assertion `node->GetParentPageId() == parent_page_id' failed.
```
偶尔会出现上面的情况，或者直接卡死在第一个测试。
