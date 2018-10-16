# 实验2 B+ Tree

## B+ Tree知识笔记
+ 平衡的
+ 叶节点中的键是数据文件中的键的拷贝，这些键以排好序的形式，从左到右分布在叶节点中
+ 根节点中至少有两个指针被使用。所有指针指向位于B-Tree下一层的存储快
+ 叶节点中，最后一个节点指向它右边的下一个叶节点。在叶节点的其他n个指针中，至少有(n+1)/2个指针被使用
+ 在内层节点中，所有的n+1个节点都可以用来指向B-Tree中下一层的块，至少有(n+1)/2个指针被使用

![](https://github.com/liu-jianhao/CMU-15-445/blob/master/Lab2-B-Tree/images/B-Tree-Node.png)
![](https://github.com/liu-jianhao/CMU-15-445/blob/master/Lab2-B-Tree/images/DeepinScreenshot_select-area_20180924103212.png)

## 实验简介



### 任务一：B+Tree页面
实现三个类：
1. B+Tree Parent Page(父类)
2. B+Tree Internal Page
3. B+Tree Leaf Page

要特别注意的一点：

每个B+树叶/内部页面对应于缓冲池提取的存储器页面的内容（即字节数组data_）。因此，每次尝试读取或写入叶子/内部页面时，都需要首先使用其唯一的page_id从缓冲池中获取页面，然后 将其转换为叶子或内部页面，并在任何写入或读取后取消固定(Unpin)页面操作。例如：
```cpp
auto *page = buffer_pool_manager->FetchPage(ValueAt(i));
auto child = reinterpret_cast<BPlusTreePage *>(page->GetData());
// 一些读取页面或修改页面的操作
buffer_pool_manager->UnpinPage(child->GetPageId(), true);
```
上面这几行代码在整个实验二中用的特别多，做的事很简单，就是从缓冲区管理器中取得一个页面，
然后用`reinterpert_cast`转换为一个实验二中的内部页面或叶子页面，接着就可以对这个页面读取或修改了，
最后要`Unpin`掉这个页面。

+ 父类肯定是要先实现的，而且也最简单，重要的是了解父类中有什么方法，因为这些方法是内部节点页面和叶子节点页面共用的。
+ 接着实现内部节点页面，难点是插入一个节点之后遇到的节点页面要分裂的情况和删除一个节点之后节点页面需要合并或重分配的情况。
+ 最后实现叶子节点页面，和内部节点页面类似，叶子节点也可能需要分裂或合并或重分配。和内部节点页面不同的是，叶子节点页面的`ValueType`是`record_id`，
所以在模板特化的时候要注意。

### 任务二：B+Tree数据结构
任务二实际就是将任务一完成的页面用B+Tree数据结构串起来，这个真正的B+Tree，也就是整个实验二的重点和难点。

+ 由于B+Tree需要分裂、合并和重分配的操作，而且这些操作是递归的，
即叶子节点页面的分裂可能会导致其父亲节点页面的分裂，以此类推，所以实现的是后要特别注意。
而且还要注意某些特殊情况，如该页面是否是根节点页面。
+ 当需要节点分裂时，需要new一个新页面，这时候要调用它们的`Init`函数

### 任务三：索引迭代器
最后一个任务，起到了辅助的作用，为了方便迭代数据，不难。


## 调试阶段
写了两天才将这三个任务写完了，但最难的地方还没有到来——调试。

```shell
$ make all b_plus_tree_test
```
出现了一大堆错误，修改完之后编译通过，运行程序：
```shell
$ test/b_plus_tree_test                                                                                                       
Running main() from gmock_main.cc
[==========] Running 5 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 5 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
2018-10-13 20:19:50 [src/disk/disk_manager.cpp:96:ReadPage] DEBUG - Read less than a page
```
程序就一直卡在这里，`disk_manager`是实验一里面`buffer pool manager`这个类有用到这个类，说明实验一的坑在实验二中出现了。再仔细看看`b_plus_tree_test.cpp`，错误应该是在插入第一个节点时就出现了问题。

继续查看实验一写的代码，发现没什么问题，又看了`b_plus_tree.cpp`中的`Insert`，里面调用了`StartNewTree`，发现这个函数第一句调用错了函数，
本应该调用`NewPage`结果写成了`FetchPage`，难怪会出错。

虽然解决了第一个错误，但接下来还有更多的错误。继续debug.......

虽然有很多错误，但是从第一个开始看起：
```
$ test/b_plus_tree_test
Running main() from gmock_main.cc
[==========] Running 5 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 5 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
[       OK ] BPlusTreeTests.InsertTest1 (0 ms)
[ RUN      ] BPlusTreeTests.InsertTest2
/home/liu/Desktop/CMU/CMU-15-445/Lab/test/index/b_plus_tree_test.cpp:111: Failure
Value of: 1
Expected: rids.size()
Which is: 0
```
`rids`是在树叶节点页面才使用的，所以应该是树叶节点插入有问题，或者计数有问题。

果然不出所料，在树叶节点页面的`Insert`方法中在其中一种情况中忘了要插入键值对....

改了之后还有错误：
```
test/b_plus_tree_test
Running main() from gmock_main.cc
[==========] Running 5 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 5 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
[       OK ] BPlusTreeTests.InsertTest1 (0 ms)
[ RUN      ] BPlusTreeTests.InsertTest2
[       OK ] BPlusTreeTests.InsertTest2 (0 ms)
[ RUN      ] BPlusTreeTests.DeleteTest1
[       OK ] BPlusTreeTests.DeleteTest1 (1 ms)
[ RUN      ] BPlusTreeTests.DeleteTest2
/home/liu/Desktop/CMU/CMU-15-445/Lab/test/index/b_plus_tree_test.cpp:297: Failure
Value of: current_key
  Actual: 2
Expected: location.GetSlotNum()
Which is: 1
/home/liu/Desktop/CMU/CMU-15-445/Lab/test/index/b_plus_tree_test.cpp:296: Failure
Value of: 0
Expected: location.GetPageId()
Which is: 1
```
错误一直循环下去直到程序崩了，检查了两天发现原来从别人那里拷贝来的测试实验代码好像有些问题，换了之后虽然程序还有bug，但没有一直循环出现错误。
```
$ test/b_plus_tree_test
Running main() from gmock_main.cc
[==========] Running 11 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 11 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
[       OK ] BPlusTreeTests.InsertTest1 (1 ms)
[ RUN      ] BPlusTreeTests.InsertTest2
[       OK ] BPlusTreeTests.InsertTest2 (1 ms)
[ RUN      ] BPlusTreeTests.InsertScale
b_plus_tree_test: /home/liu/Desktop/CMU/CMU-15-445/Lab/src/page/b_plus_tree_leaf_page.cpp:94: const std::pair<_T1, _T2>& cmudb::BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>::GetItem(int) [with KeyType = cmudb::GenericKey<8>; ValueType = cmudb::RID; KeyComparator = cmudb::GenericComparator<8>]: Assertion `0 <= index && index < GetSize()' failed.
[1]    25190 abort      test/b_plus_tree_test
```
可以看出是调用`GetItem(int index)`时index超出了范围，先查看哪里调用了这个函数：
```shell
$ grep GetItem * -nR
include/page/b_plus_tree_leaf_page.h:49:  const MappingType &GetItem(int index);
index/index_iterator.cpp:40:    return leaf_->GetItem(index_);
page/b_plus_tree_leaf_page.cpp:93:GetItem(int index) {
page/b_plus_tree_leaf_page.cpp:300:  MappingType pair = GetItem(0);
page/b_plus_tree_leaf_page.cpp:331:  MappingType pair = GetItem(GetSize()-1);
```
排除第一个和第三个函数的声明和定义，后两个肯定不会超范围，所以只能是在迭代器使用时出现了问题。

回过头继续检查，实在想不出来，只好参考别人的代码，发现需要锁？？
```cpp
page->RLatch();
buff_pool_manager_->FetchPage(leaf_->GetPageId())->RUnlatch();
```
加了之后果然测试全部通过了。
```
$ test/b_plus_tree_test                                                               master-!?
Running main() from gmock_main.cc
[==========] Running 11 tests from 1 test case.
[----------] Global test environment set-up.
[----------] 11 tests from BPlusTreeTests
[ RUN      ] BPlusTreeTests.InsertTest1
[       OK ] BPlusTreeTests.InsertTest1 (0 ms)
[ RUN      ] BPlusTreeTests.InsertTest2
[       OK ] BPlusTreeTests.InsertTest2 (0 ms)
[ RUN      ] BPlusTreeTests.InsertScale
[       OK ] BPlusTreeTests.InsertScale (266 ms)
[ RUN      ] BPlusTreeTests.InsertRandom
[       OK ] BPlusTreeTests.InsertRandom (402 ms)
[ RUN      ] BPlusTreeTests.DeleteBasic
[       OK ] BPlusTreeTests.DeleteBasic (0 ms)
[ RUN      ] BPlusTreeTests.DeleteScale
[       OK ] BPlusTreeTests.DeleteScale (2 ms)
[ RUN      ] BPlusTreeTests.DeleteRandom
[       OK ] BPlusTreeTests.DeleteRandom (24 ms)
[ RUN      ] BPlusTreeTests.DeleteTest1
[       OK ] BPlusTreeTests.DeleteTest1 (0 ms)
[ RUN      ] BPlusTreeTests.DeleteTest2
[       OK ] BPlusTreeTests.DeleteTest2 (0 ms)
[ RUN      ] BPlusTreeTests.ScaleTest
[       OK ] BPlusTreeTests.ScaleTest (395 ms)
[ RUN      ] BPlusTreeTests.RandomTest
[       OK ] BPlusTreeTests.RandomTest (675 ms)
[----------] 11 tests from BPlusTreeTests (1764 ms total)

[----------] Global test environment tear-down
[==========] 11 tests from 1 test case ran. (1765 ms total)
[  PASSED  ] 11 tests.
```

## 总结
又是将近一个月的时间，艰难的完成了实验二，看起来简单的一个B+Tree，居然需要那么多需要考虑的东西，自己还是眼高手低了。接下里准备实验三，并发控制。
