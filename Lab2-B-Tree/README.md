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

每个B+树叶/内部页面对应于缓冲池提取的存储器页面的内容（即字节数组data_）。因此，每次尝试读取或写入叶子/内部页面时，都需要首先使用其唯一的page_id从缓冲池中获取页面，然后 将其转换为叶子或内部页面，并在任何写入或读取后取消固定(Unpin)页面操作。






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
`b_plus_tree_test.cpp:111`之前一句是`tree.GetValue(index_key, rids);`，说明`GetValue`出了问题，
