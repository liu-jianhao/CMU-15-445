# 实验2 B+ Tree

## B+ Tree知识笔记
+ 平衡的
+ 叶节点中的键是数据文件中的键的拷贝，这些键以排好序的形式，从左到右分布在叶节点中
+ 根节点中至少有两个指针被使用。所有指针指向位于B-Tree下一层的存储快
+ 叶节点中，最后一个节点指向它右边的下一个叶节点。在叶节点的其他n个指针中，至少有(n+1)/2个指针被使用
+ 在内层节点中，所有的n+1个节点都可以用来指向B-Tree中下一层的块，至少有(n+1)/2个指针被使用

![](https://github.com/liu-jianhao/CMU-15-445/blob/master/Lab2-B-Tree/images/DeepinScreenshot_select-area_20180924103212.png)

## 实验简介
