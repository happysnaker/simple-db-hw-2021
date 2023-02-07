# Lab5

> 开始时间：2022/11/11
>
> 结束时间：2022/11/13
>
> 文档撰写时间：2022/11/14

实验四要求我们实现一个 B+ 树索引，这个实验已经开始上难度了，如果要自己实现一整个 B+ 树，估计得不吃不喝肝个一两周，好在，官方已经给我们写好了 80% 的代码了，只留下一部分核心的代码让我们写，属实是非常贴心了......

B+ 树数据结构：[B+树看这一篇就够了（B+树查找、插入、删除全上） - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/149287061)

B+ 树在线模拟：[B+ Tree Visualization (usfca.edu)](https://www.cs.usfca.edu/~galles/visualization/BPlusTree.html)

## 练习一

递归搜索可能包含某个值的最左侧页面，因为要求限定了最左页面，因此在与每个 `entry` 比较的时候，如果 key 小于等于 `entry` 的 key，应该递归搜索左子树，这样搜索到最终的结果一定时最左侧的页面。只有当 key 大于所有 `entry` 的值的时候，才会最终递归搜索最后一个节点的右子树。

必须要注意，这样搜索出来的结果可能是令人迷惑的，例如考虑下图：

![image-20221113181232457](https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211131812539.png)

当搜索 `5` 时，按照算法它会返回 `0001` 那个页面。所以我们在遍历时，应该要顺着叶子节点继续遍历，**直到碰到大于 key 的值。**

## 练习二

实现插入，如果插入时发现叶子节点已满，那么需要分裂页面。我们需要完成分裂的逻辑。

```java
if (leafPage.getNumEmptySlots() == 0) {
    leafPage = splitLeafPage(tid, dirtypages, leafPage, t.getField(keyField));
}
```

![img](https://img-blog.csdnimg.cn/8aa22640980646bb926e6551f92ad1f3.png)

分裂一个叶子页面需要创建一个新的右兄弟，修改双向链表指针，并将当前页的一半元组挪到新页中。然后，需要将中间节点提到父节点中去，并**修改自身父节点指向。**

在插入父节点时，可能会涉及到内部节点的递归分裂，官方的代码设计的很精妙，`splitInternalPage` 函数返回的是待插入的页面，官方也帮我们封装好了 `getParentWithEmptySlots` 方法，这个方法返回一个空闲的页，我们直接插入元素即可，无需关注具体的逻辑。

这个方法其实就是去判断父节点是否有空闲插槽，如果没有则分裂页面，返回空闲的页，感觉设计的非常好啊，分工非常明确。

```java
private BTreeInternalPage getParentWithEmptySlots(TransactionId tid, Map<PageId, Page> dirtypages,
                                                  BTreePageId parentId, Field field) throws DbException, IOException, TransactionAbortedException {

    BTreeInternalPage parent = ....;

    // split the parent if needed
    if (parent.getNumEmptySlots() == 0) {
        parent = splitInternalPage(tid, dirtypages, parent, field);
    }

    return parent;

}
```

内部节点的插入与叶子节点大体都是相同的，但也有些不同。

![内部节点](https://img-blog.csdnimg.cn/82c51044b947474992d127915ee42bea.png)

**中间节点提上去之后需要子节点就不需要在引用他了**，否则会出错，例如上图分裂如果不删除 6 的话，可参考下图：

![image-20221113184330713](https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211131843741.png)

会发现 `6` 的右指针的指向是错误的，而把 `6` 删除的话正好是正确的！下面删除也会有个父节点下沉操作，这块还是得自己画画图才能理清楚。

当页面分裂后，由于内部节点管了一群儿子，这些子节点必须更新他们的父指针！官方很贴心的给我们写好了辅助函数。

除此之外，内部节点的删除官方给了两种逻辑：`deleteKeyAndRightChild` 和 `deleteKeyAndLeftChild`，这里目前只需要删除 Right，但是下面窃取页面时需要用到 Left，要用哪个理解一下即可，调用错了将无法通过测试。

![image-20221113185256156](https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211131852197.png)

## 练习三

删除操作可能会使得页面变空，当一个页面小于半满时，他需要从兄弟节点窃取一些元素，或者与兄弟页面合并。练习三需要完成窃取的逻辑。

这里难理解的可能是内部节点的窃取，如图：

![image-20221113191135535](https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211131911581.png)

内部节点窃取需要将父节点下沉，然后将中间节点上移，并删除中间节点。删除中间节点这个上面已经讲了，父节点如果不下沉的话会导致错误结果，可能需要画图理解一下。这里尤其要注意父节点下沉后**左右指针的指向。**

## 练习四

练习四是页面的合并，这个相对简单，没有什么特别难理解的地方。

![image-20221113191303092](https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211131913138.png)

与兄弟节点合并后，删除兄弟页面，修改子节点的指向，然后删除父节点即可。

整体实验感觉实际难度并不算很大，可能就最开始需要理解一下。主要的原因感觉是官方喂的太多了，讲义上很多需要注意的点都讲明白了，而且绝大多数代码官方都已经写的差不多了.......