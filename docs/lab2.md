# Lab2

> 开始时间：2022/4/7
>
> 结束时间：2022/4/7
>
> 文档撰写时间：2022/11/8

Lab2 主要是让我们实现各类迭代运算符以过滤元组，例如实现条件过滤、连接过滤、排序、分组聚合、插入删除等，我们需要理解这些迭代器是怎么用的。

Simpledb 运算符实现的顶层接口是一个迭代器 `OpIterator`，这个迭代器将对元组进行迭代，其拥有的子类如下图：

<img src="https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211081829354.png" alt="image-20221108182856221" style="zoom: 80%;" />

我们要理解运算符与迭代器是如何工作的，参考官方的一个例子：

```sql
SELECT *
FROM some_data_file1, some_data_file2
WHERE some_data_file1.id > 1
  AND some_data_file1.field1 = some_data_file2.field1
```

这个查询包括几个步骤：

1. 全表扫描，这需要用到实验一实现的 `SeqScan` 运算符，这会产生两个迭代流。
2. 对 `some_data_file1.id > 1` 表达式进行过滤，这会构建一个 `Predict` 进行预言，并赋给 `Filter` 进行过滤，这会产生一个新的迭代流。
3. 将这个新的迭代流与另一个表的 `SeqScan` 迭代流闯入 `Join` 运算符，`Join` 需要 `JoinPredict` 进行过滤，产生最终的 `Join` 迭代流。

然后返回给 `Project` 作为结果。

Simpledb 中还有聚合迭代流 `Aggregate`，这个迭代流将返回一些特殊的元组，它只包含两个字段：一个分组字段（如果有的话），一个聚合字段。

例如对于 SQL 语句：

```sql
SELECT SUM(age), country FROM table GROUP BY country
```

聚合运算符只会返回一系列只有两个字段的元组，因为在分组查询中，非聚合字段都是无意义的。如果查询同时存在聚合和非聚合字段，这是非法的查询。

Simpledb 中似乎只支持一个字段的聚合，后面可以再观望一下。

## 练习一

练习一要求我们实现如下运算符：

- src/java/simpledb/execution/Predicate.java
- src/java/simpledb/execution/JoinPredicate.java
- src/java/simpledb/execution/Filter.java
- src/java/simpledb/execution/Join.java

如果理解了整体架构的话，实现并不复杂。

对于连接操作，通常有 **循环嵌套法、哈希集合法、排序法** 三种策略，其中后两种策略虽然之间复杂度更优，但是需要提前加载整个表到内存中预处理，对内存支持不好。

循环法很好理解，对于哈希集合法，需要预加载外表到内存中，以字段值为键，值是一些元组的集合，然后迭代内表的元组，到哈希表中匹配，如果是范围检索，可以通过 `TreeMap` 完成这一目的。

排序法得分两种情况，如果外表的值不允许重复，那么在等值情况下可以使用**双指针法**进行比较，这会非常高效；但如果存在重复的键，可能需要通过**二分查找**来确定边界。

通常，哈希法相较排序法而言，会占用更多的内存，时间复杂度较优，但常数比较大。

在这个实验中，我仅仅只使用了循环嵌套法完成，因为它适用于所有的情况，如果使用哈希集合法，`JoinPredicate` 谓词将得不到任何作用。

官方提供了一个在 `Equals` 情况下的哈希法匹配，可参考 `simpledb.execution.HashEquiJoin`。

## 练习二

实现聚合运算。这并不是一件难事。

## 练习三

实现 `HeapPage` 和 `HeapFile` 中的插入和删除元组，`HeapPage` 中的插入和删除是操作内存中的页头和元组，`HeapFile` 的插入和删除将调用缓冲池获取页并插入，**当没有足够空间时，需要新建一个页。**

`HeapFile` 以读写模式锁定获取的页，**在这个实验中，暂时不需要考虑释放锁的逻辑，但我们得留个心眼。**

这个练习的难点在于 `HeapFile` 要获取一个有足够空闲空间的页插入元组，在我的实现中，我仅仅只是简单的遍历所有页来进行判断，这种性能效率是**非常糟糕**的，甚至可能会长时间堵塞。

一个更好的办法是实现一个空闲空间管理策略，以便快速的、**线程安全的**分配一个空闲页面，这个策略在我们自己实现的数据库中实现。

这里有一个比较绕的地方，所有对外插入和删除元组操作都应该调用 `BufferPool` 接口去操作，`BufferPool` 调用了 `HeapFile` 的接口，`HeapFile` 又调用了 `BufferPool` 获取页，最后调用 `HeapPage` 的接口。

个人感觉设计的不是很合理，个人认为 `HeapFile` （或者是其他类）应该负责空闲页面管理，`BufferPool` 请求一个空闲页，调用页的接口插入似乎更合理些。

## 练习四

练习四要求我们实现 `Insert` 和 `Delete` 运算符，这个迭代器将数据源在表中插入或删除，**最终返回一个影响的行数。**

这里调用 `BufferPoll` 的接口插入或删除就行，有个坑是迭代器只有第一次调用有用，返回一个单字段的元组，字段值是影响的行数，后续无法继续调用，注意下即可。

## 练习五

实现 `BufferPool` 的驱逐策略，这是个操作系统里很经典的八股了：FiFo、SC、LRU、LFU、NRU、NFU、Clock、Againg.....

挑一个喜欢的实现即可，LRU 是公认效果最好的算法，实现 LRU 在力扣上也有，也是属于高频了。

Java 里建议直接使用 `LinkedHashMap` 来实现 LRU 策略：

```java
public BufferPool(int numPages) {
    this.numPages = numPages;
    this.pagePool = new LinkedHashMap<PageId, Page>(numPages, 0.598f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
            if (this.size() > numPages) {
                Page page = eldest.getValue();
                if (page.isDirty() != null) {
                    try {
                        flushPage(eldest.getKey());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }
            return false;
        }
    };
}
```

但是这里要考虑一个点，我们在刷新缓存落盘时，如果同时有其他事务正在写页面怎么办，会不会存在**不一致**的问题？

例如事务在此页面上正在插入一个元组，这包括：设置页头和赋值元组，如果事务成功设置了页头，但是在赋值元组之前，我们将页面落盘了，这将会导致一些严重的问题。

这有一些解决方案，例如落盘前先获得页面的写锁，但这可能会导致堵塞。

好在，这个 Lab 暂时不需要考虑锁的问题，但我们必须要思考这个问题。