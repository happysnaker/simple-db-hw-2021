# Lab6

这个实验是对练习四的补充，提出了在 STEAL 模式下的事务，在 STEAL 模式下，脏页可能会被刷新回磁盘，所以此时需要 `undo log` 来回滚操作。

Simpledb 中，在从磁盘读取页面时与刷新页面时，需要设置页面的 `old Data`，以便其他事务能够回滚到这个版本。

日志：

1. UPDATE_LOG。这种日志在 `flush` 页面时记录，日志会包含页面的之前的数据和 `flush` 时最新的数据。
2. COMMIT_LOG。包含提交日志。
3. CHECK_POINT_LOG。这种日志记录了检查点发生时，仍在活跃的事务集合，以及他们对应的第一条日志偏移。

有了这些日志，可以进行回滚和恢复。

## 练习一

实现 `rollback` 回滚，回滚需要逆序遍历日志，并将页面设置为 `beforeImage`，这里需要更新磁盘上的数据并驱逐缓存池中的页面，让其他事务从磁盘中读取回滚后的日志。

官方建议我们逆序遍历日志，但是这并不方便，我们可以顺序遍历，并维护一个页面集合，一旦我们之前处理过这个页面了，就忽略它。

## 练习二

练习二要求我们实现 `recover` 崩溃恢复。

在写这个练习时，我在思考为什么需要 `redo log`，因为事务提交时页面肯定被强制刷新了，他还是一种 `force` 模式。所以，一旦事务提交，可以保证磁盘上的页面是最新的，那么为什么需要 `redo` 呢？

自己想了想，原因是 Simpledb 的 `undo` 回滚是针对整个页面的，可能会存在版本的跳跃，举个例子，假设如下情况：

1. 事务 A 插入数据 x 并提交，日志为 `before(empty)、after(x)`
2. 事务 B 插入数据 y 并回滚，日志为 `before(x)、after(x)`
3. 事务 C 插入数据 z 并提交，日志为 `before(x)、after(x, y)`

因为事务 C 已经提交了，所以磁盘上的页面为 `(x, y)`，现在重启数据库进行恢复，如果不 `redo` 只 `undo` 的话，在 `undo` 事务 B 时，页面被回滚成 `(x)` 了，这就错了，所以需要 `redo`。因此我们需要顺序扫描日志，对已提交的日志重做、对未提交的日志回滚！

在上面那个例子中，由于事务 C 已经提交了，因此可以忽略这些日志。Simpledb 中存在一个检查点日志，这种日志记录了检查点发生时，仍在活跃的事务集合，以及他们对应的第一条日志偏移。

那么如果我们定位到最后一条检查点日志，读取这些活跃的事务，并且知道了这些活跃事务首记录的偏移，那么可以计算出这些偏移中的最小的偏移 MinOffset，则 MinOffset 之前的记录其实没必要扫描了，因为这之前的事务肯定已经提交了。

```java
// 如果有检查点,则从检查点开始定位
if (checkPointOffset != -1) {
    raf.seek(checkPointOffset);
    raf.readInt();
    raf.readLong();
    long minOffset = Integer.MAX_VALUE;
    int numXActions = raf.readInt();
    while (numXActions-- > 0) {
        raf.readLong();
        minOffset = Math.min(raf.readLong(), minOffset);
    }
    raf.seek(minOffset);
}
```

然后顺序遍历，记录已提交的日志，最后恢复即可，特别注意 `undo` 需要逆序重做。

```java
for (Long tid : allTransactionSet) {
    if (committedTransactionSet.contains(tid)) {
        for (Page page : afterImageMap.getOrDefault(tid, new ArrayList<>())) {
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        }
    } else {
        List<Page> pages = beforeImageMap.getOrDefault(tid, new ArrayList<>());
        // undo 逆序做
        Collections.reverse(pages);
        for (Page page : pages) {
            Database.getCatalog().getDatabaseFile(page.getId().getTableId()).writePage(page);
        }
    }
}
```

