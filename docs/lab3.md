# Lab3

> 开始时间：2022/11/8
>
> 结束时间：2022/11/9
>
> 文档撰写时间：2022/11/9

实验三要求我们实现一个查询优化器，负责计算各连接顺序所需的**预估**的代价，并选出最优的执行顺序。

如果仅仅只是想完成这个实验，应该不是一件难事，但是如果想弄清楚整体流程，需要花点心思。

## 代价估计

我们假定连接是使用循环嵌套法，假定外表 A 为循环外层，内表 B 为循环内层，官方的讲义中，给了我们计算连接代价的公式：
$$
Cost(A \ join \ B) = scanCost(A) + numTuples(A) \times scanCost(B)  + numTuples(A) \times numTuples(B)
$$
其中 `scanCost` 是扫描磁盘的 IO 代价，`numTuples` 是表中元组数目，表达式 $numTuples(A) \times numTuples(B)$ 表示元组过滤时 `Predict` 的 CPU 代价。

上面这个公式告诉我们**不同的连接顺序会产生不同的代价**，例如如果扫描磁盘的代价总是相同的，那么外表元组越少、表越小，所产生的代价越低。

另一个结论是：**在连接前尽可能的减少表的大小，例如先进行过滤再进行连接。**

在本实验中，**关键在于扫描表的代价和估计表的大小。**这都是我们需要完成的实验。

### 扫描磁盘代价

IO 代价是难以估算的，因为还要考虑缓存这个问题，估计页是否在缓存中是很困难的，因为其他事务还在并发的运行中，也许上一秒页还在缓存，而下一秒就被缓存所驱逐。因此，在 Simpledb 中估算代价不考虑缓存问题，总是假定页是从磁盘中读取的。

那么扫描磁盘的代价可以简单定义为：$scanCost = numPages \times factor$，factor 是一个写死的因子，它使得 IO 代价和 CPU 代价具有可比性。

### 估计表的大小

这里需要估计两种表的大小，第一种是基于字段谓词的过滤，考虑下面连接：

```sql
SELECT * FROM A, B WHERE A.X >= 3 AND A.X = B.X;
```

前面得出，连接前总是要进行过滤以减少表的大小，在这个地方要先对 A 表进行谓词过滤，但是不可能去遍历表便统计过滤后的元素，仅仅只是为了得到表的一个大小而去提前遍历表是非常不划算的。

在大多数数据库中，使用直方图去估计表（在某运算符作用下）的**基数**，这种预估不是精确的，但只需要在最开始时遍历一次表以创建直方图，后续便可以在内存**动态维护**，可以**高效**的估计表在某个运算符下的基数（即使表的元组非常多，我们也可以确定桶的数目使得估计基数总能快速的完成）。

一个直方图是由多个桶组成，每个桶象征着一个区间，元组的值落在此区间内相当于落在桶内，如图：

![image-20211015145611089](https://img-blog.csdnimg.cn/img_convert/0964d92fe9e58d5603c7d57376a84a2f.png)

例如，假定在上图中，要求我们预估 `x = 18` （假定直方图建立在 x **整型**字段上）的元组数，18 落在 `16~20` 区间内，而这个区间包含 50 个元组，这个区间最多含有 5 个值（`x=16,17,18,19,20`），通常我们会假定这些值均匀分布，那么 `x = 18` 的元组数就可以大致预估出来，即 $\frac{50}{5}=10$。

要求计算 `x > 18` 会稍微复杂些，但在纸上画画总能计算出来。实现 `>、=` 之后，其他运算符都可以根据这两个求解。**这就是练习一的内容。**练习一要求我们计算选择性，选择性乘上总元组数即可获得预估大小。

除了预估表在某个运算符下的基数大小，我们还需要预估**连接后中间表的大小**，这个大小讲义中给了大致的说明：

- 在连接运算符为等值条件下:
  - 如果比较字段是主键，那么最大的元组数不会超过非主键字段的元组数，因为主键是唯一的。
  - 如果比较的两个字段都是主键，那么最大不会超过较小那个表的元组数。
  - 其他情况下，值是难以确定的，最差可能会是两个表的笛卡尔积，在数据库中会采用启发式算法预估，即根据实际的结果调整预估的参数。
    - 这里有一个方法是基于直方对的匹配，将直方图中每一个桶看成一个元素，那这个元素去预估另一个表的基数，然后乘上桶的高度，累加即可。
  
- 在连接运算符为范围运算条件下，预估也是非常麻烦的，最差可能会是两个表的笛卡尔积，在数据库中也会采用启发式算法预估，根据实际的结果调整预估的参数，官方建议我们使用 `0.3 * m * n`。

## 确定最优的连接顺序

在能够得到两个表连接的代价后，需要确定最优的连接顺序，给定 N 各表连接，这将会产生 `N!` 种排列，这个数据是非常大的，不可能去遍历每一种排列计算代价，如果这样的话，查询优化器反而成为了拖后腿的东西。

事实上这里有一个关键点，例如对于 `abc` 三个表连接，那么顺序无论是 `abc` 还是 `cba` ，其对应的最优顺序都是一致的。这提示我们同一个集合产生的最优排序是一致的，而一个集合又可以由他的两个子集递归而来。

所以我们只需要枚举组合即可，时间复杂度为 `Cn1 + Cn2 + ... + Cnn = 2^n`。  

我第一时间想到的是一种记忆化搜索的算法，对于每个集合，枚举子集、以及子集的补集，然后递归的计算子集和补集的最优序列，最后将他们合并并统计答案，递归时可以记忆化提前跳出。

但是在我写的时候，我发现在这个实验中这种逻辑似乎是行不通的，原因是 `joins` 的集合是一个 `LogicalJoinNode` 对象，这个对象包含了两个表信息，例如 `a&b` 表示 a 连接 b。

当我的递归程序返回了两个集合时。例如一个集合是 `a&b、b&c`，另一个集合是 `c&d、d&e`，即使我得到了这两个集合的代价与基数，我也不能将他们进行连接，因为这样连接的话，`c` 表就连接了两次，这是错误的！

所以，`LogicalJoinNode` 对象仅保存要连接的一个表会不会好些.....似乎不行，因为`LogicalJoinNode` 对象还要保存运算符，运算符的逻辑可不能丢了。

官方给出的是自底向上的动态规划算法，每一个长度为 `i` 的集合都可以由长度为 `i - 1` 的集合和它自身一个表连接转换而来，官方给我们写好了一个非常重要的辅助函数：

```java
private CostCard computeCostAndCardOfSubplan(
            Map<String, TableStats> stats,
            Map<String, Double> filterSelectivities,
            LogicalJoinNode joinToRemove, Set<LogicalJoinNode> joinSet,
            double bestCostSoFar, PlanCache pc) throws ParsingException {
```

如果理解了动态规划的思想，这个函数也就能看懂了，这个函数计算 `joinSet` 从移除 `joinToRemove` 后的集合转移过来所需要的代价，并与先前的代价进行比较，返回最优的代价或者 NULL。

而移除 `joinToRemove` 后的集合所需要的代价和基数是通过 `pc` 获取的，这是一个缓存，我们需要将某个集合所需的代价缓存到这个 `pc` 中。

这里仔细看了看子集合与  `joinToRemove`  单 `LogicalJoinNode` 的连接逻辑，官方其实是判断要与 joinToRemove 中的那个表进行连接，例如集合是 `a&b、b&c`，`joinToRemove`  是 `c&d`，由于 d 不在集合的连接中，所以这里是集合与表 d 连接，运算符采用`joinToRemove`  中的运算符 。

<img src="https://happysnaker-1306579962.cos.ap-nanjing.myqcloud.com/img/typora/202211092115901.png" alt="image-20221109211512806" style="zoom: 67%;" />

代码中，最后是尝试了两种连接顺序，并选择最优的返回，交换顺序是通过 `joinToRemove.swapInnerOuter();`  方法完成的。

而且，我注意到 `filterSelectivities` 参数每一个表只存在一个选择性，那么如果一个表上存在多个选择运算符该怎么办呢？例如 `SELECT * FROM A, B WHERE A.X >= 3 AND A.Y = 3 AND A.X = B,X;`，A 表存在两个过滤谓词，这种预估也是非常棘手的，或许可以选择较大的那么结果。

一旦我们确定最优顺序，例如 `a&b、d&c、a&c` 之后，我们要开始执行它。

我们首先要获取 `a、b、c、d` 的全表扫描计划，Simpledb 中以 `subplanMap` 存储他们，key 是他们的名字。

一旦我们执行 `a&b` 之后，`subplanMap`  中表 a 的迭代流替换成了 `a&b` 的 Json 迭代流，而 `b` 将会被替换成 `a` 的名字，后续获取 `b` 将会获取 `a` 的迭代流。

```java
joins = jo.orderJoins(statsMap, filterSelectivities, explain);

for (LogicalJoinNode lj : joins) {
    OpIterator plan1;
    OpIterator plan2;
    String t1name, t2name;
	
    if (equivMap.get(lj.t1Alias) != null)
        t1name = equivMap.get(lj.t1Alias);
    else
        t1name = lj.t1Alias;

    if (equivMap.get(lj.t2Alias) != null)
        t2name = equivMap.get(lj.t2Alias);
    else
        t2name = lj.t2Alias;

    plan1 = subplanMap.get(t1name);
    plan2 = subplanMap.get(t2name);

    OpIterator j;
    j = JoinOptimizer.instantiateJoin(lj, plan1, plan2);
    // 更新 t1 的迭代流
    subplanMap.put(t1name, j);
	
    // 移除 t2 名字，换成 t1 的
    subplanMap.remove(t2name);
    equivMap.put(t2name, t1name);
    for (Map.Entry<String, String> s : equivMap.entrySet()) {
        String val = s.getValue();
        if (val.equals(t2name)) {
            s.setValue(t1name);
        }
    }

}
```

