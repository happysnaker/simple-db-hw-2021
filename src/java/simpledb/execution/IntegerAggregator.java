package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private TupleDesc td;
    /**
     * 分组运算的结果
     */
    private final Map<Field, Integer> resultMap;
    /**
     * 跟踪聚合元组的总数量，仅在 AVG 时需要使用，其余操作符仅使用 resultMap 统计
     */
    private final Map<Field, Integer> countMap;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param op          the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
        // some code goes here
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = op;
        this.resultMap = new HashMap<>();
        this.countMap = new HashMap<>();
    }

    protected void initTupleDesc(Tuple tuple) {
        Type[] typeAr;
        String[] fieldAr;
        if (gbField == NO_GROUPING) {
            typeAr = new Type[1];
            fieldAr = new String[1];
            typeAr[0] = tuple.getTupleDesc().getFieldType(aField);
            fieldAr[0] = String.format("%s(%s)", op, tuple.getTupleDesc().getFieldName(aField));
        } else {
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = gbFieldType;
            fieldAr[0] = tuple.getTupleDesc().getFieldName(gbField);
            typeAr[1] = tuple.getTupleDesc().getFieldType(aField);
            fieldAr[1] = String.format("%s(%s)", op, tuple.getTupleDesc().getFieldName(aField));
        }
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            initTupleDesc(tup);
        }
        Field aggregatorField = tup.getField(aField);
        // HashMap 的 Key 允许为空，但 ConcurrentHashMap 不允许
        Field groupByField = gbField == NO_GROUPING ? null : tup.getField(gbField);
        switch (this.op) {
            case MIN -> {
                resultMap.putIfAbsent(groupByField, (Integer) aggregatorField.getObject());
                resultMap.put(groupByField, Math.min(resultMap.get(groupByField), (Integer) aggregatorField.getObject()));
            }
            case MAX -> {
                resultMap.putIfAbsent(groupByField, (Integer) aggregatorField.getObject());
                resultMap.put(groupByField, Math.max(resultMap.get(groupByField), (Integer) aggregatorField.getObject()));
            }
            case SUM, AVG -> {
                resultMap.put(groupByField, resultMap.getOrDefault(groupByField, 0) + (Integer) aggregatorField.getObject());
                if (this.op == Op.AVG) {
                    countMap.put(groupByField, countMap.getOrDefault(groupByField, 0) + 1);
                }
            }
            case COUNT -> {
                resultMap.put(groupByField, resultMap.getOrDefault(groupByField, 0) + 1);
            }
            default -> {
                throw new RuntimeException("不支持的聚合操作符：" + op);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> it : this.resultMap.entrySet()) {
            Tuple tuple = new Tuple(this.td);
            Integer value = it.getValue();
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(this.op == Op.AVG ? value / countMap.get(it.getKey()) : value));
            } else {
                tuple.setField(0, it.getKey());
                tuple.setField(1, new IntField(this.op == Op.AVG ? value / countMap.get(it.getKey()) : value));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(this.td, tuples);
    }

}
