package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private int gbField;
    private Type gbFieldType;
    private int aField;
    private Op op;
    private TupleDesc td;
    /**
     * 分组运算的结果
     */
    private final Map<Field, Integer> resultMap;

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op op) {
        // some code goes here
        if (op != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbField = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aField = afield;
        this.op = op;
        this.resultMap = new ConcurrentHashMap<>();
    }

    protected void initTupleDesc(Tuple tuple) {
        Type[] typeAr;
        String[] fieldAr;
        if (gbField == NO_GROUPING) {
            typeAr = new Type[1];
            fieldAr = new String[1];
            typeAr[0] = Type.INT_TYPE; // 仅支持 COUNT
            fieldAr[0] = String.format("%s(%s)", op, tuple.getTupleDesc().getFieldName(aField));
        } else {
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = gbFieldType;
            fieldAr[0] = tuple.getTupleDesc().getFieldName(gbField);
            typeAr[1] = Type.INT_TYPE; // 仅支持 COUNT
            fieldAr[1] = String.format("%s(%s)", op, tuple.getTupleDesc().getFieldName(aField));
        }
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (this.td == null) {
            initTupleDesc(tup);
        }
        Field groupByField = gbField == NO_GROUPING ? null : tup.getField(gbField);
        resultMap.put(groupByField, resultMap.getOrDefault(groupByField, 0) + 1);
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> it : this.resultMap.entrySet()) {
            Tuple tuple = new Tuple(this.td);
            Integer value = it.getValue();
            if (gbField == NO_GROUPING) {
                tuple.setField(0, new IntField(value));
            } else {
                tuple.setField(0, it.getKey());
                tuple.setField(1, new IntField(value));
            }
            tuples.add(tuple);
        }
        return new TupleIterator(this.td, tuples);
    }

}
