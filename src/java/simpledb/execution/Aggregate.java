package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.NO_GROUPING;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {
    private OpIterator child;
    private int aField;
    private int gField;
    private Aggregator.Op op;
    private Aggregator aggregator;
    private OpIterator aggregatorIterator;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.aField = afield;
        this.gField = gfield;
        this.op = aop;
        Type gbFieldType = gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield);
//        child.getTupleDesc();
        if (child.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            this.aggregator = new IntegerAggregator(gField, gbFieldType, afield, aop);
        } else {
            this.aggregator = new StringAggregator(gField, gbFieldType, afield, aop);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return this.gField;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return gField == -1 ? null : child.getTupleDesc().getFieldName(gField);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return aField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return aField == -1 ? null : child.getTupleDesc().getFieldName(aField);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        this.child.open();
        super.open();
        while (this.child.hasNext()) {
            aggregator.mergeTupleIntoGroup(this.child.next());
        }
        this.aggregatorIterator = aggregator.iterator();
        this.aggregatorIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.aggregatorIterator.hasNext() ? this.aggregatorIterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.aggregatorIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // 这个函数写了很多次了，应该提升为静态方法，或者写入抽象类中
        Type[] typeAr;
        String[] fieldAr;
        Type gbFieldType = gField == -1 ? null : child.getTupleDesc().getFieldType(gField);
        if (gField == NO_GROUPING) {
            typeAr = new Type[1];
            fieldAr = new String[1];
            typeAr[0] = child.getTupleDesc().getFieldType(aField);
            fieldAr[0] = String.format("%s(%s)", op, child.getTupleDesc().getFieldName(aField));
        } else {
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = gbFieldType;
            fieldAr[0] = child.getTupleDesc().getFieldName(gField);
            typeAr[1] = child.getTupleDesc().getFieldType(aField);
            fieldAr[1] = String.format("%s(%s)", op, child.getTupleDesc().getFieldName(aField));
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    public void close() {
        // some code goes here
        this.aggregatorIterator.close();
        super.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
