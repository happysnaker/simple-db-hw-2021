package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/**
 * A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
    private int sum;
    private double width;
    private int max;
    private int min;
    private int[] buckets;

    /**
     * Create a new IntHistogram.
     * <p>
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * <p>
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * <p>
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min     The minimum integer value that will ever be passed to this class for histogramming
     * @param max     The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        // some code goes here
        this.buckets = new int[buckets];
        this.min = min;
        this.max = max + 1;     // 右区间开放
        this.width = (this.max - this.min) / (buckets * 1.0F);
    }

    /**
     * 根据值获取对应的通，一个桶总是左闭右开的
     *
     * @param v 值
     * @return 桶的下标
     */
    private int getBucketIndex(int v) {
        int index = (int) ((v - min) / width);
        // 由于使用 double 作为桶宽，可能存在精度问题
        return Math.min(this.buckets.length - 1, Math.max(0, index));
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     *
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        // some code goes here
        if (v >= min && v < max) {
            this.buckets[getBucketIndex(v)]++;
            sum++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * <p>
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v  Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        switch (op) {
            case EQUALS -> {
                if (v < min || v >= max) {
                    return 0.0f;
                }
                return (this.buckets[getBucketIndex(v)] / this.width) / this.sum;
            }
            case LESS_THAN -> {
                if (v < min || v >= max) {
                    return v < min ? 0.0f : 1.0f;
                }
                int lessThanNums = 0;
                int bucketIndex = getBucketIndex(v);
                for (int i = 0; i < bucketIndex; i++) {
                    lessThanNums += buckets[i];
                }
                double leftBound = (bucketIndex) * width + min, proportion = (v - leftBound) / width;
                return (lessThanNums + proportion * buckets[bucketIndex]) / sum;
            }
            case LESS_THAN_OR_EQ -> {
                return estimateSelectivity(Predicate.Op.LESS_THAN, v) + estimateSelectivity(Predicate.Op.EQUALS, v);
            }
            case GREATER_THAN -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN_OR_EQ, v);
            }
            case GREATER_THAN_OR_EQ -> {
                return 1 - estimateSelectivity(Predicate.Op.LESS_THAN, v);
            }
            case NOT_EQUALS -> {
                return 1 - estimateSelectivity(Predicate.Op.EQUALS, v);
            }
        }
        // some code goes here
        throw new RuntimeException();
    }

    /**
     * @return the average selectivity of this histogram.
     * <p>
     * This is not an indispensable method to implement the basic
     * join optimization. It may be needed if you want to
     * implement a more efficient optimization
     */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "sum=" + sum +
                ", width=" + width +
                ", max=" + max +
                ", min=" + min +
                ", buckets=" + Arrays.toString(buckets) +
                '}';
    }
}
