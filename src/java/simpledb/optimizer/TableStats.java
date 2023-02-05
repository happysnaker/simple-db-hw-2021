package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query.
 * <p>
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }

    public static void setStatsMap(Map<String, TableStats> s) {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     * <P><strong>请注意，应该至少要保证桶的宽度大于等于 1，否则某些桶将的高度将永远为 0</strong></P>
     */
    static final int NUM_HIST_BINS = 250;


    private int scanCost;
    private int tableId;
    private int numTuples;
    /**
     * 存储每个字段的直方图，key 是字段的下标
     */
    private Map<Integer, IntHistogram> integerIntHistogramMap;
    private Map<Integer, StringHistogram> stringIntHistogramMap;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     *
     * @param tableid       The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO. This doesn't differentiate between
     *                      sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.tableId = tableid;
        this.integerIntHistogramMap = new ConcurrentHashMap<>();
        this.stringIntHistogramMap = new ConcurrentHashMap<>();
        this.scanCost = ((HeapFile) Database.getCatalog().getDatabaseFile(tableId)).numPages() * ioCostPerPage;
        Tuple[] tuples = null;
        SeqScan seqScan = new SeqScan(new TransactionId(-2048), this.tableId);
        try {
            seqScan.open();
            tuples = seqScan.getTupleAr();
            this.numTuples = tuples.length;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("无法扫描表");
        }


        // 获取表中每个字段的最大值和最小值，这只需要计算 INT 类型，String 类型会使用默认最大最小值
        Map<Integer, int[]> minMaxMap = new HashMap<>();
        for (Tuple tuple : tuples) {
            for (int i = 0; i < tuple.getTupleDesc().numFields(); i++) {
                if (tuple.getTupleDesc().getFieldType(i) == Type.STRING_TYPE) {
                    this.stringIntHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                    continue;
                }
                int val = (int) tuple.getField(i).getObject();
                minMaxMap.putIfAbsent(i, new int[]{val, val});
                minMaxMap.put(i, new int[]{Math.min(val, minMaxMap.get(i)[0]), Math.max(val, minMaxMap.get(i)[1])});
            }
        }


        // 根据最大最小值初始化 INT 直方图
        for (Map.Entry<Integer, int[]> it : minMaxMap.entrySet()) {
            int min = it.getValue()[0];
            int max = it.getValue()[1];
            integerIntHistogramMap.put(it.getKey(), new IntHistogram(Math.min(max - min + 1, NUM_HIST_BINS), min, max));
        }

        // 构建好了之后再添加值进去
        for (Tuple tuple : tuples) {
            for (int i = 0; i < tuple.getTupleDesc().numFields(); i++) {
                if (tuple.getTupleDesc().getFieldType(i) != Type.INT_TYPE) {
                    continue;
                }
                int val = (int) tuple.getField(i).getObject();
                integerIntHistogramMap.get(i).addValue(val);
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * <p>
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     *
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return this.scanCost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     * selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        // 选择的可能性 * 总元组数
        return (int) (selectivityFactor * this.numTuples);
    }

    private TupleDesc getTd() {
        return Database.getCatalog().getTupleDesc(tableId);
    }

    /**
     * The average selectivity of the field under op.
     *
     * @param field the index of the field
     * @param op    the operator in the predicate
     *              The semantic of the method is that, given the table, and then given a
     *              tuple, of which we do not know the value of the field, return the
     *              expected selectivity. You may estimate this value from the histograms.
     */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return getTd().getFieldType(field).equals(Type.INT_TYPE) ?
                integerIntHistogramMap.get(field).avgSelectivity() : stringIntHistogramMap.get(field).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     *
     * @param field    The field over which the predicate ranges
     * @param op       The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     * predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        return getTd().getFieldType(field).equals(Type.INT_TYPE) ?
                integerIntHistogramMap.get(field).estimateSelectivity(op, (Integer) constant.getObject()) :
                stringIntHistogramMap.get(field).estimateSelectivity(op, (String) constant.getObject());
    }

    /**
     * return the total number of tuples in this table
     */
    public int totalTuples() {
        // some code goes here
        return this.numTuples;
    }

}
