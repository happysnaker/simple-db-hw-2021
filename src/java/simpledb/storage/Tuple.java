package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 元组的 ID，标识在磁盘上的位置
     */
    private RecordId rid;

    /**
     * 元组的模式
     */
    private TupleDesc td;

    /**
     * 元组的值，与 TupleDesc 对应
     */
    private Field[] fieldAr;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        this.td = td;
        this.fieldAr = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public Tuple setField(int i, Field f) {
        // some code goes here
        this.fieldAr[i] = f;
        return this;
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        return this.fieldAr[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        if (this.fieldAr.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Field field : this.fieldAr) {
            sb.append(field.toString()).append("\t");
        }
        return sb.toString().trim();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return Arrays.stream(this.fieldAr).collect(Collectors.toList()).listIterator();
    }


    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        this.td = td;
    }


    public String toStringDebug() {
        return "Tuple{" +
                "rid=" + rid +
                ", td=" + td +
                ", fieldAr=" + Arrays.toString(fieldAr) +
                '}';
    }
}
