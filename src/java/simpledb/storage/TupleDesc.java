package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * 元组的字段数组
     */
    private volatile TDItem[] items;

    /**
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        return new Iterator<TDItem>() {
            /**
             * 所有字段集合快照
             */
            private final TDItem[] items = TupleDesc.this.items;

            private int index = -1;

            @Override
            public boolean hasNext() {
                return ++index < this.items.length;
            }

            @Override
            public TDItem next() {
                return items[index];
            }
        };
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.items = new TDItem[typeAr.length];
        for (int i = 0; i < typeAr.length; i++) {
            this.items[i] = new TDItem(typeAr[i], fieldAr[i]);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // 需要思考，什么样的字段需要匿名？
        this(typeAr, new String[typeAr.length]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.items.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        }
        return this.items[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= this.items.length) {
            throw new NoSuchElementException();
        }
        return this.items[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0; i < this.items.length; i++) {
            if (this.items[i].fieldName != null && this.items[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int size = 0;
        for (TDItem tdItem : this.items) {
            size += tdItem.fieldType.getLen();
        }
        return size;
    }


    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeAr = new Type[td1.items.length + td2.items.length];
        String[] fieldAr = new String[td1.items.length + td2.items.length];
        int index = 0;
        for (TDItem tdItem : td1.items) {
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            index++;
        }
        for (TDItem tdItem : td2.items) {
            typeAr[index] = tdItem.fieldType;
            fieldAr[index] = tdItem.fieldName;
            index++;
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // 需要思考，为什么不需要比较字段名称？
        // 这说明元组的模式仅仅只与结构有关，而与字段名无关，那么字段名如何存储呢？
        if (this == o) {
            return true;
        }

        if (!(o instanceof TupleDesc)) {
            return false;
        }

        TupleDesc td = (TupleDesc) o;
        if (td.items.length != this.items.length) {
            return false;
        }
        for (int i = 0; i < td.items.length; i++) {
            if (td.items[i].fieldType != this.items[i].fieldType) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        // 哈希值与 equals 应该要具有相同的语义，在 equals 中我们按照要求只比较了 type，因此在 hashCode 中，我们也只对 type 进行计算
        Type[] typeAr = new Type[this.items.length];
        for (int i = 0; i < this.items.length; i++) {
            typeAr[i] = this.items[i].fieldType;
        }
        return Arrays.hashCode(typeAr);
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        if (this.items.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (TDItem tdItem : this.items) {
            sb.append(String.format("%s(%s), ", tdItem.fieldType.toString(), tdItem.fieldName));
        }
        return sb.substring(0, sb.length() - 2);
    }
}
