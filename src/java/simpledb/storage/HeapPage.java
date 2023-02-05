package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    /**
     * 存储槽位，表示 tuple 是否存在，由于一字节有八位，因此最后一字节可能会有些位数未使用
     */
    final byte[] header;
    /**
     * 内存字段元组，与槽位对应
     */
    final Tuple[] tuples;
    /**
     * 所能容纳的最多的元组数
     */
    final int numSlots;
    /**
     * 用以标记最近一次弄脏页面的事务 ID
     */
    private TransactionId markDirtyTid;

    byte[] oldData;
    private final Byte oldDataLock = (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     * Specifically, the number of tuples is equal to: <p>
     * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     * ceiling(no. tuple slots / 8)
     * <p>
     *
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++)
            header[i] = dis.readByte();

        tuples = new Tuple[numSlots];
        try {
            // allocate and read the actual records of this page
            for (int i = 0; i < tuples.length; i++)
                tuples[i] = readNextTuple(dis, i);
        } catch (NoSuchElementException e) {
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /**
     * Retrieve the number of tuples on this page.
     *
     * <p>注意，返回并非返回实际的元组，而是返回页所能容纳的最多的元组数，注释不是很精确</p>
     *
     * @return the number of tuples on this page
     */
    private int getNumTuples() {
        // some code goes here
        return (int) Math.floor((BufferPool.getPageSize() * 8f) / (td.getSize() * 8f+ 1));

    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     *
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        // 应该向上舍入
        return (int) Math.ceil(getNumTuples() / 8f);
    }

    /**
     * Return a view of this page before it was modified
     * -- used by recovery
     */
    public HeapPage getBeforeImage() {
        try {
            byte[] oldDataRef = null;
            synchronized (oldDataLock) {
                oldDataRef = oldData;
            }
            return new HeapPage(pid, oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }

    public void setBeforeImage() {
        synchronized (oldDataLock) {
            oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        // some code goes here
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i = 0; i < td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j = 0; j < td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @return A byte array correspond to the bytes of this page.
     * @see #HeapPage
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i = 0; i < tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j = 0; j < td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j = 0; j < td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     * that it is no longer stored on any page.
     *
     * @param t The tuple to delete
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *                     already empty.
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        int i = t.getRecordId().getTupleNumber();
//        System.out.println("DELETE Thread.currentThread() = " + Thread.currentThread() + " BEFORE DELETE " + t.toStringDebug());
        if (!t.getRecordId().getPageId().equals(this.pid)) {
            throw new DbException("元组不属于此页面");
        }
        if (!isSlotUsed(i) || !tuples[i].equals(t)) {
            throw new DbException("元组不存在");
        }

        markSlotUsed(i, false);
        tuples[i] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     * that it is now stored on this page.
     *
     * @param t The tuple to add.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *                     is mismatch.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        if (t == null || !t.getTupleDesc().equals(Database.getCatalog().getTupleDesc(pid.getTableId()))) {
            throw new DbException("插入元组模式不匹配");
        }
        // 在 Simple 中似乎不需要锁定，因为事务总是会以写定权限锁定整个页面
        // 不管怎么样，双重验证总是最稳妥的办法
        for (int i = 0; i < this.numSlots; i++) {
            if (!isSlotUsed(i)) {
                synchronized (this) {
                    if (!isSlotUsed(i)) {
//                        System.out.println("INSERT Thread.currentThread() = " + Thread.currentThread() + " BEFORE INSERT " + isSlotUsed(i) + " " + t.toStringDebug());
                        t.setRecordId(new RecordId(pid, i));
                        tuples[i] = t;
                        markSlotUsed(i, true);
//                        System.out.println("INSERT Thread.currentThread() = " + Thread.currentThread() + " INSERT POS " + i + " AFTER INSERT " + isSlotUsed(i) + " PAGE " + this + " " + t.toStringDebug());
                        return;
                    }
                }
            }
        }
        throw new DbException("页面已满");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
        // not necessary for lab1
        this.markDirtyTid = dirty ? tid : null;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
        // Not necessary for lab1
        return markDirtyTid;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int emptySlotNum = 0;
        for (int i = 0; i < getNumTuples(); i++) {
            if (!isSlotUsed(i)) {
                emptySlotNum++;
            }
        }
        return emptySlotNum;
    }


    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // i 表示第 i 个元组，从 0 开始
        // 0 在 head[0] 的最低位，1 在head[0] 的第二低位，8 在 head[1] 的最低位
        return ((header[i / 8] >> (i % 8)) & 1) == 1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean val) {
        // some code goes here
        // not necessary for lab1
        int mask = 1 << (i % 8);
        if (val) {
            this.header[i / 8] |= mask;
        } else {
            this.header[i / 8] &= (~mask);
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        return new Iterator<Tuple>() {
            // 迭代器使用元组的副本以防止多线程的干扰
            private final Tuple[] tuples = HeapPage.this.tuples.clone();
            private int index = -1;
            private Tuple next = null;

            @Override
            public boolean hasNext() {
                if (next != null) {
                    return true;
                }
                while (++index < tuples.length && !HeapPage.this.isSlotUsed(index)) {
                    // 略过所有无效的元组
                }
                return index < tuples.length && (next = tuples[index]) != null;
            }

            @Override
            public Tuple next() {
                if (next == null && !hasNext()) {
                    throw new NoSuchElementException();
                }
                Tuple ans = next;
                next = null;
                return ans;
            }
        };
    }
}

