package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    private final int tableId;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        // 如果发生哈希碰撞了怎么办
        this.tableId = f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // 这里的 ID 其实就是 tableId，我们在 catalog 中就是这么用的
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int offset = pid.getPageNumber() * BufferPool.getPageSize();
        if (offset >= file.length()) {
            throw new IllegalArgumentException("读取不存在的页面");
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] byteAr = new byte[BufferPool.getPageSize()];
            raf.seek(offset);
            int read = raf.read(byteAr, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new RuntimeException("文件格式不正确，表文件已损坏");
            }
            return new HeapPage((HeapPageId) pid, byteAr);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        // 由于页面处于锁定状态，这里的写入总是并发安全的
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            raf.write(page.getPageData());
            raf.getChannel().force(true);
        }
    }

    /**
     * 返回此 HeapFile 中的页数。
     * <p><strong>这个函数必须要实时进行计算，而不能缓存下来</strong></p>
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // 似乎插入只会插入到一个页面中，会什么需要返回一个列表？也许后面的实验会告诉我们答案
        BufferPool pool = Database.getBufferPool();
        int numPages = numPages();
        for (int i = 0; i < numPages; i++) {
            // 页面本身可能会被事务持有锁
            PageLock pageIdLock = pool.getPageIdLock(tid, new HeapPageId(this.tableId, i));
            if (pageIdLock != null) {
                // 这是一个副本
                pageIdLock = pageIdLock.clone();
            }
            // 以写权限锁定页面，因此接下来对页面的操作都是安全的
            // 不过，这样遍历堵塞获取锁，效率是非常差的，更好的办法是写一个空闲空间管理类
            // 例如一个并发安全的哈希，key 是空闲槽的数目，val 是一系列页的集合
            HeapPage page = (HeapPage) pool.getPage(tid, new HeapPageId(this.tableId, i), Permissions.READ_WRITE);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                return List.of(page);
            }
            // 要记录一下，这里并没有释放锁的逻辑，也没有标记脏页的逻辑
            // lab4 填坑，如果没有修改页面，那么应该释放掉页面上的锁
            // unsafeReleasePage 将释放所有锁，但是如果页面之前就持有锁呢？我们应该要考虑带这种情况，所以我们要预先保持事务持有的锁
            else {
                if (pageIdLock == null || (!pageIdLock.hasWriteLock(tid) && !pageIdLock.hasReadLock(tid))) {
                    // 没持有锁
                    pool.unsafeReleasePage(tid, page.pid);
                } else if (pageIdLock.hasReadLock(tid)) {
                    // 进行锁降级
                    pool.lockPage(tid, page.pid, Permissions.READ_ONLY);
                    pool.unsafeReleasePage(tid, page.pid, Permissions.READ_WRITE);
                }
                // 如果是写锁，那么不需要释放它
            }
        }
        // 页面已满，创建新页面
        // 这里要加锁，防止并发问题，setLength 和 seek 都是线程不安全的
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        synchronized (file) {
            try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
                long tail = raf.length();
                raf.setLength(tail + emptyPageData.length);
                raf.seek(tail);
                raf.write(emptyPageData);
            }
        }
        // 简单点吧，递归一下，否则在高并发环境下，新创建的页可能立马又会被瞬间用完，这就需要再次创建页面
        return insertTuple(tid, t);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        BufferPool pool = Database.getBufferPool();
        if (this.tableId != t.getRecordId().getPageId().getTableId()) {
            throw new DbException("元组不属于这个表");
        }
        HeapPage page = (HeapPage) pool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(List.of((Page) page));
    }

    // see DbFile.java for javadocs

    /**
     * <strong>这不是线程安全的，SimpleDB 中采用了非常粗暴的方式，他会锁定整个获得到的页面</strong>
     *
     * @param tid 事务 ID
     * @return 迭代器
     */
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new AbstractDbFileIterator() {
            // 读取的页号
            private int pageno;
            // 元组迭代器
            private Iterator<Tuple> tupleIterator;

            @Override
            public void open() throws TransactionAbortedException, DbException {
                this.pageno = 0;
                this.tupleIterator = getTupleIterator(0);
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                this.open();
            }

            @Override
            public void close() {
                this.tupleIterator = null;
                super.close();
            }

            @Override
            protected Tuple readNext() throws DbException, TransactionAbortedException {
                if (this.tupleIterator == null) {
                   return null;
                }
                // 这里需要注意的是，即使存在一个页面，这个页面中可能也没有任何有效的元组，这时应该迭代下一个页面
                while (!this.tupleIterator.hasNext()) {
                    if (++pageno >= HeapFile.this.numPages()) {
                        return null;
                    }
                    this.tupleIterator = getTupleIterator(pageno);
                }
                return this.tupleIterator.next();
            }

            private Iterator<Tuple> getTupleIterator(int pageno) throws TransactionAbortedException, DbException {
                HeapPageId pid = new HeapPageId(HeapFile.this.tableId, pageno);
                // Lab1 中，我们还不知道何时需要释放这个锁，这是我们需要注意的地方
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                return page.iterator();
            }
        };
    }
}

