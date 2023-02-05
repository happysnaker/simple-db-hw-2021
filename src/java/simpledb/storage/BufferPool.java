package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    /**
     * 死锁等待超时时间
     */
    public static final int DEAD_LOCK_TIMEOUT = 5000;
    /**
     * 最大缓冲数量
     */
    private final int numPages;
    /**
     * 缓冲池中的页面
     */
    public final LinkedHashMap<PageId, Page> pagePool;
    /**
     * 页面锁池，页面锁必须要是不可变对象
     */
    private final PageLockPool pageIdLockPool;
    /**
     * 此 Map 跟踪事务在哪些页面集合上获取锁，以便释放他们
     */
    private final ConcurrentHashMap<TransactionId, Set<PageLock>> transactionLockMap;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        this.numPages = numPages;
        this.transactionLockMap = new ConcurrentHashMap<>();
        this.pageIdLockPool = new PageLockPool(this.numPages << 4);
        this.pagePool = new LinkedHashMap<PageId, Page>(numPages, 0.598f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<PageId, Page> eldest) {
                if (this.size() > numPages) {
                    Page page = eldest.getValue();
                    // lab4 告诉我们不能驱逐脏页，因为事务可能正在修改它
                    // 事务会在提交时刷新所有页，这是一种简单的模式，实际中不会这样刷新
                    // 另外，即使是干净的页面也不能随意驱逐，因为可能其他事务正在引用这个页面，并且马上要修改
                    // 所以，缓存驱逐时，必须保证没有事务会引用页面，这可以通过锁来保证，我们通过对页面 ID 上锁而不是对页面对象上锁
                    // 在 simpledb 中，弄脏的页面会重新覆盖到缓存中
                    if (page.isDirty() == null) {
                        return true;
                    }
                    //  否则，需要手动驱逐，new ArrayList<>(pagePool.values())) 按照访问顺序有序，put 是加了锁，这里也是安全的
                    Collection<Page> values = pagePool.values();
                    for (Page value : new ArrayList<>(values).subList(0, values.size() - 1)) {
                        if (value.isDirty() == null) {
                            discardPage(value.getId());
                            return false;
                        }
                    }

                    throw new RuntimeException("缓存空间不足");
                }
                return false;
            }
        };
    }

    public static int getPageSize() {
        return pageSize;
    }

    /**
     * THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
     */
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * <p>在实验 1 中，无需关注 tid 和 perm</p>
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        assert tid != null;
        // some code goes here
        Page page = pagePool.get(pid);
        if (page == null) {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            // LinkedHashMap 线程不安全
            synchronized (this) {
                if (this.pagePool.containsKey(pid)) {
                    page = pagePool.get(pid);
                } else {
                    page = dbFile.readPage(pid);
                    page.setBeforeImage();
                    // 刚从磁盘读出，它总能被正确的锁定
                    try {
                        lockPage(tid, page.getId(), perm, DEAD_LOCK_TIMEOUT);
                    } catch (TimeoutException e) {
                        throw new TransactionAbortedException();
                    }
                    this.pagePool.put(pid, page);
                    return page;
                }
            }
        }
        // 考虑一个多线程问题，如果页面在获取锁的前一瞬间被驱逐了，会发生什么呢？
        try {
            lockPage(tid, page.getId(), perm, DEAD_LOCK_TIMEOUT);
        } catch (TimeoutException e) {
            throw new TransactionAbortedException();
        }
        return page;
    }

    /**
     * 堵塞的调用方法，等效于 lockPage(TransactionId, PageId, Permissions, INF)
     *
     * @see #lockPage(TransactionId, PageId, Permissions, long)
     */
    public void lockPage(TransactionId tid, PageId pid, Permissions perm) {
        try {
            lockPage(tid, pid, perm, Long.MAX_VALUE);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 事务以某种权限锁定页面，这可能会造成堵塞
     *
     * @param tid           事务，或者说是一个线程
     * @param pid           待锁定的页面
     * @param perm          权限，如果为 null，则不会向页面中加任何锁
     * @param timeoutMillis 等待锁的超时时间，他必须是一个正数，可以设置为无限大标识永不超时
     * @throws TimeoutException 如果超时未能获得锁
     */
    public void lockPage(TransactionId tid, PageId pid, Permissions perm, long timeoutMillis) throws TimeoutException {
        if (perm == null) {
            return;
        }
        PageLock pageIdLock = getPageIdLock(tid, pid);
        if (pageIdLock == null) {
            pageIdLock = pageIdLockPool.getPageIdLock(pid);
        }

        if (perm == Permissions.READ_ONLY) {
            if (!pageIdLock.tryReadLock(tid, timeoutMillis)) {
                throw new TimeoutException();
            }
        } else if (perm == Permissions.READ_WRITE) {
            // 如果线程持有读锁来申请写锁，那么首先应该释放读锁
            if (pageIdLock.hasReadLock(tid)) {
                pageIdLock.readUnLock(tid);
            }
            if (!pageIdLock.tryWriteLock(tid, timeoutMillis)) {
                throw new TimeoutException();
            }
        }
        this.transactionLockMap.putIfAbsent(tid, new HashSet<>());
        this.transactionLockMap.get(tid).add(pageIdLock);
    }


    /**
     * 释放页面上指定的锁，如果不存在锁将直接返回
     *
     * @param tid  the ID of the transaction requesting the unlock
     * @param pid  the ID of the page to unlock
     * @param perm 需要释放的锁的模式
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        PageLock pageIdLock = getPageIdLock(tid, pid);
        if (pageIdLock == null) {
            pageIdLock = pageIdLockPool.getPageIdLock(pid);
        }
        if (pageIdLock.hasWriteLock(tid) && perm == Permissions.READ_WRITE) {
            pageIdLock.writeUnLock(tid);
        }
        if (pageIdLock.hasReadLock(tid) && perm == Permissions.READ_ONLY) {
            pageIdLock.readUnLock(tid);
        }
        if (!pageIdLock.hasWriteLock(tid) && !pageIdLock.hasReadLock(tid)) {
            this.transactionLockMap.get(tid).remove(pageIdLock);
        }
        if (this.transactionLockMap.get(tid) == null) {
            this.transactionLockMap.remove(tid);
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) throws TransactionAbortedException, DbException {
        // some code goes here
        // not necessary for lab1|lab2
        unsafeReleasePage(tid, pid, Permissions.READ_ONLY);
        unsafeReleasePage(tid, pid, Permissions.READ_WRITE);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * 返回事务在给定页面上的锁
     * <p>如果事务没有持有锁，它应该返回 null</p>
     */
    public PageLock getPageIdLock(TransactionId tid, PageId p) {
        if (!transactionLockMap.containsKey(tid)) {
            return null;
        }
        for (PageLock pil : transactionLockMap.get(tid)) {
            if (pil.pid.equals(p)) {
                return pil;
            }
        }
        return null;
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        PageLock pl = getPageIdLock(tid, p);
        return pl != null && (pl.hasReadLock(tid) || pl.hasWriteLock(tid));
    }


    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        List<PageLock> pageLocks = new ArrayList<>(this.transactionLockMap.getOrDefault(tid, new HashSet<>()));
        if (commit) {
            try {
                flushPages(tid);
                for (PageLock pageLock : pageLocks) {
                    // 更新后设置前置镜像，以便下一个事务恢复它
                    // 如果池中不存在这个页面，说明页面已经被驱逐了，在读取时设置镜像
                    Page page = pagePool.get(pageLock.pid);
                    if (page != null) {
                        page.setBeforeImage();
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            for (PageLock pageLock : pageLocks) {
                discardPage(pageLock.pid);
            }
        }
        for (PageLock pageLock : pageLocks) {
            try {
                unsafeReleasePage(tid, pageLock.pid);
            } catch (TransactionAbortedException | DbException e) {
                throw new RuntimeException(e);
            }
        }
        this.transactionLockMap.remove(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            pagePool.put(p.getId(), p);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = file.deleteTuple(tid, t);
        for (Page p : pages) {
            p.markDirty(true, tid);
            // 事实上，由于这些页面本身就是从缓冲池中取出来的，因此这里似乎并不需要重新放入，因为引用并未改变
            pagePool.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Page page : this.pagePool.values()) {
            if (page.isDirty() != null) {
                DbFile dbFile = Database.getCatalog().getDatabaseFile(page.getId().getTableId());

                TransactionId dirtier = page.isDirty();
                if (dirtier != null){
                    Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
                    Database.getLogFile().force();
                }

                dbFile.writePage(page);
                page.markDirty(false, null);
            }
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pagePool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        // 调用 DbFile 刷盘即可
        Page page = this.pagePool.get(pid);
        if (page == null)
            return;
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());

        TransactionId dirtier = page.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, page.getBeforeImage(), page);
            Database.getLogFile().force();
        }

        dbFile.writePage(page);
        page.markDirty(false, null);
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        // 此时页面正在被事务锁定
        for (PageLock pl : this.transactionLockMap.getOrDefault(tid, new HashSet<>())) {
            flushPage(pl.pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // 我们在 LinkedHashMap 中实现了这个逻辑
    }
}
