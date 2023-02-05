package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <P>页面 ID 的锁，由于页面对象可能被驱逐，因此在 {@link Page} 上锁定是不正确的，对象可能会发生变更</P>
 * <P>所以我们不应该在页面对象上加锁，锁应该要针对某一个 {@link PageId}，但是 {@link PageId} 也是可变的，解决的办法是加一层抽象</P>
 * <P><strong>此类提供页面锁的实现，它支持堵塞与非堵塞两种模式获取锁</strong></P>
 * <P>通过缓存池 {@link PageLockPool#getPageIdLock(PageId)} 将 {@link PageId} 映射到不可变的 {@link PageLock} 对象上，锁定将发生在 {@link PageLock} 对象上，直到他们被 {@link PageLockPool#unsafeRemove(PageId)}</P>
 *
 * @author happysnaker
 * @date 2022/11/11
 * @email happysnaker@foxmail.com
 */
public class PageLock implements Cloneable {

    public PageId pid;

    public PageLock(PageId pid) {
        this.pid = pid;
    }

    /**
     * 这是一个特殊的标识，它标识写锁目前正在被读者占有
     */
    private final TransactionId READER_HOLE_WRITER_LOCK = new TransactionId(-2);
    /**
     * 页面上的读者集合，这是并发安全的容器，同时，如果存在事务在等待写锁时，事务会在此条件下休眠，因此任何一个事务释放写锁时，都必须唤醒等待此条件上的事务
     */
    private final Set<TransactionId> readers = ConcurrentHashMap.newKeySet();
    /**
     * 页面上的写者，只允许一个事务锁定
     */
    private volatile TransactionId writer;

    /**
     * 判断给定线程是否持有读锁
     * @param tid
     * @return
     */
    public boolean hasReadLock(TransactionId tid) {
        return readers.contains(tid);
    }


    /**
     * 判断给定线程是否持有读锁
     * @param tid
     * @return
     */
    public boolean hasWriteLock(TransactionId tid) {
        return tid.equals(writer);
    }


    /**
     * 以读者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有写锁，这个读锁将是可重入的
     *
     * @param timeoutMillis 超时时间，毫秒，它必须是正数，可以使用无穷数标识永不超时
     * @return 返回它在指定时间内是否成功获取锁
     */
    public boolean tryReadLock(TransactionId tid, long timeoutMillis) {
        // 重入
        if (this.readers.contains(tid)) {
            return true;
        }

        long endTimeout = System.currentTimeMillis() + timeoutMillis;

        if (endTimeout <= 0) {
            endTimeout = Long.MAX_VALUE;
        }

        synchronized (readers) {
            // 读者数目非空，或者事务已经持有写锁了
            if (!readers.isEmpty() || this.writer == tid) {
                readers.add(tid);
                return true;
            }

            // 否则，没有读者，必须要先获取写锁
            if (writer != null) {
                // 休眠等待写锁唤醒自身
                while (writer != null && writer != READER_HOLE_WRITER_LOCK) {
                    if (System.currentTimeMillis() >= endTimeout) {
                        return false;
                    }
                    try {
//                        System.out.println("我睡眠了!");
                        readers.wait(endTimeout - System.currentTimeMillis());
//                        System.out.println("我醒了！");
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            this.writer = READER_HOLE_WRITER_LOCK;
            readers.add(tid);
        }
        return true;
    }

    /**
     * 以写者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有读锁，这个方法也将堵塞
     *
     * @param timeoutMillis 超时时间，毫秒，它必须是正数，可以使用无穷数标识永不超时
     * @return 返回它在指定时间内是否成功获取锁
     */
    public boolean tryWriteLock(TransactionId tid, long timeoutMillis) {
        if (this.writer == tid) {
            return true;
        }
        long endTimeout = System.currentTimeMillis() + timeoutMillis;

        if (endTimeout <= 0) {
            endTimeout = Long.MAX_VALUE;
        }

        synchronized (readers) {
            // 双重判断
            while (this.writer != null) {
                if (System.currentTimeMillis() >= endTimeout) {
                    return false;
                }
                // 如果写锁占有，则休眠等待唤醒
                try {
                    readers.wait(endTimeout - System.currentTimeMillis());
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            this.writer = tid;
        }
        return true;
    }

    /**
     * 以读者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    public void readUnLock(TransactionId tid) {
        if (!readers.contains(tid)) {
            throw new IllegalMonitorStateException();
        }

        synchronized (readers) {
            readers.remove(tid);
            if (readers.isEmpty()) {
                this.writer = null;
                readers.notifyAll();
            }
        }
    }

    /**
     * 以写者模式释放页面
     *
     * @throws IllegalMonitorStateException 如果事务没有持有锁
     */
    public void writeUnLock(TransactionId tid) {
        if (this.writer != tid) {
            throw new IllegalMonitorStateException();
        }
        synchronized (readers) {
            this.writer = null;
            readers.notifyAll();
        }
    }

    /**
     * 以读者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有写锁，这个读锁将是可重入的
     */
    public void readLock(TransactionId tid) {
        tryReadLock(tid, Long.MAX_VALUE);
    }

    /**
     * 以写者模式锁定页面，这会导致堵塞，相同事务多次加锁是可重入的，<strong>但事务只允许释放一次锁</strong>
     * <br>如果同事务持有读锁，这个方法也将堵塞
     */
    public void writeLock(TransactionId tid) {
        tryWriteLock(tid, Long.MAX_VALUE);
    }

    @Override
    public PageLock clone() {
        PageLock pl = new PageLock(this.pid);
        pl.readers.addAll(this.readers);
        pl.writer = this.writer;
        return pl;
    }
}
