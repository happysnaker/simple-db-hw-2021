package simpledb.storage;

import simpledb.transaction.TransactionId;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 此对象将将可变的 {@link PageId} 映射到不可变的 {@link PageLock} 对象上，对于同一个 {@link BufferPool} 而言，此对象必须也是唯一的
 *
 * @author happysnaker
 * @date 2022/11/11
 * @email happysnaker@foxmail.com
 */
public class PageLockPool {
    /**
     * 一个超级事务，用于驱逐缓存使用
     */
    private static final TransactionId SUPER_TRANSACTION = new TransactionId(-1024);
    
    final Map<PageId, PageLock> pool;

    /**
     * {@link PageLock} 的入口，同一个 pageID 会得到唯一的 {@link PageLock}，直到他被 {@link #unsafeRemove(PageId)}
     *
     * @param pid
     * @return
     */
    public synchronized PageLock getPageIdLock(PageId pid) {
        pool.putIfAbsent(pid, new PageLock(pid));
        return pool.get(pid);
    }

    /**
     * 移除 {@link PageLock}，这个操作是很危险的，请确保在移除时 {@link PageLock} 上没有锁，并且将来也不会有事务锁定一个已经被移除的锁
     *
     * @param pid
     */
    public synchronized void unsafeRemove(PageId pid) {
        pool.remove(pid);
    }

    /**
     * 构造页面锁缓冲池，一个 {@link BufferPool} 应该唯一持有一个 {@link PageLockPool}
     * @param size 缓存最大数目，这只是个估计参数，{@link PageLockPool} 保证最终一致性，但某些时候可能会超负荷，<strong>此缓冲池保证，永远不会将正在被锁定的页面驱逐</strong>
     */
    public PageLockPool(int size) {
        this.pool = new LinkedHashMap<>() {
            // 由于 put 方法是线程安全的，这个移除动作总是线程安全的
            @Override
            protected boolean removeEldestEntry(Map.Entry<PageId, PageLock> eldest) {
                while (size() > size) {
                    Collection<PageLock> values = values();
                    // new ArrayList<>(values) 是按照访问顺序，最后一个为最新加入的数据
                    // 驱逐时要驱逐没有任何锁的页面，这可以通过获取写锁实现
                    boolean release = false;
                    for (PageLock lock : new ArrayList<>(values).subList(0, values.size() - 1)) {
                        if (lock.tryWriteLock(SUPER_TRANSACTION, 1000)) {
                            remove(lock.pid);
                            release = true;
                        }
                    }
                    
                    // 如果一个都无法释放，由于 PageIdLock 占用内存不多，可以暂时让他存留在内存中
                    if (!release) {
                        break;
                    }
                }
                return false;
            }
        };
    }
}
