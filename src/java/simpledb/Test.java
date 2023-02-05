package simpledb;

import simpledb.transaction.TransactionId;
import sun.misc.Unsafe;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author happysnaker
 * @date 2022/11/10
 * @email happysnaker@foxmail.com
 */
public class Test extends ReentrantReadWriteLock {
    long val = 0;
    public static void main(String[] args) {
        Unsafe unsafe = null;
        try {
            var getUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            getUnsafe.setAccessible(true);
            unsafe = (Unsafe) getUnsafe.get(null);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }
}
