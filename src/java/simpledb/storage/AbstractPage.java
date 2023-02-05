package simpledb.storage;

import simpledb.transaction.TransactionId;
import sun.misc.Unsafe;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author happysnaker
 * @date 2022/11/10
 * @email happysnaker@foxmail.com
 * @deprecated 这个做法是不正确的！
 */
public abstract class AbstractPage implements Page {

}

