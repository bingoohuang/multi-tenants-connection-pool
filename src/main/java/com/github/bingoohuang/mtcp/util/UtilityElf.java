package com.github.bingoohuang.mtcp.util;

import lombok.SneakyThrows;
import lombok.val;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Locale;
import java.util.concurrent.*;

import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * @author Brett Wooldridge
 */
public final class UtilityElf {
    /**
     * A constant for SQL Server's Snapshot isolation level
     */
    private static final int SQL_SERVER_SNAPSHOT_ISOLATION_LEVEL = 4096;


    public static boolean objectEquals(Object a, Object b) {
        return a == null && b == null || a != null && a.equals(b);
    }

    /**
     * @return null if string is null or empty
     */
    public static String getNullIfEmpty(final String text) {
        return text == null ? null : text.trim().isEmpty() ? null : text.trim();
    }

    /**
     * Sleep and suppress InterruptedException (but re-signal it).
     *
     * @param millis the number of milliseconds to sleep
     */
    public static void quietlySleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // I said be quiet!
            currentThread().interrupt();
        }
    }

    /**
     * Checks whether an object is an instance of given type without throwing exception when the class is not loaded.
     *
     * @param obj       the object to check
     * @param className String class
     * @return true if object is assignable from the type, false otherwise or when the class cannot be loaded
     */
    public static boolean safeIsAssignableFrom(Object obj, String className) {
        if (obj == null) return false;

        try {
            Class<?> clazz = Class.forName(className);
            return clazz.isAssignableFrom(obj.getClass());
        } catch (ClassNotFoundException ignored) {
            return false;
        }
    }

    /**
     * Create and instance of the specified class using the constructor matching the specified
     * arguments.
     *
     * @param <T>       the class type
     * @param className the name of the class to instantiate
     * @param clazz     a class to cast the result as
     * @param args      arguments to a constructor
     * @return an instance of the specified class
     */
    @SneakyThrows
    public static <T> T createInstance(final String className, final Class<T> clazz, final Object... args) {
        if (className == null) {
            return null;
        }

        Class<?> loaded = UtilityElf.class.getClassLoader().loadClass(className);
        if (args.length == 0) {
            return clazz.cast(loaded.newInstance());
        }

        Class<?>[] argClasses = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            argClasses[i] = args[i].getClass();
        }
        Constructor<?> constructor = loaded.getConstructor(argClasses);
        return clazz.cast(constructor.newInstance(args));
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queueSize     the queue size
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    public static ThreadPoolExecutor createThreadPoolExecutor(final int queueSize, final String threadName, ThreadFactory threadFactory, final RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }

        val queue = new LinkedBlockingQueue<Runnable>(queueSize);
        val executor = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, SECONDS, queue, threadFactory, policy);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    /**
     * Create a ThreadPoolExecutor.
     *
     * @param queue         the BlockingQueue to use
     * @param threadName    the thread name
     * @param threadFactory an optional ThreadFactory
     * @param policy        the RejectedExecutionHandler policy
     * @return a ThreadPoolExecutor
     */
    public static ThreadPoolExecutor createThreadPoolExecutor(final BlockingQueue<Runnable> queue, final String threadName, ThreadFactory threadFactory, final RejectedExecutionHandler policy) {
        if (threadFactory == null) {
            threadFactory = new DefaultThreadFactory(threadName, true);
        }

        val executor = new ThreadPoolExecutor(1 /*core*/, 1 /*max*/, 5 /*keepalive*/, SECONDS, queue, threadFactory, policy);
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    // ***********************************************************************
    //                       Misc. public methods
    // ***********************************************************************

    /**
     * Get the int value of a transaction isolation level by name.
     *
     * @param transactionIsolationName the name of the transaction isolation level
     * @return the int value of the isolation level or -1
     */
    public static int getTransactionIsolation(final String transactionIsolationName) {
        if (transactionIsolationName != null) {
            try {
                // use the english locale to avoid the infamous turkish locale bug
                val upperName = transactionIsolationName.toUpperCase(Locale.ENGLISH);
                if (upperName.startsWith("TRANSACTION_")) {
                    Field field = Connection.class.getField(upperName);
                    return field.getInt(null);
                }
                final int level = Integer.parseInt(transactionIsolationName);
                switch (level) {
                    case Connection.TRANSACTION_READ_UNCOMMITTED:
                    case Connection.TRANSACTION_READ_COMMITTED:
                    case Connection.TRANSACTION_REPEATABLE_READ:
                    case Connection.TRANSACTION_SERIALIZABLE:
                    case Connection.TRANSACTION_NONE:
                    case SQL_SERVER_SNAPSHOT_ISOLATION_LEVEL: // a specific isolation level for SQL server only
                        return level;
                    default:
                        throw new IllegalArgumentException();
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Invalid transaction isolation value: " + transactionIsolationName);
            }
        }

        return -1;
    }

    public static final class DefaultThreadFactory implements ThreadFactory {
        private final String threadName;
        private final boolean daemon;

        public DefaultThreadFactory(String threadName, boolean daemon) {
            this.threadName = threadName;
            this.daemon = daemon;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread thread = new Thread(r, threadName);
            thread.setDaemon(daemon);
            return thread;
        }
    }


    private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();

    public static String createRandomPoolName(String prefix) {
        val random = ThreadLocalRandom.current();
        val buf = new StringBuilder(prefix);

        for (int i = 0; i < 4; i++) {
            buf.append(ID_CHARACTERS[random.nextInt(62)]);
        }

        return buf.toString();
    }
}
