package com.github.bingoohuang.mtcp.pool;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * A Runnable that is scheduled in the future to report leaks.  The ScheduledFuture is
 * cancelled if the connection is closed before the leak time expires.
 *
 * @author Brett Wooldridge
 */
@Slf4j class ProxyLeakTask implements Runnable {
    static final ProxyLeakTask NO_LEAK;

    private ScheduledFuture<?> scheduledFuture;
    private String connectionName;
    private Exception exception;
    private String threadName;
    private boolean isLeaked;

    static {
        NO_LEAK = new ProxyLeakTask() {
            @Override
            void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
            }

            @Override
            public void run() {
            }

            @Override
            public void cancel() {
            }
        };
    }

    ProxyLeakTask(final PoolEntry poolEntry) {
        this.exception = new Exception("Apparent connection leak detected");
        this.threadName = Thread.currentThread().getName();
        this.connectionName = poolEntry.connection.toString();
    }

    private ProxyLeakTask() {
    }

    void schedule(ScheduledExecutorService executorService, long leakDetectionThreshold) {
        scheduledFuture = executorService.schedule(this, leakDetectionThreshold, TimeUnit.MILLISECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void run() {
        isLeaked = true;

        val stackTrace = exception.getStackTrace();
        val trace = new StackTraceElement[stackTrace.length - 5];
        System.arraycopy(stackTrace, 5, trace, 0, trace.length);

        exception.setStackTrace(trace);
        log.warn("Connection leak detection triggered for {} on thread {}, stack trace follows", connectionName, threadName, exception);
    }

    void cancel() {
        scheduledFuture.cancel(false);
        if (isLeaked) {
            log.info("Previously reported leaked connection {} on thread {} was returned to the pool (unleaked)", connectionName, threadName);
        }
    }
}
