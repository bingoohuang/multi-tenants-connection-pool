package com.github.bingoohuang.mtcp.pool;

import lombok.val;

import java.util.concurrent.ScheduledExecutorService;

/**
 * A factory for {@link ProxyLeakTask} Runnables that are scheduled in the future to report leaks.
 *
 * @author Brett Wooldridge
 * @author Andreas Brenk
 */
class ProxyLeakTaskFactory {
    private final ScheduledExecutorService executorService;
    private long leakDetectionThreshold;

    ProxyLeakTaskFactory(final long leakDetectionThreshold, final ScheduledExecutorService executorService) {
        this.executorService = executorService;
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    ProxyLeakTask schedule(final PoolEntry poolEntry) {
        return (leakDetectionThreshold == 0) ? ProxyLeakTask.NO_LEAK : scheduleNewTask(poolEntry);
    }

    void updateLeakDetectionThreshold(final long leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
    }

    private ProxyLeakTask scheduleNewTask(PoolEntry poolEntry) {
        val task = new ProxyLeakTask(poolEntry);
        task.schedule(executorService, leakDetectionThreshold);

        return task;
    }
}
