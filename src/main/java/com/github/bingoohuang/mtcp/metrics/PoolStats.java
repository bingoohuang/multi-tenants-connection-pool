package com.github.bingoohuang.mtcp.metrics;

import java.util.concurrent.atomic.AtomicLong;

import static com.github.bingoohuang.mtcp.util.ClockSource.currentTime;
import static com.github.bingoohuang.mtcp.util.ClockSource.plusMillis;


/**
 * @author Brett Wooldridge
 */
public abstract class PoolStats {
    private final AtomicLong reloadAt;
    private final long timeoutMs;

    protected volatile int totalConnections;
    protected volatile int idleConnections;
    protected volatile int activeConnections;
    protected volatile int pendingThreads;

    public PoolStats(final long timeoutMs) {
        this.timeoutMs = timeoutMs;
        this.reloadAt = new AtomicLong();
    }

    public int getTotalConnections() {
        if (shouldLoad()) {
            update();
        }

        return totalConnections;
    }

    public int getIdleConnections() {
        if (shouldLoad()) {
            update();
        }

        return idleConnections;
    }

    public int getActiveConnections() {
        if (shouldLoad()) {
            update();
        }

        return activeConnections;
    }

    public int getPendingThreads() {
        if (shouldLoad()) {
            update();
        }

        return pendingThreads;
    }

    protected abstract void update();

    private boolean shouldLoad() {
        for (; ; ) {
            final long now = currentTime();
            final long reloadTime = reloadAt.get();
            if (reloadTime > now) {
                return false;
            } else if (reloadAt.compareAndSet(reloadTime, plusMillis(now, timeoutMs))) {
                return true;
            }
        }
    }
}
