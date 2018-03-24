package com.github.bingoohuang.mtcp.metrics;

public interface MetricsTrackerFactory {
    /**
     * Create an instance of an MetricsTracker.
     *
     * @param poolName  the name of the pool
     * @param poolStats a PoolStats instance to use
     * @return a MetricsTracker implementation instance
     */
    MetricsTracker create(String poolName, PoolStats poolStats);
}
