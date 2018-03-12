package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.metrics.MetricsTracker;
import com.github.bingoohuang.mtcp.util.ClockSource;

/**
 * A class that delegates to a MetricsTracker implementation.  The use of a delegate
 * allows us to use the NopMetricsTrackerDelegate when metrics are disabled, which in
 * turn allows the JIT to completely optimize away to callsites to record metrics.
 */
class MetricsTrackerDelegate implements MetricsTrackerDelegatable {
    final MetricsTracker tracker;

    MetricsTrackerDelegate(MetricsTracker tracker) {
        this.tracker = tracker;
    }

    @Override
    public void recordConnectionUsage(final PoolEntry poolEntry) {
        tracker.recordConnectionUsageMillis(poolEntry.getMillisSinceBorrowed());
    }

    @Override
    public void recordConnectionCreated(long connectionCreatedMillis) {
        tracker.recordConnectionCreatedMillis(connectionCreatedMillis);
    }

    @Override
    public void recordBorrowTimeoutStats(long startTime) {
        tracker.recordConnectionAcquiredNanos(ClockSource.elapsedNanos(startTime));
    }

    @Override
    public void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
        final long now = ClockSource.currentTime();
        poolEntry.lastBorrowed = now;
        tracker.recordConnectionAcquiredNanos(ClockSource.elapsedNanos(startTime, now));
    }

    @Override
    public void recordConnectionTimeout() {
        tracker.recordConnectionTimeout();
    }

    @Override
    public void close() {
        tracker.close();
    }
}
