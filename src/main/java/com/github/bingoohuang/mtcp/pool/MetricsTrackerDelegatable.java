package com.github.bingoohuang.mtcp.pool;

public interface MetricsTrackerDelegatable extends AutoCloseable {
    default void recordConnectionUsage(PoolEntry poolEntry) {
    }

    default void recordConnectionCreated(long connectionCreatedMillis) {
    }

    default void recordBorrowTimeoutStats(long startTime) {
    }

    default void recordBorrowStats(final PoolEntry poolEntry, final long startTime) {
    }

    default void recordConnectionTimeout() {
    }

    @Override
    default void close() {
    }
}
