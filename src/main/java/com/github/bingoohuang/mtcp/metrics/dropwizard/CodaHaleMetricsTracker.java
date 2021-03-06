package com.github.bingoohuang.mtcp.metrics.dropwizard;

import com.codahale.metrics.*;
import com.github.bingoohuang.mtcp.metrics.MetricsTracker;
import com.github.bingoohuang.mtcp.metrics.PoolStats;
import lombok.Getter;

import java.util.concurrent.TimeUnit;

public final class CodaHaleMetricsTracker implements MetricsTracker {
    private final String poolName;
    @Getter private final Timer connectionObtainTimer;
    @Getter private final Histogram connectionUsage;
    @Getter private final Histogram connectionCreation;
    private final Meter connectionTimeoutMeter;
    private final MetricRegistry registry;

    private static final String METRIC_CATEGORY = "pool";
    private static final String METRIC_NAME_WAIT = "Wait";
    private static final String METRIC_NAME_USAGE = "Usage";
    private static final String METRIC_NAME_CONNECT = "ConnectionCreation";
    private static final String METRIC_NAME_TIMEOUT_RATE = "ConnectionTimeoutRate";
    private static final String METRIC_NAME_TOTAL_CONNECTIONS = "TotalConnections";
    private static final String METRIC_NAME_IDLE_CONNECTIONS = "IdleConnections";
    private static final String METRIC_NAME_ACTIVE_CONNECTIONS = "ActiveConnections";
    private static final String METRIC_NAME_PENDING_CONNECTIONS = "PendingConnections";

    public CodaHaleMetricsTracker(final String poolName, final PoolStats poolStats, final MetricRegistry registry) {
        this.poolName = poolName;
        this.registry = registry;
        this.connectionObtainTimer = registry.timer(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_WAIT));
        this.connectionUsage = registry.histogram(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_USAGE));
        this.connectionCreation = registry.histogram(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_CONNECT));
        this.connectionTimeoutMeter = registry.meter(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TIMEOUT_RATE));

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TOTAL_CONNECTIONS),
                (Gauge<Integer>) () -> poolStats.getTotalConnections());

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_IDLE_CONNECTIONS),
                (Gauge<Integer>) () -> poolStats.getIdleConnections());

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_ACTIVE_CONNECTIONS),
                (Gauge<Integer>) () -> poolStats.getActiveConnections());

        registry.register(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_PENDING_CONNECTIONS),
                (Gauge<Integer>) () -> poolStats.getPendingThreads());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_WAIT));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_USAGE));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_CONNECT));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TIMEOUT_RATE));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_TOTAL_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_IDLE_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_ACTIVE_CONNECTIONS));
        registry.remove(MetricRegistry.name(poolName, METRIC_CATEGORY, METRIC_NAME_PENDING_CONNECTIONS));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordConnectionAcquiredNanos(final long elapsedAcquiredNanos) {
        connectionObtainTimer.update(elapsedAcquiredNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordConnectionUsageMillis(final long elapsedBorrowedMillis) {
        connectionUsage.update(elapsedBorrowedMillis);
    }

    @Override
    public void recordConnectionTimeout() {
        connectionTimeoutMeter.mark();
    }

    @Override
    public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
        connectionCreation.update(connectionCreatedMillis);
    }
}
