package com.github.bingoohuang.mtcp.metrics.prometheus;


import com.github.bingoohuang.mtcp.metrics.MetricsTracker;
import com.github.bingoohuang.mtcp.metrics.MetricsTrackerFactory;
import com.github.bingoohuang.mtcp.metrics.PoolStats;

/**
 * <pre>{@code
 * LightConfig config = new LightConfig();
 * config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
 * }</pre>
 */
public class PrometheusMetricsTrackerFactory implements MetricsTrackerFactory {

    private static LightCPCollector collector;

    @Override
    public MetricsTracker create(String poolName, PoolStats poolStats) {
        getCollector().add(poolName, poolStats);
        return new PrometheusMetricsTracker(poolName);
    }

    /**
     * initialize and register collector if it isn't initialized yet
     */
    private LightCPCollector getCollector() {
        if (collector == null) {
            collector = new LightCPCollector().register();
        }
        return collector;
    }
}
