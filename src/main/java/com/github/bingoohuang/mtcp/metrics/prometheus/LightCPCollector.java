package com.github.bingoohuang.mtcp.metrics.prometheus;

import com.github.bingoohuang.mtcp.metrics.PoolStats;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class LightCPCollector extends Collector {

    private static final List<String> LABEL_NAMES = Collections.singletonList("pool");

    private final Map<String, PoolStats> poolStatsMap = new ConcurrentHashMap<>();

    @Override
    public List<MetricFamilySamples> collect() {
        return Arrays.asList(
                createGauge("lightcp_active_connections", "Active connections",
                        PoolStats::getActiveConnections),
                createGauge("lightcp_idle_connections", "Idle connections",
                        PoolStats::getIdleConnections),
                createGauge("lightcp_pending_threads", "Pending threads",
                        PoolStats::getPendingThreads),
                createGauge("lightcp_connections", "The number of current connections",
                        PoolStats::getTotalConnections)
        );
    }

    protected LightCPCollector add(String name, PoolStats poolStats) {
        poolStatsMap.put(name, poolStats);
        return this;
    }

    private GaugeMetricFamily createGauge(String metric, String help,
                                          Function<PoolStats, Integer> metricValueFunction) {
        GaugeMetricFamily metricFamily = new GaugeMetricFamily(metric, help, LABEL_NAMES);
        poolStatsMap.forEach((k, v) -> metricFamily.addMetric(
                Collections.singletonList(k),
                metricValueFunction.apply(v)
        ));
        return metricFamily;
    }
}
