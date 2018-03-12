package com.github.bingoohuang.mtcp.metrics.micrometer;

import com.github.bingoohuang.mtcp.metrics.IMetricsTracker;
import com.github.bingoohuang.mtcp.metrics.MetricsTrackerFactory;
import com.github.bingoohuang.mtcp.metrics.PoolStats;
import io.micrometer.core.instrument.MeterRegistry;

public class MicrometerMetricsTrackerFactory implements MetricsTrackerFactory {

   private final MeterRegistry registry;

   public MicrometerMetricsTrackerFactory(MeterRegistry registry) {
      this.registry = registry;
   }

   @Override
   public IMetricsTracker create(String poolName, PoolStats poolStats) {
      return new MicrometerMetricsTracker(poolName, poolStats, registry);
   }
}
