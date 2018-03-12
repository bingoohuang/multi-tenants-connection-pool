/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
