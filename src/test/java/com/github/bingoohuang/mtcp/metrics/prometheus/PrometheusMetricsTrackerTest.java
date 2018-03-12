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

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import io.prometheus.client.CollectorRegistry;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLTransientConnectionException;

import static com.github.bingoohuang.mtcp.pool.TestElf.newLightConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class PrometheusMetricsTrackerTest {

   private CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry;

   private static final String POOL_LABEL_NAME = "pool";
   private static final String QUANTILE_LABEL_NAME = "quantile";
   private static final String[] QUANTILE_LABEL_VALUES = new String[]{"0.5", "0.95", "0.99"};

   @Test
   public void recordConnectionTimeout() throws Exception {
      LightConfig config = newLightConfig();
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setJdbcUrl("jdbc:h2:mem:");
      config.setMaximumPoolSize(2);
      config.setConnectionTimeout(250);

      String[] labelNames = {POOL_LABEL_NAME};
      String[] labelValues = {config.getPoolName()};

      try (LightDataSource lightDataSource = new LightDataSource(config)) {
         try (Connection connection1 = lightDataSource.getConnection();
              Connection connection2 = lightDataSource.getConnection()) {
            try (Connection connection3 = lightDataSource.getConnection()) {
            } catch (SQLTransientConnectionException ignored) {
            }
         }

         Double total = collectorRegistry.getSampleValue(
            "lightcp_connection_timeout_total",
            labelNames,
            labelValues
         );
         assertThat(total, is(1.0));
      }
   }

   @Test
   public void connectionAcquisitionMetrics() {
      checkSummaryMetricFamily("lightcp_connection_acquired_nanos");
   }

   @Test
   public void connectionUsageMetrics() {
      checkSummaryMetricFamily("lightcp_connection_usage_millis");
   }

   @Test
   public void connectionCreationMetrics() {
      checkSummaryMetricFamily("lightcp_connection_creation_millis");
   }

   @Test
   public void testMultiplePoolName() throws Exception {
      String[] labelNames = {POOL_LABEL_NAME};

      LightConfig config = newLightConfig();
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setPoolName("first");
      config.setJdbcUrl("jdbc:h2:mem:");
      config.setMaximumPoolSize(2);
      config.setConnectionTimeout(250);
      String[] labelValues1 = {config.getPoolName()};

      try (LightDataSource ignored = new LightDataSource(config)) {
         assertThat(collectorRegistry.getSampleValue(
            "lightcp_connection_timeout_total",
            labelNames,
            labelValues1), is(0.0));

         LightConfig config2 = newLightConfig();
         config2.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
         config2.setPoolName("second");
         config2.setJdbcUrl("jdbc:h2:mem:");
         config2.setMaximumPoolSize(4);
         config2.setConnectionTimeout(250);
         String[] labelValues2 = {config2.getPoolName()};

         try (LightDataSource ignored2 = new LightDataSource(config2)) {
            assertThat(collectorRegistry.getSampleValue(
               "lightcp_connection_timeout_total",
               labelNames,
               labelValues2), is(0.0));
         }
      }
   }

   private void checkSummaryMetricFamily(String metricName) {
      LightConfig config = newLightConfig();
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setJdbcUrl("jdbc:h2:mem:");

      try (LightDataSource ignored = new LightDataSource(config)) {
         Double count = collectorRegistry.getSampleValue(
            metricName + "_count",
            new String[]{POOL_LABEL_NAME},
            new String[]{config.getPoolName()}
         );
         assertNotNull(count);

         Double sum = collectorRegistry.getSampleValue(
            metricName + "_sum",
            new String[]{POOL_LABEL_NAME},
            new String[]{config.getPoolName()}
         );
         assertNotNull(sum);

         for (String quantileLabelValue : QUANTILE_LABEL_VALUES) {
            Double quantileValue = collectorRegistry.getSampleValue(
               metricName,
               new String[]{POOL_LABEL_NAME, QUANTILE_LABEL_NAME},
               new String[]{config.getPoolName(), quantileLabelValue}
            );
            assertNotNull("q = " + quantileLabelValue, quantileValue);
         }
      }
   }
}
