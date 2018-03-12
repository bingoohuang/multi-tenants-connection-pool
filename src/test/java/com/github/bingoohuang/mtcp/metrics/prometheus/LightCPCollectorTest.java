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
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import io.prometheus.client.CollectorRegistry;
import org.junit.Test;

import java.sql.Connection;

import static com.github.bingoohuang.mtcp.pool.TestElf.newLightConfig;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LightCPCollectorTest {
   @Test
   public void noConnection() throws Exception {
      LightConfig config = newLightConfig();
      config.setMinimumIdle(0);
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      StubConnection.slowCreate = true;
      try (LightDataSource ds = new LightDataSource(config)) {
         assertThat(getValue("hikaricp_active_connections", "noConnection"), is(0.0));
         assertThat(getValue("hikaricp_idle_connections", "noConnection"), is(0.0));
         assertThat(getValue("hikaricp_pending_threads", "noConnection"), is(0.0));
         assertThat(getValue("hikaricp_connections", "noConnection"), is(0.0));
      } finally {
         StubConnection.slowCreate = false;
      }
   }

   @Test
   public void noConnectionWithoutPoolName() throws Exception {
      LightConfig config = new LightConfig();
      config.setMinimumIdle(0);
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

      StubConnection.slowCreate = true;
      try (LightDataSource ds = new LightDataSource(config)) {
         String poolName = ds.getLightConfigMXBean().getPoolName();
         assertThat(getValue("hikaricp_active_connections", poolName), is(0.0));
         assertThat(getValue("hikaricp_idle_connections", poolName), is(0.0));
         assertThat(getValue("hikaricp_pending_threads", poolName), is(0.0));
         assertThat(getValue("hikaricp_connections", poolName), is(0.0));
      } finally {
         StubConnection.slowCreate = false;
      }
   }

   @Test
   public void connection1() throws Exception {
      LightConfig config = newLightConfig();
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
      config.setMaximumPoolSize(1);

      StubConnection.slowCreate = true;
      try (LightDataSource ds = new LightDataSource(config);
           Connection connection1 = ds.getConnection()) {

         UtilityElf.quietlySleep(1000);

         assertThat(getValue("hikaricp_active_connections", "connection1"), is(1.0));
         assertThat(getValue("hikaricp_idle_connections", "connection1"), is(0.0));
         assertThat(getValue("hikaricp_pending_threads", "connection1"), is(0.0));
         assertThat(getValue("hikaricp_connections", "connection1"), is(1.0));
      } finally {
         StubConnection.slowCreate = false;
      }
   }

   @Test
   public void connectionClosed() throws Exception {
      LightConfig config = newLightConfig();
      config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
      config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
      config.setMaximumPoolSize(1);

      StubConnection.slowCreate = true;
      try (LightDataSource ds = new LightDataSource(config)) {
         try (Connection connection1 = ds.getConnection()) {
            // close immediately
         }

         assertThat(getValue("hikaricp_active_connections", "connectionClosed"), is(0.0));
         assertThat(getValue("hikaricp_idle_connections", "connectionClosed"), is(1.0));
         assertThat(getValue("hikaricp_pending_threads", "connectionClosed"), is(0.0));
         assertThat(getValue("hikaricp_connections", "connectionClosed"), is(1.0));
      } finally {
         StubConnection.slowCreate = false;
      }
   }

   private double getValue(String name, String poolName) {
      String[] labelNames = {"pool"};
      String[] labelValues = {poolName};
      return CollectorRegistry.defaultRegistry.getSampleValue(name, labelNames, labelValues);
   }

}
