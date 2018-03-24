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
    public void noConnection() {
        LightConfig config = newLightConfig();
        config.setMinimumIdle(0);
        config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        StubConnection.slowCreate = true;
        try (LightDataSource ds = new LightDataSource(config)) {
            assertThat(getValue("lightcp_active_connections", "noConnection"), is(0.0));
            assertThat(getValue("lightcp_idle_connections", "noConnection"), is(0.0));
            assertThat(getValue("lightcp_pending_threads", "noConnection"), is(0.0));
            assertThat(getValue("lightcp_connections", "noConnection"), is(0.0));
        } finally {
            StubConnection.slowCreate = false;
        }
    }

    @Test
    public void noConnectionWithoutPoolName() {
        LightConfig config = new LightConfig();
        config.setMinimumIdle(0);
        config.setMetricsTrackerFactory(new PrometheusMetricsTrackerFactory());
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        StubConnection.slowCreate = true;
        try (LightDataSource ds = new LightDataSource(config)) {
            String poolName = ds.getLightConfigMXBean().getPoolName();
            assertThat(getValue("lightcp_active_connections", poolName), is(0.0));
            assertThat(getValue("lightcp_idle_connections", poolName), is(0.0));
            assertThat(getValue("lightcp_pending_threads", poolName), is(0.0));
            assertThat(getValue("lightcp_connections", poolName), is(0.0));
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

            assertThat(getValue("lightcp_active_connections", "connection1"), is(1.0));
            assertThat(getValue("lightcp_idle_connections", "connection1"), is(0.0));
            assertThat(getValue("lightcp_pending_threads", "connection1"), is(0.0));
            assertThat(getValue("lightcp_connections", "connection1"), is(1.0));
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

            assertThat(getValue("lightcp_active_connections", "connectionClosed"), is(0.0));
            assertThat(getValue("lightcp_idle_connections", "connectionClosed"), is(1.0));
            assertThat(getValue("lightcp_pending_threads", "connectionClosed"), is(0.0));
            assertThat(getValue("lightcp_connections", "connectionClosed"), is(1.0));
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
