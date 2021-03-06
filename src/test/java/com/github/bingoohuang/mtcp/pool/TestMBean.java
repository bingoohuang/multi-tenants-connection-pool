package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightConfigMXBean;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.LightPoolMXBean;
import org.junit.Test;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TestMBean {
    @Test
    public void testMBeanRegistration() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(0);
        config.setMaxPoolSize(1);
        config.setRegisterMbeans(true);
        config.setConnectionTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        new LightDataSource(config).close();
    }

    @Test
    public void testMBeanReporting() throws SQLException, InterruptedException, MalformedObjectNameException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(3);
        config.setMaxPoolSize(5);
        config.setRegisterMbeans(true);
        config.setConnectionTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "100");

        try (LightDataSource ds = new LightDataSource(config)) {

            TestElf.getUnsealedConfig(ds).setIdleTimeout(3000);

            TimeUnit.SECONDS.sleep(1);

            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            ObjectName poolName = new ObjectName("com.github.bingoohuang.mtcp:type=Pool (testMBeanReporting)");
            LightPoolMXBean lightPoolMXBean = JMX.newMXBeanProxy(mBeanServer, poolName, LightPoolMXBean.class);

            assertEquals(0, lightPoolMXBean.getActiveConnections());
            assertEquals(3, lightPoolMXBean.getIdleConnections());

            try (Connection ignored = ds.getConnection()) {
                assertEquals(1, lightPoolMXBean.getActiveConnections());

                TimeUnit.SECONDS.sleep(1);

                assertEquals(3, lightPoolMXBean.getIdleConnections());
                assertEquals(4, lightPoolMXBean.getTotalConnections());
            }

            TimeUnit.SECONDS.sleep(2);

            assertEquals(0, lightPoolMXBean.getActiveConnections());
            assertEquals(3, lightPoolMXBean.getIdleConnections());
            assertEquals(3, lightPoolMXBean.getTotalConnections());

        } finally {
            System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");
        }
    }

    @Test
    public void testMBeanChange() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(3);
        config.setMaxPoolSize(5);
        config.setRegisterMbeans(true);
        config.setConnectionTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            LightConfigMXBean lightConfigMXBean = ds.getLightConfigMXBean();
            lightConfigMXBean.setIdleTimeout(3000);

            assertEquals(3000, ds.getIdleTimeout());
        }
    }
}
