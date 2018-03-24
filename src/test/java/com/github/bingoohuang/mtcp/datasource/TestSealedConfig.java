package com.github.bingoohuang.mtcp.datasource;

import com.github.bingoohuang.mtcp.LightDataSource;
import lombok.val;
import org.junit.Test;

import static com.github.bingoohuang.mtcp.pool.TestElf.newLightConfig;
import static org.junit.Assert.fail;

public class TestSealedConfig {
    @Test(expected = IllegalStateException.class)
    public void testSealed1() {
        val config = newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (val ds = new LightDataSource(config)) {
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
            fail("Exception should have been thrown");
        }
    }

    @Test
    public void testSealedAccessibleMethods() {
        val config = newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            ds.setConnectionTimeout(5000);
            ds.setValidationTimeout(5000);
            ds.setIdleTimeout(30000);
            ds.setLeakDetectionThreshold(60000);
            ds.setMaxLifetime(1800000);
            ds.setMinIdle(5);
            ds.setMaxPoolSize(8);
            ds.setPassword("password");
            ds.setUsername("username");
        }
    }
}
