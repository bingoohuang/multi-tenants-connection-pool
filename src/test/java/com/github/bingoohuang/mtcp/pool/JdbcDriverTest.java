package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.DriverDataSource;
import org.junit.After;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class JdbcDriverTest {
    private LightDataSource ds;

    @After
    public void teardown() {
        if (ds != null) {
            ds.close();
        }
    }

    @Test
    public void driverTest1() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
        config.setJdbcUrl("jdbc:stub");
        config.addDataSourceProperty("user", "bart");
        config.addDataSourceProperty("password", "simpson");

        ds = new LightDataSource(config);

        assertTrue(ds.isWrapperFor(DriverDataSource.class));

        DriverDataSource unwrap = ds.unwrap(DriverDataSource.class);
        assertNotNull(unwrap);

        try (Connection connection = ds.getConnection()) {
            // test that getConnection() succeeds
        }
    }

    @Test
    public void driverTest2() {
        LightConfig config = TestElf.newLightConfig();

        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDriverClassName("com.github.bingoohuang.mtcp.mocks.StubDriver");
        config.setJdbcUrl("jdbc:invalid");

        try {
            ds = new LightDataSource(config);
        } catch (RuntimeException e) {
            assertTrue(e.getMessage().contains("claims to not accept"));
        }
    }
}
