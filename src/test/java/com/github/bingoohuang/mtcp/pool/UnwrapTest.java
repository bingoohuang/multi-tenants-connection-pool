package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.mocks.StubDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * @author Brett Wooldridge
 */
public class UnwrapTest {
    @Test
    public void testUnwrapConnection() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(1);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            ds.getConnection().close();
            assertSame("Idle connections not as expected", 1, TestElf.getPool(ds).getIdleConnections());

            Connection connection = ds.getConnection();
            assertNotNull(connection);

            StubConnection unwrapped = connection.unwrap(StubConnection.class);
            assertTrue("unwrapped connection is not instance of StubConnection: " + unwrapped, (unwrapped != null && unwrapped instanceof StubConnection));
        }
    }

    @Test
    public void testUnwrapDataSource() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(1);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            StubDataSource unwrap = ds.unwrap(StubDataSource.class);
            assertNotNull(unwrap);
            assertTrue(unwrap instanceof StubDataSource);

            assertTrue(ds.isWrapperFor(LightDataSource.class));
            assertTrue(ds.unwrap(LightDataSource.class) instanceof LightDataSource);

            assertFalse(ds.isWrapperFor(getClass()));
            try {
                ds.unwrap(getClass());
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Wrapped DataSource"));
            }
        }
    }
}
