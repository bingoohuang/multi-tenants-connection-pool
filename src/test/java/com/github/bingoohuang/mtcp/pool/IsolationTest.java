package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

public class IsolationTest {
    @Test
    public void testIsolation() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setMinimumIdle(1);
            ds.setMaximumPoolSize(1);
            ds.setIsolateInternalQueries(true);
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                connection.close();

                try (Connection connection2 = ds.getConnection()) {
                    connection2.close();

                    assertNotSame(connection, connection2);
                    assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
                }
            }
        }
    }

    @Test
    public void testNonIsolation() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setMinimumIdle(1);
            ds.setMaximumPoolSize(1);
            ds.setIsolateInternalQueries(false);
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                connection.close();

                try (Connection connection2 = ds.getConnection()) {
                    connection2.close();

                    assertSame(connection.unwrap(Connection.class), connection2.unwrap(Connection.class));
                }
            }
        }
    }
}
