package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.*;

public class ConnectionStateTest {
    @Test
    public void testAutoCommit() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setAutoCommit(true);
            ds.setMinIdle(1);
            ds.setMaxPoolSize(1);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
            ds.addDataSourceProperty("user", "bar");
            ds.addDataSourceProperty("password", "secret");
            ds.addDataSourceProperty("url", "baf");
            ds.addDataSourceProperty("loginTimeout", "10");

            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setAutoCommit(false);
                connection.close();

                assertTrue(unwrap.getAutoCommit());
            }
        }
    }

    @Test
    public void testTransactionIsolation() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setTransactionIsolation("TRANSACTION_READ_COMMITTED");
            ds.setMinIdle(1);
            ds.setMaxPoolSize(1);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
                connection.close();

                assertEquals(Connection.TRANSACTION_READ_COMMITTED, unwrap.getTransactionIsolation());
            }
        }
    }

    @Test
    public void testIsolation() {
        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        config.setTransactionIsolation("TRANSACTION_REPEATABLE_READ");
        config.validate();

        int transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());
        assertSame(Connection.TRANSACTION_REPEATABLE_READ, transactionIsolation);
    }

    @Test
    public void testReadOnly() throws Exception {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setCatalog("test");
            ds.setMinIdle(1);
            ds.setMaxPoolSize(1);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setReadOnly(true);
                connection.close();

                assertFalse(unwrap.isReadOnly());
            }
        }
    }

    @Test
    public void testCatalog() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setCatalog("test");
            ds.setMinIdle(1);
            ds.setMaxPoolSize(1);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                Connection unwrap = connection.unwrap(Connection.class);
                connection.setCatalog("other");
                connection.close();

                assertEquals("test", unwrap.getCatalog());
            }
        }
    }

    @Test
    public void testCommitTracking() throws SQLException {
        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setAutoCommit(false);
            ds.setMinIdle(1);
            ds.setMaxPoolSize(1);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

            try (Connection connection = ds.getConnection()) {
                Statement statement = connection.createStatement();
                statement.execute("SELECT something");
                assertTrue(TestElf.getConnectionCommitDirtyState(connection));

                connection.commit();
                assertFalse(TestElf.getConnectionCommitDirtyState(connection));

                statement.execute("SELECT something", Statement.NO_GENERATED_KEYS);
                assertTrue(TestElf.getConnectionCommitDirtyState(connection));

                connection.rollback();
                assertFalse(TestElf.getConnectionCommitDirtyState(connection));

                ResultSet resultSet = statement.executeQuery("SELECT something");
                assertTrue(TestElf.getConnectionCommitDirtyState(connection));

                connection.rollback(null);
                assertFalse(TestElf.getConnectionCommitDirtyState(connection));

                resultSet.updateRow();
                assertTrue(TestElf.getConnectionCommitDirtyState(connection));
            }
        }
    }
}
