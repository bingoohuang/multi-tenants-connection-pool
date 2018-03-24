package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StatementTest {
    private LightDataSource ds;

    @Before
    public void setup() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinIdle(1);
        config.setMaxPoolSize(2);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        ds = new LightDataSource(config);
    }

    @After
    public void teardown() {
        ds.close();
    }

    @Test
    public void testStatementClose() throws SQLException {
        ds.getConnection().close();

        LightPool pool = TestElf.getPool(ds);
        assertTrue("Total connections not as expected", pool.getTotalConnections() >= 1);
        assertTrue("Idle connections not as expected", pool.getIdleConnections() >= 1);

        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);

            assertTrue("Total connections not as expected", pool.getTotalConnections() >= 1);
            assertTrue("Idle connections not as expected", pool.getIdleConnections() >= 0);

            Statement statement = connection.createStatement();
            assertNotNull(statement);

            connection.close();

            assertTrue(statement.isClosed());
        }
    }

    @Test
    public void testAutoStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);

            Statement statement1 = connection.createStatement();
            assertNotNull(statement1);
            Statement statement2 = connection.createStatement();
            assertNotNull(statement2);

            connection.close();

            assertTrue(statement1.isClosed());
            assertTrue(statement2.isClosed());
        }
    }

    @Test
    public void testStatementResultSetProxyClose() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);

            Statement statement1 = connection.createStatement();
            assertNotNull(statement1);
            Statement statement2 = connection.createStatement();
            assertNotNull(statement2);

            statement1.getResultSet().getStatement().close();
            statement2.getGeneratedKeys().getStatement().close();

            assertTrue(statement1.isClosed());
            assertTrue(statement2.isClosed());
        }
    }

    @Test
    public void testDoubleStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection();
             Statement statement1 = connection.createStatement()) {
            statement1.close();
            statement1.close();
        }
    }

    @Test
    public void testOutOfOrderStatementClose() throws SQLException {
        try (Connection connection = ds.getConnection();
             Statement statement1 = connection.createStatement();
             Statement statement2 = connection.createStatement()) {
            statement1.close();
            statement2.close();
        }
    }
}
