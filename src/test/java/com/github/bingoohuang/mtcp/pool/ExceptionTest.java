package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

public class ExceptionTest {
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
    public void testException1() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);

            PreparedStatement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?");
            assertNotNull(statement);

            ResultSet resultSet = statement.executeQuery();
            assertNotNull(resultSet);

            try {
                statement.getMaxFieldSize();
                fail();
            } catch (Exception e) {
                assertSame(SQLException.class, e.getClass());
            }
        }

        LightPool pool = TestElf.getPool(ds);
        assertTrue("Total (3) connections not as expected", pool.getTotalConnections() >= 0);
        assertTrue("Idle (3) connections not as expected", pool.getIdleConnections() >= 0);
    }

    @Test
    public void testUseAfterStatementClose() throws SQLException {
        Connection connection = ds.getConnection();
        assertNotNull(connection);

        try (Statement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?")) {
            statement.close();
            statement.getMoreResults();

            fail();
        } catch (SQLException e) {
            assertSame("Connection is closed", e.getMessage());
        }
    }

    @Test
    public void testUseAfterClose() throws SQLException {
        try (Connection connection = ds.getConnection()) {
            assertNotNull(connection);
            connection.close();

            try (Statement statement = connection.prepareStatement("SELECT some, thing FROM somewhere WHERE something=?")) {
                fail();
            } catch (SQLException e) {
                assertSame("Connection is closed", e.getMessage());
            }
        }
    }
}
