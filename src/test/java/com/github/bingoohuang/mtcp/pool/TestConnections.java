/*
 * Copyright (C) 2013 Brett Wooldridge
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

package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.mocks.StubDataSource;
import com.github.bingoohuang.mtcp.mocks.StubStatement;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.*;

/**
 * @author Brett Wooldridge
 */
public class TestConnections {
    @Before
    public void before() {
        TestElf.setSlf4jTargetStream(LightPool.class, System.err);
        TestElf.setSlf4jLogLevel(LightPool.class, Level.DEBUG);
        TestElf.setSlf4jLogLevel(PoolBase.class, Level.DEBUG);
    }

    @After
    public void after() {
        System.getProperties().remove("com.github.bingoohuang.mtcp.housekeeping.periodMs");
        TestElf.setSlf4jLogLevel(LightPool.class, Level.WARN);
        TestElf.setSlf4jLogLevel(PoolBase.class, Level.WARN);
    }

    @Test
    public void testCreate() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setConnectionInitSql("SELECT 1");
        config.setReadOnly(true);
        config.setConnectionTimeout(2500);
        config.setLeakDetectionThreshold(TimeUnit.SECONDS.toMillis(30));
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            ds.setLoginTimeout(10);
            assertSame(10, ds.getLoginTimeout());

            LightPool pool = TestElf.getPool(ds);
            ds.getConnection().close();
            assertSame("Total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());

            try (Connection connection = ds.getConnection();
                 PreparedStatement statement = connection.prepareStatement("SELECT * FROM device WHERE device_id=?")) {

                assertNotNull(connection);
                assertNotNull(statement);

                assertSame("Total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Idle connections not as expected", 0, pool.getIdleConnections());

                statement.setInt(1, 0);

                try (ResultSet resultSet = statement.executeQuery()) {
                    assertNotNull(resultSet);

                    assertFalse(resultSet.next());
                }
            }

            assertSame("Total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());
        }
    }

    @Test
    public void testMaxLifetime() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "100");

        TestElf.setConfigUnitTest(true);
        try (LightDataSource ds = new LightDataSource(config)) {
            System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");

            TestElf.getUnsealedConfig(ds).setMaxLifetime(700);

            LightPool pool = TestElf.getPool(ds);

            assertSame("Total connections not as expected", 0, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 0, pool.getIdleConnections());

            Connection unwrap;
            Connection unwrap2;
            try (Connection connection = ds.getConnection()) {
                unwrap = connection.unwrap(Connection.class);
                assertNotNull(connection);

                assertSame("Second total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Second idle connections not as expected", 0, pool.getIdleConnections());
            }

            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());

            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertSame(unwrap, unwrap2);
                assertSame("Second total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Second idle connections not as expected", 0, pool.getIdleConnections());
            }

            UtilityElf.quietlySleep(TimeUnit.SECONDS.toMillis(2));

            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertNotSame("Expected a different connection", unwrap, unwrap2);
            }

            assertSame("Post total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Post idle connections not as expected", 1, pool.getIdleConnections());
        } finally {
            TestElf.setConfigUnitTest(false);
        }
    }

    @Test
    public void testMaxLifetime2() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "100");

        TestElf.setConfigUnitTest(true);
        try (LightDataSource ds = new LightDataSource(config)) {
            TestElf.getUnsealedConfig(ds).setMaxLifetime(700);

            LightPool pool = TestElf.getPool(ds);
            assertSame("Total connections not as expected", 0, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 0, pool.getIdleConnections());

            Connection unwrap;
            Connection unwrap2;
            try (Connection connection = ds.getConnection()) {
                unwrap = connection.unwrap(Connection.class);
                assertNotNull(connection);

                assertSame("Second total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Second idle connections not as expected", 0, pool.getIdleConnections());
            }

            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());

            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertSame(unwrap, unwrap2);
                assertSame("Second total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Second idle connections not as expected", 0, pool.getIdleConnections());
            }

            UtilityElf.quietlySleep(800);

            try (Connection connection = ds.getConnection()) {
                unwrap2 = connection.unwrap(Connection.class);
                assertNotSame("Expected a different connection", unwrap, unwrap2);
            }

            assertSame("Post total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Post idle connections not as expected", 1, pool.getIdleConnections());
        } finally {
            TestElf.setConfigUnitTest(false);
        }
    }

    @Test
    public void testDoubleClose() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config);
             Connection connection = ds.getConnection()) {
            connection.close();

            // should no-op
            connection.abort(null);

            assertTrue("Connection should have closed", connection.isClosed());
            assertFalse("Connection should have closed", connection.isValid(5));
            assertTrue("Expected to contain ClosedConnection, but was " + connection, connection.toString().contains("ClosedConnection"));
        }
    }

    @Test
    public void testEviction() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            Connection connection = ds.getConnection();

            LightPool pool = TestElf.getPool(ds);
            assertEquals(1, pool.getTotalConnections());
            ds.evictConnection(connection);
            assertEquals(0, pool.getTotalConnections());
        }
    }

    @Test
    public void testBackfill() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(1000);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        StubConnection.slowCreate = true;
        try (LightDataSource ds = new LightDataSource(config)) {

            LightPool pool = TestElf.getPool(ds);
            UtilityElf.quietlySleep(1250);

            assertSame("Total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());

            // This will take the pool down to zero
            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);

                assertSame("Total connections not as expected", 1, pool.getTotalConnections());
                assertSame("Idle connections not as expected", 0, pool.getIdleConnections());

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

                pool.logPoolState("testBackfill() before close...");

                // The connection will be ejected from the pool here
            }

            assertSame("Total connections not as expected", 0, pool.getTotalConnections());

            pool.logPoolState("testBackfill() after close...");

            UtilityElf.quietlySleep(1250);

            assertSame("Total connections not as expected", 1, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 1, pool.getIdleConnections());
        } finally {
            StubConnection.slowCreate = false;
        }
    }

    @Test
    public void testMaximumPoolLimit() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(4);
        config.setConnectionTimeout(20000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        final AtomicReference<Exception> ref = new AtomicReference<>();

        StubConnection.count.set(0); // reset counter

        try (final LightDataSource ds = new LightDataSource(config)) {

            final LightPool pool = TestElf.getPool(ds);

            Thread[] threads = new Thread[20];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        pool.logPoolState("Before acquire ");
                        try (Connection ignored = ds.getConnection()) {
                            pool.logPoolState("After  acquire ");
                            UtilityElf.quietlySleep(500);
                        }
                    } catch (Exception e) {
                        ref.set(e);
                    }
                });
            }

            for (Thread thread : threads) {
                thread.start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            pool.logPoolState("before check ");
            assertNull((ref.get() != null ? ref.get().toString() : ""), ref.get());
            assertSame("StubConnection count not as expected", 4, StubConnection.count.get());
        }
    }

    @Test
    @SuppressWarnings("EmptyTryBlock")
    public void testOldDriver() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2500);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        StubConnection.oldDriver = true;
        StubStatement.oldDriver = true;
        try (LightDataSource ds = new LightDataSource(config)) {
            UtilityElf.quietlySleep(500);

            try (Connection ignored = ds.getConnection()) {
                // close
            }

            UtilityElf.quietlySleep(500);
            try (Connection ignored = ds.getConnection()) {
                // close
            }
        } finally {
            StubConnection.oldDriver = false;
            StubStatement.oldDriver = false;
        }
    }

    @Test
    public void testInitializationFailure1() {
        StubDataSource stubDataSource = new StubDataSource();
        stubDataSource.setThrowException(new SQLException("Connection refused"));

        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setMinimumIdle(1);
            ds.setMaximumPoolSize(1);
            ds.setConnectionTimeout(2500);
            ds.setConnectionTestQuery("VALUES 1");
            ds.setDataSource(stubDataSource);

            try (Connection ignored = ds.getConnection()) {
                fail("Initialization should have failed");
            } catch (SQLException e) {
                // passed
            }
        }
    }

    @Test
    public void testInitializationFailure2() throws SQLException {
        StubDataSource stubDataSource = new StubDataSource();
        stubDataSource.setThrowException(new SQLException("Connection refused"));

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSource(stubDataSource);

        try (LightDataSource ds = new LightDataSource(config);
             Connection ignored = ds.getConnection()) {
            fail("Initialization should have failed");
        } catch (PoolInitializationException e) {
            // passed
        }
    }

    @Test
    public void testInvalidConnectionTestQuery() {
        class BadConnection extends StubConnection {
            /** {@inheritDoc} */
            @Override
            public Statement createStatement() throws SQLException {
                throw new SQLException("Simulated exception in createStatement()");
            }
        }

        StubDataSource stubDataSource = new StubDataSource() {
            /** {@inheritDoc} */
            @Override
            public Connection getConnection() {
                return new BadConnection();
            }
        };

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(TimeUnit.SECONDS.toMillis(3));
        config.setConnectionTestQuery("VALUES 1");
        config.setInitializationFailTimeout(TimeUnit.SECONDS.toMillis(2));
        config.setDataSource(stubDataSource);

        try (LightDataSource ds = new LightDataSource(config)) {
            try (Connection ignored = ds.getConnection()) {
                fail("getConnection() should have failed");
            } catch (SQLException e) {
                assertSame("Simulated exception in createStatement()", e.getNextException().getMessage());
            }
        } catch (PoolInitializationException e) {
            assertSame("Simulated exception in createStatement()", e.getCause().getMessage());
        }

        config.setInitializationFailTimeout(0);
        try (LightDataSource ignored = new LightDataSource(config)) {
            fail("Initialization should have failed");
        } catch (PoolInitializationException e) {
            // passed
        }
    }

    @Test
    public void testPopulationSlowAcquisition() throws InterruptedException, SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMaximumPoolSize(20);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "1000");

        StubConnection.slowCreate = true;
        try (LightDataSource ds = new LightDataSource(config)) {
            System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");

            TestElf.getUnsealedConfig(ds).setIdleTimeout(3000);

            SECONDS.sleep(2);

            LightPool pool = TestElf.getPool(ds);
            assertSame("Total connections not as expected", 2, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 2, pool.getIdleConnections());

            try (Connection connection = ds.getConnection()) {
                assertNotNull(connection);

                SECONDS.sleep(20);

                assertSame("Second total connections not as expected", 20, pool.getTotalConnections());
                assertSame("Second idle connections not as expected", 19, pool.getIdleConnections());
            }

            assertSame("Idle connections not as expected", 20, pool.getIdleConnections());

            SECONDS.sleep(5);

            assertSame("Third total connections not as expected", 20, pool.getTotalConnections());
            assertSame("Third idle connections not as expected", 20, pool.getIdleConnections());
        } finally {
            StubConnection.slowCreate = false;
        }
    }

    @Test
    @SuppressWarnings("EmptyTryBlock")
    public void testMinimumIdleZero() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000L);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config);
             Connection ignored = ds.getConnection()) {
            // passed
        } catch (SQLTransientConnectionException sqle) {
            fail("Failed to obtain connection");
        }
    }
}
