package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.mocks.StubDataSource;
import com.github.bingoohuang.mtcp.util.ClockSource;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.*;

public class TestConnectionTimeoutRetry {
    @Test
    public void testConnectionRetries() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));

            long start = ClockSource.currentTime();
            try (Connection connection = ds.getConnection()) {
                connection.close();
                fail("Should not have been able to get a connection.");
            } catch (SQLException e) {
                long elapsed = ClockSource.elapsedMillis(start);
                long timeout = config.getConnectionTimeout();
                assertTrue("Didn't wait long enough for timeout", (elapsed >= timeout));
            }
        }
    }

    @Test
    public void testConnectionRetries2() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            final StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    stubDataSource.setThrowException(null);
                }
            }, 300, TimeUnit.MILLISECONDS);

            long start = ClockSource.currentTime();
            try {
                try (Connection connection = ds.getConnection()) {
                    // close immediately
                }

                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue("Connection returned too quickly, something is wrong.", elapsed > 250);
                assertTrue("Waited too long to get a connection.", elapsed < config.getConnectionTimeout());
            } catch (SQLException e) {
                fail("Should not have timed out: " + e.getMessage());
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionRetries3() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(2800);
        config.setValidationTimeout(2800);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            final Connection connection1 = ds.getConnection();
            final Connection connection2 = ds.getConnection();
            assertNotNull(connection1);
            assertNotNull(connection2);

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        connection1.close();
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                }
            }, 800, MILLISECONDS);

            long start = ClockSource.currentTime();
            try {
                try (Connection connection3 = ds.getConnection()) {
                    // close immediately
                }

                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue("Waited too long to get a connection.", (elapsed >= 700) && (elapsed < 950));
            } catch (SQLException e) {
                fail("Should not have timed out.");
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionRetries5() throws SQLException {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(2);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            final Connection connection1 = ds.getConnection();

            long start = ClockSource.currentTime();

            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);
            scheduler.schedule(new Runnable() {
                @Override
                public void run() {
                    try {
                        connection1.close();
                    } catch (Exception e) {
                        e.printStackTrace(System.err);
                    }
                }
            }, 250, MILLISECONDS);

            StubDataSource stubDataSource = ds.unwrap(StubDataSource.class);
            stubDataSource.setThrowException(new SQLException("Connection refused"));

            try {
                try (Connection connection2 = ds.getConnection()) {
                    // close immediately
                }

                long elapsed = ClockSource.elapsedMillis(start);
                assertTrue("Waited too long to get a connection.", (elapsed >= 250) && (elapsed < config.getConnectionTimeout()));
            } catch (SQLException e) {
                fail("Should not have timed out.");
            } finally {
                scheduler.shutdownNow();
            }
        }
    }

    @Test
    public void testConnectionIdleFill() throws Exception {
        StubConnection.slowCreate = false;

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(10);
        config.setConnectionTimeout(2000);
        config.setValidationTimeout(2000);
        config.setConnectionTestQuery("VALUES 2");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "400");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos, true);
        TestElf.setSlf4jTargetStream(LightPool.class, ps);

        try (LightDataSource ds = new LightDataSource(config)) {
            TestElf.setSlf4jLogLevel(LightPool.class, Level.DEBUG);

            LightPool pool = TestElf.getPool(ds);
            try (
                    Connection connection1 = ds.getConnection();
                    Connection connection2 = ds.getConnection();
                    Connection connection3 = ds.getConnection();
                    Connection connection4 = ds.getConnection();
                    Connection connection5 = ds.getConnection();
                    Connection connection6 = ds.getConnection();
                    Connection connection7 = ds.getConnection()) {

                sleep(1300);

                assertSame("Total connections not as expected", 10, pool.getTotalConnections());
                assertSame("Idle connections not as expected", 3, pool.getIdleConnections());
            }

            assertSame("Total connections not as expected", 10, pool.getTotalConnections());
            assertSame("Idle connections not as expected", 10, pool.getIdleConnections());
        }
    }

    @Before
    public void before() {
        TestElf.setSlf4jLogLevel(LightPool.class, Level.INFO);
    }

    @After
    public void after() {
        System.getProperties().remove("com.github.bingoohuang.mtcp.housekeeping.periodMs");
        TestElf.setSlf4jLogLevel(LightPool.class, Level.INFO);
    }
}
