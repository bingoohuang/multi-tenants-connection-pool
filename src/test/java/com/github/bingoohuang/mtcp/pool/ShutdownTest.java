
package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * @author Brett Wooldridge
 */
public class ShutdownTest {
    @Before
    public void beforeTest() {
        TestElf.setSlf4jLogLevel(PoolBase.class, Level.DEBUG);
        TestElf.setSlf4jLogLevel(LightPool.class, Level.DEBUG);
        StubConnection.count.set(0);
    }

    @After
    public void afterTest() {
        TestElf.setSlf4jLogLevel(PoolBase.class, Level.WARN);
        TestElf.setSlf4jLogLevel(LightPool.class, Level.WARN);
        StubConnection.slowCreate = false;
    }

    @Test
    public void testShutdown1() {
        Assert.assertSame("StubConnection count not as expected", 0, StubConnection.count.get());

        StubConnection.slowCreate = true;

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            LightPool pool = TestElf.getPool(ds);

            Thread[] threads = new Thread[10];
            for (int i = 0; i < 10; i++) {
                threads[i] = new Thread() {
                    @Override
                    public void run() {
                        try {
                            if (ds.getConnection() != null) {
                                UtilityElf.quietlySleep(SECONDS.toMillis(1));
                            }
                        } catch (SQLException e) {
                        }
                    }
                };
                threads[i].setDaemon(true);
            }
            for (int i = 0; i < 10; i++) {
                threads[i].start();
            }

            UtilityElf.quietlySleep(1800L);

            assertTrue("Total connection count not as expected, ", pool.getTotalConnections() > 0);

            ds.close();

            assertSame("Active connection count not as expected, ", 0, pool.getActiveConnections());
            assertSame("Idle connection count not as expected, ", 0, pool.getIdleConnections());
            assertSame("Total connection count not as expected, ", 0, pool.getTotalConnections());
            assertTrue(ds.isClosed());
        }
    }

    @Test
    public void testShutdown2() {
        assertSame("StubConnection count not as expected", 0, StubConnection.count.get());

        StubConnection.slowCreate = true;

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            LightPool pool = TestElf.getPool(ds);

            UtilityElf.quietlySleep(1200L);

            assertTrue("Total connection count not as expected, ", pool.getTotalConnections() > 0);

            ds.close();

            assertSame("Active connection count not as expected, ", 0, pool.getActiveConnections());
            assertSame("Idle connection count not as expected, ", 0, pool.getIdleConnections());
            assertSame("Total connection count not as expected, ", 0, pool.getTotalConnections());
            assertTrue(ds.toString().startsWith("LightDataSource (") && ds.toString().endsWith(")"));
        }
    }

    @Test
    public void testShutdown3() {
        assertSame("StubConnection count not as expected", 0, StubConnection.count.get());

        StubConnection.slowCreate = false;

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            LightPool pool = TestElf.getPool(ds);

            UtilityElf.quietlySleep(1200L);

            assertTrue("Total connection count not as expected, ", pool.getTotalConnections() == 5);

            ds.close();

            assertSame("Active connection count not as expected, ", 0, pool.getActiveConnections());
            assertSame("Idle connection count not as expected, ", 0, pool.getIdleConnections());
            assertSame("Total connection count not as expected, ", 0, pool.getTotalConnections());
        }
    }

    @Test
    public void testShutdown4() {
        StubConnection.slowCreate = true;

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(10);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            UtilityElf.quietlySleep(500L);

            ds.close();

            long startTime = ClockSource.currentTime();
            while (ClockSource.elapsedMillis(startTime) < SECONDS.toMillis(5) && threadCount() > 0) {
                UtilityElf.quietlySleep(250);
            }

            assertSame("Unreleased connections after shutdown", 0, TestElf.getPool(ds).getTotalConnections());
        }
    }

    @Test
    public void testShutdown5() throws SQLException {
        Assert.assertSame("StubConnection count not as expected", 0, StubConnection.count.get());

        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            LightPool pool = TestElf.getPool(ds);

            Connection[] connections = new Connection[5];
            for (int i = 0; i < 5; i++) {
                connections[i] = ds.getConnection();
            }

            Assert.assertTrue("Total connection count not as expected, ", pool.getTotalConnections() == 5);

            ds.close();

            Assert.assertSame("Active connection count not as expected, ", 0, pool.getActiveConnections());
            Assert.assertSame("Idle connection count not as expected, ", 0, pool.getIdleConnections());
            Assert.assertSame("Total connection count not as expected, ", 0, pool.getTotalConnections());
        }
    }

    @Test
    public void testAfterShutdown() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(5);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            ds.close();
            try {
                ds.getConnection();
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("has been closed."));
            }
        }
    }

    @Test
    public void testShutdownDuringInit() {
        final LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config)) {
            StubConnection.slowCreate = true;
            UtilityElf.quietlySleep(3000L);
        }
    }

    @Test
    public void testThreadedShutdown() throws Exception {
        final LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(5);
        config.setConnectionTimeout(1000);
        config.setValidationTimeout(1000);
        config.setInitializationFailTimeout(0);
        config.setConnectionTestQuery("VALUES 1");
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        for (int i = 0; i < 4; i++) {
            try (final LightDataSource ds = new LightDataSource(config)) {
                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try (Connection connection = ds.getConnection()) {
                            for (int i = 0; i < 10; i++) {
                                Connection connection2 = null;
                                try {
                                    connection2 = ds.getConnection();
                                    PreparedStatement stmt = connection2.prepareStatement("SOMETHING");
                                    UtilityElf.quietlySleep(20);
                                    stmt.getMaxFieldSize();
                                } catch (SQLException e) {
                                    try {
                                        if (connection2 != null) {
                                            connection2.close();
                                        }
                                    } catch (SQLException e2) {
                                        if (e2.getMessage().contains("shutdown") || e2.getMessage().contains("evicted")) {
                                            break;
                                        }
                                    }
                                }
                            }
                        } catch (Exception e) {
                            Assert.fail(e.getMessage());
                        } finally {
                            ds.close();
                        }
                    }
                };
                t.start();

                Thread t2 = new Thread() {
                    @Override
                    public void run() {
                        UtilityElf.quietlySleep(100);
                        try {
                            ds.close();
                        } catch (IllegalStateException e) {
                            Assert.fail(e.getMessage());
                        }
                    }
                };
                t2.start();

                t.join();
                t2.join();

                ds.close();
            }
        }
    }

    private int threadCount() {
        Thread[] threads = new Thread[Thread.activeCount() * 2];
        Thread.enumerate(threads);

        int count = 0;
        for (Thread thread : threads) {
            count += (thread != null && thread.getName().startsWith("Light")) ? 1 : 0;
        }

        return count;
    }
}
