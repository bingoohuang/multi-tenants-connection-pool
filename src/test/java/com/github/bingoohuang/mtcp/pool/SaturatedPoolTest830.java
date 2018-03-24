package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.StubConnection;
import com.github.bingoohuang.mtcp.mocks.StubStatement;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Level;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

/**
 * @author Brett Wooldridge
 */
@Slf4j
public class SaturatedPoolTest830 {
    private static final int MAX_POOL_SIZE = 10;

    @Test
    public void saturatedPoolTest() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(5);
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(1000);
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        StubConnection.slowCreate = true;
        StubStatement.setSimulatedQueryTime(1000);
        TestElf.setSlf4jLogLevel(LightPool.class, Level.DEBUG);
        System.setProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs", "5000");

        final long start = ClockSource.currentTime();

        try (final LightDataSource ds = new LightDataSource(config)) {
            LinkedBlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
            ThreadPoolExecutor threadPool = new ThreadPoolExecutor(50 /*core*/, 50 /*max*/, 2 /*keepalive*/, SECONDS, queue, new ThreadPoolExecutor.CallerRunsPolicy());
            threadPool.allowCoreThreadTimeOut(true);

            AtomicInteger windowIndex = new AtomicInteger();
            boolean[] failureWindow = new boolean[100];
            Arrays.fill(failureWindow, true);

            // Initial saturation
            for (int i = 0; i < 50; i++) {
                threadPool.execute(() -> {
                    try (Connection conn = ds.getConnection();
                         Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT bogus FROM imaginary");
                    } catch (SQLException e) {
                        log.info(e.getMessage());
                    }
                });
            }

            long sleep = 80;
            outer:
            while (true) {
                UtilityElf.quietlySleep(sleep);

                if (ClockSource.elapsedMillis(start) > SECONDS.toMillis(12) && sleep < 100) {
                    sleep = 100;
                    log.warn("Switching to 100ms sleep");
                } else if (ClockSource.elapsedMillis(start) > SECONDS.toMillis(6) && sleep < 90) {
                    sleep = 90;
                    log.warn("Switching to 90ms sleep");
                }

                threadPool.execute(() -> {
                    int ndx = windowIndex.incrementAndGet() % failureWindow.length;

                    try (Connection conn = ds.getConnection();
                         Statement stmt = conn.createStatement()) {
                        stmt.execute("SELECT bogus FROM imaginary");
                        failureWindow[ndx] = false;
                    } catch (SQLException e) {
                        log.info(e.getMessage());
                        failureWindow[ndx] = true;
                    }
                });

                for (int i = 0; i < failureWindow.length; i++) {
                    if (failureWindow[i]) {
                        if (ClockSource.elapsedMillis(start) % (SECONDS.toMillis(1) - sleep) < sleep) {
                            log.info("Active threads {}, submissions per second {}, waiting threads {}",
                                    threadPool.getActiveCount(),
                                    SECONDS.toMillis(1) / sleep,
                                    TestElf.getPool(ds).getThreadsAwaitingConnection());
                        }
                        continue outer;
                    }
                }

                log.info("Timeouts have subsided.");
                log.info("Active threads {}, submissions per second {}, waiting threads {}",
                        threadPool.getActiveCount(),
                        SECONDS.toMillis(1) / sleep,
                        TestElf.getPool(ds).getThreadsAwaitingConnection());
                break;
            }

            log.info("Waiting for completion of {} active tasks.", threadPool.getActiveCount());
            while (TestElf.getPool(ds).getActiveConnections() > 0) {
                UtilityElf.quietlySleep(50);
            }

            assertEquals("Rate not in balance at 10req/s", SECONDS.toMillis(1) / sleep, 10L);
        } finally {
            StubStatement.setSimulatedQueryTime(0);
            StubConnection.slowCreate = false;
            System.clearProperty("com.github.bingoohuang.mtcp.housekeeping.periodMs");
            TestElf.setSlf4jLogLevel(LightPool.class, Level.INFO);
        }
    }
}
