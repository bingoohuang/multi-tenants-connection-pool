package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.ConcurrentBag;
import org.apache.logging.log4j.Level;
import org.junit.After;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.bingoohuang.mtcp.pool.TestElf.newLightConfig;
import static com.github.bingoohuang.mtcp.pool.TestElf.setSlf4jLogLevel;
import static org.junit.Assert.fail;

/**
 * @author Matthew Tambara (matthew.tambara@liferay.com)
 */
public class ConnectionRaceConditionTest {

    public static final int ITERATIONS = 10_000;

    @Test
    public void testRaceCondition() throws Exception {
        LightConfig config = newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(5000);
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        setSlf4jLogLevel(ConcurrentBag.class, Level.INFO);

        final AtomicReference<Exception> ref = new AtomicReference<>(null);

        // Initialize LightPool with no initial connections and room to grow
        try (final LightDataSource ds = new LightDataSource(config)) {
            ExecutorService threadPool = Executors.newFixedThreadPool(2);
            for (int i = 0; i < ITERATIONS; i++) {
                threadPool.submit(new Callable<Exception>() {
                    /** {@inheritDoc} */
                    @Override
                    public Exception call() {
                        if (ref.get() == null) {
                            Connection c2;
                            try {
                                c2 = ds.getConnection();
                                ds.evictConnection(c2);
                            } catch (Exception e) {
                                ref.set(e);
                            }
                        }
                        return null;
                    }
                });
            }

            threadPool.shutdown();
            threadPool.awaitTermination(30, TimeUnit.SECONDS);

            if (ref.get() != null) {
                LoggerFactory.getLogger(ConnectionRaceConditionTest.class).error("Task failed", ref.get());
                fail("Task failed");
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @After
    public void after() {
        System.getProperties().remove("com.github.bingoohuang.mtcp.housekeeping.periodMs");

        setSlf4jLogLevel(LightPool.class, Level.WARN);
        setSlf4jLogLevel(ConcurrentBag.class, Level.WARN);
    }
}
