package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * @author Martin Stříž (striz@raynet.cz)
 */
public class HouseKeeperCleanupTest {

    private ScheduledThreadPoolExecutor executor;

    @Before
    public void before() {
        ThreadFactory threadFactory = new UtilityElf.DefaultThreadFactory("global housekeeper", true);

        executor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
        executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        executor.setRemoveOnCancelPolicy(true);
    }

    @Test
    public void testHouseKeeperCleanupWithCustomExecutor() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(10);
        config.setInitializationFailTimeout(Long.MAX_VALUE);
        config.setConnectionTimeout(2500);
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");
        config.setScheduledExecutor(executor);

        LightConfig config2 = TestElf.newLightConfig();
        config.copyStateTo(config2);

        try (
                final LightDataSource ds1 = new LightDataSource(config);
                final LightDataSource ds2 = new LightDataSource(config2)
        ) {
            assertEquals("Scheduled tasks count not as expected, ", 2, executor.getQueue().size());
        }

        assertEquals("Scheduled tasks count not as expected, ", 0, executor.getQueue().size());
    }

    @After
    public void after() throws Exception {
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);
    }

}
