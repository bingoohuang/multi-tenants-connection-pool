package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @author Matthew Tambara (matthew.tambara@liferay.com)
 */
public class ConcurrentCloseConnectionTest {
    @Test
    public void testConcurrentClose() throws Exception {
        LightConfig config = TestElf.newLightConfig();
        config.setDataSourceClassName("com.github.bingoohuang.mtcp.mocks.StubDataSource");

        try (LightDataSource ds = new LightDataSource(config);
             final Connection connection = ds.getConnection()) {

            ExecutorService executorService = Executors.newFixedThreadPool(10);

            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < 500; i++) {
                final PreparedStatement preparedStatement =
                        connection.prepareStatement("");

                futures.add(executorService.submit((Callable<Void>) () -> {
                    preparedStatement.close();

                    return null;
                }));
            }

            executorService.shutdown();

            for (Future<?> future : futures) {
                future.get();
            }
        }
    }
}
