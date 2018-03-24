/**
 *
 */
package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.mocks.MockDataSource;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.Connection;
import java.sql.SQLException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

/**
 * Test for cases when db network connectivity goes down and close is called on existing connections. By default Light
 * blocks longer than getMaximumTimeout (it can hang for a lot of time depending on driver timeout settings). Closing
 * async the connections fixes this issue.
 */
public class TestConnectionCloseBlocking {
    private static volatile boolean shouldFail = false;

    // @Test
    public void testConnectionCloseBlocking() {
        LightConfig config = TestElf.newLightConfig();
        config.setMinimumIdle(0);
        config.setMaximumPoolSize(1);
        config.setConnectionTimeout(1500);
        config.setDataSource(new CustomMockDataSource());

        long start = ClockSource.currentTime();
        try (LightDataSource ds = new LightDataSource(config);
             Connection connection = ds.getConnection()) {

            connection.close();

            // Light only checks for validity for connections with lastAccess > 1000 ms so we sleep for 1001 ms to force
            // Light to do a connection validation which will fail and will trigger the connection to be closed
            UtilityElf.quietlySleep(1100L);

            shouldFail = true;

            // on physical connection close we sleep 2 seconds
            try (Connection connection2 = ds.getConnection()) {
                assertTrue("Waited longer than timeout", (ClockSource.elapsedMillis(start) < config.getConnectionTimeout()));
            }
        } catch (SQLException e) {
            assertTrue("getConnection failed because close connection took longer than timeout", (ClockSource.elapsedMillis(start) < config.getConnectionTimeout()));
        }
    }

    private static class CustomMockDataSource extends MockDataSource {
        @Override
        public Connection getConnection() throws SQLException {
            Connection mockConnection = super.getConnection();
            when(mockConnection.isValid(anyInt())).thenReturn(!shouldFail);
            doAnswer(new Answer<Void>() {
                @Override
                public Void answer(InvocationOnMock invocation) throws Throwable {
                    if (shouldFail) {
                        SECONDS.sleep(2);
                    }
                    return null;
                }
            }).when(mockConnection).close();
            return mockConnection;
        }
    }

}
