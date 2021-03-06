package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.metrics.MetricsTracker;
import com.github.bingoohuang.mtcp.mocks.StubDataSource;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLTransientConnectionException;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * @author wvuong@chariotsolutions.com on 2/16/17.
 */
public class MetricsTrackerTest {

    @Test(expected = SQLTransientConnectionException.class)
    public void connectionTimeoutIsRecorded() throws Exception {
        int timeoutMillis = 1000;
        int timeToCreateNewConnectionMillis = timeoutMillis * 2;

        StubDataSource stubDataSource = new StubDataSource();
        stubDataSource.setConnectionAcquistionTime(timeToCreateNewConnectionMillis);

        StubMetricsTracker metricsTracker = new StubMetricsTracker();

        try (LightDataSource ds = TestElf.newLightDataSource()) {
            ds.setMinIdle(0);
            ds.setMaxPoolSize(1);
            ds.setConnectionTimeout(timeoutMillis);
            ds.setDataSource(stubDataSource);
            ds.setMetricsTrackerFactory((poolName, poolStats) -> metricsTracker);

            try (Connection c = ds.getConnection()) {
                fail("Connection shouldn't have been successfully created due to configured connection timeout");

            } finally {
                // assert that connection timeout was measured
                assertThat(metricsTracker.connectionTimeoutRecorded, is(true));
                // assert that measured time to acquire connection should be roughly equal or greater than the configured connection timeout time
                assertTrue(metricsTracker.connectionAcquiredNanos >= TimeUnit.NANOSECONDS.convert(timeoutMillis, TimeUnit.MILLISECONDS));
            }
        }
    }

    private static class StubMetricsTracker implements MetricsTracker {

        private Long connectionCreatedMillis;
        private Long connectionAcquiredNanos;
        private Long connectionBorrowedMillis;
        private boolean connectionTimeoutRecorded;

        @Override
        public void recordConnectionCreatedMillis(long connectionCreatedMillis) {
            this.connectionCreatedMillis = connectionCreatedMillis;
        }

        @Override
        public void recordConnectionAcquiredNanos(long elapsedAcquiredNanos) {
            this.connectionAcquiredNanos = elapsedAcquiredNanos;
        }

        @Override
        public void recordConnectionUsageMillis(long elapsedBorrowedMillis) {
            this.connectionBorrowedMillis = elapsedBorrowedMillis;
        }

        @Override
        public void recordConnectionTimeout() {
            this.connectionTimeoutRecorded = true;
        }
    }
}
