package com.github.bingoohuang.mtcp.metrics.dropwizard;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.pool.LightPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Provides Dropwizard HealthChecks.  Two health checks are provided:
 * <ul>
 * <li>ConnectivityCheck</li>
 * <li>Connection99Percent</li>
 * </ul>
 * The ConnectivityCheck will use the <code>connectionTimeout</code>, unless the health check property
 * <code>connectivityCheckTimeoutMs</code> is defined.  However, if either the <code>connectionTimeout</code>
 * or the <code>connectivityCheckTimeoutMs</code> is 0 (infinite), a timeout of 10 seconds will be used.
 * <p>
 * The Connection99Percent health check will only be registered if the health check property
 * <code>expected99thPercentileMs</code> is defined and greater than 0.
 *
 * @author Brett Wooldridge
 */
public final class CodahaleHealthChecker {
    /**
     * Register Dropwizard health checks.
     *
     * @param pool        the pool to register health checks for
     * @param lightConfig the pool configuration
     * @param registry    the HealthCheckRegistry into which checks will be registered
     */
    public static void registerHealthChecks(final LightPool pool, final LightConfig lightConfig, final HealthCheckRegistry registry) {
        final Properties healthCheckProperties = lightConfig.getHealthCheckProperties();
        final MetricRegistry metricRegistry = (MetricRegistry) lightConfig.getMetricRegistry();

        final long checkTimeoutMs = Long.parseLong(healthCheckProperties.getProperty("connectivityCheckTimeoutMs", String.valueOf(lightConfig.getConnectionTimeout())));
        registry.register(MetricRegistry.name(lightConfig.getPoolName(), "pool", "ConnectivityCheck"), new ConnectivityHealthCheck(pool, checkTimeoutMs));

        final long expected99thPercentile = Long.parseLong(healthCheckProperties.getProperty("expected99thPercentileMs", "0"));
        if (metricRegistry != null && expected99thPercentile > 0) {
            SortedMap<String, Timer> timers = metricRegistry.getTimers(new MetricFilter() {
                @Override
                public boolean matches(String name, Metric metric) {
                    return name.equals(MetricRegistry.name(lightConfig.getPoolName(), "pool", "Wait"));
                }
            });

            if (!timers.isEmpty()) {
                final Timer timer = timers.entrySet().iterator().next().getValue();
                registry.register(MetricRegistry.name(lightConfig.getPoolName(), "pool", "Connection99Percent"), new Connection99Percent(timer, expected99thPercentile));
            }
        }
    }

    private CodahaleHealthChecker() {
        // private constructor
    }

    private static class ConnectivityHealthCheck extends HealthCheck {
        private final LightPool pool;
        private final long checkTimeoutMs;

        ConnectivityHealthCheck(final LightPool pool, final long checkTimeoutMs) {
            this.pool = pool;
            this.checkTimeoutMs = (checkTimeoutMs > 0 && checkTimeoutMs != Integer.MAX_VALUE ? checkTimeoutMs : TimeUnit.SECONDS.toMillis(10));
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Result check() {
            try (Connection connection = pool.getConnection(checkTimeoutMs)) {
                return Result.healthy();
            } catch (SQLException e) {
                return Result.unhealthy(e);
            }
        }
    }

    private static class Connection99Percent extends HealthCheck {
        private final Timer waitTimer;
        private final long expected99thPercentile;

        Connection99Percent(final Timer waitTimer, final long expected99thPercentile) {
            this.waitTimer = waitTimer;
            this.expected99thPercentile = expected99thPercentile;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected Result check() {
            final long the99thPercentile = TimeUnit.NANOSECONDS.toMillis(Math.round(waitTimer.getSnapshot().get99thPercentile()));
            return the99thPercentile <= expected99thPercentile ? Result.healthy() : Result.unhealthy("99th percentile connection wait time of %dms exceeds the threshold %dms", the99thPercentile, expected99thPercentile);
        }
    }
}
