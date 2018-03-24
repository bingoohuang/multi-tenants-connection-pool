package com.github.bingoohuang.mtcp.pool;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.LightDataSource;
import com.github.bingoohuang.mtcp.LightPoolMXBean;
import com.github.bingoohuang.mtcp.metrics.MetricsTrackerFactory;
import com.github.bingoohuang.mtcp.metrics.PoolStats;
import com.github.bingoohuang.mtcp.metrics.dropwizard.CodahaleHealthChecker;
import com.github.bingoohuang.mtcp.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.github.bingoohuang.mtcp.metrics.micrometer.MicrometerMetricsTrackerFactory;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.ConcurrentBag;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.*;

import static com.github.bingoohuang.mtcp.util.UtilityElf.createThreadPoolExecutor;
import static java.util.Collections.unmodifiableCollection;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This is the primary connection pool class that provides the basic
 * pooling behavior for LightCP.
 *
 * @author Brett Wooldridge
 */
@Slf4j
public final class LightPool extends PoolBase implements LightPoolMXBean, ConcurrentBag.BagStateListener {
    public static final int POOL_NORMAL = 0;
    public static final int POOL_SHUTDOWN = 2;
    public volatile int poolState;

    private final long ALIVE_BYPASS_WINDOW_MS = Long.getLong("com.github.bingoohuang.mtcp.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
    private final long HOUSEKEEPING_PERIOD_MS = Long.getLong("com.github.bingoohuang.mtcp.housekeeping.periodMs", SECONDS.toMillis(30));

    private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
    private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";

    private final PoolEntryCreator POOL_ENTRY_CREATOR = new PoolEntryCreator(null /*logging prefix*/);
    private final PoolEntryCreator POST_FILL_POOL_ENTRY_CREATOR = new PoolEntryCreator("After adding ");
    private final Collection<Runnable> addConnectionQueue;
    private final ThreadPoolExecutor addConnectionExecutor;
    private final ThreadPoolExecutor closeConnectionExecutor;
    private final ConcurrentBag<PoolEntry> connectionBag;
    private final ProxyLeakTaskFactory leakTaskFactory;
    private final ScheduledExecutorService houseKeepingExecutorService;
    private ScheduledFuture<?> houseKeeperTask;

    /**
     * Construct a LightPool with the specified configuration.
     *
     * @param config a LightConfig instance
     */
    public LightPool(final LightConfig config) {
        super(config);

        this.connectionBag = new ConcurrentBag<>(this);

        this.houseKeepingExecutorService = initializeHouseKeepingExecutorService();

        checkFailFast(config);

        if (config.getMetricsTrackerFactory() != null) {
            setMetricsTrackerFactory(config.getMetricsTrackerFactory());
        } else {
            setMetricRegistry(config.getMetricRegistry());
        }

        setHealthCheckRegistry(config.getHealthCheckRegistry());

        registerMBeans(this);

        val threadFactory = config.getThreadFactory();

        final LinkedBlockingQueue<Runnable> addConnectionQueue = new LinkedBlockingQueue<>(config.getMaxPoolSize());
        this.addConnectionQueue = unmodifiableCollection(addConnectionQueue);
        this.addConnectionExecutor = createThreadPoolExecutor(addConnectionQueue, poolName + " connection adder", threadFactory, new ThreadPoolExecutor.DiscardPolicy());
        this.closeConnectionExecutor = createThreadPoolExecutor(config.getMaxPoolSize(), poolName + " connection closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());

        this.leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);

        this.houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(
                new HouseKeeper(), 100L, HOUSEKEEPING_PERIOD_MS, MILLISECONDS);
    }

    /**
     * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
     *
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection() throws SQLException {
        return getConnection(connectionTimeout);
    }

    /**
     * Get a connection from the pool, or timeout after the specified number of milliseconds.
     *
     * @param hardTimeout the maximum time to wait for a connection from the pool
     * @return a java.sql.Connection instance
     * @throws SQLException thrown if a timeout occurs trying to obtain a connection
     */
    public Connection getConnection(final long hardTimeout) throws SQLException {
        val startTime = ClockSource.currentTime();

        try {
            long timeout = hardTimeout;
            do {
                val poolEntry = connectionBag.borrow(timeout, MILLISECONDS);
                if (poolEntry == null) {
                    break; // We timed out... break and throw exception
                }

                val now = ClockSource.currentTime();
                if (poolEntry.isMarkedEvicted() || isEntryDead(poolEntry, now)) {
                    val reason = poolEntry.isMarkedEvicted() ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE;
                    closeConnection(poolEntry, reason);
                    timeout = hardTimeout - ClockSource.elapsedMillis(startTime);
                } else {
                    metricsTracker.recordBorrowStats(poolEntry, startTime);
                    markTenantCode(poolEntry);
                    val leakTask = leakTaskFactory.schedule(poolEntry);
                    return poolEntry.createProxyConnection(leakTask, now);
                }
            } while (timeout > 0L);

            metricsTracker.recordBorrowTimeoutStats(startTime);
            throw createTimeoutException(startTime);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
        }
    }

    private void markTenantCode(PoolEntry entry) {
        val tenantEnvAware = config.getTenantEnvironmentAware();
        if (tenantEnvAware == null) return;

        val tid = tenantEnvAware.getTenantId();
        val prevTid = entry.getTenantId();
        val tidSame = UtilityElf.objectEquals(tid, prevTid);
        if (!tidSame) entry.setTenantId(tid);
        if (tidSame) return;

        tenantEnvAware.switchTenantDatabase(entry.connection);
    }

    private boolean isEntryDead(PoolEntry poolEntry, long now) {
        val elapsedMillis = ClockSource.elapsedMillis(poolEntry.lastAccessed, now);
        return elapsedMillis > ALIVE_BYPASS_WINDOW_MS && !isConnectionAlive(poolEntry.connection);
    }

    /**
     * Shutdown the pool, closing all idle connections and aborting or closing
     * active connections.
     *
     * @throws InterruptedException thrown if the thread is interrupted during shutdown
     */
    public synchronized void shutdown() throws InterruptedException {
        try {
            poolState = POOL_SHUTDOWN;

            if (addConnectionExecutor == null) { // pool never started
                return;
            }

            logPoolState("Before shutdown ");

            if (houseKeeperTask != null) {
                houseKeeperTask.cancel(false);
                houseKeeperTask = null;
            }

            softEvictConnections();

            addConnectionExecutor.shutdown();
            addConnectionExecutor.awaitTermination(getLoginTimeout(), SECONDS);

            destroyHouseKeepingExecutorService();

            connectionBag.close();

            val assassinExecutor = createThreadPoolExecutor(config.getMaxPoolSize(), poolName + " connection assassinator",
                    config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
            try {
                val start = ClockSource.currentTime();
                val millis = SECONDS.toMillis(10);
                do {
                    abortActiveConnections(assassinExecutor);
                    softEvictConnections();
                }
                while (getTotalConnections() > 0 && ClockSource.elapsedMillis(start) < millis);
            } finally {
                assassinExecutor.shutdown();
                assassinExecutor.awaitTermination(10L, SECONDS);
            }

            shutdownNetworkTimeoutExecutor();
            closeConnectionExecutor.shutdown();
            closeConnectionExecutor.awaitTermination(10L, SECONDS);
        } finally {
            logPoolState("After shutdown ");
            unregisterMBeans();
            metricsTracker.close();
        }
    }

    /**
     * Evict a Connection from the pool.
     *
     * @param connection the Connection to evict (actually a {@link ProxyConnection})
     */
    public void evictConnection(Connection connection) {
        val proxyConnection = (ProxyConnection) connection;
        proxyConnection.cancelLeakTask();

        try {
            softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", !connection.isClosed() /* owner */);
        } catch (SQLException e) {
            // unreachable in LightCP, but we're still forced to catch it
        }
    }

    /**
     * Set a metrics registry to be used when registering metrics collectors.  The LightDataSource prevents this
     * method from being called more than once.
     *
     * @param metricRegistry the metrics registry instance to use
     */
    public void setMetricRegistry(Object metricRegistry) {
        if (UtilityElf.safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")) {
            setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));
        } else if (UtilityElf.safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry")) {
            setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory((MeterRegistry) metricRegistry));
        } else {
            setMetricsTrackerFactory(null);
        }
    }

    /**
     * Set the MetricsTrackerFactory to be used to create the MetricsTracker instance used by the pool.
     *
     * @param metricsTrackerFactory an instance of a class that subclasses MetricsTrackerFactory
     */
    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
        if (metricsTrackerFactory != null) {
            val tracker = metricsTrackerFactory.create(config.getPoolName(), getPoolStats());
            this.metricsTracker = new MetricsTrackerDelegate(tracker);
        } else {
            this.metricsTracker = new NopMetricsTrackerDelegate();
        }
    }

    /**
     * Set the health check registry to be used when registering health checks.  Currently only Codahale health
     * checks are supported.
     *
     * @param healthCheckRegistry the health check registry instance to use
     */
    public void setHealthCheckRegistry(Object healthCheckRegistry) {
        if (healthCheckRegistry != null) {
            CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
        }
    }

    // ***********************************************************************
    //                        BagStateListener callback
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBagItem(final int waiting) {
        val shouldAdd = waiting - addConnectionQueue.size() >= 0; // Yes, >= is intentional.
        if (shouldAdd) {
            addConnectionExecutor.submit(POOL_ENTRY_CREATOR);
        }
    }

    // ***********************************************************************
    //                        LightPoolMBean methods
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public int getActiveConnections() {
        return connectionBag.countStateUsing();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getIdleConnections() {
        return connectionBag.countStateFree();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTotalConnections() {
        return connectionBag.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getThreadsAwaitingConnection() {
        return connectionBag.getWaitingThreadCount();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void softEvictConnections() {
        connectionBag.values().forEach(poolEntry -> softEvictConnection(poolEntry, "(connection evicted)", false /* not owner */));
    }

    // ***********************************************************************
    //                           Package methods
    // ***********************************************************************

    /**
     * Log the current pool state at debug level.
     *
     * @param prefix an optional prefix to prepend the log message
     */
    void logPoolState(String... prefix) {
        if (log.isDebugEnabled()) {
            log.debug("{} - {}stats (total={}, active={}, idle={}, waiting={})",
                    poolName, (prefix.length > 0 ? prefix[0] : ""),
                    getTotalConnections(), getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection());
        }
    }

    /**
     * Recycle PoolEntry (add back to the pool)
     *
     * @param poolEntry the PoolEntry to recycle
     */
    @Override
    void recycle(final PoolEntry poolEntry) {
        metricsTracker.recordConnectionUsage(poolEntry);

        connectionBag.requite(poolEntry);
    }

    /**
     * Permanently close the real (underlying) connection (eat any exception).
     *
     * @param poolEntry     poolEntry having the connection to close
     * @param closureReason reason to close
     */
    @Override
    void closeConnection(final PoolEntry poolEntry, final String closureReason) {
        if (connectionBag.remove(poolEntry)) {
            val connection = poolEntry.close();
            closeConnectionExecutor.execute(() -> {
                quietlyClose(connection, closureReason);
                if (poolState == POOL_NORMAL) {
                    fillPool();
                }
            });
        }
    }

    @SuppressWarnings("unused")
    int[] getPoolStateCounts() {
        return connectionBag.getStateCounts();
    }


    // ***********************************************************************
    //                           Private methods
    // ***********************************************************************

    /**
     * Creating new poolEntry.  If maxLifetime is configured, create a future End-of-life task with 2.5% variance from
     * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
     */
    private PoolEntry createPoolEntry() {
        try {
            val poolEntry = newPoolEntry();

            val maxLifetime = config.getMaxLifetime();
            if (maxLifetime > 0) {
                // variance up to 2.5% of the maxlifetime
                val variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong(maxLifetime / 40) : 0;
                val lifetime = maxLifetime - variance;
                poolEntry.setFutureEol(houseKeepingExecutorService.schedule(
                        () -> {
                            if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false /* not owner */)) {
                                addBagItem(connectionBag.getWaitingThreadCount());
                            }
                        },
                        lifetime, MILLISECONDS));
            }

            return poolEntry;
        } catch (Exception e) {
            if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
                log.debug("{} - Cannot acquire connection from data source", poolName, (e instanceof ConnectionSetupException ? e.getCause() : e));
            }
            return null;
        }
    }

    /**
     * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
     */
    private synchronized void fillPool() {
        int a = config.getMaxPoolSize() - getTotalConnections();
        int b = config.getMinIdle() - getIdleConnections();

        for (int i = 0, ii = Math.min(a, b) - addConnectionQueue.size(); i < ii; i++) {
            addConnectionExecutor.submit((i < ii - 1) ? POOL_ENTRY_CREATOR : POST_FILL_POOL_ENTRY_CREATOR);
        }
    }

    /**
     * Attempt to abort or close active connections.
     *
     * @param assassinExecutor the ExecutorService to pass to Connection.abort()
     */
    private void abortActiveConnections(final ExecutorService assassinExecutor) {
        for (val poolEntry : connectionBag.valuesUsing()) {
            val connection = poolEntry.close();
            try {
                connection.abort(assassinExecutor);
            } catch (Throwable e) {
                quietlyClose(connection, "(connection aborted during shutdown)");
            } finally {
                connectionBag.remove(poolEntry);
            }
        }
    }

    /**
     * If initializationFailFast is configured, check that we have DB connectivity.
     *
     * @param config
     * @throws PoolInitializationException if fails to create or validate connection
     * @see LightConfig#setInitializationFailTimeout(long)
     */
    private void checkFailFast(LightConfig config) {
        if (config.getTenantEnvironmentAware() != null) {
            return; // ignore when multi-tenants environment is setup
        }

        val initializationTimeout = this.config.getInitializationFailTimeout();
        if (initializationTimeout < 0) {
            return;
        }

        val startTime = ClockSource.currentTime();
        do {
            val poolEntry = createPoolEntry();
            if (poolEntry != null) {
                if (this.config.getMinIdle() > 0) {
                    connectionBag.add(poolEntry);
                    log.debug("{} - Added connection {}", poolName, poolEntry.connection);
                } else {
                    quietlyClose(poolEntry.close(), "(initialization check complete and minimumIdle is zero)");
                }

                return;
            }

            if (getLastConnectionFailure() instanceof ConnectionSetupException) {
                throwPoolInitializationException(getLastConnectionFailure().getCause());
            }

            UtilityElf.quietlySleep(SECONDS.toMillis(1));
        } while (ClockSource.elapsedMillis(startTime) < initializationTimeout);

        if (initializationTimeout > 0) {
            throwPoolInitializationException(getLastConnectionFailure());
        }
    }

    /**
     * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
     * that cause attached.
     *
     * @param t the Throwable that caused the pool to fail to initialize (possibly null)
     */
    private void throwPoolInitializationException(Throwable t) {
        log.error("{} - Exception during pool initialization.", poolName, t);
        destroyHouseKeepingExecutorService();
        throw new PoolInitializationException(t);
    }

    /**
     * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
     * through {@link LightDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
     * <p>
     * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link ConcurrentBag}),
     * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
     * the next time someone tries to acquire it from the pool.
     *
     * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
     * @param reason    the reason that the connection is being evicted
     * @param owner     true if the caller is the owner of the connection, false otherwise
     * @return true if the connection was evicted (closed), false if it was merely marked for eviction
     */
    private boolean softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner) {
        poolEntry.markEvicted();
        if (owner || connectionBag.reserve(poolEntry)) {
            closeConnection(poolEntry, reason);
            return true;
        }

        return false;
    }

    /**
     * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
     * to be used in the {@link LightConfig}, then we use that.  If no Executor was specified (typical), then create
     * an Executor and configure it.
     *
     * @return either the user specified {@link ScheduledExecutorService}, or the one we created
     */
    private ScheduledExecutorService initializeHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            val threadFactory = Optional.ofNullable(config.getThreadFactory())
                    .orElse(new UtilityElf.DefaultThreadFactory(poolName + " housekeeper", true));
            val executor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
            executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            executor.setRemoveOnCancelPolicy(true);
            return executor;
        } else {
            return config.getScheduledExecutor();
        }
    }

    /**
     * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
     */
    private void destroyHouseKeepingExecutorService() {
        if (config.getScheduledExecutor() == null) {
            houseKeepingExecutorService.shutdownNow();
        }
    }

    /**
     * Create a PoolStats instance that will be used by metrics tracking, with a pollable resolution of 1 second.
     *
     * @return a PoolStats instance
     */
    private PoolStats getPoolStats() {
        return new PoolStats(SECONDS.toMillis(1)) {
            @Override
            protected void update() {
                this.pendingThreads = LightPool.this.getThreadsAwaitingConnection();
                this.idleConnections = LightPool.this.getIdleConnections();
                this.totalConnections = LightPool.this.getTotalConnections();
                this.activeConnections = LightPool.this.getActiveConnections();
            }
        };
    }

    /**
     * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
     * timeout occurred when trying to acquire a Connection from the pool.  If there was an underlying cause for the
     * timeout, e.g. a SQLException thrown by the driver while trying to create a new Connection, then use the
     * SQL State from that exception as our own and additionally set that exception as the "next" SQLException inside
     * of our exception.
     * <p>
     * As a side-effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
     *
     * @param startTime the start time (timestamp) of the acquisition attempt
     * @return a SQLException to be thrown from {@link #getConnection()}
     */
    private SQLException createTimeoutException(long startTime) {
        logPoolState("Timeout failure ");
        metricsTracker.recordConnectionTimeout();

        String sqlState = null;
        val originalException = getLastConnectionFailure();
        if (originalException instanceof SQLException) {
            sqlState = ((SQLException) originalException).getSQLState();
        }
        val connectionException = new SQLTransientConnectionException(poolName +
                " - Connection is not available, request timed out after "
                + ClockSource.elapsedMillis(startTime) + "ms.", sqlState, originalException);
        if (originalException instanceof SQLException) {
            connectionException.setNextException((SQLException) originalException);
        }

        return connectionException;
    }


    // ***********************************************************************
    //                      Non-anonymous Inner-classes
    // ***********************************************************************

    /**
     * Creating and adding poolEntries (connections) to the pool.
     */
    private final class PoolEntryCreator implements Callable<Boolean> {
        private final String loggingPrefix;

        PoolEntryCreator(String loggingPrefix) {
            this.loggingPrefix = loggingPrefix;
        }

        @Override
        public Boolean call() {
            long sleepBackoff = 250L;
            while (poolState == POOL_NORMAL && shouldCreateAnotherConnection()) {
                val poolEntry = createPoolEntry();
                if (poolEntry != null) {
                    connectionBag.add(poolEntry);
                    log.debug("{} - Added connection {}", poolName, poolEntry.connection);
                    if (loggingPrefix != null) {
                        logPoolState(loggingPrefix);
                    }
                    return Boolean.TRUE;
                }

                // failed to get connection from db, sleep and retry
                UtilityElf.quietlySleep(sleepBackoff);
                sleepBackoff = Math.min(SECONDS.toMillis(10), Math.min(connectionTimeout, (long) (sleepBackoff * 1.5)));
            }
            // Pool is suspended or shutdown or at max size
            return Boolean.FALSE;
        }

        /**
         * We only create connections if we need another idle connection or have threads still waiting
         * for a new connection.  Otherwise we bail out of the request to create.
         *
         * @return true if we should create a connection, false if the need has disappeared
         */
        private boolean shouldCreateAnotherConnection() {
            return getTotalConnections() < config.getMaxPoolSize() &&
                    (connectionBag.getWaitingThreadCount() > 0 || getIdleConnections() < config.getMinIdle());
        }
    }

    /**
     * The house keeping task to retire and maintain minimum idle connections.
     */
    private final class HouseKeeper implements Runnable {
        private volatile long previous = ClockSource.plusMillis(ClockSource.currentTime(), -HOUSEKEEPING_PERIOD_MS);

        @Override
        public void run() {
            try {
                // refresh timeouts in case they changed via MBean
                connectionTimeout = config.getConnectionTimeout();
                validationTimeout = config.getValidationTimeout();
                leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold());

                val idleTimeout = config.getIdleTimeout();
                val now = ClockSource.currentTime();

                // Detect retrograde time, allowing +128ms as per NTP spec.
                if (ClockSource.plusMillis(now, 128) < ClockSource.plusMillis(previous, HOUSEKEEPING_PERIOD_MS)) {
                    log.warn("{} - Retrograde clock change detected (housekeeper delta={}), soft-evicting connections from pool.",
                            poolName, ClockSource.elapsedDisplayString(previous, now));
                    previous = now;
                    softEvictConnections();
                    return;
                } else if (now > ClockSource.plusMillis(previous, (3 * HOUSEKEEPING_PERIOD_MS) / 2)) {
                    // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
                    log.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).",
                            poolName, ClockSource.elapsedDisplayString(previous, now));
                }

                previous = now;

                String afterPrefix = "Pool ";
                if (idleTimeout > 0L && config.getMinIdle() < config.getMaxPoolSize()) {
                    logPoolState("Before cleanup ");
                    afterPrefix = "After cleanup  ";

                    val notInUse = connectionBag.valuesFree();
                    int toRemove = notInUse.size() - config.getMinIdle();
                    for (val entry : notInUse) {
                        if (toRemove > 0 && ClockSource.elapsedMillis(entry.lastAccessed, now) > idleTimeout && connectionBag.reserve(entry)) {
                            closeConnection(entry, "(connection has passed idleTimeout)");
                            toRemove--;
                        }
                    }
                }

                logPoolState(afterPrefix);

                fillPool(); // Try to maintain minimum connections
            } catch (Exception e) {
                log.error("Unexpected exception in housekeeping task", e);
            }
        }
    }

}
