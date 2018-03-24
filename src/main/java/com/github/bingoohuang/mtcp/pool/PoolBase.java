package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.LightConfig;
import com.github.bingoohuang.mtcp.util.ClockSource;
import com.github.bingoohuang.mtcp.util.DriverDataSource;
import com.github.bingoohuang.mtcp.util.PropertyElf;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.github.bingoohuang.mtcp.pool.ProxyConnection.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@Slf4j
abstract class PoolBase {
    public final LightConfig config;
    public MetricsTrackerDelegatable metricsTracker;
    protected final String poolName;
    long connectionTimeout;
    long validationTimeout;

    private static final String[] RESET_STATES = {"readOnly", "autoCommit", "isolation", "catalog", "netTimeout", "schema"};
    private static final int UNINITIALIZED = -1;
    private static final int TRUE = 1;
    private static final int FALSE = 0;

    private int networkTimeout;
    private int isNetworkTimeoutSupported;
    private int isQueryTimeoutSupported;
    private int defaultTransactionIsolation;
    private int transactionIsolation;
    private Executor netTimeoutExecutor;
    private DataSource dataSource;

    private final String catalog;
    private final String schema;
    private final boolean isReadOnly;
    private final boolean isAutoCommit;

    private final boolean isUseJdbc4Validation;
    private final boolean isIsolateInternalQueries;
    private final AtomicReference<Throwable> lastConnectionFailure;

    private volatile boolean isValidChecked;

    private final AtomicInteger connectionSeq = new AtomicInteger();


    PoolBase(final LightConfig config) {
        this.config = config;

        this.networkTimeout = UNINITIALIZED;
        this.catalog = config.getCatalog();
        this.schema = config.getSchema();
        this.isReadOnly = config.isReadOnly();
        this.isAutoCommit = config.isAutoCommit();
        this.transactionIsolation = UtilityElf.getTransactionIsolation(config.getTransactionIsolation());

        this.isQueryTimeoutSupported = UNINITIALIZED;
        this.isNetworkTimeoutSupported = UNINITIALIZED;
        this.isUseJdbc4Validation = config.getConnectionTestQuery() == null;
        this.isIsolateInternalQueries = config.isIsolateInternalQueries();

        this.poolName = config.getPoolName();
        this.connectionTimeout = config.getConnectionTimeout();
        this.validationTimeout = config.getValidationTimeout();
        this.lastConnectionFailure = new AtomicReference<>();

        initializeDataSource();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return poolName;
    }

    abstract void recycle(final PoolEntry poolEntry);

    abstract void closeConnection(final PoolEntry poolEntry, final String closureReason);

    // ***********************************************************************
    //                           JDBC methods
    // ***********************************************************************

    void quietlyClose(final Connection connection, final String closureReason) {
        if (connection != null) {
            try {
                log.debug("{} - Closing connection {}: {}", poolName, connection, closureReason);
                try {
                    setNetworkTimeout(connection, SECONDS.toMillis(15));
                } finally {
                    connection.close(); // continue with the close even if setNetworkTimeout() throws
                }
                log.debug("{} - Closed connection {}", poolName, connection);
            } catch (Throwable e) {
                log.debug("{} - Closing connection {} failed", poolName, connection, e);
            }
        }
    }

    boolean isConnectionAlive(final Connection connection) {
        try {
            try {
                setNetworkTimeout(connection, validationTimeout);

                val validationSeconds = (int) Math.max(1000L, validationTimeout) / 1000;

                if (isUseJdbc4Validation) {
                    return connection.isValid(validationSeconds);
                }

                try (val statement = connection.createStatement()) {
                    if (isNetworkTimeoutSupported != TRUE) {
                        setQueryTimeout(statement, validationSeconds);
                    }

                    statement.execute(config.getConnectionTestQuery());
                }
            } finally {
                setNetworkTimeout(connection, networkTimeout);

                if (isIsolateInternalQueries && !isAutoCommit) {
                    connection.rollback();
                }
            }

            return true;
        } catch (Exception e) {
            lastConnectionFailure.set(e);
            log.warn("{} - Failed to validate connection {} ({})", poolName, connection, e.getMessage());
            return false;
        }
    }

    Throwable getLastConnectionFailure() {
        return lastConnectionFailure.get();
    }

    public DataSource getUnwrappedDataSource() {
        return dataSource;
    }

    // ***********************************************************************
    //                         PoolEntry methods
    // ***********************************************************************
    PoolEntry newPoolEntry() throws Exception {
        return new PoolEntry(newConnection(), this, isReadOnly, isAutoCommit, connectionSeq.incrementAndGet());
    }

    void resetConnectionState(final Connection connection, final ProxyConnection proxyConnection, final int dirtyBits) throws SQLException {
        int resetBits = 0;

        if ((dirtyBits & DIRTY_BIT_READONLY) != 0 && proxyConnection.getReadOnlyState() != isReadOnly) {
            connection.setReadOnly(isReadOnly);
            resetBits |= DIRTY_BIT_READONLY;
        }

        if ((dirtyBits & DIRTY_BIT_AUTOCOMMIT) != 0 && proxyConnection.getAutoCommitState() != isAutoCommit) {
            connection.setAutoCommit(isAutoCommit);
            resetBits |= DIRTY_BIT_AUTOCOMMIT;
        }

        if ((dirtyBits & DIRTY_BIT_ISOLATION) != 0 && proxyConnection.getTransactionIsolationState() != transactionIsolation) {
            connection.setTransactionIsolation(transactionIsolation);
            resetBits |= DIRTY_BIT_ISOLATION;
        }

        if ((dirtyBits & DIRTY_BIT_CATALOG) != 0 && catalog != null && !catalog.equals(proxyConnection.getCatalogState())) {
            connection.setCatalog(catalog);
            resetBits |= DIRTY_BIT_CATALOG;
        }

        if ((dirtyBits & DIRTY_BIT_NETTIMEOUT) != 0 && proxyConnection.getNetworkTimeoutState() != networkTimeout) {
            setNetworkTimeout(connection, networkTimeout);
            resetBits |= DIRTY_BIT_NETTIMEOUT;
        }

        if ((dirtyBits & DIRTY_BIT_SCHEMA) != 0 && schema != null && !schema.equals(proxyConnection.getSchemaState())) {
            connection.setSchema(schema);
            resetBits |= DIRTY_BIT_SCHEMA;
        }

        if (resetBits != 0 && log.isDebugEnabled()) {
            log.debug("{} - Reset ({}) on connection {}", poolName, stringFromResetBits(resetBits), connection);
        }
    }

    void shutdownNetworkTimeoutExecutor() {
        if (netTimeoutExecutor instanceof ThreadPoolExecutor) {
            ((ThreadPoolExecutor) netTimeoutExecutor).shutdownNow();
        }
    }

    long getLoginTimeout() {
        try {
            return (dataSource != null) ? dataSource.getLoginTimeout() : SECONDS.toSeconds(5);
        } catch (SQLException e) {
            return SECONDS.toSeconds(5);
        }
    }

    // ***********************************************************************
    //                       JMX methods
    // ***********************************************************************

    /**
     * Register MBeans for LightConfig and LightPool.
     *
     * @param lightPool a LightPool instance
     */
    void registerMBeans(final LightPool lightPool) {
        if (!config.isRegisterMbeans()) {
            return;
        }

        try {
            val mBeanServer = ManagementFactory.getPlatformMBeanServer();

            val beanConfigName = new ObjectName("com.github.bingoohuang.mtcp:type=PoolConfig (" + poolName + ")");
            val beanPoolName = new ObjectName("com.github.bingoohuang.mtcp:type=Pool (" + poolName + ")");
            if (!mBeanServer.isRegistered(beanConfigName)) {
                mBeanServer.registerMBean(config, beanConfigName);
                mBeanServer.registerMBean(lightPool, beanPoolName);
            } else {
                log.error("{} - JMX name ({}) is already registered.", poolName, poolName);
            }
        } catch (Exception e) {
            log.warn("{} - Failed to register management beans.", poolName, e);
        }
    }

    /**
     * Unregister MBeans for LightConfig and LightPool.
     */
    void unregisterMBeans() {
        if (!config.isRegisterMbeans()) {
            return;
        }

        try {
            val mBeanServer = ManagementFactory.getPlatformMBeanServer();

            val beanConfigName = new ObjectName("com.github.bingoohuang.mtcp:type=PoolConfig (" + poolName + ")");
            val beanPoolName = new ObjectName("com.github.bingoohuang.mtcp:type=Pool (" + poolName + ")");
            if (mBeanServer.isRegistered(beanConfigName)) {
                mBeanServer.unregisterMBean(beanConfigName);
                mBeanServer.unregisterMBean(beanPoolName);
            }
        } catch (Exception e) {
            log.warn("{} - Failed to unregister management beans.", poolName, e);
        }
    }

    // ***********************************************************************
    //                          Private methods
    // ***********************************************************************

    /**
     * Create/initialize the underlying DataSource.
     */
    private void initializeDataSource() {
        val jdbcUrl = config.getJdbcUrl();
        val username = config.getUsername();
        val password = config.getPassword();
        val dsClassName = config.getDataSourceClassName();
        val driverClassName = config.getDriverClassName();
        val dataSourceJNDI = config.getDataSourceJNDI();
        val dataSourceProperties = config.getDataSourceProperties();

        val dataSource = createDataSource(jdbcUrl, username, password, dsClassName, driverClassName, dataSourceJNDI, dataSourceProperties);
        if (dataSource != null) {
            setLoginTimeout(dataSource);
            createNetworkTimeoutExecutor(dataSource, dsClassName, jdbcUrl);
        }

        this.dataSource = dataSource;
    }

    private DataSource createDataSource(String jdbcUrl, String username, String password, String dsClassName, String driverClassName, String dataSourceJNDI, Properties dataSourceProperties) {
        DataSource dataSource = config.getDataSource();
        if (dsClassName != null && dataSource == null) {
            dataSource = UtilityElf.createInstance(dsClassName, DataSource.class);
            PropertyElf.setTargetFromProperties(dataSource, dataSourceProperties);
        } else if (jdbcUrl != null && dataSource == null) {
            dataSource = new DriverDataSource(jdbcUrl, driverClassName, dataSourceProperties, username, password);
        } else if (dataSourceJNDI != null && dataSource == null) {
            try {
                val ic = new InitialContext();
                dataSource = (DataSource) ic.lookup(dataSourceJNDI);
            } catch (NamingException e) {
                throw new PoolInitializationException(e);
            }
        }
        return dataSource;
    }

    /**
     * Obtain connection from data source.
     *
     * @return a Connection connection
     */
    private Connection newConnection() throws Exception {
        final long start = ClockSource.currentTime();

        Connection connection = null;
        try {
            val username = config.getUsername();
            val password = config.getPassword();

            connection = (username == null) ? dataSource.getConnection() : dataSource.getConnection(username, password);
            if (connection == null) {
                throw new SQLTransientConnectionException("DataSource returned null unexpectedly");
            }

            setupConnection(connection);
            lastConnectionFailure.set(null);
            return connection;
        } catch (Exception e) {
            if (connection != null) {
                quietlyClose(connection, "(Failed to create/setup connection)");
            } else if (getLastConnectionFailure() == null) {
                log.debug("{} - Failed to create/setup connection: {}", poolName, e.getMessage());
            }

            lastConnectionFailure.set(e);
            throw e;
        } finally {
            // tracker will be null during failFast check
            if (metricsTracker != null) {
                metricsTracker.recordConnectionCreated(ClockSource.elapsedMillis(start));
            }
        }
    }

    /**
     * Setup a connection initial state.
     *
     * @param connection a Connection
     * @throws ConnectionSetupException thrown if any exception is encountered
     */
    private void setupConnection(final Connection connection) throws ConnectionSetupException {
        try {
            if (networkTimeout == UNINITIALIZED) {
                networkTimeout = getAndSetNetworkTimeout(connection, validationTimeout);
            } else {
                setNetworkTimeout(connection, validationTimeout);
            }

            connection.setReadOnly(isReadOnly);
            connection.setAutoCommit(isAutoCommit);

            checkDriverSupport(connection);

            if (transactionIsolation != defaultTransactionIsolation) {
                connection.setTransactionIsolation(transactionIsolation);
            }

            if (catalog != null) {
                connection.setCatalog(catalog);
            }

            if (schema != null) {
                connection.setSchema(schema);
            }

            executeSql(connection, config.getConnectionInitSql(), true);

            setNetworkTimeout(connection, networkTimeout);
        } catch (SQLException e) {
            throw new ConnectionSetupException(e);
        }
    }

    /**
     * Execute isValid() or connection test query.
     *
     * @param connection a Connection to check
     */
    private void checkDriverSupport(final Connection connection) throws SQLException {
        if (!isValidChecked) {
            try {
                if (isUseJdbc4Validation) {
                    connection.isValid(1);
                } else {
                    executeSql(connection, config.getConnectionTestQuery(), false);
                }
            } catch (Throwable e) {
                log.error("{} - Failed to execute" + (isUseJdbc4Validation ? " isValid() for connection, configure" : "") + " connection test query ({}).", poolName, e.getMessage());
                throw e;
            }

            try {
                defaultTransactionIsolation = connection.getTransactionIsolation();
                if (transactionIsolation == -1) {
                    transactionIsolation = defaultTransactionIsolation;
                }
            } catch (SQLException e) {
                log.warn("{} - Default transaction isolation level detection failed ({}).", poolName, e.getMessage());
                if (e.getSQLState() != null && !e.getSQLState().startsWith("08")) {
                    throw e;
                }
            }

            isValidChecked = true;
        }
    }

    /**
     * Set the query timeout, if it is supported by the driver.
     *
     * @param statement  a statement to set the query timeout on
     * @param timeoutSec the number of seconds before timeout
     */
    private void setQueryTimeout(final Statement statement, final int timeoutSec) {
        if (isQueryTimeoutSupported != FALSE) {
            try {
                statement.setQueryTimeout(timeoutSec);
                isQueryTimeoutSupported = TRUE;
            } catch (Throwable e) {
                if (isQueryTimeoutSupported == UNINITIALIZED) {
                    isQueryTimeoutSupported = FALSE;
                    log.info("{} - Failed to set query timeout for statement. ({})", poolName, e.getMessage());
                }
            }
        }
    }

    /**
     * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
     * driver supports it.  Return the pre-existing value of the network timeout.
     *
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @return the pre-existing network timeout value
     */
    private int getAndSetNetworkTimeout(final Connection connection, final long timeoutMs) {
        if (isNetworkTimeoutSupported != FALSE) {
            try {
                val originalTimeout = connection.getNetworkTimeout();
                connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
                isNetworkTimeoutSupported = TRUE;
                return originalTimeout;
            } catch (Throwable e) {
                if (isNetworkTimeoutSupported == UNINITIALIZED) {
                    isNetworkTimeoutSupported = FALSE;

                    log.info("{} - Driver does not support get/set network timeout for connections. ({})", poolName, e.getMessage());
                    if (validationTimeout < SECONDS.toMillis(1)) {
                        log.warn("{} - A validationTimeout of less than 1 second cannot be honored on drivers without setNetworkTimeout() support.", poolName);
                    } else if (validationTimeout % SECONDS.toMillis(1) != 0) {
                        log.warn("{} - A validationTimeout with fractional second granularity cannot be honored on drivers without setNetworkTimeout() support.", poolName);
                    }
                }
            }
        }

        return 0;
    }

    /**
     * Set the network timeout, if <code>isUseNetworkTimeout</code> is <code>true</code> and the
     * driver supports it.
     *
     * @param connection the connection to set the network timeout on
     * @param timeoutMs  the number of milliseconds before timeout
     * @throws SQLException throw if the connection.setNetworkTimeout() call throws
     */
    private void setNetworkTimeout(final Connection connection, final long timeoutMs) throws SQLException {
        if (isNetworkTimeoutSupported == TRUE) {
            connection.setNetworkTimeout(netTimeoutExecutor, (int) timeoutMs);
        }
    }

    /**
     * Execute the user-specified init SQL.
     *
     * @param connection the connection to initialize
     * @param sql        the SQL to execute
     * @param isCommit   whether to commit the SQL after execution or not
     * @throws SQLException throws if the init SQL execution fails
     */
    private void executeSql(final Connection connection, final String sql, final boolean isCommit) throws SQLException {
        if (sql != null) {
            try (Statement statement = connection.createStatement()) {
                // connection was created a few milliseconds before, so set query timeout is omitted (we assume it will succeed)
                statement.execute(sql);
            }

            if (isIsolateInternalQueries && !isAutoCommit) {
                if (isCommit) {
                    connection.commit();
                } else {
                    connection.rollback();
                }
            }
        }
    }

    private void createNetworkTimeoutExecutor(final DataSource dataSource, final String dsClassName, final String jdbcUrl) {
        // Temporary hack for MySQL issue: http://bugs.mysql.com/bug.php?id=75615
        if (isMySQL(dataSource, dsClassName, jdbcUrl)) {
            netTimeoutExecutor = SameThreadExecutor.INSTANCE;
        } else {
            ThreadFactory threadFactory = config.getThreadFactory();
            threadFactory = threadFactory != null ? threadFactory : new UtilityElf.DefaultThreadFactory(poolName + " network timeout executor", true);
            val executor = (ThreadPoolExecutor) Executors.newCachedThreadPool(threadFactory);
            executor.setKeepAliveTime(15, SECONDS);
            executor.allowCoreThreadTimeOut(true);
            netTimeoutExecutor = executor;
        }
    }

    private boolean isMySQL(DataSource dataSource, String dsClassName, String jdbcUrl) {
        return dsClassName != null && dsClassName.contains("Mysql")
                || jdbcUrl != null && jdbcUrl.contains("mysql")
                || dataSource != null && dataSource.getClass().getName().contains("Mysql");
    }

    /**
     * Set the loginTimeout on the specified DataSource.
     *
     * @param dataSource the DataSource
     */
    private void setLoginTimeout(final DataSource dataSource) {
        if (connectionTimeout != Integer.MAX_VALUE) {
            try {
                dataSource.setLoginTimeout(Math.max(1, (int) MILLISECONDS.toSeconds(500L + connectionTimeout)));
            } catch (Throwable e) {
                log.info("{} - Failed to set login timeout for data source. ({})", poolName, e.getMessage());
            }
        }
    }

    /**
     * This will create a string for debug logging. Given a set of "reset bits", this
     * method will return a concatenated string, for example:
     * <p>
     * Input : 0b00110
     * Output: "autoCommit, isolation"
     *
     * @param bits a set of "reset bits"
     * @return a string of which states were reset
     */
    private String stringFromResetBits(final int bits) {
        val sb = new StringBuilder();
        for (int ndx = 0; ndx < RESET_STATES.length; ndx++) {
            if ((bits & (0b1 << ndx)) != 0) {
                sb.append(RESET_STATES[ndx]).append(", ");
            }
        }

        sb.setLength(sb.length() - 2);  // trim trailing comma
        return sb.toString();
    }
}
