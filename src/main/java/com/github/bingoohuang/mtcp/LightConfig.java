package com.github.bingoohuang.mtcp;

import com.codahale.metrics.health.HealthCheckRegistry;
import com.github.bingoohuang.mtcp.metrics.MetricsTrackerFactory;
import com.github.bingoohuang.mtcp.util.PropertyElf;
import com.github.bingoohuang.mtcp.util.UtilityElf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.security.AccessControlException;
import java.sql.Connection;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;

import static com.github.bingoohuang.mtcp.util.DriverElf.loadDriverClass;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

@SuppressWarnings({"SameParameterValue", "unused"})
@Slf4j
public class LightConfig implements LightConfigMXBean {
    private static final char[] ID_CHARACTERS = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ".toCharArray();
    private static final long CONNECTION_TIMEOUT = SECONDS.toMillis(30);
    private static final long VALIDATION_TIMEOUT = SECONDS.toMillis(5);
    private static final long IDLE_TIMEOUT = MINUTES.toMillis(10);
    private static final long MAX_LIFETIME = MINUTES.toMillis(30);
    private static final int DEFAULT_POOL_SIZE = 10;

    private static boolean unitTest = false;

    // Properties changeable at runtime through the LightConfigMXBean
    //
    @Getter private volatile long connectionTimeout;
    @Getter private volatile long validationTimeout;
    @Getter private volatile long idleTimeout;
    @Setter @Getter private volatile long leakDetectionThreshold;
    @Setter @Getter private volatile long maxLifetime;
    @Getter private volatile int maxPoolSize;
    @Getter private volatile int minIdle;
    @Setter @Getter private volatile String username;
    @Setter @Getter private volatile String password;

    @Getter private String tenantEnvironmentAwareClassName;
    @Setter @Getter private TenantEnvironmentAware tenantEnvironmentAware;

    // Properties NOT changeable at runtime
    //
    @Getter private long initializationFailTimeout;
    @Getter private String catalog;
    @Getter private String connectionInitSql;
    @Getter private String connectionTestQuery;
    @Getter private String dataSourceClassName;
    private String dataSourceJndiName;
    private String driverClassName;
    private String jdbcUrl;
    @Getter private String poolName;
    @Getter private String schema;
    private String transactionIsolationName;
    private boolean isAutoCommit;
    @Getter private boolean isReadOnly;
    private boolean isIsolateInternalQueries;
    private boolean isRegisterMbeans;
    private boolean isAllowPoolSuspension;
    @Getter private DataSource dataSource;
    private Properties dataSourceProperties;
    private ThreadFactory threadFactory;
    @Getter private ScheduledExecutorService scheduledExecutor;
    private MetricsTrackerFactory metricsTrackerFactory;
    private Object metricRegistry;
    private Object healthCheckRegistry;
    private Properties healthCheckProperties;

    private volatile boolean sealed;

    /**
     * Default constructor
     */
    public LightConfig() {
        dataSourceProperties = new Properties();
        healthCheckProperties = new Properties();

        minIdle = -1;
        maxPoolSize = -1;
        maxLifetime = MAX_LIFETIME;
        connectionTimeout = CONNECTION_TIMEOUT;
        validationTimeout = VALIDATION_TIMEOUT;
        idleTimeout = IDLE_TIMEOUT;
        initializationFailTimeout = 1;
        isAutoCommit = true;

        String systemProp = System.getProperty("lightcp.configurationFile");
        if (systemProp != null) {
            loadProperties(systemProp);
        }
    }

    /**
     * Construct a LightConfig from the specified properties object.
     *
     * @param properties the name of the property file
     */
    public LightConfig(Properties properties) {
        this();
        PropertyElf.setTargetFromProperties(this, properties);
    }

    /**
     * Construct a LightConfig from the specified property file name.  <code>propertyFileName</code>
     * will first be treated as a path in the file-system, and if that fails the
     * Class.getResourceAsStream(propertyFileName) will be tried.
     *
     * @param propertyFileName the name of the property file
     */
    public LightConfig(String propertyFileName) {
        this();

        loadProperties(propertyFileName);
    }

    // ***********************************************************************
    //                       LightConfigMXBean methods
    // ***********************************************************************

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConnectionTimeout(long connectionTimeoutMs) {
        if (connectionTimeoutMs == 0) {
            this.connectionTimeout = Integer.MAX_VALUE;
        } else if (connectionTimeoutMs < 250) {
            throw new IllegalArgumentException("connectionTimeout cannot be less than 250ms");
        } else {
            this.connectionTimeout = connectionTimeoutMs;
        }
    }

    public void setTenantEnvironmentAwareClassName(String tenantEnvironmentAwareClassName) {
        this.tenantEnvironmentAwareClassName = tenantEnvironmentAwareClassName;
        this.tenantEnvironmentAware = UtilityElf.createInstance(tenantEnvironmentAwareClassName, TenantEnvironmentAware.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setIdleTimeout(long idleTimeoutMs) {
        if (idleTimeoutMs < 0) {
            throw new IllegalArgumentException("idleTimeout cannot be negative");
        }
        this.idleTimeout = idleTimeoutMs;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxPoolSize(int maxPoolSize) {
        if (maxPoolSize < 1) {
            throw new IllegalArgumentException("maxPoolSize cannot be less than 1");
        }
        this.maxPoolSize = maxPoolSize;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMinIdle(int minIdle) {
        if (minIdle < 0) {
            throw new IllegalArgumentException("minimumIdle cannot be negative");
        }
        this.minIdle = minIdle;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setValidationTimeout(long validationTimeoutMs) {
        if (validationTimeoutMs < 250) {
            throw new IllegalArgumentException("validationTimeout cannot be less than 250ms");
        }

        this.validationTimeout = validationTimeoutMs;
    }

    // ***********************************************************************
    //                     All other configuration methods
    // ***********************************************************************


    /**
     * Set the default catalog name to be set on connections.
     *
     * @param catalog the catalog name, or null
     */
    public void setCatalog(String catalog) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.catalog = catalog;
    }


    /**
     * Set the SQL query to be executed to test the validity of connections. Using
     * the JDBC4 <code>Connection.isValid()</code> method to test connection validity can
     * be more efficient on some databases and is recommended.
     *
     * @param connectionTestQuery a SQL query string
     */
    public void setConnectionTestQuery(String connectionTestQuery) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.connectionTestQuery = connectionTestQuery;
    }

    /**
     * Set the SQL string that will be executed on all new connections when they are
     * created, before they are added to the pool.  If this query fails, it will be
     * treated as a failed connection attempt.
     *
     * @param connectionInitSql the SQL to execute on new connections
     */
    public void setConnectionInitSql(String connectionInitSql) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.connectionInitSql = connectionInitSql;
    }

    /**
     * Set a {@link DataSource} for the pool to explicitly wrap.  This setter is not
     * available through property file based initialization.
     *
     * @param dataSource a specific {@link DataSource} to be wrapped by the pool
     */
    public void setDataSource(DataSource dataSource) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.dataSource = dataSource;
    }

    /**
     * Set the fully qualified class name of the JDBC {@link DataSource} that will be used create Connections.
     *
     * @param className the fully qualified name of the JDBC {@link DataSource} class
     */
    public void setDataSourceClassName(String className) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.dataSourceClassName = className;
    }

    /**
     * Add a property (name/value pair) that will be used to configure the {@link DataSource}/{@link java.sql.Driver}.
     * <p>
     * In the case of a {@link DataSource}, the property names will be translated to Java setters following the Java Bean
     * naming convention.  For example, the property {@code cachePrepStmts} will translate into {@code setCachePrepStmts()}
     * with the {@code value} passed as a parameter.
     * <p>
     * In the case of a {@link java.sql.Driver}, the property will be added to a {@link Properties} instance that will
     * be passed to the driver during {@link java.sql.Driver#connect(String, Properties)} calls.
     *
     * @param propertyName the name of the property
     * @param value        the value to be used by the DataSource/Driver
     */
    public void addDataSourceProperty(String propertyName, Object value) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        dataSourceProperties.put(propertyName, value);
    }

    public String getDataSourceJNDI() {
        return this.dataSourceJndiName;
    }

    public void setDataSourceJNDI(String jndiDataSource) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.dataSourceJndiName = jndiDataSource;
    }

    public Properties getDataSourceProperties() {
        return dataSourceProperties;
    }

    public void setDataSourceProperties(Properties dsProperties) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        dataSourceProperties.putAll(dsProperties);
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        Class<?> driverClass = loadDriverClass(driverClassName, true);
        if (driverClass == null) {
            throw new RuntimeException("Failed to load driver class " + driverClassName + " in either of LightConfig class loader or Thread context classloader");
        }

        try {
            driverClass.newInstance();
            this.driverClassName = driverClassName;
        } catch (Exception e) {
            throw new RuntimeException("Failed to instantiate class " + driverClassName, e);
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.jdbcUrl = jdbcUrl;
    }

    /**
     * Get the default auto-commit behavior of connections in the pool.
     *
     * @return the default auto-commit behavior of connections
     */
    public boolean isAutoCommit() {
        return isAutoCommit;
    }

    /**
     * Set the default auto-commit behavior of connections in the pool.
     *
     * @param isAutoCommit the desired auto-commit default for connections
     */
    public void setAutoCommit(boolean isAutoCommit) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.isAutoCommit = isAutoCommit;
    }

    /**
     * Get the pool suspension behavior (allowed or disallowed).
     *
     * @return the pool suspension behavior
     */
    public boolean isAllowPoolSuspension() {
        return isAllowPoolSuspension;
    }

    /**
     * Set whether or not pool suspension is allowed.  There is a performance
     * impact when pool suspension is enabled.  Unless you need it (for a
     * redundancy system for example) do not enable it.
     *
     * @param isAllowPoolSuspension the desired pool suspension allowance
     */
    public void setAllowPoolSuspension(boolean isAllowPoolSuspension) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.isAllowPoolSuspension = isAllowPoolSuspension;
    }

    /**
     * Set the pool initialization failure timeout.  This setting applies to pool
     * initialization when {@link LightDataSource} is constructed with a {@link LightConfig},
     * or when {@link LightDataSource} is constructed using the no-arg constructor
     * and {@link LightDataSource#getConnection()} is called.
     * <ul>
     * <li>Any value greater than zero will be treated as a timeout for pool initialization.
     * The calling thread will be blocked from continuing until a successful connection
     * to the database, or until the timeout is reached.  If the timeout is reached, then
     * a {@code PoolInitializationException} will be thrown. </li>
     * <li>A value of zero will <i>not</i>  prevent the pool from starting in the
     * case that a connection cannot be obtained. However, upon start the pool will
     * attempt to obtain a connection and validate that the {@code connectionTestQuery}
     * and {@code connectionInitSql} are valid.  If those validations fail, an exception
     * will be thrown.  If a connection cannot be obtained, the validation is skipped
     * and the the pool will start and continue to try to obtain connections in the
     * background.  This can mean that callers to {@code DataSource#getConnection()} may
     * encounter exceptions. </li>
     * <li>A value less than zero will <i>not</i> bypass any connection attempt and
     * validation during startup, and therefore the pool will start immediately.  The
     * pool will continue to try to obtain connections in the background. This can mean
     * that callers to {@code DataSource#getConnection()} may encounter exceptions. </li>
     * </ul>
     * Note that if this timeout value is greater than or equal to zero (0), and therefore an
     * initial connection validation is performed, this timeout does not override the
     * {@code connectionTimeout} or {@code validationTimeout}; they will be honored before this
     * timeout is applied.  The default value is one millisecond.
     *
     * @param initializationFailTimeout the number of milliseconds before the
     *                                  pool initialization fails, or 0 to validate connection setup but continue with
     *                                  pool start, or less than zero to skip all initialization checks and start the
     *                                  pool without delay.
     */
    public void setInitializationFailTimeout(long initializationFailTimeout) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.initializationFailTimeout = initializationFailTimeout;
    }

    /**
     * Get whether or not the construction of the pool should throw an exception
     * if the minimum number of connections cannot be created.
     *
     * @return whether or not initialization should fail on error immediately
     * @deprecated
     */
    @Deprecated
    public boolean isInitializationFailFast() {
        return initializationFailTimeout > 0;
    }

    /**
     * Set whether or not the construction of the pool should throw an exception
     * if the minimum number of connections cannot be created.
     *
     * @param failFast true if the pool should fail if the minimum connections cannot be created
     * @deprecated
     */
    @Deprecated
    public void setInitializationFailFast(boolean failFast) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        log.warn("The initializationFailFast propery is deprecated, see initializationFailTimeout");

        initializationFailTimeout = (failFast ? 1 : -1);
    }

    /**
     * Determine whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
     * via {@link Connection#rollback()}.  Defaults to {@code false}.
     *
     * @return {@code true} if internal pool queries are isolated, {@code false} if not
     */
    public boolean isIsolateInternalQueries() {
        return isIsolateInternalQueries;
    }

    /**
     * Configure whether internal pool queries, principally aliveness checks, will be isolated in their own transaction
     * via {@link Connection#rollback()}.  Defaults to {@code false}.
     *
     * @param isolate {@code true} if internal pool queries should be isolated, {@code false} if not
     */
    public void setIsolateInternalQueries(boolean isolate) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.isIsolateInternalQueries = isolate;
    }

    @Deprecated
    public boolean isJdbc4ConnectionTest() {
        return false;
    }

    @Deprecated
    public void setJdbc4ConnectionTest(boolean useIsValid) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        log.warn("The jdbcConnectionTest property is now deprecated, see the documentation for connectionTestQuery");
    }

    public MetricsTrackerFactory getMetricsTrackerFactory() {
        return metricsTrackerFactory;
    }

    public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory) {
        if (metricRegistry != null) {
            throw new IllegalStateException("cannot use setMetricsTrackerFactory() and setMetricRegistry() together");
        }

        this.metricsTrackerFactory = metricsTrackerFactory;
    }

    /**
     * Get the MetricRegistry instance to used for registration of metrics used by LightCP.  Default is {@code null}.
     *
     * @return the MetricRegistry instance that will be used
     */
    public Object getMetricRegistry() {
        return metricRegistry;
    }

    /**
     * Set a MetricRegistry instance to use for registration of metrics used by LightCP.
     *
     * @param metricRegistry the MetricRegistry instance to use
     */
    public void setMetricRegistry(Object metricRegistry) {
        if (metricsTrackerFactory != null) {
            throw new IllegalStateException("cannot use setMetricRegistry() and setMetricsTrackerFactory() together");
        }

        if (metricRegistry != null) {
            metricRegistry = getObjectOrPerformJndiLookup(metricRegistry);

            if (!UtilityElf.safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")
                    && !(UtilityElf.safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry"))) {
                throw new IllegalArgumentException("Class must be instance of com.codahale.metrics.MetricRegistry or io.micrometer.core.instrument.MeterRegistry");
            }
        }

        this.metricRegistry = metricRegistry;
    }

    /**
     * Get the HealthCheckRegistry that will be used for registration of health checks by LightCP.  Currently only
     * Codahale/DropWizard is supported for health checks.
     *
     * @return the HealthCheckRegistry instance that will be used
     */
    public Object getHealthCheckRegistry() {
        return healthCheckRegistry;
    }

    /**
     * Set the HealthCheckRegistry that will be used for registration of health checks by LightCP.  Currently only
     * Codahale/DropWizard is supported for health checks.  Default is {@code null}.
     *
     * @param healthCheckRegistry the HealthCheckRegistry to be used
     */
    public void setHealthCheckRegistry(Object healthCheckRegistry) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        if (healthCheckRegistry != null) {
            healthCheckRegistry = getObjectOrPerformJndiLookup(healthCheckRegistry);

            if (!(healthCheckRegistry instanceof HealthCheckRegistry)) {
                throw new IllegalArgumentException("Class must be an instance of com.codahale.metrics.health.HealthCheckRegistry");
            }
        }

        this.healthCheckRegistry = healthCheckRegistry;
    }

    public Properties getHealthCheckProperties() {
        return healthCheckProperties;
    }

    public void setHealthCheckProperties(Properties healthCheckProperties) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.healthCheckProperties.putAll(healthCheckProperties);
    }

    public void addHealthCheckProperty(String key, String value) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        healthCheckProperties.setProperty(key, value);
    }

    /**
     * Configures the Connections to be added to the pool as read-only Connections.
     *
     * @param readOnly {@code true} if the Connections in the pool are read-only, {@code false} if not
     */
    public void setReadOnly(boolean readOnly) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.isReadOnly = readOnly;
    }

    /**
     * Determine whether LightCP will self-register {@link LightConfigMXBean} and {@link LightPoolMXBean} instances
     * in JMX.
     *
     * @return {@code true} if LightCP will register MXBeans, {@code false} if it will not
     */
    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    public boolean isRegisterMbeans() {
        return isRegisterMbeans;
    }

    /**
     * Configures whether LightCP self-registers the {@link LightConfigMXBean} and {@link LightPoolMXBean} in JMX.
     *
     * @param register {@code true} if LightCP should register MXBeans, {@code false} if it should not
     */
    public void setRegisterMbeans(boolean register) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.isRegisterMbeans = register;
    }


    /**
     * Set the name of the connection pool.  This is primarily used for the MBean
     * to uniquely identify the pool configuration.
     *
     * @param poolName the name of the connection pool to use
     */
    public void setPoolName(String poolName) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.poolName = poolName;
    }

    /**
     * Get the ScheduledExecutorService used for housekeeping.
     *
     * @return the executor
     */
    @Deprecated
    public ScheduledThreadPoolExecutor getScheduledExecutorService() {
        return (ScheduledThreadPoolExecutor) scheduledExecutor;
    }

    /**
     * Set the ScheduledExecutorService used for housekeeping.
     *
     * @param executor the ScheduledExecutorService
     */
    @Deprecated
    public void setScheduledExecutorService(ScheduledThreadPoolExecutor executor) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.scheduledExecutor = executor;
    }

    /**
     * Set the ScheduledExecutorService used for housekeeping.
     *
     * @param executor the ScheduledExecutorService
     */
    public void setScheduledExecutor(ScheduledExecutorService executor) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.scheduledExecutor = executor;
    }

    public String getTransactionIsolation() {
        return transactionIsolationName;
    }

    /**
     * Set the default schema name to be set on connections.
     */
    public void setSchema(String schema) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.schema = schema;
    }

    /**
     * Set the default transaction isolation level.  The specified value is the
     * constant name from the <code>Connection</code> class, eg.
     * <code>TRANSACTION_REPEATABLE_READ</code>.
     *
     * @param isolationLevel the name of the isolation level
     */
    public void setTransactionIsolation(String isolationLevel) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.transactionIsolationName = isolationLevel;
    }

    /**
     * Get the thread factory used to create threads.
     *
     * @return the thread factory (may be null, in which case the default thread factory is used)
     */
    public ThreadFactory getThreadFactory() {
        return threadFactory;
    }

    /**
     * Set the thread factory to be used to create threads.
     *
     * @param threadFactory the thread factory (setting to null causes the default thread factory to be used)
     */
    public void setThreadFactory(ThreadFactory threadFactory) {
        if (sealed)
            throw new IllegalStateException("The configuration of the pool is sealed once started.  Use LightConfigMXBean for runtime changes.");

        this.threadFactory = threadFactory;
    }

    void seal() {
        this.sealed = true;
    }

    /**
     * Deprecated, use {@link #copyStateTo(LightConfig)}.
     * <p>
     * Copies the state of {@code this} into {@code other}.
     * </p>
     *
     * @param other Other {@link LightConfig} to copy the state to.
     */
    @Deprecated
    public void copyState(LightConfig other) {
        copyStateTo(other);
    }

    /**
     * Copies the state of {@code this} into {@code other}.
     *
     * @param other Other {@link LightConfig} to copy the state to.
     */
    public void copyStateTo(LightConfig other) {
        for (Field field : LightConfig.class.getDeclaredFields()) {
            if (!Modifier.isFinal(field.getModifiers())) {
                field.setAccessible(true);
                try {
                    field.set(other, field.get(this));
                } catch (Exception e) {
                    throw new RuntimeException("Failed to copy LightConfig state: " + e.getMessage(), e);
                }
            }
        }

        other.sealed = false;
    }

    // ***********************************************************************
    //                          Private methods
    // ***********************************************************************

    @SuppressWarnings("StatementWithEmptyBody")
    public void validate() {
        if (poolName == null) {
            poolName = generatePoolName();
        } else if (isRegisterMbeans && poolName.contains(":")) {
            throw new IllegalArgumentException("poolName cannot contain ':' when used with JMX");
        }

        // treat empty property as null
        catalog = UtilityElf.getNullIfEmpty(catalog);
        connectionInitSql = UtilityElf.getNullIfEmpty(connectionInitSql);
        connectionTestQuery = UtilityElf.getNullIfEmpty(connectionTestQuery);
        transactionIsolationName = UtilityElf.getNullIfEmpty(transactionIsolationName);
        dataSourceClassName = UtilityElf.getNullIfEmpty(dataSourceClassName);
        dataSourceJndiName = UtilityElf.getNullIfEmpty(dataSourceJndiName);
        driverClassName = UtilityElf.getNullIfEmpty(driverClassName);
        jdbcUrl = UtilityElf.getNullIfEmpty(jdbcUrl);

        // Check Data Source Options
        if (dataSource != null) {
            if (dataSourceClassName != null) {
                log.warn("{} - using dataSource and ignoring dataSourceClassName.", poolName);
            }
        } else if (dataSourceClassName != null) {
            if (driverClassName != null) {
                log.error("{} - cannot use driverClassName and dataSourceClassName together.", poolName);
                // NOTE: This exception text is referenced by a Spring Boot FailureAnalyzer, it should not be
                // changed without first notifying the Spring Boot developers.
                throw new IllegalStateException("cannot use driverClassName and dataSourceClassName together.");
            } else if (jdbcUrl != null) {
                log.warn("{} - using dataSourceClassName and ignoring jdbcUrl.", poolName);
            }
        } else if (jdbcUrl != null || dataSourceJndiName != null) {
            // ok
        } else if (driverClassName != null) {
            log.error("{} - jdbcUrl is required with driverClassName.", poolName);
            throw new IllegalArgumentException("jdbcUrl is required with driverClassName.");
        } else {
            log.error("{} - dataSource or dataSourceClassName or jdbcUrl is required.", poolName);
            throw new IllegalArgumentException("dataSource or dataSourceClassName or jdbcUrl is required.");
        }

        validateNumerics();

        if (log.isDebugEnabled() || unitTest) {
            logConfiguration();
        }
    }

    private void validateNumerics() {
        if (maxLifetime != 0 && maxLifetime < SECONDS.toMillis(30)) {
            log.warn("{} - maxLifetime is less than 30000ms, setting to default {}ms.", poolName, MAX_LIFETIME);
            maxLifetime = MAX_LIFETIME;
        }

        if (idleTimeout + SECONDS.toMillis(1) > maxLifetime && maxLifetime > 0) {
            log.warn("{} - idleTimeout is close to or more than maxLifetime, disabling it.", poolName);
            idleTimeout = 0;
        }

        if (idleTimeout != 0 && idleTimeout < SECONDS.toMillis(10)) {
            log.warn("{} - idleTimeout is less than 10000ms, setting to default {}ms.", poolName, IDLE_TIMEOUT);
            idleTimeout = IDLE_TIMEOUT;
        }

        if (leakDetectionThreshold > 0 && !unitTest) {
            if (leakDetectionThreshold < SECONDS.toMillis(2) || (leakDetectionThreshold > maxLifetime && maxLifetime > 0)) {
                log.warn("{} - leakDetectionThreshold is less than 2000ms or more than maxLifetime, disabling it.", poolName);
                leakDetectionThreshold = 0;
            }
        }

        if (connectionTimeout < 250) {
            log.warn("{} - connectionTimeout is less than 250ms, setting to {}ms.", poolName, CONNECTION_TIMEOUT);
            connectionTimeout = CONNECTION_TIMEOUT;
        }

        if (validationTimeout < 250) {
            log.warn("{} - validationTimeout is less than 250ms, setting to {}ms.", poolName, VALIDATION_TIMEOUT);
            validationTimeout = VALIDATION_TIMEOUT;
        }

        if (maxPoolSize < 1) {
            maxPoolSize = (minIdle <= 0) ? DEFAULT_POOL_SIZE : minIdle;
        }

        if (minIdle < 0 || minIdle > maxPoolSize) {
            minIdle = maxPoolSize;
        }
    }

    @SuppressWarnings("StatementWithEmptyBody")
    private void logConfiguration() {
        log.debug("{} - configuration:", poolName);
        final Set<String> propertyNames = new TreeSet<>(PropertyElf.getPropertyNames(LightConfig.class));
        for (String prop : propertyNames) {
            try {
                Object value = PropertyElf.getProperty(prop, this);
                if ("dataSourceProperties".equals(prop)) {
                    val dsProps = PropertyElf.copyProperties(dataSourceProperties);
                    dsProps.setProperty("password", "<masked>");
                    value = dsProps;
                }

                if ("initializationFailTimeout".equals(prop) && initializationFailTimeout == Long.MAX_VALUE) {
                    value = "infinite";
                } else if ("transactionIsolation".equals(prop) && transactionIsolationName == null) {
                    value = "default";
                } else if (prop.matches("scheduledExecutorService|threadFactory") && value == null) {
                    value = "internal";
                } else if (prop.contains("jdbcUrl") && value instanceof String) {
                    value = ((String) value).replaceAll("([?&;]password=)[^&#;]*(.*)", "$1<masked>$2");
                } else if (prop.contains("password")) {
                    value = "<masked>";
                } else if (value instanceof String) {
                    value = "\"" + value + "\""; // quote to see lead/trailing spaces is any
                } else if (value == null) {
                    value = "none";
                }
                log.debug((prop + "................................................").substring(0, 32) + value);
            } catch (Exception e) {
                // continue
            }
        }
    }

    private void loadProperties(String propertyFileName) {
        val propFile = new File(propertyFileName);
        try (val is = propFile.isFile() ? new FileInputStream(propFile) : this.getClass().getResourceAsStream(propertyFileName)) {
            if (is != null) {
                val props = new Properties();
                props.load(is);
                PropertyElf.setTargetFromProperties(this, props);
            } else {
                throw new IllegalArgumentException("Cannot find property file: " + propertyFileName);
            }
        } catch (IOException io) {
            throw new RuntimeException("Failed to read property file", io);
        }
    }

    private String generatePoolName() {
        val prefix = "LightPool-";
        try {
            // Pool number is global to the VM to avoid overlapping pool numbers in classloader scoped environments
            synchronized (System.getProperties()) {
                val next = String.valueOf(Integer.getInteger("com.github.bingoohuang.mtcp.pool_number", 0) + 1);
                System.setProperty("com.github.bingoohuang.mtcp.pool_number", next);
                return prefix + next;
            }
        } catch (AccessControlException e) {
            // The SecurityManager didn't allow us to read/write system properties
            // so just generate a random pool number instead
            val random = ThreadLocalRandom.current();
            val buf = new StringBuilder(prefix);

            for (int i = 0; i < 4; i++) {
                buf.append(ID_CHARACTERS[random.nextInt(62)]);
            }

            log.info("assigned random pool name '{}' (security manager prevented access to system properties)", buf);

            return buf.toString();
        }
    }

    private Object getObjectOrPerformJndiLookup(Object object) {
        if (object instanceof String) {
            try {
                val initCtx = new InitialContext();
                return initCtx.lookup((String) object);
            } catch (NamingException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return object;
    }
}
