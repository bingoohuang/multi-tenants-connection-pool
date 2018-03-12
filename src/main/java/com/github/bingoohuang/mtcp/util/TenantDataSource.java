package com.github.bingoohuang.mtcp.util;

import com.github.bingoohuang.mtcp.TenantCodeAware;
import com.google.common.collect.Maps;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.val;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.concurrent.ConcurrentMap;

public class TenantDataSource implements DataSource {
    private ConcurrentMap<String, ConnectionConfig> driverMap = Maps.newConcurrentMap();
    private TenantCodeAware tenantCodeAware;
    private TenantJdbcConfigurator tenantJdbcConfigurator;
    private Driver driver;

    public TenantDataSource() {

    }

    public TenantDataSource(TenantCodeAware tenantCodeAware, TenantJdbcConfigurator tenantJdbcConfigurator) {
        this.tenantCodeAware = tenantCodeAware;
        this.tenantJdbcConfigurator = tenantJdbcConfigurator;
        this.driver = createDriver(tenantJdbcConfigurator.getJdbcUrl());
    }

    @SneakyThrows
    public void setTenantCodeAwareClass(String tenantCodeAwareClass) {
        this.tenantCodeAware = (TenantCodeAware) Class.forName(tenantCodeAwareClass).newInstance();
    }

    @SneakyThrows
    public void setTenantJdbcConfiguratorClass(String tenantJdbcConfiguratorClass) {
        this.tenantJdbcConfigurator = (TenantJdbcConfigurator) Class.forName(tenantJdbcConfiguratorClass).newInstance();
        this.driver = createDriver(tenantJdbcConfigurator.getJdbcUrl());
    }

    @SneakyThrows
    private Driver createDriver(String jdbcUrl) {
        val driverClassName = DriverElf.getDriverClassName(jdbcUrl);
        return (Driver) Class.forName(driverClassName).newInstance();
    }

    @Value
    public static class ConnectionConfig {
        private final String jdbcUrl, user, password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        val config = getConnectionConfig();
        return DriverManager.getConnection(config.jdbcUrl, config.user, config.password);
    }

    private ConnectionConfig getConnectionConfig() {
        String tenantCode = tenantCodeAware.getTenantCode();
        ConnectionConfig config = driverMap.get(tenantCode);
        if (config == null) {
            val newConfig = tenantJdbcConfigurator.getConnectionConfig(tenantCode);
            val oldConfig = driverMap.putIfAbsent(tenantCode, newConfig);
            config = oldConfig != null ? oldConfig : newConfig;

        }
        return config;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLogWriter(PrintWriter logWriter) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
        DriverManager.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() {
        return DriverManager.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

}
