package com.github.bingoohuang.mtcp.mocks;

import com.github.bingoohuang.mtcp.util.UtilityElf;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

/**
 * @author Brett Wooldridge
 */
public class StubDataSource implements DataSource {
    private String user;
    private String password;
    private PrintWriter logWriter;
    private SQLException throwException;
    private long connectionAcquistionTime = 0;
    private int loginTimeout;

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setURL(String url) {
        // we don't care
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PrintWriter getLogWriter() {
        return logWriter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLogWriter(PrintWriter out) {
        this.logWriter = out;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setLoginTimeout(int seconds) {
        this.loginTimeout = seconds;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getLoginTimeout() {
        return loginTimeout;
    }

    /**
     * {@inheritDoc}
     */
    public Logger getParentLogger() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }

        throw new SQLException("Wrapped DataSource is not an instance of " + iface);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        if (throwException != null) {
            throw throwException;
        }
        if (connectionAcquistionTime > 0) {
            UtilityElf.quietlySleep(connectionAcquistionTime);
        }

        return new StubConnection();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection(String username, String password) {
        return new StubConnection();
    }

    public void setThrowException(SQLException e) {
        this.throwException = e;
    }

    public void setConnectionAcquistionTime(long connectionAcquisitionTime) {
        this.connectionAcquistionTime = connectionAcquisitionTime;
    }
}
