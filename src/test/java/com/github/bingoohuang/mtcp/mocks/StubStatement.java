package com.github.bingoohuang.mtcp.mocks;

import com.github.bingoohuang.mtcp.util.UtilityElf;

import java.sql.*;

/**
 * @author Brett Wooldridge
 */
public class StubStatement implements Statement {
    public static volatile boolean oldDriver;

    private static volatile long simulatedQueryTime;
    private boolean closed;
    private Connection connection;

    public StubStatement(Connection connection) {
        this.connection = connection;
    }

    public static void setSimulatedQueryTime(long time) {
        simulatedQueryTime = time;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        checkClosed();
        return (T) this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        StubResultSet resultSet = new StubResultSet();
        return resultSet;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxFieldSize() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getMaxRows() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setMaxRows(int max) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getQueryTimeout() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (oldDriver) {
            throw new SQLFeatureNotSupportedException();
        }

        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void cancel() throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() throws SQLException {
        checkClosed();
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCursorName(String name) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        if (simulatedQueryTime > 0) {
            UtilityElf.quietlySleep(simulatedQueryTime);
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return new StubResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchDirection() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setFetchSize(int rows) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getFetchSize() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetConcurrency() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetType() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        return new StubResultSet();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getResultSetHoldability() throws SQLException {
        checkClosed();
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() {
        return closed;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isPoolable() throws SQLException {
        checkClosed();
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void closeOnCompletion() throws SQLException {
        checkClosed();
    }

    /**
     * {@inheritDoc}
     */
    public boolean isCloseOnCompletion() throws SQLException {
        checkClosed();
        return false;
    }

    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
}
