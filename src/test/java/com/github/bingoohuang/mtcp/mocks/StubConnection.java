/*
 * Copyright (C) 2013 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.bingoohuang.mtcp.mocks;

import com.github.bingoohuang.mtcp.util.UtilityElf;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Brett Wooldridge
 */
public class StubConnection extends StubBaseConnection implements Connection {
    public static final AtomicInteger count = new AtomicInteger();
    public static volatile boolean slowCreate;
    public static volatile boolean oldDriver;

    private static long foo;
    private boolean autoCommit;
    private int isolation = Connection.TRANSACTION_READ_COMMITTED;
    private String catalog;

    static {
        foo = System.currentTimeMillis();
    }

    public StubConnection() {
        count.incrementAndGet();
        if (slowCreate) {
            UtilityElf.quietlySleep(1000);
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }

        if (iface.isInstance(this)) {
            return (T) this;
        }

        throw new SQLException("Wrapped connection is not an instance of " + iface);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String nativeSQL(String sql) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        this.autoCommit = autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commit() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {

    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isClosed() throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatabaseMetaData getMetaData() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isReadOnly() throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setCatalog(String catalog) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        this.catalog = catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getCatalog() {
        return catalog;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        this.isolation = level;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getTransactionIsolation() {
        return isolation;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void clearWarnings() throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return new StubPreparedStatement(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Class<?>> getTypeMap() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTypeMap(Map<String, Class<?>> map) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setHoldability(int holdability) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getHoldability() {
        return (int) foo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Savepoint setSavepoint(String name) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollback(Savepoint savepoint) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void releaseSavepoint(Savepoint savepoint) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return new StubPreparedStatement(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return new StubPreparedStatement(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return new StubPreparedStatement(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return new StubPreparedStatement(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Clob createClob() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Blob createBlob() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NClob createNClob() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SQLXML createSQLXML() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isValid(int timeout) throws SQLException {
        if (throwException) {
            throw new SQLException();
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(String name, String value) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setClientInfo(Properties properties) {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getClientInfo(String name) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Properties getClientInfo() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Array createArrayOf(String typeName, Object[] elements) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Struct createStruct(String typeName, Object[] attributes) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public void setSchema(String schema) {
    }

    /**
     * {@inheritDoc}
     */
    public String getSchema() {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    public void abort(Executor executor) throws SQLException {
        throw new SQLException("Intentional exception during abort");
    }

    /**
     * {@inheritDoc}
     */
    public void setNetworkTimeout(Executor executor, int milliseconds) {
    }

    /**
     * {@inheritDoc}
     */
    public int getNetworkTimeout() {
        if (oldDriver) {
            throw new AbstractMethodError();
        }

        return 0;
    }

}
