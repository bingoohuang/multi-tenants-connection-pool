package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.util.FastList;

import java.sql.*;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 * @author Brett Wooldridge
 */
public final class ProxyFactory {
    /**
     * Create a proxy for the specified {@link Connection} instance.
     *
     * @param poolEntry      the PoolEntry holding pool state
     * @param connection     the raw database Connection
     * @param openStatements a reusable list to track open Statement instances
     * @param leakTask       the ProxyLeakTask for this connection
     * @param now            the current timestamp
     * @param isReadOnly     the default readOnly state of the connection
     * @param isAutoCommit   the default autoCommit state of the connection
     * @return a proxy that wraps the specified {@link Connection}
     */
    static LightProxyConnection getProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit) {
        return new LightProxyConnection(poolEntry, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    static LightProxyStatement getProxyStatement(final ProxyConnection connection, final Statement statement) {
        return new LightProxyStatement(connection, statement);
    }

    static LightProxyCallableStatement getProxyCallableStatement(final ProxyConnection connection, final CallableStatement statement) {
        return new LightProxyCallableStatement(connection, statement);
    }

    static LightProxyPreparedStatement getProxyPreparedStatement(final ProxyConnection connection, final PreparedStatement statement) {
        return new LightProxyPreparedStatement(connection, statement);
    }

    static LightProxyResultSet getProxyResultSet(final ProxyConnection connection, final ProxyStatement statement, final ResultSet resultSet) {
        return new LightProxyResultSet(connection, statement, resultSet);
    }
}
