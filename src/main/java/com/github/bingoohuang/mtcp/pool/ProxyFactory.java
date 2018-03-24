/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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

package com.github.bingoohuang.mtcp.pool;

import com.github.bingoohuang.mtcp.util.FastList;

import java.sql.*;

/**
 * A factory class that produces proxies around instances of the standard
 * JDBC interfaces.
 *
 * @author Brett Wooldridge
 */
@SuppressWarnings("unused")
public final class ProxyFactory {
    private ProxyFactory() {
        // unconstructable
    }

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
    static ProxyConnection getProxyConnection(final PoolEntry poolEntry, final Connection connection, final FastList<Statement> openStatements, final ProxyLeakTask leakTask, final long now, final boolean isReadOnly, final boolean isAutoCommit) {
        return new LightProxyConnection(poolEntry, connection, openStatements, leakTask, now, isReadOnly, isAutoCommit);
    }

    static Statement getProxyStatement(final ProxyConnection connection, final Statement statement) {
        return new LightProxyStatement(connection, statement);
    }

    static CallableStatement getProxyCallableStatement(final ProxyConnection connection, final CallableStatement statement) {
        return new LightProxyCallableStatement(connection, statement);
    }

    static PreparedStatement getProxyPreparedStatement(final ProxyConnection connection, final PreparedStatement statement) {
        return new LightProxyPreparedStatement(connection, statement);
    }

    static ResultSet getProxyResultSet(final ProxyConnection connection, final ProxyStatement statement, final ResultSet resultSet) {
        // Body is replaced (injected) by JavassistProxyFactory
        return new LightProxyResultSet(connection, statement, resultSet);
    }
}
